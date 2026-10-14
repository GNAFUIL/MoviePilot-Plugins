# -*- coding: utf-8 -*-
"""
刷流转移续种 (BrushReseed) —— MoviePilot 插件
=====================================================
把「做流」下载器里已完成的刷流种子，从固态暂存目录【移动到】机械盘保种文件夹，
并重新定位 + 强制校验 + 自动做种(续种)；可选转到 Transmission 长期保种。
动态触发：做流堆积≥阈值 / 固态剩余<安全线 / 距上次≥兜底间隔，才移动(省机械盘)。
清理：机械盘剩余<阈值时，删最旧其次最大。只处理刷流目录，绝不触碰媒体库。
新增：种子标签同步转移(把 qbt 种子标签同步到 TR，并打保种标记)。
"""
import os
import time
import json
import shutil
import base64
import sqlite3
import subprocess
import urllib.request
import urllib.parse
import http.cookiejar
import threading
from datetime import datetime, timedelta
from typing import Any, List, Dict, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.sdk.config import settings
from app.sdk.logging import logger
from app.plugins import _PluginBase

try:
    from bencode import bdecode as _bdecode
except Exception:
    _bdecode = None


class BrushReseed(_PluginBase):
    plugin_name = "刷流转移续种"
    plugin_desc = "把做流已完成的刷流种子从固态移到机械盘保种，校验续种；可转TR长期保种。动态触发、标签同步、清理。不碰媒体库。"
    plugin_icon = "brushreseed.png"
    plugin_version = "1.1"
    plugin_author = "assistant"
    author_url = ""
    plugin_config_prefix = "brushreseed_"
    plugin_order = 21
    auth_level = 2

    # ===== 私有属性 =====
    _scheduler = None
    _enabled = False
    _onlyonce = False
    _cron = "*/30 * * * *"
    _downloader = "做流"                 # 来源下载器名(读 MoviePilot Downloaders 配置)
    _frompath = "/volume1/Download/link/做流"
    _topath = "/volume2/刷流保种"
    _autostart = True
    _notify = False
    _use_tag_filter = False
    _tags = ""
    # 动态触发
    _hdd_free_gb = 1024
    _trigger_size_gb = 150
    _ssd_free_gb = 150
    _max_interval_h = 12
    _last_file = "/volume1/@appstore/MoviePilot/config/brush_reseed_last.txt"
    # TR 保种
    _tr_enable = False
    _tr_remove_qbt = True
    _tr_url = ""
    _tr_user = ""
    _tr_pass = ""
    _tr_sid = None
    # 标签同步转移
    _tag_sync = True                     # 转移时同步种子标签到 TR
    _seed_tag = "刷流保种"                # 保种标记标签

    # qbt(从 MoviePilot Downloaders 配置读)
    _qbt_url = ""
    _qbt_user = ""
    _qbt_pass = ""
    _qbt_sid = None

    # ==================================================================
    # 生命周期
    # ==================================================================
    def init_plugin(self, config: dict = None):
        logger.info("刷流转移续种：init_plugin 被调用")
        self.stop_service()
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron") or "*/30 * * * *"
            self._downloader = config.get("downloader") or "做流"
            self._frompath = config.get("frompath") or self._frompath
            self._topath = config.get("topath") or self._topath
            self._autostart = config.get("autostart")
            self._notify = config.get("notify")
            self._use_tag_filter = config.get("use_tag_filter")
            self._tags = config.get("tags") or ""
            self._hdd_free_gb = float(config.get("hdd_free_gb") if config.get("hdd_free_gb") is not None else 1024)
            self._trigger_size_gb = float(config.get("trigger_size_gb") if config.get("trigger_size_gb") is not None else 150)
            self._ssd_free_gb = float(config.get("ssd_free_gb") if config.get("ssd_free_gb") is not None else 150)
            self._max_interval_h = float(config.get("max_interval_h") if config.get("max_interval_h") is not None else 12)
            self._tr_enable = config.get("tr_enable")
            self._tr_remove_qbt = config.get("tr_remove_qbt")
            self._tr_url = config.get("tr_url") or ""
            self._tr_user = config.get("tr_user") or ""
            self._tr_pass = config.get("tr_pass") or ""
            self._tag_sync = config.get("tag_sync")
            self._seed_tag = config.get("seed_tag") or "刷流保种"

        if self._enabled:
            logger.info(f"刷流转移续种：启用，onlyonce={self._onlyonce} cron={self._cron} 触发={self._trigger_size_gb}")
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._onlyonce:
                logger.info("刷流转移续种：立即运行一次")
                self._scheduler.add_job(self.transfer, 'date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=5),
                                        name="刷流转移续种-立即")
                self._onlyonce = False
            else:
                try:
                    self._scheduler.add_job(self.transfer, 'cron', **self._parse_cron(self._cron),
                                            name="刷流转移续种-周期")
                    logger.info(f"刷流转移续种：按 cron「{self._cron}」调度")
                except Exception as e:
                    logger.error(f"刷流转移续种 调度出错：{e}")
            self._scheduler.start()

    @staticmethod
    def _parse_cron(expr: str) -> dict:
        parts = expr.split()
        keys = ['minute', 'hour', 'day', 'month', 'day_of_week']
        return {k: parts[i] if i < len(parts) else '*' for i, k in enumerate(keys)}

    def get_state(self) -> bool:
        return self._enabled

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"退出刷流转移续种失败：{e}")

    # ==================================================================
    # 配置表单
    # ==================================================================
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {'component': 'VForm', 'content': [
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VSwitch', 'props': {'model': 'tr_enable', 'label': '转TR长期保种'}}]},
                ]},
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期(cron)'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'downloader', 'label': '来源下载器名'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VSwitch', 'props': {'model': 'autostart', 'label': '校验后自动做种'}}]},
                ]},
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'frompath', 'label': '源目录(固态刷流)'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'topath', 'label': '目标目录(机械保种)'}}]},
                ]},
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'trigger_size_gb', 'label': '触发:做流堆积(GB)'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'ssd_free_gb', 'label': '触发:固态剩余< (GB)'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'max_interval_h', 'label': '触发:兜底间隔(小时)'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'hdd_free_gb', 'label': '清理:机械盘剩余< (GB)'}}]},
                ]},
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VSwitch', 'props': {'model': 'tag_sync', 'label': '转移时同步标签到TR'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'seed_tag', 'label': '保种标记标签'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VSwitch', 'props': {'model': 'notify', 'label': '转移时通知'}}]},
                ]},
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'tr_url', 'label': 'TR 地址'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'tr_user', 'label': 'TR 用户'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'tr_pass', 'label': 'TR 密码', 'type': 'password'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VSwitch', 'props': {'model': 'tr_remove_qbt', 'label': '转TR后从qbt移除'}}]},
                ]},
                {'component': 'VRow', 'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VSwitch', 'props': {'model': 'use_tag_filter', 'label': '仅处理指定标签'}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 8}, 'content': [
                        {'component': 'VTextField', 'props': {'model': 'tags', 'label': '标签(逗号分隔,空=全部)'}}]},
                ]},
            ]}
        ], {
            "enabled": False, "onlyonce": False, "cron": "*/30 * * * *", "downloader": "做流",
            "frompath": "/volume1/Download/link/做流", "topath": "/volume2/刷流保种",
            "autostart": True, "notify": False, "use_tag_filter": False, "tags": "",
            "trigger_size_gb": 150, "ssd_free_gb": 150, "max_interval_h": 12, "hdd_free_gb": 1024,
            "tr_enable": False, "tr_remove_qbt": True, "tr_url": "", "tr_user": "", "tr_pass": "",
            "tag_sync": True, "seed_tag": "刷流保种",
        }

    def get_page(self) -> List[dict]:
        return [{'component': 'VAlert', 'props': {'type': 'info', 'text': '刷流转移续种：按触发条件把做流已完成种子移到机械盘保种并续种/转TR。详见日志。'}}]

    # ==================================================================
    # qbt 连接(从 MoviePilot Downloaders 配置读)
    # ==================================================================
    def _resolve_qbt_conn(self):
        if self._qbt_url:
            return
        try:
            db = sqlite3.connect('/volume1/@appstore/MoviePilot/config/user.db')
            v = db.execute("SELECT value FROM systemconfig WHERE key='Downloaders'").fetchone()
            db.close()
            dls = json.loads(v[0])
            for d in dls:
                if d.get('name') == self._downloader:
                    c = d.get('config') or {}
                    host = c.get('host') or ''
                    if host and ':' not in host:
                        host = host + ':8085'
                    self._qbt_url = 'http://' + host
                    self._qbt_user = c.get('username') or ''
                    self._qbt_pass = c.get('password') or ''
                    logger.info(f"刷流转移续种：qbt连接 url={self._qbt_url} user={self._qbt_user} passlen={len(self._qbt_pass)}")
                    return
        except Exception as e:
            logger.error(f"读取下载器配置失败：{e}")
        self._qbt_url = "http://192.168.31.170:8085"
        self._qbt_user = "admin"

    def _qbt_login(self):
        self._resolve_qbt_conn()
        self._qbt_cookies = http.cookiejar.CookieJar()
        self._qbt_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self._qbt_cookies))
        data = urllib.parse.urlencode({'username': self._qbt_user, 'password': self._qbt_pass}).encode()
        req = urllib.request.Request(self._qbt_url + '/api/v2/auth/login', data=data, method='POST')
        req.add_header('Content-Type', 'application/x-www-form-urlencoded')
        try:
            r = self._qbt_opener.open(req, timeout=15)
            body = r.read().decode().strip()
            has = any('QBT_SID' in c.name for c in self._qbt_cookies)
            if body == 'Ok.' or has:
                return True
            raise Exception(f"qbt 登录失败：{body}")
        except Exception as e:
            raise Exception(f"qbt 登录异常：{e}")

    def _qbt(self, path, params=None, is_json=False):
        if not getattr(self, '_qbt_opener', None):
            self._qbt_login()
        url = self._qbt_url + path
        data = urllib.parse.urlencode(params).encode() if params else None
        req = urllib.request.Request(url, data=data, method='POST' if params else 'GET')
        r = self._qbt_opener.open(req, timeout=60)
        body = r.read().decode()
        return json.loads(body) if is_json else body

    def _qbt_set_location(self, h, location):
        self._qbt("/api/v2/torrents/setLocation", {"hashes": h, "location": location})

    # ==================================================================
    # TR
    # ==================================================================
    def _tr_rpc(self, method, args=None):
        body = json.dumps({'method': method, 'arguments': args or {}}).encode()
        req = urllib.request.Request(self._tr_url, data=body, method='POST')
        req.add_header('Content-Type', 'application/json')
        req.add_header('Authorization', 'Basic ' + base64.b64encode(f"{self._tr_user}:{self._tr_pass}".encode()).decode())
        if self._tr_sid:
            req.add_header('X-Transmission-Session-Id', self._tr_sid)
        try:
            r = urllib.request.urlopen(req, timeout=60)
            return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 409:
                self._tr_sid = e.headers.get('X-Transmission-Session-Id')
                req.add_header('X-Transmission-Session-Id', self._tr_sid)
                r = urllib.request.urlopen(req, timeout=60)
                return json.loads(r.read().decode())
            raise

    def _get_tracker(self, h):
        try:
            trks = self._qbt("/api/v2/torrents/trackers?hash=" + h, is_json=True) or []
            for tk in trks:
                u = tk.get("url") or ""
                if u.startswith("http") and "announce" in u:
                    return u
        except Exception:
            pass
        return None

    def _transfer_to_tr(self, h, name, tags=None):
        if not self._tr_enable:
            return False
        bt = "/volume1/@appdata/qBittorrent/qBittorrent_conf/data/BT_backup"
        tp = os.path.join(bt, h + ".torrent")
        if not os.path.exists(tp):
            logger.warning(f"转TR: 找不到 {name} 的 .torrent({tp})")
            return False
        with open(tp, "rb") as f:
            meta = base64.b64encode(f.read()).decode()
        try:
            res = self._tr_rpc('torrent-add', {'metainfo': meta, 'download-dir': self._topath, 'paused': True})
        except Exception as e:
            logger.warning(f"转TR: 添加 {name} 出错：{e}")
            return False
        if res.get('result') != 'success':
            logger.warning(f"转TR: 添加 {name} 失败：{res}")
            return False
        logger.info(f"转TR: {name} 已加入 TR 保种(暂停，待校验)")
        try:
            args = res.get('arguments') or {}
            ta = args.get('torrent-added') or args.get('torrent-duplicate') or {}
            tid = ta.get('id')
            if tid is not None:
                try:
                    u = self._get_tracker(h)
                    if u:
                        self._tr_rpc('torrent-set', {'ids': [tid], 'trackerAdd': [u]})
                except Exception as e:
                    logger.warning(f"转TR: 补 tracker 出错 {e}")
                # 标签同步转移：把 qbt 标签同步到 TR + 打保种标记
                if self._tag_sync:
                    try:
                        tset = []
                        for t in (tags or []):
                            if t:
                                tset.append(t)
                        if self._seed_tag and self._seed_tag not in tset:
                            tset.append(self._seed_tag)
                        if tset:
                            self._tr_rpc('torrent-set', {'ids': [tid], 'labels': tset})
                            logger.info(f"转TR: {name} 标签同步 {tset}")
                    except Exception as e:
                        logger.warning(f"转TR: 标签同步出错 {e}")
                self._tr_rpc('torrent-verify', {'ids': [tid]})
                logger.info(f"转TR: {name} 已触发本地校验，校验完自动做种")
                threading.Thread(target=self._tr_verify_then_start, args=(tid, name), daemon=True).start()
        except Exception as e:
            logger.warning(f"转TR: 处理 {name} 出错：{e}")
        if self._tr_remove_qbt:
            try:
                self._qbt("/api/v2/torrents/delete", {"hashes": h, "deleteFiles": "false"})
                logger.info(f"转TR: {name} 已从 qbt 移除(避免双开)")
            except Exception as e:
                logger.warning(f"转TR: 从 qbt 移除 {name} 出错：{e}")
        return True

    def _tr_verify_then_start(self, tid, name):
        for _ in range(240):
            time.sleep(30)
            try:
                res = self._tr_rpc('torrent-get', {'ids': [tid], 'fields': ['status', 'percentDone', 'leftUntilDone']})
                ts = (res.get('arguments') or {}).get('torrents', [])
                if not ts:
                    return
                t = ts[0]
                st = t.get('status'); pd = t.get('percentDone') or 0
                if st not in (1, 2):
                    if pd >= 1.0:
                        self._tr_rpc('torrent-start', {'ids': [tid]})
                        logger.info(f"转TR: {name} 校验完成(100%)，已开始做种")
                    else:
                        logger.warning(f"转TR: {name} 校验后仍不完整({pd:.0%})，保持暂停不重下")
                    return
            except Exception as e:
                logger.warning(f"转TR: 监控 {name} 校验出错 {e}")
                return

    # ==================================================================
    # 触发 / 移动 / 清理
    # ==================================================================
    def _dir_size(self, path):
        total = 0
        try:
            for root, _d, files in os.walk(path):
                for f in files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except Exception:
                        pass
        except Exception:
            pass
        return total

    def _wait_moved(self, name, topath, timeout=1800, interval=5):
        probe = os.path.join(topath, name)
        deadline = time.time() + timeout
        last = -1; stable = 0
        while time.time() < deadline:
            if os.path.exists(probe):
                cur = self._dir_size(probe)
                if cur == last and cur > 0:
                    stable += 1
                    if stable >= 2:
                        logger.info(f"移动完成：{name} ({cur/1024**3:.1f}GB)")
                        return True
                else:
                    stable = 0
                last = cur
            time.sleep(interval)
        logger.warning(f"等待移动超时：{name}")

    def _should_transfer(self):
        frompath = os.path.normpath(self._frompath)
        frompaths = [os.path.normpath(x.strip()) for x in self._frompath.split(",") if x.strip()]
        completed = 0
        try:
            for t in (self._qbt("/api/v2/torrents/info", is_json=True) or []):
                sp = os.path.normpath(t.get("save_path") or "")
                cp = os.path.normpath(t.get("content_path") or "")
                if any(sp == fp or sp.startswith(fp + os.sep) for fp in frompaths) and (t.get("progress") or 0) >= 1:
                    completed += int(t.get("size") or 0)
        except Exception as e:
            logger.warning(f"触发检查:读种子出错 {e}")
        try:
            free_gb = shutil.disk_usage(frompath).free / 1024 ** 3
        except Exception:
            free_gb = 9999
        last = 0
        try:
            if os.path.exists(self._last_file):
                last = float(open(self._last_file).read().strip() or 0)
        except Exception:
            last = 0
        since_h = (time.time() - last) / 3600 if last else 9999.0
        logger.info(f"触发检查: 做流已完成 {completed/1024**3:.1f}GB(阈值{self._trigger_size_gb:.0f}) / "
                    f"固态剩余 {free_gb:.0f}GB(安全线{self._ssd_free_gb:.0f}) / 距上次 {since_h:.1f}h(兜底{self._max_interval_h:.0f})")
        if completed / 1024 ** 3 >= self._trigger_size_gb:
            return True
        if free_gb < self._ssd_free_gb:
            return True
        if since_h >= self._max_interval_h:
            return True
        logger.info("→ 未达触发条件，本轮跳过(不移动，省机械盘)")
        return False

    def _mark_transferred(self):
        try:
            with open(self._last_file, "w") as f:
                f.write(str(time.time()))
        except Exception as e:
            logger.warning(f"写转移时间失败：{e}")

    def transfer(self):
        logger.info("刷流转移续种：transfer 开始执行")
        try:
            if not self._should_transfer():
                return
        except Exception as e:
            logger.warning(f"触发检查异常，继续转移: {e}")
        logger.info("开始扫描刷流种子…")
        frompaths = [os.path.normpath(x.strip()) for x in self._frompath.split(",") if x.strip()]
        topath = os.path.normpath(self._topath)
        if not any(os.path.exists(fp) for fp in frompaths):
            logger.warning(f"源目录都不存在：{frompaths}")
            return
        os.makedirs(topath, exist_ok=True)
        try:
            os.chmod(topath, 0o777)
        except Exception:
            pass
        try:
            _all = self._qbt("/api/v2/torrents/info", is_json=True) or []
            torrents = [t for t in _all if (t.get("progress") or 0) >= 1]
        except Exception as e:
            logger.error(f"获取已完成种子失败：{e}")
            return
        tag_filter = {t.strip() for t in self._tags.split(",") if t.strip()} if self._use_tag_filter else None
        moved = 0
        for t in torrents:
            try:
                h = t.get("hash") or t.get("id") or ""
                name = t.get("name") or ""
                save_path = os.path.normpath(t.get("save_path") or t.get("content_path") or "")
                tags = [x.strip() for x in (t.get("tags") or "").replace("，", ",").split(",") if x.strip()]
                if not any(save_path == fp or save_path.startswith(fp + os.sep) for fp in frompaths):
                    continue
                if tag_filter and not (set(tags) & tag_filter):
                    continue
                logger.info(f"转移：{name} → {topath}")
                self._qbt_set_location(h, topath)
                moved += 1
                self._wait_moved(name, topath)
                if self._tr_enable:
                    self._transfer_to_tr(h, name, tags=tags)
                else:
                    try:
                        self._qbt("/api/v2/torrents/recheck", {"hashes": h})
                    except Exception as e:
                        logger.warning(f"校验 {name} 出错：{e}")
                    if self._autostart:
                        try:
                            self._qbt("/api/v2/torrents/start", {"hashes": h})
                        except Exception as e:
                            logger.warning(f"开始做种 {name} 出错：{e}")
            except Exception as e:
                logger.error(f"处理种子出错：{e}")
        logger.info(f"本轮完成：转移 {moved} 个刷流种子到 {topath}")
        if moved:
            self._mark_transferred()
        try:
            self.trim()
        except Exception as e:
            logger.warning(f"清理出错：{e}")

    def trim(self):
        topath = os.path.normpath(self._topath)
        threshold = float(self._hdd_free_gb or 1024) * 1024 ** 3
        try:
            free = shutil.disk_usage(topath).free
        except Exception:
            free = threshold + 1
        seeds = []
        try:
            for t in (self._qbt("/api/v2/torrents/info", is_json=True) or []):
                cp = os.path.normpath(t.get("content_path") or "")
                if cp == topath or cp.startswith(topath + os.sep):
                    seeds.append({'client': 'qbt', 'id': t.get('hash'), 'name': t.get('name') or '',
                                  'size': int(t.get('size') or 0), 'added': int(t.get('added_on') or 0)})
        except Exception as e:
            logger.warning(f"清理：读 qbt 种子出错 {e}")
        if self._tr_enable:
            try:
                res = self._tr_rpc('torrent-get', {'fields': ['id', 'name', 'totalSize', 'addedDate', 'downloadDir']})
                for t in (res.get('arguments') or {}).get('torrents', []):
                    dd = os.path.normpath(t.get('downloadDir') or "")
                    if dd == topath or dd.startswith(topath + os.sep):
                        seeds.append({'client': 'tr', 'id': t.get('id'), 'name': t.get('name') or '',
                                      'size': int(t.get('totalSize') or 0), 'added': int(t.get('addedDate') or 0)})
            except Exception as e:
                logger.warning(f"清理：读 TR 种子出错 {e}")
        if free >= threshold:
            logger.info(f"机械盘剩余 {free/1024**3:.0f}GB 未低于 {self._hdd_free_gb:.0f}GB，不清理")
            return
        logger.warning(f"机械盘剩余 {free/1024**3:.0f}GB 低于 {self._hdd_free_gb:.0f}GB，开始清理最旧其次最大")
        target = int(threshold * 1.1)
        seeds.sort(key=lambda x: (x['added'], -x['size']))
        removed = 0
        for s in seeds:
            if free >= target:
                break
            try:
                if s['client'] == 'qbt':
                    self._qbt("/api/v2/torrents/delete", {"hashes": s['id'], "deleteFiles": "true"})
                else:
                    self._tr_rpc('torrent-remove', {'ids': [s['id']], 'delete-local-data': True})
                free += s['size']
                removed += 1
                logger.info(f"清理[{s['client']}]：{s['name'][:38]} ({s['size']/1024**3:.1f}GB) 已删种子+文件")
            except Exception as e:
                logger.warning(f"清理 {s['name'][:30]} 出错：{e}")
        logger.info(f"清理完成：删了 {removed} 个，机械盘剩余升至 {free/1024**3:.0f}GB")
