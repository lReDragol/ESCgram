from __future__ import annotations

import os
import threading
import re
from typing import List, Optional, Dict, Any, Tuple

from PySide6.QtCore import QObject, Signal, Slot


class DialogsStreamWorker(QObject):
    batch = Signal(list)   # List[{"id","title","type"}]
    done = Signal()

    def __init__(self, server, limit=400, batch_size=60):
        super().__init__()
        self.server = server
        self.limit = limit
        self.batch_size = batch_size
        from queue import Queue
        self._q = Queue()
        self._stop = False

    def stop(self) -> None:
        self._stop = True
        try:
            self._q.put_nowait(None)
        except Exception:
            pass

    @Slot()
    def run(self):
        def _start_stream() -> None:
            try:
                self.server.stream_telegram_chats(
                    on_batch=lambda b: self._q.put(list(b)),
                    on_done=lambda: self._q.put(None),
                    limit=self.limit,
                    batch_size=self.batch_size
                )
            except Exception:
                try:
                    self._q.put(None)
                except Exception:
                    pass

        threading.Thread(target=_start_stream, daemon=True, name="dialogs-stream-bridge").start()
        while True:
            if self._stop:
                break
            item = self._q.get()
            if item is None:
                break
            self.batch.emit(item)
        self.done.emit()


class HistoryWorker(QObject):
    batch = Signal(list)
    finished = Signal()

    def __init__(
        self,
        server,
        chat_id: str,
        limit: int = 80,
        batch_size: int = 20,
        *,
        include_deleted: bool = False,
    ):
        super().__init__()
        self.server = server
        self.chat_id = chat_id
        self.limit = limit
        self.batch_size = max(1, int(batch_size))
        self.include_deleted = bool(include_deleted)
        self._stop = False
        try:
            self.remote_timeout = float(os.getenv("DRAGO_HISTORY_REMOTE_TIMEOUT", "25.0") or 25.0)
        except Exception:
            self.remote_timeout = 25.0

    def stop(self):
        self._stop = True

    @Slot()
    def run(self):
        def _emit(items: List[dict]) -> None:
            if not items:
                return
            seq = list(reversed(items))
            B = self.batch_size
            for i in range(0, len(seq), B):
                if self._stop:
                    break
                self.batch.emit(seq[i:i + B])

        def _entry_changed(prev: dict, cur: dict) -> bool:
            watched = (
                "text",
                "entities",
                "type",
                "file_path",
                "thumb_path",
                "file_size",
                "reply_to",
                "is_deleted",
                "forward_info",
                "duration",
                "waveform",
                "media_group_id",
            )
            for key in watched:
                if prev.get(key) != cur.get(key):
                    return True
            return False

        # Phase 1: show cached messages immediately (no network).
        cached: List[dict] = []
        if hasattr(self.server, "fetch_chat_history_cached"):
            try:
                try:
                    cached = self.server.fetch_chat_history_cached(
                        self.chat_id,
                        limit=self.limit,
                        include_deleted=self.include_deleted,
                    ) or []
                except TypeError:
                    cached = self.server.fetch_chat_history_cached(
                        self.chat_id,
                        limit=self.limit,
                    ) or []
            except Exception:
                cached = []
        cached_by_id = {}
        for item in cached:
            try:
                cached_by_id[int(item.get("id"))] = item
            except Exception:
                continue
        if self._stop:
            self.finished.emit()
            return
        _emit(cached)

        # Phase 2: refresh from Telegram (merged with cache).
        try:
            try:
                msgs = self.server.fetch_chat_history(
                    self.chat_id,
                    limit=self.limit,
                    download_media=False,
                    timeout=self.remote_timeout,
                    include_deleted=self.include_deleted,
                )
            except TypeError:
                msgs = self.server.fetch_chat_history(
                    self.chat_id,
                    limit=self.limit,
                    download_media=False,
                    timeout=self.remote_timeout,
                )
        except Exception:
            msgs = []
        if self._stop:
            self.finished.emit()
            return
        if cached_by_id and msgs:
            filtered: List[dict] = []
            for item in list(msgs or []):
                try:
                    mid = int(item.get("id"))
                except Exception:
                    filtered.append(item)
                    continue
                prev = cached_by_id.get(mid)
                if prev is None or _entry_changed(prev, item):
                    filtered.append(item)
            msgs = filtered
        _emit(list(msgs or []))
        self.finished.emit()


class LastDateWorker(QObject):
    tick = Signal(str, int)
    done = Signal()

    def __init__(self, server, chat_ids: List[str], limit_each: int = 1):
        super().__init__()
        self.server = server
        self.chat_ids = list(chat_ids)
        self.limit_each = limit_each
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    @Slot()
    def run(self):
        import time

        for cid in self.chat_ids:
            if self._stop:
                break
            ts = 0
            try:
                msgs = self.server.fetch_chat_history(cid, limit=self.limit_each, download_media=False)
                if msgs:
                    ts = int(msgs[0].get("date") or 0)
            except Exception:
                ts = 0
            if self._stop:
                break
            self.tick.emit(str(cid), int(ts))
            slept = 0.0
            while not self._stop and slept < 2.2:
                time.sleep(0.1)
                slept += 0.1
        self.done.emit()


class ReleaseCheckWorker(QObject):
    finished = Signal(dict)

    def __init__(self, *, repo: str, current_version: str):
        super().__init__()
        self.repo = str(repo or "").strip()
        self.current_version = str(current_version or "").strip()

    @staticmethod
    def _normalize_version(value: str) -> Optional[Tuple[int, ...]]:
        raw = str(value or "").strip()
        if not raw:
            return None
        raw = raw.lstrip("vV")
        parts = re.findall(r"\d+", raw)
        if not parts:
            return None
        try:
            return tuple(int(p) for p in parts[:4])
        except Exception:
            return None

    @classmethod
    def _is_newer(cls, remote: str, local: str) -> bool:
        rv = cls._normalize_version(remote)
        lv = cls._normalize_version(local)
        if rv is None or lv is None:
            return False
        width = max(len(rv), len(lv))
        rvp = rv + (0,) * (width - len(rv))
        lvp = lv + (0,) * (width - len(lv))
        return rvp > lvp

    @staticmethod
    def _pick_asset(assets: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not assets:
            return None
        target = "windows" if os.name == "nt" else "linux"
        by_name = {str(a.get("name") or "").lower(): a for a in assets}
        if target == "windows":
            for suffix in ("setup.exe", ".exe"):
                for name, asset in by_name.items():
                    if name.endswith(suffix):
                        return asset
        if target == "linux":
            for suffix in (".tar.gz", ".tgz", ".appimage"):
                for name, asset in by_name.items():
                    if name.endswith(suffix):
                        return asset
        for name, asset in by_name.items():
            if name.endswith(".zip"):
                return asset
        return assets[0]

    @Slot()
    def run(self) -> None:
        payload: Dict[str, Any] = {
            "ok": False,
            "current_version": self.current_version,
            "latest_version": "",
            "update_available": False,
            "download_url": "",
            "asset_name": "",
            "release_page": "",
            "error": "",
        }
        repo = self.repo
        if not repo:
            payload["error"] = "Не задан репозиторий обновлений"
            self.finished.emit(payload)
            return
        try:
            import requests

            url = f"https://api.github.com/repos/{repo}/releases/latest"
            resp = requests.get(url, timeout=(3.5, 8.0), headers={"Accept": "application/vnd.github+json"})
            resp.raise_for_status()
            data = resp.json() if resp.content else {}
            tag_name = str(data.get("tag_name") or "").strip()
            html_url = str(data.get("html_url") or "").strip()
            assets = list(data.get("assets") or [])
            picked = self._pick_asset(assets)
            asset_url = str((picked or {}).get("browser_download_url") or "").strip()
            asset_name = str((picked or {}).get("name") or "").strip()
            payload.update(
                {
                    "ok": True,
                    "latest_version": tag_name,
                    "release_page": html_url,
                    "download_url": asset_url,
                    "asset_name": asset_name,
                    "update_available": bool(tag_name and self._is_newer(tag_name, self.current_version)),
                }
            )
        except Exception as exc:
            payload["error"] = str(exc)
        self.finished.emit(payload)


class UpdateDownloadWorker(QObject):
    progress = Signal(int, int)  # downloaded_bytes, total_bytes
    finished = Signal(dict)

    def __init__(self, *, url: str, output_path: str):
        super().__init__()
        self.url = str(url or "").strip()
        self.output_path = str(output_path or "").strip()

    @Slot()
    def run(self) -> None:
        payload: Dict[str, Any] = {"ok": False, "path": self.output_path, "error": ""}
        if not self.url:
            payload["error"] = "Пустой URL обновления"
            self.finished.emit(payload)
            return
        if not self.output_path:
            payload["error"] = "Не задан путь сохранения обновления"
            self.finished.emit(payload)
            return
        try:
            import requests

            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            with requests.get(self.url, stream=True, timeout=(5.0, 30.0)) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length") or 0)
                downloaded = 0
                with open(self.output_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        downloaded += len(chunk)
                        self.progress.emit(downloaded, total)
            payload["ok"] = True
        except Exception as exc:
            payload["error"] = str(exc)
        self.finished.emit(payload)
