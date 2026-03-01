from __future__ import annotations

import os
import threading
from typing import List

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
