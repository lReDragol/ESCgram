from __future__ import annotations

import os
from typing import Any, List, Optional, Tuple

from PySide6.QtCore import QObject, Signal, Slot


class StickerSetsWorker(QObject):
    done = Signal(list)   # List[dict]
    error = Signal(str)

    def __init__(self, tg_adapter: Any):
        super().__init__()
        self.tg = tg_adapter

    @Slot()
    def run(self) -> None:
        try:
            res = []
            if self.tg and hasattr(self.tg, "list_sticker_sets_sync"):
                res = self.tg.list_sticker_sets_sync() or []
            self.done.emit(list(res))
        except Exception as exc:
            self.error.emit(str(exc))
            self.done.emit([])


class RecentStickersWorker(QObject):
    done = Signal(list)   # List[dict]
    error = Signal(str)

    def __init__(self, tg_adapter: Any):
        super().__init__()
        self.tg = tg_adapter

    @Slot()
    def run(self) -> None:
        try:
            res = []
            if self.tg and hasattr(self.tg, "get_recent_stickers_sync"):
                res = self.tg.get_recent_stickers_sync(attached=False) or []
            self.done.emit(list(res))
        except Exception as exc:
            self.error.emit(str(exc))
            self.done.emit([])


class SavedGifsWorker(QObject):
    done = Signal(list)   # List[dict]
    error = Signal(str)

    def __init__(self, source: Any, *, limit: int = 24):
        super().__init__()
        self.source = source
        self.limit = max(1, int(limit or 24))

    @Slot()
    def run(self) -> None:
        try:
            rows = []
            getter = getattr(self.source, "get_saved_gifs", None)
            if callable(getter):
                rows = getter(limit=self.limit) or []
            else:
                getter = getattr(self.source, "get_saved_gifs_sync", None)
                if callable(getter):
                    rows = getter(limit=self.limit) or []
            self.done.emit(list(rows))
        except Exception as exc:
            self.error.emit(str(exc))
            self.done.emit([])


class StickerSetItemsWorker(QObject):
    done = Signal(list)   # List[dict]
    error = Signal(str)

    def __init__(self, tg_adapter: Any, *, set_id: int, access_hash: int):
        super().__init__()
        self.tg = tg_adapter
        self.set_id = int(set_id or 0)
        self.access_hash = int(access_hash or 0)

    @Slot()
    def run(self) -> None:
        try:
            res = []
            if self.tg and hasattr(self.tg, "get_sticker_set_items_sync"):
                res = self.tg.get_sticker_set_items_sync(set_id=self.set_id, access_hash=self.access_hash) or []
            self.done.emit(list(res))
        except Exception as exc:
            self.error.emit(str(exc))
            self.done.emit([])


class StickerThumbWorker(QObject):
    thumb_ready = Signal(str, str)  # file_id, path
    finished = Signal()

    def __init__(self, tg_adapter: Any, tasks: List[Tuple[str, str]]):
        super().__init__()
        self.tg = tg_adapter
        self.tasks = list(tasks or [])
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    @Slot()
    def run(self) -> None:
        try:
            for file_id, target in self.tasks:
                if self._stop:
                    break
                fid = str(file_id or "").strip()
                out_path = str(target or "").strip()
                if not fid or not out_path:
                    continue
                if os.path.isfile(out_path):
                    self.thumb_ready.emit(fid, out_path)
                    continue
                try:
                    if self.tg and hasattr(self.tg, "download_file_id_sync"):
                        path = self.tg.download_file_id_sync(fid, file_name=out_path, timeout=12.0) or ""
                    else:
                        path = ""
                except Exception:
                    path = ""
                if path:
                    self.thumb_ready.emit(fid, str(path))
        finally:
            self.finished.emit()
