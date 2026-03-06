from __future__ import annotations

import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from PySide6.QtCore import QObject, Signal, Slot
from PySide6.QtGui import QColor, QPixmap

from ui.components.avatar import make_avatar_pixmap


class _DownloadSignal(QObject):
    ready = Signal(str, str, str, str)  # cache_key, kind, entity_id, path


@dataclass
class _AvatarMeta:
    kind: str          # "chat" | "user"
    entity_id: str
    title: str
    initials: str
    background: QColor


class AvatarCache:
    """Resolve and cache avatar pixmaps without blocking the GUI thread."""

    def __init__(
        self,
        server,
        size: int = 40,
        *,
        on_ready: Optional[Callable[[str, str], None]] = None,
        max_workers: int = 3,
    ) -> None:
        self._server = server
        self._size = max(16, size)
        self._cache: Dict[str, QPixmap] = {}
        self._paths: Dict[str, Optional[str]] = {}
        self._failed_at: Dict[str, float] = {}
        self._pending: Dict[str, _AvatarMeta] = {}
        self._on_ready = on_ready
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="avatar-cache")
        self._signal = _DownloadSignal()
        self._signal.ready.connect(self._on_download_ready)

    def shutdown(self) -> None:
        with self._lock:
            self._pending.clear()
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)  # type: ignore[call-arg]
        except TypeError:
            # Python < 3.9 compatibility (cancel_futures unavailable)
            self._executor.shutdown(wait=False)

    def assistant(self) -> QPixmap:
        key = "assistant"
        if key not in self._cache:
            self._cache[key] = make_avatar_pixmap(
                self._size,
                None,
                "AI",
                background=QColor("#1d3b54"),
            )
        return self._cache[key]

    def chat(self, chat_id: str, info: Dict[str, Any]) -> QPixmap:
        title = str(info.get("title") or chat_id)
        photo_small = info.get("photo_small_id") or info.get("photo_small")
        cache_key = f"chat:{chat_id}:{photo_small or 'none'}"
        path = self._paths.get(cache_key)
        background = self._color(f"chat:{chat_id}")
        initials = self._initials(title)

        if path:
            pix = self._cache.get(cache_key)
            if pix:
                return pix
            pix = make_avatar_pixmap(self._size, path, initials, background=background)
            self._cache[cache_key] = pix
            return pix

        placeholder = self._cache.get(cache_key)
        if placeholder is None:
            placeholder = make_avatar_pixmap(self._size, None, initials, background=background)
            self._cache[cache_key] = placeholder

        failed_at = float(self._failed_at.get(cache_key, 0.0) or 0.0)
        can_retry = (time.time() - failed_at) >= 3.0
        if can_retry:
            # Even if photo id is missing in dialog payload, Telegram can often resolve it by chat id.
            self._schedule_download(
                cache_key=cache_key,
                kind="chat",
                entity_id=str(chat_id),
                title=title,
                initials=initials,
                background=background,
                fetch_args={"chat_id": str(chat_id), "file_id": (str(photo_small) if photo_small else None)},
            )

        return placeholder

    def user(self, user_id: str, header: str) -> QPixmap:
        normalized_id = user_id or "unknown"
        cache_key = f"user:{normalized_id}"
        background = self._color(cache_key)
        initials = self._initials(header)
        path = self._paths.get(cache_key)

        if path:
            pix = self._cache.get(cache_key)
            if pix:
                return pix
            pix = make_avatar_pixmap(self._size, path, initials, background=background)
            self._cache[cache_key] = pix
            return pix

        placeholder = self._cache.get(cache_key)
        if placeholder is None:
            placeholder = make_avatar_pixmap(self._size, None, initials, background=background)
            self._cache[cache_key] = placeholder

        failed_at = float(self._failed_at.get(cache_key, 0.0) or 0.0)
        can_retry = (time.time() - failed_at) >= 3.0
        if (cache_key not in self._paths or not path) and can_retry:
            self._schedule_download(
                cache_key=cache_key,
                kind="user",
                entity_id=normalized_id,
                title=header,
                initials=initials,
                background=background,
                fetch_args={"user_id": normalized_id},
            )

        return placeholder

    def _schedule_download(
        self,
        *,
        cache_key: str,
        kind: str,
        entity_id: str,
        title: str,
        initials: str,
        background: QColor,
        fetch_args: Dict[str, Any],
    ) -> None:
        with self._lock:
            if cache_key in self._pending:
                return
            cached_path = self._paths.get(cache_key)
            # Do not queue duplicate work when we already have a resolved avatar path.
            # Empty strings/None mean previous attempt failed and should be retryable.
            if isinstance(cached_path, str) and cached_path.strip():
                return
            self._pending[cache_key] = _AvatarMeta(
                kind=kind,
                entity_id=entity_id,
                title=title,
                initials=initials,
                background=background,
            )

        def _task() -> None:
            path: Optional[str] = None
            try:
                if kind == "chat":
                    path = self._server.ensure_chat_avatar(
                        fetch_args["chat_id"],
                        file_id=fetch_args.get("file_id"),
                    )
                elif kind == "user":
                    path = self._server.ensure_user_avatar(fetch_args["user_id"])
            except Exception:
                path = None
            finally:
                self._signal.ready.emit(cache_key, kind, entity_id, path or "")

        try:
            self._executor.submit(_task)
        except RuntimeError:
            # Executor already shut down; just drop the request.
            pass

    @Slot(str, str, str, str)
    def _on_download_ready(self, cache_key: str, kind: str, entity_id: str, path: str) -> None:
        meta = self._pending.pop(cache_key, None)
        normalized = path or ""
        self._paths[cache_key] = normalized
        if normalized:
            self._failed_at.pop(cache_key, None)
        else:
            self._failed_at[cache_key] = time.time()
        if not meta:
            return

        if normalized:
            pix = make_avatar_pixmap(self._size, path, meta.initials, background=meta.background)
            self._cache[cache_key] = pix

        if self._on_ready:
            try:
                self._on_ready(kind, entity_id)
            except Exception:
                pass

    @staticmethod
    def _initials(title: str) -> str:
        words = [w for w in (title or "").strip().split() if w]
        if not words:
            return "?"
        if len(words) == 1:
            return words[0][:2].upper()
        return (words[0][0] + words[1][0]).upper()

    @staticmethod
    def _color(key: str) -> QColor:
        palette = [
            "#2b5278",
            "#118ab2",
            "#ef476f",
            "#06d6a0",
            "#ffd166",
            "#8338ec",
            "#ff6d00",
        ]
        digest = hashlib.sha1(key.encode("utf-8", "ignore")).hexdigest()
        idx = int(digest[:2], 16) % len(palette)
        return QColor(palette[idx])
