from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QPoint, Qt, QSignalBlocker, QThread, QTimer, Signal
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QTabWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from ui.emoji_picker import DEFAULT_EMOJIS
from ui.sticker_workers import RecentStickersWorker, StickerSetItemsWorker, StickerSetsWorker, StickerThumbWorker


class MediaPickerPopup(QFrame):
    emojiSelected = Signal(str)
    stickerSelected = Signal(str)
    gifPickRequested = Signal()

    def __init__(self, tg_adapter: Any, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent, Qt.WindowType.Popup)
        self.tg = tg_adapter
        self.setObjectName("mediaPicker")
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setStyleSheet(
            "QFrame#mediaPicker{background-color:#0f1b27;border:1px solid rgba(255,255,255,0.08);"
            "border-radius:14px;padding:10px;}"
            "QTabWidget::pane{border:0;}"
            "QTabBar::tab{background:transparent;color:#bfc8d6;padding:6px 10px;margin-right:6px;}"
            "QTabBar::tab:selected{color:#f4f7ff;background-color:rgba(255,255,255,0.06);border-radius:10px;}"
            "QToolButton{background:transparent;border:none;font-size:18px;padding:6px;}"
            "QToolButton:hover{background-color:rgba(255,255,255,0.08);border-radius:10px;}"
        )

        self._sets_thread: Optional[QThread] = None
        self._recent_thread: Optional[QThread] = None
        self._items_thread: Optional[QThread] = None
        self._thumb_thread: Optional[QThread] = None
        self._sets_worker: Optional[StickerSetsWorker] = None
        self._recent_worker: Optional[RecentStickersWorker] = None
        self._items_worker: Optional[StickerSetItemsWorker] = None
        self._thumb_worker: Optional[StickerThumbWorker] = None
        self._thumb_tasks: List[tuple[str, str]] = []

        self._recent_stickers: List[Dict[str, Any]] = []
        self._current_stickers: List[Dict[str, Any]] = []
        self._sticker_buttons: Dict[str, QToolButton] = {}

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        self.tabs = QTabWidget()
        root.addWidget(self.tabs, 1)

        self.tabs.addTab(self._build_emoji_tab(), "Эмодзи")
        self.tabs.addTab(self._build_stickers_tab(), "Стикеры")
        self.tabs.addTab(self._build_gif_tab(), "GIF")

        # Lazy load sticker data on show (avoid UI freezes).
        QTimer.singleShot(0, self._lazy_refresh_stickers)

    def showEvent(self, event) -> None:  # type: ignore[override]
        super().showEvent(event)
        # Resume any pending thumbnail downloads when the popup becomes visible.
        QTimer.singleShot(0, self._start_next_thumb_batch)

    # ------------------------------------------------------------------ #
    # Popup positioning

    def popup_above(self, anchor: QWidget) -> None:
        self.adjustSize()
        origin = anchor.mapToGlobal(QPoint(0, 0))
        x = max(12, origin.x() - 220)
        y = origin.y() - self.height() - 10
        self.move(x, y)
        self.show()

    # ------------------------------------------------------------------ #
    # Emoji tab

    def _build_emoji_tab(self) -> QWidget:
        tab = QWidget()
        layout = QGridLayout(tab)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(4)
        cols = 9
        for idx, emoji in enumerate(DEFAULT_EMOJIS):
            btn = QToolButton(tab)
            btn.setText(str(emoji))
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.clicked.connect(lambda _=False, e=str(emoji): self._select_emoji(e))
            layout.addWidget(btn, idx // cols, idx % cols)
        layout.setRowStretch((len(DEFAULT_EMOJIS) // cols) + 1, 1)
        return tab

    def _select_emoji(self, emoji: str) -> None:
        self.emojiSelected.emit(str(emoji))

    # ------------------------------------------------------------------ #
    # Stickers tab

    def _build_stickers_tab(self) -> QWidget:
        tab = QWidget()
        root = QVBoxLayout(tab)
        root.setContentsMargins(6, 6, 6, 6)
        root.setSpacing(8)

        top = QHBoxLayout()
        top.setContentsMargins(0, 0, 0, 0)
        top.setSpacing(6)
        top.addWidget(QLabel("Набор:"), 0)

        self.sets_combo = QComboBox()
        self.sets_combo.setMinimumWidth(260)
        self.sets_combo.currentIndexChanged.connect(self._on_sticker_set_selected)
        top.addWidget(self.sets_combo, 1)

        self.btn_refresh = QPushButton("↻")
        self.btn_refresh.setFixedWidth(34)
        self.btn_refresh.setToolTip("Обновить")
        self.btn_refresh.clicked.connect(self._lazy_refresh_stickers)
        top.addWidget(self.btn_refresh, 0)

        root.addLayout(top)

        self.sticker_status = QLabel("")
        self.sticker_status.setStyleSheet("color:#9fa6b1;font-size:11px;")
        root.addWidget(self.sticker_status)

        self.sticker_scroll = QScrollArea()
        self.sticker_scroll.setWidgetResizable(True)
        self.sticker_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.sticker_scroll.setFrameShape(QFrame.Shape.NoFrame)

        container = QWidget()
        self._sticker_grid = QGridLayout(container)
        self._sticker_grid.setContentsMargins(0, 0, 0, 0)
        self._sticker_grid.setSpacing(6)
        self.sticker_scroll.setWidget(container)
        root.addWidget(self.sticker_scroll, 1)

        self._populate_sets_placeholder()
        return tab

    def _populate_sets_placeholder(self) -> None:
        blocker = QSignalBlocker(self.sets_combo)
        self.sets_combo.clear()
        self.sets_combo.addItem("Недавние", {"kind": "recent"})
        del blocker

    def _lazy_refresh_stickers(self) -> None:
        # Avoid parallel refresh storms.
        if getattr(self, "_refresh_pending", False):
            return
        setattr(self, "_refresh_pending", True)
        QTimer.singleShot(0, self._refresh_stickers_async)

    def _refresh_stickers_async(self) -> None:
        setattr(self, "_refresh_pending", False)

        if not (self.tg and hasattr(self.tg, "is_authorized_sync") and self.tg.is_authorized_sync()):
            self.sticker_status.setText("Telegram недоступен или нет авторизации")
            self._populate_sets_placeholder()
            self._populate_sticker_grid([])
            return

        self.sticker_status.setText("Загружаю стикеры…")

        # Cancel previous workers if any.
        for thread_attr in ("_recent_thread", "_sets_thread"):
            thread = getattr(self, thread_attr, None)
            if thread is not None and thread.isRunning():
                try:
                    thread.quit()
                    thread.wait(200)
                except Exception:
                    pass

        # Recent stickers
        recent_thread = QThread(self)
        recent_worker = RecentStickersWorker(self.tg)
        recent_worker.moveToThread(recent_thread)
        recent_thread.started.connect(recent_worker.run)
        recent_worker.done.connect(self._on_recent_stickers_loaded)
        recent_worker.done.connect(recent_thread.quit)
        recent_worker.done.connect(recent_worker.deleteLater)
        recent_thread.finished.connect(recent_thread.deleteLater)
        self._recent_thread = recent_thread
        self._recent_worker = recent_worker
        recent_thread.start()

        # Sticker sets
        sets_thread = QThread(self)
        sets_worker = StickerSetsWorker(self.tg)
        sets_worker.moveToThread(sets_thread)
        sets_thread.started.connect(sets_worker.run)
        sets_worker.done.connect(self._on_sticker_sets_loaded)
        sets_worker.done.connect(sets_thread.quit)
        sets_worker.done.connect(sets_worker.deleteLater)
        sets_thread.finished.connect(sets_thread.deleteLater)
        self._sets_thread = sets_thread
        self._sets_worker = sets_worker
        sets_thread.start()

    def _on_recent_stickers_loaded(self, stickers: list) -> None:
        self._recent_worker = None
        try:
            self._recent_stickers = list(stickers or [])
        except Exception:
            self._recent_stickers = []

        current = self.sets_combo.currentData() or {}
        if isinstance(current, dict) and current.get("kind") == "recent":
            self._populate_sticker_grid(self._recent_stickers)

        self._update_sticker_status()

    def _on_sticker_sets_loaded(self, sets: list) -> None:
        self._sets_worker = None
        current_data = self.sets_combo.currentData()
        current_key = ""
        if isinstance(current_data, dict) and current_data.get("kind") == "set":
            current_key = str(current_data.get("id") or "")

        blocker = QSignalBlocker(self.sets_combo)
        self.sets_combo.clear()
        self.sets_combo.addItem("Недавние", {"kind": "recent"})
        for s in list(sets or []):
            if not isinstance(s, dict):
                continue
            title = str(s.get("title") or s.get("short_name") or "Sticker set").strip()
            self.sets_combo.addItem(title, {"kind": "set", **s})
        del blocker

        if current_key:
            for idx in range(self.sets_combo.count()):
                data = self.sets_combo.itemData(idx)
                if isinstance(data, dict) and str(data.get("id") or "") == current_key:
                    self.sets_combo.setCurrentIndex(idx)
                    break

        self._update_sticker_status()

    def _update_sticker_status(self) -> None:
        current = self.sets_combo.currentData() or {}
        if isinstance(current, dict) and current.get("kind") == "recent":
            count = len(self._recent_stickers)
            self.sticker_status.setText(f"Недавние: {count}")
        elif isinstance(current, dict) and current.get("kind") == "set":
            title = str(current.get("title") or current.get("short_name") or "Набор")
            self.sticker_status.setText(title)
        else:
            self.sticker_status.setText("")

    def _on_sticker_set_selected(self) -> None:
        data = self.sets_combo.currentData() or {}
        if not isinstance(data, dict):
            return
        kind = data.get("kind")
        if kind == "recent":
            self._populate_sticker_grid(self._recent_stickers)
            self._update_sticker_status()
            return
        if kind == "set":
            set_id = int(data.get("id") or 0)
            access_hash = int(data.get("access_hash") or 0)
            if set_id and access_hash:
                self._load_set_items_async(set_id, access_hash)
            return

    def _load_set_items_async(self, set_id: int, access_hash: int) -> None:
        self.sticker_status.setText("Загружаю набор…")

        thread = getattr(self, "_items_thread", None)
        if thread is not None and thread.isRunning():
            try:
                thread.quit()
                thread.wait(200)
            except Exception:
                pass
        self._items_worker = None

        thread = QThread(self)
        worker = StickerSetItemsWorker(self.tg, set_id=set_id, access_hash=access_hash)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.done.connect(self._on_set_items_loaded)
        worker.done.connect(thread.quit)
        worker.done.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        self._items_thread = thread
        self._items_worker = worker
        thread.start()

    def _on_set_items_loaded(self, stickers: list) -> None:
        self._items_worker = None
        try:
            self._populate_sticker_grid(list(stickers or []))
        except Exception:
            self._populate_sticker_grid([])
        self._update_sticker_status()

    def _clear_sticker_grid(self) -> None:
        self._cancel_thumb_worker()
        self._thumb_tasks = []
        self._sticker_buttons.clear()
        while self._sticker_grid.count():
            item = self._sticker_grid.takeAt(0)
            widget = item.widget() if item else None
            if widget:
                widget.deleteLater()

    def _populate_sticker_grid(self, stickers: List[Dict[str, Any]]) -> None:
        self._clear_sticker_grid()
        self._current_stickers = list(stickers or [])
        cols = 6

        for idx, item in enumerate(self._current_stickers):
            if not isinstance(item, dict):
                continue
            file_id = str(item.get("file_id") or "").strip()
            if not file_id:
                continue
            mime = str(item.get("mime") or "")
            emoji = str(item.get("emoji") or "")
            is_static = ("webp" in mime.lower()) or ("png" in mime.lower())

            btn = QToolButton(self)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.setFixedSize(58, 58)
            btn.setIconSize(btn.size())
            btn.setToolTip(emoji or "Стикер")

            if not is_static:
                btn.setText("🌀" if "tgsticker" in mime.lower() or "tgs" in mime.lower() else "🎞")
                btn.setStyleSheet("font-size:20px;color:#9fa6b1;")
            else:
                # Show a lightweight placeholder until the thumbnail arrives
                # (otherwise the grid may look empty on slow networks).
                btn.setText(emoji or "⬜")
                btn.setStyleSheet("font-size:18px;color:#9fa6b1;")

            btn.clicked.connect(lambda _=False, fid=file_id: self._select_sticker(fid))
            self._sticker_buttons[file_id] = btn
            self._sticker_grid.addWidget(btn, idx // cols, idx % cols)

        self._sticker_grid.setRowStretch((len(self._current_stickers) // cols) + 1, 1)
        self._start_thumb_downloads(self._current_stickers)

    def _select_sticker(self, file_id: str) -> None:
        self.stickerSelected.emit(str(file_id))

    def _thumb_cache_path(self, file_id: str) -> str:
        root = Path("media") / "stickers_cache"
        root.mkdir(parents=True, exist_ok=True)
        digest = hashlib.blake2b(file_id.encode("utf-8", "ignore"), digest_size=16).hexdigest()
        return str(root / f"{digest}.webp")

    def _start_thumb_downloads(self, stickers: List[Dict[str, Any]]) -> None:
        if not (self.tg and hasattr(self.tg, "download_file_id_sync")):
            return
        tasks: List[tuple[str, str]] = []
        for item in stickers:
            if not isinstance(item, dict):
                continue
            fid = str(item.get("file_id") or "").strip()
            mime = str(item.get("mime") or "")
            if not fid:
                continue
            if not (("webp" in mime.lower()) or ("png" in mime.lower())):
                continue
            out_path = self._thumb_cache_path(fid)
            tasks.append((fid, out_path))
        if not tasks:
            return
        self._thumb_tasks = list(tasks)
        self._start_next_thumb_batch()

    def _start_next_thumb_batch(self) -> None:
        if not self._thumb_tasks:
            return
        if not self.isVisible():
            return
        thread = getattr(self, "_thumb_thread", None)
        if thread is not None and thread.isRunning():
            return
        batch = self._thumb_tasks[:24]
        self._thumb_tasks = self._thumb_tasks[24:]

        thread = QThread(self)
        worker = StickerThumbWorker(self.tg, batch)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.thumb_ready.connect(self._on_thumb_ready)
        worker.finished.connect(self._on_thumb_batch_finished)
        worker.finished.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        self._thumb_thread = thread
        self._thumb_worker = worker
        thread.start()

    def _on_thumb_batch_finished(self) -> None:
        self._thumb_worker = None
        if not self._thumb_tasks:
            return
        QTimer.singleShot(0, self._start_next_thumb_batch)

    def _cancel_thumb_worker(self) -> None:
        worker = getattr(self, "_thumb_worker", None)
        thread = getattr(self, "_thumb_thread", None)
        self._thumb_worker = None
        self._thumb_thread = None
        if worker is not None:
            try:
                worker.stop()
            except Exception:
                pass
        if thread is not None and thread.isRunning():
            try:
                thread.quit()
                thread.wait(200)
            except Exception:
                pass

    def _on_thumb_ready(self, file_id: str, path: str) -> None:
        btn = self._sticker_buttons.get(str(file_id))
        if not btn:
            return
        if not path or not os.path.isfile(path):
            return
        pix = QPixmap(path)
        if pix.isNull():
            return
        scaled = pix.scaled(btn.iconSize(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
        btn.setIcon(QIcon(scaled))
        btn.setText("")

    # ------------------------------------------------------------------ #
    # GIF tab

    def _build_gif_tab(self) -> QWidget:
        tab = QWidget()
        root = QVBoxLayout(tab)
        root.setContentsMargins(6, 6, 6, 6)
        root.setSpacing(10)
        hint = QLabel("GIF пока отправляется как файл с диска.")
        hint.setStyleSheet("color:#9fa6b1;font-size:11px;")
        root.addWidget(hint)
        btn = QPushButton("Выбрать GIF…")
        btn.clicked.connect(lambda: self.gifPickRequested.emit())
        root.addWidget(btn, 0, Qt.AlignmentFlag.AlignLeft)
        root.addStretch(1)
        return tab
