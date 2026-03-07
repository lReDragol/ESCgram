from __future__ import annotations

import hashlib
import time
from typing import Any, Dict, Optional, List

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QScrollArea,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from ui.avatar_cache import AvatarCache
from ui.components.avatar import AvatarWidget
from ui.message_widgets import Bubble, ChatItemWidget, TextMessageWidget
from ui.styles import StyleManager

DEFAULT_USERNAME_COLORS = [
    "#ff8b8b",
    "#ffb86c",
    "#ffd479",
    "#7be094",
    "#5ec7ff",
    "#80a0ff",
    "#ff7ad8",
    "#7dd7ff",
    "#9de27f",
    "#f7a8ff",
]


class MessageFeedMixin:
    """Mixin that manages the message feed and avatar rendering."""

    chat_scroll: QScrollArea
    chat_history_layout: QVBoxLayout
    chat_history_wrap: QWidget
    user_input: QTextEdit
    _message_widgets: Dict[int, ChatItemWidget]
    avatar_cache: AvatarCache
    _avatar_size: int

    def _build_feed(self) -> QWidget:
        layout = QVBoxLayout()

        self._msg_search_last_index: Optional[int] = None
        self._msg_search_match_indexes: List[int] = []
        self._msg_search_bar = QWidget()
        self._msg_search_bar.setVisible(False)
        search_row = QHBoxLayout(self._msg_search_bar)
        search_row.setContentsMargins(8, 6, 8, 6)
        search_row.setSpacing(6)

        self._msg_search_edit = QLineEdit()
        self._msg_search_edit.setPlaceholderText("Поиск по сообщениям…")
        self._msg_search_edit.textChanged.connect(self._on_message_search_text_changed)
        self._msg_search_edit.returnPressed.connect(lambda: self.find_in_messages(forward=True))
        search_row.addWidget(self._msg_search_edit, 1)

        self._msg_search_status = QLabel("")
        self._msg_search_status.setMinimumWidth(72)
        search_row.addWidget(self._msg_search_status, 0, Qt.AlignmentFlag.AlignRight)

        btn_prev = QToolButton()
        btn_prev.setText("↑")
        btn_prev.setToolTip("Предыдущее совпадение (Shift+F3)")
        btn_prev.clicked.connect(lambda: self.find_in_messages(forward=False))
        search_row.addWidget(btn_prev)

        btn_next = QToolButton()
        btn_next.setText("↓")
        btn_next.setToolTip("Следующее совпадение (F3)")
        btn_next.clicked.connect(lambda: self.find_in_messages(forward=True))
        search_row.addWidget(btn_next)

        btn_close = QToolButton()
        btn_close.setText("✕")
        btn_close.setToolTip("Закрыть поиск (Esc)")
        btn_close.clicked.connect(self.hide_message_search)
        search_row.addWidget(btn_close)

        layout.addWidget(self._msg_search_bar, 0)

        self.chat_scroll = QScrollArea()
        self.chat_scroll.setWidgetResizable(True)
        StyleManager.instance().bind_stylesheet(self.chat_scroll, "message_feed.scroll_area")

        self.chat_history_wrap = QWidget()
        self.chat_history_layout = QVBoxLayout(self.chat_history_wrap)
        self.chat_history_layout.setContentsMargins(8, 8, 8, 8)
        self.chat_history_layout.setSpacing(10)
        self.chat_history_layout.addStretch(1)
        self.chat_scroll.setWidget(self.chat_history_wrap)
        layout.addWidget(self.chat_scroll, 1)

        vbar = self.chat_scroll.verticalScrollBar()
        vbar.rangeChanged.connect(self._on_scroll_range_changed)
        vbar.valueChanged.connect(self._on_scroll_value_changed)

        self._pending_feed_unread = 0
        viewport = self.chat_scroll.viewport()
        self._jump_to_latest_btn = QToolButton(viewport)
        self._jump_to_latest_btn.setText("▼")
        self._jump_to_latest_btn.setToolTip("К последним сообщениям")
        self._jump_to_latest_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._jump_to_latest_btn.setFixedSize(42, 42)
        self._jump_to_latest_btn.setStyleSheet(
            "QToolButton{background-color:rgba(43,82,120,0.96);color:#dfefff;border:none;border-radius:21px;font-size:18px;font-weight:700;}"
            "QToolButton:hover{background-color:rgba(66,123,180,0.98);}"
        )
        self._jump_to_latest_btn.clicked.connect(self._jump_to_latest_clicked)
        self._jump_to_latest_btn.hide()

        self._jump_unread_badge = QLabel("", viewport)
        self._jump_unread_badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._jump_unread_badge.setStyleSheet(
            "background-color:#4aa6ea;color:white;border-radius:10px;padding:1px 6px;font-size:11px;font-weight:700;"
        )
        self._jump_unread_badge.hide()
        QTimer.singleShot(0, self._position_jump_button)

        return_widget = QWidget()
        return_widget.setLayout(layout)
        return return_widget

    def _bind_message_context_menu(self, widget: QWidget) -> None:
        if not widget:
            return
        if bool(getattr(self, "_use_global_context_menu", False)):
            return

        def _attach(source: QWidget) -> None:
            try:
                source.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
                source.customContextMenuRequested.connect(
                    lambda pos, w=widget, src=source: self._on_message_context_menu_requested(w, pos, src)
                )
            except Exception:
                return

        _attach(widget)
        # Propagate to children so right-click on inner labels/media also works.
        try:
            for child in widget.findChildren(QWidget):
                _attach(child)
        except Exception:
            pass

    def _on_message_context_menu_requested(self, widget: QWidget, pos, source: Optional[QWidget] = None) -> None:
        handler = getattr(self, "_show_message_context_menu", None)
        if not callable(handler):
            handler = getattr(self, "show_message_context_menu", None)
        if not callable(handler):
            return
        try:
            origin = source or widget
            global_pos = origin.mapToGlobal(pos)
        except Exception:
            global_pos = None
        if global_pos is None:
            return
        try:
            handler(widget, global_pos)
        except Exception:
            pass

    def show_message_search(self) -> None:
        bar = getattr(self, "_msg_search_bar", None)
        edit = getattr(self, "_msg_search_edit", None)
        if not (bar and edit):
            return
        bar.setVisible(True)
        edit.setFocus()
        try:
            edit.selectAll()
        except Exception:
            pass

    def hide_message_search(self) -> None:
        bar = getattr(self, "_msg_search_bar", None)
        if bar:
            bar.setVisible(False)
        self._reset_message_search()

    def _reset_message_search(self) -> None:
        self._msg_search_last_index = None
        self._msg_search_match_indexes = []
        lbl = getattr(self, "_msg_search_status", None)
        if lbl:
            lbl.setText("")
        for widget in list(getattr(self, "_message_order", []) or []):
            try:
                setter = getattr(widget, "set_search_query", None)
                if callable(setter):
                    setter("", active=False)
            except Exception:
                continue

    def _on_message_search_text_changed(self) -> None:
        self._reset_message_search()
        edit = getattr(self, "_msg_search_edit", None)
        if edit and str(edit.text() or "").strip():
            self.find_in_messages(forward=True)

    @staticmethod
    def _widget_search_text(widget: QWidget) -> str:
        for attr in ("_original_text", "text"):
            try:
                value = getattr(widget, attr, None)
            except Exception:
                value = None
            if isinstance(value, str) and value:
                return value
        return ""

    def find_in_messages(self, *, forward: bool = True) -> None:
        edit = getattr(self, "_msg_search_edit", None)
        lbl = getattr(self, "_msg_search_status", None)
        if not edit:
            return
        query = str(edit.text() or "").strip().lower()
        if not query:
            if lbl:
                lbl.setText("")
            return

        order = list(getattr(self, "_message_order", []))
        if not order:
            if lbl:
                lbl.setText("0")
            return

        matches: List[int] = []
        for idx, widget in enumerate(order):
            text = self._widget_search_text(widget)
            if query in text.lower():
                matches.append(idx)
        self._msg_search_match_indexes = matches
        total_matches = len(matches)
        if not matches:
            if lbl:
                lbl.setText("0 / 0")
            for widget in order:
                try:
                    setter = getattr(widget, "set_search_query", None)
                    if callable(setter):
                        setter("", active=False)
                except Exception:
                    pass
            return

        start = self._msg_search_last_index
        if start is None:
            idx = 0 if forward else (len(order) - 1)
        else:
            idx = (start + (1 if forward else -1)) % len(order)

        checked = 0
        found_idx: Optional[int] = None
        while checked < len(order):
            widget = order[idx]
            text = self._widget_search_text(widget)
            if query in text.lower():
                found_idx = idx
                break
            idx = (idx + (1 if forward else -1)) % len(order)
            checked += 1

        if found_idx is None:
            if lbl:
                lbl.setText("0")
            return

        self._msg_search_last_index = found_idx
        current_match_no = 1
        try:
            current_match_no = matches.index(found_idx) + 1
        except Exception:
            current_match_no = 1
        if lbl:
            lbl.setText(f"{current_match_no} / {total_matches}")
        for idx_widget, widget in enumerate(order):
            try:
                setter = getattr(widget, "set_search_query", None)
                if callable(setter):
                    setter(query if idx_widget in matches else "", active=(idx_widget == found_idx))
            except Exception:
                pass
        target = order[found_idx]
        wrap = getattr(target, "_row_wrap", None) or target
        try:
            self.chat_scroll.ensureWidgetVisible(wrap, 0, 48)
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Message helpers

    @staticmethod
    def _dispose_widget_tree(widget: Optional[QWidget]) -> None:
        """Dispose message widgets nested inside row wrappers before Qt destroys them."""
        if widget is None:
            return
        targets: List[QWidget] = [widget]
        try:
            targets.extend(widget.findChildren(QWidget))
        except Exception:
            pass
        seen: set[int] = set()
        for node in targets:
            if node is None:
                continue
            marker = id(node)
            if marker in seen:
                continue
            seen.add(marker)
            disposer = getattr(node, "safe_dispose", None)
            if not callable(disposer):
                continue
            try:
                disposer()
            except Exception:
                pass

    def clear_feed(self) -> None:
        old_wrap = getattr(self, "chat_history_wrap", None)
        old_layout = getattr(self, "chat_history_layout", None)
        try:
            self.chat_scroll.setUpdatesEnabled(False)
        except Exception:
            pass
        # Fast swap: build a fresh container instead of deleting hundreds of rows one-by-one.
        new_wrap = QWidget()
        new_layout = QVBoxLayout(new_wrap)
        if old_layout is not None:
            try:
                new_layout.setContentsMargins(old_layout.contentsMargins())
                new_layout.setSpacing(old_layout.spacing())
            except Exception:
                new_layout.setContentsMargins(8, 8, 8, 8)
                new_layout.setSpacing(10)
        else:
            new_layout.setContentsMargins(8, 8, 8, 8)
            new_layout.setSpacing(10)
        new_layout.addStretch(1)
        self.chat_history_wrap = new_wrap
        self.chat_history_layout = new_layout
        try:
            self.chat_scroll.setWidget(new_wrap)
        except Exception:
            pass
        try:
            if getattr(self, "_jump_to_latest_btn", None):
                self._jump_to_latest_btn.raise_()
            if getattr(self, "_jump_unread_badge", None):
                self._jump_unread_badge.raise_()
        except Exception:
            pass
        self._dispose_widget_tree(old_wrap)
        if old_wrap is not None:
            try:
                old_wrap.deleteLater()
            except Exception:
                pass
        try:
            self.chat_scroll.setUpdatesEnabled(True)
        except Exception:
            pass
        self._message_widgets.clear()
        self._message_order = []
        self._clear_jump_indicator()
        QTimer.singleShot(0, self._position_jump_button)

    def _insert_message_widget(
        self,
        content: QWidget,
        *,
        role: str,
        chat_id: Optional[str],
        user_id: Optional[str],
        header: str,
        position: Optional[int] = None,
    ) -> None:
        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(4)
        row.setAlignment(Qt.AlignmentFlag.AlignBottom)
        wrap = QWidget()
        wrap.setLayout(row)

        if not hasattr(self, "avatar_cache"):
            on_ready = getattr(self, "_on_avatar_ready", None)
            self.avatar_cache = AvatarCache(self.server, size=self._avatar_size, on_ready=on_ready)

        avatar_kind = ""
        avatar_id = ""
        chat_ref = chat_id or self.current_chat_id
        hide_avatar = self._is_private_dialog(chat_ref)
        hide_my_avatar = role == "me" and not bool(getattr(self, "_show_my_avatar_enabled", True))

        avatar_widget: Optional[AvatarWidget] = None
        if not hide_avatar and not hide_my_avatar:
            if role == "assistant":
                avatar = self.avatar_cache.assistant()
            elif role == "me":
                avatar_kind = "user"
                avatar_id = str(user_id or getattr(self, "_my_id", "me"))
                avatar = self.avatar_cache.user(avatar_id, header or "Вы")
            elif user_id:
                normalized_sender = str(user_id)
                # sender_id can be a chat/channel id (negative) when messages are sent
                # on behalf of a channel (sender_chat) or by anonymous admins.
                if normalized_sender.startswith("-") and normalized_sender.lstrip("-").isdigit():
                    avatar_kind = "chat"
                    avatar_id = normalized_sender
                    info = self.all_chats.get(avatar_id, {"title": header})
                    avatar = self.avatar_cache.chat(avatar_id, info)
                else:
                    avatar_kind = "user"
                    avatar_id = normalized_sender
                    avatar = self.avatar_cache.user(avatar_id, header)
            elif chat_id:
                avatar_kind = "chat"
                avatar_id = str(chat_id)
                info = self.all_chats.get(avatar_id, {"title": header})
                avatar = self.avatar_cache.chat(avatar_id, info)
            else:
                avatar = self.avatar_cache.assistant()

            avatar_widget = AvatarWidget(size=self._avatar_size)
            avatar_widget.set_pixmap(avatar)
            avatar_widget.setToolTip(header or "")
            avatar_widget.setProperty("avatar_kind", avatar_kind)
            avatar_widget.setProperty("avatar_id", avatar_id)
            size_setter = getattr(avatar_widget, "set_avatar_size", None)
            if callable(size_setter):
                size_setter(self._avatar_size_for(chat_ref, role))

        avatar_holder: Optional[QWidget] = None
        if avatar_widget is not None:
            avatar_holder = QWidget()
            holder = QVBoxLayout(avatar_holder)
            holder.setContentsMargins(0, 0, 0, 0)
            holder.setSpacing(0)
            holder.addStretch(1)
            holder.addWidget(avatar_widget, 0, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignBottom)

        gap = 4
        if role == "me":
            row.addStretch(1)
            row.addWidget(content, 0)
            if avatar_holder is not None:
                row.addSpacing(gap)
                row.addWidget(avatar_holder, 0)
        else:
            if avatar_holder is not None:
                row.addWidget(avatar_holder, 0)
                row.addSpacing(gap)
            row.addWidget(content, 0)
            row.addStretch(1)

        # Make the message context menu reliably available even if user clicks on
        # avatar / padding / wrapper area around the actual message widget.
        try:
            mid = getattr(content, "_message_id", None)
            if mid is None:
                mid = getattr(content, "msg_id", None)
            if mid is not None:
                setattr(wrap, "_message_id", int(mid))
        except Exception:
            pass
        self._bind_message_context_menu(wrap)

        if not hasattr(self, "_message_order"):
            self._message_order = []
        order_len = len(self._message_order)
        insert_at = order_len if position is None else max(0, min(int(position), order_len))
        self._message_order.insert(insert_at, content)
        self.chat_history_layout.insertWidget(insert_at, wrap)
        setattr(content, "_row_wrap", wrap)

    def _is_private_dialog(self, chat_id: Optional[str]) -> bool:
        return self._dialog_type(chat_id) == "private"

    def _dialog_type(self, chat_id: Optional[str]) -> str:
        cid = str(chat_id or self.current_chat_id or "")
        if not cid:
            return ""
        info = {}
        try:
            info = self.all_chats.get(cid, {})
        except Exception:
            info = {}
        return str(info.get("type", "")).lower()

    def _avatar_size_for(self, chat_id: Optional[str], role: str) -> int:
        dialog_type = self._dialog_type(chat_id)
        if dialog_type in {"group", "supergroup", "megagroup", "channel"}:
            return 32
        return self._avatar_size

    def _display_color_for(self, user_id: Optional[str], header: str, role: str) -> Optional[str]:
        if role in {"me", "assistant"}:
            return None
        key = (user_id or header or "").strip()
        if not key:
            return None
        palette = StyleManager.instance().value("message_feed.username_colors", DEFAULT_USERNAME_COLORS)
        if not palette:
            palette = DEFAULT_USERNAME_COLORS
        digest = hashlib.blake2b(key.encode("utf-8", "ignore"), digest_size=1).digest()[0]
        return palette[digest % len(palette)]

    def add_text_item(
        self,
        header: str,
        text: str,
        role: str = "other",
        *,
        chat_id: Optional[str] = None,
        user_id: Optional[str] = None,
        entities: Optional[List[Dict[str, Any]]] = None,
        has_hidden: bool = False,
        msg_id: Optional[int] = None,
        reply_to: Optional[int] = None,
        reply_preview: Optional[Dict[str, Any]] = None,
        is_deleted: bool = False,
        forward_info: Optional[Dict[str, Any]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        reactions: Optional[List[Dict[str, Any]]] = None,
        timestamp: Optional[int] = None,
        insert_at: Optional[int] = None,
    ) -> TextMessageWidget:
        color = self._display_color_for(user_id, header, role)
        widget = TextMessageWidget(
            header,
            text,
            role=role,
            entities=entities,
            chat_id=chat_id or self.current_chat_id,
            msg_id=msg_id,
        )
        self._bind_message_context_menu(widget)
        if color:
            widget.set_header_color(color)
        if forward_info:
            widget.set_forward_info(forward_info)
        if reply_preview:
            widget.set_reply_preview(reply_preview)
        if reply_markup:
            widget.set_reply_markup(reply_markup)
        if reactions:
            setter = getattr(widget, "set_reactions", None)
            if callable(setter):
                widget.set_reactions(reactions)
        if has_hidden and hasattr(widget, "set_has_hidden"):
            try:
                widget.set_has_hidden(True)
            except Exception:
                pass
        if is_deleted:
            widget.set_deleted(True)

        ts = int(timestamp) if timestamp is not None else int(time.time())
        setattr(widget, "_message_timestamp", ts)
        setattr(widget, "_message_role", role)
        if msg_id is not None:
            setattr(widget, "_message_id", int(msg_id))

        self._insert_message_widget(
            widget,
            role=role,
            chat_id=chat_id or self.current_chat_id,
            user_id=user_id,
            header=header,
            position=insert_at,
        )
        if msg_id is not None:
            try:
                self._message_widgets[int(msg_id)] = widget
            except Exception:
                pass
        host = getattr(self, "_on_message_widget_created", None)
        if callable(host):
            try:
                host(widget, msg_id, chat_id or self.current_chat_id)
            except Exception:
                pass

        should_scroll = True
        hook = getattr(self, "_after_media_widget_added", None)
        if callable(hook):
            try:
                result = hook(widget)
                if isinstance(result, bool):
                    should_scroll = bool(result)
            except Exception:
                pass
        if should_scroll:
            self._scroll_to_bottom()
            self._clear_jump_indicator()
        elif role != "me" and not bool(getattr(self, "_loading_history", False)):
            self._notify_new_message_while_scrolled()
        return widget

    def add_media_item(
        self,
        *,
        kind: str,
        header: str,
        text: str = "",
        text_entities: Optional[List[Dict[str, Any]]] = None,
        role: str = "other",
        file_path: Optional[str] = None,
        msg_id: Optional[int] = None,
        chat_id: Optional[str] = None,
        user_id: Optional[str] = None,
        thumb_path: Optional[str] = None,
        file_size: Optional[int] = None,
        has_hidden: bool = False,
        reply_to: Optional[int] = None,
        reply_preview: Optional[Dict[str, Any]] = None,
        is_deleted: bool = False,
        voice_waveform: bool = True,
        forward_info: Optional[Dict[str, Any]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        reactions: Optional[List[Dict[str, Any]]] = None,
        duration_ms: Optional[int] = None,
        waveform: Optional[List[int]] = None,
        media_group_id: Optional[str] = None,
        timestamp: Optional[int] = None,
        insert_at: Optional[int] = None,
    ) -> ChatItemWidget:
        color = self._display_color_for(user_id, header, role)
        widget = ChatItemWidget(
            kind,
            header,
            text=text,
            text_entities=text_entities,
            role=role,
            file_path=file_path,
            chat_id=chat_id or self.current_chat_id,
            msg_id=msg_id,
            server=self.server,
            thumb_path=thumb_path,
            file_size=file_size,
            voice_waveform=voice_waveform,
            duration_ms=duration_ms,
            waveform=waveform,
        )
        self._bind_message_context_menu(widget)
        if forward_info:
            widget.set_forward_info(forward_info)
        if reply_preview:
            widget.set_reply_preview(reply_preview)
        if reply_markup:
            widget.set_reply_markup(reply_markup)
        if reactions:
            setter = getattr(widget, "set_reactions", None)
            if callable(setter):
                widget.set_reactions(reactions)
        if is_deleted:
            widget.set_deleted(True)
        if has_hidden and hasattr(widget, "set_has_hidden"):
            try:
                widget.set_has_hidden(True)
            except Exception:
                pass
        if color:
            widget.set_header_color(color)
        try:
            setattr(widget, "_media_group_id", str(media_group_id or "").strip())
        except Exception:
            pass
        ts = int(timestamp) if timestamp is not None else int(time.time())
        setattr(widget, "_message_timestamp", ts)
        setattr(widget, "_message_role", role)
        if msg_id is not None:
            setattr(widget, "_message_id", int(msg_id))
        self._insert_message_widget(
            widget,
            role=role,
            chat_id=chat_id or self.current_chat_id,
            user_id=user_id,
            header=header,
            position=insert_at,
        )
        if msg_id is not None:
            try:
                self._message_widgets[int(msg_id)] = widget
            except Exception:
                pass
        host = getattr(self, "_on_message_widget_created", None)
        if callable(host):
            try:
                host(widget, msg_id, chat_id or self.current_chat_id)
            except Exception:
                pass
        should_scroll = True
        hook = getattr(self, "_after_media_widget_added", None)
        if callable(hook):
            try:
                result = hook(widget)
                if isinstance(result, bool):
                    should_scroll = bool(result)
            except Exception:
                pass
        if should_scroll:
            self._scroll_to_bottom()
            self._clear_jump_indicator()
        elif role != "me" and not bool(getattr(self, "_loading_history", False)):
            self._notify_new_message_while_scrolled()
        return widget

    # ------------------------------------------------------------------ #
    # Event helpers

    def _scroll_to_bottom(self) -> None:
        def _go() -> None:
            bar = self.chat_scroll.verticalScrollBar()
            setattr(self, "_feed_scroll_lock_mode", "")
            setattr(self, "_feed_autostick_block_until", 0.0)
            bar.setValue(bar.maximum())
            self._clear_jump_indicator()
            self._position_jump_button()

        QTimer.singleShot(0, _go)

    def _on_scroll_range_changed(self, _min: int, _max: int) -> None:
        """Stick to bottom only when appropriate (avoid heavy jumping during history load)."""
        if getattr(self, "_loading_history", False):
            return
        try:
            bar = self.chat_scroll.verticalScrollBar()
            lock_mode = str(getattr(self, "_feed_scroll_lock_mode", "") or "").strip().lower()
            lock_until = float(getattr(self, "_feed_autostick_block_until", 0.0) or 0.0)
            if lock_mode == "top" and time.monotonic() < lock_until:
                bar.setValue(bar.minimum())
                self._update_jump_button_visibility()
                self._position_jump_button()
                return
            if time.monotonic() < lock_until:
                self._update_jump_button_visibility()
                self._position_jump_button()
                return
            # Only auto-stick if the user is already near the bottom.
            threshold = int(getattr(self, "_scroll_stick_threshold", 96) or 96)
            if (bar.maximum() - bar.value()) <= threshold:
                bar.setValue(_max)
                self._clear_jump_indicator()
            self._update_jump_button_visibility()
            self._position_jump_button()
        except Exception:
            pass

    def _on_scroll_value_changed(self, _value: int) -> None:
        if self._is_feed_near_bottom():
            self._clear_jump_indicator()
        self._update_jump_button_visibility()
        self._position_jump_button()
        hook = getattr(self, "_on_feed_scroll_changed", None)
        if callable(hook):
            try:
                hook(int(_value))
            except Exception:
                pass

    def _is_feed_near_bottom(self, threshold: int = 96) -> bool:
        checker = getattr(self, "_is_user_near_bottom", None)
        if callable(checker):
            try:
                return bool(checker(threshold))
            except Exception:
                pass
        try:
            bar = self.chat_scroll.verticalScrollBar()
            return (bar.maximum() - bar.value()) <= int(threshold)
        except Exception:
            return True

    def _notify_new_message_while_scrolled(self) -> None:
        if self._is_feed_near_bottom():
            self._clear_jump_indicator()
            return
        self._pending_feed_unread = int(getattr(self, "_pending_feed_unread", 0) or 0) + 1
        self._update_jump_button_visibility()
        self._position_jump_button()

    def _clear_jump_indicator(self) -> None:
        self._pending_feed_unread = 0
        self._update_jump_button_visibility()

    def _jump_to_latest_clicked(self) -> None:
        setattr(self, "_feed_scroll_lock_mode", "")
        setattr(self, "_feed_autostick_block_until", 0.0)
        self._scroll_to_bottom()

    def _update_jump_button_visibility(self) -> None:
        btn = getattr(self, "_jump_to_latest_btn", None)
        badge = getattr(self, "_jump_unread_badge", None)
        if not btn:
            return
        unread = max(0, int(getattr(self, "_pending_feed_unread", 0) or 0))
        show_btn = not self._is_feed_near_bottom()
        btn.setVisible(show_btn)
        if badge:
            if show_btn and unread > 0:
                badge.setText(str(unread if unread < 1000 else "999+"))
                badge.adjustSize()
                badge.show()
            else:
                badge.hide()

    def _position_jump_button(self) -> None:
        btn = getattr(self, "_jump_to_latest_btn", None)
        badge = getattr(self, "_jump_unread_badge", None)
        viewport = getattr(self.chat_scroll, "viewport", lambda: None)()
        if not btn or viewport is None:
            return
        margin_right = 14
        margin_bottom = 14
        x = max(0, viewport.width() - btn.width() - margin_right)
        y = max(0, viewport.height() - btn.height() - margin_bottom)
        btn.move(x, y)
        try:
            btn.raise_()
        except Exception:
            pass
        if badge:
            bx = x - max(2, badge.width() // 4)
            by = max(0, y - badge.height() // 3)
            badge.move(bx, by)
            try:
                badge.raise_()
            except Exception:
                pass
