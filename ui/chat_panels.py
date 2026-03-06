from __future__ import annotations

from datetime import datetime
from functools import partial
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QPoint, Qt, Signal, QUrl
from PySide6.QtGui import QPixmap, QDesktopServices
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMenu,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from ui.components.avatar import AvatarWidget


def format_chat_subtitle(info: Dict[str, Any]) -> str:
    if not isinstance(info, dict):
        return ""
    parts: List[str] = []
    ctype = str(info.get("type") or "").strip().lower()
    type_map = {
        "private": "Личные сообщения",
        "bot": "Бот",
        "user": "Пользователь",
        "group": "Группа",
        "supergroup": "Супергруппа",
        "channel": "Канал",
    }
    if ctype:
        parts.append(type_map.get(ctype, ctype.title()))
    members_count = info.get("members_count")
    try:
        members = int(members_count) if members_count is not None else 0
    except Exception:
        members = 0
    if members > 0:
        parts.append(f"{members:,}".replace(",", " ") + " участников")
    username = str(info.get("username") or "").strip()
    if username:
        parts.append(f"@{username}")
    return " • ".join([part for part in parts if part])


class _ClickableFrame(QFrame):
    clicked = Signal()

    def mouseReleaseEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mouseReleaseEvent(event)


class ChatHeaderBar(QFrame):
    infoRequested = Signal()
    menuRequested = Signal(QPoint)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setObjectName("chatHeaderBar")
        self.setStyleSheet(
            "QFrame#chatHeaderBar{background-color:#102033;border-bottom:1px solid rgba(255,255,255,0.06);}"
            "QLabel#chatHeaderTitle{color:#f4f7ff;font-size:17px;font-weight:700;}"
            "QLabel#chatHeaderSubtitle{color:#8da8c4;font-size:12px;}"
            "QPushButton{background-color:rgba(255,255,255,0.04);color:#dfe7f5;border:none;border-radius:14px;padding:6px 10px;}"
            "QPushButton:hover{background-color:rgba(255,255,255,0.09);}"
        )
        layout = QHBoxLayout(self)
        layout.setContentsMargins(14, 10, 14, 10)
        layout.setSpacing(10)

        self._click_area = _ClickableFrame(self)
        self._click_area.setFrameShape(QFrame.Shape.NoFrame)
        self._click_area.setCursor(Qt.CursorShape.PointingHandCursor)
        click_layout = QHBoxLayout(self._click_area)
        click_layout.setContentsMargins(0, 0, 0, 0)
        click_layout.setSpacing(10)

        self.avatar = AvatarWidget(size=42, parent=self._click_area)
        click_layout.addWidget(self.avatar, 0, Qt.AlignmentFlag.AlignVCenter)

        text_col = QVBoxLayout()
        text_col.setContentsMargins(0, 0, 0, 0)
        text_col.setSpacing(2)
        self.title_label = QLabel("Выберите чат")
        self.title_label.setObjectName("chatHeaderTitle")
        self.subtitle_label = QLabel("")
        self.subtitle_label.setObjectName("chatHeaderSubtitle")
        self.subtitle_label.hide()
        text_col.addWidget(self.title_label)
        text_col.addWidget(self.subtitle_label)
        click_layout.addLayout(text_col, 1)
        layout.addWidget(self._click_area, 1)

        self.btn_more = QPushButton("⋯", self)
        self.btn_more.setFixedWidth(42)
        layout.addWidget(self.btn_more, 0)

        self._click_area.clicked.connect(self.infoRequested.emit)
        self.btn_more.clicked.connect(self._emit_menu_requested)

    def set_chat(self, *, title: str, subtitle: str = "", avatar: Optional[QPixmap] = None) -> None:
        self.title_label.setText(str(title or "Чат"))
        subtitle_text = str(subtitle or "").strip()
        self.subtitle_label.setText(subtitle_text)
        self.subtitle_label.setVisible(bool(subtitle_text))
        if avatar is not None and not avatar.isNull():
            self.avatar.set_pixmap(avatar)

    def _emit_menu_requested(self) -> None:
        try:
            point = self.btn_more.mapToGlobal(self.btn_more.rect().bottomRight())
        except Exception:
            point = QPoint()
        self.menuRequested.emit(point)


class _StatsSection(QWidget):
    def __init__(self, title: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        header = QLabel(title)
        header.setStyleSheet("color:#f4f7ff;font-size:15px;font-weight:700;")
        layout.addWidget(header)
        self.body = QVBoxLayout()
        self.body.setContentsMargins(0, 0, 0, 0)
        self.body.setSpacing(6)
        layout.addLayout(self.body)


class ChatInfoDialog(QDialog):
    def __init__(
        self,
        info: Dict[str, Any],
        *,
        avatar: Optional[QPixmap] = None,
        sections: Optional[Dict[str, Any]] = None,
        callbacks: Optional[Dict[str, Any]] = None,
        embedded: bool = False,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self._info = dict(info or {})
        self._sections = dict(sections or {})
        self._callbacks = dict(callbacks or {})
        self._embedded = bool(embedded)

        self.setWindowTitle("Профиль чата")
        self.resize(680, 680)
        self.setStyleSheet(
            "QDialog{background-color:#0f1b27;color:#dfe7f5;}"
            "QLabel{color:#dfe7f5;}"
            "QTabWidget::pane{border:1px solid rgba(255,255,255,0.08);border-radius:10px;top:-1px;background:#102033;}"
            "QTabBar::tab{background:rgba(255,255,255,0.04);color:#b9cce3;padding:8px 12px;margin-right:4px;border-top-left-radius:8px;border-top-right-radius:8px;}"
            "QTabBar::tab:selected{background:rgba(89,183,255,0.22);color:#f4f7ff;}"
            "QPushButton{background:rgba(255,255,255,0.05);color:#dfe7f5;border:none;border-radius:8px;padding:7px 10px;}"
            "QPushButton:hover{background:rgba(255,255,255,0.10);}"
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        header = QHBoxLayout()
        header.setSpacing(12)
        avatar_widget = AvatarWidget(size=64, parent=self)
        if avatar is not None and not avatar.isNull():
            avatar_widget.set_pixmap(avatar)
        header.addWidget(avatar_widget, 0, Qt.AlignmentFlag.AlignTop)

        text_col = QVBoxLayout()
        text_col.setSpacing(4)
        title_label = QLabel(str(self._info.get("title") or self._info.get("id") or "Чат"))
        title_label.setStyleSheet("font-size:20px;font-weight:700;color:#f4f7ff;")
        text_col.addWidget(title_label)
        subtitle = format_chat_subtitle(self._info)
        if subtitle:
            subtitle_label = QLabel(subtitle)
            subtitle_label.setStyleSheet("color:#8da8c4;font-size:12px;")
            subtitle_label.setWordWrap(True)
            text_col.addWidget(subtitle_label)
        header.addLayout(text_col, 1)
        root.addLayout(header)

        tabs = QTabWidget(self)
        tabs.addTab(self._wrap_scroll(self._build_overview_tab()), "Обзор")
        tabs.addTab(self._build_media_tab(), "Медиа")
        tabs.addTab(self._build_files_tab(), "Файлы")
        tabs.addTab(self._build_links_tab(), "Ссылки")
        tabs.addTab(self._wrap_scroll(self._build_members_tab()), "Участники")
        tabs.addTab(self._wrap_scroll(self._build_actions_tab()), "Действия")
        root.addWidget(tabs, 1)

        if not self._embedded:
            buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
            buttons.rejected.connect(self.reject)
            buttons.accepted.connect(self.accept)
            root.addWidget(buttons)
        else:
            self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def _wrap_scroll(self, widget: QWidget) -> QScrollArea:
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(widget)
        return scroll

    @staticmethod
    def _format_ts(ts: Any) -> str:
        try:
            value = int(ts or 0)
        except Exception:
            value = 0
        if value <= 0:
            return "неизвестно"
        try:
            return datetime.fromtimestamp(value).strftime("%d.%m.%Y %H:%M")
        except Exception:
            return str(value)

    @staticmethod
    def _format_size(size: Any) -> str:
        try:
            value = int(size or 0)
        except Exception:
            value = 0
        if value <= 0:
            return "n/a"
        units = ["B", "KB", "MB", "GB", "TB"]
        amount = float(value)
        unit_idx = 0
        while amount >= 1024.0 and unit_idx < len(units) - 1:
            amount /= 1024.0
            unit_idx += 1
        if unit_idx == 0:
            return f"{int(amount)} {units[unit_idx]}"
        return f"{amount:.1f} {units[unit_idx]}"

    def _call(self, key: str) -> None:
        cb = self._callbacks.get(key)
        if callable(cb):
            try:
                cb()
            except Exception:
                pass

    def _make_empty_tab(self, text: str) -> QWidget:
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)
        lbl = QLabel(text)
        lbl.setWordWrap(True)
        lbl.setStyleSheet("color:#8da8c4;")
        layout.addWidget(lbl)
        layout.addStretch(1)
        return tab

    def _build_overview_tab(self) -> QWidget:
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(12)

        about = str(self._info.get("about") or "").strip()
        if about:
            about_label = QLabel(about)
            about_label.setWordWrap(True)
            about_label.setStyleSheet("padding:2px 0 10px 0;color:#c7d4e7;")
            layout.addWidget(about_label)

        details = QGridLayout()
        details.setHorizontalSpacing(10)
        details.setVerticalSpacing(8)
        rows = [
            ("ID", self._info.get("id")),
            ("Юзернейм", f"@{self._info.get('username')}" if self._info.get("username") else ""),
            ("Телефон", self._info.get("phone")),
            ("Тип", str(self._info.get("type") or "").strip()),
            ("Участники", self._info.get("members_count")),
            ("Проверка", "Да" if self._info.get("is_verified") else "Нет"),
            ("Премиум", "Да" if self._info.get("is_premium") else "Нет"),
        ]
        row_idx = 0
        for label, value in rows:
            value_str = str(value or "").strip()
            if not value_str:
                continue
            left = QLabel(label)
            left.setStyleSheet("color:#8da8c4;")
            right = QLabel(value_str)
            right.setWordWrap(True)
            details.addWidget(left, row_idx, 0, Qt.AlignmentFlag.AlignTop)
            details.addWidget(right, row_idx, 1)
            row_idx += 1
        if row_idx:
            layout.addLayout(details)

        reactions = list(self._info.get("available_reactions") or [])
        if reactions:
            section = _StatsSection("Доступные реакции", tab)
            chips = QLabel(" ".join(str(item) for item in reactions[:80]))
            chips.setWordWrap(True)
            chips.setStyleSheet("color:#59b7ff;font-size:18px;")
            section.body.addWidget(chips)
            layout.addWidget(section)

        layout.addStretch(1)
        return tab

    def _create_cards_tab(self, rows: List[Dict[str, Any]], *, mode: str) -> QWidget:
        if not rows:
            if mode == "media":
                return self._make_empty_tab("Медиа из этого чата появится здесь после загрузки истории.")
            if mode == "files":
                return self._make_empty_tab("Файлы из этого чата появятся здесь после загрузки истории.")
            if mode == "links":
                return self._make_empty_tab("Ссылки из сообщений этого чата появятся здесь.")
            return self._make_empty_tab("Пока нет данных.")

        container = QWidget(self)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)
        for row in rows:
            card = QFrame(container)
            card.setStyleSheet("QFrame{background:transparent;border:none;border-bottom:1px solid rgba(255,255,255,0.06);}")
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(0, 6, 0, 10)
            card_layout.setSpacing(6)

            if mode == "links":
                url = str(row.get("url") or "")
                top = QLabel(url)
                top.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                top.setStyleSheet("color:#59b7ff;font-weight:600;")
                card_layout.addWidget(top)
                context = str(row.get("context") or "").strip()
                if context:
                    context_lbl = QLabel(context)
                    context_lbl.setWordWrap(True)
                    context_lbl.setStyleSheet("color:#b5c7dc;")
                    card_layout.addWidget(context_lbl)
                meta = QLabel(
                    f"Сообщение #{int(row.get('id') or 0)} • {self._format_ts(row.get('date'))}"
                )
                meta.setStyleSheet("color:#8da8c4;font-size:11px;")
                card_layout.addWidget(meta)
                actions = QHBoxLayout()
                open_btn = QPushButton("Открыть ссылку", card)
                open_btn.clicked.connect(partial(QDesktopServices.openUrl, QUrl(url)))
                actions.addWidget(open_btn, 0)
                actions.addStretch(1)
                card_layout.addLayout(actions)
            else:
                file_name = str(row.get("file_name") or "").strip()
                msg_text = str(row.get("text") or "").strip()
                title = file_name or msg_text or f"Сообщение #{int(row.get('id') or 0)}"
                top = QLabel(title)
                top.setWordWrap(True)
                top.setStyleSheet("color:#f4f7ff;font-weight:600;")
                card_layout.addWidget(top)

                typ = str(row.get("type") or "file")
                meta_parts = [
                    f"#{int(row.get('id') or 0)}",
                    typ,
                    self._format_ts(row.get("date")),
                    self._format_size(row.get("file_size")),
                ]
                meta = QLabel(" • ".join([part for part in meta_parts if part]))
                meta.setStyleSheet("color:#8da8c4;font-size:11px;")
                card_layout.addWidget(meta)
                file_path = str(row.get("file_path") or "").strip()
                if file_path:
                    path_lbl = QLabel(file_path)
                    path_lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                    path_lbl.setStyleSheet("color:#9fb3cc;font-size:11px;")
                    card_layout.addWidget(path_lbl)
                    actions = QHBoxLayout()
                    open_btn = QPushButton("Открыть файл", card)
                    open_btn.clicked.connect(partial(QDesktopServices.openUrl, QUrl.fromLocalFile(file_path)))
                    actions.addWidget(open_btn, 0)
                    actions.addStretch(1)
                    card_layout.addLayout(actions)
            layout.addWidget(card)
        layout.addStretch(1)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(container)
        return scroll

    def _build_media_tab(self) -> QWidget:
        rows = [row for row in list(self._sections.get("media") or []) if isinstance(row, dict)]
        return self._create_cards_tab(rows, mode="media")

    def _build_files_tab(self) -> QWidget:
        rows = [row for row in list(self._sections.get("files") or []) if isinstance(row, dict)]
        return self._create_cards_tab(rows, mode="files")

    def _build_links_tab(self) -> QWidget:
        rows = [row for row in list(self._sections.get("links") or []) if isinstance(row, dict)]
        return self._create_cards_tab(rows, mode="links")

    def _build_members_tab(self) -> QWidget:
        members = [row for row in list(self._sections.get("members") or []) if isinstance(row, dict)]
        if not members:
            return self._make_empty_tab("Список участников появится после синхронизации чата.")
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)
        for row in members:
            line = QFrame(tab)
            line.setStyleSheet("QFrame{background:transparent;border:none;border-bottom:1px solid rgba(255,255,255,0.06);}")
            line_layout = QHBoxLayout(line)
            line_layout.setContentsMargins(0, 6, 0, 10)
            line_layout.setSpacing(10)
            left = QVBoxLayout()
            name = str(row.get("name") or row.get("id") or "unknown")
            username = str(row.get("username") or "").strip()
            status = str(row.get("status") or "").strip()
            kind = str(row.get("type") or "").strip()
            head = name
            if username:
                head += f" (@{username})"
            if status:
                head += f" • {status}"
            if kind:
                head += f" [{kind}]"
            title = QLabel(head)
            title.setWordWrap(True)
            title.setStyleSheet("color:#f4f7ff;font-weight:600;")
            left.addWidget(title)
            msgs = int(row.get("messages") or 0)
            deleted = int(row.get("deleted_messages") or 0)
            last_date = self._format_ts(row.get("last_date"))
            details = QLabel(f"Сообщений: {msgs} • удалено: {deleted} • активен: {last_date}")
            details.setStyleSheet("color:#8da8c4;font-size:11px;")
            left.addWidget(details)
            line_layout.addLayout(left, 1)
            layout.addWidget(line)
        layout.addStretch(1)
        return tab

    def _build_actions_tab(self) -> QWidget:
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title = QLabel("Быстрые действия")
        title.setStyleSheet("font-size:16px;font-weight:700;color:#f4f7ff;")
        layout.addWidget(title)

        stats_btn = QPushButton("Открыть статистику чата", tab)
        stats_btn.clicked.connect(partial(self._call, "show_stats"))
        layout.addWidget(stats_btn, 0)

        read_btn = QPushButton("Пометить чат прочитанным", tab)
        read_btn.clicked.connect(partial(self._call, "mark_read"))
        layout.addWidget(read_btn, 0)

        leave_btn = QPushButton("Покинуть чат / канал", tab)
        leave_btn.setStyleSheet(
            "QPushButton{background:rgba(255,96,96,0.18);color:#ffd7d7;border:none;border-radius:8px;padding:7px 10px;}"
            "QPushButton:hover{background:rgba(255,96,96,0.30);}"
        )
        leave_btn.clicked.connect(partial(self._call, "leave_chat"))
        layout.addWidget(leave_btn, 0)

        note = QLabel("Часть действий может быть недоступна в приватных диалогах или без прав администратора.")
        note.setWordWrap(True)
        note.setStyleSheet("color:#8da8c4;")
        layout.addWidget(note)
        layout.addStretch(1)
        return tab


class ChatStatisticsDialog(QDialog):
    def __init__(self, title: str, data: Dict[str, Any], parent: Optional[QWidget] = None, *, embedded: bool = False) -> None:
        super().__init__(parent)
        self._embedded = bool(embedded)
        self.setWindowTitle("Статистика чата")
        self.resize(520, 640)
        self.setStyleSheet(
            "QDialog{background-color:#0f1b27;color:#dfe7f5;}"
            "QProgressBar{background-color:rgba(255,255,255,0.05);border:none;border-radius:6px;height:10px;text-align:center;}"
            "QProgressBar::chunk{background-color:#59b7ff;border-radius:6px;}"
            "QTabWidget::pane{border:1px solid rgba(255,255,255,0.08);border-radius:10px;background:#102033;}"
            "QTabBar::tab{background:rgba(255,255,255,0.04);color:#b9cce3;padding:8px 12px;margin-right:4px;border-top-left-radius:8px;border-top-right-radius:8px;}"
            "QTabBar::tab:selected{background:rgba(89,183,255,0.22);color:#f4f7ff;}"
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        heading = QLabel(str(title or "Чат"))
        heading.setStyleSheet("font-size:20px;font-weight:700;color:#f4f7ff;")
        root.addWidget(heading)

        tabs = QTabWidget(self)
        root.addWidget(tabs, 1)

        summary_tab, summary_layout = self._stats_tab()
        activity_tab, activity_layout = self._stats_tab()
        reactions_tab, reactions_layout = self._stats_tab()
        senders_tab, senders_layout = self._stats_tab()
        polls_tab, polls_layout = self._stats_tab()
        risk_tab, risk_layout = self._stats_tab()

        tabs.addTab(summary_tab, "Сводка")
        tabs.addTab(activity_tab, "Активность")
        tabs.addTab(reactions_tab, "Реакции")
        tabs.addTab(senders_tab, "Отправители")
        tabs.addTab(polls_tab, "Опросы")
        tabs.addTab(risk_tab, "Риски")

        summary = _StatsSection("Сводка", self)
        metrics = [
            ("Сообщений", int(data.get("total_messages") or 0)),
            ("Медиа", int(data.get("media_messages") or 0)),
            ("Удалено", int(data.get("deleted_messages") or 0)),
            ("Просмотры", int(data.get("total_views") or 0)),
            ("Пересылки", int(data.get("total_forwards") or 0)),
            ("Реакции", int(data.get("total_reactions") or 0)),
        ]
        for label, value in metrics:
            row = QLabel(f"{label}: {value}")
            row.setStyleSheet("color:#dfe7f5;font-size:13px;")
            summary.body.addWidget(row)
        summary_layout.addWidget(summary)

        engagement = data.get("engagement") if isinstance(data.get("engagement"), dict) else {}
        if engagement:
            section = _StatsSection("Вовлечённость", self)
            section.body.addWidget(QLabel(f"Просмотров на сообщение: {float(engagement.get('views_per_message') or 0):.2f}"))
            section.body.addWidget(QLabel(f"Реакций на 100 сообщений: {float(engagement.get('reactions_per_100_messages') or 0):.2f}"))
            section.body.addWidget(QLabel(f"Пересылок на 100 сообщений: {float(engagement.get('forwards_per_100_messages') or 0):.2f}"))
            summary_layout.addWidget(section)

        hourly = [row for row in list(data.get("hourly_activity") or []) if isinstance(row, dict)]
        if hourly:
            section = _StatsSection("Часы пик", self)
            ranked = sorted(hourly, key=lambda row: int(row.get("count") or 0), reverse=True)
            top_hours = sorted(ranked[:12], key=lambda row: int(row.get("hour") or 0))
            max_count = max([int(row.get("count") or 0) for row in top_hours] or [1])
            for row in top_hours:
                hour = int(row.get("hour") or 0)
                count = int(row.get("count") or 0)
                line = QHBoxLayout()
                label = QLabel(f"{hour:02d}:00")
                label.setFixedWidth(54)
                bar = QProgressBar(self)
                bar.setRange(0, max_count)
                bar.setValue(count)
                line.addWidget(label, 0)
                line.addWidget(bar, 1)
                line.addWidget(QLabel(str(count)), 0)
                section.body.addLayout(line)
            activity_layout.addWidget(section)

        daily = [row for row in list(data.get("daily_activity") or []) if isinstance(row, dict)]
        if daily:
            section = _StatsSection("Дни", self)
            max_count = max([int(row.get("count") or 0) for row in daily[-14:]] or [1])
            for row in daily[-14:]:
                day = str(row.get("day") or "")
                count = int(row.get("count") or 0)
                line = QHBoxLayout()
                label = QLabel(day or "день")
                label.setMinimumWidth(100)
                bar = QProgressBar(self)
                bar.setRange(0, max_count)
                bar.setValue(count)
                line.addWidget(label, 0)
                line.addWidget(bar, 1)
                line.addWidget(QLabel(str(count)), 0)
                section.body.addLayout(line)
            activity_layout.addWidget(section)

        reactions = list(data.get("top_reactions") or [])
        if reactions:
            section = _StatsSection("Топ реакций", self)
            max_count = max(int(item.get("count") or 0) for item in reactions) or 1
            for item in reactions:
                line = QHBoxLayout()
                symbol = QLabel(str(item.get("emoji") or item.get("title") or "?"))
                symbol.setFixedWidth(42)
                count = int(item.get("count") or 0)
                text = QLabel(str(count))
                bar = QProgressBar(self)
                bar.setRange(0, max_count)
                bar.setValue(count)
                line.addWidget(symbol, 0)
                line.addWidget(bar, 1)
                line.addWidget(text, 0)
                section.body.addLayout(line)
            reactions_layout.addWidget(section)

        senders = list(data.get("top_senders") or [])
        if senders:
            section = _StatsSection("Активность отправителей", self)
            max_count = max(int(item.get("count") or 0) for item in senders) or 1
            for item in senders:
                name = str(item.get("name") or item.get("sender_id") or "unknown")
                username = str(item.get("username") or "").strip()
                sender_type = str(item.get("type") or "").strip().lower()
                count = int(item.get("count") or 0)
                label_text = name
                if username:
                    label_text += f" (@{username})"
                if sender_type:
                    label_text += f" [{sender_type}]"
                line = QHBoxLayout()
                label = QLabel(label_text)
                label.setMinimumWidth(180)
                label.setWordWrap(True)
                bar = QProgressBar(self)
                bar.setRange(0, max_count)
                bar.setValue(count)
                line.addWidget(label, 1)
                line.addWidget(bar, 1)
                line.addWidget(QLabel(str(count)), 0)
                section.body.addLayout(line)
            senders_layout.addWidget(section)

        reason_labels = {
            "bot_like_sender": "похож на бота",
            "high_message_share": "высокая доля сообщений",
            "dominant_sender": "доминирует в потоке сообщений",
        }
        suspicious = list(data.get("suspicious_senders") or [])
        if suspicious:
            section = _StatsSection("Подозрительные отправители", self)
            for row in suspicious:
                name = str(row.get("name") or row.get("sender_id") or "unknown")
                username = str(row.get("username") or "").strip()
                reason_items = [
                    reason_labels.get(str(x).strip(), str(x).strip())
                    for x in list(row.get("reasons") or [])
                    if str(x).strip()
                ]
                reasons = ", ".join(reason_items)
                count = int(row.get("count") or 0)
                share = float(row.get("share") or 0.0)
                details = name
                if username:
                    details += f" (@{username})"
                details += f" • сообщений: {count} • доля: {share * 100.0:.1f}%"
                if reasons:
                    details += f"\nПризнаки: {reasons}"
                lbl = QLabel(details)
                lbl.setWordWrap(True)
                lbl.setStyleSheet("color:#ffd78c;")
                section.body.addWidget(lbl)
            risk_layout.addWidget(section)

        anomaly_labels = {
            "reactions_exceed_views": "Реакций существенно больше просмотров.",
            "forwards_exceed_views": "Пересылок существенно больше просмотров.",
            "multiple_suspicious_senders": "Обнаружено несколько подозрительных отправителей.",
        }
        anomaly_flags = [str(x) for x in list(data.get("anomaly_flags") or []) if str(x).strip()]
        if anomaly_flags:
            section = _StatsSection("Аномалии", self)
            for flag in anomaly_flags:
                section.body.addWidget(QLabel(f"• {anomaly_labels.get(flag, flag)}"))
            risk_layout.addWidget(section)

        polls_summary = data.get("polls_summary") if isinstance(data.get("polls_summary"), dict) else {}
        if polls_summary:
            section = _StatsSection("Сводка по опросам", self)
            section.body.addWidget(QLabel(f"Опросов: {int(polls_summary.get('total_polls') or 0)}"))
            section.body.addWidget(QLabel(f"Открытых: {int(polls_summary.get('open_polls') or 0)}"))
            section.body.addWidget(QLabel(f"Закрытых: {int(polls_summary.get('closed_polls') or 0)}"))
            section.body.addWidget(QLabel(f"Сумма голосов: {int(polls_summary.get('total_voters') or 0)}"))
            polls_layout.addWidget(section)

        polls = list(data.get("polls") or [])
        if polls:
            for poll in polls[:10]:
                block = _StatsSection(str(poll.get("question") or "Опрос"), self)
                total_voters = int(poll.get("total_voter_count") or 0)
                block.body.addWidget(QLabel(f"Голосов: {total_voters}"))
                max_votes = max([int(opt.get("voter_count") or 0) for opt in list(poll.get("options") or [])] or [1])
                for option in list(poll.get("options") or []):
                    row = QHBoxLayout()
                    label = QLabel(str(option.get("text") or ""))
                    label.setWordWrap(True)
                    label.setMinimumWidth(160)
                    value = int(option.get("voter_count") or 0)
                    bar = QProgressBar(self)
                    bar.setRange(0, max_votes)
                    bar.setValue(value)
                    row.addWidget(label, 1)
                    row.addWidget(bar, 1)
                    row.addWidget(QLabel(str(value)), 0)
                    block.body.addLayout(row)
                polls_layout.addWidget(block)

        for layout in (summary_layout, activity_layout, reactions_layout, senders_layout, polls_layout, risk_layout):
            layout.addStretch(1)

        if not self._embedded:
            buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
            buttons.rejected.connect(self.reject)
            root.addWidget(buttons)
        else:
            self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def _stats_tab(self) -> tuple[QScrollArea, QVBoxLayout]:
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        container = QWidget(scroll)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)
        scroll.setWidget(container)
        return scroll, layout


class MessageStatisticsDialog(QDialog):
    def __init__(self, data: Dict[str, Any], parent: Optional[QWidget] = None, *, embedded: bool = False) -> None:
        super().__init__(parent)
        self._embedded = bool(embedded)
        self.setWindowTitle("Статистика сообщения")
        self.resize(500, 540)
        self.setStyleSheet(
            "QDialog{background-color:#0f1b27;color:#dfe7f5;}"
            "QProgressBar{background-color:rgba(255,255,255,0.05);border:none;border-radius:6px;height:10px;text-align:center;}"
            "QProgressBar::chunk{background-color:#59b7ff;border-radius:6px;}"
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        container = QWidget(scroll)
        body = QVBoxLayout(container)
        body.setContentsMargins(0, 0, 0, 0)
        body.setSpacing(12)
        scroll.setWidget(container)
        root.addWidget(scroll, 1)

        message = dict(data.get("message") or {})
        deleted_snapshot = data.get("deleted_snapshot") if isinstance(data.get("deleted_snapshot"), dict) else None

        preview = QLabel(str(message.get("text") or deleted_snapshot.get("snapshot_text") if deleted_snapshot else ""))
        preview.setWordWrap(True)
        preview.setStyleSheet("padding:0;color:#dfe7f5;")
        body.addWidget(preview)

        summary = _StatsSection("Показатели", self)
        rows = [
            ("Тип", message.get("type") or "text"),
            ("Просмотры", int(message.get("views") or 0)),
            ("Пересылки", int(message.get("forwards") or 0)),
            ("Удалено", "Да" if message.get("is_deleted") else "Нет"),
        ]
        for label, value in rows:
            summary.body.addWidget(QLabel(f"{label}: {value}"))
        body.addWidget(summary)

        sender_profile = data.get("sender_profile") if isinstance(data.get("sender_profile"), dict) else None
        if sender_profile:
            section = _StatsSection("Отправитель", self)
            sender_name = str(sender_profile.get("name") or sender_profile.get("id") or "unknown")
            sender_username = str(sender_profile.get("username") or "")
            sender_type = str(sender_profile.get("type") or "")
            section.body.addWidget(QLabel(f"Имя: {sender_name}"))
            if sender_username:
                section.body.addWidget(QLabel(f"Username: @{sender_username}"))
            if sender_type:
                section.body.addWidget(QLabel(f"Тип: {sender_type}"))
            body.addWidget(section)

        reactions = list(message.get("reactions") or [])
        if reactions:
            section = _StatsSection("Реакции", self)
            max_count = max(int(item.get("count") or 0) for item in reactions) or 1
            for item in reactions:
                row = QHBoxLayout()
                row.addWidget(QLabel(str(item.get("emoji") or item.get("title") or "?")), 0)
                bar = QProgressBar(self)
                count = int(item.get("count") or 0)
                bar.setRange(0, max_count)
                bar.setValue(count)
                row.addWidget(bar, 1)
                row.addWidget(QLabel(str(count)), 0)
                section.body.addLayout(row)
            body.addWidget(section)

        poll = message.get("poll") if isinstance(message.get("poll"), dict) else None
        if poll:
            section = _StatsSection(str(poll.get("question") or "Опрос"), self)
            max_votes = max([int(opt.get("voter_count") or 0) for opt in list(poll.get("options") or [])] or [1])
            for option in list(poll.get("options") or []):
                row = QHBoxLayout()
                row.addWidget(QLabel(str(option.get("text") or "")), 1)
                bar = QProgressBar(self)
                value = int(option.get("voter_count") or 0)
                bar.setRange(0, max_votes)
                bar.setValue(value)
                row.addWidget(bar, 1)
                row.addWidget(QLabel(str(value)), 0)
                section.body.addLayout(row)
            body.addWidget(section)

        if deleted_snapshot:
            section = _StatsSection("Удаление", self)
            section.body.addWidget(QLabel(f"Источник: {deleted_snapshot.get('source') or 'telegram'}"))
            deleted_at = int(deleted_snapshot.get("deleted_at") or 0)
            if deleted_at > 0:
                section.body.addWidget(QLabel(f"Время: {deleted_at}"))
            snapshot_text = str(deleted_snapshot.get("snapshot_text") or "").strip()
            if snapshot_text:
                label = QLabel(snapshot_text)
                label.setWordWrap(True)
                label.setStyleSheet("color:#9fb3cc;")
                section.body.addWidget(label)
            body.addWidget(section)

        risk_labels = {
            "reactions_exceed_views": "Реакций больше ожидаемого относительно просмотров.",
            "forwards_exceed_views": "Пересылок больше ожидаемого относительно просмотров.",
            "sender_bot_like": "Отправитель похож на бот-аккаунт.",
        }
        risk_flags = [str(x) for x in list(data.get("risk_flags") or []) if str(x).strip()]
        if risk_flags:
            section = _StatsSection("Анти-накрутка: риск-флаги", self)
            for flag in risk_flags:
                lbl = QLabel(f"• {risk_labels.get(flag, flag)}")
                lbl.setStyleSheet("color:#ffd78c;")
                section.body.addWidget(lbl)
            body.addWidget(section)

        body.addStretch(1)
        if not self._embedded:
            buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
            buttons.rejected.connect(self.reject)
            root.addWidget(buttons)
        else:
            self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)


def build_header_menu(parent: QWidget) -> QMenu:
    menu = QMenu(parent)
    menu.setStyleSheet(
        "QMenu{background-color:#102033;color:#dfe7f5;border:1px solid rgba(255,255,255,0.08);padding:6px;}"
        "QMenu::item{padding:7px 24px 7px 12px;border-radius:8px;}"
        "QMenu::item:selected{background-color:rgba(255,255,255,0.08);}"
    )
    return menu
