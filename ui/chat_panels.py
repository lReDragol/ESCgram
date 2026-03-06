from __future__ import annotations

from typing import Any, Dict, List, Optional

from PySide6.QtCore import QPoint, Qt, Signal
from PySide6.QtGui import QPixmap
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

        self.btn_info = QPushButton("Профиль", self)
        self.btn_more = QPushButton("⋯", self)
        self.btn_more.setFixedWidth(42)
        layout.addWidget(self.btn_info, 0)
        layout.addWidget(self.btn_more, 0)

        self._click_area.clicked.connect(self.infoRequested.emit)
        self.btn_info.clicked.connect(self.infoRequested.emit)
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
    def __init__(self, info: Dict[str, Any], *, avatar: Optional[QPixmap] = None, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Профиль чата")
        self.resize(460, 520)
        self.setStyleSheet(
            "QDialog{background-color:#0f1b27;color:#dfe7f5;}"
            "QLabel{color:#dfe7f5;}"
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
        title_label = QLabel(str(info.get("title") or info.get("id") or "Чат"))
        title_label.setStyleSheet("font-size:20px;font-weight:700;color:#f4f7ff;")
        text_col.addWidget(title_label)
        subtitle = format_chat_subtitle(info)
        if subtitle:
            subtitle_label = QLabel(subtitle)
            subtitle_label.setStyleSheet("color:#8da8c4;font-size:12px;")
            subtitle_label.setWordWrap(True)
            text_col.addWidget(subtitle_label)
        header.addLayout(text_col, 1)
        root.addLayout(header)

        about = str(info.get("about") or "").strip()
        if about:
            about_label = QLabel(about)
            about_label.setWordWrap(True)
            about_label.setStyleSheet("background-color:rgba(255,255,255,0.04);border-radius:12px;padding:10px 12px;color:#c7d4e7;")
            root.addWidget(about_label)

        details = QGridLayout()
        details.setHorizontalSpacing(10)
        details.setVerticalSpacing(8)
        rows = [
            ("ID", info.get("id")),
            ("Юзернейм", f"@{info.get('username')}" if info.get("username") else ""),
            ("Телефон", info.get("phone")),
            ("Тип", str(info.get("type") or "").strip()),
            ("Участники", info.get("members_count")),
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
            root.addLayout(details)

        reactions = list(info.get("available_reactions") or [])
        if reactions:
            section = _StatsSection("Доступные реакции", self)
            chips = QLabel(" ".join(str(item) for item in reactions[:32]))
            chips.setWordWrap(True)
            chips.setStyleSheet("color:#59b7ff;font-size:18px;")
            section.body.addWidget(chips)
            root.addWidget(section)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
        buttons.rejected.connect(self.reject)
        buttons.accepted.connect(self.accept)
        root.addWidget(buttons)


class ChatStatisticsDialog(QDialog):
    def __init__(self, title: str, data: Dict[str, Any], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Статистика чата")
        self.resize(520, 640)
        self.setStyleSheet(
            "QDialog{background-color:#0f1b27;color:#dfe7f5;}"
            "QProgressBar{background-color:rgba(255,255,255,0.05);border:none;border-radius:6px;height:10px;text-align:center;}"
            "QProgressBar::chunk{background-color:#59b7ff;border-radius:6px;}"
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        heading = QLabel(str(title or "Чат"))
        heading.setStyleSheet("font-size:20px;font-weight:700;color:#f4f7ff;")
        root.addWidget(heading)

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
        root.addWidget(summary)

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
            root.addWidget(section)

        polls = list(data.get("polls") or [])
        if polls:
            scroll = QScrollArea(self)
            scroll.setWidgetResizable(True)
            scroll.setFrameShape(QFrame.Shape.NoFrame)
            container = QWidget(scroll)
            container_layout = QVBoxLayout(container)
            container_layout.setContentsMargins(0, 0, 0, 0)
            container_layout.setSpacing(12)
            for poll in polls[:10]:
                block = _StatsSection(str(poll.get("question") or "Опрос"), container)
                total_voters = int(poll.get("total_voter_count") or 0)
                block.body.addWidget(QLabel(f"Голосов: {total_voters}"))
                max_votes = max([int(opt.get("voter_count") or 0) for opt in list(poll.get("options") or [])] or [1])
                for option in list(poll.get("options") or []):
                    row = QHBoxLayout()
                    label = QLabel(str(option.get("text") or ""))
                    label.setWordWrap(True)
                    label.setMinimumWidth(160)
                    value = int(option.get("voter_count") or 0)
                    bar = QProgressBar(container)
                    bar.setRange(0, max_votes)
                    bar.setValue(value)
                    row.addWidget(label, 1)
                    row.addWidget(bar, 1)
                    row.addWidget(QLabel(str(value)), 0)
                    block.body.addLayout(row)
                container_layout.addWidget(block)
            container_layout.addStretch(1)
            scroll.setWidget(container)
            root.addWidget(scroll, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)


class MessageStatisticsDialog(QDialog):
    def __init__(self, data: Dict[str, Any], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
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

        message = dict(data.get("message") or {})
        deleted_snapshot = data.get("deleted_snapshot") if isinstance(data.get("deleted_snapshot"), dict) else None

        preview = QLabel(str(message.get("text") or deleted_snapshot.get("snapshot_text") if deleted_snapshot else ""))
        preview.setWordWrap(True)
        preview.setStyleSheet("background-color:rgba(255,255,255,0.04);border-radius:12px;padding:10px 12px;color:#dfe7f5;")
        root.addWidget(preview)

        summary = _StatsSection("Показатели", self)
        rows = [
            ("Тип", message.get("type") or "text"),
            ("Просмотры", int(message.get("views") or 0)),
            ("Пересылки", int(message.get("forwards") or 0)),
            ("Удалено", "Да" if message.get("is_deleted") else "Нет"),
        ]
        for label, value in rows:
            summary.body.addWidget(QLabel(f"{label}: {value}"))
        root.addWidget(summary)

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
            root.addWidget(section)

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
            root.addWidget(section)

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
            root.addWidget(section)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)


def build_header_menu(parent: QWidget) -> QMenu:
    menu = QMenu(parent)
    menu.setStyleSheet(
        "QMenu{background-color:#102033;color:#dfe7f5;border:1px solid rgba(255,255,255,0.08);padding:6px;}"
        "QMenu::item{padding:7px 24px 7px 12px;border-radius:8px;}"
        "QMenu::item:selected{background-color:rgba(255,255,255,0.08);}"
    )
    return menu
