from __future__ import annotations

from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import (
    QCheckBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from ui.styles import StyleManager

ASSETS_DIR = Path(__file__).with_name("assets") / "icons"


def _icon_path(name: str) -> Optional[str]:
    path = ASSETS_DIR / name
    if path.exists():
        return str(path)
    return None


def _icon(name: str) -> QIcon:
    source = _icon_path(name)
    return QIcon(source) if source else QIcon()


class MenuActionButton(QPushButton):
    def __init__(self, text: str, icon_name: str, action_id: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(text, parent)
        self.action_id = action_id
        self.setIcon(_icon(icon_name))
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFlat(True)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        StyleManager.instance().bind_stylesheet(self, "settings.action_button")


class MenuToggleRow(QWidget):
    toggled = Signal(bool)

    def __init__(self, text: str, icon_name: str, parent: Optional[QWidget] = None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(10)

        icon_label = QLabel()
        icon_label.setPixmap(_icon(icon_name).pixmap(18, 18))
        layout.addWidget(icon_label, 0)

        self.checkbox = QCheckBox(text)
        self.checkbox.setCursor(Qt.CursorShape.PointingHandCursor)
        StyleManager.instance().bind_stylesheet(self.checkbox, "settings.toggle_row.label")
        layout.addWidget(self.checkbox, 1)

        self.checkbox.toggled.connect(self.toggled.emit)


class SettingsDrawer(QWidget):
    """Side menu styled similarly to AyuGram's drawer."""

    back_requested = Signal()
    auto_download_toggled = Signal(bool)
    ghost_mode_toggled = Signal(bool)
    voice_waveform_toggled = Signal(bool)
    night_mode_toggled = Signal(bool)
    streamer_mode_toggled = Signal(bool)
    show_my_avatar_toggled = Signal(bool)
    menu_action_requested = Signal(str)
    update_requested = Signal()

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self._style_mgr = StyleManager.instance()
        self._style_mgr.bind_stylesheet(self, "settings.drawer.background")

        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(8)

        self.btn_back = QToolButton()
        self.btn_back.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_back.setIcon(_icon("menu/menu_settings.png"))
        self.btn_back.setText("←")
        self.btn_back.clicked.connect(self.back_requested.emit)
        header.addWidget(self.btn_back, 0, Qt.AlignmentFlag.AlignLeft)

        title = QLabel("Меню")
        self._style_mgr.bind_stylesheet(title, "settings.drawer.title")
        header.addWidget(title, 0, Qt.AlignmentFlag.AlignLeft)
        header.addStretch(1)
        root.addLayout(header)

        self.empty_label = QLabel("")
        self.empty_label.hide()
        root.addWidget(self.empty_label)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._style_mgr.bind_stylesheet(scroll, "settings.drawer.scroll")
        root.addWidget(scroll, 1)

        container = QWidget()
        self._container_layout = QVBoxLayout(container)
        self._container_layout.setContentsMargins(0, 0, 0, 0)
        self._container_layout.setSpacing(12)
        scroll.setWidget(container)

        self._build_update_footer(root)

        self._build_account_section()
        self._build_action_section()
        self._build_toggle_section()
        self._container_layout.addStretch(1)

    def _build_update_footer(self, root: QVBoxLayout) -> None:
        footer = QFrame()
        self._style_mgr.bind_stylesheet(footer, "settings.drawer.card")
        layout = QVBoxLayout(footer)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(6)

        self._update_label = QLabel("")
        self._update_label.setWordWrap(True)
        self._style_mgr.bind_stylesheet(self._update_label, "settings.account.hint")
        layout.addWidget(self._update_label)

        self._btn_update = QPushButton("Обновить")
        self._btn_update.setCursor(Qt.CursorShape.PointingHandCursor)
        self._btn_update.clicked.connect(self.update_requested.emit)
        self._btn_update.hide()
        layout.addWidget(self._btn_update, 0, Qt.AlignmentFlag.AlignLeft)

        root.addWidget(footer, 0)
        self.set_update_state("Проверка обновлений...", can_update=False, in_progress=True)

    def _build_account_section(self) -> None:
        card = QFrame()
        self._style_mgr.bind_stylesheet(card, "settings.drawer.card")
        layout = QHBoxLayout(card)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        self._account_avatar = QLabel()
        self._account_avatar.setFixedSize(42, 42)
        self._account_avatar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._style_mgr.bind_stylesheet(self._account_avatar, "settings.account.avatar")
        layout.addWidget(self._account_avatar, 0)

        info = QVBoxLayout()
        info.setSpacing(2)
        self._account_name = QLabel("Текущий аккаунт")
        self._style_mgr.bind_stylesheet(self._account_name, "settings.account.name")
        info.addWidget(self._account_name)
        self._account_hint = QLabel("Нажмите, чтобы переключить аккаунт")
        self._style_mgr.bind_stylesheet(self._account_hint, "settings.account.hint")
        info.addWidget(self._account_hint)
        layout.addLayout(info, 1)

        btn_accounts = MenuActionButton("Аккаунты", "limits/accounts.png", "accounts", self)
        btn_accounts.clicked.connect(lambda: self.menu_action_requested.emit(btn_accounts.action_id))
        layout.addWidget(btn_accounts)

        def _card_click(event):  # type: ignore[override]
            self.menu_action_requested.emit("accounts")
            QWidget.mouseReleaseEvent(card, event)

        card.mouseReleaseEvent = _card_click  # type: ignore[assignment]
        self._container_layout.addWidget(card)

    def set_theme(self, theme: dict) -> None:
        """
        Compatibility stub: allows caller to pass bubble/theme info.
        Currently no-op because drawer does not render bubble previews.
        """
        _ = theme

    def _build_action_section(self) -> None:
        section = QVBoxLayout()
        section.setSpacing(6)
        caption = QLabel("Действия")
        self._style_mgr.bind_stylesheet(caption, "settings.section.caption")
        section.addWidget(caption)

        actions = [
            ("Архив", "menu/add_to_folder.png", "archive"),
            ("Мой профиль", "menu/contacts_alphabet.png", "profile"),
            ("Кошелёк", "menu/read_ticks.png", "wallet"),
            ("Создать группу", "menu/groups_create.png", "create_group"),
            ("Создать канал", "menu/channel.png", "create_channel"),
            ("Контакты", "menu/contacts_alphabet.png", "contacts"),
            ("Звонки", "menu/calls_receive.png", "calls"),
            ("Избранное", "menu/saved_messages.png", "saved"),
            ("Прочитать всё локально", "menu/read.png", "read_local"),
            ("Прочитать всё на сервере", "menu/read_ticks.png", "read_remote"),
            ("Настройки", "menu/menu_settings.png", "settings"),
        ]
        for text, icon_name, action_id in actions:
            btn = MenuActionButton(text, icon_name, action_id, self)
            btn.clicked.connect(lambda _, bid=action_id: self.menu_action_requested.emit(bid))
            section.addWidget(btn)
        self._container_layout.addLayout(section)

    def set_account_info(self, title: str, subtitle: Optional[str] = None, avatar: Optional[QPixmap] = None) -> None:

        if title:

            self._account_name.setText(title)

        if subtitle is not None:

            self._account_hint.setText(subtitle)

        if avatar is not None and not avatar.isNull():

            scaled = avatar.scaled(42, 42, Qt.AspectRatioMode.KeepAspectRatioByExpanding, Qt.TransformationMode.SmoothTransformation)

            self._account_avatar.setPixmap(scaled)

            plain_style = self._style_mgr.stylesheet("settings.account.avatar_plain") or "border-radius:21px;"

            self._account_avatar.setStyleSheet(plain_style)

        else:

            self._account_avatar.setPixmap(QPixmap())

            self._account_avatar.setText("??")

            self._style_mgr.bind_stylesheet(self._account_avatar, "settings.account.avatar")



    def _build_toggle_section(self) -> None:
        section = QVBoxLayout()
        section.setSpacing(6)
        caption = QLabel("Режимы")
        self._style_mgr.bind_stylesheet(caption, "settings.section.caption")
        section.addWidget(caption)

        self.chk_night_mode = self._create_toggle(section, "Ночной режим", "menu/night_mode.png")
        self.chk_night_mode.toggled.connect(lambda value: self.night_mode_toggled.emit(bool(value)))
        self.chk_ghost_mode = self._create_toggle(section, "Режим призрака", "ayu/ghost.png")
        self.chk_ghost_mode.toggled.connect(lambda value: self.ghost_mode_toggled.emit(bool(value)))
        self.chk_streamer_mode = self._create_toggle(section, "Режим стримера", "ayu/streamer.png")
        self.chk_streamer_mode.toggled.connect(lambda value: self.streamer_mode_toggled.emit(bool(value)))

        # Hidden compatibility controls (keep signal wiring intact).
        self.chk_ai_enabled = QCheckBox(self)
        self.chk_ai_enabled.hide()
        self.chk_auto_reply = QCheckBox(self)
        self.chk_auto_reply.hide()
        self.chk_auto_download = QCheckBox(self)
        self.chk_auto_download.hide()
        self.chk_voice_waveform = QCheckBox(self)
        self.chk_voice_waveform.hide()
        self.chk_show_my_avatar = QCheckBox(self)
        self.chk_show_my_avatar.hide()

        self._container_layout.addLayout(section)

    def _create_toggle(self, container: QVBoxLayout, label: str, icon_name: str) -> QCheckBox:
        row = MenuToggleRow(label, icon_name, self)
        container.addWidget(row)
        return row.checkbox

    def set_controls_enabled(self, enabled: bool) -> None:
        self.chk_ghost_mode.setEnabled(True)
        self.chk_night_mode.setEnabled(True)
        self.chk_streamer_mode.setEnabled(True)
        self.chk_ai_enabled.setEnabled(bool(enabled))
        self.chk_auto_reply.setEnabled(False)
        self.empty_label.setVisible(False)

    def sync_flags(self, *, enabled: bool, auto: bool) -> None:
        # Hidden AI toggles are kept only for backward compatibility.
        self.chk_ai_enabled.blockSignals(True)
        self.chk_ai_enabled.setChecked(enabled)
        self.chk_ai_enabled.blockSignals(False)
        self.chk_auto_reply.blockSignals(True)
        self.chk_auto_reply.setChecked(auto and enabled)
        self.chk_auto_reply.blockSignals(False)

    def set_auto_download_checked(self, checked: bool) -> None:
        _ = checked
        self.chk_auto_download.blockSignals(True)
        self.chk_auto_download.setChecked(bool(checked))
        self.chk_auto_download.blockSignals(False)

    def set_ghost_mode_checked(self, checked: bool) -> None:
        self.chk_ghost_mode.blockSignals(True)
        self.chk_ghost_mode.setChecked(bool(checked))
        self.chk_ghost_mode.blockSignals(False)

    def set_voice_waveform_checked(self, checked: bool) -> None:
        _ = checked
        self.chk_voice_waveform.blockSignals(True)
        self.chk_voice_waveform.setChecked(bool(checked))
        self.chk_voice_waveform.blockSignals(False)

    def set_show_my_avatar_checked(self, checked: bool) -> None:
        _ = checked
        self.chk_show_my_avatar.blockSignals(True)
        self.chk_show_my_avatar.setChecked(bool(checked))
        self.chk_show_my_avatar.blockSignals(False)

    def set_night_mode_checked(self, checked: bool) -> None:
        _ = checked
        self.chk_night_mode.blockSignals(True)
        self.chk_night_mode.setChecked(bool(checked))
        self.chk_night_mode.blockSignals(False)

    def set_streamer_mode_checked(self, checked: bool) -> None:
        _ = checked
        self.chk_streamer_mode.blockSignals(True)
        self.chk_streamer_mode.setChecked(bool(checked))
        self.chk_streamer_mode.blockSignals(False)

    def set_update_state(self, text: str, *, can_update: bool, in_progress: bool = False) -> None:
        if hasattr(self, "_update_label"):
            self._update_label.setText(str(text or ""))
        if hasattr(self, "_btn_update"):
            self._btn_update.setVisible(bool(can_update))
            self._btn_update.setEnabled(bool(can_update) and not in_progress)
            if in_progress and can_update:
                self._btn_update.setText("Обновляем...")
            else:
                self._btn_update.setText("Обновить")

