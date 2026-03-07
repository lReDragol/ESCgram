from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from PySide6.QtCore import (
    QEasingCurve,
    QEvent,
    QPoint,
    Qt,
    QSize,
    QTimer,
    QObject,
    QVariantAnimation,
)
from PySide6.QtGui import QIcon, QPixmap, QColor, QPainter
from PySide6.QtWidgets import (
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QSizePolicy,
    QFrame,
    QToolButton,
    QVBoxLayout,
    QHBoxLayout,
    QWidget,
    QButtonGroup,
)

from ui.settings_panel import SettingsDrawer
from ui.styles import StyleManager
try:
    from PySide6.QtGui import QImage
except ImportError:
    try:
        from PyQt6.QtGui import QImage
    except ImportError:
        from PyQt5.QtGui import QImage

ASSETS_DIR = Path(__file__).with_name("assets")
FOLDER_ICONS_DIR = ASSETS_DIR / "icons" / "folders"


@dataclass
class SidebarUI:
    loading_label: QLabel
    container: QWidget


@dataclass(frozen=True)
class FolderSpec:
    folder_id: str
    label: str
    icon_name: str


class FolderButton(QPushButton):
    """AyuGram-like folder tile with compact icon + counter."""

    def __init__(self, spec: FolderSpec, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.spec = spec
        self._style_mgr = StyleManager.instance()
        self.setCheckable(True)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.setFixedSize(76, 66)
        self.setProperty("folder_id", spec.folder_id)
        self._style_mgr.bind_stylesheet(self, "chat_sidebar.folder_button")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 6, 0, 4)
        layout.setSpacing(2)
        layout.setAlignment(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter)

        self._icon_label = QLabel()
        self._icon_label.setFixedSize(36, 36)
        self._icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._icon_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        self._style_mgr.bind_stylesheet(self._icon_label, "chat_sidebar.folder_icon_holder")
        layout.addWidget(self._icon_label, 0, Qt.AlignmentFlag.AlignHCenter)

        self._title_label = QLabel(spec.label)
        self._style_mgr.bind_stylesheet(self._title_label, "chat_sidebar.folder_title")
        self._title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._title_label.setWordWrap(True)
        self._title_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        layout.addWidget(self._title_label, 0, Qt.AlignmentFlag.AlignHCenter)

        self._count_label = QLabel("")
        self._style_mgr.bind_stylesheet(self._count_label, "chat_sidebar.folder_counter")
        self._count_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._count_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        layout.addWidget(self._count_label, 0, Qt.AlignmentFlag.AlignHCenter)
        self._set_icon_pixmap()
        self.set_count(0)

    def _set_icon_pixmap(self) -> None:
        icon_path = FOLDER_ICONS_DIR / self.spec.icon_name
        pixmap = QPixmap(str(icon_path)) if icon_path.exists() else QPixmap()
        if pixmap.isNull():
            pixmap = QPixmap(32, 32)
            pixmap.fill(Qt.GlobalColor.transparent)

        color_hex = str(self._style_mgr.value("colors.chat_sidebar.folder_icon", "#768c9e"))
        tinted = self._tint_pixmap(pixmap, QColor(color_hex))

        # Увеличиваем силуэт и укладываем в размер икон-лейбла
        box = self._icon_label.size()
        enlarged = self._enlarge_nontransparent(tinted, scale=1.7, box=box)
        self._icon_label.setPixmap(enlarged)

    @staticmethod
    def _tint_pixmap(source: QPixmap, color: QColor, threshold: int = 16) -> QPixmap:
        if source.isNull():
            return source
        fmt = getattr(QImage.Format, 'Format_ARGB32',
                      getattr(QImage, 'Format_ARGB32', None))
        img = source.toImage().convertToFormat(fmt)
        w, h = img.width(), img.height()

        # строим альфу из яркости: чем темнее (ближе к чёрному), тем прозрачнее
        for y in range(h):
            for x in range(w):
                rgba = img.pixel(x, y)
                r = (rgba >> 16) & 0xFF
                g = (rgba >> 8) & 0xFF
                b = rgba & 0xFF
                # простая яркость (0..255)
                y8 = (77 * r + 150 * g + 29 * b) >> 8
                # 0..255: ниже порога — 0 (прозрачный), выше — плавный рост
                a = 0 if y8 <= threshold else min(255, (y8 - threshold) * 255 // (255 - threshold))
                img.setPixel(x, y, (a << 24) | (color.red() << 16) | (color.green() << 8) | color.blue())

        return QPixmap.fromImage(img)

    def set_count(self, count: int) -> None:
        value = max(0, int(count or 0))
        if value <= 0:
            self._count_label.setText("")
            self._count_label.hide()
            return
        text = str(value) if value < 1000 else "999+"
        self._count_label.setText(text)
        self._count_label.show()

    @staticmethod
    def _enlarge_nontransparent(source: QPixmap, *, scale: float, box: QSize) -> QPixmap:
        if source.isNull():
            return source

        # 1) Находим границы непрозрачных пикселей по альфе
        fmt = getattr(QImage.Format, 'Format_ARGB32',
                      getattr(QImage, 'Format_ARGB32', None))
        img = source.toImage().convertToFormat(fmt)
        w, h = img.width(), img.height()

        left, top, right, bottom = w, h, -1, -1
        for y in range(h):
            for x in range(w):
                if (img.pixel(x, y) >> 24) & 0xFF:  # alpha > 0
                    if x < left:   left = x
                    if y < top:    top = y
                    if x > right:  right = x
                    if y > bottom: bottom = y

        # Если альфа пустая — просто впишем как есть
        if right < left or bottom < top:
            return source.scaled(box, Qt.AspectRatioMode.KeepAspectRatio,
                                 Qt.TransformationMode.SmoothTransformation)

        # 2) Кроп по содержимому
        from PySide6.QtCore import QRect
        crop_rect = QRect(left, top, right - left + 1, bottom - top + 1)
        content = source.copy(crop_rect)

        # 3) Увеличиваем силуэт на 50%, но не больше размеров box
        target_w = min(int(content.width() * scale), max(1, box.width()))
        target_h = min(int(content.height() * scale), max(1, box.height()))
        scaled = content.scaled(target_w, target_h,
                                Qt.AspectRatioMode.KeepAspectRatio,
                                Qt.TransformationMode.SmoothTransformation)

        # 4) Кладём по центру прозрачного холста box
        result = QPixmap(box)
        result.fill(Qt.GlobalColor.transparent)
        p = QPainter(result)
        x = (box.width() - scaled.width()) // 2
        y = (box.height() - scaled.height()) // 2
        p.drawPixmap(x, y, scaled)
        p.end()
        return result


class ChatListRowWidget(QWidget):
    """Compact chat row with title, meta info and right unread badge."""

    def __init__(
        self,
        *,
        title: str,
        meta: str,
        unread: int,
        avatar_size: int = 40,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, False)
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        self._avatar_size = max(28, int(avatar_size or 40))
        self._avatar_cache_key: Optional[tuple] = None
        self._avatar_pixmap_key: Optional[int] = None

        root = QHBoxLayout(self)
        root.setContentsMargins(6, 4, 6, 4)
        root.setSpacing(8)

        self._avatar = QLabel(self)
        self._avatar.setFixedSize(self._avatar_size, self._avatar_size)
        self._avatar.setStyleSheet("border-radius:18px; background:rgba(40,56,74,0.65);")
        self._avatar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        root.addWidget(self._avatar, 0, Qt.AlignmentFlag.AlignVCenter)

        text_col = QVBoxLayout()
        text_col.setContentsMargins(0, 0, 0, 0)
        text_col.setSpacing(1)

        self._title = QLabel(str(title or "").strip(), self)
        self._title.setStyleSheet("font-size:13px; font-weight:600; color:#dce9f8; background:transparent;")
        self._title.setWordWrap(False)
        text_col.addWidget(self._title, 0)

        self._meta = QLabel(str(meta or "").strip(), self)
        self._meta.setStyleSheet("font-size:11px; color:#8ea3bb; background:transparent;")
        self._meta.setWordWrap(False)
        self._meta.setVisible(bool(meta))
        text_col.addWidget(self._meta, 0)

        root.addLayout(text_col, 1)

        self._badge = QLabel("", self)
        self._badge.setMinimumWidth(22)
        self._badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._badge.setStyleSheet(
            "background-color:#2f94d5; color:white; border-radius:10px; padding:1px 7px; font-size:11px; font-weight:700;"
        )
        root.addWidget(self._badge, 0, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.set_unread(unread)

    def set_avatar(self, pixmap: Optional[QPixmap]) -> None:
        if pixmap is None or pixmap.isNull():
            self._avatar.setPixmap(QPixmap())
            self._avatar_cache_key = None
            self._avatar_pixmap_key = None
            return
        try:
            self._avatar_pixmap_key = int(pixmap.cacheKey())
        except Exception:
            self._avatar_pixmap_key = None
        scaled = pixmap.scaled(
            self._avatar_size,
            self._avatar_size,
            Qt.AspectRatioMode.KeepAspectRatioByExpanding,
            Qt.TransformationMode.SmoothTransformation,
        )
        self._avatar.setPixmap(scaled)

    def set_avatar_cached(self, pixmap: Optional[QPixmap], *, cache_key: Optional[tuple] = None) -> None:
        key = cache_key if cache_key is not None else ("none",)
        pixmap_key = None
        if pixmap is not None and not pixmap.isNull():
            try:
                pixmap_key = int(pixmap.cacheKey())
            except Exception:
                pixmap_key = None
        if self._avatar_cache_key == key and self._avatar_pixmap_key == pixmap_key:
            return
        self._avatar_cache_key = key
        self.set_avatar(pixmap)

    def set_title(self, title: str) -> None:
        normalized = str(title or "").strip()
        if self._title.text() != normalized:
            self._title.setText(normalized)

    def set_meta(self, meta: str) -> None:
        normalized = str(meta or "").strip()
        if self._meta.text() != normalized:
            self._meta.setText(normalized)
        self._meta.setVisible(bool(normalized))

    def update_row(self, *, title: str, meta: str, unread: int) -> None:
        self.set_title(title)
        self.set_meta(meta)
        self.set_unread(unread)

    def set_unread(self, unread: int) -> None:
        count = max(0, int(unread or 0))
        if count <= 0:
            self._badge.setVisible(False)
            self._badge.clear()
            return
        self._badge.setVisible(True)
        self._badge.setText(str(count if count < 1000 else "999+"))


class ChatSidebarMixin:
    """Mixin that encapsulates the chat list, search, and settings drawer UI."""

    # ---- stubs (чтобы инспектор IDE не орал на «Unresolved attribute reference») ----
    server: Any                      # должен предоставить get_ai_flags(), set_ai_flags(), …
    history: Dict[str, Any]          # {"chats": {chat_id: [messages...]}}
    all_chats: Dict[str, Dict[str, Any]]
    current_chat_id: Optional[str]
    avatar_cache: Any                # опционально: .chat(chat_id, info) -> QPixmap
    auto_ai_checkbox: Optional[Any]  # опционально: QCheckBox
    _avatar_size: int = 44           # дефолт на случай, если хост не задаёт

    # слоты, которые обычно реализует хост; заглушки убирают ворнинги инспектора
    def on_chat_list_clicked(self, item: QListWidgetItem) -> None:
        """Должен быть переопределён хостом."""
        # no-op заглушка: не бросаем исключение специально
        return

    def update_ai_controls_state(self) -> None:
        """Должен быть переопределён хостом."""
        return

    # ---- UI ----
    chat_list: QListWidget
    search: QLineEdit
    menu_button: QToolButton
    settings_panel: SettingsDrawer
    _settings_anim: QVariantAnimation
    _settings_visible: bool
    _left_wrap: QWidget
    sidebar_ui: SidebarUI

    def _build_sidebar(self) -> QWidget:
        self._style_mgr = getattr(self, "_style_mgr", StyleManager.instance())
        left = QVBoxLayout()
        left.setContentsMargins(0, 8, 8, 8)
        left.setSpacing(10)
        self._chat_list_override_mode: str = ""
        self._chat_list_override_rows: List[Dict[str, Any]] = []

        search_row = QHBoxLayout()
        search_row.setContentsMargins(0, 0, 0, 0)
        search_row.setSpacing(8)

        self.menu_button = QToolButton()
        self.menu_button.setCheckable(True)
        self.menu_button.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.menu_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.menu_button.setAutoRaise(True)
        self.menu_button.setText("☰")
        self.menu_button.setIconSize(QSize(20, 20))
        self.menu_button.setToolTip("Настройки")
        self._style_mgr.bind_stylesheet(self.menu_button, "chat_sidebar.menu_button")
        self.menu_button.toggled.connect(self._toggle_settings_panel)
        search_row.addWidget(self.menu_button, 0, Qt.AlignmentFlag.AlignLeft)

        self.search = QLineEdit(placeholderText="Поиск (имя, @username, id)…")
        self.search.setClearButtonEnabled(True)
        self.search.textChanged.connect(self._on_search_text_changed)
        search_row.addWidget(self.search, 1)

        search_container = QWidget()
        search_container.setLayout(search_row)
        left.addWidget(search_container)

        loading_label = QLabel("")
        self._style_mgr.bind_stylesheet(loading_label, "chat_sidebar.loading")
        loading_label.hide()
        left.addWidget(loading_label)

        folders_row = QHBoxLayout()
        folders_row.setContentsMargins(0, 0, 0, 0)
        folders_row.setSpacing(0)

        folder_wrap = QFrame()
        folder_wrap.setObjectName("folderPanel")
        self._style_mgr.bind_stylesheet(folder_wrap, "chat_sidebar.folder_panel")
        folder_wrap.setFixedWidth(72)
        folder_wrap.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Expanding)
        folder_column = QVBoxLayout(folder_wrap)
        folder_column.setContentsMargins(0, 0, 0, 0)
        folder_column.setSpacing(0)

        self.folder_group = QButtonGroup(self)
        self.folder_group.setExclusive(True)
        self._active_folder = "all"
        self.folder_buttons: Dict[str, FolderButton] = {}
        self.folder_specs: List[FolderSpec] = [
            FolderSpec("all", "Все чаты", "folders_all.png"),
            FolderSpec("unread", "Непрочит.", "folders_unread.png"),
            FolderSpec("private", "Личные", "folders_private.png"),
            FolderSpec("group", "Группы", "folders_group.png"),
            FolderSpec("channel", "Каналы", "folders_channels.png"),
            FolderSpec("bot", "Боты", "folders_bots.png"),
        ]
        self._folder_counts: Dict[str, int] = {}
        self._chat_items_by_id: Dict[str, QListWidgetItem] = {}
        self._chat_row_widgets_by_id: Dict[str, ChatListRowWidget] = {}
        self._chat_list_order: List[str] = []
        self._chat_list_data_signature = None
        for idx, spec in enumerate(self.folder_specs):
            btn = FolderButton(spec)
            self.folder_group.addButton(btn, idx)
            folder_column.addWidget(btn)
            self.folder_buttons[spec.folder_id] = btn
            if spec.folder_id == "all":
                btn.setChecked(True)
        folder_column.addStretch(1)
        self.folder_group.buttonToggled.connect(self._on_folder_button_toggled)

        self.chat_list = QListWidget()
        # слот предоставляется хост-классом; заглушка выше убрала ворнинг инспектора
        self.chat_list.itemClicked.connect(self.on_chat_list_clicked)
        self.chat_list.setUniformItemSizes(False)
        self.chat_list.setAlternatingRowColors(False)
        self.chat_list.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.chat_list.setIconSize(QSize(self._avatar_size, self._avatar_size))
        self.chat_list.setSpacing(0)
        self.chat_list.setMinimumWidth(270 + 20)
        row_h = max(32, self._avatar_size)
        self.chat_list.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        list_style = self._style_mgr.stylesheet("chat_sidebar.list", {"row_height": row_h})
        self.chat_list.setStyleSheet(list_style)
        folders_row.addWidget(folder_wrap, 0)
        folders_row.addWidget(self.chat_list, 1)
        left.addLayout(folders_row, 1)
        try:
            self.chat_list.viewport().installEventFilter(cast(QObject, self))
        except Exception:
            pass
        try:
            vbar = self.chat_list.verticalScrollBar()
            vbar.valueChanged.connect(lambda _value=0: self._schedule_visible_chat_avatar_refresh())
            vbar.rangeChanged.connect(lambda _min=0, _max=0: self._schedule_visible_chat_avatar_refresh())
        except Exception:
            pass
        self._chat_avatar_refresh_timer = QTimer(self.chat_list)
        self._chat_avatar_refresh_timer.setSingleShot(True)
        self._chat_avatar_refresh_timer.timeout.connect(self._refresh_visible_chat_avatars)

        self._left_wrap = QWidget()
        self._left_wrap.setLayout(left)
        # подсказка типам для инспектора (миксин фактически QObject через хост)
        self._left_wrap.installEventFilter(cast(QObject, self))

        host_widget = cast(QWidget, self)
        self.settings_panel = SettingsDrawer(host_widget)
        self.settings_panel.set_controls_enabled(False)
        self.settings_panel.hide()
        self._settings_visible = False
        self._settings_hidden_offset = 12

        # родителем анимации делаем сам settings_panel -> исчез «Expected QObject»
        self._settings_anim = QVariantAnimation(self.settings_panel)
        self._settings_anim.setDuration(260)
        self._settings_anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        self._settings_anim.valueChanged.connect(self._on_settings_anim_value)
        self._settings_anim.finished.connect(self._on_settings_anim_finished)
        self.settings_panel.chk_ai_enabled.toggled.connect(self._on_settings_ai_toggled)
        self.settings_panel.chk_auto_reply.toggled.connect(self._on_settings_auto_toggled)
        self.settings_panel.auto_download_toggled.connect(self._on_settings_auto_download_toggled)
        self.settings_panel.ghost_mode_toggled.connect(self._on_settings_ghost_toggled)
        self.settings_panel.voice_waveform_toggled.connect(self._on_settings_waveform_toggled)
        self.settings_panel.show_my_avatar_toggled.connect(self._on_settings_show_my_avatar_toggled)
        self.settings_panel.night_mode_toggled.connect(self._on_settings_night_toggled)
        self.settings_panel.streamer_mode_toggled.connect(self._on_settings_streamer_toggled)
        self.settings_panel.menu_action_requested.connect(self._on_settings_menu_action)
        self.settings_panel.back_requested.connect(lambda: self.menu_button.setChecked(False))
        QTimer.singleShot(0, self._reposition_settings_panel)

        self.sidebar_ui = SidebarUI(loading_label=loading_label, container=self._left_wrap)
        return self._left_wrap

    # ------------------------------------------------------------------ #
    # Settings drawer helpers

    def _toggle_settings_panel(self, visible: bool) -> None:
        if self._settings_visible == visible and self.settings_panel.isVisible() == visible:
            return
        current_raw = self._settings_anim.currentValue()
        current_value = float(current_raw) if current_raw is not None else (1.0 if self._settings_visible else 0.0)
        self._settings_visible = visible
        shown_pos, hidden_pos = self._settings_panel_positions()
        self.settings_panel.show()
        start_ratio = current_value
        end_ratio = 1.0 if visible else 0.0
        self._settings_anim.stop()
        self._settings_anim.setStartValue(start_ratio)
        self._settings_anim.setEndValue(end_ratio)
        self._settings_anim.start()
        if hasattr(self, "_on_settings_panel_toggled"):
            getattr(self, "_on_settings_panel_toggled")(visible)
        if visible:
            self.update_ai_controls_state()

    def _on_settings_anim_value(self, value: float) -> None:
        ratio = max(0.0, min(1.0, float(value)))
        shown_pos, hidden_pos = self._settings_panel_positions()
        x = hidden_pos.x() + (shown_pos.x() - hidden_pos.x()) * ratio
        y = shown_pos.y()
        self.settings_panel.move(int(x), y)
        if hasattr(self, "_on_settings_animation_progress"):
            try:
                getattr(self, "_on_settings_animation_progress")(ratio)
            except Exception:
                pass

    def _on_settings_anim_finished(self) -> None:
        if not self._settings_visible:
            self.settings_panel.hide()
            self._reposition_settings_panel()

    def _reposition_settings_panel(self) -> None:
        ratio = 1.0 if self._settings_visible else 0.0
        self._on_settings_anim_value(ratio)
        self.settings_panel.raise_()

    def _settings_panel_positions(self) -> tuple[QPoint, QPoint]:
        width = max(240, min(360, max(1, self._left_wrap.width())))
        height = max(1, self.height())
        self.settings_panel.setFixedSize(width, height)
        anchor = self._left_wrap.mapTo(self, QPoint(0, 0))
        shown = QPoint(anchor.x(), 0)
        hidden = QPoint(anchor.x() - width - self._settings_hidden_offset, 0)
        return shown, hidden

    def eventFilter(self, obj, event):  # type: ignore[override]
        if obj is self._left_wrap and event.type() == QEvent.Type.Resize:
            self._reposition_settings_panel()
        if obj is getattr(self, "chat_list", None) and event.type() == QEvent.Type.Resize:
            self._schedule_visible_chat_avatar_refresh()
        viewport = getattr(getattr(self, "chat_list", None), "viewport", lambda: None)()
        if obj is viewport and event.type() in {QEvent.Type.Resize, QEvent.Type.Show}:
            self._schedule_visible_chat_avatar_refresh()
        return False

    def _schedule_visible_chat_avatar_refresh(self) -> None:
        timer = getattr(self, "_chat_avatar_refresh_timer", None)
        if timer is None:
            return
        if timer.isActive():
            timer.stop()
        timer.start(80)

    def _on_settings_auto_toggled(self, checked: bool) -> None:
        if not self.current_chat_id:
            return
        checkbox = getattr(self, "auto_ai_checkbox", None)
        if checkbox is not None:
            checkbox.setChecked(bool(checked))

    def _on_settings_ai_toggled(self, checked: bool) -> None:
        if not self.current_chat_id:
            return
        kwargs = {"ai": bool(checked)}
        if not checked:
            kwargs["auto"] = False
        self.server.set_ai_flags(self.current_chat_id, **kwargs)
        if not checked:
            checkbox = getattr(self, "auto_ai_checkbox", None)
            if checkbox is not None:
                checkbox.blockSignals(True)
                checkbox.setChecked(False)
                checkbox.blockSignals(False)
        self.populate_chat_list()
        self.update_ai_controls_state()

    def _on_settings_auto_download_toggled(self, checked: bool) -> None:
        handler = getattr(self, "on_auto_download_setting_changed", None)
        if callable(handler):
            handler(bool(checked))

    def _on_settings_ghost_toggled(self, checked: bool) -> None:
        handler = getattr(self, "on_ghost_mode_setting_changed", None)
        if callable(handler):
            handler(bool(checked))

    def _on_settings_waveform_toggled(self, checked: bool) -> None:
        handler = getattr(self, "on_voice_waveform_setting_changed", None)
        if callable(handler):
            handler(bool(checked))

    def _on_settings_show_my_avatar_toggled(self, checked: bool) -> None:
        handler = getattr(self, "on_show_my_avatar_setting_changed", None)
        if callable(handler):
            handler(bool(checked))

    def _on_settings_night_toggled(self, checked: bool) -> None:
        handler = getattr(self, "on_night_mode_setting_changed", None)
        if callable(handler):
            handler(bool(checked))

    def _on_settings_streamer_toggled(self, checked: bool) -> None:
        handler = getattr(self, "on_streamer_mode_setting_changed", None)
        if callable(handler):
            handler(bool(checked))

    def _on_search_text_changed(self, text: str) -> None:
        handler = getattr(self, "on_sidebar_search_changed", None)
        if callable(handler):
            try:
                handled = bool(handler(str(text or "")))
            except Exception:
                handled = False
            if handled:
                return
        self._apply_filter(str(text or ""))

    def _on_settings_menu_action(self, action_id: str) -> None:
        handler = getattr(self, "on_sidebar_action_requested", None)
        if callable(handler):
            handler(str(action_id))

    def _on_folder_button_toggled(self, button: FolderButton, checked: bool) -> None:
        if not (button and checked):
            return
        if getattr(self, "_chat_list_override_mode", ""):
            self._chat_list_override_mode = ""
            self._chat_list_override_rows = []
        folder_id = button.property("folder_id") or "all"
        self._active_folder = str(folder_id)
        self.populate_chat_list()

    def set_chat_list_override(self, *, mode: str, rows: List[Dict[str, Any]]) -> None:
        self._chat_list_override_mode = str(mode or "").strip()
        normalized: List[Dict[str, Any]] = []
        for row in list(rows or []):
            if isinstance(row, dict):
                normalized.append(dict(row))
        self._chat_list_override_rows = normalized
        try:
            self.search.clear()
        except Exception:
            pass
        self.populate_chat_list()

    def clear_chat_list_override(self) -> None:
        if not getattr(self, "_chat_list_override_mode", "") and not getattr(self, "_chat_list_override_rows", None):
            return
        self._chat_list_override_mode = ""
        self._chat_list_override_rows = []
        self.populate_chat_list()

    # ------------------------------------------------------------------ #
    # Chat list helpers

    @staticmethod
    def _chat_id_aliases(chat_id: str) -> List[str]:
        cid = str(chat_id or "").strip()
        if not cid:
            return []
        aliases: List[str] = [cid]
        unsigned = cid.lstrip("-")
        if unsigned and unsigned not in aliases:
            aliases.append(unsigned)
        if unsigned.startswith("100") and len(unsigned) > 3:
            short = unsigned[3:]
            if short and short not in aliases:
                aliases.append(short)
        return aliases

    @staticmethod
    def _chat_type_tokens(chat_type: str) -> List[str]:
        mapping = {
            "private": ["private", "личка", "личные"],
            "bot": ["bot", "бот"],
            "group": ["group", "группа"],
            "supergroup": ["supergroup", "супергруппа", "group", "группа"],
            "megagroup": ["megagroup", "мегагруппа", "group", "группа"],
            "channel": ["channel", "канал"],
        }
        key = str(chat_type or "").lower()
        return mapping.get(key, [key] if key else [])

    def _build_chat_search_blob(self, chat_id: str, info: Dict[str, Any]) -> str:
        title = str(info.get("title_display") or info.get("title") or chat_id).strip().lower()
        username = str(info.get("username") or "").strip().lower()
        id_tokens = [tok.lower() for tok in self._chat_id_aliases(chat_id)]
        type_tokens = [tok.lower() for tok in self._chat_type_tokens(str(info.get("type") or ""))]
        parts = [title, username, str(chat_id).lower(), *id_tokens, *type_tokens]
        return " ".join(p for p in parts if p)

    def _apply_filter(self, text: str) -> None:
        query = (text or "").strip().lower()
        if not query:
            for idx in range(self.chat_list.count()):
                item = self.chat_list.item(idx)
                if item:
                    item.setHidden(False)
            return

        tokens = [t for t in query.split() if t]
        for idx in range(self.chat_list.count()):
            item = self.chat_list.item(idx)
            if not item:
                continue
            chat_id = str(item.data(Qt.ItemDataRole.UserRole) or "")
            info = item.data(Qt.ItemDataRole.UserRole + 1) or {}
            display_source = info.get("title_display") or info.get("title") or item.text()
            title = str(display_source).lower()
            username = str(info.get("username", "")).lower()
            id_aliases = info.get("_id_aliases")
            if not isinstance(id_aliases, list):
                id_aliases = self._chat_id_aliases(chat_id)
            haystack = str(info.get("_search_blob") or f"{title} {username} {chat_id}").lower()
            match = True
            for token in tokens:
                if token.startswith("@"):
                    handle = token.lstrip("@")
                    if handle and handle not in username:
                        match = False
                        break
                    continue
                if token.startswith("id:"):
                    needle = token[3:].strip()
                    if needle and not any(needle in str(alias).lower() for alias in id_aliases):
                        match = False
                        break
                    continue
                if token not in haystack:
                    match = False
                    break
            item.setHidden(not match)

    def populate_chat_list(self) -> None:
        current_id = self.current_chat_id
        search_text = self.search.text()
        chat_items_by_id: Dict[str, QListWidgetItem] = {}
        chat_rows_by_id: Dict[str, ChatListRowWidget] = {}

        self.chat_list.setUpdatesEnabled(False)
        try:
            override_mode = str(getattr(self, "_chat_list_override_mode", "") or "").strip()
            if override_mode:
                self._populate_chat_list_override(
                    current_id=current_id,
                    search_text=search_text,
                    chat_items_by_id=chat_items_by_id,
                    chat_rows_by_id=chat_rows_by_id,
                )
                return

            all_ids = set(self.history.get("chats", {}).keys()) | set(self.all_chats.keys())
            search_active = bool((search_text or "").strip())

            def _build_item(chat_id: str) -> Dict[str, object]:
                base = dict(self.all_chats.get(chat_id, {}))
                base.setdefault("title", chat_id)
                base.setdefault("type", "private")
                base.setdefault("username", "")
                last_ts = base.get("last_ts") or self._history_last_ts(chat_id)
                base["last_ts"] = int(last_ts or 0)
                base["pinned"] = bool(base.get("pinned"))
                base["unread_count"] = max(0, int(base.get("unread_count") or 0))
                base["id"] = chat_id
                ai_flags = self.server.get_ai_flags(chat_id)
                base["_ai_flags"] = ai_flags
                base["_used_ai"] = self._history_has_ai_messages(chat_id)
                base["_ai_indicator"] = self._compose_indicator(chat_id, info=base)
                base["_sort_importance"] = self._importance_for_sort(chat_id, info=base)
                return base

            items = [_build_item(cid) for cid in all_ids]
            items.sort(key=self._chat_sort_key)
            self._update_folder_counts(items)

            active_filter = getattr(self, "_active_folder", "all")
            hidden_set = set(getattr(self, "_hidden_chats", set()))
            custom_titles = dict(getattr(self, "_chat_custom_titles", {}))
            hide_hidden = bool(getattr(self, "_hide_hidden_chats", False))

            visible_rows: List[Dict[str, object]] = []
            signature_rows: List[tuple] = []
            for info in items:
                cid = str(info["id"])
                if (not search_active) and (not self._passes_folder_filter(info, active_filter)):
                    continue
                is_hidden = cid in hidden_set
                if is_hidden and hide_hidden:
                    continue
                title = str(custom_titles.get(cid, (info.get("title") or cid).strip()))
                indicator = str(info.get("_ai_indicator") or self._compose_indicator(cid, info=info))
                unread_count = max(0, int(info.get("unread_count") or 0))
                type_label = self._type_label(str(info.get("type", "")))
                meta_parts: List[str] = []
                if type_label:
                    meta_parts.append(type_label)
                if info.get("pinned"):
                    meta_parts.append("Закреплён")
                if indicator == "🟢":
                    meta_parts.append("AI: авто")
                elif indicator == "🔴":
                    meta_parts.append("AI: выкл")
                elif indicator == "⚪":
                    meta_parts.append("AI")
                if is_hidden:
                    meta_parts.append("Скрыт")
                meta = " • ".join(meta_parts).strip()
                info = dict(info)
                info["title_display"] = title
                info["_id_aliases"] = self._chat_id_aliases(cid)
                info["_search_blob"] = self._build_chat_search_blob(cid, info)

                signature_rows.append(
                    (
                        cid,
                        title,
                        int(info.get("last_ts") or 0),
                        unread_count,
                        int(bool(info.get("pinned"))),
                        str(info.get("type") or ""),
                        str(info.get("username") or ""),
                        indicator,
                        int(is_hidden),
                        str(info.get("photo_small_id") or info.get("photo_small") or ""),
                    )
                )
                visible_rows.append(
                    {
                        "id": cid,
                        "info": info,
                        "title": title,
                        "meta": meta,
                        "unread": unread_count,
                    }
                )

            data_signature = (
                tuple(signature_rows),
                str(active_filter),
                int(hide_hidden),
            )
            if data_signature == getattr(self, "_chat_list_data_signature", None):
                self._refresh_visible_chat_avatars()
                if current_id:
                    item = self._chat_items_by_id.get(current_id)
                    if item:
                        self.chat_list.setCurrentItem(item)
                if search_text:
                    self._apply_filter(search_text)
                return
            ordered_ids = [str(row["id"]) for row in visible_rows]
            can_update_in_place = (
                ordered_ids == list(getattr(self, "_chat_list_order", []))
                and len(ordered_ids) == self.chat_list.count()
            )
            if can_update_in_place:
                for idx, cid in enumerate(ordered_ids):
                    item = self.chat_list.item(idx)
                    if item is None or str(item.data(Qt.ItemDataRole.UserRole) or "") != cid:
                        can_update_in_place = False
                        break

            current_item: Optional[QListWidgetItem] = None
            row_h = max(32, self._avatar_size + 8)  # высота строки ≈ аватар + небольшой отступ
            if can_update_in_place:
                for idx, row in enumerate(visible_rows):
                    cid = str(row["id"])
                    info = dict(row["info"]) if isinstance(row.get("info"), dict) else {}
                    title = str(row.get("title") or cid)
                    meta = str(row.get("meta") or "")
                    unread = int(row.get("unread") or 0)
                    item = self.chat_list.item(idx)
                    if item is None:
                        continue
                    item.setData(Qt.ItemDataRole.UserRole, cid)
                    item.setData(Qt.ItemDataRole.UserRole + 1, info)
                    row_widget = self.chat_list.itemWidget(item)
                    if isinstance(row_widget, ChatListRowWidget):
                        row_widget.update_row(title=title, meta=meta, unread=unread)
                        pixmap, avatar_key = self._chat_list_avatar_payload(cid, info, title)
                        if pixmap is not None:
                            row_widget.set_avatar_cached(pixmap, cache_key=avatar_key)
                        chat_rows_by_id[cid] = row_widget
                    chat_items_by_id[cid] = item
                    if current_id and cid == current_id:
                        current_item = item
            else:
                self.chat_list.clear()
                for row in visible_rows:
                    cid = str(row["id"])
                    info = dict(row["info"]) if isinstance(row.get("info"), dict) else {}
                    title = str(row.get("title") or cid)
                    meta = str(row.get("meta") or "")
                    unread = int(row.get("unread") or 0)
                    item = QListWidgetItem("")
                    item.setData(Qt.ItemDataRole.UserRole, cid)
                    item.setData(Qt.ItemDataRole.UserRole + 1, info)
                    item.setSizeHint(QSize(270, max(row_h, self._avatar_size + 12)))
                    row_widget = ChatListRowWidget(
                        title=title,
                        meta=meta,
                        unread=unread,
                        avatar_size=self._avatar_size,
                        parent=self.chat_list,
                    )
                    pixmap, avatar_key = self._chat_list_avatar_payload(cid, info, title)
                    if pixmap is not None:
                        row_widget.set_avatar_cached(pixmap, cache_key=avatar_key)
                    self.chat_list.addItem(item)
                    self.chat_list.setItemWidget(item, row_widget)
                    chat_items_by_id[cid] = item
                    chat_rows_by_id[cid] = row_widget
                    if current_id and cid == current_id:
                        current_item = item

            if current_item:
                self.chat_list.setCurrentItem(current_item)
            if search_text:
                self._apply_filter(search_text)
            self._chat_list_order = ordered_ids
            self._chat_list_data_signature = data_signature
            self._chat_items_by_id = chat_items_by_id
            self._chat_row_widgets_by_id = chat_rows_by_id
        finally:
            self.chat_list.setUpdatesEnabled(True)

    def _refresh_visible_chat_avatars(self) -> None:
        viewport = self.chat_list.viewport()
        visible_rect = viewport.rect() if viewport is not None else None
        count = int(self.chat_list.count() or 0)
        for idx in range(count):
            item = self.chat_list.item(idx)
            if item is None:
                continue
            if visible_rect is not None:
                try:
                    if not self.chat_list.visualItemRect(item).intersects(visible_rect):
                        continue
                except Exception:
                    pass
            cid = str(item.data(Qt.ItemDataRole.UserRole) or "")
            if not cid:
                continue
            info_dict = dict(self.all_chats.get(cid, {}))
            raw_item_info = item.data(Qt.ItemDataRole.UserRole + 1) or {}
            if isinstance(raw_item_info, dict):
                info_dict.update(dict(raw_item_info))
            title = str(info_dict.get("title_display") or info_dict.get("title") or cid)
            pixmap, avatar_key = self._chat_list_avatar_payload(cid, info_dict, title)
            if pixmap is None:
                continue
            row_widget = self.chat_list.itemWidget(item)
            if isinstance(row_widget, ChatListRowWidget):
                row_widget.set_avatar_cached(pixmap, cache_key=avatar_key)
            elif row_widget and hasattr(row_widget, "set_avatar"):
                try:
                    row_widget.set_avatar(pixmap)
                except Exception:
                    pass

    def _chat_list_avatar_payload(
        self,
        chat_id: str,
        info: Dict[str, Any],
        title: str,
    ) -> tuple[Optional[QPixmap], Optional[tuple]]:
        if not hasattr(self, "avatar_cache"):
            return None, None
        if str(chat_id).startswith("__"):
            return None, None
        try:
            photo_id = str(info.get("photo_small_id") or info.get("photo_small") or "")
            avatar_key = ("chat", str(chat_id), photo_id, int(self._avatar_size))
            pixmap = self.avatar_cache.chat(str(chat_id), info)  # type: ignore[attr-defined]
            return pixmap, avatar_key
        except Exception:
            return None, None

    def _populate_chat_list_override(
        self,
        *,
        current_id: Optional[str],
        search_text: str,
        chat_items_by_id: Dict[str, QListWidgetItem],
        chat_rows_by_id: Dict[str, ChatListRowWidget],
    ) -> None:
        rows = list(getattr(self, "_chat_list_override_rows", []) or [])
        mode = str(getattr(self, "_chat_list_override_mode", "") or "")
        row_h = max(32, self._avatar_size + 8)
        visible_rows: List[Dict[str, object]] = []
        signature_rows: List[tuple] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            cid = str(row.get("id") or "").strip()
            if not cid:
                continue
            info = dict(row.get("info") or {})
            title = str(row.get("title") or info.get("title") or cid)
            meta = str(row.get("meta") or "")
            unread = max(0, int(row.get("unread") or info.get("unread_count") or 0))
            info.setdefault("id", cid)
            info.setdefault("title", title)
            info.setdefault("title_display", title)
            info.setdefault("_id_aliases", self._chat_id_aliases(cid))
            info.setdefault("_search_blob", self._build_chat_search_blob(cid, info))
            visible_rows.append(
                {"id": cid, "info": info, "title": title, "meta": meta, "unread": unread}
            )
            signature_rows.append((cid, title, meta, unread, str(info.get("type") or ""), str(info.get("photo_small_id") or info.get("photo_small") or "")))

        data_signature = ("override", mode, tuple(signature_rows))
        if data_signature == getattr(self, "_chat_list_data_signature", None):
            if current_id:
                item = self._chat_items_by_id.get(current_id)
                if item:
                    self.chat_list.setCurrentItem(item)
            if search_text:
                self._apply_filter(search_text)
            return

        self.chat_list.clear()
        current_item: Optional[QListWidgetItem] = None
        for row in visible_rows:
            cid = str(row["id"])
            info = dict(row["info"]) if isinstance(row.get("info"), dict) else {}
            title = str(row.get("title") or cid)
            meta = str(row.get("meta") or "")
            unread = int(row.get("unread") or 0)
            item = QListWidgetItem("")
            item.setData(Qt.ItemDataRole.UserRole, cid)
            item.setData(Qt.ItemDataRole.UserRole + 1, info)
            item.setSizeHint(QSize(270, max(row_h, self._avatar_size + 12)))
            row_widget = ChatListRowWidget(
                title=title,
                meta=meta,
                unread=unread,
                avatar_size=self._avatar_size,
                parent=self.chat_list,
            )
            pixmap, avatar_key = self._chat_list_avatar_payload(cid, info, title)
            if pixmap is not None:
                row_widget.set_avatar_cached(pixmap, cache_key=avatar_key)
            self.chat_list.addItem(item)
            self.chat_list.setItemWidget(item, row_widget)
            chat_items_by_id[cid] = item
            chat_rows_by_id[cid] = row_widget
            if current_id and cid == current_id:
                current_item = item

        if current_item:
            self.chat_list.setCurrentItem(current_item)
        if search_text:
            self._apply_filter(search_text)
        self._chat_list_order = [str(row["id"]) for row in visible_rows]
        self._chat_list_data_signature = data_signature
        self._chat_items_by_id = chat_items_by_id
        self._chat_row_widgets_by_id = chat_rows_by_id

    def _history_has_ai_messages(self, chat_id: str) -> bool:
        cache = getattr(self, "_history_ai_cache", None)
        if not isinstance(cache, dict):
            cache = {}
            self._history_ai_cache = cache
        chats = self.history.get("chats", {})
        bucket = chats.get(chat_id, []) if isinstance(chats, dict) else []
        if not isinstance(bucket, list):
            bucket = []
        size = len(bucket)
        marker = None
        if bucket:
            tail = bucket[-1]
            if isinstance(tail, dict):
                marker = tail.get("id", tail.get("ts"))
        cached = cache.get(chat_id)
        if isinstance(cached, tuple) and len(cached) == 3 and cached[0] == size and cached[1] == marker:
            return bool(cached[2])
        used_ai = any(isinstance(m, dict) and m.get("role") == "assistant" for m in bucket)
        cache[chat_id] = (size, marker, used_ai)
        return used_ai

    def _compose_indicator(self, chat_id: str, *, info: Optional[Dict[str, Any]] = None) -> str:
        flags = None
        if isinstance(info, dict):
            maybe_flags = info.get("_ai_flags")
            if isinstance(maybe_flags, dict):
                flags = maybe_flags
        if flags is None:
            flags = self.server.get_ai_flags(chat_id)
        if isinstance(info, dict) and "_used_ai" in info:
            used_ai = bool(info.get("_used_ai"))
        else:
            used_ai = self._history_has_ai_messages(chat_id)
        if flags.get("auto", False):
            return "🟢"
        if flags.get("ai") is False:
            return "🔴"
        if used_ai:
            return "⚪"
        return ""

    def _update_folder_counts(self, items: List[Dict[str, Any]]) -> None:
        if not getattr(self, "folder_buttons", None):
            return
        specs = getattr(self, "folder_specs", [])
        if not specs:
            return
        counts: Dict[str, int] = {spec.folder_id: 0 for spec in specs}
        for info in items:
            for spec in specs:
                if self._folder_matches(info, spec.folder_id):
                    counts[spec.folder_id] += 1
        self._folder_counts = counts
        for fid, button in self.folder_buttons.items():
            try:
                button.set_count(counts.get(fid, 0))
            except Exception:
                continue

    def _importance_for_sort(self, chat_id: str, *, info: Optional[Dict[str, Any]] = None) -> int:
        indicator = str((info or {}).get("_ai_indicator") or self._compose_indicator(chat_id, info=info))
        return {"🟢": 3, "🔴": 2, "⚪": 1}.get(indicator, 0)

    @staticmethod
    def _folder_matches(info: Dict[str, Any], folder_id: str) -> bool:
        folder_id = folder_id or "all"
        tname = str(info.get("type", "")).lower()
        username = str(info.get("username", "")).lower()
        unread = int(info.get("unread_count") or 0)
        if folder_id == "all":
            return True
        if folder_id == "unread":
            return unread > 0
        if folder_id == "private":
            return tname == "private" and not username.endswith("bot")
        if folder_id == "group":
            return tname in {"group", "supergroup", "megagroup"}
        if folder_id == "channel":
            return tname == "channel"
        if folder_id == "bot":
            return tname == "bot" or username.endswith("bot")
        return True

    @staticmethod
    def _passes_folder_filter(info: Dict[str, Any], active: Optional[str]) -> bool:
        return ChatSidebarMixin._folder_matches(info, active or "all")

    def _history_last_ts(self, chat_id: str) -> int:
        chats = self.history.get("chats", {})
        bucket = chats.get(chat_id)
        if not bucket:
            return 0
        latest = 0
        for entry in bucket:
            if not isinstance(entry, dict):
                continue
            ts = entry.get("ts")
            if ts is None:
                continue
            try:
                latest = max(latest, int(ts))
            except Exception:
                continue
        return latest

    def _chat_sort_key(self, info: Dict[str, Any]) -> tuple:
        cid = str(info.get("id", ""))
        pinned = 1 if info.get("pinned") else 0
        unread = max(0, int(info.get("unread_count") or 0))
        unread_flag = 1 if unread > 0 else 0
        last_ts = int(info.get("last_ts") or 0)
        importance = int(info.get("_sort_importance") or self._importance_for_sort(cid, info=info))
        title = str(info.get("title", "")).lower()
        # Primary order expected by users: pinned first, then recent activity.
        return (-pinned, -last_ts, -unread_flag, -unread, -importance, title)

    @staticmethod
    def _type_icon(chat_type: str) -> str:
        tname = (chat_type or "").lower()
        if tname == "private":
            return "👤"
        if tname == "bot":
            return "🤖"
        if tname == "group":
            return "👥"
        if tname == "supergroup":
            return "👥★"
        if tname == "channel":
            return "📣"
        return "💬"

    @staticmethod
    def _type_label(chat_type: str) -> str:
        tname = (chat_type or "").lower()
        if tname == "private":
            return "Личные"
        if tname == "bot":
            return "Бот"
        if tname in {"group", "supergroup", "megagroup"}:
            return "Группа"
        if tname == "channel":
            return "Канал"
        return ""
