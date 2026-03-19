from __future__ import annotations

from bisect import bisect_left
import ctypes
import json
import mimetypes
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import webbrowser
from typing import Any, Dict, List, Optional, Iterable

from utils import app_paths

from PySide6.QtCore import (
    Qt, Slot, QThread, QTimer, QPoint, QRect, QEvent, QUrl,
    QEasingCurve, QPropertyAnimation, QSequentialAnimationGroup, Property
)
from PySide6.QtGui import QColor, QIcon, QMouseEvent, QWheelEvent, QPixmap, QRegion, QTextCursor, QKeySequence, QShortcut, QPainter
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QCheckBox,
    QHBoxLayout,
    QGraphicsOpacityEffect,
    QInputDialog,
    QLabel,
    QMenu,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QFrame,
    QWidget,
    QSystemTrayIcon,
)
try:
    from shiboken6 import isValid as _qt_is_valid
except Exception:  # pragma: no cover
    def _qt_is_valid(obj: object) -> bool:
        return obj is not None

from utils.zwc import (
    decode_zwc,
    encode_caret_hidden_fragments,
    encode_zwc,
    is_zwc_only,
    contains_zwc,
    reveal_zwc_fragments_with_entities,
)
from utils.text_markup import parse_tg_style_markup
from ui.account_manager import AccountManagerDialog
from ui.auth_dialog import AuthDialog
from ui.avatar_cache import AvatarCache
from ui.chat_sidebar import ChatSidebarMixin
from ui.common import HAVE_QTMULTIMEDIA, HAVE_SD, load_history, log, save_history
from ui.settings_window import SettingsWindow
from ui.auto_download import AutoDownloadPolicy
from ui.config_store import DEFAULT_CONFIG, load_config, save_config
from ui.dialog_workers import (
    BulkAvatarRefreshWorker,
    BulkChatStatisticsWorker,
    ChatProfileLoadWorker,
    DialogsStreamWorker,
    FfmpegInstallWorker,
    GlobalPeerSearchWorker,
    HistoryWorker,
    LastDateWorker,
    ReleaseCheckWorker,
    UpdateDownloadWorker,
    VoiceDepsInstallWorker,
)
from ui.account_workers import AccountProfileWorker
from ui.event_pump import EventPump
from utils.app_meta import get_app_version, get_update_repo, resolve_app_icon_path
from utils.logging_setup import configure_logging, current_log_dir, current_log_files
from ui.message_feed import MessageFeedMixin
import ui.media_render as media_render_module
import ui.message_widgets as message_widgets_module
from ui.message_widgets import (
    DEFAULT_BUBBLE_THEME,
    ChatItemWidget,
    MessageReplyMarkupWidget,
    ReplyPreviewWidget,
    TextMessageWidget,
    set_bubble_theme,
)
from ui.media_viewer import MediaViewerDialog
from ui.media_picker import MediaPickerPopup
from ui.chat_panels import (
    ChatHeaderBar,
    ChatInfoDialog,
    ChatStatisticsDialog,
    MessageStatisticsDialog,
    build_header_menu,
    format_chat_subtitle,
)
from ui.send_media_inline_preview import InlineMediaPreviewBar
from ui.send_media_workers import FfmpegConvertWorker, MediaBatchSendWorker, MediaSendWorker
from ui.styles import StyleManager, apply_theme
from ui.components.avatar import AvatarWidget

try:
    from ui.common import sd, sf  # type: ignore
except ImportError:
    sd = None
    sf = None


_ORPHAN_QT_THREADS: set[QThread] = set()


class ChatInputTextEdit(QTextEdit):
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._placeholder_text = ""

    def setPlaceholderText(self, text: str) -> None:  # type: ignore[override]
        self._placeholder_text = str(text or "")
        super().setPlaceholderText("")
        self.viewport().update()

    def placeholderText(self) -> str:  # type: ignore[override]
        return str(self._placeholder_text or "")

    def paintEvent(self, event) -> None:  # type: ignore[override]
        super().paintEvent(event)
        if self.toPlainText():
            return
        placeholder = str(self._placeholder_text or "").strip()
        if not placeholder:
            return
        painter = QPainter(self.viewport())
        try:
            color = self.palette().color(self.foregroundRole())
            color.setAlpha(120)
            painter.setPen(color)
            try:
                cursor = self.textCursor()
                cursor.movePosition(QTextCursor.MoveOperation.Start)
                caret_rect = self.cursorRect(cursor)
            except Exception:
                caret_rect = QRect(10, 0, 10, int(self.fontMetrics().height()))
            left = max(8, int(caret_rect.left()))
            top = max(0, int(caret_rect.top()))
            draw_rect = QRect(
                left,
                top,
                max(0, self.viewport().width() - left - 8),
                max(int(caret_rect.height()), int(self.fontMetrics().height()) + 4),
            )
            painter.drawText(draw_rect, int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter), placeholder)
        finally:
            painter.end()


class ChatWindow(QWidget, ChatSidebarMixin, MessageFeedMixin):
    def __init__(self, server, tg_adapter, config: Optional[Dict[str, Any]] = None):
        # базовая инициализация
        QWidget.__init__(self)
        self.setWindowTitle("ESCgram")
        icon_path = resolve_app_icon_path()
        if icon_path:
            try:
                self.setWindowIcon(QIcon(icon_path))
            except Exception:
                pass
        self.server = server
        self.tg = tg_adapter
        try:
            custom_emoji_provider = getattr(message_widgets_module, "set_custom_emoji_provider", None)
            if callable(custom_emoji_provider):
                custom_emoji_provider(self.tg)
        except Exception:
            pass
        self._prepend_local_ffmpeg_to_path()

        # конфиг и фичи
        self._config: Dict[str, Any] = config or load_config()
        self._ensure_config_defaults()
        self._apply_ai_config_from_settings(self._config.get("ai") if isinstance(self._config.get("ai"), dict) else {})
        features_cfg = self._config.get("features", {})
        theme_cfg = self._config.get("theme", {})
        self._auto_download_enabled = bool(features_cfg.get("auto_download_media", False))
        self._ghost_mode_enabled    = bool(features_cfg.get("ghost_mode", False))
        self._voice_waveform_enabled = bool(features_cfg.get("voice_waveform", True))
        self._streamer_mode_enabled  = bool(features_cfg.get("streamer_mode", False))
        self._hide_hidden_chats = bool(features_cfg.get("hide_hidden_chats", True))
        self._show_my_avatar_enabled = bool(features_cfg.get("show_my_avatar", True))
        self._keep_deleted_messages = bool(features_cfg.get("keep_deleted_messages", True))
        try:
            volume_raw = int(features_cfg.get("media_volume", 100) or 100)
        except Exception:
            volume_raw = 100
        self._media_volume_percent = max(0, min(100, volume_raw))
        self._apply_media_volume_env()
        self._auto_download_policy = AutoDownloadPolicy.from_config(self._config.get("auto_download"))
        self._theme_mode = str(theme_cfg.get("mode") or "night")
        self._night_mode_enabled = (self._theme_mode == "night")

        # геометрия окна
        window_cfg = self._config.get("window", {})
        width  = int(window_cfg.get("width",  1000) or 1000)
        height = int(window_cfg.get("height",  700) or  700)
        self.resize(width, height)

        # --- анти-мерцание и тёмный фон (убираем белые полосы при анимации) ---
        # используем Qt6-путь с перечислениями WidgetAttribute
        self.setAttribute(Qt.WidgetAttribute.WA_OpaquePaintEvent, True)   # рисуем весь фон сами
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)   # стиль на top-level виджете
        pal = self.palette()
        pal.setColor(self.backgroundRole(), QColor("#0E1621"))            # базовый тёмный
        self.setPalette(pal)
        self.setAutoFillBackground(True)

        # ====== стартовое состояние анимации раскрытия ======
        # tiny-rect от центра, чтобы showEvent мог запустить _animate_open()
        START_W, START_H = 6, 6
        cx, cy = max(0, self.width() // 2), max(0, self.height() // 2)
        self._reveal: QRect = QRect(
            max(0, cx - START_W // 2),
            max(0, cy - START_H // 2),
            START_W,
            START_H,
        )
        # задаём маску сразу, чтобы не было «просветов» до первого тика анимации
        self.setMask(QRegion(self._reveal))
        # ссылки на анимации (чтобы их не «съел» GC)
        self._a1 = None
        self._a2 = None
        self._open_group = None
        self._anim_open_done: bool = False

        # модели/состояния
        self.history: Dict[str, Any] = load_history()
        self._history_limit = 200
        self._history_initial_limit = 30
        self._history_prefetch_limit = 300
        self._history_chunk_size = 120
        self._history_scroll_threshold = 40
        self._history_current_limit = 0
        self._history_auto_scroll_on_finish = True
        self._history_force_top_on_finish = False
        self._history_load_requested_by_scroll = False
        self._history_scroll_enabled = False
        self.all_chats: Dict[str, Dict[str, Any]] = {}
        self._chat_items_by_id: Dict[str, Any] = {}
        self.current_chat_id: Optional[str] = None
        self._my_id: Optional[str] = None
        self._loading_history: bool = False
        self._message_widgets: Dict[int, Any] = {}
        self._message_cache: Dict[int, Dict[str, Any]] = {}
        self._reply_index: Dict[int, List[int]] = {}
        self._chat_last_message_id: Dict[str, int] = {}
        self._selected_message_ids: set[int] = set()
        self._selection_anchor_mid: Optional[int] = None
        self._selection_drag_active: bool = False
        self._selection_last_range_mid: Optional[int] = None
        self._selection_drag_base_ids: set[int] = set()
        self._selection_drag_range_ids: set[int] = set()
        self._runtime_toasts_enabled = bool(int(os.getenv("DRAGO_RUNTIME_TOASTS", "0") or 0))
        self._pending_reply_to: Optional[int] = None
        self._reply_bar: Optional[QFrame] = None
        self._reply_preview_widget: Optional[ReplyPreviewWidget] = None
        self._chat_header: Optional[ChatHeaderBar] = None
        self._chat_header_info_cache: Dict[str, Dict[str, Any]] = {}
        self._media_preview_bar: Optional[InlineMediaPreviewBar] = None
        self._bot_keyboard_bar: Optional[MessageReplyMarkupWidget] = None
        self._bot_reply_markup_by_chat: Dict[str, Optional[Dict[str, Any]]] = {}
        self._pending_media_preview: Optional[Dict[str, Any]] = None
        self._avatar_size = 40
        self._pending_account_revert: Optional[str] = None
        self._startup_auth_retries: int = 0
        self._startup_auth_retry_reason: str = ""
        self._media_popup: Optional[MediaPickerPopup] = None
        self._settings_window: Optional[SettingsWindow] = None
        self._active_account_user_id: str = ""
        self._account_profile_thread: Optional[QThread] = None
        self._media_job_active: bool = False
        self._media_convert_thread: Optional[QThread] = None
        self._media_convert_worker: Optional[FfmpegConvertWorker] = None
        self._media_send_thread: Optional[QThread] = None
        self._chat_details_host: Optional[QFrame] = None
        self._chat_details_header_label: Optional[QLabel] = None
        self._chat_details_container: Optional[QWidget] = None
        self._chat_details_layout: Optional[QVBoxLayout] = None
        self._media_send_worker: Optional[MediaSendWorker] = None
        self._media_batch_thread: Optional[QThread] = None
        self._media_batch_worker: Optional[MediaBatchSendWorker] = None
        self._media_send_reply_to: Optional[int] = None
        self._media_send_reply_preview: Optional[Dict[str, Any]] = None
        self._media_busy_state: Optional[Dict[str, str]] = None
        self._media_busy_last_toast_at: float = 0.0
        self._media_active_tmpdir: Optional[str] = None
        self._local_media_seq: int = 0
        self._input_min_height = 34
        self._input_max_height = 260
        self._pending_reply_updates: set[int] = set()
        self._pending_local_deletes: set[int] = set()
        self._pending_delete_echo_counts: Dict[tuple[str, int], int] = {}
        self._media_group_refresh_pending: bool = False
        self._pending_jump_message_id: Optional[int] = None
        self._jump_retry_count: int = 0
        self._chat_profile_thread: Optional[QThread] = None
        self._chat_profile_worker: Optional[ChatProfileLoadWorker] = None
        self._chat_profile_seq: int = 0
        self._use_global_context_menu = False
        self._app_version = get_app_version()
        self._update_repo = get_update_repo()
        self._latest_version: str = ""
        self._update_download_url: str = ""
        self._update_thread: Optional[QThread] = None
        self._update_worker: Optional[ReleaseCheckWorker] = None
        self._update_download_thread: Optional[QThread] = None
        self._update_download_worker: Optional[UpdateDownloadWorker] = None
        self._update_in_progress: bool = False
        self._media_viewers: set[QWidget] = set()
        self._global_search_query: str = ""
        self._global_search_seq: int = 0
        self._global_search_thread: Optional[QThread] = None
        self._global_search_worker: Optional[GlobalPeerSearchWorker] = None
        self._ffmpeg_install_thread: Optional[QThread] = None
        self._ffmpeg_install_worker: Optional[FfmpegInstallWorker] = None
        self._voice_deps_thread: Optional[QThread] = None
        self._voice_deps_worker: Optional[VoiceDepsInstallWorker] = None
        self._orphan_threads: set[QThread] = set()
        self._tray_icon: Optional[QSystemTrayIcon] = None
        self._tray_menu: Optional[QMenu] = None
        self._tray_quit_requested: bool = False
        self._tray_minimize_notice_shown: bool = False

        # аватары
        self.avatar_cache = AvatarCache(
            self.server, size=self._avatar_size, on_ready=self._on_avatar_ready,
        )

        # долгий клик по «Отправить»
        self._send_hold = QTimer(self)
        self._send_hold.setSingleShot(True)
        self._send_hold.timeout.connect(self._send_long)
        self._send_long_mode = False

        # фоновые потоки
        self._dialogs_threads: set[QThread] = set()
        self._dialogs_workers: set[DialogsStreamWorker] = set()
        self._dialogs_stream_active: bool = False
        self._hist_thread: Optional[QThread] = None
        self._hist_worker: Optional[HistoryWorker] = None
        self._ts_thread: Optional[QThread] = None
        self._ts_worker: Optional[LastDateWorker] = None

        # таймеры UI
        self._resort_timer = QTimer(self)
        self._repaint_timer = QTimer(self)
        self._repaint_timer.setSingleShot(True)
        self._repaint_timer.timeout.connect(self._flush_chat_list_refresh)
        self._chat_list_refresh_pending: bool = False
        self._chat_list_last_refresh_at: float = 0.0
        self._history_save_pending: bool = False
        self._history_save_timer = QTimer(self)
        self._history_save_timer.setSingleShot(True)
        self._history_save_timer.timeout.connect(self._flush_history_save)
        self._ts_warmup_done = False
        self._global_search_timer = QTimer(self)
        self._global_search_timer.setSingleShot(True)
        self._global_search_timer.timeout.connect(self._start_global_peer_search)

        # сборка UI
        self._init_event_pump()
        self._init_ui()
        self._init_system_tray()

        # синхронизация состояния панели настроек
        if hasattr(self, "settings_panel"):
            self.settings_panel.set_auto_download_checked(self._auto_download_enabled)
            self.settings_panel.set_ghost_mode_checked(self._ghost_mode_enabled)
            self.settings_panel.set_voice_waveform_checked(self._voice_waveform_enabled)
            self.settings_panel.set_night_mode_checked(self._night_mode_enabled)
            self.settings_panel.set_streamer_mode_checked(self._streamer_mode_enabled)
            if hasattr(self.settings_panel, "set_show_my_avatar_checked"):
                self.settings_panel.set_show_my_avatar_checked(self._show_my_avatar_enabled)
            if hasattr(self.settings_panel, "update_requested"):
                try:
                    self.settings_panel.update_requested.connect(self._on_update_button_clicked)
                except Exception:
                    pass
            if hasattr(self.settings_panel, "set_update_state"):
                try:
                    self.settings_panel.set_update_state(
                        f"Версия {self._app_version}. Проверка обновлений...",
                        can_update=False,
                        in_progress=True,
                    )
                except Exception:
                    pass
            QTimer.singleShot(1500, self._start_update_check)

        # применяем ghost-mode (если есть поддержка в адаптере)
        try:
            if hasattr(self.tg, "set_ghost_mode"):
                self.tg.set_ghost_mode(self._ghost_mode_enabled)
        except Exception:
            log.exception("Failed to apply initial ghost mode state")

        # полупрозрачный оверлей под панель настроек
        self._settings_overlay = QWidget(self)
        self._settings_overlay.hide()
        self._settings_overlay.setAttribute(Qt.WidgetAttribute.WA_NoSystemBackground, True)
        StyleManager.instance().bind_stylesheet(self._settings_overlay, "main.settings_overlay")
        self._settings_overlay.setGeometry(self.rect())
        self._settings_overlay.mousePressEvent   = self._on_settings_overlay_clicked        # type: ignore[assignment]
        self._settings_overlay.mouseReleaseEvent = self._on_settings_overlay_mouse_release  # type: ignore[assignment]
        self._settings_overlay.mouseMoveEvent    = self._on_settings_overlay_mouse_move     # type: ignore[assignment]
        self._settings_overlay.wheelEvent        = self._on_settings_overlay_wheel          # type: ignore[assignment]
        self._settings_overlay_effect = QGraphicsOpacityEffect(self._settings_overlay)
        self._settings_overlay.setGraphicsEffect(self._settings_overlay_effect)
        self._settings_overlay_effect.setOpacity(0.0)

        # оверлей «Режим стримера»
        self._streamer_overlay = QLabel("Режим стримера активен", self)
        self._streamer_overlay.setAlignment(Qt.AlignmentFlag.AlignCenter)
        StyleManager.instance().bind_stylesheet(self._streamer_overlay, "main.streamer_overlay")
        self._streamer_overlay.hide()

        # голосовые элементы / обновления
        self._init_voice_controls()
        self._init_refresh_timers()

        # применяем тему и стартовые режимы
        self._apply_theme_variant(self._theme_mode)
        self._set_streamer_mode(self._streamer_mode_enabled, persist=False)

        # первичная гидратация/аккаунт
        self._hydrate_cached_dialogs()
        self._sync_account_card()

    def _ensure_config_defaults(self) -> None:
        window_cfg = self._config.setdefault("window", {})
        window_cfg.setdefault("width", 1000)
        window_cfg.setdefault("height", 700)

        theme_cfg = self._config.setdefault("theme", {})
        theme_cfg.setdefault("mode", "night")
        theme_cfg.setdefault("palette", {})
        bubbles = theme_cfg.setdefault("bubbles", {})
        for role in ("me", "assistant", "other"):
            bubbles.setdefault(role, {})

        if "auto_download" not in self._config:
            self._config["auto_download"] = json.loads(json.dumps(DEFAULT_CONFIG.get("auto_download", {})))

        features_cfg = self._config.setdefault("features", {})
        features_cfg.setdefault("auto_download_media", False)
        features_cfg.setdefault("ghost_mode", False)
        features_cfg.setdefault("voice_waveform", True)
        features_cfg.setdefault("streamer_mode", False)
        features_cfg.setdefault("hide_hidden_chats", True)
        features_cfg.setdefault("show_my_avatar", True)
        features_cfg.setdefault("keep_deleted_messages", True)
        features_cfg.setdefault("media_volume", 100)

        ai_cfg = self._config.setdefault("ai", {})
        defaults = DEFAULT_CONFIG.get("ai", {}) if isinstance(DEFAULT_CONFIG.get("ai"), dict) else {}
        ai_cfg.setdefault("model", defaults.get("model", "gemma2"))
        ai_cfg.setdefault("context", defaults.get("context", 2048))
        ai_cfg.setdefault("use_cuda", defaults.get("use_cuda", True))
        ai_cfg.setdefault("prompt", defaults.get("prompt", ""))
        ai_cfg.setdefault("cross_chat_context", defaults.get("cross_chat_context", True))
        ai_cfg.setdefault("cross_chat_limit", defaults.get("cross_chat_limit", 6))

        tools_cfg = self._config.setdefault("tools", {})
        default_tools = DEFAULT_CONFIG.get("tools", {}) if isinstance(DEFAULT_CONFIG.get("tools"), dict) else {}
        for key, raw_defaults in default_tools.items():
            if not isinstance(raw_defaults, dict):
                continue
            bucket = tools_cfg.setdefault(str(key), {})
            for item_key, item_value in raw_defaults.items():
                bucket.setdefault(item_key, item_value)

    def _apply_ai_config_from_settings(self, state: Dict[str, Any], *, reset_model: bool = True) -> None:
        model = str(state.get("model") or "").strip()
        if model:
            os.environ["DRAGO_AI_MODEL"] = model

        ctx = state.get("context")
        try:
            ctx_int = int(ctx) if ctx is not None else None
        except Exception:
            ctx_int = None
        if ctx_int and ctx_int > 0:
            os.environ["DRAGO_NUM_CTX"] = str(ctx_int)
        else:
            os.environ.pop("DRAGO_NUM_CTX", None)

        use_cuda = bool(state.get("use_cuda", True))
        if use_cuda:
            os.environ.pop("DRAGO_FORCE_CPU", None)
            os.environ.pop("DRAGO_NUM_GPU", None)
        else:
            os.environ["DRAGO_FORCE_CPU"] = "1"
            os.environ["DRAGO_NUM_GPU"] = "0"
            os.environ.pop("DRAGO_FORCE_GPU", None)

        cross_chat_context = bool(state.get("cross_chat_context", True))
        os.environ["DRAGO_AI_CROSS_CHAT"] = "1" if cross_chat_context else "0"

        cross_chat_limit = state.get("cross_chat_limit")
        try:
            cross_chat_limit_int = int(cross_chat_limit) if cross_chat_limit is not None else 6
        except Exception:
            cross_chat_limit_int = 6
        cross_chat_limit_int = max(0, min(cross_chat_limit_int, 20))
        os.environ["DRAGO_AI_CROSS_CHAT_LIMIT"] = str(cross_chat_limit_int)

        prompt = str(state.get("prompt") or "")
        try:
            import ai as ai_module

            ai_module.update_prompt_template(prompt)
            if reset_model:
                ai_module.reset_cached_model()
        except Exception:
            log.exception("Failed to apply AI config")

    def _persist_feature_flag(self, key: str, value: bool) -> bool:
        """Safely persist boolean feature toggles to config."""
        features_cfg = self._config.setdefault("features", {})
        previous = bool(features_cfg.get(key, False))
        features_cfg[key] = bool(value)
        try:
            save_config(self._config)
            return True
        except Exception:
            features_cfg[key] = previous
            log.exception("Failed to persist feature flag %s", key)
            return False

    def _apply_media_volume_env(self) -> None:
        try:
            value = int(getattr(self, "_media_volume_percent", 100) or 100)
        except Exception:
            value = 100
        value = max(0, min(100, value))
        os.environ["DRAGO_MEDIA_VOLUME"] = str(value)

    @staticmethod
    def _volume_ratio(percent: int) -> float:
        try:
            value = int(percent)
        except Exception:
            value = 100
        value = max(0, min(100, value))
        return float(value) / 100.0

    def _apply_media_volume_to_active_players(self) -> None:
        ratio = self._volume_ratio(int(getattr(self, "_media_volume_percent", 100)))
        for widget in list(getattr(self, "_message_widgets", {}).values()):
            if widget is None:
                continue
            for attr in ("_audio_output", "_audio"):
                output = getattr(widget, attr, None)
                if output is None:
                    continue
                try:
                    output.setVolume(ratio)
                except Exception:
                    continue
        preview = getattr(self, "_media_preview_bar", None)
        if preview is not None:
            output = getattr(preview, "_audio", None)
            if output is not None:
                try:
                    output.setVolume(ratio)
                except Exception:
                    pass

    def _stop_active_media_playback(self) -> None:
        for widget in list(getattr(self, "_message_widgets", {}).values()):
            if widget is None:
                continue
            for attr in ("player", "_player"):
                player = getattr(widget, attr, None)
                if player is None:
                    continue
                try:
                    player.pause()
                except Exception:
                    pass
                try:
                    player.setSource(QUrl())
                except Exception:
                    pass
        preview = getattr(self, "_media_preview_bar", None)
        if preview is not None:
            stopper = getattr(preview, "stop", None)
            if callable(stopper):
                try:
                    stopper()
                except Exception:
                    pass

    def _apply_config_theme(self) -> None:
        self._apply_theme_variant(self._theme_mode)

    def _apply_theme_variant(self, mode: str) -> None:
        app = QApplication.instance()
        if app is None:
            return
        theme_cfg = self._config.setdefault("theme", {})
        freeze_targets = [
            self,
            getattr(self, "chat_history_wrap", None),
            getattr(self, "chat_list", None),
            getattr(self, "_chat_details_host", None),
        ]
        for widget in freeze_targets:
            if widget is None or not _qt_is_valid(widget):
                continue
            try:
                widget.setUpdatesEnabled(False)
            except Exception:
                continue

        try:
            mgr = StyleManager.instance()
            try:
                mgr.set_active_profile(mode)
            except Exception:
                pass

            extra_palette = theme_cfg.get("palette")
            extra_stylesheet = theme_cfg.get("stylesheet")
            overrides: Dict[str, Any] = {}
            if isinstance(extra_palette, dict):
                overrides["palette"] = dict(extra_palette)
            if isinstance(extra_stylesheet, list):
                overrides["stylesheet"] = list(extra_stylesheet)
            elif isinstance(extra_stylesheet, str) and extra_stylesheet.strip():
                overrides["stylesheet"] = str(extra_stylesheet)
            if overrides:
                apply_theme(app, overrides=overrides)

            bubbles: Dict[str, Dict[str, str]] = {}
            try:
                raw_bubbles = mgr.bubbles()
                if isinstance(raw_bubbles, dict):
                    for role, colors in raw_bubbles.items():
                        if isinstance(colors, dict):
                            bubbles[str(role)] = {str(k): str(v) for k, v in colors.items() if isinstance(k, str) and isinstance(v, str)}
            except Exception:
                bubbles = {}
            if not bubbles:
                bubbles = {role: dict(DEFAULT_BUBBLE_THEME.get(role, DEFAULT_BUBBLE_THEME["other"])) for role in ("me", "assistant", "other")}

            extra_bubbles = theme_cfg.get("bubbles")
            if isinstance(extra_bubbles, dict):
                for role, entry in extra_bubbles.items():
                    if role in bubbles and isinstance(entry, dict):
                        bubbles[role].update({str(k): str(v) for k, v in entry.items() if isinstance(k, str) and isinstance(v, str)})

            set_bubble_theme(bubbles)
            if hasattr(self, "settings_panel"):
                self.settings_panel.set_theme(bubbles)

            self._theme_mode = mode
            self._night_mode_enabled = (mode == "night")
            theme_cfg["mode"] = mode
            try:
                save_config(self._config)
            except Exception:
                log.debug("Не удалось сохранить настройки темы")
            if hasattr(self, "settings_panel"):
                self.settings_panel.set_night_mode_checked(self._night_mode_enabled)
        finally:
            for widget in freeze_targets:
                if widget is None or not _qt_is_valid(widget):
                    continue
                try:
                    widget.setUpdatesEnabled(True)
                    widget.update()
                except Exception:
                    continue

    def _hydrate_cached_dialogs(self) -> None:
        try:
            cached = self.server.list_cached_dialogs(limit=400)
        except Exception:
            cached = []
        if cached:
            self.on_dialogs_batch(cached)

    def _init_voice_controls(self) -> None:
        # Telegram-like behaviour:
        # - hold → record voice
        # - release → send
        # - short click → show actions menu (send audio / video note)
        self._voice_pressing: bool = False
        self._voice_hold = QTimer(self)
        self._voice_hold.setSingleShot(True)
        self._voice_hold.timeout.connect(self._voice_long)
        self._recording = False
        self._rec_stream = None
        self._rec_writer = None
        self._rec_path: Optional[str] = None
        self._recent_outgoing: Dict[str, List[Dict[str, Any]]] = {
            "voice": [],
            "audio": [],
            "video": [],
            "video_note": [],
            "document": [],
        }
        self.btn_voice.pressed.connect(self._voice_pressed)
        self.btn_voice.released.connect(self._voice_released)
        self._update_voice_button()

    def _update_settings_overlay_geometry(self) -> None:
        if self._settings_overlay and self._settings_overlay.isVisible():
            self._settings_overlay.setGeometry(self.rect())

    def _set_settings_overlay_visible(self, visible: bool) -> None:
        if not hasattr(self, "_settings_overlay"):
            return
        if visible:
            self._settings_overlay.setGeometry(self.rect())
            self._settings_overlay.show()
            self._settings_overlay.raise_()
            if hasattr(self, "settings_panel"):
                self.settings_panel.raise_()
        else:
            self._settings_overlay.hide()

    def _on_settings_panel_toggled(self, visible: bool) -> None:
        self._set_settings_overlay_visible(visible)

    def _on_settings_theme_changed(self, colors: Dict[str, str]) -> None:
        theme_cfg = self._config.setdefault("theme", {})
        bubbles_cfg = theme_cfg.setdefault("bubbles", {})
        updated: Dict[str, Dict[str, str]] = {}
        for role, color in colors.items():
            if not isinstance(color, str):
                continue
            qcolor = QColor(color)
            if not qcolor.isValid():
                continue
            defaults = dict(DEFAULT_BUBBLE_THEME.get(role, DEFAULT_BUBBLE_THEME["other"]))
            role_cfg = dict(bubbles_cfg.get(role, defaults))
            role_cfg["bg"] = qcolor.name()
            role_cfg["border"] = QColor(qcolor).darker(120).name()
            updated[role] = role_cfg
            bubbles_cfg[role] = role_cfg

        if updated:
            set_bubble_theme(bubbles_cfg)
            if hasattr(self, "settings_panel"):
                self.settings_panel.set_theme(bubbles_cfg)
            save_config(self._config)

    def _on_settings_animation_progress(self, value: float) -> None:
        if not hasattr(self, "_settings_overlay_effect"):
            return
        progress = max(0.0, min(1.0, float(value)))
        self._settings_overlay_effect.setOpacity(progress * 0.75)
        if progress > 0 and not self._settings_overlay.isVisible():
            self._settings_overlay.show()
        elif progress <= 0:
            self._settings_overlay.hide()

    def _set_night_mode(self, enabled: bool) -> None:
        target = "night" if enabled else "day"
        pending = str(getattr(self, "_pending_theme_mode", "") or "")
        if target == self._theme_mode and pending != target:
            return
        self._pending_theme_mode = target
        timer = getattr(self, "_theme_apply_timer", None)
        if timer is None:
            timer = QTimer(self)
            timer.setSingleShot(True)
            timer.timeout.connect(self._apply_pending_theme_mode)
            self._theme_apply_timer = timer
        try:
            timer.start(45)
        except Exception:
            self._apply_pending_theme_mode()

    def _apply_pending_theme_mode(self) -> None:
        target = str(getattr(self, "_pending_theme_mode", "") or "").strip()
        if not target:
            return
        self._pending_theme_mode = ""
        if target == self._theme_mode:
            return
        self._apply_theme_variant(target)

    def _set_streamer_mode(self, enabled: bool, *, persist: bool = True) -> None:
        enabled = bool(enabled)
        previous = getattr(self, "_streamer_mode_enabled", False)
        if persist:
            if not self._persist_feature_flag("streamer_mode", enabled):
                if hasattr(self, "settings_panel"):
                    self.settings_panel.set_streamer_mode_checked(previous)
                return
        self._streamer_mode_enabled = enabled
        if hasattr(self, "_streamer_overlay"):
            self._streamer_overlay.setGeometry(self.rect())
            self._streamer_overlay.setVisible(enabled)
        if hasattr(self, "settings_panel"):
            self.settings_panel.set_streamer_mode_checked(enabled)

    def _persist_window_config(self) -> None:
        window_cfg = self._config.setdefault("window", {})
        window_cfg["width"] = int(max(100, self.width()))
        window_cfg["height"] = int(max(100, self.height()))

    # ===== АНИМАЦИЯ РАСКРЫТИЯ ОКНА =====

    def getRevealRect(self) -> QRect:
        return QRect(self._reveal)

    def setRevealRect(self, r: QRect) -> None:
        # +1/-1 по всем краям: убираем 1px щели на HiDPI и целочисленной интерполяции QRect
        r = QRect(r.adjusted(-1, -1, 1, 1))
        self._reveal = r
        self.setMask(QRegion(self._reveal))
        # немедленно перерисовываем только нужную область (быстрее и без «белых» кадров)
        self.update(self._reveal)

    revealRect = Property(QRect, getRevealRect, setRevealRect)

    def _animate_open(self, vert_ms: int = 600, horz_ms: int = 600) -> None:
        W, H = self.width(), self.height()
        start_w = self._reveal.width()
        start_h = self._reveal.height()

        vert_start = QRect((W - start_w) // 2, (H - start_h) // 2, start_w, start_h)
        vert_end = QRect((W - start_w) // 2, 0, start_w, H)
        a1 = QPropertyAnimation(self, b"revealRect")
        a1.setDuration(vert_ms)
        a1.setStartValue(vert_start)
        a1.setEndValue(vert_end)
        a1.setEasingCurve(QEasingCurve.Type.OutCubic)

        horz_start = vert_end
        horz_end = QRect(0, 0, W, H)
        a2 = QPropertyAnimation(self, b"revealRect")
        a2.setDuration(horz_ms)
        a2.setStartValue(horz_start)
        a2.setEndValue(horz_end)
        a2.setEasingCurve(QEasingCurve.Type.OutCubic)

        group = QSequentialAnimationGroup(self)
        group.addAnimation(a1)
        group.addAnimation(a2)
        group.finished.connect(self.clearMask)
        group.finished.connect(lambda: self.update())

        # <<< важное: не даём GC убить анимации во время проигрывания >>>
        self._a1, self._a2, self._open_group = a1, a2, group
        group.start()

    def showEvent(self, e) -> None:
        super().showEvent(e)
        if not getattr(self, "_anim_open_done", False):
            self._anim_open_done = True
            self._animate_open(vert_ms=600, horz_ms=600)

    def _on_settings_overlay_clicked(self, event) -> None:
        """Скрыть настройки только если клик вне панели."""
        if self._forward_overlay_event(event):
            return
        panel = getattr(self, "settings_panel", None)
        overlay = getattr(self, "_settings_overlay", None)
        if panel and overlay and panel.isVisible():
            try:
                global_pos = overlay.mapToGlobal(event.position().toPoint())  # type: ignore[attr-defined]
                panel_rect = QRect(panel.mapToGlobal(QPoint(0, 0)), panel.size())
                if panel_rect.contains(global_pos):
                    mapped = panel.mapFromGlobal(global_pos)
                    proxy_event = QMouseEvent(
                        event.type(),
                        mapped,
                        panel.mapFromGlobal(global_pos),
                        event.button(),
                        event.buttons(),
                        event.modifiers(),
                    )
                    QApplication.sendEvent(panel, proxy_event)
                    return
            except Exception:
                pass
        try:
            event.accept()
        except Exception:
            pass
        menu_btn = getattr(self, "menu_button", None)
        if menu_btn:
            menu_btn.setChecked(False)
        else:
            self._toggle_settings_panel(False)

    def _on_settings_overlay_mouse_release(self, event) -> None:
        if self._forward_overlay_event(event):
            return
        QWidget.mouseReleaseEvent(self._settings_overlay, event)

    def _on_settings_overlay_mouse_move(self, event) -> None:
        if self._forward_overlay_event(event):
            return
        QWidget.mouseMoveEvent(self._settings_overlay, event)

    def _on_settings_overlay_wheel(self, event: QWheelEvent) -> None:
        if self._forward_overlay_event(event):
            return
        event.ignore()

    def _forward_overlay_event(self, event) -> bool:
        panel = getattr(self, "settings_panel", None)
        overlay = getattr(self, "_settings_overlay", None)
        if not (panel and overlay and panel.isVisible()):
            return False
        try:
            local = event.position().toPoint()  # type: ignore[attr-defined]
        except Exception:
            try:
                local = event.pos()
            except Exception:
                return False
        global_pos = overlay.mapToGlobal(local)
        panel_rect = QRect(panel.mapToGlobal(QPoint(0, 0)), panel.size())
        if not panel_rect.contains(global_pos):
            return False
        mapped = panel.mapFromGlobal(global_pos)
        if isinstance(event, QMouseEvent):
            proxy = QMouseEvent(
                event.type(),
                mapped,
                event.globalPosition(),
                event.button(),
                event.buttons(),
                event.modifiers(),
            )
            QApplication.sendEvent(panel, proxy)
            return True
        if isinstance(event, QWheelEvent):
            proxy = QWheelEvent(
                mapped,
                event.globalPosition(),
                event.pixelDelta(),
                event.angleDelta(),
                event.buttons(),
                event.modifiers(),
                event.phase(),
                event.inverted(),
                event.source(),
            )
            QApplication.sendEvent(panel, proxy)
            return True
        return False

    def resizeEvent(self, e) -> None:
        """Держим оверлей на весь размер окна при ресайзе."""
        super().resizeEvent(e)
        try:
            if hasattr(self, "_settings_overlay") and self._settings_overlay:
                self._settings_overlay.setGeometry(self.rect())
            if hasattr(self, "_streamer_overlay") and self._streamer_overlay:
                self._streamer_overlay.setGeometry(self.rect())
            if hasattr(self, "_reposition_settings_panel"):
                self._reposition_settings_panel()
        except Exception:
            pass

    def _init_event_pump(self) -> None:
        self.pump = EventPump(self.server)
        self.pump_thread = QThread(self)
        self.pump_thread.setObjectName("pump_thread")
        self.pump.moveToThread(self.pump_thread)
        self.pump_thread.started.connect(self.pump.run)
        self.pump.gui_ai_message.connect(self._on_ai_msg)
        self.pump.gui_user_echo.connect(self._on_user_echo)
        self.pump.gui_message_sent.connect(self._on_gui_message_sent)
        self.pump.gui_peer_message.connect(self._on_peer_message)
        self.pump.gui_media.connect(self._on_gui_media)
        self.pump.gui_media_progress.connect(self._on_media_progress)
        self.pump.gui_touch_dialog.connect(self._on_touch_dialog)
        self.pump.gui_messages_deleted.connect(self._on_gui_messages_deleted)
        self.pump_thread.start()

    def _init_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(self._build_sidebar())

        center = QWidget()
        center_layout = QVBoxLayout(center)
        center_layout.setContentsMargins(0, 0, 0, 0)
        center_layout.setSpacing(0)

        center_body = QWidget(center)
        center_body_layout = QHBoxLayout(center_body)
        center_body_layout.setContentsMargins(0, 0, 0, 0)
        center_body_layout.setSpacing(0)

        chat_area = QWidget(center_body)
        chat_area_layout = QVBoxLayout(chat_area)
        chat_area_layout.setContentsMargins(0, 0, 0, 0)
        chat_area_layout.setSpacing(0)
        chat_area_layout.addWidget(self._build_chat_header(), 0)
        chat_area_layout.addWidget(self._build_feed(), 1)
        try:
            self.chat_scroll.viewport().installEventFilter(self)
        except Exception:
            pass
        QTimer.singleShot(0, self._apply_bubble_widths)

        bottom_wrap = QWidget()
        bottom_layout = QVBoxLayout(bottom_wrap)
        bottom_layout.setContentsMargins(0, 0, 0, 0)
        bottom_layout.setSpacing(6)
        bottom_layout.addWidget(self._build_reply_bar(), 0)
        bottom_layout.addWidget(self._build_media_preview_bar(), 0)
        bottom_layout.addWidget(self._build_bot_keyboard_bar(), 0)
        bottom_layout.addLayout(self._build_bottom_row())
        chat_area_layout.addWidget(bottom_wrap, 0)

        center_body_layout.addWidget(chat_area, 1)
        center_body_layout.addWidget(self._build_chat_details_host(), 0)
        center_layout.addWidget(center_body, 1)

        splitter.addWidget(center)
        splitter.setStretchFactor(1, 1)
        root.addWidget(splitter, 1)

        if self.tg.is_authorized_sync():
            QTimer.singleShot(1000, self.refresh_telegram_chats_async)
        else:
            QTimer.singleShot(0, lambda: self._ensure_authorized(prompt_reason="startup"))

        self._shortcut_find = QShortcut(QKeySequence.Find, self)
        self._shortcut_find.activated.connect(self.show_message_search)
        self._shortcut_find_next = QShortcut(QKeySequence.FindNext, self)
        self._shortcut_find_next.activated.connect(lambda: self.find_in_messages(forward=True))
        self._shortcut_find_prev = QShortcut(QKeySequence.FindPrevious, self)
        self._shortcut_find_prev.activated.connect(lambda: self.find_in_messages(forward=False))
        self._shortcut_find_close = QShortcut(QKeySequence("Esc"), self)
        self._shortcut_find_close.activated.connect(self.hide_message_search)

    def _build_chat_header(self) -> QWidget:
        header = ChatHeaderBar(self)
        header.infoRequested.connect(self._show_current_chat_info)
        header.menuRequested.connect(self._show_chat_header_menu)
        self._chat_header = header
        self._refresh_chat_header()
        return header

    def eventFilter(self, obj, event):  # type: ignore[override]
        try:
            handled = ChatSidebarMixin.eventFilter(self, obj, event)
        except Exception:
            handled = False
        if handled:
            return True

        viewport = getattr(getattr(self, "chat_scroll", None), "viewport", lambda: None)()
        if viewport is not None and obj is viewport and event.type() == QEvent.Type.ContextMenu:
            try:
                pos = event.pos()
            except Exception:
                pos = None
            if pos is not None:
                widget = self._find_message_widget_at(viewport, pos)
                if widget is not None:
                    try:
                        global_pos = viewport.mapToGlobal(pos)
                    except Exception:
                        global_pos = None
                    if global_pos is not None:
                        self._show_message_context_menu(widget, global_pos)
                        return True
        if viewport is not None and obj is viewport and event.type() == QEvent.Type.MouseButtonPress:
            if (
                str(getattr(self, "_feed_scroll_lock_mode", "") or "").strip().lower() == "top"
                and not bool(getattr(self, "_loading_history", False))
            ):
                self._feed_scroll_lock_mode = ""
                self._feed_autostick_block_until = 0.0
            try:
                button = event.button()
            except Exception:
                button = None
            if button == Qt.MouseButton.LeftButton:
                try:
                    pos = event.position().toPoint()
                except Exception:
                    try:
                        pos = event.pos()
                    except Exception:
                        pos = None
                if pos is not None:
                    widget = self._find_message_widget_at(viewport, pos)
                    mid = self._message_id_from_widget(widget) if widget is not None else None
                    if mid is not None and int(mid) > 0 and self._selected_message_ids:
                        start_mid = int(mid)
                        self._selection_drag_active = True
                        self._selection_drag_base_ids = set(self._selected_message_ids)
                        self._selection_drag_range_ids = set()
                        self._selection_anchor_mid = start_mid
                        self._extend_message_selection_range(start_mid)
                        return True
                    if mid is None and self._selected_message_ids:
                        self._selection_drag_active = False
                        self._selection_anchor_mid = None
                        self._selection_last_range_mid = None
                        self._selection_drag_base_ids.clear()
                        self._selection_drag_range_ids.clear()
        if viewport is not None and obj is viewport and event.type() == QEvent.Type.MouseMove:
            if self._selection_drag_active and (QApplication.mouseButtons() & Qt.MouseButton.LeftButton):
                try:
                    pos = event.position().toPoint()
                except Exception:
                    try:
                        pos = event.pos()
                    except Exception:
                        pos = None
                if pos is not None:
                    widget = self._find_message_widget_at(viewport, pos)
                    mid = self._message_id_from_widget(widget) if widget is not None else None
                    if mid is not None and int(mid) > 0:
                        self._extend_message_selection_range(int(mid))
        if viewport is not None and obj is viewport and event.type() == QEvent.Type.Wheel:
            if (
                str(getattr(self, "_feed_scroll_lock_mode", "") or "").strip().lower() == "top"
                and not bool(getattr(self, "_loading_history", False))
            ):
                self._feed_scroll_lock_mode = ""
                self._feed_autostick_block_until = 0.0
            if self._selection_drag_active and (QApplication.mouseButtons() & Qt.MouseButton.LeftButton):
                QTimer.singleShot(0, self._extend_selection_to_cursor)
        if viewport is not None and obj is viewport and event.type() == QEvent.Type.MouseButtonRelease:
            try:
                button = event.button()
            except Exception:
                button = None
            if button == Qt.MouseButton.LeftButton:
                self._selection_drag_active = False
                self._selection_drag_base_ids.clear()
                self._selection_drag_range_ids.clear()
        if viewport is not None and obj is viewport and event.type() == QEvent.Type.Resize:
            self._schedule_bubble_width_update()
        return False

    def _find_message_widget_at(self, viewport: QWidget, pos: QPoint) -> Optional[QWidget]:
        wrap = getattr(self, "chat_history_wrap", None)
        if wrap is None:
            return None
        try:
            global_pos = viewport.mapToGlobal(pos)
            local = wrap.mapFromGlobal(global_pos)
            target = wrap.childAt(local)
        except Exception:
            target = None
        node = target
        while node is not None:
            if self._message_id_from_widget(node) is not None:
                return node
            try:
                node = node.parent()
            except Exception:
                node = None
        return None

    def _schedule_bubble_width_update(self) -> None:
        timer = getattr(self, "_bubble_width_timer", None)
        if timer is None:
            timer = QTimer(self)
            timer.setSingleShot(True)
            timer.timeout.connect(self._apply_bubble_widths)
            self._bubble_width_timer = timer
        try:
            timer.start(0)
        except Exception:
            pass

    def _bubble_max_width(self) -> int:
        viewport = getattr(getattr(self, "chat_scroll", None), "viewport", lambda: None)()
        vw = int(viewport.width()) if viewport is not None else int(self.width())
        ratio = float(StyleManager.instance().metric("message_widgets.metrics.bubble_max_ratio", 0.54) or 0.54)
        maxw = int(vw * ratio)
        return max(280, min(920, maxw))

    def _apply_bubble_widths(self) -> None:
        maxw = self._bubble_max_width()
        seen: set[int] = set()
        widgets: list = []
        try:
            widgets.extend(list(getattr(self, "_message_order", []) or []))
        except Exception:
            pass
        try:
            widgets.extend(list(getattr(self, "_message_widgets", {}).values() or []))
        except Exception:
            pass

        for widget in widgets:
            if widget is None:
                continue
            if id(widget) in seen:
                continue
            seen.add(id(widget))
            for attr in ("bubble", "_caption_bubble"):
                bubble = getattr(widget, attr, None)
                if isinstance(bubble, QWidget):
                    try:
                        bubble.setMaximumWidth(maxw)
                    except Exception:
                        pass

    def _build_reply_bar(self) -> QWidget:
        bar = QFrame()
        bar.setObjectName("replyBar")
        bar.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        StyleManager.instance().bind_stylesheet(bar, "main.reply_bar")

        layout = QHBoxLayout(bar)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(8)

        preview = ReplyPreviewWidget(bar)
        layout.addWidget(preview, 1)

        btn_close = QToolButton(bar)
        btn_close.setText("✕")
        btn_close.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_close.setToolTip("Отменить ответ")
        btn_close.clicked.connect(self._clear_reply_target)
        StyleManager.instance().bind_stylesheet(btn_close, "main.reply_bar_close")
        layout.addWidget(btn_close, 0, Qt.AlignmentFlag.AlignTop)

        bar.hide()
        self._reply_bar = bar
        self._reply_preview_widget = preview
        return bar

    def _build_media_preview_bar(self) -> QWidget:
        bar = InlineMediaPreviewBar(self)
        bar.cancelRequested.connect(lambda: self._cancel_pending_media_preview(toast=True))
        bar.sendRequested.connect(self._confirm_pending_media_send)
        bar.hide()
        self._media_preview_bar = bar
        return bar

    def _build_bot_keyboard_bar(self) -> QWidget:
        bar = MessageReplyMarkupWidget("reply", self)
        bar.buttonActivated.connect(self._handle_reply_markup_action)
        bar.hide()
        self._bot_keyboard_bar = bar
        return bar

    def _build_chat_details_host(self) -> QWidget:
        host = QFrame(self)
        host.setObjectName("chatDetailsHost")
        host.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        host.setFixedWidth(438)
        host.setStyleSheet(
            "QFrame#chatDetailsHost{background-color:#102033;border-left:1px solid rgba(255,255,255,0.06);}"
            "QPushButton{background-color:rgba(255,255,255,0.05);color:#dfe7f5;border:none;border-radius:14px;padding:6px 10px;}"
            "QPushButton:hover{background-color:rgba(255,255,255,0.11);}"
            "QLabel#chatDetailsHeader{color:#f4f7ff;font-size:16px;font-weight:700;}"
        )
        layout = QVBoxLayout(host)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        header = QFrame(host)
        header.setObjectName("chatDetailsHeaderBar")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(12, 10, 12, 10)
        header_layout.setSpacing(8)
        btn_back = QPushButton("←", header)
        btn_back.setFixedWidth(40)
        btn_back.clicked.connect(self._close_chat_details)
        header_layout.addWidget(btn_back, 0)
        title = QLabel("Профиль", header)
        title.setObjectName("chatDetailsHeader")
        header_layout.addWidget(title, 1)
        btn_close = QPushButton("✕", header)
        btn_close.setFixedWidth(40)
        btn_close.clicked.connect(self._close_chat_details)
        header_layout.addWidget(btn_close, 0)
        layout.addWidget(header, 0)

        scroll = QScrollArea(host)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setStyleSheet("QScrollArea{background:transparent;border:none;}QScrollArea>QWidget>QWidget{background:transparent;}")
        layout.addWidget(scroll, 1)

        container = QWidget(scroll)
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.setSpacing(0)
        scroll.setWidget(container)

        host.hide()
        self._chat_details_host = host
        self._chat_details_scroll = scroll
        self._chat_details_header_label = title
        self._chat_details_container = container
        self._chat_details_layout = container_layout
        return host

    def _set_chat_details_widget(self, title: str, widget: Optional[QWidget]) -> None:
        header = getattr(self, "_chat_details_header_label", None)
        if header is not None:
            header.setText(str(title or "Профиль"))
        layout = getattr(self, "_chat_details_layout", None)
        host = getattr(self, "_chat_details_host", None)
        if layout is None or host is None:
            return
        while layout.count():
            item = layout.takeAt(0)
            child = item.widget()
            if child is not None:
                child.setParent(None)
                child.deleteLater()
        if widget is None:
            host.hide()
            return
        widget.setParent(host)
        widget.setWindowFlags(Qt.WindowType.Widget)
        layout.addWidget(widget, 1)
        widget.show()
        host.show()
        host.raise_()

    def _close_chat_details(self) -> None:
        self._set_chat_details_widget("", None)

    def _update_bot_keyboard_bar(self) -> None:
        bar = getattr(self, "_bot_keyboard_bar", None)
        if bar is None:
            return
        chat_id = str(self.current_chat_id or "")
        markup = self._bot_reply_markup_by_chat.get(chat_id) if chat_id else None
        default_placeholder = str(getattr(self, "_default_input_placeholder", "Сообщение...") or "Сообщение...")
        placeholder = default_placeholder
        markup_type = str(markup.get("type") or "").strip().lower() if isinstance(markup, dict) else ""
        custom_placeholder = str(markup.get("placeholder") or "").strip() if isinstance(markup, dict) else ""
        if markup_type == "reply":
            bar.set_markup(markup)
            bar.show()
            placeholder = custom_placeholder or "Сообщение боту..."
        else:
            bar.clear_buttons()
            bar.hide()
            if markup_type == "force_reply":
                placeholder = custom_placeholder or "Бот ожидает ваш ответ..."
        try:
            self.user_input.setPlaceholderText(placeholder)
        except Exception:
            pass

    def _consume_one_time_keyboard(self, chat_id: str) -> None:
        markup = self._bot_reply_markup_by_chat.get(chat_id)
        if isinstance(markup, dict) and bool(markup.get("one_time_keyboard")):
            self._bot_reply_markup_by_chat[chat_id] = None
            if chat_id == str(self.current_chat_id or ""):
                self._update_bot_keyboard_bar()

    def _show_inline_media_preview(
        self,
        kind: str,
        *,
        chat_id: str,
        src_path: str,
        tmpdir: str,
        out_path: str,
    ) -> bool:
        bar = getattr(self, "_media_preview_bar", None)
        if bar is None:
            return False
        try:
            preview_path = out_path
            if str(kind or "").strip().lower() == "voice":
                # QtMultimedia on Windows often cannot decode OGG/Opus (Telegram voice notes),
                # so preview the original source file instead of the converted .ogg.
                preview_path = src_path
            bar.set_media(kind, preview_path, src_path)
            bar.show()
            self._pending_media_preview = {
                "kind": str(kind or "").strip().lower(),
                "chat_id": str(chat_id or ""),
                "src_path": str(src_path or ""),
                "tmpdir": str(tmpdir or ""),
                "out_path": str(out_path or ""),
            }
            try:
                self.user_input.setFocus()
            except Exception:
                pass
            return True
        except Exception:
            log.exception("Failed to show inline media preview")
            try:
                bar.hide()
            except Exception:
                pass
            self._pending_media_preview = None
            return False

    def _cancel_pending_media_preview(self, *, toast: bool = False) -> None:
        ctx = getattr(self, "_pending_media_preview", None)
        if not ctx:
            # Nothing pending; just ensure the widget is hidden/stopped.
            bar = getattr(self, "_media_preview_bar", None)
            if bar is not None:
                try:
                    bar.stop()
                    bar.hide()
                except Exception:
                    pass
            return

        self._pending_media_preview = None
        bar = getattr(self, "_media_preview_bar", None)
        if bar is not None:
            try:
                bar.stop()
                bar.hide()
            except Exception:
                pass

        tmpdir = str(ctx.get("tmpdir") or "")
        self._safe_cleanup_temp_dir(tmpdir)
        self._media_active_tmpdir = None
        self._media_job_active = False
        if toast:
            self._toast("Отправка отменена")

    @Slot()
    def _confirm_pending_media_send(self) -> None:
        ctx = getattr(self, "_pending_media_preview", None)
        if not ctx:
            return
        self._pending_media_preview = None
        bar = getattr(self, "_media_preview_bar", None)
        if bar is not None:
            try:
                bar.stop()
                bar.hide()
            except Exception:
                pass
        try:
            self._send_prepared_media(
                str(ctx.get("kind") or ""),
                str(ctx.get("chat_id") or ""),
                str(ctx.get("src_path") or ""),
                str(ctx.get("tmpdir") or ""),
                str(ctx.get("out_path") or ""),
            )
        except Exception:
            log.exception("Failed to confirm media send from inline preview")

    def _build_bottom_row(self) -> QHBoxLayout:
        bottom_row = QHBoxLayout()
        bottom_row.setContentsMargins(0, 8, 0, 8)
        bottom_row.setSpacing(6)

        self.btn_attach = QToolButton()
        self.btn_attach.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_attach.setText("📎")
        self.btn_attach.setToolTip("Отправить файлы")
        self.btn_attach.clicked.connect(self._pick_files_and_send)
        bottom_row.addWidget(self.btn_attach)

        self.user_input = ChatInputTextEdit()
        self.user_input.setMinimumHeight(self._input_min_height)
        self.user_input.setMaximumHeight(self._input_max_height)
        self.user_input.setStyleSheet(
            "font-family:'Segoe UI Emoji','Noto Color Emoji','Apple Color Emoji','Segoe UI',sans-serif;"
        )
        self._default_input_placeholder = "Сообщение..."
        self.user_input.setPlaceholderText(self._default_input_placeholder)
        self.user_input.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.user_input.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.user_input.customContextMenuRequested.connect(self._show_input_context_menu)
        self.user_input.textChanged.connect(self._adjust_input_height)
        try:
            self.user_input.document().setDocumentMargin(4.0)
        except Exception:
            pass
        bottom_row.addWidget(self.user_input, 1)

        self.btn_media = QToolButton()
        self.btn_media.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_media.setText("😊")
        self.btn_media.setToolTip("Эмодзи / стикеры / GIF")
        self.btn_media.clicked.connect(self._toggle_media_picker)
        bottom_row.addWidget(self.btn_media)

        self.btn_send = QPushButton("Отправить")
        self._send_button_label = self.btn_send.text()
        self.btn_send.setAutoDefault(False)
        self.btn_send.pressed.connect(self._send_pressed)
        self.btn_send.released.connect(self._send_released)
        bottom_row.addWidget(self.btn_send)

        self.auto_ai_checkbox = QCheckBox("🤖")
        self.auto_ai_checkbox.setToolTip("Автоответ AI для текущего чата")
        self.auto_ai_checkbox.stateChanged.connect(self.on_auto_ai_changed)
        bottom_row.addWidget(self.auto_ai_checkbox)

        self.btn_voice = QToolButton()
        self.btn_voice.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_voice.setContextMenuPolicy(Qt.ContextMenuPolicy.ActionsContextMenu)
        voice_action = self.btn_voice.addAction("Отправить аудио…")
        voice_action.triggered.connect(self._pick_mp3_and_send)
        video_action = self.btn_voice.addAction("Отправить кружок…")
        video_action.triggered.connect(self._pick_video_note_and_send)
        bottom_row.addWidget(self.btn_voice)

        QTimer.singleShot(0, self._adjust_input_height)

        return bottom_row

    @Slot()
    def _adjust_input_height(self) -> None:
        editor = getattr(self, "user_input", None)
        if not isinstance(editor, QTextEdit):
            return
        try:
            doc_h = int(editor.document().size().height())
        except Exception:
            doc_h = int(editor.fontMetrics().lineSpacing())
        frame = int(editor.frameWidth() * 2)
        margins = editor.contentsMargins()
        target = doc_h + frame + int(margins.top()) + int(margins.bottom()) + 4
        target = max(int(self._input_min_height), min(int(self._input_max_height), int(target)))
        try:
            editor.setFixedHeight(target)
        except Exception:
            pass
        need_scroll = target >= int(self._input_max_height)
        editor.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded if need_scroll else Qt.ScrollBarPolicy.ScrollBarAlwaysOff
        )

    @staticmethod
    def _classify_attachment_kind(path: str) -> str:
        ext = os.path.splitext(str(path or ""))[1].lower()
        mime = str(mimetypes.guess_type(path)[0] or "").lower()
        if ext in {".gif"}:
            return "animation"
        if mime.startswith("image/") or ext in {".jpg", ".jpeg", ".png", ".bmp"}:
            return "image"
        if mime.startswith("video/") or ext in {".mp4", ".mov", ".m4v", ".mkv", ".webm", ".avi"}:
            return "video"
        if mime.startswith("audio/") or ext in {".mp3", ".m4a", ".flac", ".wav", ".aac", ".ogg", ".oga", ".opus"}:
            if ext in {".ogg", ".oga", ".opus"}:
                return "voice"
            return "audio"
        return "document"

    @Slot()
    def _pick_files_and_send(self) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            QMessageBox.information(self, "Файлы", "Сначала выберите чат.")
            return
        if self._media_job_in_progress():
            self._toast("Дождитесь завершения текущей отправки")
            return

        paths, _ = QFileDialog.getOpenFileNames(
            self,
            "Выбрать файлы",
            "",
            "Media files (*.jpg *.jpeg *.png *.bmp *.gif *.mp4 *.mov *.m4v *.mkv *.webm *.avi *.mp3 *.wav *.m4a *.flac *.ogg *.oga *.opus);;All files (*.*)",
        )
        selected = [str(p) for p in (paths or []) if p and os.path.isfile(p)]
        if not selected:
            return

        caption_text = str(self.user_input.toPlainText() or "").strip()
        prepared: List[Dict[str, Any]] = []
        for src in selected:
            kind = self._classify_attachment_kind(src)
            stable_path = src
            try:
                size = int(os.path.getsize(src))
            except Exception:
                size = 0
            try:
                cached_path, cached_size = self._cache_outgoing_media(kind, src)
                if cached_path and os.path.isfile(cached_path):
                    stable_path = cached_path
                    size = int(cached_size or size)
            except Exception:
                pass
            prepared.append(
                {
                    "kind": kind,
                    "path": stable_path,
                    "size": int(size or 0),
                    "caption": "",
                }
            )

        if not prepared:
            return
        if caption_text:
            prepared[0]["caption"] = caption_text

        reply_to = getattr(self, "_pending_reply_to", None)
        reply_preview = self._build_reply_preview(reply_to)
        self._media_send_reply_to = int(reply_to) if reply_to is not None else None
        self._media_send_reply_preview = reply_preview

        local_group_id = f"local-{int(time.time() * 1000)}" if len(prepared) > 1 else None
        pending_ids: List[Optional[int]] = []
        now_ts = int(time.time())
        for idx, item in enumerate(prepared):
            local_id = self._add_pending_local_media_widget(
                kind=str(item["kind"]),
                chat_id=chat_id,
                media_path=str(item["path"]),
                file_size=int(item["size"] or 0),
                caption_text=str(item.get("caption") or "") if idx == 0 else "",
                media_group_id=local_group_id,
                timestamp=now_ts,
            )
            pending_ids.append(local_id)

        if caption_text:
            try:
                self.user_input.clear()
            except Exception:
                pass
        self._clear_reply_target()
        self._start_media_batch_send(chat_id=chat_id, items=prepared, pending_ids=pending_ids)

    def _start_media_batch_send(
        self,
        *,
        chat_id: str,
        items: List[Dict[str, Any]],
        pending_ids: List[Optional[int]],
    ) -> None:
        if not items:
            return
        try:
            timeout = max(180.0, 70.0 * float(len(items)))
        except Exception:
            timeout = 300.0
        self._start_media_busy("Файлы", "Отправляю файлы…")
        self._media_job_active = True
        self._media_batch_ctx = {
            "chat_id": str(chat_id or ""),
            "pending_ids": list(pending_ids),
            "items": [dict(x) for x in items],
        }
        try:
            thread = QThread(self)
            thread.setObjectName("media_batch_thread")
            worker = MediaBatchSendWorker(
                self.tg,
                chat_id=str(chat_id or ""),
                items=list(items),
                reply_to=self._media_send_reply_to,
                timeout_sec=timeout,
            )
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            worker.done.connect(self._on_media_batch_done_payload)
            worker.done.connect(thread.quit)
            worker.done.connect(worker.deleteLater)
            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(self._clear_media_batch_refs)
            self._media_batch_thread = thread
            self._media_batch_worker = worker
            thread.start()
        except Exception:
            log.exception("Failed to start media batch send worker")
            self._media_batch_ctx = None
            self._stop_media_busy()
            self._media_job_active = False
            self._media_send_reply_to = None
            self._media_send_reply_preview = None
            for pid in pending_ids:
                if pid is not None:
                    self._remove_message_widget(pid)
            self._toast("Не удалось запустить отправку файлов")

    @Slot(dict)
    def _on_media_batch_done_payload(self, payload: Dict[str, Any]) -> None:
        ctx = getattr(self, "_media_batch_ctx", None)
        self._media_batch_ctx = None
        self._stop_media_busy()
        self._media_job_active = False
        self._media_send_reply_to = None
        self._media_send_reply_preview = None
        if not isinstance(ctx, dict):
            return
        chat_id = str(ctx.get("chat_id") or "")
        pending_ids = list(ctx.get("pending_ids") or [])
        self._on_media_batch_done(chat_id=chat_id, pending_ids=pending_ids, payload=dict(payload or {}))

    def _on_media_batch_done(
        self,
        *,
        chat_id: str,
        pending_ids: List[Optional[int]],
        payload: Dict[str, Any],
    ) -> None:
        results_raw = payload.get("results")
        results: List[Dict[str, Any]] = [dict(r) for r in results_raw if isinstance(r, dict)] if isinstance(results_raw, list) else []
        best_by_index: Dict[int, Dict[str, Any]] = {}
        for row in results:
            try:
                idx = int(row.get("index"))
            except Exception:
                continue
            prev = best_by_index.get(idx)
            if prev is None or (not bool(prev.get("ok")) and bool(row.get("ok"))):
                best_by_index[idx] = row

        mids_raw = payload.get("message_ids")
        message_ids: List[int] = []
        if isinstance(mids_raw, list):
            for mid in mids_raw:
                try:
                    as_int = int(mid)
                except Exception:
                    continue
                if as_int > 0:
                    message_ids.append(as_int)

        promoted: set[int] = set()
        for idx, mid in enumerate(message_ids):
            if idx >= len(pending_ids):
                break
            pid = pending_ids[idx]
            if pid is None:
                continue
            widget = self._message_widgets.get(int(pid))
            if widget is None:
                continue
            self._promote_local_widget_message_id(widget, msg_id=int(mid), timestamp=int(time.time()))
            promoted.add(idx)

        failed_indices = {idx for idx, row in best_by_index.items() if not bool(row.get("ok"))}
        if not best_by_index and not bool(payload.get("ok")):
            failed_indices = {idx for idx in range(len(pending_ids))}
        for idx, pid in enumerate(pending_ids):
            if pid is None:
                continue
            if idx in failed_indices:
                self._remove_message_widget(int(pid))
                continue

        self._refresh_media_group_layout()

        if bool(payload.get("ok")):
            self._toast("Файлы отправлены")
            return
        err = str(payload.get("error") or "").strip()
        if err:
            self._toast(err)
        else:
            self._toast("Часть файлов не отправилась")

    def _on_message_widget_created(self, widget: QWidget, msg_id: Optional[int], chat_id: Optional[str]) -> None:
        try:
            maxw = self._bubble_max_width()
        except Exception:
            return
        for attr in ("bubble", "_caption_bubble"):
            bubble = getattr(widget, attr, None)
            if isinstance(bubble, QWidget):
                try:
                    bubble.setMaximumWidth(maxw)
                except Exception:
                    pass
        if isinstance(widget, ChatItemWidget):
            try:
                widget.on_media_activate = self._on_media_widget_activate
            except Exception:
                pass
        for signal_name, handler in (
            ("commandActivated", self._handle_message_command),
            ("replyMarkupButtonActivated", self._handle_reply_markup_action),
        ):
            signal = getattr(widget, signal_name, None)
            if signal is None:
                continue
            try:
                signal.connect(handler)
            except Exception:
                pass

    @Slot(str)
    def _handle_message_command(self, command: str) -> None:
        chat_id = str(self.current_chat_id or "")
        text = str(command or "").strip()
        if not chat_id or not text:
            return
        self.server.gui_send_message(chat_id=chat_id, user_id="me", text=text)

    @Slot(dict)
    def _handle_reply_markup_action(self, payload: Dict[str, Any]) -> None:
        action = dict(payload or {})
        url = str(action.get("url") or action.get("web_app_url") or action.get("login_url") or "").strip()
        if url:
            try:
                QDesktopServices.openUrl(QUrl(url))
            except Exception:
                pass
            return
        switch_query = str(action.get("switch_inline_query_current_chat") or action.get("switch_inline_query") or "").strip()
        if switch_query:
            try:
                self.user_input.insertPlainText(switch_query)
                self.user_input.setFocus()
            except Exception:
                pass
            return
        chat_id = str(action.get("chat_id") or self.current_chat_id or "").strip()
        if not chat_id:
            return
        if bool(action.get("request_contact")):
            profile_getter = getattr(self.tg, "get_self_profile_sync", None)
            sender = getattr(self.tg, "send_contact_sync", None)
            if not callable(profile_getter) or not callable(sender):
                self._toast("Отправка контакта недоступна")
                return
            profile = dict(profile_getter() or {})
            phone = str(profile.get("phone") or "").strip()
            first_name = str(profile.get("first_name") or profile.get("username") or "Контакт").strip()
            last_name = str(profile.get("last_name") or "").strip()
            if not phone:
                QMessageBox.warning(self, "Контакт", "В текущем аккаунте Telegram нет номера телефона.")
                return
            ok = bool(
                sender(
                    chat_id=chat_id,
                    phone_number=phone,
                    first_name=first_name,
                    last_name=last_name or None,
                )
            )
            if ok:
                self._consume_one_time_keyboard(chat_id)
                self._toast("Контакт отправлен")
            else:
                self._toast("Не удалось отправить контакт")
            return
        if bool(action.get("request_location")):
            sender = getattr(self.tg, "send_location_sync", None)
            if not callable(sender):
                self._toast("Отправка геопозиции недоступна")
                return
            coords_text, ok = QInputDialog.getText(
                self,
                "Геопозиция",
                "Введите широту и долготу через запятую.\nПример: 55.751244, 37.618423",
            )
            if not ok:
                return
            parts = [part.strip() for part in str(coords_text or "").split(",")]
            if len(parts) != 2:
                QMessageBox.warning(self, "Геопозиция", "Нужно указать две координаты: широту и долготу.")
                return
            try:
                latitude = float(parts[0])
                longitude = float(parts[1])
            except Exception:
                QMessageBox.warning(self, "Геопозиция", "Координаты должны быть числами.")
                return
            ok_send = bool(sender(chat_id=chat_id, latitude=latitude, longitude=longitude))
            if ok_send:
                self._consume_one_time_keyboard(chat_id)
                self._toast("Геопозиция отправлена")
            else:
                self._toast("Не удалось отправить геопозицию")
            return
        if action.get("request_poll") or action.get("request_users") or action.get("request_chat"):
            self._toast("Этот тип bot-кнопки пока не реализован полностью")
            return
        try:
            inline_message_id = int(action.get("message_id") or 0)
        except Exception:
            inline_message_id = 0
        try:
            inline_row = int(action.get("row") or 0)
            inline_col = int(action.get("col") or 0)
        except Exception:
            inline_row = -1
            inline_col = -1
        if inline_message_id > 0 and inline_row >= 0 and inline_col >= 0:
            result = self.server.press_inline_button(
                chat_id,
                inline_message_id,
                inline_row,
                inline_col,
            )
            if bool(result.get("ok")):
                text = str(result.get("text") or "").strip()
                if text:
                    self._toast(text)
            else:
                err = str(result.get("error") or "").strip()
                if err:
                    self._toast(err)
            return
        text = str(action.get("text") or "").strip()
        if text:
            self.server.gui_send_message(chat_id=chat_id, user_id="me", text=text)
            self._consume_one_time_keyboard(chat_id)

    def _on_media_widget_activate(self, payload: Dict[str, Any]) -> bool:
        kind = str(payload.get("kind") or "").strip().lower()
        file_path = str(payload.get("file_path") or "").strip()
        thumb_path = str(payload.get("thumb_path") or "").strip()
        path = str(payload.get("path") or "").strip()
        candidate = path
        if not candidate:
            if file_path and os.path.isfile(file_path):
                candidate = file_path
            elif thumb_path and os.path.isfile(thumb_path):
                candidate = thumb_path

        if kind in {"video", "video_note"}:
            # Video viewer requires the media file itself; thumbnail-only fallback is not enough.
            if not file_path or not os.path.isfile(file_path):
                return False
            candidate = file_path
        elif not candidate or not os.path.isfile(candidate):
            return False

        try:
            viewer = MediaViewerDialog(media_path=candidate, kind=kind, parent=self)
            self._media_viewers.add(viewer)
            viewer.destroyed.connect(lambda *_args, w=viewer: self._media_viewers.discard(w))
            viewer.setGeometry(self.rect())
            viewer.show()
            viewer.raise_()
            viewer.activateWindow()
            return True
        except Exception:
            return False

    def _init_refresh_timers(self) -> None:
        self._timer = QTimer(self)
        self._timer.timeout.connect(self.refresh_telegram_chats_async)
        self._timer.start(5 * 60_000)

        self._resort_timer.setInterval(5 * 60_000)
        self._resort_timer.timeout.connect(self.populate_chat_list)
        self._resort_timer.start()

    def _schedule_chat_list_refresh(self, delay_ms: int = 120) -> None:
        self._chat_list_refresh_pending = True
        # Coalesce frequent updates to avoid expensive full chat-list rebuilds on bursts.
        now = time.monotonic()
        if bool(getattr(self, "_dialogs_stream_active", False)):
            min_gap = 0.90
        else:
            min_gap = 0.30
        elapsed = now - float(getattr(self, "_chat_list_last_refresh_at", 0.0) or 0.0)
        effective_delay = max(0, int(delay_ms))
        if bool(getattr(self, "_dialogs_stream_active", False)):
            effective_delay = max(effective_delay, 220)
        if elapsed < min_gap:
            effective_delay = max(effective_delay, int((min_gap - elapsed) * 1000))
        try:
            self._repaint_timer.start(effective_delay)
        except Exception:
            self._chat_list_refresh_pending = False
            self.populate_chat_list()

    @Slot()
    def _flush_chat_list_refresh(self) -> None:
        if not getattr(self, "_chat_list_refresh_pending", False):
            return
        self._chat_list_refresh_pending = False
        self._chat_list_last_refresh_at = time.monotonic()
        self.populate_chat_list()

    def _ensure_chat_meta(self, chat_id: str) -> Dict[str, Any]:
        info = dict(self.all_chats.get(chat_id, {}))
        if not info:
            info = {
                "title": chat_id,
                "type": "private",
                "last_ts": 0,
                "unread_count": 0,
                "pinned": False,
            }
        info.setdefault("title", chat_id)
        info.setdefault("type", "private")
        info["last_ts"] = int(info.get("last_ts") or 0)
        info["unread_count"] = max(0, int(info.get("unread_count") or 0))
        info["pinned"] = bool(info.get("pinned", False))
        return info

    def _apply_chat_activity(
        self,
        chat_id: str,
        *,
        ts: Optional[int] = None,
        unread_delta: int = 0,
        clear_unread: bool = False,
        refresh_delay_ms: Optional[int] = None,
    ) -> None:
        if not chat_id:
            return
        info = self._ensure_chat_meta(chat_id)
        prev_last_ts = int(info.get("last_ts") or 0)
        prev_unread = int(info.get("unread_count") or 0)
        if ts is not None:
            info["last_ts"] = max(int(info.get("last_ts") or 0), int(ts or 0))
        if clear_unread:
            info["unread_count"] = 0
        elif unread_delta:
            info["unread_count"] = max(0, int(info.get("unread_count") or 0) + int(unread_delta))
        self.all_chats[chat_id] = info
        changed = (int(info.get("last_ts") or 0) != prev_last_ts) or (int(info.get("unread_count") or 0) != prev_unread)
        if refresh_delay_ms is not None:
            if changed or clear_unread or unread_delta:
                self._schedule_chat_list_refresh(refresh_delay_ms)

    def _mark_message_seen(self, chat_id: str, msg_id: Optional[int]) -> bool:
        if not chat_id or msg_id is None:
            return False
        try:
            mid = int(msg_id)
        except Exception:
            return False
        if mid <= 0:
            return False
        last = int(self._chat_last_message_id.get(chat_id, 0) or 0)
        if mid > last:
            self._chat_last_message_id[chat_id] = mid
            return True
        return False

    def _stop_global_search_worker(self) -> None:
        worker = getattr(self, "_global_search_worker", None)
        thread = getattr(self, "_global_search_thread", None)
        self._global_search_worker = None
        self._global_search_thread = None
        if worker is not None and hasattr(worker, "stop"):
            try:
                worker.stop()
            except Exception:
                pass
        if thread is not None and thread.isRunning():
            try:
                thread.quit()
                thread.wait(220)
            except Exception:
                pass

    def on_sidebar_search_changed(self, text: str) -> bool:
        query = str(text or "")
        self._apply_filter(query)
        stripped = query.strip()
        self._global_search_query = stripped
        if not stripped or len(stripped) < 2:
            timer = getattr(self, "_global_search_timer", None)
            if timer is not None:
                try:
                    timer.stop()
                except Exception:
                    pass
            self._stop_global_search_worker()
            return True
        timer = getattr(self, "_global_search_timer", None)
        if timer is not None:
            try:
                timer.start(260)
            except Exception:
                self._start_global_peer_search()
        else:
            self._start_global_peer_search()
        return True

    @Slot()
    def _start_global_peer_search(self) -> None:
        query = str(getattr(self, "_global_search_query", "") or "").strip()
        if not query:
            return
        if not hasattr(self.tg, "is_authorized_sync") or not self.tg.is_authorized_sync():
            return
        self._stop_global_search_worker()
        self._global_search_seq = int(getattr(self, "_global_search_seq", 0) or 0) + 1
        seq = self._global_search_seq
        try:
            thread = QThread(self)
            thread.setObjectName("global_peer_search_thread")
            worker = GlobalPeerSearchWorker(self.server, query=query, limit=36)
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            worker.done.connect(lambda rows, s=seq, q=query: self._on_global_peer_search_done(rows, s, q))
            worker.done.connect(thread.quit)
            worker.done.connect(worker.deleteLater)
            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(lambda: setattr(self, "_global_search_worker", None))
            thread.finished.connect(lambda: setattr(self, "_global_search_thread", None))
            self._global_search_worker = worker
            self._global_search_thread = thread
            thread.start()
        except Exception:
            log.exception("Failed to start global peer search worker")
            self._global_search_worker = None
            self._global_search_thread = None

    def _on_global_peer_search_done(self, rows: List[Dict[str, Any]], seq: int, query: str) -> None:
        try:
            if int(seq) != int(getattr(self, "_global_search_seq", 0) or 0):
                return
        except Exception:
            return
        current_query = str(getattr(self, "search", None).text() if hasattr(self, "search") else "")
        if current_query.strip().lower() != str(query or "").strip().lower():
            return
        updated = False
        for row in list(rows or []):
            if not isinstance(row, dict):
                continue
            cid = str(row.get("id") or "").strip()
            if not cid:
                continue
            prev = self._ensure_chat_meta(cid)
            title = str(row.get("title") or prev.get("title") or cid).strip() or cid
            ctype = str(row.get("type") or prev.get("type") or "private").strip().lower() or "private"
            username = str(row.get("username") or prev.get("username") or "").strip()
            photo_small = row.get("photo_small_id") or prev.get("photo_small_id") or prev.get("photo_small")
            self.all_chats[cid] = {
                "title": title,
                "type": ctype,
                "last_ts": int(prev.get("last_ts") or 0),
                "username": username,
                "photo_small_id": photo_small,
                "pinned": bool(prev.get("pinned", False)),
                "unread_count": max(0, int(prev.get("unread_count") or 0)),
            }
            updated = True
        if updated:
            self._schedule_chat_list_refresh(0)
        self._apply_filter(current_query)

    # ------------------------------------------------------------------ #
    # Chat list interactions

    def on_chat_list_clicked(self, item) -> None:
        chat_id = str(item.data(Qt.ItemDataRole.UserRole) or "").strip()
        if not chat_id:
            return
        if chat_id == "__back__":
            if hasattr(self, "clear_chat_list_override"):
                self.clear_chat_list_override()
            return
        if getattr(self, "_chat_list_override_mode", ""):
            if hasattr(self, "clear_chat_list_override"):
                self.clear_chat_list_override()
        self.switch_chat(chat_id)

    def switch_chat(self, chat_id: str) -> None:
        if chat_id == self.current_chat_id:
            return
        # If a send-preview is open for the previous chat, close it and cleanup temp files.
        try:
            self._cancel_pending_media_preview(toast=False)
        except Exception:
            pass
        try:
            self._stop_active_media_playback()
        except Exception:
            pass
        self._message_cache.clear()
        self._reply_index.clear()
        self._clear_message_selection()
        self._stop_chat_profile_loader()
        self._pending_jump_message_id = None
        self._jump_retry_count = 0
        self._history_force_top_on_finish = False
        self.current_chat_id = chat_id
        self._close_chat_details()
        self._apply_chat_activity(chat_id, refresh_delay_ms=0)
        self._refresh_chat_header()
        self._update_bot_keyboard_bar()
        self.update_ai_controls_state()
        self.load_chat_history_async(auto_scroll=True)

    def _current_chat_meta(self) -> Dict[str, Any]:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return {}
        info = dict(self.all_chats.get(chat_id, {}))
        cached = self._chat_header_info_cache.get(chat_id)
        if isinstance(cached, dict):
            info.update({key: value for key, value in cached.items() if value not in (None, "")})
        info.setdefault("id", chat_id)
        info.setdefault("title", info.get("title") or chat_id)
        return info

    def _refresh_chat_header(self) -> None:
        header = getattr(self, "_chat_header", None)
        if not header:
            return
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            header.set_chat(title="Выберите чат", subtitle="", avatar=None)
            return
        info = self._current_chat_meta()
        title = str(info.get("title") or chat_id)
        subtitle = format_chat_subtitle(info)
        avatar = None
        try:
            avatar = self.avatar_cache.chat(chat_id, info)
        except Exception:
            avatar = None
        header.set_chat(title=title, subtitle=subtitle, avatar=avatar)

    def _show_current_chat_info(self) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        info = self._current_chat_meta()
        avatar = None
        try:
            avatar = self.avatar_cache.chat(chat_id, info)
        except Exception:
            avatar = None
        callbacks = {
            "show_stats": self._show_current_chat_statistics,
            "mark_read": lambda: self._mark_current_chat_read(local=False),
            "leave_chat": self._leave_current_chat_from_profile,
            "jump_to_message": self._jump_to_chat_message,
            "load_profile_section": lambda section, chat=chat_id: self._load_chat_profile_section(chat, section),
        }
        panel = ChatInfoDialog(
            info,
            avatar=avatar,
            sections={},
            callbacks=callbacks,
            parent=self,
            embedded=True,
        )
        self._set_chat_details_widget("Профиль", panel)

    def _build_chat_profile_loading_widget(self) -> QWidget:
        box = QFrame(self)
        box.setStyleSheet("QFrame{background:transparent;border:none;} QLabel{background:transparent;border:none;}")
        layout = QVBoxLayout(box)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(8)
        title = QLabel("Загрузка профиля…", box)
        title.setStyleSheet("color:#dfefff;font-size:15px;font-weight:700;")
        note = QLabel("Подгружаю медиа, файлы, ссылки и участников в фоне.", box)
        note.setWordWrap(True)
        note.setStyleSheet("color:#8da8c4;font-size:12px;")
        layout.addWidget(title, 0)
        layout.addWidget(note, 0)
        layout.addStretch(1)
        return box

    def _stop_chat_profile_loader(self) -> None:
        worker = getattr(self, "_chat_profile_worker", None)
        thread = getattr(self, "_chat_profile_thread", None)
        self._chat_profile_worker = None
        self._chat_profile_thread = None
        if worker is not None and hasattr(worker, "stop"):
            try:
                worker.stop()
            except Exception:
                pass
        if thread is not None:
            try:
                if _qt_is_valid(thread) and thread.isRunning():
                    thread.quit()
                    thread.wait(350)
            except Exception:
                pass

    def _start_chat_profile_loader(self, chat_id: str) -> None:
        cid = str(chat_id or "")
        if not cid:
            return
        self._stop_chat_profile_loader()
        self._chat_profile_seq += 1
        seq = int(self._chat_profile_seq)
        thread = QThread(self)
        thread.setObjectName("chat_profile_load_thread")
        worker = ChatProfileLoadWorker(
            self.server,
            cid,
            media_limit=90,
            file_limit=90,
            link_limit=140,
            members_limit=100,
        )
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.done.connect(lambda chat, full, sections, s=seq: self._on_chat_profile_loaded(chat, full, sections, s))
        worker.done.connect(thread.quit)
        worker.done.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda: setattr(self, "_chat_profile_worker", None))
        thread.finished.connect(lambda: setattr(self, "_chat_profile_thread", None))
        self._chat_profile_worker = worker
        self._chat_profile_thread = thread
        thread.start()

    def _load_chat_profile_section(self, chat_id: str, section: str) -> List[Dict[str, Any]]:
        cid = str(chat_id or "").strip()
        key = str(section or "").strip().lower()
        if not cid or not key:
            return []
        limits = {
            "media": 90,
            "files": 90,
            "links": 140,
            "members": 100,
        }
        limit = int(limits.get(key, 80) or 80)
        getter = getattr(self.server, "get_chat_profile_section", None)
        if callable(getter):
            try:
                rows = getter(cid, key, limit=limit) or []
                return [dict(row) for row in list(rows or []) if isinstance(row, dict)]
            except Exception:
                return []
        getter = getattr(self.server, "get_chat_profile_sections", None)
        if callable(getter):
            try:
                payload = getter(
                    cid,
                    media_limit=limit if key == "media" else 1,
                    file_limit=limit if key == "files" else 1,
                    link_limit=limit if key == "links" else 1,
                    members_limit=limit if key == "members" else 1,
                ) or {}
                rows = payload.get(key) if isinstance(payload, dict) else []
                return [dict(row) for row in list(rows or []) if isinstance(row, dict)]
            except Exception:
                return []
        return []

    def _on_chat_profile_loaded(
        self,
        chat_id: str,
        full_info: Dict[str, Any],
        sections: Dict[str, Any],
        seq: int,
    ) -> None:
        if int(seq) != int(getattr(self, "_chat_profile_seq", 0) or 0):
            return
        cid = str(chat_id or "")
        if not cid:
            return
        if cid != str(self.current_chat_id or ""):
            return
        host = getattr(self, "_chat_details_host", None)
        header = getattr(self, "_chat_details_header_label", None)
        if host is None or not bool(host.isVisible()):
            return
        if header is not None and str(header.text() or "").strip().lower() not in {"профиль", "мой профиль"}:
            return
        info = self._current_chat_meta()
        full = dict(full_info or {})
        if full:
            info.update(full)
            self._chat_header_info_cache[cid] = dict(full)
            merged = dict(self.all_chats.get(cid, {}))
            merged.update(full)
            self.all_chats[cid] = merged
            self._refresh_chat_header()
        avatar = None
        try:
            avatar = self.avatar_cache.chat(cid, info)
        except Exception:
            avatar = None
        callbacks = {
            "show_stats": self._show_current_chat_statistics,
            "mark_read": lambda: self._mark_current_chat_read(local=False),
            "leave_chat": self._leave_current_chat_from_profile,
            "jump_to_message": self._jump_to_chat_message,
        }
        panel = ChatInfoDialog(
            info,
            avatar=avatar,
            sections=dict(sections or {}),
            callbacks=callbacks,
            parent=self,
            embedded=True,
        )
        self._set_chat_details_widget("Профиль", panel)

    def _leave_current_chat_from_profile(self) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        info = dict(self.all_chats.get(chat_id, {}))
        title = str(info.get("title") or chat_id)
        reply = QMessageBox.question(
            self,
            "Покинуть чат",
            f"Покинуть «{title}»?\nЭто действие выполнится в Telegram.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        ok = False
        if hasattr(self.server, "leave_chat"):
            try:
                ok = bool(self.server.leave_chat(chat_id))
            except Exception:
                ok = False
        if not ok:
            QMessageBox.warning(self, "Покинуть чат", "Не удалось покинуть чат.")
            return
        self._toast("Чат покинут")
        self._close_chat_details()
        self.all_chats.pop(chat_id, None)
        self.current_chat_id = None
        self.clear_feed()
        self._refresh_chat_header()
        self.populate_chat_list()
        QTimer.singleShot(100, self.refresh_telegram_chats_async)

    def _show_current_chat_statistics(self, *, scan: bool = False) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        limit = max(int(getattr(self, "_history_prefetch_limit", 300) or 300), 300)
        stats = self.server.scan_chat_statistics(chat_id, limit=0 if scan else limit) if scan else self.server.get_chat_statistics(chat_id, limit=limit)
        if not stats:
            self._toast("Нет данных для статистики")
            return
        if scan:
            self._toast("Скан завершён")
        title = str(self._current_chat_meta().get("title") or chat_id)
        panel = ChatStatisticsDialog(
            title,
            stats,
            parent=self,
            embedded=True,
            callbacks={"scan": lambda: self._show_current_chat_statistics(scan=True)},
        )
        self._set_chat_details_widget("Статистика", panel)

    def _show_message_statistics(self, message_id: int) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        try:
            mid = int(message_id)
        except Exception:
            return
        if mid <= 0:
            return
        data = self.server.get_message_statistics(chat_id, mid)
        if not data:
            self._toast("Статистика недоступна")
            return
        panel = MessageStatisticsDialog(data, parent=self, embedded=True)
        self._set_chat_details_widget("Статистика сообщения", panel)

    def _scroll_to_message_widget(self, msg_id: int) -> bool:
        try:
            mid = int(msg_id)
        except Exception:
            return False
        if mid <= 0:
            return False
        widget = self._message_widgets.get(mid)
        if widget is None:
            return False
        target = getattr(widget, "_row_wrap", None) or widget
        try:
            self.chat_scroll.ensureWidgetVisible(target, 0, 120)
            return True
        except Exception:
            return False

    def _jump_to_chat_message(self, message_id: int) -> None:
        try:
            mid = int(message_id)
        except Exception:
            return
        if mid <= 0 or not self.current_chat_id:
            return
        if self._scroll_to_message_widget(mid):
            return
        self._pending_jump_message_id = mid
        self._jump_retry_count = 0
        current_limit = int(getattr(self, "_history_current_limit", 0) or 0)
        min_limit = max(
            int(getattr(self, "_history_prefetch_limit", 300) or 300),
            int(getattr(self, "_history_initial_limit", 80) or 80),
            500,
        )
        target_limit = max(current_limit, min_limit)
        self.load_chat_history_async(reset=False, limit=target_limit, auto_scroll=False)
        self._toast("Подгружаю историю к сообщению…")

    def _resolve_pending_message_jump(self) -> None:
        pending = getattr(self, "_pending_jump_message_id", None)
        if pending is None:
            return
        try:
            target_mid = int(pending)
        except Exception:
            self._pending_jump_message_id = None
            return
        if self._scroll_to_message_widget(target_mid):
            self._pending_jump_message_id = None
            self._jump_retry_count = 0
            return
        current_limit = int(getattr(self, "_history_current_limit", 0) or 0)
        hard_cap = 5000
        if current_limit >= hard_cap:
            self._pending_jump_message_id = None
            self._jump_retry_count = 0
            self._toast("Сообщение не найдено в локальной истории")
            return
        retries = int(getattr(self, "_jump_retry_count", 0) or 0)
        if retries >= 8:
            self._pending_jump_message_id = None
            self._jump_retry_count = 0
            self._toast("Не удалось быстро перейти к сообщению")
            return
        chunk = max(180, int(getattr(self, "_history_chunk_size", 120) or 120))
        next_limit = min(hard_cap, current_limit + chunk)
        self._jump_retry_count = retries + 1
        self.load_chat_history_async(reset=False, limit=next_limit, auto_scroll=False)

    @Slot(QPoint)
    def _show_chat_header_menu(self, global_pos: QPoint) -> None:
        if not self.current_chat_id:
            return
        menu = build_header_menu(self)
        menu.addAction("Открыть профиль").triggered.connect(self._show_current_chat_info)
        menu.addAction("Статистика чата").triggered.connect(self._show_current_chat_statistics)
        menu.addAction("Анти-накрутка").triggered.connect(self._show_current_chat_statistics)
        menu.addSeparator()
        menu.addAction("Обновить историю").triggered.connect(lambda: self.load_chat_history_async(reset=True))
        menu.addAction("Пометить прочитанным").triggered.connect(lambda: self._mark_current_chat_read(local=False))
        menu.addAction("Покинуть чат").triggered.connect(self._leave_current_chat_from_profile)
        try:
            menu.exec(global_pos)
        finally:
            try:
                menu.deleteLater()
            except Exception:
                pass

    def update_ai_controls_state(self) -> None:
        if not self.current_chat_id:
            self.auto_ai_checkbox.blockSignals(True)
            self.auto_ai_checkbox.setChecked(False)
            self.auto_ai_checkbox.setEnabled(False)
            self.auto_ai_checkbox.blockSignals(False)
            if hasattr(self, "settings_panel"):
                self.settings_panel.set_controls_enabled(False)
                self.settings_panel.sync_flags(enabled=False, auto=False)
            return

        flags = self.server.get_ai_flags(self.current_chat_id)
        ai_enabled = bool(flags.get("ai", True))
        auto_enabled = bool(flags.get("auto", False))

        self.auto_ai_checkbox.blockSignals(True)
        self.auto_ai_checkbox.setEnabled(ai_enabled)
        self.auto_ai_checkbox.setChecked(auto_enabled and ai_enabled)
        self.auto_ai_checkbox.blockSignals(False)

        if hasattr(self, "settings_panel"):
            self.settings_panel.set_controls_enabled(True)
            self.settings_panel.sync_flags(enabled=ai_enabled, auto=auto_enabled)

    def on_auto_download_setting_changed(self, enabled: bool) -> None:
        previous = getattr(self, "_auto_download_enabled", False)
        desired = bool(enabled)
        if previous == desired:
            return
        self._auto_download_enabled = desired
        if not self._persist_feature_flag("auto_download_media", desired):
            self._auto_download_enabled = previous
            panel = getattr(self, "settings_panel", None)
            if panel:
                panel.set_auto_download_checked(previous)
            self._toast("Не удалось сохранить настройку автозагрузки")
            return
        if self._auto_download_enabled:
            self._apply_auto_download_to_feed()

    def on_ghost_mode_setting_changed(self, enabled: bool) -> None:
        previous = getattr(self, "_ghost_mode_enabled", False)
        desired = bool(enabled)
        if previous == desired:
            return
        self._ghost_mode_enabled = desired
        if not self._persist_feature_flag("ghost_mode", desired):
            self._ghost_mode_enabled = previous
            panel = getattr(self, "settings_panel", None)
            if panel:
                panel.set_ghost_mode_checked(previous)
            self._toast("Не удалось сохранить режим призрака")
            return
        try:
            if hasattr(self.tg, "set_ghost_mode"):
                self.tg.set_ghost_mode(self._ghost_mode_enabled)
        except Exception:
            log.exception("Failed to toggle ghost mode")
            self._ghost_mode_enabled = previous
            self._persist_feature_flag("ghost_mode", previous)
            panel = getattr(self, "settings_panel", None)
            if panel:
                panel.set_ghost_mode_checked(previous)
            self._toast("Не удалось переключить режим призрака")
            return
        toast_msg = "Режим призрака включен" if self._ghost_mode_enabled else "Режим призрака выключен"
        self._toast(toast_msg)

    def on_voice_waveform_setting_changed(self, enabled: bool) -> None:
        previous = getattr(self, "_voice_waveform_enabled", True)
        desired = bool(enabled)
        if desired and not HAVE_QTMULTIMEDIA:
            self._toast("QtMultimedia недоступен — дорожка голосовых отключена")
            panel = getattr(self, "settings_panel", None)
            if panel:
                panel.set_voice_waveform_checked(previous)
            return
        if previous == desired:
            return
        self._voice_waveform_enabled = desired
        if not self._persist_feature_flag("voice_waveform", desired):
            self._voice_waveform_enabled = previous
            panel = getattr(self, "settings_panel", None)
            if panel:
                panel.set_voice_waveform_checked(previous)
            self._toast("Не удалось сохранить настройку дорожки голосовых")
            return
        self._refresh_voice_wave_widgets()
        toast_msg = "Улучшенная дорожка голосовых включена" if self._voice_waveform_enabled else "Улучшенная дорожка голосовых отключена"
        self._toast(toast_msg)

    def on_hide_hidden_chats_setting_changed(self, enabled: bool) -> None:
        previous = getattr(self, "_hide_hidden_chats", True)
        desired = bool(enabled)
        if previous == desired:
            return
        self._hide_hidden_chats = desired
        if not self._persist_feature_flag("hide_hidden_chats", desired):
            self._hide_hidden_chats = previous
            self._toast("Не удалось сохранить настройку скрытых чатов")
            return
        self.populate_chat_list()

    def on_show_my_avatar_setting_changed(self, enabled: bool) -> None:
        previous = getattr(self, "_show_my_avatar_enabled", True)
        desired = bool(enabled)
        if previous == desired:
            return
        self._show_my_avatar_enabled = desired
        if not self._persist_feature_flag("show_my_avatar", desired):
            self._show_my_avatar_enabled = previous
            panel = getattr(self, "settings_panel", None)
            if panel and hasattr(panel, "set_show_my_avatar_checked"):
                panel.set_show_my_avatar_checked(previous)
            self._toast("Не удалось сохранить настройку аватара")
            return
        if self.current_chat_id:
            try:
                self.clear_feed()
            except Exception:
                pass
            self._message_cache.clear()
            self._reply_index.clear()
            self.load_chat_history_async()

    def on_keep_deleted_messages_setting_changed(self, enabled: bool) -> None:
        previous = getattr(self, "_keep_deleted_messages", True)
        desired = bool(enabled)
        if previous == desired:
            return
        self._keep_deleted_messages = desired
        if not self._persist_feature_flag("keep_deleted_messages", desired):
            self._keep_deleted_messages = previous
            self._toast("Не удалось сохранить настройку удалённых сообщений")
            return
            if self.current_chat_id:
                self.load_chat_history_async()

    def on_media_volume_setting_changed(self, value: int) -> None:
        try:
            new_value = int(value)
        except Exception:
            new_value = int(getattr(self, "_media_volume_percent", 100) or 100)
        new_value = max(0, min(100, new_value))
        previous = int(getattr(self, "_media_volume_percent", 100) or 100)
        if new_value == previous:
            return
        self._media_volume_percent = new_value
        self._apply_media_volume_env()
        self._apply_media_volume_to_active_players()
        features_cfg = self._config.setdefault("features", {})
        features_cfg["media_volume"] = int(new_value)
        try:
            save_config(self._config)
        except Exception:
            self._media_volume_percent = previous
            self._apply_media_volume_env()
            self._apply_media_volume_to_active_players()

    def _on_ai_settings_changed(self, payload: Dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return

        ai_cfg = self._config.setdefault("ai", {})
        changed = False
        reset_model = False

        if "model" in payload:
            model = str(payload.get("model") or "").strip()
            if model and ai_cfg.get("model") != model:
                ai_cfg["model"] = model
                changed = True
                reset_model = True

        if "context" in payload:
            ctx_value = payload.get("context")
            ctx_int: Optional[int]
            if ctx_value is None:
                ctx_int = None
            else:
                try:
                    ctx_int = int(ctx_value)
                    if ctx_int <= 0:
                        ctx_int = None
                except Exception:
                    ctx_int = None
            if ai_cfg.get("context") != ctx_int:
                ai_cfg["context"] = ctx_int
                changed = True
                reset_model = True

        if "use_cuda" in payload:
            use_cuda = bool(payload.get("use_cuda", True))
            if bool(ai_cfg.get("use_cuda", True)) != use_cuda:
                ai_cfg["use_cuda"] = use_cuda
                changed = True
                reset_model = True

        if "prompt" in payload:
            prompt = str(payload.get("prompt") or "")
            if str(ai_cfg.get("prompt") or "") != prompt:
                ai_cfg["prompt"] = prompt
                changed = True

        if "cross_chat_context" in payload:
            cross_chat_context = bool(payload.get("cross_chat_context", True))
            if bool(ai_cfg.get("cross_chat_context", True)) != cross_chat_context:
                ai_cfg["cross_chat_context"] = cross_chat_context
                changed = True

        if "cross_chat_limit" in payload:
            value = payload.get("cross_chat_limit")
            try:
                parsed = int(value)
            except Exception:
                parsed = 6
            parsed = max(0, min(parsed, 20))
            if int(ai_cfg.get("cross_chat_limit", 6) or 6) != parsed:
                ai_cfg["cross_chat_limit"] = parsed
                changed = True

        if changed:
            try:
                save_config(self._config)
            except Exception:
                log.exception("Failed to persist AI settings")
            self._apply_ai_config_from_settings(ai_cfg, reset_model=reset_model)

    def on_night_mode_setting_changed(self, enabled: bool) -> None:
        self._set_night_mode(bool(enabled))

    def on_streamer_mode_setting_changed(self, enabled: bool) -> None:
        self._set_streamer_mode(bool(enabled))

    def _set_update_panel_state(self, text: str, *, can_update: bool, in_progress: bool = False) -> None:
        panel = getattr(self, "settings_panel", None)
        if panel is None or not hasattr(panel, "set_update_state"):
            return
        try:
            panel.set_update_state(text, can_update=can_update, in_progress=in_progress)
        except Exception:
            pass

    def _start_update_check(self) -> None:
        thread = getattr(self, "_update_thread", None)
        if thread is not None:
            try:
                if thread.isRunning():
                    return
            except Exception:
                pass
        self._set_update_panel_state(
            f"Версия {self._app_version}. Проверка обновлений...",
            can_update=False,
            in_progress=True,
        )
        thread = QThread()
        thread.setObjectName("update_check_thread")
        worker = ReleaseCheckWorker(repo=self._update_repo, current_version=self._app_version)
        self._orphan_threads.add(thread)
        _ORPHAN_QT_THREADS.add(thread)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(self._on_update_check_finished)
        worker.finished.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda th=thread: self._orphan_threads.discard(th))
        thread.finished.connect(lambda th=thread: _ORPHAN_QT_THREADS.discard(th))
        self._update_thread = thread
        self._update_worker = worker
        thread.start()

    @Slot(dict)
    def _on_update_check_finished(self, payload: Dict[str, Any]) -> None:
        self._update_worker = None
        self._update_thread = None
        data = payload if isinstance(payload, dict) else {}
        ok = bool(data.get("ok"))
        if not ok:
            self._latest_version = ""
            self._update_download_url = ""
            self._set_update_panel_state(
                f"Версия {self._app_version}. Обновление недоступно.",
                can_update=False,
                in_progress=False,
            )
            return

        latest = str(data.get("latest_version") or "").strip()
        self._latest_version = latest
        self._update_download_url = str(data.get("download_url") or "").strip()
        update_available = bool(data.get("update_available")) and bool(self._update_download_url)
        if update_available:
            self._set_update_panel_state(
                f"Доступно обновление {latest} (текущая {self._app_version})",
                can_update=True,
                in_progress=False,
            )
        else:
            shown = latest or self._app_version
            self._set_update_panel_state(
                f"Версия {shown} (актуальная)",
                can_update=False,
                in_progress=False,
            )

    @Slot()
    def _on_update_button_clicked(self) -> None:
        if self._update_in_progress:
            return
        if not self._update_download_url:
            self._start_update_check()
            return
        self._update_in_progress = True
        self._set_update_panel_state(
            "Скачивание обновления...",
            can_update=True,
            in_progress=True,
        )
        file_name = os.path.basename(self._update_download_url.split("?", 1)[0]) or "ESCgram-update.bin"
        output_path = str(app_paths.temp_dir() / file_name)
        thread = QThread()
        thread.setObjectName("update_download_thread")
        worker = UpdateDownloadWorker(url=self._update_download_url, output_path=output_path)
        self._orphan_threads.add(thread)
        _ORPHAN_QT_THREADS.add(thread)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.progress.connect(self._on_update_download_progress)
        worker.finished.connect(self._on_update_download_finished)
        worker.finished.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda th=thread: self._orphan_threads.discard(th))
        thread.finished.connect(lambda th=thread: _ORPHAN_QT_THREADS.discard(th))
        self._update_download_thread = thread
        self._update_download_worker = worker
        thread.start()

    @Slot(int, int)
    def _on_update_download_progress(self, downloaded: int, total: int) -> None:
        if total > 0:
            ratio = max(0.0, min(1.0, float(downloaded) / float(total)))
            percent = int(ratio * 100.0)
            text = f"Скачивание обновления... {percent}%"
        else:
            mb = float(downloaded) / (1024.0 * 1024.0)
            text = f"Скачивание обновления... {mb:.1f} MB"
        self._set_update_panel_state(text, can_update=True, in_progress=True)

    @Slot(dict)
    def _on_update_download_finished(self, payload: Dict[str, Any]) -> None:
        self._update_download_worker = None
        self._update_download_thread = None
        self._update_in_progress = False
        data = payload if isinstance(payload, dict) else {}
        if not bool(data.get("ok")):
            err = str(data.get("error") or "Не удалось скачать обновление.")
            self._set_update_panel_state(
                f"Ошибка обновления: {err}",
                can_update=True,
                in_progress=False,
            )
            try:
                QMessageBox.warning(self, "Обновление", err)
            except Exception:
                pass
            return

        update_path = str(data.get("path") or "").strip()
        if not update_path or not os.path.isfile(update_path):
            self._set_update_panel_state(
                "Ошибка обновления: файл не найден.",
                can_update=True,
                in_progress=False,
            )
            return
        self._apply_downloaded_update(update_path)

    def _apply_downloaded_update(self, update_path: str) -> None:
        if os.name != "nt":
            self._apply_downloaded_update_posix(update_path)
            return

        current_exe = str(sys.executable or "").strip()
        if not current_exe or not os.path.isfile(current_exe):
            self._set_update_panel_state(
                "Обновление скачано. Запустите установщик вручную.",
                can_update=True,
                in_progress=False,
            )
            return

        script_path = str(app_paths.temp_dir() / "escgram_apply_update.cmd")
        quoted_installer = update_path.replace('"', '""')
        quoted_exe = current_exe.replace('"', '""')
        quoted_install = os.path.dirname(current_exe).replace('"', '""')
        quoted_data = str(app_paths.get_data_dir()).replace('"', '""')
        script = (
            "@echo off\r\n"
            "setlocal\r\n"
            f"set \"DATA_DIR={quoted_data}\"\r\n"
            "set \"BACKUP_DIR=%TEMP%\\escgram_data_backup_%RANDOM%%RANDOM%\"\r\n"
            "if exist \"%DATA_DIR%\" (\r\n"
            "  mkdir \"%BACKUP_DIR%\" >nul 2>&1\r\n"
            "  robocopy \"%DATA_DIR%\" \"%BACKUP_DIR%\" /E /NFL /NDL /NJH /NJS /NC /NS >nul\r\n"
            ")\r\n"
            "timeout /t 1 /nobreak >nul\r\n"
            f"start /wait \"\" \"{quoted_installer}\" /VERYSILENT /SUPPRESSMSGBOXES /NORESTART /DIR=\"{quoted_install}\" /DATADIR=\"{quoted_data}\"\r\n"
            "if exist \"%BACKUP_DIR%\" (\r\n"
            "  mkdir \"%DATA_DIR%\" >nul 2>&1\r\n"
            "  robocopy \"%BACKUP_DIR%\" \"%DATA_DIR%\" /E /NFL /NDL /NJH /NJS /NC /NS >nul\r\n"
            "  rmdir /S /Q \"%BACKUP_DIR%\" >nul 2>&1\r\n"
            ")\r\n"
            "if exist \"%SystemRoot%\\System32\\ie4uinit.exe\" (\r\n"
            "  \"%SystemRoot%\\System32\\ie4uinit.exe\" -ClearIconCache >nul 2>&1\r\n"
            "  \"%SystemRoot%\\System32\\ie4uinit.exe\" -show >nul 2>&1\r\n"
            ")\r\n"
            f"start \"\" \"{quoted_exe}\" --data-dir \"{quoted_data}\"\r\n"
            "del \"%~f0\"\r\n"
        )
        try:
            with open(script_path, "w", encoding="utf-8", newline="") as fh:
                fh.write(script)
            flags = 0
            if hasattr(subprocess, "CREATE_NO_WINDOW"):
                flags = int(getattr(subprocess, "CREATE_NO_WINDOW"))
            subprocess.Popen(["cmd", "/c", script_path], creationflags=flags)
            self._set_update_panel_state(
                "Обновление запускается...",
                can_update=False,
                in_progress=False,
            )
            QApplication.instance().quit()
        except Exception as exc:
            self._set_update_panel_state(
                "Ошибка запуска обновления.",
                can_update=True,
                in_progress=False,
            )
            try:
                QMessageBox.warning(
                    self,
                    "Обновление",
                    f"Не удалось запустить обновление автоматически:\n{exc}\n\nФайл:\n{update_path}",
                )
            except Exception:
                pass

    def _apply_downloaded_update_posix(self, update_path: str) -> None:
        current_exe = str(sys.executable or "").strip()
        if not current_exe or not os.path.isfile(current_exe):
            self._set_update_panel_state(
                "Обновление скачано. Установите вручную из файла.",
                can_update=True,
                in_progress=False,
            )
            return
        if not tarfile.is_tarfile(update_path):
            self._set_update_panel_state(
                "Обновление скачано. Формат не поддерживается для автоустановки.",
                can_update=True,
                in_progress=False,
            )
            return

        install_dir = os.path.dirname(current_exe)
        if not install_dir:
            self._set_update_panel_state(
                "Обновление скачано. Не удалось определить папку установки.",
                can_update=True,
                in_progress=False,
            )
            return

        data_dir = str(app_paths.get_data_dir())
        script_path = str(app_paths.temp_dir() / "escgram_apply_update.sh")
        quoted_archive = update_path.replace('"', '\\"')
        quoted_install = install_dir.replace('"', '\\"')
        quoted_exe = current_exe.replace('"', '\\"')
        quoted_data = data_dir.replace('"', '\\"')

        script = (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "sleep 1\n"
            "TMP_DIR=\"$(mktemp -d)\"\n"
            "BACKUP_DIR=\"$(mktemp -d)\"\n"
            "cleanup(){ rm -rf \"$TMP_DIR\"; }\n"
            "trap cleanup EXIT\n"
            f"if [[ -d \"{quoted_data}\" ]]; then\n"
            "  if command -v rsync >/dev/null 2>&1; then\n"
            f"    rsync -a \"{quoted_data}\"/ \"$BACKUP_DIR\"/\n"
            "  else\n"
            f"    cp -a \"{quoted_data}\"/. \"$BACKUP_DIR\"/ 2>/dev/null || true\n"
            "  fi\n"
            "fi\n"
            f"tar -xzf \"{quoted_archive}\" -C \"$TMP_DIR\"\n"
            "SRC_DIR=\"\"\n"
            "if [[ -d \"$TMP_DIR/ESCgram\" ]]; then SRC_DIR=\"$TMP_DIR/ESCgram\"; fi\n"
            "if [[ -z \"$SRC_DIR\" && -d \"$TMP_DIR/dist_linux/ESCgram\" ]]; then SRC_DIR=\"$TMP_DIR/dist_linux/ESCgram\"; fi\n"
            "if [[ -z \"$SRC_DIR\" ]]; then\n"
            "  CANDIDATE=\"$(find \"$TMP_DIR\" -maxdepth 3 -type f -name 'ESCgram' | head -n 1 || true)\"\n"
            "  if [[ -n \"$CANDIDATE\" ]]; then SRC_DIR=\"$(dirname \"$CANDIDATE\")\"; fi\n"
            "fi\n"
            "if [[ -z \"$SRC_DIR\" ]]; then\n"
            "  exit 2\n"
            "fi\n"
            f"mkdir -p \"{quoted_install}\"\n"
            "if command -v rsync >/dev/null 2>&1; then\n"
            f"  rsync -a --delete \"$SRC_DIR\"/ \"{quoted_install}\"/\n"
            "else\n"
            f"  rm -rf \"{quoted_install}\"/*\n"
            f"  cp -a \"$SRC_DIR\"/. \"{quoted_install}\"/\n"
            "fi\n"
            f"mkdir -p \"{quoted_data}\"\n"
            "if command -v rsync >/dev/null 2>&1; then\n"
            f"  rsync -a \"$BACKUP_DIR\"/ \"{quoted_data}\"/ 2>/dev/null || true\n"
            "else\n"
            f"  cp -a \"$BACKUP_DIR\"/. \"{quoted_data}\"/ 2>/dev/null || true\n"
            "fi\n"
            "rm -rf \"$BACKUP_DIR\" >/dev/null 2>&1 || true\n"
            f"nohup \"{quoted_exe}\" --data-dir \"{quoted_data}\" >/dev/null 2>&1 &\n"
        )
        try:
            with open(script_path, "w", encoding="utf-8") as fh:
                fh.write(script)
            os.chmod(script_path, 0o755)
            subprocess.Popen(["bash", script_path], start_new_session=True)
            self._set_update_panel_state(
                "Обновление запускается...",
                can_update=False,
                in_progress=False,
            )
            QApplication.instance().quit()
        except Exception as exc:
            self._set_update_panel_state(
                "Ошибка запуска обновления.",
                can_update=True,
                in_progress=False,
            )
            try:
                QMessageBox.warning(
                    self,
                    "Обновление",
                    f"Не удалось запустить автообновление:\n{exc}\n\nФайл:\n{update_path}",
                )
            except Exception:
                pass

    def on_sidebar_action_requested(self, action_id: str) -> None:
        actions = {
            "read_local": lambda: self._mark_current_chat_read(local=True),
            "read_remote": lambda: self._mark_current_chat_read(local=False),
            "accounts": self._open_account_manager,
            "settings": self._open_settings_window,
            "saved": self._open_saved_messages,
            "profile": self._show_profile_dialog,
            "contacts": self._open_contacts_picker,
            "create_group": self._create_group_dialog,
            "create_channel": self._create_channel_dialog,
            "archive": self._open_archive_picker,
            "wallet": self._open_wallet_chat,
            "calls": self._show_calls_stub,
        }
        handler = actions.get(action_id)
        if handler:
            handler()
            return
        self._toast("Действие пока недоступно")

    def _open_saved_messages(self) -> None:
        user_id = ""
        try:
            user_id = str(self.server.get_self_user_id() or "")
        except Exception:
            user_id = ""
        if not user_id and hasattr(self.tg, "get_self_id_sync"):
            try:
                user_id = str(self.tg.get_self_id_sync() or "")
            except Exception:
                user_id = ""
        if not user_id:
            QMessageBox.warning(self, "Избранное", "Не удалось определить ID аккаунта.")
            return
        self.switch_chat(user_id)

    def _show_profile_dialog(self) -> None:
        meta = None
        if hasattr(self.tg, "refresh_active_account_profile"):
            try:
                meta = self.tg.refresh_active_account_profile()
            except Exception:
                meta = None
        if not isinstance(meta, dict):
            meta = {}
        info = {
            "id": str(meta.get("user_id") or meta.get("id") or ""),
            "title": str(meta.get("full_name") or meta.get("title") or "Профиль"),
            "username": str(meta.get("username") or ""),
            "phone": str(meta.get("phone") or ""),
            "type": "user",
            "is_premium": bool(meta.get("is_premium", False)),
            "is_verified": bool(meta.get("is_verified", False)),
            "about": str(meta.get("bio") or meta.get("about") or ""),
        }
        panel = ChatInfoDialog(info, avatar=self._account_avatar_pixmap(meta), sections={}, callbacks={}, parent=self, embedded=True)
        self._set_chat_details_widget("Мой профиль", panel)

    def _pick_from_list(self, title: str, label: str, items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not items:
            return None
        labels: List[str] = []
        for row in items:
            text = str(row.get("label") or row.get("title") or row.get("id") or "")
            labels.append(text)
        selection, ok = QInputDialog.getItem(self, title, label, labels, 0, False)
        if not ok or not selection:
            return None
        try:
            idx = labels.index(selection)
        except ValueError:
            return None
        return items[idx] if 0 <= idx < len(items) else None

    def _open_contacts_picker(self) -> None:
        if not hasattr(self.tg, "get_contacts_sync"):
            QMessageBox.information(self, "Контакты", "Контакты недоступны.")
            return
        try:
            contacts = list(self.tg.get_contacts_sync())
        except Exception:
            contacts = []
        if not contacts:
            QMessageBox.information(self, "Контакты", "Список контактов пуст.")
            return
        rows: List[Dict[str, Any]] = [
            {
                "id": "__back__",
                "title": "← Назад к чатам",
                "meta": "",
                "unread": 0,
                "info": {"title": "Назад", "type": "service"},
            }
        ]
        for row in contacts:
            title = str(row.get("title") or row.get("username") or row.get("id") or "")
            username = str(row.get("username") or "")
            phone = str(row.get("phone") or "")
            parts = [title]
            if username:
                parts.append(f"@{username}")
            if phone:
                parts.append(phone)
            label = " • ".join([p for p in parts if p])
            cid = str(row.get("id") or "").strip()
            if not cid:
                continue
            existing = dict(self.all_chats.get(cid, {}))
            ctype = str(existing.get("type") or ("bot" if username.endswith("bot") else "private"))
            info = {
                **existing,
                "id": cid,
                "title": title or cid,
                "type": ctype,
                "username": username,
                "photo_small_id": row.get("photo_small_id") or row.get("photo_small") or existing.get("photo_small_id") or existing.get("photo_small"),
            }
            rows.append(
                {
                    "id": cid,
                    "title": title or cid,
                    "meta": label,
                    "unread": int(existing.get("unread_count") or 0),
                    "info": info,
                }
            )
        if hasattr(self, "set_chat_list_override"):
            self.set_chat_list_override(mode="contacts", rows=rows)
            return
        # Fallback (shouldn't happen): open first contact.
        for row in rows:
            cid = str(row.get("id") or "")
            if cid and cid != "__back__":
                self.switch_chat(cid)
                return

    def _open_archive_picker(self) -> None:
        if not hasattr(self.tg, "list_archived_chats_sync"):
            QMessageBox.information(self, "Архив", "Архив недоступен.")
            return
        try:
            rows = list(self.tg.list_archived_chats_sync(limit=200))
        except Exception:
            rows = []
        if not rows:
            QMessageBox.information(self, "Архив", "Архив пуст.")
            return
        override_rows: List[Dict[str, Any]] = [
            {
                "id": "__back__",
                "title": "← Назад к чатам",
                "meta": "",
                "unread": 0,
                "info": {"title": "Назад", "type": "service"},
            }
        ]
        for row in rows:
            cid = str(row.get("id") or "").strip()
            if not cid:
                continue
            title = str(row.get("title") or cid)
            username = str(row.get("username") or "")
            ctype = str(row.get("type") or self.all_chats.get(cid, {}).get("type") or "private")
            meta_parts: List[str] = [self._type_label(ctype)] if self._type_label(ctype) else []
            if username:
                meta_parts.append(f"@{username}")
            meta = " • ".join([x for x in meta_parts if x])
            info = {
                **dict(self.all_chats.get(cid, {})),
                "id": cid,
                "title": title,
                "type": ctype,
                "username": username,
                "photo_small_id": row.get("photo_small_id") or row.get("photo_small") or self.all_chats.get(cid, {}).get("photo_small_id"),
                "last_ts": int(row.get("last_ts") or row.get("last_message_date") or self.all_chats.get(cid, {}).get("last_ts") or 0),
                "unread_count": int(row.get("unread_count") or self.all_chats.get(cid, {}).get("unread_count") or 0),
            }
            override_rows.append(
                {
                    "id": cid,
                    "title": title,
                    "meta": meta,
                    "unread": int(info.get("unread_count") or 0),
                    "info": info,
                }
            )
        if hasattr(self, "set_chat_list_override"):
            self.set_chat_list_override(mode="archive", rows=override_rows)
            return
        for row in override_rows:
            cid = str(row.get("id") or "")
            if cid and cid != "__back__":
                self.switch_chat(cid)
                return

    def _create_group_dialog(self) -> None:
        if not hasattr(self.tg, "create_group_sync"):
            QMessageBox.information(self, "Создать группу", "Создание групп недоступно.")
            return
        title, ok = QInputDialog.getText(self, "Создать группу", "Название группы:")
        if not ok or not str(title or "").strip():
            return
        users_raw, ok = QInputDialog.getText(
            self,
            "Создать группу",
            "Участники (ID или @username, через запятую):",
        )
        if not ok:
            return
        users = [u.strip() for u in str(users_raw or "").split(",") if u.strip()]
        if not users:
            QMessageBox.warning(self, "Создать группу", "Нужно указать хотя бы одного участника.")
            return
        chat_id = None
        try:
            chat_id = self.tg.create_group_sync(str(title), users)
        except Exception:
            chat_id = None
        if not chat_id:
            QMessageBox.warning(self, "Создать группу", "Не удалось создать группу.")
            return
        self.switch_chat(str(chat_id))

    def _create_channel_dialog(self) -> None:
        if not hasattr(self.tg, "create_channel_sync"):
            QMessageBox.information(self, "Создать канал", "Создание каналов недоступно.")
            return
        title, ok = QInputDialog.getText(self, "Создать канал", "Название канала:")
        if not ok or not str(title or "").strip():
            return
        desc, _ = QInputDialog.getMultiLineText(self, "Создать канал", "Описание (необязательно):")
        chat_id = None
        try:
            chat_id = self.tg.create_channel_sync(str(title), str(desc or ""))
        except Exception:
            chat_id = None
        if not chat_id:
            QMessageBox.warning(self, "Создать канал", "Не удалось создать канал.")
            return
        self.switch_chat(str(chat_id))

    def _open_wallet_chat(self) -> None:
        if not hasattr(self.tg, "resolve_username_sync"):
            QMessageBox.information(self, "Кошелёк", "Кошелёк недоступен.")
            return
        chat_id = None
        try:
            chat_id = self.tg.resolve_username_sync("wallet")
        except Exception:
            chat_id = None
        if not chat_id:
            QMessageBox.warning(self, "Кошелёк", "Не удалось открыть чат кошелька.")
            return
        self.switch_chat(str(chat_id))

    def _show_calls_stub(self) -> None:
        QMessageBox.information(self, "Звонки", "История звонков пока не поддерживается.")

    def _mark_current_chat_read(self, *, local: bool) -> None:
        chat_id = self.current_chat_id
        if not chat_id:
            self._toast("Нет активного чата")
            return
        if local:
            self._apply_chat_activity(chat_id, clear_unread=True, refresh_delay_ms=0)
            self._toast("Помечено как прочитанное локально")
            return
        ok = False
        if hasattr(self.tg, "mark_chat_read_sync"):
            try:
                ok = bool(self.tg.mark_chat_read_sync(chat_id))
            except Exception:
                ok = False
        self._toast("Помечено как прочитанное на сервере" if ok else "Не удалось отправить отметку о прочтении")

    def _refresh_voice_wave_widgets(self) -> None:
        """Sync waveform widgets visibility with the master toggle."""
        for mid, widget in list(getattr(self, "_message_widgets", {}).items()):
            cache = self._message_cache.get(mid)
            if not cache or cache.get("kind") != "voice":
                continue
            setter = getattr(widget, "set_voice_waveform_enabled", None)
            if callable(setter):
                try:
                    setter(self._voice_waveform_enabled)
                except Exception:
                    log.debug("Failed to refresh waveform widget for message %s", mid)

    def _after_media_widget_added(self, widget: ChatItemWidget) -> bool:
        # Avoid O(N^2) relayouts during history loads.
        self._schedule_media_group_refresh()
        if self._loading_history:
            return False
        should_stick = self._is_user_near_bottom()
        self._maybe_auto_download_widget(widget)
        return should_stick

    def _is_widget_near_viewport(self, widget: QWidget, margin: int = 260) -> bool:
        viewport = getattr(getattr(self, "chat_scroll", None), "viewport", lambda: None)()
        if viewport is None or not _qt_is_valid(viewport):
            return True
        target = getattr(widget, "_row_wrap", None) or widget
        if target is None or not _qt_is_valid(target):
            return False
        # mapTo() requires target to be in viewport parent hierarchy.
        parent = target
        in_hierarchy = False
        try:
            while parent is not None:
                if parent is viewport:
                    in_hierarchy = True
                    break
                parent = parent.parentWidget() if hasattr(parent, "parentWidget") else None
        except Exception:
            in_hierarchy = False
        if not in_hierarchy:
            return False
        try:
            point = target.mapTo(viewport, QPoint(0, 0))
            top = int(point.y())
            height = int(max(1, target.height()))
            bottom = top + height
            return (bottom >= -int(margin)) and (top <= int(viewport.height()) + int(margin))
        except Exception:
            return True

    def _refresh_media_group_layout(self) -> None:
        order = list(getattr(self, "_message_order", []) or [])
        total = len(order)
        if total <= 0:
            return
        supported_album_kinds = {"image", "video", "animation"}
        for widget in order:
            row_wrap = getattr(widget, "_row_wrap", None)
            if row_wrap is not None:
                try:
                    row_wrap.show()
                except Exception:
                    pass
            album_setter = getattr(widget, "set_media_group_items", None)
            if callable(album_setter):
                try:
                    album_setter(None)
                except Exception:
                    pass

        idx = 0
        while idx < total:
            widget = order[idx]
            group_id = str(getattr(widget, "_media_group_id", "") or "").strip()
            setter = getattr(widget, "set_media_group_position", None)
            role_key = str(getattr(widget, "_message_role", "") or "").lower()
            if not group_id:
                if callable(setter):
                    try:
                        setter("single")
                    except Exception:
                        pass
                idx += 1
                continue

            end = idx + 1
            while end < total:
                probe = order[end]
                same_group = str(getattr(probe, "_media_group_id", "") or "").strip() == group_id
                same_role = str(getattr(probe, "_message_role", "") or "").lower() == role_key
                if not (same_group and same_role):
                    break
                end += 1
            group_widgets = order[idx:end]

            can_album = len(group_widgets) > 1
            if can_album:
                for member in group_widgets:
                    kind = str(getattr(member, "kind", "") or "").lower()
                    deleted_checker = getattr(member, "is_deleted", None)
                    is_deleted = bool(deleted_checker()) if callable(deleted_checker) else False
                    if kind not in supported_album_kinds or is_deleted:
                        can_album = False
                        break

            if can_album:
                first = group_widgets[0]
                album_items: List[Dict[str, Any]] = []
                for member in group_widgets:
                    row_wrap = getattr(member, "_row_wrap", None)
                    member_id = self._message_id_from_widget(member)
                    album_items.append(
                        {
                            "kind": str(getattr(member, "kind", "") or "").lower(),
                            "file_path": getattr(member, "file_path", None),
                            "thumb_path": getattr(member, "thumb_path", None),
                            "path": getattr(member, "file_path", None) or getattr(member, "thumb_path", None),
                            "message_id": int(member_id) if member_id is not None else None,
                        }
                    )
                for member in group_widgets:
                    row_wrap = getattr(member, "_row_wrap", None)
                    if member is first:
                        if callable(setter := getattr(member, "set_media_group_position", None)):
                            try:
                                setter("single")
                            except Exception:
                                pass
                        album_setter = getattr(member, "set_media_group_items", None)
                        if callable(album_setter):
                            try:
                                album_setter(album_items)
                            except Exception:
                                pass
                        if row_wrap is not None:
                            try:
                                row_wrap.show()
                            except Exception:
                                pass
                    else:
                        if row_wrap is not None:
                            try:
                                row_wrap.hide()
                            except Exception:
                                pass
                idx = end
                continue

            group_total = len(group_widgets)
            for local_idx, member in enumerate(group_widgets):
                pos = "single"
                if group_total > 1:
                    if local_idx == 0:
                        pos = "top"
                    elif local_idx == group_total - 1:
                        pos = "bottom"
                    else:
                        pos = "middle"
                member_setter = getattr(member, "set_media_group_position", None)
                if callable(member_setter):
                    try:
                        member_setter(pos)
                    except Exception:
                        pass
                row_wrap = getattr(member, "_row_wrap", None)
                if row_wrap is not None:
                    try:
                        row_wrap.show()
                    except Exception:
                        pass
            idx = end

    def _maybe_auto_download_widget(self, widget: ChatItemWidget) -> None:
        if not self._auto_download_enabled or self._loading_history:
            return
        if not isinstance(widget, ChatItemWidget):
            return
        if widget.file_path and os.path.isfile(widget.file_path):
            return
        if widget.download_state not in {"idle", "error"}:
            return
        if not widget.server or not widget.chat_id or widget.msg_id is None:
            return
        auto_types = {"image", "voice", "video_note", "video", "animation"}
        if widget.kind not in auto_types:
            return
        if not self._is_widget_near_viewport(widget):
            return
        chat_info = self.all_chats.get(str(widget.chat_id), {})
        chat_type = chat_info.get("type", "private")
        if not self._auto_download_policy.should_download(
            chat_type=chat_type,
            kind=widget.kind,
            file_size=widget.file_size,
        ):
            return
        if getattr(widget, "_auto_dl_pending", False):
            return
        setattr(widget, "_auto_dl_pending", True)
        QTimer.singleShot(120, lambda w=widget: self._start_widget_auto_download(w))

    def _start_widget_auto_download(self, widget: ChatItemWidget) -> None:
        if not _qt_is_valid(widget):
            return
        setattr(widget, "_auto_dl_pending", False)
        row_wrap = getattr(widget, "_row_wrap", None)
        if row_wrap is not None and (not _qt_is_valid(row_wrap) or row_wrap.parent() is None):
            return
        if getattr(widget, "_disposed", False):
            return
        controls_alive = getattr(widget, "_download_controls_alive", None)
        if callable(controls_alive):
            try:
                if not controls_alive():
                    return
            except Exception:
                return
        if widget.download_state not in {"idle", "error"}:
            return
        if not self._is_widget_near_viewport(widget):
            return
        try:
            widget._start_download()
        except Exception:
            log.exception("Auto-download failed for message %s/%s", widget.chat_id, widget.msg_id)

    def _apply_auto_download_to_feed(self) -> None:
        """Trigger auto-download for currently rendered media widgets."""
        for widget in list(self._message_widgets.values()):
            self._maybe_auto_download_widget(widget)

    def _on_feed_scroll_changed(self, _value: int) -> None:
        if self._auto_download_enabled and not self._loading_history:
            self._apply_auto_download_to_feed()
        self._maybe_load_more_history_from_scroll()

    def _maybe_load_more_history_from_scroll(self) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id or not chat_id.lstrip("-").isdigit():
            return
        if self._loading_history:
            return
        if not bool(getattr(self, "_history_scroll_enabled", False)):
            return
        try:
            bar = self.chat_scroll.verticalScrollBar()
        except Exception:
            return
        if bar is None or int(bar.value()) > int(getattr(self, "_history_scroll_threshold", 40) or 40):
            return
        current_limit = int(getattr(self, "_history_current_limit", 0) or 0)
        if current_limit <= 0:
            return
        chunk = int(getattr(self, "_history_chunk_size", 140) or 140)
        soft_cap = int(getattr(self, "_history_prefetch_limit", 500) or 500)
        if current_limit < soft_cap:
            next_limit = min(soft_cap, current_limit + chunk)
        else:
            next_limit = current_limit + chunk
        if next_limit <= current_limit:
            return
        self._history_load_requested_by_scroll = True
        self.load_chat_history_async(reset=False, limit=next_limit, auto_scroll=False)

    def _is_user_near_bottom(self, threshold: int = 96) -> bool:
        try:
            bar = self.chat_scroll.verticalScrollBar()
        except Exception:
            return True
        return (bar.maximum() - bar.value()) <= threshold

    def _cache_message(
        self,
        msg_id: Optional[int],
        *,
        text: str,
        kind: str,
        sender: str,
        reply_to: Optional[int],
        is_deleted: bool,
        forward_info: Optional[Dict[str, Any]] = None,
        duration: Optional[int] = None,
        waveform: Optional[List[int]] = None,
        media_group_id: Optional[str] = None,
        reactions: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        if msg_id is None:
            return
        try:
            mid = int(msg_id)
        except Exception:
            return
        cached: Dict[str, Any] = {
            "id": mid,
            "text": text or "",
            "kind": kind or "text",
            "sender": sender or "",
            "reply_to": int(reply_to) if reply_to else None,
            "is_deleted": bool(is_deleted),
            "forward_info": forward_info or None,
            "duration": duration,
            "waveform": list(waveform) if isinstance(waveform, list) else None,
            "media_group_id": (str(media_group_id).strip() if media_group_id else None),
            "reactions": [dict(item) for item in list(reactions or []) if isinstance(item, dict)] or None,
        }
        self._message_cache[mid] = cached
        if reply_to:
            try:
                parent = int(reply_to)
            except Exception:
                parent = None
            if parent is not None:
                bucket = self._reply_index.setdefault(parent, [])
                if mid not in bucket:
                    bucket.append(mid)

    def _build_reply_preview(self, reply_to: Optional[int]) -> Optional[Dict[str, Any]]:
        if reply_to is None:
            return None
        try:
            target_id = int(reply_to)
        except Exception:
            return None
        cached = self._message_cache.get(target_id)
        if cached is None and self.current_chat_id:
            details = self.server.get_message_details_for_ui(self.current_chat_id, target_id)
            if details:
                cached = {
                    "id": target_id,
                    "text": details.get("text") or "",
                    "kind": details.get("type") or "text",
                    "sender": details.get("sender") or "",
                    "reply_to": details.get("reply_to"),
                    "is_deleted": bool(details.get("is_deleted")),
                    "forward_info": details.get("forward_info"),
                    "duration": details.get("duration"),
                    "waveform": details.get("waveform"),
                    "media_group_id": details.get("media_group_id"),
                }
                self._message_cache[target_id] = cached
                reply_parent = details.get("reply_to")
                if reply_parent:
                    try:
                        parent = int(reply_parent)
                    except Exception:
                        parent = None
                    if parent is not None:
                        bucket = self._reply_index.setdefault(parent, [])
                        if target_id not in bucket:
                            bucket.append(target_id)
        if not cached:
            return None
        return {
            "id": target_id,
            "text": cached.get("text") or "",
            "kind": cached.get("kind") or "text",
            "sender": cached.get("sender") or "",
            "is_deleted": bool(cached.get("is_deleted")),
            "forward_info": cached.get("forward_info"),
            "duration": cached.get("duration"),
            "waveform": cached.get("waveform"),
        }

    def _prime_reply_preview_cache(self, messages: List[Dict[str, Any]]) -> None:
        if not self.current_chat_id or not messages:
            return
        missing: List[int] = []
        seen: set[int] = set()
        for entry in messages:
            reply_to_raw = entry.get("reply_to")
            try:
                reply_to = int(reply_to_raw) if reply_to_raw is not None else None
            except Exception:
                reply_to = None
            if reply_to is None or reply_to <= 0 or reply_to in self._message_cache or reply_to in seen:
                continue
            seen.add(reply_to)
            missing.append(reply_to)
        if not missing:
            return
        try:
            fetched = self.server.get_messages_details_for_ui(self.current_chat_id, missing)
        except Exception:
            fetched = {}
        if not isinstance(fetched, dict):
            return
        for target_id, details in fetched.items():
            if not isinstance(details, dict):
                continue
            try:
                msg_id = int(target_id)
            except Exception:
                continue
            cached = {
                "id": msg_id,
                "text": details.get("text") or "",
                "kind": details.get("type") or "text",
                "sender": details.get("sender") or "",
                "reply_to": details.get("reply_to"),
                "is_deleted": bool(details.get("is_deleted")),
                "forward_info": details.get("forward_info"),
                "duration": details.get("duration"),
                "waveform": details.get("waveform"),
                "media_group_id": details.get("media_group_id"),
            }
            self._message_cache[msg_id] = cached
            reply_parent = details.get("reply_to")
            if reply_parent:
                try:
                    parent = int(reply_parent)
                except Exception:
                    parent = None
                if parent is not None:
                    bucket = self._reply_index.setdefault(parent, [])
                    if msg_id not in bucket:
                        bucket.append(msg_id)

    def _update_reply_references(self, target_id: Optional[int]) -> None:
        if target_id is None:
            return
        try:
            key = int(target_id)
        except Exception:
            return
        refs = self._reply_index.get(key, [])
        if not refs:
            return
        preview = self._build_reply_preview(key)
        for mid in refs:
            widget = self._message_widgets.get(mid)
            if not widget:
                continue
            try:
                if hasattr(widget, "set_reply_preview"):
                    widget.set_reply_preview(preview)
            except Exception:
                continue

    def _flush_reply_updates(self, ids: Iterable[int]) -> None:
        for mid in ids:
            try:
                self._update_reply_references(int(mid))
            except Exception:
                continue

    def _schedule_media_group_refresh(self) -> None:
        if self._media_group_refresh_pending:
            return
        self._media_group_refresh_pending = True
        QTimer.singleShot(60, self._flush_media_group_refresh)

    def _flush_media_group_refresh(self) -> None:
        self._media_group_refresh_pending = False
        try:
            self._refresh_media_group_layout()
        except Exception:
            pass

    def _message_insert_index(self, msg_id: Optional[int]) -> Optional[int]:
        """Return insertion position that keeps _message_order sorted by message id."""
        if msg_id is None:
            return None
        try:
            mid = int(msg_id)
        except Exception:
            return None
        if mid <= 0:
            return None
        order = list(getattr(self, "_message_order", []) or [])
        if not order:
            return None
        try:
            last_id = self._message_id_from_widget(order[-1])
        except Exception:
            last_id = None
        if last_id is not None and last_id > 0 and mid > last_id:
            return None
        try:
            first_id = self._message_id_from_widget(order[0])
        except Exception:
            first_id = None
        if first_id is not None and first_id > 0 and mid < first_id:
            return 0
        indexed: List[tuple[int, int]] = []
        for idx, widget in enumerate(order):
            existing_id = self._message_id_from_widget(widget)
            if existing_id is None or existing_id <= 0:
                continue
            indexed.append((int(existing_id), idx))
        if not indexed:
            return None
        numeric_ids = [item[0] for item in indexed]
        position = bisect_left(numeric_ids, mid)
        if position >= len(indexed):
            return None
        return indexed[position][1]

    @staticmethod
    def _widget_text(widget: object) -> str:
        for attr in ("_original_text", "text"):
            try:
                val = getattr(widget, attr, None)
            except Exception:
                val = None
            if isinstance(val, str):
                return val
        return ""

    def _promote_local_widget_message_id(self, widget: QWidget, *, msg_id: int, timestamp: Optional[int] = None) -> None:
        try:
            mid = int(msg_id)
        except Exception:
            return
        if mid <= 0:
            return
        old_mid: Optional[int] = None
        for attr in ("_message_id", "msg_id"):
            try:
                raw = getattr(widget, attr, None)
            except Exception:
                raw = None
            if raw is None:
                continue
            try:
                old_mid = int(raw)
                break
            except Exception:
                continue
        if old_mid is not None and old_mid in self._message_widgets and old_mid != mid:
            try:
                self._message_widgets.pop(old_mid, None)
            except Exception:
                pass
        was_selected = bool(old_mid is not None and old_mid in self._selected_message_ids)
        try:
            setattr(widget, "_message_id", mid)
        except Exception:
            pass
        if hasattr(widget, "msg_id"):
            try:
                setattr(widget, "msg_id", mid)
            except Exception:
                pass
        if timestamp is not None:
            try:
                setattr(widget, "_message_timestamp", int(timestamp))
            except Exception:
                pass
        try:
            self._message_widgets[mid] = widget
        except Exception:
            pass
        row_wrap = getattr(widget, "_row_wrap", None)
        if row_wrap is not None:
            try:
                setattr(row_wrap, "_message_id", mid)
            except Exception:
                pass
        if old_mid is not None:
            try:
                self._selected_message_ids.discard(int(old_mid))
            except Exception:
                pass
        if was_selected:
            try:
                self._selected_message_ids.add(mid)
            except Exception:
                pass
        if old_mid is not None and old_mid != mid:
            cached = self._message_cache.pop(int(old_mid), None)
            if isinstance(cached, dict) and mid not in self._message_cache:
                cached["id"] = mid
                self._message_cache[mid] = cached

    def _find_pending_local_text_widget(self, text: str) -> Optional[QWidget]:
        target = str(text or "").strip()
        fallback: Optional[QWidget] = None
        if not target:
            return fallback
        for widget in reversed(list(getattr(self, "_message_order", []) or [])):
            role = str(getattr(widget, "role", "") or getattr(widget, "_message_role", "")).lower()
            if role != "me":
                continue
            wid = self._message_id_from_widget(widget)
            if wid is not None and wid > 0:
                continue
            wkind = str(getattr(widget, "kind", "") or "text").lower()
            if wkind != "text":
                continue
            local_text = str(self._widget_text(widget) or "").strip()
            if local_text == target:
                return widget
            if fallback is None:
                fallback = widget
        return fallback

    def _find_pending_local_media_widget(self, kind: str, file_size: int) -> Optional[QWidget]:
        want = str(kind or "").strip().lower()
        fallback: Optional[QWidget] = None
        for widget in reversed(list(getattr(self, "_message_order", []) or [])):
            role = str(getattr(widget, "role", "")).lower()
            if role != "me":
                continue
            wkind = str(getattr(widget, "kind", "")).lower()
            if wkind != want:
                continue
            wid = self._message_id_from_widget(widget)
            if wid is not None and wid > 0:
                continue
            try:
                wsize = int(getattr(widget, "file_size", 0) or 0)
            except Exception:
                wsize = 0
            if fallback is None:
                fallback = widget
            if file_size > 0 and wsize > 0 and wsize != file_size:
                continue
            return widget
        return fallback

    @staticmethod
    def _utf16_len_text(value: str) -> int:
        total = 0
        for ch in str(value or ""):
            total += 2 if ord(ch) > 0xFFFF else 1
        return total

    def _decode_hidden_display(self, raw_text: str) -> tuple[str, Optional[List[Dict[str, Any]]], bool]:
        text = str(raw_text or "")
        hidden_flag = is_zwc_only(text)
        if not (hidden_flag or contains_zwc(text)):
            return text, None, False
        display, hidden_entities, has_hidden = reveal_zwc_fragments_with_entities(text)
        display_text = str(display or "")
        entities = hidden_entities or None
        if hidden_flag and not display_text:
            decoded = decode_zwc(text)
            if decoded:
                display_text = str(decoded)
                entities = [{"type": "hidden", "offset": 0, "length": self._utf16_len_text(display_text)}]
                has_hidden = True
        return display_text, entities, bool(has_hidden)

    # ------------------------------------------------------------------ #
    # Event pump handlers

    @Slot(str, str)
    def _on_ai_msg(self, chat_id: str, text: str) -> None:
        self._remember_history_entry(
            chat_id,
            role="assistant",
            text=text,
            sender_id="assistant",
            sender_name="AI",
        )
        if self.current_chat_id and chat_id == self.current_chat_id:
            self.add_text_item("🤖 AI", text, role="assistant", chat_id=chat_id, user_id="assistant")

    @Slot(str, str, str, object)
    def _on_user_echo(self, chat_id: str, text: str, user_id: str, payload: object) -> None:
        mine = (user_id == "me") or (self._my_id and str(user_id) == str(self._my_id))
        role = "me" if mine else "other"
        header = "Вы" if mine else (self.server.get_user_display_name(user_id) or "Пользователь")
        sender_id_val = str(self._my_id or user_id or "me") if mine else str(user_id)
        hidden_flag = is_zwc_only(text)
        decoded = decode_zwc(text) if hidden_flag else None
        entities = None
        if isinstance(payload, dict):
            maybe_entities = payload.get("entities")
            if isinstance(maybe_entities, list):
                entities = maybe_entities
        reply_to = None
        if isinstance(payload, dict):
            try:
                reply_to_raw = payload.get("reply_to")
                reply_to = int(reply_to_raw) if reply_to_raw is not None else None
            except Exception:
                reply_to = None
        local_msg_id: Optional[int] = None
        if isinstance(payload, dict):
            try:
                local_raw = payload.get("id")
                local_msg_id = int(local_raw) if local_raw is not None else None
            except Exception:
                local_msg_id = None
        reply_preview = self._build_reply_preview(reply_to)

        # For Telegram chats (numeric IDs), storage is the single source of truth.
        # Persisting local echoes into history.json causes duplicates/reordering.
        if not str(chat_id or "").lstrip("-").isdigit():
            self._remember_history_entry(
                chat_id,
                role=role,
                text=text,
                sender_id=sender_id_val,
                sender_name=header,
                hidden=hidden_flag,
                decoded_text=decoded,
            )
        if chat_id != (self.current_chat_id or ""):
            return

        if contains_zwc(text) or hidden_flag:
            display, hidden_entities, has_hidden = self._decode_hidden_display(text)
            self.add_text_item(
                header,
                display,
                role=role,
                chat_id=chat_id,
                user_id=user_id,
                entities=hidden_entities,
                has_hidden=has_hidden,
                msg_id=local_msg_id,
                reply_to=reply_to,
                reply_preview=reply_preview,
            )
            return

        self.add_text_item(
            header,
            text,
            role=role,
            chat_id=chat_id,
            user_id=user_id,
            entities=entities,
            msg_id=local_msg_id,
            reply_to=reply_to,
            reply_preview=reply_preview,
        )

    @Slot(str, int, int)
    def _on_gui_message_sent(self, chat_id: str, local_id: int, message_id: int) -> None:
        try:
            local_mid = int(local_id)
            server_mid = int(message_id)
        except Exception:
            return
        if local_mid >= 0 or server_mid <= 0:
            return

        widget = self._message_widgets.get(local_mid)
        if widget is not None:
            self._promote_local_widget_message_id(widget, msg_id=server_mid, timestamp=int(time.time()))

        if local_mid in self._pending_local_deletes:
            self._pending_local_deletes.discard(local_mid)
            try:
                ok_send = bool(self.server.delete_messages(chat_id=chat_id, message_ids=[server_mid]))
            except Exception:
                ok_send = False
            if ok_send:
                self._remember_delete_echo(chat_id, [server_mid])
                self._on_gui_messages_deleted(chat_id, [server_mid])
                if chat_id == str(self.current_chat_id or ""):
                    self._toast("Сообщение удалено")
            elif chat_id == str(self.current_chat_id or ""):
                self._toast("Не удалось удалить сообщение")

    @Slot(str, dict)
    def _on_peer_message(self, chat_id: str, payload: dict) -> None:
        """Render live incoming/edited text messages (Telegram → server → GUI)."""
        if not chat_id:
            return

        msg_id_raw = payload.get("id") or 0
        try:
            msg_id = int(msg_id_raw)
        except Exception:
            msg_id = 0

        self._ensure_my_id_for_history()
        sender_id = str(payload.get("sender_id") or "")
        mine = bool(self._my_id and sender_id and str(sender_id) == str(self._my_id))
        is_deleted = bool(payload.get("is_deleted", False))
        ts_raw = payload.get("ts")
        try:
            timestamp = int(ts_raw) if ts_raw is not None else None
        except Exception:
            timestamp = None

        is_new_message = self._mark_message_seen(chat_id, msg_id if msg_id else None)
        is_active_chat = chat_id == (self.current_chat_id or "")
        if is_active_chat:
            self._apply_chat_activity(chat_id, ts=timestamp, clear_unread=True, refresh_delay_ms=80)
        else:
            unread_delta = 1 if (not mine and not is_deleted and (is_new_message or msg_id <= 0)) else 0
            self._apply_chat_activity(chat_id, ts=timestamp, unread_delta=unread_delta, refresh_delay_ms=80)
            return

        role = "me" if mine else "other"
        header = "Вы" if mine else str(payload.get("sender") or self.server.get_user_display_name(sender_id) or sender_id or "Неизвестно")

        text = str(payload.get("text") or "")
        hidden_flag = is_zwc_only(text)
        has_hidden = False
        display_text = text
        entities = None
        if contains_zwc(text) or hidden_flag:
            display_text, entities, has_hidden = self._decode_hidden_display(text)
        else:
            maybe_entities = payload.get("entities")
            if isinstance(maybe_entities, list):
                entities = maybe_entities

        reply_to_raw = payload.get("reply_to")
        try:
            reply_to = int(reply_to_raw) if reply_to_raw is not None else None
        except Exception:
            reply_to = None

        forward_info = payload.get("forward_info") if isinstance(payload.get("forward_info"), dict) else None
        reply_markup = payload.get("reply_markup") if isinstance(payload.get("reply_markup"), dict) else None
        reactions = payload.get("reactions") if isinstance(payload.get("reactions"), list) else None
        if reply_markup:
            self._bot_reply_markup_by_chat[chat_id] = reply_markup
            if chat_id == str(self.current_chat_id or ""):
                self._update_bot_keyboard_bar()

        existing = self._message_widgets.get(msg_id) if msg_id else None
        if existing is None and mine and msg_id > 0:
            pending = self._find_pending_local_text_widget(display_text)
            if pending is not None:
                self._promote_local_widget_message_id(pending, msg_id=msg_id, timestamp=timestamp)
                existing = pending
        if existing is not None:
            try:
                setter = getattr(existing, "set_message_text", None)
                if callable(setter):
                    setter(display_text, entities=entities)
                if hasattr(existing, "set_deleted"):
                    existing.set_deleted(is_deleted)
                if hasattr(existing, "set_has_hidden"):
                    existing.set_has_hidden(bool(has_hidden))
                if hasattr(existing, "set_reply_preview"):
                    existing.set_reply_preview(self._build_reply_preview(reply_to))
                if hasattr(existing, "set_forward_info"):
                    existing.set_forward_info(forward_info if isinstance(forward_info, dict) else None)
                if hasattr(existing, "set_reply_markup"):
                    existing.set_reply_markup(reply_markup if isinstance(reply_markup, dict) else None)
                if hasattr(existing, "set_reactions"):
                    existing.set_reactions(reactions if isinstance(reactions, list) else None)
            except Exception:
                pass
        else:
            reply_preview = self._build_reply_preview(reply_to)
            insert_at = self._message_insert_index(msg_id if msg_id else None)
            self.add_text_item(
                header,
                display_text,
                role=role,
                chat_id=chat_id,
                user_id=sender_id,
                entities=entities,
                has_hidden=has_hidden,
                msg_id=msg_id if msg_id else None,
                reply_to=reply_to,
                reply_preview=reply_preview,
                is_deleted=is_deleted,
                forward_info=forward_info,
                reply_markup=reply_markup if isinstance(reply_markup, dict) else None,
                reactions=reactions if isinstance(reactions, list) else None,
                timestamp=timestamp,
                insert_at=insert_at,
            )

        if msg_id:
            self._cache_message(
                msg_id,
                text=text,
                kind="text",
                sender=header,
                reply_to=reply_to,
                is_deleted=is_deleted,
                forward_info=forward_info,
                reactions=reactions if isinstance(reactions, list) else None,
            )
            self._update_reply_references(msg_id)

    @Slot(str, dict)
    def _on_gui_media(self, chat_id: str, payload: dict) -> None:
        if not chat_id:
            return

        kind_raw = (payload.get("mtype") or "").lower()
        kind = {"photo": "image", "gif": "animation"}.get(kind_raw, kind_raw)
        mime = str(payload.get("mime") or "").lower()
        file_name_hint = str(payload.get("file_name") or "").lower()
        if kind == "audio":
            waveform_hint = payload.get("waveform")
            looks_like_voice = (
                isinstance(waveform_hint, list)
                or "audio/ogg" in mime
                or "audio/opus" in mime
                or "application/ogg" in mime
                or file_name_hint.endswith((".ogg", ".oga", ".opus"))
            )
            if looks_like_voice:
                kind = "voice"
        msg_id = int(payload.get("message_id") or payload.get("id") or 0)
        if not msg_id:
            return

        self._ensure_my_id_for_history()
        text = payload.get("text") or ""
        file_path = payload.get("file_path")
        file_size = int(payload.get("file_size") or 0)
        user_id = str(payload.get("user_id") or "")
        mine = bool(self._my_id and user_id and str(user_id) == str(self._my_id))
        is_deleted = bool(payload.get("is_deleted", False))
        ts_raw = payload.get("ts")
        try:
            timestamp = int(ts_raw) if ts_raw is not None else None
        except Exception:
            timestamp = None

        is_new_message = self._mark_message_seen(chat_id, msg_id)
        is_active_chat = chat_id == (self.current_chat_id or "")
        if is_active_chat:
            self._apply_chat_activity(chat_id, ts=timestamp, clear_unread=True, refresh_delay_ms=80)
        else:
            unread_delta = 1 if (not mine and not is_deleted and is_new_message) else 0
            self._apply_chat_activity(chat_id, ts=timestamp, unread_delta=unread_delta, refresh_delay_ms=80)
            return

        role = "me" if mine else "other"
        header = "Вы" if mine else str(payload.get("sender") or self.server.get_user_display_name(user_id) or user_id or "Неизвестно")
        forward_info = payload.get("forward_info") if isinstance(payload.get("forward_info"), dict) else None
        text_entities = payload.get("entities") if isinstance(payload.get("entities"), list) else None
        reply_markup = payload.get("reply_markup") if isinstance(payload.get("reply_markup"), dict) else None
        reactions = payload.get("reactions") if isinstance(payload.get("reactions"), list) else None
        if reply_markup:
            self._bot_reply_markup_by_chat[chat_id] = reply_markup
            if chat_id == str(self.current_chat_id or ""):
                self._update_bot_keyboard_bar()
        duration_raw = payload.get("duration")
        try:
            duration_ms = int(duration_raw) * 1000 if duration_raw else None
        except Exception:
            duration_ms = None
        waveform_payload = payload.get("waveform")
        if isinstance(waveform_payload, list):
            try:
                waveform = [int(v) for v in waveform_payload]
            except Exception:
                waveform = None
        else:
            waveform = None

        if mine and not file_path and file_size > 0 and kind in {"video", "video_note", "audio", "voice", "document"}:
            prefer_kind = "voice" if kind == "audio" else kind
            local = self._match_outgoing_local(prefer_kind, file_size)
            if local and os.path.isfile(local):
                file_path = local

        reply_to_raw = payload.get("reply_to")
        try:
            reply_to = int(reply_to_raw) if reply_to_raw is not None else None
        except Exception:
            reply_to = None
        media_group_id_raw = payload.get("media_group_id")
        media_group_id = str(media_group_id_raw).strip() if media_group_id_raw else None

        reply_preview = self._build_reply_preview(reply_to)

        existing = self._message_widgets.get(msg_id)
        if existing is None and mine and msg_id > 0:
            pending = self._find_pending_local_media_widget(kind, file_size)
            if pending is not None:
                self._promote_local_widget_message_id(pending, msg_id=msg_id, timestamp=timestamp)
                existing = pending
        if existing is not None:
            try:
                if hasattr(existing, "set_deleted"):
                    existing.set_deleted(is_deleted)
                if hasattr(existing, "set_reply_preview"):
                    existing.set_reply_preview(reply_preview)
                if hasattr(existing, "set_forward_info"):
                    existing.set_forward_info(forward_info if isinstance(forward_info, dict) else None)
                if hasattr(existing, "set_reply_markup"):
                    existing.set_reply_markup(reply_markup if isinstance(reply_markup, dict) else None)
                if hasattr(existing, "set_reactions"):
                    existing.set_reactions(reactions if isinstance(reactions, list) else None)
                if hasattr(existing, "set_caption"):
                    existing.set_caption(text, entities=text_entities)
                elif hasattr(existing, "set_message_text"):
                    existing.set_message_text(text, entities=text_entities)
                setattr(existing, "_media_group_id", str(media_group_id or ""))
                if file_path and os.path.isfile(file_path) and hasattr(existing, "show_downloaded_media"):
                    existing.show_downloaded_media(kind, file_path)
            except Exception:
                pass
        else:
            insert_at = self._message_insert_index(msg_id)
            self.add_media_item(
                kind=kind,
                header=header,
                text=text,
                text_entities=text_entities,
                role=role,
                msg_id=msg_id,
                chat_id=chat_id,
                user_id=user_id,
                thumb_path=payload.get("thumb_path"),
                file_path=file_path,
                file_size=file_size,
                reply_to=reply_to,
                reply_preview=reply_preview,
                is_deleted=is_deleted,
                voice_waveform=self._voice_waveform_enabled,
                forward_info=forward_info,
                reply_markup=reply_markup if isinstance(reply_markup, dict) else None,
                reactions=reactions if isinstance(reactions, list) else None,
                duration_ms=duration_ms,
                waveform=waveform,
                media_group_id=media_group_id,
                insert_at=insert_at,
            )

        self._cache_message(
            msg_id,
            text=text,
            kind=kind,
            sender=header,
            reply_to=reply_to,
            is_deleted=is_deleted,
            forward_info=forward_info,
            duration=int(duration_raw) if duration_raw is not None else None,
            waveform=waveform,
            media_group_id=media_group_id,
            reactions=reactions if isinstance(reactions, list) else None,
        )
        self._refresh_media_group_layout()
        self._update_reply_references(msg_id)

    @Slot(str, dict)
    def _on_media_progress(self, chat_id: str, payload: dict) -> None:
        if chat_id != (self.current_chat_id or ""):
            return
        msg_id = int(payload.get("message_id") or payload.get("id") or 0)
        if not msg_id:
            return
        widget = self._message_widgets.get(msg_id)
        if widget:
            try:
                widget.update_download_state(payload)
            except Exception:
                pass

    @Slot(str, list)
    def _on_gui_messages_deleted(self, chat_id: str, message_ids: List[int]) -> None:
        if not message_ids:
            return
        keep_deleted = bool(getattr(self, "_keep_deleted_messages", True))
        current = self.current_chat_id or ""
        for mid in message_ids:
            try:
                key = int(mid)
            except Exception:
                continue
            cached = self._message_cache.get(key)
            was_deleted = bool(cached.get("is_deleted")) if isinstance(cached, dict) else False
            echo_key = (str(chat_id or "").strip(), key)
            suppress_repeat_removal = False
            if was_deleted:
                pending_echo_count = int(self._pending_delete_echo_counts.get(echo_key, 0) or 0)
                if pending_echo_count > 0:
                    suppress_repeat_removal = True
                    if pending_echo_count <= 1:
                        self._pending_delete_echo_counts.pop(echo_key, None)
                    else:
                        self._pending_delete_echo_counts[echo_key] = pending_echo_count - 1
            if isinstance(cached, dict):
                cached["is_deleted"] = True
            elif keep_deleted:
                self._message_cache[key] = {
                    "id": key,
                    "text": "",
                    "kind": "text",
                    "sender": "",
                    "reply_to": None,
                    "is_deleted": True,
                    "forward_info": None,
                    "duration": None,
                    "waveform": None,
                    "media_group_id": None,
                }
            if key in self._selected_message_ids:
                self._selected_message_ids.discard(key)
            refs = self._reply_index.pop(key, [])
            if refs:
                for ref_id in refs:
                    try:
                        self._update_reply_references(int(ref_id))
                    except Exception:
                        continue
            for parent_id, bucket in list(self._reply_index.items()):
                if key in bucket:
                    self._reply_index[parent_id] = [x for x in bucket if x != key]
            if chat_id == current:
                if keep_deleted and was_deleted and not suppress_repeat_removal:
                    self._message_cache.pop(key, None)
                    self._remove_message_widget(key)
                    continue
                if keep_deleted:
                    widget = self._message_widgets.get(key)
                    if widget is not None and hasattr(widget, "set_deleted"):
                        try:
                            widget.set_deleted(True)
                        except Exception:
                            pass
                else:
                    self._message_cache.pop(key, None)
                    self._remove_message_widget(key)
            elif not keep_deleted:
                self._message_cache.pop(key, None)

    def _remove_message_widget(self, msg_id: int) -> None:
        try:
            mid = int(msg_id)
        except Exception:
            return
        widget = self._message_widgets.pop(mid, None)
        self._selected_message_ids.discard(mid)
        self._message_cache.pop(mid, None)
        if not widget:
            return
        try:
            if widget in getattr(self, "_message_order", []):
                self._message_order.remove(widget)
        except Exception:
            pass
        row_wrap = getattr(widget, "_row_wrap", None)
        target = row_wrap if row_wrap is not None else widget
        try:
            disposer = getattr(self, "_dispose_widget_tree", None)
            if callable(disposer):
                disposer(target)
        except Exception:
            pass
        try:
            self.chat_history_layout.removeWidget(target)
        except Exception:
            pass
        try:
            target.deleteLater()
        except Exception:
            pass
        try:
            self._refresh_media_group_layout()
        except Exception:
            pass

    @Slot(str, int)
    def _on_touch_dialog(self, chat_id: str, ts: int) -> None:
        if not chat_id:
            return
        self._apply_chat_activity(chat_id, ts=int(ts or 0), refresh_delay_ms=120)

    def _remember_delete_echo(self, chat_id: str, message_ids: List[int]) -> None:
        cid = str(chat_id or "").strip()
        if not cid:
            return
        for mid in message_ids:
            try:
                key = (cid, int(mid))
            except Exception:
                continue
            self._pending_delete_echo_counts[key] = int(self._pending_delete_echo_counts.get(key, 0) or 0) + 1

    # ------------------------------------------------------------------ #
    # History loading

    def _stop_history_worker(self, *, wait_ms: int = 900, force_terminate: bool = False) -> None:
        thread = getattr(self, "_hist_thread", None)
        worker = getattr(self, "_hist_worker", None)
        if worker is not None and hasattr(worker, "stop"):
            try:
                worker.stop()
            except Exception:
                pass
        if thread is not None:
            try:
                if _qt_is_valid(thread) and thread.isRunning():
                    thread.quit()
                    if not thread.wait(max(0, int(wait_ms or 0))):
                        log.warning(
                            "History thread did not stop within %sms",
                            int(wait_ms or 0),
                        )
                        if force_terminate:
                            try:
                                thread.terminate()
                                thread.wait(1500)
                                log.warning("History thread was force-terminated during shutdown")
                            except Exception:
                                log.exception("Failed to force-terminate history thread")
            except Exception:
                pass
        self._hist_worker = None
        self._hist_thread = None

    def load_chat_history_async(
        self,
        *,
        reset: bool = True,
        limit: Optional[int] = None,
        auto_scroll: Optional[bool] = None,
    ) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            self._loading_history = False
            return

        if auto_scroll is None:
            auto_scroll = False if reset else False
        self._history_auto_scroll_on_finish = bool(auto_scroll)
        self._history_anchor_to_top = bool(reset and not auto_scroll)
        self._history_force_top_on_finish = bool(reset and not auto_scroll)
        self._feed_scroll_lock_mode = "top" if self._history_anchor_to_top else ""
        self._feed_autostick_block_until = time.monotonic() + (1.2 if self._history_anchor_to_top else 0.0)

        if reset:
            self.clear_feed()
            self._clear_jump_indicator()
            self._clear_message_selection()
            self._clear_reply_target()
            self.hide_message_search()
            self._bot_reply_markup_by_chat[str(chat_id)] = None
            self._update_bot_keyboard_bar()
            self._message_cache.clear()
            self._reply_index.clear()
            self._history_load_requested_by_scroll = False
            self._history_scroll_enabled = False
            target_limit = int(limit or getattr(self, "_history_initial_limit", 80) or 80)
        else:
            target_limit = int(limit or getattr(self, "_history_current_limit", 0) or 0)
            if target_limit <= int(getattr(self, "_history_current_limit", 0) or 0):
                return

        self._history_current_limit = max(1, target_limit)
        self._loading_history = True
        self._stop_history_worker()

        if chat_id.lstrip("-").isdigit():
            self._hist_thread = QThread()
            self._hist_thread.setObjectName("history_thread")
            self._orphan_threads.add(self._hist_thread)
            _ORPHAN_QT_THREADS.add(self._hist_thread)
            self._hist_worker = HistoryWorker(
                self.server,
                chat_id,
                limit=self._history_current_limit,
                batch_size=32,
                include_deleted=bool(getattr(self, "_keep_deleted_messages", True)),
            )
            self._hist_worker.moveToThread(self._hist_thread)
            self._hist_thread.started.connect(self._hist_worker.run)
            self._hist_worker.batch.connect(self.on_history_batch)
            self._hist_worker.finished.connect(self.on_history_finished)
            self._hist_worker.finished.connect(self._hist_thread.quit)
            self._hist_worker.finished.connect(self._hist_worker.deleteLater)
            self._hist_thread.finished.connect(self._hist_thread.deleteLater)
            self._hist_thread.finished.connect(lambda th=self._hist_thread: self._orphan_threads.discard(th))
            self._hist_thread.finished.connect(lambda th=self._hist_thread: _ORPHAN_QT_THREADS.discard(th))
            self._hist_thread.start()
            return

        # Local (non-Telegram numeric) history fallback.
        messages = self.history.get("chats", {}).get(chat_id, [])
        if isinstance(messages, list):
            if self._history_current_limit > 0:
                messages = messages[-self._history_current_limit :]
            self.on_history_batch(messages)
        self.on_history_finished()

    @Slot(list)
    def on_history_batch(self, messages: List[Dict[str, Any]]) -> None:
        self._ensure_my_id_for_history()
        self._prime_reply_preview_cache(messages)
        wrap = getattr(self, "chat_history_wrap", None)
        if wrap:
            try:
                wrap.setUpdatesEnabled(False)
            except Exception:
                wrap = None

        pending_reply_updates: set[int] = set()
        try:
            for entry in messages:
                kind_raw = (entry.get("type") or "text").lower()
                if kind_raw == "audio":
                    mime = str(entry.get("mime") or "").lower()
                    file_name_hint = str(entry.get("file_name") or "").lower()
                    waveform_hint = entry.get("waveform")
                    if (
                        isinstance(waveform_hint, list)
                        or "audio/ogg" in mime
                        or "audio/opus" in mime
                        or "application/ogg" in mime
                        or file_name_hint.endswith((".ogg", ".oga", ".opus"))
                    ):
                        kind_raw = "voice"
                text = entry.get("text") or ""
                entities = entry.get("entities") if isinstance(entry.get("entities"), list) else None
                msg_id_raw = entry.get("id")
                try:
                    msg_id = int(msg_id_raw)
                except Exception:
                    msg_id = None
                if msg_id is not None and self.current_chat_id:
                    self._mark_message_seen(self.current_chat_id, msg_id)
                sender_id = str(entry.get("sender_id") or "")
                reply_to_raw = entry.get("reply_to")
                try:
                    reply_to = int(reply_to_raw) if reply_to_raw is not None else None
                except Exception:
                    reply_to = None
                is_deleted = bool(entry.get("is_deleted", False))
                forward_info = entry.get("forward_info")
                reply_markup = entry.get("reply_markup") if isinstance(entry.get("reply_markup"), dict) else None
                if reply_markup and self.current_chat_id:
                    self._bot_reply_markup_by_chat[str(self.current_chat_id)] = reply_markup
                duration_raw = entry.get("duration")
                try:
                    duration_ms = int(duration_raw) * 1000 if duration_raw else None
                except Exception:
                    duration_ms = None
                waveform_entry = entry.get("waveform")
                waveform = list(waveform_entry) if isinstance(waveform_entry, list) else None
                media_group_id_raw = entry.get("media_group_id")
                media_group_id = str(media_group_id_raw).strip() if media_group_id_raw else None

                role_hint = str(entry.get("role") or "").strip().lower()
                mine = bool(self._my_id and sender_id == self._my_id)
                if role_hint == "assistant":
                    role = "assistant"
                else:
                    role = "me" if mine else ("assistant" if kind_raw == "assistant" else "other")
                if role == "assistant":
                    header = str(entry.get("sender") or "🤖 AI")
                else:
                    header = "Вы" if mine else (entry.get("sender") or "Неизвестно")

                reply_preview = self._build_reply_preview(reply_to)
                media_kinds = {"image", "photo", "animation", "gif", "video", "video_note", "audio", "voice", "document", "sticker"}
                kind_for_cache = kind_raw

                existing = self._message_widgets.get(msg_id) if msg_id is not None else None

                if kind_raw in media_kinds and msg_id is not None:
                    normalized = {"photo": "image", "gif": "animation"}.get(kind_raw, kind_raw)
                    kind_for_cache = normalized
                    caption_text = str(text or "")
                    caption_entities = entities
                    caption_has_hidden = False
                    if caption_text and (contains_zwc(caption_text) or is_zwc_only(caption_text)):
                        caption_text, caption_entities, caption_has_hidden = self._decode_hidden_display(caption_text)

                    if existing is not None:
                        try:
                            if hasattr(existing, "set_deleted"):
                                existing.set_deleted(is_deleted)
                            if hasattr(existing, "set_reply_preview"):
                                existing.set_reply_preview(reply_preview)
                            if hasattr(existing, "set_forward_info"):
                                existing.set_forward_info(forward_info if isinstance(forward_info, dict) else None)
                            if hasattr(existing, "set_reply_markup"):
                                existing.set_reply_markup(reply_markup if isinstance(reply_markup, dict) else None)
                            reactions = entry.get("reactions") if isinstance(entry.get("reactions"), list) else None
                            if hasattr(existing, "set_reactions"):
                                existing.set_reactions(reactions if isinstance(reactions, list) else None)
                            if hasattr(existing, "set_caption"):
                                existing.set_caption(caption_text, entities=caption_entities)
                            if hasattr(existing, "set_has_hidden"):
                                existing.set_has_hidden(bool(caption_has_hidden))
                            setattr(existing, "_media_group_id", str(media_group_id or ""))
                            file_path = entry.get("file_path")
                            if file_path and os.path.isfile(file_path) and hasattr(existing, "show_downloaded_media"):
                                existing.show_downloaded_media(normalized, file_path)
                        except Exception:
                            pass
                    else:
                        insert_at = self._message_insert_index(msg_id)
                        self.add_media_item(
                            kind=normalized,
                            header=header,
                            text=caption_text,
                            text_entities=caption_entities,
                            role=role,
                            file_path=entry.get("file_path"),
                            msg_id=msg_id,
                            chat_id=self.current_chat_id,
                            user_id=sender_id,
                            thumb_path=entry.get("thumb_path"),
                            file_size=entry.get("file_size"),
                            has_hidden=caption_has_hidden,
                            reply_to=reply_to,
                            reply_preview=reply_preview,
                            is_deleted=is_deleted,
                            voice_waveform=self._voice_waveform_enabled,
                            forward_info=forward_info if isinstance(forward_info, dict) else None,
                            reply_markup=reply_markup if isinstance(reply_markup, dict) else None,
                            reactions=entry.get("reactions") if isinstance(entry.get("reactions"), list) else None,
                            duration_ms=duration_ms,
                            waveform=waveform,
                            media_group_id=media_group_id,
                            insert_at=insert_at,
                        )
                else:
                    display_text = str(text or "")
                    entities_for_display = entities
                    has_hidden = False
                    if display_text and (contains_zwc(display_text) or is_zwc_only(display_text)):
                        display_text, entities_for_display, has_hidden = self._decode_hidden_display(display_text)

                    if existing is not None:
                        try:
                            setter = getattr(existing, "set_message_text", None)
                            if callable(setter):
                                setter(display_text, entities=entities_for_display)
                            if hasattr(existing, "set_deleted"):
                                existing.set_deleted(is_deleted)
                            if hasattr(existing, "set_reply_preview"):
                                existing.set_reply_preview(reply_preview)
                            if hasattr(existing, "set_forward_info"):
                                existing.set_forward_info(forward_info if isinstance(forward_info, dict) else None)
                            if hasattr(existing, "set_reply_markup"):
                                existing.set_reply_markup(reply_markup if isinstance(reply_markup, dict) else None)
                            reactions = entry.get("reactions") if isinstance(entry.get("reactions"), list) else None
                            if hasattr(existing, "set_reactions"):
                                existing.set_reactions(reactions if isinstance(reactions, list) else None)
                            if hasattr(existing, "set_has_hidden"):
                                existing.set_has_hidden(bool(has_hidden))
                        except Exception:
                            pass
                    else:
                        insert_at = self._message_insert_index(msg_id)
                        self.add_text_item(
                            header,
                            display_text,
                            role=role,
                            chat_id=self.current_chat_id,
                            user_id=sender_id,
                            entities=entities_for_display,
                            has_hidden=has_hidden,
                            msg_id=msg_id,
                            reply_to=reply_to,
                            reply_preview=reply_preview,
                            is_deleted=is_deleted,
                            forward_info=forward_info if isinstance(forward_info, dict) else None,
                            reply_markup=reply_markup if isinstance(reply_markup, dict) else None,
                            reactions=entry.get("reactions") if isinstance(entry.get("reactions"), list) else None,
                            insert_at=insert_at,
                        )
                if not self._loading_history:
                    self._apply_bubble_widths()

                self._cache_message(
                    msg_id,
                    text=text,
                    kind=kind_for_cache,
                    sender=header,
                    reply_to=reply_to,
                    is_deleted=is_deleted,
                    forward_info=forward_info if isinstance(forward_info, dict) else None,
                    duration=int(duration_raw) if duration_raw is not None else None,
                    waveform=waveform,
                    media_group_id=media_group_id,
                    reactions=entry.get("reactions") if isinstance(entry.get("reactions"), list) else None,
                )
                if msg_id is not None:
                    pending_reply_updates.add(msg_id)
            if pending_reply_updates:
                self._flush_reply_updates(pending_reply_updates)
            if self._loading_history:
                self._schedule_media_group_refresh()
            else:
                self._refresh_media_group_layout()
        finally:
            if wrap:
                try:
                    wrap.setUpdatesEnabled(True)
                except Exception:
                    pass
        if getattr(self, "_pending_jump_message_id", None) is not None:
            self._scroll_to_message_widget(int(self._pending_jump_message_id))
        self._update_bot_keyboard_bar()

    @Slot()
    def on_history_finished(self) -> None:
        self._loading_history = False
        if self.current_chat_id:
            self._apply_chat_activity(self.current_chat_id, clear_unread=True, refresh_delay_ms=0)
        if bool(getattr(self, "_history_auto_scroll_on_finish", True)):
            self._feed_scroll_lock_mode = ""
            self._feed_autostick_block_until = 0.0
            QTimer.singleShot(0, self._scroll_to_bottom)
            QTimer.singleShot(60, self._scroll_to_bottom)
        elif bool(getattr(self, "_history_anchor_to_top", False)):
            self._feed_scroll_lock_mode = "top"
            self._feed_autostick_block_until = time.monotonic() + 1.4
            def _scroll_to_history_top() -> None:
                try:
                    bar = self.chat_scroll.verticalScrollBar()
                    bar.setValue(bar.minimum())
                    self._clear_jump_indicator()
                    self._position_jump_button()
                except Exception:
                    pass
            QTimer.singleShot(0, _scroll_to_history_top)
            QTimer.singleShot(60, _scroll_to_history_top)
            QTimer.singleShot(180, _scroll_to_history_top)
            QTimer.singleShot(420, _scroll_to_history_top)
        self._history_anchor_to_top = False
        if getattr(self, "_auto_download_enabled", False):
            QTimer.singleShot(180, self._apply_auto_download_to_feed)
        if bool(getattr(self, "_history_load_requested_by_scroll", False)):
            self._history_load_requested_by_scroll = False
            return
        # Enable scroll-triggered paging after initial render settles.
        QTimer.singleShot(220, lambda: setattr(self, "_history_scroll_enabled", True))
        if getattr(self, "_pending_jump_message_id", None) is not None:
            QTimer.singleShot(40, self._resolve_pending_message_jump)
        if bool(getattr(self, "_history_force_top_on_finish", False)):
            def _force_top() -> None:
                try:
                    bar = self.chat_scroll.verticalScrollBar()
                    bar.setValue(bar.minimum())
                    self._clear_jump_indicator()
                    self._position_jump_button()
                except Exception:
                    pass
            QTimer.singleShot(0, _force_top)
            QTimer.singleShot(80, _force_top)
            QTimer.singleShot(220, _force_top)
            QTimer.singleShot(520, _force_top)
        self._history_force_top_on_finish = False

    # ------------------------------------------------------------------ #
    # Telegram dialogs refresh

    def refresh_telegram_chats_async(self) -> None:
        if not self.tg.is_authorized_sync():
            return
        if getattr(self, "_dialogs_stream_active", False):
            return
        self._dialogs_stream_active = True
        self.sidebar_ui.loading_label.setText("Загружаю чаты…")
        self.sidebar_ui.loading_label.show()

        thread = QThread(self)
        thread.setObjectName("dialogs_stream_thread")
        worker = DialogsStreamWorker(self.server, batch_size=60)
        self._dialogs_workers.add(worker)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.batch.connect(self.on_dialogs_batch)
        worker.done.connect(self._on_dialogs_worker_done)
        worker.done.connect(lambda w=worker: self._dialogs_workers.discard(w))
        worker.done.connect(self._on_dialogs_stream_done)
        worker.done.connect(thread.quit)
        worker.done.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(lambda th=thread: self._on_dialogs_thread_finished(th))
        self._dialogs_threads.add(thread)

        def _fallback() -> None:
            if not self.all_chats:
                chats = self.server.list_all_telegram_chats(limit=400)
                if chats:
                    self.on_dialogs_batch(chats)
                self.sidebar_ui.loading_label.setText("Готово (fallback)")

        QTimer.singleShot(8000, _fallback)
        thread.start()

    @Slot()
    def _on_dialogs_worker_done(self) -> None:
        self._dialogs_stream_active = False

    def _on_dialogs_thread_finished(self, thread: QThread) -> None:
        try:
            self._dialogs_threads.discard(thread)
        except Exception:
            pass
        self._dialogs_stream_active = False

    def _on_avatar_ready(self, kind: str, entity_id: str) -> None:
        target_id = str(entity_id or "")
        if not target_id:
            return
        if kind in {"chat", "user"}:
            candidate_ids = {target_id}
            if kind == "chat":
                try:
                    candidate_ids.update(set(getattr(self, "_chat_id_aliases")(target_id)))
                except Exception:
                    pass
            for cid in list(getattr(self, "_chat_items_by_id", {}).keys()):
                try:
                    aliases = set(getattr(self, "_chat_id_aliases")(cid))
                except Exception:
                    aliases = {cid}
                if not (candidate_ids & aliases):
                    continue
                item = self._chat_items_by_id.get(cid)
                if not item:
                    continue
                item_info = item.data(Qt.ItemDataRole.UserRole + 1) or {}
                info = dict(self.all_chats.get(cid, {}))
                if isinstance(item_info, dict):
                    info.update(dict(item_info))
                title = str(info.get("title_display") or info.get("title") or cid)
                payload = getattr(self, "_chat_list_avatar_payload", None)
                if not callable(payload):
                    continue
                try:
                    pixmap, avatar_key = payload(cid, info, title)
                except Exception:
                    pixmap, avatar_key = None, None
                if pixmap is None:
                    continue
                row_widget = getattr(self, "_chat_row_widgets_by_id", {}).get(cid)
                if row_widget and hasattr(row_widget, "set_avatar_cached"):
                    row_widget.set_avatar_cached(pixmap, cache_key=avatar_key)
                elif row_widget and hasattr(row_widget, "set_avatar"):
                    row_widget.set_avatar(pixmap)

        self._refresh_feed_avatars(kind, target_id)
        if kind == "chat" and target_id == str(self.current_chat_id or ""):
            self._refresh_chat_header()
        if kind == "user" and target_id and target_id == str(getattr(self, "_active_account_user_id", "") or ""):
            try:
                self._sync_account_card()
            except Exception:
                pass
        try:
            self._refresh_visible_chat_avatars()
        except Exception:
            pass

    def _refresh_feed_avatars(self, kind: str, entity_id: str) -> None:
        for idx in range(self.chat_history_layout.count()):
            layout_item = self.chat_history_layout.itemAt(idx)
            container = layout_item.widget() if layout_item else None
            if not container:
                continue
            avatars = container.findChildren(AvatarWidget)
            if not avatars:
                continue
            for avatar in avatars:
                widget_kind = str(avatar.property("avatar_kind") or "")
                widget_id = str(avatar.property("avatar_id") or "")
                if widget_kind != kind or widget_id != entity_id:
                    continue
                try:
                    if kind == "chat":
                        info = self.all_chats.get(entity_id, {"title": entity_id})
                        pixmap = self.avatar_cache.chat(entity_id, info)
                    elif kind == "user":
                        header = avatar.toolTip() or entity_id
                        pixmap = self.avatar_cache.user(entity_id, header)
                    else:
                        continue
                    avatar.set_pixmap(pixmap)
                    avatar.update()
                except Exception:
                    continue

    @Slot()
    def _on_dialogs_stream_done(self) -> None:
        self.sidebar_ui.loading_label.setText("Готово")
        self._schedule_chat_list_refresh(0)
        if not self._resort_timer.isActive():
            self._resort_timer.start()

    @Slot(list)
    def on_dialogs_batch(self, chunk: List[Dict[str, Any]]) -> None:
        new_ids = []
        active_chat = self.current_chat_id or ""
        for info in chunk:
            cid = str(info["id"])
            if cid not in self.all_chats:
                new_ids.append(cid)
            prev = self.all_chats.get(cid, {})
            last_ts = info.get("last_ts")
            if last_ts is None:
                last_ts = info.get("last_message_date")
            if last_ts is None:
                last_ts = prev.get("last_ts", 0)
            try:
                merged_last_ts = max(int(prev.get("last_ts") or 0), int(last_ts or 0))
            except Exception:
                merged_last_ts = int(last_ts or 0)
            incoming_unread = info.get("unread_count", prev.get("unread_count", 0))
            try:
                unread_val = max(0, int(incoming_unread or 0))
            except Exception:
                unread_val = max(0, int(prev.get("unread_count") or 0))
            prev_unread = max(0, int(prev.get("unread_count") or 0))
            merged_unread = 0 if cid == active_chat else max(unread_val, prev_unread)
            self.all_chats[cid] = {
                "title": info.get("title") or prev.get("title") or cid,
                "type": info.get("type") or prev.get("type") or "",
                "last_ts": merged_last_ts,
                "username": info.get("username") or prev.get("username"),
                "photo_small_id": info.get("photo_small_id") or info.get("photo_small") or prev.get("photo_small_id") or prev.get("photo_small"),
                "pinned": bool(info.get("pinned", prev.get("pinned", False))),
                "unread_count": merged_unread,
            }
        self._schedule_chat_list_refresh(60)
        if active_chat and active_chat in self.all_chats:
            self._refresh_chat_header()
        if new_ids and not self._ts_warmup_done:
            self._start_last_ts_worker(new_ids[:40])
            self._ts_warmup_done = True

    def _start_last_ts_worker(self, chat_ids: List[str]) -> None:
        prev_worker = getattr(self, "_ts_worker", None)
        if prev_worker is not None and hasattr(prev_worker, "stop"):
            try:
                prev_worker.stop()
            except Exception:
                pass
        prev_thread = getattr(self, "_ts_thread", None)
        if prev_thread is not None:
            try:
                if prev_thread.isRunning():
                    prev_thread.quit()
                    prev_thread.wait(1000)
            except Exception:
                pass

        thread = QThread(self)
        thread.setObjectName("last_ts_thread")
        worker = LastDateWorker(self.server, chat_ids, limit_each=1)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.tick.connect(self._on_last_ts_tick)
        worker.done.connect(thread.quit)
        worker.done.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.start()
        self._ts_thread = thread
        self._ts_worker = worker

    @Slot(str, int)
    def _on_last_ts_tick(self, chat_id: str, ts: int) -> None:
        if chat_id in self.all_chats:
            self._apply_chat_activity(chat_id, ts=int(ts or 0), refresh_delay_ms=120)

    # ------------------------------------------------------------------ #
    # Message sending

    def _toggle_emoji_picker(self) -> None:
        # Backward-compat: route to the unified media picker.
        self._toggle_media_picker()

    def _toggle_media_picker(self) -> None:
        if not hasattr(self, "btn_media"):
            return
        if self._media_popup is None:
            self._media_popup = MediaPickerPopup(self.tg, self)
            self._media_popup.emojiSelected.connect(self._insert_emoji)
            self._media_popup.stickerSelected.connect(self._send_sticker_file_id)
            self._media_popup.gifSelected.connect(self._send_saved_gif_file_id)
            self._media_popup.gifPickRequested.connect(self._pick_gif_and_send)
        if self._media_popup.isVisible():
            self._media_popup.hide()
        else:
            self._media_popup.popup_above(self.btn_media)

    def _clear_reply_target(self) -> None:
        self._pending_reply_to = None
        bar = getattr(self, "_reply_bar", None)
        if bar is not None:
            try:
                bar.hide()
            except Exception:
                pass

    def _set_reply_target(self, message_id: int) -> None:
        try:
            mid = int(message_id)
        except Exception:
            return
        if mid <= 0:
            return
        preview = self._build_reply_preview(mid) or {
            "id": mid,
            "text": "",
            "kind": "text",
            "sender": "Сообщение",
            "is_deleted": False,
        }
        self._pending_reply_to = mid
        widget = getattr(self, "_reply_preview_widget", None)
        if widget is not None:
            try:
                widget.set_data(preview)
            except Exception:
                pass
        bar = getattr(self, "_reply_bar", None)
        if bar is not None:
            try:
                bar.show()
            except Exception:
                pass
        try:
            self.user_input.setFocus()
        except Exception:
            pass

    def _copy_message_link(self, message_id: int) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        try:
            mid = int(message_id)
        except Exception:
            return
        info = self.all_chats.get(chat_id, {})
        username = str(info.get("username") or "").strip()
        if username:
            link = f"https://t.me/{username}/{mid}"
        else:
            link = f"{chat_id}:{mid}"
        try:
            QApplication.clipboard().setText(link)
            self._toast("Ссылка скопирована")
        except Exception:
            pass

    def _forward_message(self, message_id: int) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        try:
            mid = int(message_id)
        except Exception:
            return
        to_chat_id = self._choose_forward_target_chat()
        if not to_chat_id:
            return
        try:
            ok_send = bool(self.server.forward_message(from_chat_id=chat_id, message_id=mid, to_chat_id=to_chat_id))
        except Exception:
            ok_send = False
        self._toast("Переслано" if ok_send else "Не удалось переслать")

    def _choose_forward_target_chat(self) -> Optional[str]:
        items: List[str] = []
        mapping: Dict[str, str] = {}
        for cid, info in sorted(self.all_chats.items(), key=lambda kv: str(kv[1].get("title") or kv[0]).lower()):
            title = str(info.get("title") or cid)
            label = f"{title} ({cid})"
            items.append(label)
            mapping[label] = str(cid)

        if not items:
            self._toast("Нет доступных чатов для пересылки")
            return None
        chosen, ok = QInputDialog.getItem(self, "Переслать", "Куда:", items, 0, False)
        if not ok or not chosen:
            return None
        return mapping.get(str(chosen))

    def _forward_selected_messages(self) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        mids = sorted({int(mid) for mid in self._selected_message_ids if int(mid) > 0})
        if not mids:
            self._toast("Нет выбранных сообщений")
            return
        to_chat_id = self._choose_forward_target_chat()
        if not to_chat_id:
            return
        try:
            ok_send = bool(self.server.forward_messages(from_chat_id=chat_id, message_ids=mids, to_chat_id=to_chat_id))
        except Exception:
            ok_send = False
        self._toast("Выбранные сообщения пересланы" if ok_send else "Не удалось переслать выбранные сообщения")

    def _delete_message(self, message_id: int) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        keep_deleted = bool(getattr(self, "_keep_deleted_messages", True))
        try:
            mid = int(message_id)
        except Exception:
            return
        if mid <= 0:
            if str(chat_id or "").lstrip("-").isdigit():
                self._pending_local_deletes.add(mid)
                if not keep_deleted:
                    self._purge_messages_locally(chat_id, [mid], purge_storage=False)
                self._toast("Удаление будет завершено после отправки")
                return
            self._purge_messages_locally(chat_id, [mid], purge_storage=False)
            self._toast("Локальное сообщение удалено")
            return
        cached = self._message_cache.get(mid) or {}
        was_deleted = bool(cached.get("is_deleted"))
        if was_deleted:
            self._purge_messages_locally(chat_id, [mid], purge_storage=True)
            self._toast("Удалённое сообщение удалено из чата")
            return
        try:
            ok_send = bool(self.server.delete_messages(chat_id=chat_id, message_ids=[mid]))
        except Exception:
            ok_send = False
        if ok_send:
            self._remember_delete_echo(chat_id, [mid])
            self._on_gui_messages_deleted(chat_id, [mid])
            self._toast("Сообщение удалено")
            return
        self._toast("Не удалось удалить сообщение")

    def _delete_selected_messages(self) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        keep_deleted = bool(getattr(self, "_keep_deleted_messages", True))
        mids_all = sorted({int(mid) for mid in self._selected_message_ids})
        if not mids_all:
            self._toast("Нет выбранных сообщений")
            return
        local_mids = [mid for mid in mids_all if mid <= 0]
        mids = [mid for mid in mids_all if mid > 0]
        has_pending_local_delete = False
        if local_mids:
            if str(chat_id or "").lstrip("-").isdigit():
                for local_mid in local_mids:
                    self._pending_local_deletes.add(int(local_mid))
                has_pending_local_delete = True
                if not keep_deleted:
                    self._purge_messages_locally(chat_id, local_mids, purge_storage=False)
            else:
                self._purge_messages_locally(chat_id, local_mids, purge_storage=False)
        deleteds = [mid for mid in mids if bool((self._message_cache.get(mid) or {}).get("is_deleted"))]
        if deleteds:
            self._purge_messages_locally(chat_id, deleteds, purge_storage=True)
            mids = [mid for mid in mids if mid not in deleteds]
        try:
            ok_send = bool(self.server.delete_messages(chat_id=chat_id, message_ids=mids)) if mids else True
        except Exception:
            ok_send = False
        if ok_send:
            if mids:
                self._remember_delete_echo(chat_id, mids)
                self._on_gui_messages_deleted(chat_id, mids)
            self._clear_message_selection()
            if has_pending_local_delete and mids:
                self._toast("Часть сообщений будет удалена после отправки")
            elif has_pending_local_delete:
                self._toast("Удаление будет завершено после отправки")
            else:
                self._toast("Выбранные сообщения удалены")
            return
        self._toast("Не удалось удалить выбранные сообщения")

    def _set_message_reaction(self, message_id: int, reaction: str) -> None:
        chat_id = str(self.current_chat_id or "")
        if not chat_id:
            return
        try:
            mid = int(message_id)
        except Exception:
            return
        if mid <= 0:
            return
        ok = False
        try:
            ok = bool(self.server.set_message_reaction(chat_id=chat_id, message_id=mid, reaction=reaction))
        except Exception:
            ok = False
        self._toast("Реакция отправлена" if ok else "Не удалось отправить реакцию")

    def _set_message_selected(self, msg_id: int, selected: bool) -> None:
        try:
            mid = int(msg_id)
        except Exception:
            return
        widget = self._message_widgets.get(mid)
        if widget is None:
            # Fallback for widgets that are in feed but absent in id-map.
            for candidate in list(getattr(self, "_message_order", []) or []):
                found = self._message_id_from_widget(candidate)
                try:
                    if found is not None and int(found) == mid:
                        widget = candidate
                        self._message_widgets[mid] = candidate
                        break
                except Exception:
                    continue
        if widget is None:
            return
        setter = getattr(widget, "set_selected", None)
        if callable(setter):
            try:
                setter(bool(selected))
            except Exception:
                pass

    def _set_message_selection_state(self, msg_id: int, selected: bool, *, notify: bool = True) -> None:
        try:
            mid = int(msg_id)
        except Exception:
            return
        if mid == 0:
            return
        already = mid in self._selected_message_ids
        if selected:
            if not already:
                self._selected_message_ids.add(mid)
                self._set_message_selected(mid, True)
        else:
            if already:
                self._selected_message_ids.discard(mid)
                self._set_message_selected(mid, False)
        count = len(self._selected_message_ids)
        if count == 1 and selected:
            self._selection_anchor_mid = mid
            self._selection_last_range_mid = mid
        elif count == 0:
            self._selection_anchor_mid = None
            self._selection_last_range_mid = None
            self._selection_drag_active = False
            self._selection_drag_base_ids.clear()
            self._selection_drag_range_ids.clear()
        if notify:
            self._toast(f"Выбрано: {count}" if count else "Выделение снято")

    def _toggle_message_selection(self, msg_id: int, *, notify: bool = True) -> None:
        try:
            mid = int(msg_id)
        except Exception:
            return
        if mid == 0:
            return
        if mid in self._selected_message_ids:
            self._set_message_selection_state(mid, False, notify=notify)
        else:
            self._set_message_selection_state(mid, True, notify=notify)

    def _ordered_message_ids(self) -> List[int]:
        ordered: List[int] = []
        for widget in list(getattr(self, "_message_order", []) or []):
            mid = self._message_id_from_widget(widget)
            if mid is None:
                continue
            try:
                val = int(mid)
            except Exception:
                continue
            if val > 0:
                ordered.append(val)
        return ordered

    def _extend_message_selection_range(self, target_mid: int) -> None:
        try:
            target = int(target_mid)
        except Exception:
            return
        if target <= 0:
            return
        if self._selection_drag_active:
            self._apply_drag_selection_range(target)
            return
        anchor = self._selection_anchor_mid
        if anchor is None or anchor <= 0:
            self._selection_anchor_mid = target
            self._set_message_selection_state(target, True, notify=False)
            self._selection_last_range_mid = target
            return
        if self._selection_last_range_mid == target:
            return

        ordered = self._ordered_message_ids()
        if not ordered:
            self._set_message_selection_state(target, True, notify=False)
            self._selection_last_range_mid = target
            return
        try:
            i1 = ordered.index(int(anchor))
            i2 = ordered.index(target)
        except ValueError:
            self._set_message_selection_state(target, True, notify=False)
            self._selection_last_range_mid = target
            return
        start = min(i1, i2)
        end = max(i1, i2)
        for mid in ordered[start : end + 1]:
            self._set_message_selection_state(mid, True, notify=False)
        self._selection_last_range_mid = target

    def _apply_drag_selection_range(self, target_mid: int) -> None:
        try:
            target = int(target_mid)
        except Exception:
            return
        if target <= 0:
            return
        anchor = self._selection_anchor_mid
        if anchor is None or anchor <= 0:
            self._selection_anchor_mid = target
            anchor = target
        if self._selection_last_range_mid == target:
            return

        ordered = self._ordered_message_ids()
        if not ordered:
            return
        try:
            i1 = ordered.index(int(anchor))
            i2 = ordered.index(target)
        except ValueError:
            self._selection_last_range_mid = target
            return
        start = min(i1, i2)
        end = max(i1, i2)
        range_ids = set(ordered[start : end + 1])
        base = set(getattr(self, "_selection_drag_base_ids", set()))
        desired = base | range_ids
        current = set(self._selected_message_ids)

        for mid in sorted(current - desired):
            self._selected_message_ids.discard(mid)
            self._set_message_selected(mid, False)
        for mid in sorted(desired - current):
            self._selected_message_ids.add(mid)
            self._set_message_selected(mid, True)

        self._selection_drag_range_ids = range_ids
        self._selection_last_range_mid = target

    def _extend_selection_to_cursor(self) -> None:
        viewport = getattr(getattr(self, "chat_scroll", None), "viewport", lambda: None)()
        if viewport is None:
            return
        try:
            local = viewport.mapFromGlobal(self.cursor().pos())
        except Exception:
            return
        if not viewport.rect().contains(local):
            return
        widget = self._find_message_widget_at(viewport, local)
        mid = self._message_id_from_widget(widget) if widget is not None else None
        if mid is None:
            return
        self._extend_message_selection_range(int(mid))

    def _capture_selected_messages_screenshot(self) -> None:
        mids = sorted({int(mid) for mid in self._selected_message_ids if int(mid) > 0})
        if not mids:
            QMessageBox.information(self, "Скриншот", "Сначала выделите сообщения.")
            return

        wraps: List[QWidget] = []
        for mid in mids:
            widget = self._message_widgets.get(mid)
            if widget is None:
                continue
            wrap = getattr(widget, "_row_wrap", None) or widget
            if isinstance(wrap, QWidget):
                wraps.append(wrap)
        if not wraps:
            QMessageBox.information(self, "Скриншот", "Не удалось получить выделенные сообщения.")
            return

        target_rect: Optional[QRect] = None
        for wrap in wraps:
            rect = wrap.geometry()
            target_rect = rect if target_rect is None else target_rect.united(rect)
        if target_rect is None:
            QMessageBox.information(self, "Скриншот", "Не удалось подготовить область скриншота.")
            return
        target_rect.adjust(-8, -8, 8, 8)
        bounds = self.chat_history_wrap.rect()
        target_rect = target_rect.intersected(bounds)
        if target_rect.isEmpty():
            QMessageBox.information(self, "Скриншот", "Пустая область скриншота.")
            return

        default_name = f"escgram_selected_{int(time.time())}.png"
        out_path, _ = QFileDialog.getSaveFileName(
            self,
            "Сохранить скриншот сообщений",
            default_name,
            "PNG (*.png)",
        )
        if not out_path:
            return
        if not out_path.lower().endswith(".png"):
            out_path += ".png"
        pix = self.chat_history_wrap.grab(target_rect)
        if pix.isNull():
            QMessageBox.warning(self, "Скриншот", "Не удалось создать скриншот.")
            return
        if pix.save(out_path, "PNG"):
            self._toast("Скриншот сохранён")
        else:
            QMessageBox.warning(self, "Скриншот", "Не удалось сохранить файл.")

    def _clear_message_selection(self) -> None:
        ids = list(self._selected_message_ids)
        self._selected_message_ids.clear()
        self._selection_anchor_mid = None
        self._selection_last_range_mid = None
        self._selection_drag_active = False
        self._selection_drag_base_ids.clear()
        self._selection_drag_range_ids.clear()
        if not ids:
            return
        for mid in ids:
            self._set_message_selected(mid, False)

    def _message_id_from_widget(self, widget: object) -> Optional[int]:
        if widget is None:
            return None

        def _extract_mid(obj: object) -> Optional[int]:
            for attr in ("_message_id", "msg_id"):
                try:
                    val = getattr(obj, attr, None)
                except Exception:
                    val = None
                if val is None:
                    continue
                try:
                    mid = int(val)
                except Exception:
                    continue
                if mid != 0:
                    return mid
            return None

        direct = _extract_mid(widget)
        if direct is not None:
            return direct

        # Some message containers store id on nested child widgets.
        if isinstance(widget, QWidget):
            try:
                for child in widget.findChildren(QWidget):
                    mid = _extract_mid(child)
                    if mid is not None:
                        return mid
            except Exception:
                pass
        return None

    def _purge_messages_locally(self, chat_id: str, message_ids: List[int], *, purge_storage: bool) -> None:
        mids: List[int] = []
        for mid in message_ids:
            try:
                mids.append(int(mid))
            except Exception:
                continue
        if not mids:
            return
        current = str(self.current_chat_id or "")
        for key in mids:
            self._selected_message_ids.discard(key)
            self._message_cache.pop(key, None)
            refs = self._reply_index.pop(key, [])
            for ref_id in refs:
                try:
                    self._update_reply_references(int(ref_id))
                except Exception:
                    continue
            for parent_id, bucket in list(self._reply_index.items()):
                if key in bucket:
                    self._reply_index[parent_id] = [x for x in bucket if x != key]
            if chat_id == current:
                self._remove_message_widget(key)
        if purge_storage and hasattr(self.server, "purge_local_messages"):
            try:
                server_mids = [int(mid) for mid in mids if int(mid) > 0]
                if server_mids:
                    self.server.purge_local_messages(chat_id, server_mids)
            except Exception:
                pass

    def _show_message_context_menu(self, widget: QWidget, global_pos: QPoint) -> None:
        msg_id = self._message_id_from_widget(widget)

        def _extract_text() -> str:
            for attr in ("_original_text", "text"):
                try:
                    val = getattr(widget, attr, None)
                except Exception:
                    val = None
                if isinstance(val, str) and val.strip():
                    return val
            try:
                if hasattr(widget, "toPlainText"):
                    return str(widget.toPlainText() or "")
            except Exception:
                pass
            return ""

        def _copy_text() -> None:
            text = _extract_text()
            if not text:
                return
            try:
                QApplication.clipboard().setText(text)
                self._toast("Текст скопирован")
            except Exception:
                pass

        def _quote_text() -> None:
            text = _extract_text()
            if not text:
                return
            # Telegram-like quote block.
            lines = (text or "").strip().splitlines() or [text.strip()]
            quoted = "\n".join([("> " + ln) if ln else ">" for ln in lines])
            try:
                cur = self.user_input.textCursor()
                if cur is not None:
                    if self.user_input.toPlainText().strip():
                        cur.insertText("\n")
                    cur.insertText(quoted + "\n")
                    self.user_input.setTextCursor(cur)
                else:
                    self.user_input.insertPlainText(quoted + "\n")
                self.user_input.setFocus()
            except Exception:
                pass

        menu = QMenu(self)
        StyleManager.instance().bind_stylesheet(menu, "context_menu.message_menu")
        has_server_id = bool(msg_id is not None and int(msg_id) > 0)
        reply_action = menu.addAction("Ответить")
        if has_server_id:
            reply_action.triggered.connect(lambda: self._set_reply_target(int(msg_id)))
        else:
            # Locally-rendered messages may not have Telegram message id yet.
            reply_action.triggered.connect(_quote_text)

        menu.addAction("Копировать текст").triggered.connect(_copy_text)

        if has_server_id:
            mid = int(msg_id)
            reaction_menu = menu.addMenu("Реакция")
            for emoji in ("👍", "❤️", "🔥", "😂", "😢", "😡"):
                reaction_menu.addAction(emoji).triggered.connect(lambda _, e=emoji, mid=mid: self._set_message_reaction(mid, e))
            menu.addSeparator()
            menu.addAction("Статистика сообщения").triggered.connect(lambda: self._show_message_statistics(mid))
            menu.addAction("Анти-накрутка / аналитика").triggered.connect(lambda: self._show_message_statistics(mid))
            menu.addAction("Удалить сообщение").triggered.connect(lambda: self._delete_message(mid))
            menu.addAction("Копировать ссылку на сообщение").triggered.connect(lambda: self._copy_message_link(mid))
            menu.addAction("Переслать").triggered.connect(lambda: self._forward_message(mid))
            if mid in self._selected_message_ids:
                menu.addAction("Снять выделение").triggered.connect(lambda: self._toggle_message_selection(mid))
            else:
                menu.addAction("Выделить").triggered.connect(lambda: self._toggle_message_selection(mid))
        else:
            local_mid = int(msg_id) if msg_id is not None else 0
            menu.addSeparator()
            menu.addAction("Удалить сообщение").triggered.connect(lambda mid=local_mid: self._delete_message(mid))
            react = menu.addMenu("Реакция")
            react.setEnabled(False)
            copy_link = menu.addAction("Копировать ссылку на сообщение")
            copy_link.setEnabled(False)
            forward = menu.addAction("Переслать")
            forward.setEnabled(False)
            if local_mid in self._selected_message_ids:
                menu.addAction("Снять выделение").triggered.connect(lambda mid=local_mid: self._toggle_message_selection(mid))
            else:
                menu.addAction("Выделить").triggered.connect(lambda mid=local_mid: self._toggle_message_selection(mid))

        if self._selected_message_ids:
            menu.addSeparator()
            menu.addAction(f"Удалить выбранные ({len(self._selected_message_ids)})").triggered.connect(self._delete_selected_messages)
            menu.addAction(f"Переслать выбранные ({len(self._selected_message_ids)})").triggered.connect(self._forward_selected_messages)
            menu.addAction(f"Скриншот выбранных ({len(self._selected_message_ids)})").triggered.connect(self._capture_selected_messages_screenshot)
            menu.addAction("Снять выделение со всех").triggered.connect(self._clear_message_selection)

        menu.addSeparator()
        menu.addAction("Пометить чат прочитанным").triggered.connect(lambda: self._mark_current_chat_read(local=False))

        try:
            menu.exec(global_pos)
        finally:
            try:
                menu.deleteLater()
            except Exception:
                pass

    @Slot(object)
    def _show_input_context_menu(self, pos) -> None:
        try:
            menu = self.user_input.createStandardContextMenu()
        except Exception:
            return
        try:
            menu.addSeparator()
            menu.addAction("Жирный").triggered.connect(lambda: self._wrap_input_selection("*", "*"))
            menu.addAction("Курсив").triggered.connect(lambda: self._wrap_input_selection("_", "_"))
            menu.addAction("Подчёркнутый").triggered.connect(lambda: self._wrap_input_selection("__", "__"))
            menu.addAction("Зачёркнутый").triggered.connect(lambda: self._wrap_input_selection("~", "~"))
            menu.addAction("Спойлер").triggered.connect(lambda: self._wrap_input_selection("||", "||"))
            menu.addAction("Скрыть фрагмент (невидимое)").triggered.connect(lambda: self._wrap_input_selection("^", "^"))
            menu.addAction("Код").triggered.connect(lambda: self._wrap_input_selection("`", "`"))
            menu.addAction("Блок кода").triggered.connect(lambda: self._wrap_input_selection("```\n", "\n```"))
            menu.exec(self.user_input.mapToGlobal(pos))
        finally:
            try:
                menu.deleteLater()
            except Exception:
                pass

    def _wrap_input_selection(self, left: str, right: str) -> None:
        cursor = self.user_input.textCursor()
        if cursor is None:
            return
        if cursor.hasSelection():
            selected = cursor.selectedText().replace("\u2029", "\n")
            cursor.insertText(f"{left}{selected}{right}")
        else:
            cursor.insertText(f"{left}{right}")
            try:
                cursor.movePosition(QTextCursor.MoveOperation.Left, QTextCursor.MoveMode.MoveAnchor, len(right))
            except Exception:
                pass
            self.user_input.setTextCursor(cursor)
        self.user_input.setFocus()

    def _hide_input_selection_invisible(self) -> None:
        # Backward compatibility (older callers): use caret markers in the input field.
        self._wrap_input_selection("^", "^")

    def _show_voice_actions_menu(self) -> None:
        btn = getattr(self, "btn_voice", None)
        if btn is None:
            return
        try:
            actions = btn.actions()
        except Exception:
            actions = []
        if not actions:
            return
        menu = QMenu(self)
        for act in actions:
            try:
                menu.addAction(act)
            except Exception:
                continue
        menu.exec(btn.mapToGlobal(btn.rect().bottomLeft()))

    def _pick_sticker_and_send(self) -> None:
        chat_id = self.current_chat_id
        if not chat_id:
            QMessageBox.information(self, "Стикер", "Сначала выберите чат.")
            return
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Выбрать стикер",
            "",
            "Stickers (*.webp *.png *.tgs *.webm);;Images (*.webp *.png);;All files (*.*)",
        )
        if not path:
            return

        try:
            cached_path, size = self._cache_outgoing_media("sticker", path)
        except Exception:
            cached_path = path
            try:
                size = os.path.getsize(path)
            except Exception:
                size = 0

        sender_id = self._my_id or "me"
        ok = False
        if hasattr(self.tg, "send_sticker_sync"):
            try:
                ok = bool(self.tg.send_sticker_sync(chat_id=chat_id, sticker_path=cached_path))
            except Exception:
                ok = False
        elif hasattr(self.tg, "send_document_sync"):
            try:
                ok = bool(self.tg.send_document_sync(chat_id=chat_id, document_path=cached_path))
            except Exception:
                ok = False

        if ok:
            self.add_media_item(
                kind="sticker",
                header="Вы",
                text="",
                role="me",
                file_path=cached_path if os.path.isfile(cached_path) else None,
                file_size=size,
                chat_id=chat_id,
                user_id=sender_id,
            )
            self._toast("Стикер отправлен")
        else:
            QMessageBox.warning(self, "Стикер", "Не удалось отправить стикер.")

    @Slot(str)
    def _insert_emoji(self, emoji: str) -> None:
        try:
            cursor = self.user_input.textCursor()
            cursor.insertText(str(emoji))
            self.user_input.setTextCursor(cursor)
            self.user_input.setFocus()
        except Exception:
            pass
        try:
            if self._media_popup and self._media_popup.isVisible():
                self._media_popup.hide()
        except Exception:
            pass

    @Slot(str)
    def _send_sticker_file_id(self, file_id: str) -> None:
        chat_id = self.current_chat_id
        if not chat_id:
            self._toast("Сначала выберите чат")
            return
        fid = str(file_id or "").strip()
        if not fid:
            return

        self._ensure_my_id_for_history()
        sender_id = self._my_id or "me"
        reply_to = getattr(self, "_pending_reply_to", None)

        mid: Optional[int] = None
        try:
            if hasattr(self.tg, "send_sticker_id_with_id_sync"):
                mid = self.tg.send_sticker_id_with_id_sync(chat_id=chat_id, sticker_file_id=fid, reply_to=reply_to)
            elif hasattr(self.tg, "send_sticker_id_sync"):
                ok = bool(self.tg.send_sticker_id_sync(chat_id=chat_id, sticker_file_id=fid, reply_to=reply_to))
                mid = None if not ok else None
        except Exception:
            mid = None

        if not mid:
            QMessageBox.warning(self, "Стикер", "Не удалось отправить стикер.")
            return

        widget = self.add_media_item(
            kind="sticker",
            header="Вы",
            text="",
            role="me",
            file_path=None,
            file_size=0,
            msg_id=int(mid),
            chat_id=chat_id,
            user_id=sender_id,
        )
        try:
            widget._start_download()
        except Exception:
            pass
        self._clear_reply_target()

        try:
            if self._media_popup and self._media_popup.isVisible():
                self._media_popup.hide()
        except Exception:
            pass

    @Slot()
    def _pick_gif_and_send(self) -> None:
        chat_id = self.current_chat_id
        if not chat_id:
            self._toast("Сначала выберите чат")
            return
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Выбрать GIF",
            "",
            "GIF/Animation (*.gif *.mp4 *.webm);;All files (*.*)",
        )
        if not path:
            return
        stable_path = path
        try:
            size = int(os.path.getsize(path))
        except Exception:
            size = 0

        try:
            cached_path, cached_size = self._cache_outgoing_media("animation", path)
            if cached_path and os.path.isfile(cached_path):
                stable_path = cached_path
                size = int(cached_size or size)
        except Exception:
            pass

        caption_text = str(self.user_input.toPlainText() or "").strip()
        pending_id = self._add_pending_local_media_widget(
            kind="animation",
            chat_id=chat_id,
            media_path=stable_path,
            file_size=size,
            caption_text=caption_text,
            timestamp=int(time.time()),
        )
        if caption_text:
            try:
                self.user_input.clear()
            except Exception:
                pass
        self._clear_reply_target()
        self._start_media_batch_send(
            chat_id=chat_id,
            items=[
                {
                    "kind": "animation",
                    "path": stable_path,
                    "size": int(size or 0),
                    "caption": caption_text,
                }
            ],
            pending_ids=[pending_id],
        )
        try:
            if self._media_popup and self._media_popup.isVisible():
                self._media_popup.hide()
        except Exception:
            pass

    @Slot(str)
    def _send_saved_gif_file_id(self, file_id: str) -> None:
        chat_id = self.current_chat_id
        if not chat_id:
            self._toast("Сначала выберите чат")
            return
        fid = str(file_id or "").strip()
        if not fid:
            return
        reply_to = getattr(self, "_pending_reply_to", None)
        mid: Optional[int] = None
        sender = getattr(self.tg, "send_animation_id_with_id_sync", None)
        if callable(sender):
            try:
                mid = sender(chat_id=chat_id, animation_file_id=fid, reply_to=reply_to)
            except Exception:
                mid = None
        if not mid:
            QMessageBox.warning(self, "GIF", "Не удалось отправить GIF из истории.")
            return
        self._ensure_my_id_for_history()
        sender_id = self._my_id or "me"
        widget = self.add_media_item(
            kind="animation",
            header="Вы",
            text="",
            role="me",
            file_path=None,
            file_size=0,
            msg_id=int(mid),
            chat_id=chat_id,
            user_id=sender_id,
        )
        try:
            widget._start_download()
        except Exception:
            pass
        self._clear_reply_target()

    def send_message(self) -> None:
        raw = self.user_input.toPlainText() or ""
        if not (raw.strip() and self.current_chat_id):
            return
        encoded, _ = encode_caret_hidden_fragments(raw)
        plain, entities = parse_tg_style_markup(encoded)
        if not plain.strip():
            return
        reply_to = getattr(self, "_pending_reply_to", None)
        self.server.gui_send_message(
            chat_id=self.current_chat_id,
            user_id="me",
            text=plain,
            reply_to=reply_to,
            entities=entities or None,
        )
        self.user_input.clear()
        self._clear_reply_target()

    def on_auto_ai_changed(self, state: int) -> None:
        if not self.current_chat_id:
            return
        desired = bool(state)
        if desired:
            self.server.set_ai_flags(self.current_chat_id, ai=True, auto=True)
        else:
            self.server.set_ai_flags(self.current_chat_id, auto=False)
        self.populate_chat_list()
        self.update_ai_controls_state()

    def _send_pressed(self) -> None:
        txt = (self.user_input.toPlainText() or "").strip()
        if not txt:
            self._send_hold.stop()
            self._send_long_mode = False
            return
        self._send_long_mode = False
        self._send_hold.start(420)

    def _send_long(self) -> None:
        self._send_long_mode = True
        try:
            self.btn_send.setText("\u0421\u043a\u0440\u044b\u0442\u043e")
        except Exception:
            pass

    def _send_released(self) -> None:
        txt = (self.user_input.toPlainText() or "").strip()
        if self._send_hold.isActive():
            self._send_hold.stop()
            if txt:
                self.send_message()
        else:
            if txt:
                self.send_message_invisible()
        self._send_long_mode = False
        QTimer.singleShot(200, lambda: self.btn_send.setText(self._send_button_label))

    def send_message_invisible(self) -> None:
        text = (self.user_input.toPlainText() or "").strip()
        if not (text and self.current_chat_id):
            return
        try:
            hidden = encode_zwc(text)
        except Exception as exc:
            QMessageBox.warning(self, "Невидимое", f"Не удалось закодировать: {exc}")
            return
        reply_to = getattr(self, "_pending_reply_to", None)
        self.server.gui_send_message(chat_id=self.current_chat_id, user_id="me", text=hidden, reply_to=reply_to)
        self.user_input.clear()
        self._clear_reply_target()

    def _schedule_history_save(self, delay_ms: int = 350) -> None:
        self._history_save_pending = True
        timer = getattr(self, "_history_save_timer", None)
        if timer is None:
            self._flush_history_save()
            return
        try:
            timer.start(max(0, int(delay_ms)))
        except Exception:
            self._flush_history_save()

    @Slot()
    def _flush_history_save(self) -> None:
        if not getattr(self, "_history_save_pending", False):
            return
        self._history_save_pending = False
        try:
            save_history(self.history)
        except Exception:
            log.exception("Failed to persist history")

    def _remember_history_entry(
        self,
        chat_id: str,
        *,
        role: str,
        text: str,
        sender_id: str = "",
        sender_name: str = "",
        hidden: bool = False,
        decoded_text: Optional[str] = None,
    ) -> None:
        chats = self.history.setdefault("chats", {})
        bucket = chats.setdefault(chat_id, [])
        entry = {
            "id": int(time.time() * 1000),
            "type": "assistant" if role == "assistant" else "text",
            "text": text,
            "sender_id": sender_id,
            "sender": sender_name,
            "role": role,
            "ts": int(time.time()),
        }
        if hidden:
            entry["hidden"] = True
        if decoded_text is not None:
            entry["decoded_text"] = decoded_text
        bucket.append(entry)
        if len(bucket) > self._history_limit:
            del bucket[:-self._history_limit]
        self._schedule_history_save()

    def _voice_pressed(self) -> None:
        self._voice_pressing = True
        # Start recording quickly if user keeps holding.
        self._voice_hold.start(160)

    def _voice_released(self) -> None:
        self._voice_pressing = False
        if self._voice_hold.isActive():
            self._voice_hold.stop()
            # Short click: open actions menu instead of switching modes.
            if not self._recording:
                self._show_voice_actions_menu()
        else:
            if self._recording:
                self._stop_recording_and_send()

    def _voice_long(self) -> None:
        if not self._voice_pressing:
            return
        if self._recording:
            return
        self._start_recording()

    def _update_voice_button(self) -> None:
        if self._recording:
            self.btn_voice.setText("⏺️")
            self.btn_voice.setToolTip("Идёт запись… отпустите, чтобы отправить")
            return
        self.btn_voice.setText("🎙")
        self.btn_voice.setToolTip("Удерживать — запись голосового; клик — меню (аудио/кружок)")

    def _start_recording(self) -> None:
        if not self.current_chat_id:
            QMessageBox.information(self, "Голосовое", "Сначала выберите чат.")
            return
        if not HAVE_SD:
            QMessageBox.warning(
                self,
                "Запись голоса",
                "Для записи установите библиотеки:\n  pip install sounddevice soundfile",
            )
            self._update_voice_button()
            return

        try:
            fd, path = tempfile.mkstemp(suffix=".wav", prefix="rec_")
            os.close(fd)
            self._rec_path = path
            self._rec_writer = sf.SoundFile(path, mode="w", samplerate=48_000, channels=1, subtype="PCM_16")
            self._rec_stream = sd.InputStream(
                samplerate=48_000,
                channels=1,
                callback=lambda indata, *_: self._rec_writer.write(indata.copy()),
            )
            self._rec_stream.start()
            self._recording = True
            self.btn_voice.setText("⏺️")
            self._toast("Запись… отпустите кнопку, чтобы отправить как голосовое")
        except Exception as exc:
            self._recording = False
            self._rec_path = None
            if self._rec_stream:
                try:
                    self._rec_stream.stop()
                    self._rec_stream.close()
                except Exception:
                    pass
            if self._rec_writer:
                try:
                    self._rec_writer.close()
                except Exception:
                    pass
            self._rec_stream = None
            self._rec_writer = None
            QMessageBox.critical(self, "Запись голоса", f"Не удалось начать запись:\n{exc}")
            self._update_voice_button()

    def _stop_recording_and_send(self) -> None:
        try:
            if self._rec_stream:
                self._rec_stream.stop()
                self._rec_stream.close()
            if self._rec_writer:
                self._rec_writer.close()
        finally:
            self._rec_stream = None
            self._rec_writer = None
            self._recording = False
            self._update_voice_button()

        if not self._rec_path:
            return
        path = self._rec_path
        self._rec_path = None
        self._convert_and_send_as_voice(path)

    def _pick_mp3_and_send(self) -> None:
        if not self.current_chat_id:
            QMessageBox.information(self, "Голосовое", "Сначала выберите чат.")
            return
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Выбрать аудио/видео для голосового",
            "",
            "Audio/Video files (*.mp3 *.wav *.m4a *.flac *.ogg *.oga *.mp4 *.m4v *.mov *.webm *.mkv *.avi);;All files (*.*)",
        )
        if path:
            self._convert_and_send_as_voice(path)

    def _pick_video_note_and_send(self) -> None:
        if not self.current_chat_id:
            QMessageBox.information(self, "Кружок", "Сначала выберите чат.")
            return
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Выбрать видео для кружка",
            "",
            "Video files (*.mp4 *.mov *.mkv *.avi *.webm *.m4v);;All files (*.*)",
        )
        if path:
            self._convert_and_send_as_video_note(path)

    def _toast(self, text: str) -> None:
        message = str(text or "").strip()
        if not message:
            return
        lower = message.lower()
        suppressed = (
            "сообщение удалено",
            "удалённое сообщение удалено",
            "выбранные сообщения удалены",
            "файлы отправлены",
            "голосовое отправлено",
            "кружок отправлен",
        )
        if any(phrase in lower for phrase in suppressed):
            return
        if not bool(getattr(self, "_runtime_toasts_enabled", False)):
            return
        layout = getattr(self, "chat_history_layout", None)
        if layout is None:
            log.warning("Toast requested before chat history init: %s", message)
            QMessageBox.information(self, "ESCgram", message)
            return
        info = QLabel(message)
        info.setStyleSheet("color:#9bb6d6; font-size:12px; padding:4px 10px;")
        info.setWordWrap(True)

        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.addStretch(1)
        row.addWidget(info, 0)
        row.addStretch(1)

        wrap = QWidget()
        wrap.setLayout(row)
        idx = max(0, layout.count() - 1)
        layout.insertWidget(idx, wrap)
        self._scroll_to_bottom()
        QTimer.singleShot(2000, wrap.deleteLater)

    def _media_cache_dir(self) -> str:
        base = app_paths.ensure_dir(app_paths.get_data_dir() / "media_cache")
        return str(base)

    def _register_outgoing(self, kind: str, size: int, path: str) -> None:
        bucket = self._recent_outgoing.setdefault(kind, [])
        bucket.append({"size": int(size), "path": path, "ts": time.time()})
        if len(bucket) > 20:
            del bucket[:-20]

    def _cache_outgoing_media(self, kind: str, src_path: str) -> tuple[str, int]:
        size = os.path.getsize(src_path)
        ext = os.path.splitext(src_path)[1] or ""
        name = f"{kind}_{int(time.time())}_{size}{ext}"
        dst = os.path.join(self._media_cache_dir(), name)
        shutil.copy2(src_path, dst)
        self._register_outgoing(kind, size, dst)
        return dst, size

    def _match_outgoing_local(self, kind: str, file_size: int) -> Optional[str]:
        for item in reversed(self._recent_outgoing.get(kind, [])):
            if int(item.get("size") or 0) == int(file_size or 0):
                candidate = str(item.get("path") or "")
                if candidate and os.path.isfile(candidate):
                    return candidate
        return None

    def _next_local_media_message_id(self) -> int:
        seq = int(getattr(self, "_local_media_seq", 0) or 0) + 1
        self._local_media_seq = seq
        # Keep a separate negative range from text local-echo IDs.
        return -1_000_000_000 - seq

    def _add_pending_local_media_widget(
        self,
        *,
        kind: str,
        chat_id: str,
        media_path: str,
        file_size: int,
        caption_text: str = "",
        media_group_id: Optional[str] = None,
        timestamp: Optional[int] = None,
    ) -> Optional[int]:
        if not chat_id or str(chat_id) != str(self.current_chat_id or ""):
            return None

        local_mid = self._next_local_media_message_id()
        reply_to = self._media_send_reply_to
        reply_preview = self._media_send_reply_preview
        resolved_size = int(file_size or 0)
        if resolved_size <= 0 and media_path:
            try:
                resolved_size = int(os.path.getsize(media_path))
            except Exception:
                resolved_size = 0

        try:
            self.add_media_item(
                kind=str(kind or "").strip().lower(),
                header="Вы",
                text=str(caption_text or ""),
                role="me",
                file_path=media_path,
                msg_id=local_mid,
                chat_id=chat_id,
                user_id=str(self._my_id or "me"),
                file_size=resolved_size,
                reply_to=reply_to,
                reply_preview=reply_preview,
                is_deleted=False,
                voice_waveform=self._voice_waveform_enabled,
                forward_info=None,
                media_group_id=media_group_id,
                timestamp=timestamp,
            )
            self._cache_message(
                local_mid,
                text=str(caption_text or ""),
                kind=str(kind or "").strip().lower(),
                sender="Вы",
                reply_to=reply_to,
                is_deleted=False,
                forward_info=None,
                media_group_id=media_group_id,
            )
            if reply_to is not None:
                self._update_reply_references(reply_to)
            return local_mid
        except Exception:
            log.exception("Failed to add local pending media bubble")
            return None

    def _start_media_busy(self, title: str, text: str) -> None:
        self._media_busy_state = {"title": str(title or ""), "text": str(text or "")}
        now = time.monotonic()
        # Do not open separate modal windows: only lightweight in-chat toast.
        if now - float(getattr(self, "_media_busy_last_toast_at", 0.0) or 0.0) >= 0.6:
            self._media_busy_last_toast_at = now
            if text:
                self._toast(text)

    def _stop_media_busy(self) -> None:
        self._media_busy_state = None

    def _media_job_in_progress(self) -> bool:
        if getattr(self, "_media_job_active", False):
            return True
        convert_thread = getattr(self, "_media_convert_thread", None)
        send_thread = getattr(self, "_media_send_thread", None)
        batch_thread = getattr(self, "_media_batch_thread", None)
        return bool(
            (convert_thread is not None and convert_thread.isRunning())
            or (send_thread is not None and send_thread.isRunning())
            or (batch_thread is not None and batch_thread.isRunning())
        )

    def _clear_media_convert_refs(self) -> None:
        self._media_convert_worker = None
        self._media_convert_thread = None

    def _clear_media_send_refs(self) -> None:
        self._media_send_worker = None
        self._media_send_thread = None

    def _clear_media_batch_refs(self) -> None:
        self._media_batch_worker = None
        self._media_batch_thread = None

    @staticmethod
    def _safe_cleanup_temp_dir(tmpdir: Optional[str]) -> None:
        if not tmpdir:
            return
        try:
            if os.path.isdir(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass

    @staticmethod
    def _local_ffmpeg_bin_dir() -> str:
        try:
            return str(app_paths.telegram_workdir() / "ffmpeg" / "bin")
        except Exception:
            return ""

    def _prepend_local_ffmpeg_to_path(self) -> None:
        bin_dir = self._local_ffmpeg_bin_dir()
        if not bin_dir or not os.path.isdir(bin_dir):
            return
        current = os.environ.get("PATH", "")
        parts = [p for p in current.split(os.pathsep) if p]
        norm_target = os.path.normcase(os.path.normpath(bin_dir))
        for part in parts:
            if os.path.normcase(os.path.normpath(part)) == norm_target:
                return
        os.environ["PATH"] = bin_dir + (os.pathsep + current if current else "")

    def _resolve_ffmpeg_binary(self) -> Optional[str]:
        exe = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
        bin_dir = self._local_ffmpeg_bin_dir()
        if bin_dir:
            candidate = os.path.join(bin_dir, exe)
            if os.path.isfile(candidate):
                return candidate
        return shutil.which("ffmpeg")

    def _resolve_ffprobe_binary(self) -> Optional[str]:
        exe = "ffprobe.exe" if os.name == "nt" else "ffprobe"
        bin_dir = self._local_ffmpeg_bin_dir()
        if bin_dir:
            candidate = os.path.join(bin_dir, exe)
            if os.path.isfile(candidate):
                return candidate
        return shutil.which("ffprobe")

    def _active_settings_window(self) -> Optional[SettingsWindow]:
        dlg = getattr(self, "_settings_window", None)
        if dlg is None:
            return None
        try:
            if not _qt_is_valid(dlg):
                return None
        except Exception:
            return None
        return dlg

    def _install_ffmpeg_from_settings(self) -> None:
        thread = getattr(self, "_ffmpeg_install_thread", None)
        if thread is not None and thread.isRunning():
            dlg = self._active_settings_window()
            if dlg and hasattr(dlg, "set_ffmpeg_install_finished"):
                dlg.set_ffmpeg_install_finished(ok=False, message="Установка ffmpeg уже выполняется")
            return
        target_root = str(app_paths.telegram_workdir())
        dlg = self._active_settings_window()
        if dlg and hasattr(dlg, "set_ffmpeg_install_progress"):
            dlg.set_ffmpeg_install_progress("Запущена установка ffmpeg…", 0, 0)
        try:
            thread = QThread(self)
            thread.setObjectName("ffmpeg_install_thread")
            worker = FfmpegInstallWorker(target_root=target_root)
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            worker.progress.connect(self._on_ffmpeg_install_progress)
            worker.finished.connect(self._on_ffmpeg_install_finished)
            worker.finished.connect(thread.quit)
            worker.finished.connect(worker.deleteLater)
            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(lambda: setattr(self, "_ffmpeg_install_thread", None))
            thread.finished.connect(lambda: setattr(self, "_ffmpeg_install_worker", None))
            self._ffmpeg_install_thread = thread
            self._ffmpeg_install_worker = worker
            thread.start()
        except Exception:
            log.exception("Failed to start ffmpeg installer")
            self._ffmpeg_install_thread = None
            self._ffmpeg_install_worker = None
            QMessageBox.warning(self, "ffmpeg", "Не удалось запустить установку ffmpeg.")

    @Slot(str, int, int)
    def _on_ffmpeg_install_progress(self, status: str, done: int, total: int) -> None:
        dlg = self._active_settings_window()
        if dlg and hasattr(dlg, "set_ffmpeg_install_progress"):
            dlg.set_ffmpeg_install_progress(str(status or ""), int(done or 0), int(total or 0))

    @Slot(dict)
    def _on_ffmpeg_install_finished(self, payload: Dict[str, Any]) -> None:
        ok = bool((payload or {}).get("ok"))
        path = str((payload or {}).get("path") or "").strip()
        err = str((payload or {}).get("error") or "").strip()
        dlg = self._active_settings_window()
        if ok and path:
            self._prepend_local_ffmpeg_to_path()
            if dlg and hasattr(dlg, "set_ffmpeg_install_finished"):
                dlg.set_ffmpeg_install_finished(ok=True, message=f"ffmpeg установлен: {path}")
            QMessageBox.information(self, "ffmpeg", f"ffmpeg установлен:\n{path}")
            return
        if dlg and hasattr(dlg, "set_ffmpeg_install_finished"):
            dlg.set_ffmpeg_install_finished(ok=False, message=f"Не удалось установить ffmpeg. {err}")
        QMessageBox.warning(self, "ffmpeg", f"Не удалось установить ffmpeg.\n{err or 'Неизвестная ошибка.'}")

    def _install_voice_dependencies_from_settings(self) -> None:
        thread = getattr(self, "_voice_deps_thread", None)
        if thread is not None and thread.isRunning():
            dlg = self._active_settings_window()
            if dlg and hasattr(dlg, "set_voice_deps_install_finished"):
                dlg.set_voice_deps_install_finished(ok=False, message="Установка зависимостей уже выполняется")
            return
        dlg = self._active_settings_window()
        if dlg and hasattr(dlg, "set_voice_deps_install_progress"):
            dlg.set_voice_deps_install_progress("Запущена установка зависимостей голосовых…")
        try:
            thread = QThread(self)
            thread.setObjectName("voice_deps_install_thread")
            worker = VoiceDepsInstallWorker(target_dir=str(app_paths.telegram_workdir() / "pydeps"))
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            worker.progress.connect(self._on_voice_deps_install_progress)
            worker.finished.connect(self._on_voice_deps_install_finished)
            worker.finished.connect(thread.quit)
            worker.finished.connect(worker.deleteLater)
            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(lambda: setattr(self, "_voice_deps_thread", None))
            thread.finished.connect(lambda: setattr(self, "_voice_deps_worker", None))
            self._voice_deps_thread = thread
            self._voice_deps_worker = worker
            thread.start()
        except Exception:
            log.exception("Failed to start voice dependencies installer")
            self._voice_deps_thread = None
            self._voice_deps_worker = None
            QMessageBox.warning(self, "Голосовые", "Не удалось запустить установку зависимостей.")

    @Slot(str)
    def _on_voice_deps_install_progress(self, text: str) -> None:
        dlg = self._active_settings_window()
        if dlg and hasattr(dlg, "set_voice_deps_install_progress"):
            dlg.set_voice_deps_install_progress(str(text or ""))

    @Slot(dict)
    def _on_voice_deps_install_finished(self, payload: Dict[str, Any]) -> None:
        ok = bool((payload or {}).get("ok"))
        err = str((payload or {}).get("error") or "").strip()
        out = str((payload or {}).get("output") or "").strip()
        target_dir = str((payload or {}).get("target_dir") or "").strip()
        dlg = self._active_settings_window()
        if ok:
            if target_dir and target_dir not in sys.path:
                try:
                    sys.path.insert(0, target_dir)
                except Exception:
                    pass
            message = "Зависимости для голосовых установлены."
            if out:
                message = message + "\n\n" + out[-1200:]
            if dlg and hasattr(dlg, "set_voice_deps_install_finished"):
                dlg.set_voice_deps_install_finished(ok=True, message=message)
            QMessageBox.information(self, "Голосовые", message)
            return
        if dlg and hasattr(dlg, "set_voice_deps_install_finished"):
            dlg.set_voice_deps_install_finished(ok=False, message=f"Не удалось установить зависимости. {err}")
        QMessageBox.warning(self, "Голосовые", f"Не удалось установить зависимости.\n{err or 'Неизвестная ошибка.'}")

    def _build_voice_ffmpeg_cmd(self, ffmpeg: str, src_path: str, dst_path: str) -> List[str]:
        return [
            ffmpeg,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            src_path,
            "-vn",
            "-acodec",
            "libopus",
            "-ar",
            "48000",
            "-ac",
            "1",
            "-b:a",
            "48k",
            "-application",
            "voip",
            "-map_metadata",
            "-1",
            dst_path,
        ]

    def _build_video_note_ffmpeg_cmd(self, ffmpeg: str, src_path: str, dst_path: str) -> List[str]:
        return [
            ffmpeg,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            src_path,
            "-vf",
            r"crop=min(iw\,ih):min(iw\,ih),scale=480:480",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-profile:v",
            "baseline",
            "-level",
            "3.0",
            "-pix_fmt",
            "yuv420p",
            "-r",
            "30",
            "-c:a",
            "aac",
            "-b:a",
            "64k",
            "-ac",
            "1",
            "-ar",
            "44100",
            "-movflags",
            "+faststart",
            "-shortest",
            dst_path,
        ]

    def _prepare_and_preview_media(self, kind: str, src_path: str) -> None:
        chat_id = self.current_chat_id
        if not chat_id:
            return
        if not os.path.isfile(src_path):
            title = "Голосовое" if kind == "voice" else "Кружок"
            QMessageBox.warning(self, title, "Исходный файл не найден.")
            return
        if self._media_job_in_progress():
            self._toast("Дождитесь завершения текущей отправки")
            return

        ffmpeg = self._resolve_ffmpeg_binary()
        if not ffmpeg:
            if kind == "voice":
                QMessageBox.warning(
                    self,
                    "Голосовое",
                    "Не найден ffmpeg — конвертация в OGG/Opus невозможна.\n"
                    "Установите ffmpeg в настройках или добавьте его в PATH.",
                )
            else:
                QMessageBox.warning(
                    self,
                    "Кружок",
                    "Не найден ffmpeg — конвертация в кружок невозможна.\n"
                    "Установите ffmpeg в настройках или добавьте его в PATH.",
                )
            return

        tmpdir = tempfile.mkdtemp(prefix="voice_" if kind == "voice" else "videonote_")
        out_name = "voice.ogg" if kind == "voice" else "note.mp4"
        out_path = os.path.join(tmpdir, out_name)
        self._media_active_tmpdir = tmpdir

        if kind == "voice":
            cmd = self._build_voice_ffmpeg_cmd(ffmpeg, src_path, out_path)
            self._start_media_busy("Голосовое", "Подготавливаю голосовое для предпрослушивания…")
            timeout = 240.0
        else:
            cmd = self._build_video_note_ffmpeg_cmd(ffmpeg, src_path, out_path)
            self._start_media_busy("Кружок", "Подготавливаю кружок для предпросмотра…")
            timeout = 300.0

        self._media_job_active = True
        # Store context on the window instance so the done handler is a real Qt slot,
        # guaranteeing queued delivery to the GUI thread (avoid UI calls from worker thread).
        self._media_convert_ctx = {
            "kind": kind,
            "chat_id": chat_id,
            "src_path": src_path,
            "tmpdir": tmpdir,
            "out_path": out_path,
        }
        try:
            thread = QThread(self)
            thread.setObjectName("media_convert_thread")
            worker = FfmpegConvertWorker(cmd, out_path, timeout_sec=timeout)
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            worker.done.connect(self._on_media_convert_done_payload)
            worker.done.connect(thread.quit)
            worker.done.connect(worker.deleteLater)
            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(self._clear_media_convert_refs)
            self._media_convert_thread = thread
            self._media_convert_worker = worker
            thread.start()
        except Exception as exc:
            log.exception("Failed to start conversion worker (%s): %s", kind, exc)
            self._media_convert_ctx = None
            self._stop_media_busy()
            self._safe_cleanup_temp_dir(tmpdir)
            self._media_active_tmpdir = None
            self._media_job_active = False
            QMessageBox.warning(self, "Отправка медиа", "Не удалось запустить фоновую конвертацию.")

    @Slot(dict)
    def _on_media_convert_done_payload(self, payload: Dict[str, Any]) -> None:
        ctx = getattr(self, "_media_convert_ctx", None)
        self._media_convert_ctx = None
        if not isinstance(ctx, dict) or not ctx:
            # Best-effort cleanup if context is missing (e.g. window is closing).
            try:
                self._stop_media_busy()
            except Exception:
                pass
            try:
                self._safe_cleanup_temp_dir(getattr(self, "_media_active_tmpdir", None))
            except Exception:
                pass
            self._media_active_tmpdir = None
            self._media_job_active = False
            return
        self._on_media_conversion_done(
            str(ctx.get("kind") or ""),
            str(ctx.get("chat_id") or ""),
            str(ctx.get("src_path") or ""),
            str(ctx.get("tmpdir") or ""),
            str(ctx.get("out_path") or ""),
            payload if isinstance(payload, dict) else {},
        )

    def _on_media_conversion_done(
        self,
        kind: str,
        chat_id: str,
        src_path: str,
        tmpdir: str,
        out_path: str,
        payload: Dict[str, Any],
    ) -> None:
        self._stop_media_busy()

        ok = bool((payload or {}).get("ok"))
        err = str((payload or {}).get("error") or "").strip()
        if not ok:
            if err:
                log.warning("Media conversion failed (%s): %s", kind, err)
            msg = (
                "Конвертация голосового не удалась."
                if kind == "voice"
                else "Конвертация кружка не удалась."
            )
            self._toast(msg)
            self._safe_cleanup_temp_dir(tmpdir)
            self._media_active_tmpdir = None
            self._media_job_active = False
            return

        # If user switched chats while conversion was running, silently cancel.
        if str(self.current_chat_id or "") != str(chat_id or ""):
            self._safe_cleanup_temp_dir(tmpdir)
            self._media_active_tmpdir = None
            self._media_job_active = False
            return

        if self._show_inline_media_preview(
            kind,
            chat_id=chat_id,
            src_path=src_path,
            tmpdir=tmpdir,
            out_path=out_path,
        ):
            return

        self._toast("Предпросмотр недоступен, отправляю без него")
        self._send_prepared_media(kind, chat_id, src_path, tmpdir, out_path)

    def _send_prepared_media(
        self,
        kind: str,
        chat_id: str,
        src_path: str,
        tmpdir: str,
        media_path: str,
    ) -> None:
        reply_to = getattr(self, "_pending_reply_to", None)
        reply_preview = self._build_reply_preview(reply_to)
        self._media_send_reply_to = int(reply_to) if reply_to is not None else None
        self._media_send_reply_preview = reply_preview
        # Clear reply bar early, like text sending does.
        self._clear_reply_target()

        if kind == "voice":
            self._start_media_busy("Голосовое", "Отправляю голосовое сообщение…")
            timeout = 140.0
        else:
            self._start_media_busy("Кружок", "Отправляю кружок…")
            timeout = 220.0

        stable_media_path = media_path
        try:
            cached_path, _ = self._cache_outgoing_media(kind, media_path)
            if cached_path and os.path.isfile(cached_path):
                stable_media_path = cached_path
        except Exception:
            log.debug("Failed to cache outgoing %s before send; fallback to temp file", kind, exc_info=True)

        local_msg_id = self._add_pending_local_media_widget(
            kind=kind,
            chat_id=chat_id,
            media_path=stable_media_path,
            file_size=int(os.path.getsize(stable_media_path)) if stable_media_path and os.path.isfile(stable_media_path) else 0,
            timestamp=int(time.time()),
        )

        try:
            thread = QThread(self)
            thread.setObjectName("media_send_thread")
            worker = MediaSendWorker(
                self.tg,
                kind=kind,
                chat_id=chat_id,
                media_path=stable_media_path,
                reply_to=reply_to,
                timeout_sec=timeout,
            )
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            # Store context so we can use a Qt slot (queued to GUI thread).
            self._media_send_ctx = {
                "kind": kind,
                "chat_id": chat_id,
                "src_path": src_path,
                "tmpdir": tmpdir,
                "media_path": stable_media_path,
                "local_msg_id": local_msg_id,
            }
            worker.done.connect(self._on_media_send_done_payload)
            worker.done.connect(thread.quit)
            worker.done.connect(worker.deleteLater)
            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(self._clear_media_send_refs)
            self._media_send_thread = thread
            self._media_send_worker = worker
            thread.start()
        except Exception as exc:
            log.exception("Failed to start send worker (%s): %s", kind, exc)
            self._media_send_ctx = None
            self._stop_media_busy()
            self._safe_cleanup_temp_dir(tmpdir)
            self._media_active_tmpdir = None
            self._media_job_active = False
            self._media_send_reply_to = None
            self._media_send_reply_preview = None
            if local_msg_id is not None:
                self._remove_message_widget(local_msg_id)
            self._toast("Не удалось запустить отправку медиа")

    @Slot(dict)
    def _on_media_send_done_payload(self, payload: Dict[str, Any]) -> None:
        ctx = getattr(self, "_media_send_ctx", None)
        self._media_send_ctx = None
        if not isinstance(ctx, dict) or not ctx:
            try:
                self._stop_media_busy()
            except Exception:
                pass
            try:
                self._safe_cleanup_temp_dir(getattr(self, "_media_active_tmpdir", None))
            except Exception:
                pass
            self._media_active_tmpdir = None
            self._media_job_active = False
            self._media_send_reply_to = None
            self._media_send_reply_preview = None
            return
        self._on_media_send_done(
            str(ctx.get("kind") or ""),
            str(ctx.get("chat_id") or ""),
            str(ctx.get("src_path") or ""),
            str(ctx.get("tmpdir") or ""),
            str(ctx.get("media_path") or ""),
            int(ctx.get("local_msg_id") or 0) or None,
            payload if isinstance(payload, dict) else {},
        )

    def _on_media_send_done(
        self,
        kind: str,
        chat_id: str,
        src_path: str,
        tmpdir: str,
        media_path: str,
        local_msg_id: Optional[int],
        payload: Dict[str, Any],
    ) -> None:
        self._stop_media_busy()

        ok = bool((payload or {}).get("ok"))
        err = str((payload or {}).get("error") or "").strip()
        self._media_send_reply_to = None
        self._media_send_reply_preview = None

        if ok:
            # Do not add a second local bubble here. Wait for Telegram update event
            # and reconcile with cached local path in _on_gui_media.
            if kind == "voice":
                self._toast("Голосовое отправлено")
            else:
                self._toast("Кружок отправлен")
        else:
            if local_msg_id is not None:
                self._remove_message_widget(local_msg_id)
            if err:
                log.warning("Media send failed (%s): %s", kind, err)
            self._toast("Не удалось отправить голосовое" if kind == "voice" else "Не удалось отправить кружок")

        self._safe_cleanup_temp_dir(tmpdir)
        self._media_active_tmpdir = None
        self._media_job_active = False

    def _reconcile_pending_media_after_send(self, chat_id: str, local_msg_id: int) -> None:
        # Kept for backward compatibility with older call-sites.
        # Full history reload here causes visible UI freeze and feed jumps.
        return

    def _convert_and_send_as_voice(self, src_path: str) -> None:
        self._prepare_and_preview_media("voice", src_path)

    def _convert_and_send_as_video_note(self, src_path: str) -> None:
        self._prepare_and_preview_media("video_note", src_path)

    # ------------------------------------------------------------------ #
    # Utility helpers

    def _ensure_authorized(self, prompt_reason: str = "manual") -> None:
        if self.tg.is_authorized_sync():
            self._startup_auth_retries = 0
            self._startup_auth_retry_reason = ""
            self._refresh_account_profile_async()
            self.refresh_telegram_chats_async()
            return
        # A just-started/switched Telegram client may still be warming up.
        # Delay auth prompt a bit unless user explicitly requested adding a new account.
        if prompt_reason != "add_account":
            auth_invalid = bool(getattr(self.tg, "_auth_invalid", False))
            pending_new_session = bool(getattr(self.tg, "_pending_session_name", None))
            has_known_session = False
            if hasattr(self.tg, "list_accounts"):
                try:
                    has_known_session = bool(self.tg.list_accounts())
                except Exception:
                    has_known_session = False
            if not has_known_session and hasattr(self.tg, "current_session_name"):
                try:
                    current = str(self.tg.current_session_name() or "").strip()
                except Exception:
                    current = ""
                checker = getattr(self.tg, "_session_exists", None)
                if current and callable(checker):
                    try:
                        has_known_session = bool(checker(current))
                    except Exception:
                        has_known_session = False

            reason_prev = str(getattr(self, "_startup_auth_retry_reason", "") or "")
            if reason_prev != prompt_reason:
                self._startup_auth_retries = 0
            self._startup_auth_retry_reason = prompt_reason
            retries = int(getattr(self, "_startup_auth_retries", 0) or 0)
            if has_known_session and not auth_invalid and not pending_new_session and retries < 5:
                self._startup_auth_retries = retries + 1
                QTimer.singleShot(1200, lambda: self._ensure_authorized(prompt_reason=prompt_reason))
                return
            self._startup_auth_retries = 0
            self._startup_auth_retry_reason = ""

        dlg = AuthDialog(self.tg, self)
        dlg.login_success.connect(self._handle_login_success)
        dlg.exec()
        if not self.tg.is_authorized_sync() and self._pending_account_revert:
            try:
                if hasattr(self.tg, "cancel_pending_account_session"):
                    self.tg.cancel_pending_account_session(self._pending_account_revert)
                else:
                    self.tg.switch_account(self._pending_account_revert)
            except Exception:
                log.warning("Failed to revert to previous account", exc_info=True)
            finally:
                self._pending_account_revert = None
                self._reset_state_after_account_change()
                self._sync_account_card()

    def _handle_login_success(self) -> None:
        self._pending_account_revert = None
        self._startup_auth_retries = 0
        self._startup_auth_retry_reason = ""
        self._refresh_account_profile_async()
        self.refresh_telegram_chats_async()

    def _refresh_account_profile_async(self) -> None:
        if not hasattr(self.tg, "refresh_active_account_profile"):
            self._sync_account_card()
            return
        thread = getattr(self, "_account_profile_thread", None)
        if thread is not None and thread.isRunning():
            return
        thread = QThread(self)
        thread.setObjectName("account_profile_thread")
        worker = AccountProfileWorker(self.tg)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.done.connect(self._on_account_profile_loaded)
        worker.done.connect(thread.quit)
        worker.done.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        self._account_profile_thread = thread
        thread.start()

    @Slot(dict)
    def _on_account_profile_loaded(self, meta: Dict[str, Any]) -> None:
        try:
            if meta and hasattr(self.tg, "get_active_account_meta"):
                # AccountStore is already updated inside refresh_active_account_profile().
                pass
        except Exception:
            pass
        self._sync_account_card()

    def _record_account_profile(self) -> None:
        if not hasattr(self.tg, "refresh_active_account_profile"):
            return
        meta = self.tg.refresh_active_account_profile()
        if not meta:
            return
        subtitle = meta.get("phone") or (f"@{meta.get('username')}" if meta.get("username") else "")
        if hasattr(self, "settings_panel"):
            avatar = self._account_avatar_pixmap(meta)
            self.settings_panel.set_account_info(meta.get("title") or "Аккаунт", subtitle, avatar)

    def _sync_account_card(self) -> None:
        panel = getattr(self, "settings_panel", None)
        if not panel or not hasattr(self.tg, "get_active_account_meta"):
            return
        info = self.tg.get_active_account_meta() or {}
        self._active_account_user_id = str(info.get("user_id") or info.get("id") or "")
        subtitle = info.get("phone") or (f"@{info.get('username')}" if info.get("username") else "")
        avatar = self._account_avatar_pixmap(info)
        panel.set_account_info(info.get("title") or "Аккаунт", subtitle, avatar)

        if (
            not self._active_account_user_id
            and hasattr(self.tg, "is_authorized_sync")
            and self.tg.is_authorized_sync()
        ):
            self._refresh_account_profile_async()

    def _reset_state_after_account_change(self) -> None:
        self._history_save_pending = False
        timer = getattr(self, "_history_save_timer", None)
        if timer is not None:
            try:
                timer.stop()
            except Exception:
                pass
        try:
            self._stop_active_media_playback()
        except Exception:
            pass
        self.history = load_history()
        self.all_chats.clear()
        self.current_chat_id = None
        self.chat_list.clear()
        self._chat_items_by_id = {}
        self._bot_reply_markup_by_chat.clear()
        self._update_bot_keyboard_bar()
        self._close_chat_details()
        self.clear_feed()
        self._message_widgets.clear()
        self._message_cache.clear()
        self._reply_index.clear()
        self._chat_last_message_id.clear()

    def _open_account_manager(self) -> None:
        dlg = AccountManagerDialog(self.tg, self)
        dlg.account_switched.connect(self._handle_account_switched)
        dlg.account_add_requested.connect(self._handle_account_add_requested)
        dlg.account_deleted.connect(self._sync_account_card)
        dlg.exec()

    def _handle_account_switched(self) -> None:
        self._pending_account_revert = None
        self._reset_state_after_account_change()
        self._sync_account_card()
        self._ensure_authorized(prompt_reason="switch")

    def _handle_account_add_requested(self) -> None:
        try:
            previous = self.tg.current_session_name()
        except Exception:
            previous = None
        self._pending_account_revert = previous
        if hasattr(self.tg, "prepare_new_account_session"):
            new_session = self.tg.prepare_new_account_session()
            log.info("Switching to new session %s for login", new_session)
        else:
            self._toast("Переключение аккаунтов недоступно в этой сборке")
            self._pending_account_revert = None
            return
        self._reset_state_after_account_change()
        self._sync_account_card()
        self._ensure_authorized(prompt_reason="add_account")

    def _account_avatar_pixmap(self, meta: Dict[str, Any]) -> Optional[QPixmap]:
        avatar_cache = getattr(self, "avatar_cache", None)
        if not avatar_cache:
            return None
        user_id = meta.get("user_id") or meta.get("id") or getattr(self, "_my_id", None)
        if not user_id:
            return None
        title = meta.get("title") or meta.get("username") or meta.get("phone") or "User"
        try:
            return avatar_cache.user(str(user_id), title)
        except Exception:
            return None

    def _open_settings_window(self) -> None:
        state = {
            "auto_download": getattr(self, "_auto_download_enabled", False),
            "ghost_mode": getattr(self, "_ghost_mode_enabled", False),
            "voice_waveform": getattr(self, "_voice_waveform_enabled", True),
            "night_mode": getattr(self, "_night_mode_enabled", True),
            "streamer_mode": getattr(self, "_streamer_mode_enabled", False),
            "hide_hidden_chats": getattr(self, "_hide_hidden_chats", True),
            "show_my_avatar": getattr(self, "_show_my_avatar_enabled", True),
            "keep_deleted_messages": getattr(self, "_keep_deleted_messages", True),
            "media_volume": int(getattr(self, "_media_volume_percent", 100) or 100),
            "auto_ai": bool(self.auto_ai_checkbox.isChecked()) if hasattr(self, "auto_ai_checkbox") else False,
        }
        callbacks = {
            "auto_download": self.on_auto_download_setting_changed,
            "ghost_mode": self.on_ghost_mode_setting_changed,
            "voice_waveform": self.on_voice_waveform_setting_changed,
            "night_mode": self._set_night_mode,
            "streamer_mode": lambda checked: self._set_streamer_mode(bool(checked)),
            "hide_hidden_chats": self.on_hide_hidden_chats_setting_changed,
            "show_my_avatar": self.on_show_my_avatar_setting_changed,
            "keep_deleted_messages": self.on_keep_deleted_messages_setting_changed,
            "media_volume": self.on_media_volume_setting_changed,
            "install_ffmpeg": self._install_ffmpeg_from_settings,
            "install_voice_deps": self._install_voice_dependencies_from_settings,
            "refresh_all_avatars": self._refresh_all_avatars_from_settings,
            "scan_all_chats": self._scan_all_chats_from_settings,
            "send_bug_report": self._send_bug_report_from_settings,
        }
        if hasattr(self, "auto_ai_checkbox"):
            callbacks["auto_ai"] = lambda checked: self.auto_ai_checkbox.setChecked(bool(checked))
        ai_state = self._config.get("ai") if isinstance(self._config.get("ai"), dict) else {}
        dlg = SettingsWindow(
            state=state,
            callbacks=callbacks,
            ai_state=dict(ai_state),
            ai_callbacks={"on_change": self._on_ai_settings_changed},
            tool_state=self._snapshot_tools_state(),
            parent=self,
        )
        self._settings_window = dlg
        self._set_settings_tools_busy(self._tools_job_running())
        try:
            dlg.exec()
        finally:
            self._settings_window = None

    def _set_settings_tools_status(self, message: str) -> None:
        window = getattr(self, "_settings_window", None)
        if window is None:
            return
        setter = getattr(window, "set_tools_status", None)
        if callable(setter):
            try:
                setter(str(message or ""))
            except Exception:
                pass

    def _set_settings_tools_progress(self, message: str, done: int = 0, total: int = 0) -> None:
        window = self._active_settings_window()
        if window is None:
            return
        setter = getattr(window, "set_tools_progress", None)
        if callable(setter):
            try:
                setter(str(message or ""), int(done or 0), int(total or 0))
            except Exception:
                pass

    def _set_settings_tools_busy(self, busy: bool) -> None:
        window = self._active_settings_window()
        if window is None:
            return
        setter = getattr(window, "set_tools_busy", None)
        if callable(setter):
            try:
                setter(bool(busy))
            except Exception:
                pass

    def _sync_settings_tool_state(self, key: str) -> None:
        window = self._active_settings_window()
        if window is None or not key:
            return
        setter = getattr(window, "set_tool_state", None)
        if callable(setter):
            try:
                setter(str(key), self._tool_state_snapshot(str(key)))
            except Exception:
                pass

    def _snapshot_tools_state(self) -> Dict[str, Dict[str, Any]]:
        tools_cfg = self._config.get("tools", {})
        if not isinstance(tools_cfg, dict):
            return {}
        snapshot: Dict[str, Dict[str, Any]] = {}
        for key, value in tools_cfg.items():
            if isinstance(value, dict):
                snapshot[str(key)] = dict(value)
        return snapshot

    def _tool_state_snapshot(self, key: str) -> Dict[str, Any]:
        tools_cfg = self._config.get("tools", {})
        if not isinstance(tools_cfg, dict):
            return {}
        value = tools_cfg.get(str(key))
        if isinstance(value, dict):
            return dict(value)
        return {}

    def _persist_tool_state(
        self,
        key: str,
        *,
        ok: bool,
        message: str,
        total: int,
        done: int,
        failed: int,
        stopped: bool = False,
    ) -> None:
        tools_cfg = self._config.setdefault("tools", {})
        state = tools_cfg.setdefault(str(key), {})
        state.update(
            {
                "has_run": True,
                "last_run_at": int(time.time()),
                "last_ok": bool(ok),
                "last_total": max(0, int(total or 0)),
                "last_done": max(0, int(done or 0)),
                "last_failed": max(0, int(failed or 0)),
                "last_stopped": bool(stopped),
                "last_message": str(message or "").strip(),
            }
        )
        try:
            save_config(self._config)
        except Exception:
            log.exception("Failed to save tools state for %s", key)
        self._sync_settings_tool_state(str(key))

    def _tools_job_running(self) -> bool:
        for attr_name in ("_bulk_avatar_thread", "_bulk_stats_thread"):
            thread = getattr(self, attr_name, None)
            if thread is None:
                continue
            try:
                if _qt_is_valid(thread) and thread.isRunning():
                    return True
            except Exception:
                continue
        return False

    def _load_tool_chat_rows(self, *, limit: int = 2000) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}

        def _merge_row(raw_row: Dict[str, Any]) -> None:
            if not isinstance(raw_row, dict):
                return
            cid = str(raw_row.get("id") or "").strip()
            if not cid or cid.startswith("__"):
                return
            prev = dict(merged.get(cid, {}))
            prev.update({key: value for key, value in raw_row.items() if value not in (None, "")})
            prev["id"] = cid
            merged[cid] = prev

        try:
            cached_rows = self.server.list_cached_dialogs(limit=limit)
        except Exception:
            cached_rows = []
        for row in list(cached_rows or []):
            _merge_row(row)

        for cid, info in list(getattr(self, "all_chats", {}).items()):
            row = dict(info or {})
            row.setdefault("id", cid)
            _merge_row(row)

        try:
            remote_rows = self.server.list_all_telegram_chats(limit=limit)
        except Exception:
            remote_rows = []
        for row in list(remote_rows or []):
            _merge_row(row)

        rows: List[Dict[str, Any]] = []
        all_chats = getattr(self, "all_chats", None)
        if not isinstance(all_chats, dict):
            self.all_chats = {}
            all_chats = self.all_chats
        for cid, row in merged.items():
            prev = dict(all_chats.get(cid, {}) or {})
            normalized = dict(prev)
            normalized.update(
                {
                    "id": cid,
                    "title": row.get("title") or prev.get("title") or cid,
                    "type": row.get("type") or prev.get("type") or "private",
                    "username": row.get("username") or prev.get("username") or "",
                    "photo_small_id": row.get("photo_small_id") or row.get("photo_small") or prev.get("photo_small_id") or prev.get("photo_small"),
                    "pinned": bool(row.get("pinned", prev.get("pinned", False))),
                    "last_ts": int(row.get("last_ts") or row.get("last_message_date") or prev.get("last_ts") or 0),
                    "unread_count": max(0, int(row.get("unread_count") or prev.get("unread_count") or 0)),
                }
            )
            all_chats[cid] = normalized
            rows.append(normalized)
        return rows

    def _refresh_all_avatars_from_settings(self) -> tuple[bool, str]:
        if not hasattr(self, "avatar_cache"):
            return False, "Кэш аватарок недоступен."
        if self._tools_job_running():
            message = "Уже выполняется другая операция Tools."
            self._set_settings_tools_status(message)
            return False, message
        rows = self._load_tool_chat_rows(limit=2000)
        if not rows:
            message = "Нет чатов для подгрузки аватарок."
            self._set_settings_tools_status(message)
            return False, message
        try:
            thread = QThread(self)
            thread.setObjectName("bulk_avatar_refresh_thread")
            worker = BulkAvatarRefreshWorker(self.server, rows=rows)
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            worker.progress.connect(self._on_bulk_avatar_progress)
            worker.finished.connect(self._on_bulk_avatar_finished)
            worker.finished.connect(thread.quit)
            worker.finished.connect(worker.deleteLater)
            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(lambda: setattr(self, "_bulk_avatar_worker", None))
            thread.finished.connect(lambda: setattr(self, "_bulk_avatar_thread", None))
            self._bulk_avatar_worker = worker
            self._bulk_avatar_thread = thread
            thread.start()
        except Exception:
            log.exception("Failed to start bulk avatar refresh")
            self._bulk_avatar_worker = None
            self._bulk_avatar_thread = None
            message = "Не удалось запустить подгрузку аватарок."
            self._set_settings_tools_status(message)
            return False, message
        self._set_settings_tools_busy(True)
        self._set_settings_tools_progress(f"Подгрузка аватарок: 0/{len(rows)}", 0, len(rows))
        message = f"Запущена принудительная подгрузка аватарок: {len(rows)}"
        self._set_settings_tools_status(message)
        return True, message

    def _scan_all_chats_from_settings(self) -> tuple[bool, str]:
        if self._tools_job_running():
            msg = "Уже выполняется другая операция Tools."
            self._set_settings_tools_status(msg)
            return False, msg
        rows = self._load_tool_chat_rows(limit=2000)
        chat_ids = [str(row.get("id") or "").strip() for row in list(rows or []) if isinstance(row, dict)]
        chat_ids = [cid for cid in chat_ids if cid and not cid.startswith("__")]
        if not chat_ids:
            msg = "Нет чатов для анализа."
            self._set_settings_tools_status(msg)
            return False, msg
        try:
            thread = QThread(self)
            thread.setObjectName("bulk_chat_statistics_thread")
            worker = BulkChatStatisticsWorker(self.server, chat_ids=chat_ids, limit=0)
            worker.moveToThread(thread)
            thread.started.connect(worker.run)
            worker.progress.connect(self._on_bulk_stats_progress)
            worker.finished.connect(self._on_bulk_stats_finished)
            worker.finished.connect(thread.quit)
            worker.finished.connect(worker.deleteLater)
            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(lambda: setattr(self, "_bulk_stats_worker", None))
            thread.finished.connect(lambda: setattr(self, "_bulk_stats_thread", None))
            self._bulk_stats_worker = worker
            self._bulk_stats_thread = thread
            thread.start()
        except Exception:
            log.exception("Failed to start bulk chat statistics scan")
            msg = "Не удалось запустить общий анализ."
            self._set_settings_tools_status(msg)
            return False, msg
        self._set_settings_tools_busy(True)
        self._set_settings_tools_progress(f"Анализ: 0/{len(chat_ids)}", 0, len(chat_ids))
        msg = f"Запущен анализ чатов: {len(chat_ids)}"
        self._set_settings_tools_status(msg)
        return True, msg

    @Slot(int, int, str)
    def _on_bulk_avatar_progress(self, done: int, total: int, chat_id: str) -> None:
        cid = str(chat_id or "").strip()
        if cid and hasattr(self, "avatar_cache"):
            info = dict(getattr(self, "all_chats", {}).get(cid, {}) or {})
            title = str(info.get("title_display") or info.get("title") or cid)
            chat_type = str(info.get("type") or "").strip().lower()
            try:
                if chat_type in {"private", "user", "bot"} and cid.lstrip("-").isdigit():
                    self.avatar_cache.user(cid, title)
                else:
                    self.avatar_cache.chat(cid, info)
            except Exception:
                pass
        msg = f"Подгрузка аватарок: {int(done)}/{int(total)}"
        if cid:
            msg += f" • {cid}"
        self._set_settings_tools_progress(msg, int(done), int(total))
        self._set_settings_tools_status(msg)

    @Slot(dict)
    def _on_bulk_avatar_finished(self, payload: Dict[str, Any]) -> None:
        data = dict(payload or {})
        refreshed = int(data.get("refreshed") or 0)
        total = int(data.get("total") or 0)
        failed = int(data.get("failed") or 0)
        stopped = bool(data.get("stopped"))
        if stopped:
            msg = f"Подгрузка остановлена: {refreshed}/{total}, ошибок {failed}"
        else:
            msg = f"Подгрузка завершена: {refreshed}/{total}, ошибок {failed}"
        self._set_settings_tools_progress(msg, total if total > 0 and not stopped else refreshed, total)
        self._set_settings_tools_status(msg)
        self._persist_tool_state(
            "refresh_all_avatars",
            ok=bool(data.get("ok")) and not stopped and failed == 0,
            message=msg,
            total=total,
            done=refreshed,
            failed=failed,
            stopped=stopped,
        )
        self._set_settings_tools_busy(False)

    @Slot(int, int, str)
    def _on_bulk_stats_progress(self, done: int, total: int, chat_id: str) -> None:
        msg = f"Анализ: {int(done)}/{int(total)} • {str(chat_id or '')}"
        self._set_settings_tools_progress(msg, int(done), int(total))
        self._set_settings_tools_status(msg)

    @Slot(dict)
    def _on_bulk_stats_finished(self, payload: Dict[str, Any]) -> None:
        data = dict(payload or {})
        scanned = int(data.get("scanned") or 0)
        total = int(data.get("total") or 0)
        failed = int(data.get("failed") or 0)
        if bool(data.get("stopped")):
            msg = f"Анализ остановлен: {scanned}/{total}, ошибок {failed}"
        else:
            msg = f"Анализ завершён: {scanned}/{total}, ошибок {failed}"
        self._set_settings_tools_progress(msg, total if total > 0 and not bool(data.get("stopped")) else scanned, total)
        self._set_settings_tools_status(msg)
        self._persist_tool_state(
            "scan_all_chats",
            ok=bool(data.get("ok")) and not bool(data.get("stopped")) and failed == 0,
            message=msg,
            total=total,
            done=scanned,
            failed=failed,
            stopped=bool(data.get("stopped")),
        )
        self._set_settings_tools_busy(False)

    @staticmethod
    def _read_log_tail(path: str, *, max_lines: int = 260, max_chars: int = 40000) -> str:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                lines = fh.readlines()
        except Exception:
            return ""
        tail = "".join(lines[-max_lines:])
        if len(tail) > max_chars:
            tail = tail[-max_chars:]
        return tail.strip()

    def _collect_bug_report_logs(self) -> str:
        entries: List[str] = []
        try:
            entries = [str(p) for p in current_log_files() if p.is_file()]
        except Exception:
            entries = []
        if not entries:
            logs_root = app_paths.logs_dir()
            try:
                entries = sorted(
                    [str(p) for p in logs_root.iterdir() if p.is_file()],
                    key=lambda p: os.path.getmtime(p),
                    reverse=True,
                )
            except Exception:
                entries = []
        parts: List[str] = []
        for path in entries[:5]:
            tail = self._read_log_tail(path)
            if not tail:
                continue
            name = os.path.basename(path)
            parts.append(f"### {name}\n```text\n{tail}\n```")
        return "\n\n".join(parts)

    def _send_bug_report_from_settings(self, comment: str) -> tuple[bool, str]:
        repo = str(getattr(self, "_update_repo", "") or get_update_repo()).strip()
        if "/" not in repo:
            return False, "Не задан GitHub-репозиторий для баг-репорта."
        comment_text = str(comment or "").strip()
        if not comment_text:
            comment_text = "Без описания (отправлено из формы баг-репорта)."
        version = str(getattr(self, "_app_version", "") or get_app_version())
        logs_block = self._collect_bug_report_logs()
        log_dir = current_log_dir() or app_paths.logs_dir()
        os_label = " ".join(
            part for part in (
                platform.system(),
                platform.release(),
                platform.version(),
                platform.machine(),
            ) if str(part or "").strip()
        ).strip() or sys.platform
        title = f"[BUG] ESCgram {version} - {time.strftime('%Y-%m-%d %H:%M:%S')}"
        body = (
            "### Описание\n"
            f"{comment_text}\n\n"
            "### Окружение\n"
            f"- Версия: `{version}`\n"
            f"- OS: `{os_label}`\n"
            f"- Python: `{sys.version.split()[0]}`\n\n"
            "### Пути\n"
            f"- Data dir: `{app_paths.get_data_dir()}`\n"
            f"- Log dir: `{log_dir}`\n\n"
            "### Логи\n"
            f"{logs_block or 'Логи не найдены.'}\n"
        )

        token = str(os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN") or "").strip()
        if token:
            try:
                import requests

                api = f"https://api.github.com/repos/{repo}/issues"
                resp = requests.post(
                    api,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/vnd.github+json",
                    },
                    json={"title": title[:200], "body": body[:64000]},
                    timeout=(5.0, 18.0),
                )
                if 200 <= int(resp.status_code) < 300:
                    data = resp.json() if resp.content else {}
                    url = str(data.get("html_url") or "").strip()
                    if url:
                        return True, f"Баг-репорт отправлен: {url}"
                    return True, "Баг-репорт отправлен."
            except Exception:
                log.exception("Failed to submit bug report via GitHub API")

        try:
            issue_url = (
                f"https://github.com/{repo}/issues/new"
                f"?title={urllib.parse.quote(title[:180])}"
                f"&body={urllib.parse.quote(body[:7000])}"
            )
            webbrowser.open(issue_url)
            return True, "Открыта форма issue на GitHub. Проверьте и отправьте репорт в браузере."
        except Exception:
            return False, "Не удалось открыть GitHub для отправки баг-репорта."

    def _shutdown_background_workers(self) -> None:
        def _thread_is_running(thread: object) -> bool:
            if thread is None:
                return False
            try:
                if not _qt_is_valid(thread):
                    return False
            except Exception:
                pass
            try:
                return bool(getattr(thread, "isRunning")())
            except Exception:
                return False

        def _drain_tracked_threads(threads: Iterable[object], wait_ms: int) -> None:
            active_threads: List[QThread] = []
            for thread in list(threads or []):
                if not _thread_is_running(thread):
                    continue
                try:
                    if hasattr(thread, "requestInterruption"):
                        thread.requestInterruption()
                except Exception:
                    pass
                try:
                    thread.quit()
                except Exception:
                    pass
                active_threads.append(thread)
            for thread in active_threads:
                try:
                    thread.wait(max(0, int(wait_ms or 0)))
                except Exception:
                    pass

        for timer_name in (
            "_timer",
            "_resort_timer",
            "_repaint_timer",
            "_bubble_width_timer",
            "_send_hold",
            "_voice_hold",
            "_history_save_timer",
            "_global_search_timer",
        ):
            timer = getattr(self, timer_name, None)
            if timer is None:
                continue
            try:
                timer.stop()
            except Exception:
                pass

        pump = getattr(self, "pump", None)
        if pump is not None:
            try:
                pump._running = False
            except Exception:
                pass
        pump_thread = getattr(self, "pump_thread", None)
        if _thread_is_running(pump_thread):
            try:
                pump_thread.quit()
                pump_thread.wait(1500)
            except Exception:
                pass

        dialog_workers = list(getattr(self, "_dialogs_workers", set()) or [])
        for worker in dialog_workers:
            if worker is None:
                continue
            if hasattr(worker, "stop"):
                try:
                    worker.stop()
                except Exception:
                    pass
        self._dialogs_workers.clear()

        dialog_threads = list(getattr(self, "_dialogs_threads", set()) or [])
        for dthread in dialog_threads:
            if not _thread_is_running(dthread):
                continue
            try:
                dthread.quit()
                dthread.wait(1500)
            except Exception:
                pass
        self._dialogs_threads.clear()
        self._dialogs_stream_active = False

        self._stop_history_worker(wait_ms=20000, force_terminate=True)
        self._loading_history = False
        self._stop_chat_profile_loader()

        ts_thread = getattr(self, "_ts_thread", None)
        ts_worker = getattr(self, "_ts_worker", None)
        if ts_worker is not None and hasattr(ts_worker, "stop"):
            try:
                ts_worker.stop()
            except Exception:
                pass
        if _thread_is_running(ts_thread):
            try:
                ts_thread.quit()
                ts_thread.wait(1000)
            except Exception:
                pass
        self._ts_worker = None
        self._ts_thread = None
        self._stop_global_search_worker()

        update_thread = getattr(self, "_update_thread", None)
        update_worker = getattr(self, "_update_worker", None)
        if update_worker is not None and hasattr(update_worker, "stop"):
            try:
                update_worker.stop()
            except Exception:
                pass
        if _thread_is_running(update_thread):
            try:
                update_thread.quit()
                update_thread.wait(10000)
            except Exception:
                pass
        self._update_worker = None
        self._update_thread = None

        update_dl_thread = getattr(self, "_update_download_thread", None)
        update_dl_worker = getattr(self, "_update_download_worker", None)
        if update_dl_worker is not None and hasattr(update_dl_worker, "stop"):
            try:
                update_dl_worker.stop()
            except Exception:
                pass
        if _thread_is_running(update_dl_thread):
            try:
                update_dl_thread.quit()
                update_dl_thread.wait(5000)
            except Exception:
                pass
        self._update_download_worker = None
        self._update_download_thread = None

        ffmpeg_thread = getattr(self, "_ffmpeg_install_thread", None)
        if _thread_is_running(ffmpeg_thread):
            try:
                ffmpeg_thread.quit()
                ffmpeg_thread.wait(1200)
            except Exception:
                pass
        self._ffmpeg_install_worker = None
        self._ffmpeg_install_thread = None

        voice_deps_thread = getattr(self, "_voice_deps_thread", None)
        if _thread_is_running(voice_deps_thread):
            try:
                voice_deps_thread.quit()
                voice_deps_thread.wait(1200)
            except Exception:
                pass
        self._voice_deps_worker = None
        self._voice_deps_thread = None

        profile_thread = getattr(self, "_account_profile_thread", None)
        if _thread_is_running(profile_thread):
            try:
                profile_thread.quit()
                profile_thread.wait(1000)
            except Exception:
                pass
        self._account_profile_thread = None

        self._stop_media_busy()
        self._media_job_active = False
        self._safe_cleanup_temp_dir(getattr(self, "_media_active_tmpdir", None))
        self._media_active_tmpdir = None

        convert_thread = getattr(self, "_media_convert_thread", None)
        if _thread_is_running(convert_thread):
            try:
                convert_thread.quit()
                convert_thread.wait(1000)
            except Exception:
                pass
        self._clear_media_convert_refs()

        send_thread = getattr(self, "_media_send_thread", None)
        if _thread_is_running(send_thread):
            try:
                send_thread.quit()
                send_thread.wait(1500)
            except Exception:
                pass
        self._clear_media_send_refs()
        self._media_send_ctx = None

        batch_thread = getattr(self, "_media_batch_thread", None)
        if _thread_is_running(batch_thread):
            try:
                batch_thread.quit()
                batch_thread.wait(1500)
            except Exception:
                pass
        self._clear_media_batch_refs()
        self._media_batch_ctx = None

        try:
            self.clear_feed()
        except Exception:
            pass
        try:
            _drain_tracked_threads(getattr(message_widgets_module, "_MESSAGE_WIDGET_THREADS", set()), wait_ms=2500)
        except Exception:
            pass
        try:
            _drain_tracked_threads(getattr(media_render_module, "_MEDIA_RENDER_THREADS", set()), wait_ms=2500)
        except Exception:
            pass

    def _init_system_tray(self) -> None:
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return
        try:
            icon = self.windowIcon()
            if icon.isNull():
                icon_path = resolve_app_icon_path()
                if icon_path:
                    icon = QIcon(icon_path)
            tray = QSystemTrayIcon(icon, self)
            tray.setToolTip("ESCgram")
            menu = QMenu(self)
            act_open = menu.addAction("Открыть ESCgram")
            act_exit = menu.addAction("Выход")
            act_open.triggered.connect(self._restore_from_tray)

            def _quit_from_tray() -> None:
                self._tray_quit_requested = True
                app = QApplication.instance()
                if app is not None:
                    app.quit()

            act_exit.triggered.connect(_quit_from_tray)
            tray.setContextMenu(menu)
            tray.activated.connect(self._on_tray_activated)
            tray.show()
            self._tray_icon = tray
            self._tray_menu = menu
        except Exception:
            self._tray_icon = None
            self._tray_menu = None
            log.exception("Failed to initialize system tray")

    @Slot(QSystemTrayIcon.ActivationReason)
    def _on_tray_activated(self, reason) -> None:
        try:
            if reason in (
                QSystemTrayIcon.ActivationReason.Trigger,
                QSystemTrayIcon.ActivationReason.DoubleClick,
                QSystemTrayIcon.ActivationReason.MiddleClick,
            ):
                self._restore_from_tray()
        except Exception:
            pass

    def _restore_from_tray(self) -> None:
        try:
            self.show()
            self.setWindowState((self.windowState() & ~Qt.WindowState.WindowMinimized) | Qt.WindowState.WindowActive)
            self.raise_()
            self.activateWindow()
        except Exception:
            pass

    def _minimize_to_tray(self) -> None:
        tray = getattr(self, "_tray_icon", None)
        if tray is None:
            return
        if not self.isMinimized():
            return
        try:
            self.hide()
            if not self._tray_minimize_notice_shown:
                self._tray_minimize_notice_shown = True
                tray.showMessage(
                    "ESCgram",
                    "Приложение свернуто в системный трей.",
                    QSystemTrayIcon.MessageIcon.Information,
                    1800,
                )
        except Exception:
            pass

    def changeEvent(self, event) -> None:  # type: ignore[override]
        try:
            if event is not None and event.type() == QEvent.Type.WindowStateChange:
                if self.isMinimized() and getattr(self, "_tray_icon", None) is not None:
                    QTimer.singleShot(0, self._minimize_to_tray)
        except Exception:
            pass
        super().changeEvent(event)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        tray = getattr(self, "_tray_icon", None)
        if tray is not None:
            try:
                tray.hide()
            except Exception:
                pass
        self._shutdown_background_workers()
        self._flush_history_save()
        save_history(self.history)
        self._persist_window_config()
        save_config(self._config)
        try:
            self.avatar_cache.shutdown()
        except Exception:
            pass
        super().closeEvent(event)

    def _ensure_my_id_for_history(self) -> None:
        if self._my_id:
            return
        for name in ("get_self_id", "get_my_id", "get_self_user_id", "my_user_id", "get_self_id_sync"):
            if hasattr(self.tg, name):
                try:
                    mid = getattr(self.tg, name)()
                    if mid:
                        self._my_id = str(mid)
                        return
                except Exception:
                    continue


def run_gui(server, tg_adapter) -> None:
    # When GUI is launched directly, keep logs in the selected data dir by default.
    configure_logging(log_directory=os.getenv("DRAGO_LOG_DIR") or str(app_paths.logs_dir()))
    if os.name == "nt":
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("lReDragol.ESCgram")
        except Exception:
            pass
    app = QApplication.instance() or QApplication([])
    icon_path = resolve_app_icon_path()
    if icon_path:
        try:
            app.setWindowIcon(QIcon(icon_path))
        except Exception:
            pass
    config = load_config()
    window = ChatWindow(server, tg_adapter, config=config)
    window.show()
    app.exec()
