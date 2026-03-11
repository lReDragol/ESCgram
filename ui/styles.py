from __future__ import annotations

import copy
import json
import weakref
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from PySide6.QtCore import QObject, Signal, QTimer
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication, QWidget

from utils import app_paths


def _deep_merge(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge dictionaries and return dst."""
    for key, value in src.items():
        if (
            key in dst
            and isinstance(dst[key], dict)
            and isinstance(value, dict)
        ):
            _deep_merge(dst[key], value)
        else:
            dst[key] = copy.deepcopy(value)
    return dst


def _ensure_path(tree: Dict[str, Any], parts: List[str]) -> Tuple[Dict[str, Any], str]:
    current = tree
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    return current, parts[-1]


class StyleManager(QObject):
    """Loads theme definition from styles.json and manages bindings."""

    style_changed = Signal(dict)
    profile_changed = Signal(str)

    _instance: Optional["StyleManager"] = None

    def __init__(self) -> None:
        super().__init__()
        self._bundled_path = Path(__file__).with_name("styles.json")
        self._path = self._resolve_styles_path()
        self._bindings: List[Tuple[weakref.ReferenceType[QWidget], str]] = []
        self._subscribers: List[Callable[[], None]] = []
        self._themes: Dict[str, Dict[str, Any]] = {}
        self._presets: Dict[str, Dict[str, Any]] = {}
        self._active_name: Optional[str] = None
        self._active_store: Dict[str, Any] = {}
        self._active_profile: Dict[str, Any] = {}
        self._bindings_refresh_pending = False
        self._load()
        self.apply_palette()

    # ------------------------------------------------------------------ #
    # Singleton helpers

    @classmethod
    def instance(cls) -> "StyleManager":
        if cls._instance is None:
            cls._instance = StyleManager()
        return cls._instance

    # ------------------------------------------------------------------ #

    def _load(self) -> None:
        if not self._path.exists():
            # Fallback: bundled theme file.
            self._path = self._bundled_path
        if not self._path.exists():
            raise FileNotFoundError(f"styles.json not found at {self._path}")
        with self._path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        self._themes = data.setdefault("themes", {})
        self._presets = data.setdefault("presets", {})
        self._data = data
        if not self._themes:
            raise ValueError("styles.json must define at least one theme")
        self._active_name = data.get("active") or next(iter(self._themes))
        self._select_store(self._active_name)

    def _resolve_styles_path(self) -> Path:
        """
        Use a writable copy of styles.json stored in the selected data dir.

        This avoids writing into the install directory (e.g. Program Files) when
        persisting the selected theme/profile.
        """
        try:
            user_path = app_paths.get_data_dir() / "styles.json"
            if user_path.is_file():
                return user_path
            # Copy bundled themes on first run.
            if self._bundled_path.is_file():
                user_path.parent.mkdir(parents=True, exist_ok=True)
                user_path.write_text(self._bundled_path.read_text(encoding="utf-8"), encoding="utf-8")
                return user_path
        except Exception:
            pass
        return self._bundled_path

    def _select_store(self, name: str) -> None:
        if name in self._themes:
            self._active_store = self._themes[name]
        elif name in self._presets:
            preset = self._presets[name]
            self._active_store = preset.setdefault("data", {})
        else:
            fallback = next(iter(self._themes))
            self._active_store = self._themes[fallback]
            name = fallback
        self._active_name = name
        self._active_profile = copy.deepcopy(self._active_store)
        if self._data.get("active") != name:
            self._data["active"] = name
            self._write()

    def reload(self) -> None:
        self._load()
        self.apply_palette()
        self._refresh_bindings()
        self._notify_subscribers()
        self.style_changed.emit(self._active_profile)

    # ------------------------------------------------------------------ #
    # Accessors

    @property
    def active_profile(self) -> Dict[str, Any]:
        return self._active_profile

    def active_profile_name(self) -> str:
        return self._active_name or next(iter(self._themes))

    def profile_list(self) -> List[Dict[str, str]]:
        profiles: List[Dict[str, str]] = []
        for key, entry in self._themes.items():
            profiles.append({
                "name": key,
                "label": entry.get("label", key.title()),
                "type": "theme",
            })
        for key, entry in self._presets.items():
            profiles.append({
                "name": key,
                "label": entry.get("label", key.title()),
                "type": "preset",
            })
        return profiles

    def palette_value(self, key: str, default: Optional[str] = None) -> Optional[str]:
        palette = self._active_profile.get("palette", {})
        return palette.get(key, default)

    def metric(self, key: str, default: Any = None) -> Any:
        sentinel = object()
        value = self.value(key, sentinel)
        if value is not sentinel:
            return value
        combined_key = f"metrics.{key}" if not key.startswith("metrics.") else key
        metrics = self.value(combined_key, sentinel)
        return default if metrics is sentinel else metrics

    def stylesheet(
        self,
        key: str,
        mapping: Optional[Dict[str, Any]] = None,
        default: str = "",
    ) -> str:
        import string

        styles = self._active_profile.get("styles", {})
        template = styles.get(key)
        if template is None:
            return default
        if mapping:
            try:
                return string.Template(template).safe_substitute(mapping)
            except Exception:
                return template
        return template

    def bubbles(self) -> Dict[str, Any]:
        return copy.deepcopy(self._active_profile.get("bubbles", {}))

    def value(self, path: str, default: Any = None) -> Any:
        node: Any = self._active_profile
        for part in path.split("."):
            if not isinstance(node, dict):
                return default
            node = node.get(part)
            if node is None:
                return default
        return copy.deepcopy(node)

    # ------------------------------------------------------------------ #
    # Mutation helpers

    def set_active_profile(self, name: str) -> None:
        if name == self._active_name:
            return
        self._select_store(name)
        self.apply_palette()
        self._refresh_bindings_async()
        self._notify_subscribers()
        self.profile_changed.emit(name)
        self.style_changed.emit(self._active_profile)

    def update_value(self, path: str, value: Any) -> None:
        self.update_values({path: value})

    def update_values(self, updates: Dict[str, Any]) -> None:
        if not updates:
            return
        for path, value in updates.items():
            if not path:
                continue
            parts = path.split(".")
            container, last_key = _ensure_path(self._active_store, parts)
            container[last_key] = value
        self._commit_changes()

    def mapping(self, name: str) -> Dict[str, Any]:
        data = self._active_profile.get(name, {})
        if isinstance(data, dict):
            return copy.deepcopy(data)
        return {}

    def update_mapping_entries(self, name: str, updates: Dict[str, Any]) -> None:
        if not updates:
            return
        target = self._active_store.setdefault(name, {})
        if not isinstance(target, dict):
            target = self._active_store[name] = {}
        for key, value in updates.items():
            target[key] = copy.deepcopy(value)
        self._commit_changes()

    def save_preset(self, name: str, label: Optional[str] = None) -> None:
        snapshot = copy.deepcopy(self._active_profile)
        self._presets[name] = {
            "label": label or name.title(),
            "data": snapshot,
        }
        self._write()

    def delete_preset(self, name: str) -> None:
        if name in self._presets:
            del self._presets[name]
            if self._active_name == name:
                self.set_active_profile(next(iter(self._themes)))
            self._write()

    def subscribe(self, callback: Callable[[], None]) -> None:
        if callback not in self._subscribers:
            self._subscribers.append(callback)

    # ------------------------------------------------------------------ #
    # Qt palette / stylesheet helpers

    def apply_palette(self, overrides: Optional[Dict[str, Any]] = None) -> None:
        app = QApplication.instance()
        if app is None:
            return
        profile = copy.deepcopy(self._active_profile)
        if overrides:
            palette_override = overrides.get("palette")
            if isinstance(palette_override, dict):
                profile.setdefault("palette", {}).update(palette_override)
            stylesheet_override = overrides.get("stylesheet")
            if stylesheet_override:
                existing = profile.get("app_stylesheet", [])
                if isinstance(existing, str):
                    existing = [existing]
                else:
                    existing = list(existing)
                if isinstance(stylesheet_override, str):
                    existing.append(stylesheet_override)
                elif isinstance(stylesheet_override, list):
                    existing.extend(stylesheet_override)
                profile["app_stylesheet"] = existing

        palette_def = profile.get("palette", {})
        palette = QPalette(app.palette())
        for role_name, color_hex in palette_def.items():
            if role_name == "Disabled":
                continue
            role = getattr(QPalette.ColorRole, role_name, None)
            if role is None:
                continue
            palette.setColor(role, QColor(color_hex))

        disabled = palette_def.get("Disabled", {})
        if isinstance(disabled, dict):
            for role_name, color_hex in disabled.items():
                role = getattr(QPalette.ColorRole, role_name, None)
                if role is None:
                    continue
                palette.setColor(QPalette.ColorGroup.Disabled, role, QColor(color_hex))

        app.setPalette(palette)

        app_styles = profile.get("app_stylesheet", [])
        if isinstance(app_styles, str):
            app.setStyleSheet(app_styles)
        elif isinstance(app_styles, list):
            app.setStyleSheet("\n".join(app_styles))

    def bind_stylesheet(self, widget: QWidget, key: str) -> None:
        def _cleanup(_ref: weakref.ReferenceType[QWidget]) -> None:
            self._bindings[:] = [(ref, style_key) for ref, style_key in self._bindings if ref is not _ref]

        ref = weakref.ref(widget, _cleanup)
        self._bindings.append((ref, key))
        widget.setStyleSheet(self.stylesheet(key))

    def _refresh_bindings(self) -> None:
        alive: List[Tuple[weakref.ReferenceType[QWidget], str]] = []
        for ref, key in self._bindings:
            widget = ref()
            if widget is None:
                continue
            widget.setStyleSheet(self.stylesheet(key))
            alive.append((ref, key))
        self._bindings = alive

    def _refresh_bindings_async(self) -> None:
        if self._bindings_refresh_pending:
            return
        self._bindings_refresh_pending = True

        def _run() -> None:
            self._bindings_refresh_pending = False
            self._refresh_bindings()

        QTimer.singleShot(0, _run)

    def _notify_subscribers(self) -> None:
        for callback in list(self._subscribers):
            try:
                callback()
            except Exception:
                pass

    # ------------------------------------------------------------------ #

    def _write(self) -> None:
        payload = json.dumps(self._data, indent=2, ensure_ascii=False)
        try:
            self._path.write_text(payload, encoding="utf-8")
            return
        except Exception:
            # Switch to a writable location and retry.
            try:
                self._path = self._resolve_styles_path()
                self._path.write_text(payload, encoding="utf-8")
            except Exception:
                # Style persistence must never crash the UI.
                pass

    def _commit_changes(self) -> None:
        self._active_profile = copy.deepcopy(self._active_store)
        self._write()
        self.apply_palette()
        self._refresh_bindings_async()
        self._notify_subscribers()
        self.style_changed.emit(self._active_profile)


def apply_theme(app: QApplication, overrides: Optional[Dict[str, Any]] = None) -> None:
    """Backwards compatibility shim for legacy imports."""
    StyleManager.instance().apply_palette(overrides)
