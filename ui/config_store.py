from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from utils import app_paths

_DEFAULT_AUTO_DOWNLOAD = {
    "user": {
        "image": 12 * 1024 * 1024,
        "voice": 12 * 1024 * 1024,
        "video_note": 50 * 1024 * 1024,
        "animation": 50 * 1024 * 1024,
        "video": 50 * 1024 * 1024,
        "document": 12 * 1024 * 1024,
        "audio": 12 * 1024 * 1024,
    },
    "group": {
        "image": 12 * 1024 * 1024,
        "voice": 12 * 1024 * 1024,
        "video_note": 50 * 1024 * 1024,
        "animation": 50 * 1024 * 1024,
        "video": 50 * 1024 * 1024,
        "document": 12 * 1024 * 1024,
        "audio": 12 * 1024 * 1024,
    },
    "channel": {
        "image": 12 * 1024 * 1024,
        "voice": 12 * 1024 * 1024,
        "video_note": 50 * 1024 * 1024,
        "animation": 50 * 1024 * 1024,
        "video": 50 * 1024 * 1024,
        "document": 0,
        "audio": 12 * 1024 * 1024,
    },
}

DEFAULT_CONFIG: Dict[str, Any] = {
    "window": {
        "width": 1000,
        "height": 700,
    },
    "theme": {
        "mode": "night",
        "palette": {},
        "bubbles": {
            "me": {
                "bg": "#2b5278",
                "border": "#3a71a1",
                "text": "#f4f7ff",
                "link": "#59b7ff",
            },
            "assistant": {
                "bg": "#1f4a3a",
                "border": "#2e6a53",
                "text": "#f2fff7",
                "link": "#59b7ff",
            },
            "other": {
                "bg": "#182533",
                "border": "#243247",
                "text": "#dfe6f0",
                "link": "#59b7ff",
            },
        },
    },
    "auto_download": _DEFAULT_AUTO_DOWNLOAD,
    "features": {
        "auto_download_media": False,
        "ghost_mode": False,
        "voice_waveform": True,
        "streamer_mode": False,
        "show_my_avatar": True,
        "keep_deleted_messages": True,
    },
    "ai": {
        "model": "gemma2",
        "context": 2048,
        "use_cuda": True,
        "prompt": "",
    },
}


def _config_dir() -> Path:
    return app_paths.get_data_dir()


def _config_path() -> Path:
    return _config_dir() / "settings.json"


def _legacy_config_path() -> Path:
    base = os.getenv("APPDATA")
    if base:
        return Path(base) / "DragoGUI" / "settings.json"
    return Path.home() / ".drago_gui" / "settings.json"


def _deep_merge(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(base)
    for key, value in extra.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config() -> Dict[str, Any]:
    path = _config_path()
    if not path.exists():
        legacy = _legacy_config_path()
        if legacy.exists():
            try:
                data = json.loads(legacy.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("Invalid config structure")
                return _deep_merge(DEFAULT_CONFIG, data)
            except Exception:
                return json.loads(json.dumps(DEFAULT_CONFIG))
        return json.loads(json.dumps(DEFAULT_CONFIG))
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Invalid config structure")
        return _deep_merge(DEFAULT_CONFIG, data)
    except Exception:
        # fallback to defaults if parsing fails
        return json.loads(json.dumps(DEFAULT_CONFIG))


def save_config(config: Dict[str, Any]) -> None:
    target = _config_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
