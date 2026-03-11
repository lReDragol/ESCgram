from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Optional


APP_DIR_NAME = "DragoGUI"
BOOTSTRAP_FILE = "bootstrap.json"

_DATA_DIR: Optional[Path] = None


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def user_config_dir() -> Path:
    """
    Small always-writable directory used for bootstrap settings (e.g. chosen data dir).

    On Windows this uses %APPDATA% (Roaming).
    """
    base = os.getenv("APPDATA")
    if base:
        return (Path(base) / APP_DIR_NAME).expanduser()
    return Path.home() / f".{APP_DIR_NAME.lower()}"


def bootstrap_path() -> Path:
    return user_config_dir() / BOOTSTRAP_FILE


def load_bootstrap() -> dict:
    path = bootstrap_path()
    try:
        if path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}
    return {}


def save_bootstrap(*, data_dir: str) -> None:
    path = bootstrap_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"data_dir": str(data_dir)}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def set_data_dir(path: Optional[str]) -> Path:
    """Override data directory for the current process."""
    global _DATA_DIR
    if path:
        p = Path(path).expanduser()
        if not p.is_absolute():
            p = (Path.cwd() / p)
        _DATA_DIR = p.resolve()
        os.environ["DRAGO_DATA_DIR"] = str(_DATA_DIR)
    return get_data_dir()


def _legacy_project_root() -> Path:
    # project root = parent of utils/
    return Path(__file__).resolve().parents[1]


def get_data_dir() -> Path:
    """
    Resolve the persistent data directory.

    Order:
    1) in-process override (set_data_dir)
    2) DRAGO_DATA_DIR env
    3) bootstrap.json in user_config_dir()
    4) default:
       - frozen build: user_config_dir()/userdata
       - source run: project_root/userdata, unless legacy dirs exist (then project root)
    """
    global _DATA_DIR
    if _DATA_DIR is not None:
        return _DATA_DIR

    env = os.getenv("DRAGO_DATA_DIR")
    if env:
        p = Path(env).expanduser()
        if not p.is_absolute():
            p = (Path.cwd() / p)
        _DATA_DIR = p.resolve()
        return _DATA_DIR

    boot = load_bootstrap()
    boot_dir = str(boot.get("data_dir") or "").strip()
    if boot_dir:
        p = Path(boot_dir).expanduser()
        if not p.is_absolute():
            p = (Path.cwd() / p)
        _DATA_DIR = p.resolve()
        return _DATA_DIR

    if _is_frozen():
        _DATA_DIR = (user_config_dir() / "userdata").resolve()
        return _DATA_DIR

    # Dev/source run: keep compatibility if legacy data exists in project root.
    root = _legacy_project_root()
    legacy_markers = [
        root / "data",
        root / "media",
        root / "avatars",
        root / "history.json",
        root / "accounts.json",
    ]
    if any(p.exists() for p in legacy_markers):
        _DATA_DIR = root
        return _DATA_DIR

    _DATA_DIR = (root / "userdata").resolve()
    return _DATA_DIR


def ensure_dir(path: Path) -> Path:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return path


def logs_dir() -> Path:
    return ensure_dir(get_data_dir() / "logs")


def db_path() -> Path:
    # Keep legacy folder name ("data") for backwards-compat.
    return get_data_dir() / "data" / "drago.db"


def db_dir() -> Path:
    return ensure_dir(db_path().parent)


def media_dir() -> Path:
    return ensure_dir(get_data_dir() / "media")


def temp_dir() -> Path:
    return ensure_dir(get_data_dir() / "temp")


def avatars_dir() -> Path:
    return ensure_dir(get_data_dir() / "avatars")


def chats_dir() -> Path:
    return ensure_dir(get_data_dir() / "chats")


def telegram_workdir() -> Path:
    base = get_data_dir()
    # Backward-compat: in source runs, sessions historically lived in project root.
    # Keep using that location if we detect existing session files.
    if not _is_frozen():
        root = _legacy_project_root()
        try:
            if base.resolve() == root.resolve():
                if any(root.glob("*.session")) or any(root.glob("*.session-journal")):
                    return ensure_dir(root)
        except Exception:
            pass
    return ensure_dir(base / "telegram")
