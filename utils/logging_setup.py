"""Centralized logging configuration for the Drago client."""

from __future__ import annotations

import logging
import logging.config
import os
import time
import sys
import atexit
import faulthandler
import threading
from pathlib import Path
from typing import List, Optional

_CONFIGURED = False
_HOOKS_INSTALLED = False
_CURRENT_LOG_DIR: Optional[Path] = None
_CURRENT_LOG_FILE: Optional[Path] = None
_FAULT_HANDLER_STREAM = None
_QT_MESSAGE_HANDLER_INSTALLED = False

DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"


class _DedupFilter(logging.Filter):
    """Drop duplicate log records that repeat too frequently."""

    def __init__(self, window: float = 1.5) -> None:
        super().__init__()
        self.window = window
        self._last_key: Optional[tuple[str, int, str]] = None
        self._last_time: float = 0.0

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        message = record.getMessage()
        key = (record.name, record.levelno, message)
        now = time.monotonic()
        if key == self._last_key and (now - self._last_time) < self.window:
            return False
        self._last_key = key
        self._last_time = now
        return True


def _resolve_log_dir(explicit: Optional[str]) -> Path:
    base = explicit or os.getenv("DRAGO_LOG_DIR") or "logs"
    base_path = Path(base).expanduser()
    if not base_path.is_absolute():
        # Anchor relative paths to the project root (parent of utils/).
        project_root = Path(__file__).resolve().parents[1]
        base_path = project_root / base_path
    path = base_path.resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def configure_logging(level: Optional[str] = None, *, log_directory: Optional[str] = None) -> None:
    """Configure root logging once with optional rotating file handler."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    resolved_level = (level or os.getenv("DRAGO_LOG_LEVEL", "INFO")).upper()
    handlers: dict[str, dict] = {
        "console": {
            "class": "logging.StreamHandler",
            "level": resolved_level,
            "formatter": "default",
            "filters": ["dedup"],
        }
    }
    root_handlers = ["console"]

    log_file = os.getenv("DRAGO_LOG_FILE")
    target_dir: Optional[Path] = None

    if log_directory or log_file or os.getenv("DRAGO_LOG_DIR"):
        target_dir = _resolve_log_dir(log_directory or Path(log_file).parent if log_file else None)

    if target_dir is None and os.getenv("DRAGO_ENABLE_FILE_LOGS", "1") not in {"0", "false", "False"}:
        target_dir = _resolve_log_dir("logs")

    filename: Optional[Path] = None
    if target_dir:
        filename = Path(log_file) if log_file else target_dir / "log.log"
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": resolved_level,
            "formatter": "default",
            "filters": ["dedup"],
            "filename": str(filename),
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 5,
            "encoding": "utf-8",
        }
        root_handlers.append("file")

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": os.getenv("DRAGO_LOG_FORMAT", DEFAULT_FORMAT),
                    "datefmt": os.getenv("DRAGO_LOG_DATEFMT", DEFAULT_DATEFMT),
                }
            },
            "filters": {"dedup": {"()": _DedupFilter}},
            "handlers": handlers,
            "root": {"level": resolved_level, "handlers": root_handlers},
            "loggers": {
                "pyrogram": {"level": os.getenv("DRAGO_PYROGRAM_LOG_LEVEL", "WARNING"), "propagate": True},
                "pyrogram.session": {"level": "WARNING", "propagate": True},
                "pyrogram.dispatcher": {"level": "WARNING", "propagate": True},
                "aiohttp": {"level": "WARNING", "propagate": True},
                "asyncio": {"level": "WARNING", "propagate": True},
            },
        }
    )

    logging.captureWarnings(True)
    atexit.register(logging.shutdown)

    _install_exception_hooks(target_dir)
    _install_qt_message_handler()

    global _CURRENT_LOG_DIR, _CURRENT_LOG_FILE
    _CURRENT_LOG_DIR = target_dir
    _CURRENT_LOG_FILE = filename

    try:
        logging.getLogger("logging").info("Logging to %s", str(filename) if target_dir else "console-only")
    except Exception:
        pass

    _CONFIGURED = True


def _install_exception_hooks(target_dir: Optional[Path]) -> None:
    global _HOOKS_INSTALLED
    if _HOOKS_INSTALLED:
        return
    _HOOKS_INSTALLED = True

    log = logging.getLogger("crash")

    def _excepthook(exc_type, exc, tb):
        try:
            log.error("Unhandled exception", exc_info=(exc_type, exc, tb))
        finally:
            try:
                if hasattr(sys, "__excepthook__"):
                    sys.__excepthook__(exc_type, exc, tb)
            except Exception:
                pass

    sys.excepthook = _excepthook

    if hasattr(threading, "excepthook"):
        original = threading.excepthook

        def _thread_hook(args):
            try:
                log.error("Unhandled thread exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
            finally:
                try:
                    original(args)
                except Exception:
                    pass

        threading.excepthook = _thread_hook

    # Enable faulthandler to capture native crashes.
    try:
        global _FAULT_HANDLER_STREAM
        crash_dir = target_dir or _resolve_log_dir(None)
        crash_file = crash_dir / "crash.log"
        fh = crash_file.open("a", encoding="utf-8")
        _FAULT_HANDLER_STREAM = fh
        faulthandler.enable(file=fh, all_threads=True)
    except Exception:
        pass


def _install_qt_message_handler() -> None:
    global _QT_MESSAGE_HANDLER_INSTALLED
    if _QT_MESSAGE_HANDLER_INSTALLED:
        return
    try:
        from PySide6.QtCore import qInstallMessageHandler
    except Exception:
        return

    qt_log = logging.getLogger("qt")

    def _handler(_msg_type, _context, message):
        try:
            text = str(message or "").strip()
        except Exception:
            text = ""
        if text:
            qt_log.warning(text)

    try:
        qInstallMessageHandler(_handler)
        _QT_MESSAGE_HANDLER_INSTALLED = True
    except Exception:
        pass


def current_log_dir() -> Optional[Path]:
    return _CURRENT_LOG_DIR


def current_log_files() -> List[Path]:
    candidates: List[Path] = []
    collected: set[str] = set()
    if _CURRENT_LOG_FILE is not None:
        try:
            candidates.append(_CURRENT_LOG_FILE)
            parent = _CURRENT_LOG_FILE.parent
        except Exception:
            parent = None
    else:
        parent = _CURRENT_LOG_DIR
    if parent is not None:
        try:
            for pattern in ("*.log", "*.log.*"):
                for path in sorted(parent.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True):
                    key = str(path.resolve())
                    if key in collected or not path.is_file():
                        continue
                    collected.add(key)
                    candidates.append(path)
        except Exception:
            pass
    out: List[Path] = []
    emitted: set[str] = set()
    for path in candidates:
        try:
            key = str(path.resolve())
        except Exception:
            key = str(path)
        if key in emitted:
            continue
        emitted.add(key)
        out.append(path)
    if not out and _CURRENT_LOG_DIR is not None:
        try:
            return [p for p in sorted(_CURRENT_LOG_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True) if p.is_file()]
        except Exception:
            return []
    return out
