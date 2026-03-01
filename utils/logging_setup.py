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
from typing import Optional

_CONFIGURED = False
_HOOKS_INSTALLED = False

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
        crash_dir = target_dir or _resolve_log_dir(None)
        crash_file = crash_dir / "crash.log"
        fh = crash_file.open("a", encoding="utf-8")
        faulthandler.enable(file=fh, all_threads=True)
    except Exception:
        pass
