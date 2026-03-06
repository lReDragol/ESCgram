from __future__ import annotations

import os
import sys
import signal
import argparse
from typing import Optional

from utils.error_guard import guard_module
from utils.logging_setup import configure_logging
from utils import app_paths
from PySide6.QtCore import QLockFile

SERVICE_TOKEN = os.getenv("DRAGO_SERVICE_TOKEN", "dev-service-token")


def _acquire_single_instance_lock() -> Optional[QLockFile]:
    try:
        lock_path = str(app_paths.temp_dir() / "escgram.instance.lock")
    except Exception:
        return None
    lock = QLockFile(lock_path)
    # Auto-recover if previous process crashed and left stale lock file.
    lock.setStaleLockTime(120000)
    try:
        if lock.tryLock(0):
            return lock
    except Exception:
        return None
    return None

def _prepend_local_ffmpeg_to_path() -> None:
    exe_name = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
    try:
        bin_dir = app_paths.telegram_workdir() / "ffmpeg" / "bin"
    except Exception:
        return
    if not (bin_dir / exe_name).is_file():
        return
    current = os.environ.get("PATH", "")
    norm_target = os.path.normcase(os.path.normpath(str(bin_dir)))
    for part in [p for p in current.split(os.pathsep) if p]:
        if os.path.normcase(os.path.normpath(part)) == norm_target:
            return
    os.environ["PATH"] = str(bin_dir) + (os.pathsep + current if current else "")


def _prepend_local_pydeps_to_sys_path() -> None:
    try:
        deps_dir = app_paths.telegram_workdir() / "pydeps"
    except Exception:
        return
    if not deps_dir.is_dir():
        return
    candidate = str(deps_dir)
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--help", action="store_true")
    args, _unknown = parser.parse_known_args()
    if args.help:
        print("ESCgram\n\nOptions:\n  --data-dir PATH   Where to store app data (db/media/logs/sessions)\n")
        return
    if args.data_dir:
        # One switch controls the whole app storage location.
        app_paths.set_data_dir(str(args.data_dir))
        try:
            app_paths.save_bootstrap(data_dir=str(app_paths.get_data_dir()))
        except Exception:
            pass
    _prepend_local_ffmpeg_to_path()
    _prepend_local_pydeps_to_sys_path()
    instance_lock = _acquire_single_instance_lock()
    if instance_lock is None:
        print("ESCgram уже запущен. Вторая копия не будет открыта.")
        return

    from server import ServerCore
    from telegram import TelegramAdapter
    from gui_chat import run_gui

    configure_logging(log_directory=os.getenv("DRAGO_LOG_DIR") or str(app_paths.logs_dir()))
    server = ServerCore(service_token=SERVICE_TOKEN)
    server.start()  # aiohttp в своём потоке/loop

    tg = TelegramAdapter()
    tg.set_server(server)
    tg.start()  # non-interactive connect

    server.set_telegram_adapter(tg)

    try:
        run_gui(server, tg)
    finally:
        tg.stop()
        server.stop()
        try:
            instance_lock.unlock()
        except Exception:
            pass

guard_module(globals())

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    main()
