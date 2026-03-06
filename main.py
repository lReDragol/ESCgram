from __future__ import annotations

import os
import sys
import signal
import argparse

from server import ServerCore
from utils.error_guard import guard_module
from utils.logging_setup import configure_logging
from utils import app_paths
from telegram import TelegramAdapter
from gui_chat import run_gui

SERVICE_TOKEN = os.getenv("DRAGO_SERVICE_TOKEN", "dev-service-token")

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

guard_module(globals())

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    main()
