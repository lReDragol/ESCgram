# telegram.py — Pyrogram adapter (clean)
from __future__ import annotations

import asyncio
import hashlib
import contextlib
import json
import logging
import os
import pathlib
import sys
import threading
import time
import subprocess
import shutil
import tempfile
import uuid
from collections import deque
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple, TYPE_CHECKING
import sqlite3
from concurrent.futures import TimeoutError as FuturesTimeoutError, CancelledError as FuturesCancelledError

from utils.account_store import AccountStore
from utils import app_paths
from utils.error_guard import ensure_asyncio_exception_logging, guard_module
log = logging.getLogger("telegram_adapter")


class _PyrogramNoiseFilter(logging.Filter):
    """Drop known noisy media-id errors that are handled by fallback paths."""

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        try:
            msg = str(record.getMessage() or "").upper()
        except Exception:
            msg = ""
        if "FILE_ID_INVALID" in msg:
            return False
        return True


_pyro_logger = logging.getLogger("pyrogram.client")
if not getattr(_pyro_logger, "_escgram_noise_filter_installed", False):
    _pyro_logger.addFilter(_PyrogramNoiseFilter())
    setattr(_pyro_logger, "_escgram_noise_filter_installed", True)

if TYPE_CHECKING:
    from storage import Storage

# ---- optional deps ----
try:
    import qrcode  # for QR login
    HAVE_QR = True
except Exception:
    HAVE_QR = False

try:
    from pyrogram import Client, filters, enums
    from pyrogram.types import InputMediaPhoto, InputMediaVideo, Message, MessageEntity
    from pyrogram.errors import SessionPasswordNeeded
    from pyrogram.handlers import RawUpdateHandler
    from pyrogram.raw import functions as raw_fn, types as raw_types
    HAVE_PYROGRAM = True
except Exception:
    HAVE_PYROGRAM = False


# ----------------------------- config -----------------------------
def _load_config() -> Dict[str, Any]:
    candidates: List[Path] = []
    seen: Set[str] = set()

    def _add(path: Optional[Path]) -> None:
        if not path:
            return
        try:
            key = str(path.resolve())
        except Exception:
            key = str(path)
        if key in seen:
            return
        seen.add(key)
        candidates.append(path)

    env_path = str(os.getenv("DRAGO_CONFIG_PATH") or "").strip()
    if env_path:
        _add(Path(env_path).expanduser())

    # Preferred: config stored alongside app data (safe for installs).
    try:
        data_dir = app_paths.get_data_dir()
        _add(data_dir / "config.json")
        _add(data_dir / "userdata" / "config.json")
    except Exception:
        data_dir = None
        pass

    # Backward-compat: project root config.json.
    module_dir = Path(__file__).resolve().parent
    _add(module_dir / "config.json")
    _add(module_dir / "userdata" / "config.json")

    # Current working directory fallbacks.
    cwd = Path.cwd()
    _add(cwd / "config.json")
    _add(cwd / "userdata" / "config.json")

    # Frozen binary dir fallbacks.
    try:
        exe_dir = Path(sys.executable).resolve().parent
    except Exception:
        exe_dir = None
    _add(exe_dir / "config.json" if exe_dir else None)
    _add(exe_dir / "userdata" / "config.json" if exe_dir else None)

    # Last-resort defaults from bundled/project example config.
    _add(module_dir / "config.example.json")
    _add(cwd / "config.example.json")
    _add(exe_dir / "config.example.json" if exe_dir else None)
    try:
        _add(app_paths.user_config_dir() / "config.json")
    except Exception:
        pass

    for path in candidates:
        try:
            if path.is_file():
                cfg = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(cfg, dict):
                    log.info("[TG] config loaded from %s", path)
                    return cfg
        except Exception:
            continue
    return {}


@dataclass
class _DownloadJob:
    job_id: str
    chat_id: str
    message_id: int
    target_dir: Path
    progress_cb: Callable[[str, Dict[str, Any]], None]
    future: Optional[asyncio.Task] = None
    resume_event: Optional[asyncio.Event] = None
    cancel_event: Optional[asyncio.Event] = None
    file_path: Optional[str] = None
    current: int = 0
    total: int = 0
    state: str = "idle"
    last_emit: float = field(default=0.0, repr=False)



# =========================== TelegramAdapter ===========================
class TelegramAdapter:
    """
    Чистая версия адаптера:
      • запуск/останов клиента в отдельном потоке
      • QR-вход с корректным ImportLoginToken и 2FA
      • телефон+код (+2FA)
      • получение/стриминг диалогов
      • отправка текста, аудио (music), голосовых (voice/OGG Opus)
      • история и скачивание медиа

    Без дублированных методов и без self._log (только module logger `log`).
    """

    # -------------------- ctor / basic state --------------------
    def __init__(self):
        self._server = None  # type: Optional["ServerCore"]
        self._storage: Optional["Storage"] = None

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event: Optional[asyncio.Event] = None

        self._enabled = False
        self._client: Optional["Client"] = None
        self._connected = False
        self._initialized = False
        self._auth_invalid = False

        # cache: (user_id, ts)
        self._me_cache: Optional[tuple[str, float]] = None

        # config
        cfg = _load_config()
        raw_api_id = cfg.get("telegram_api_id") or os.getenv("DRAGO_TG_API_ID") or os.getenv("TELEGRAM_API_ID")
        raw_api_hash = cfg.get("telegram_api_hash") or os.getenv("DRAGO_TG_API_HASH") or os.getenv("TELEGRAM_API_HASH")
        self._api_id = self._normalize_api_id(raw_api_id)
        self._api_hash = self._normalize_api_hash(raw_api_hash)
        self._allowed_users: List[str] = cfg.get("allowed_users", [])
        self._admin_users: List[str] = cfg.get("admin_users", [])
        self._chat_ids: List[str] = cfg.get("chat_ids", [])
        # Legacy allow-lists may block live UI updates; keep strict filtering opt-in.
        self._strict_live_filter: bool = self._as_bool(cfg.get("strict_live_filter", False), default=False)

        self._enabled = HAVE_PYROGRAM and bool(self._api_id is not None and self._api_hash)
        self._ghost_mode_enabled = False

        # auth state
        self._current_phone_hash: Optional[str] = None
        self._qr_handler: Optional["RawUpdateHandler"] = None
        self._qr_event: Optional[asyncio.Event] = None
        self._qr_pwd_event: Optional[asyncio.Event] = None
        self._qr_pwd_value: Optional[str] = None

        # sessions/workdir + persistent cache
        self._workdir = app_paths.telegram_workdir()
        self._media_root = app_paths.media_dir()
        self._avatar_dir = app_paths.avatars_dir()
        self._account_store = AccountStore(self._workdir)
        stored_session = self._account_store.active_session
        picked = stored_session or self._pick_existing_session_name(["my_account_gui"])
        self._session_name = picked
        self._account_store.ensure_account(self._session_name)
        self._account_store.set_active(self._session_name)
        self._download_jobs: Dict[str, _DownloadJob] = {}
        self._message_to_job: Dict[Tuple[str, int], str] = {}
        self._download_lock = threading.Lock()

        self._local_outgoing_ids: Deque[int] = deque()
        self._local_outgoing_lookup: Set[int] = set()
        self._local_outgoing_limit = 400
        self._local_outgoing_lock = threading.Lock()
        self._last_auth_error_at: float = 0.0
        self._last_history_timeout_at: float = 0.0
        log.info(f"[TG] using session name: {self._session_name} (workdir={self._workdir})")

    def set_server(self, server: "ServerCore") -> None:
        self._server = server

    def set_storage(self, storage: Optional["Storage"]) -> None:
        self._storage = storage

    def set_ghost_mode(self, enabled: bool) -> None:
        self._ghost_mode_enabled = bool(enabled)

    def _pick_existing_session_name(self, candidates: List[str]) -> str:
        for name in candidates:
            if (self._workdir / f"{name}.session").exists():
                return name
        return candidates[0]

    def _generate_session_name(self, base: str = "account") -> str:
        idx = 1
        while (self._workdir / f"{base}_{idx}.session").exists():
            idx += 1
        return f"{base}_{idx}"

    def _delete_session_files(self, session_name: str) -> None:
        for suffix in (".session", ".session-journal"):
            candidate = self._workdir / f"{session_name}{suffix}"
            if candidate.exists():
                try:
                    candidate.unlink()
                except Exception:
                    pass

    def current_session_name(self) -> str:
        return self._session_name

    def list_accounts(self) -> List[Dict[str, Any]]:
        return self._account_store.list_accounts(self._session_name)

    def get_active_account_meta(self) -> Dict[str, Any]:
        meta = self._account_store.get_account(self._session_name)
        meta.setdefault("session", self._session_name)
        return meta

    def prepare_new_account_session(self) -> str:
        new_session = self._generate_session_name()
        self._activate_session(new_session, clean=True)
        self._account_store.update_account(new_session, title="Новый аккаунт")
        return new_session

    def switch_account(self, session_name: str) -> None:
        if not session_name or session_name == self._session_name:
            return
        if not (self._workdir / f"{session_name}.session").exists():
            raise FileNotFoundError(f"Session '{session_name}' не найден")
        self._activate_session(session_name)

    def _activate_session(self, session_name: str, *, clean: bool = False) -> None:
        self.stop()
        if clean:
            self._delete_session_files(session_name)
        self._session_name = session_name
        self._account_store.ensure_account(session_name)
        self._account_store.set_active(session_name)
        self._account_store.update_account(session_name, last_used=time.time())
        self._connected = False
        self._initialized = False
        self._auth_invalid = False
        self.start()

    def refresh_active_account_profile(self) -> Optional[Dict[str, Any]]:
        profile = self.get_self_profile_sync()
        if not profile:
            return None
        full_name = " ".join(filter(None, [profile.get("first_name"), profile.get("last_name")])).strip()
        title = f"@{profile['username']}" if profile.get("username") else (full_name or self._session_name)
        meta = {
            "title": title,
            "phone": profile.get("phone") or "",
            "username": profile.get("username") or "",
            "user_id": profile.get("user_id"),
            "full_name": full_name,
            "last_used": time.time(),
        }
        self._account_store.update_account(self._session_name, **meta)
        return meta

    def get_self_profile_sync(self, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
        if not self._client:
            return None

        async def _get_me():
            me = await self._client.get_me()
            return {
                "user_id": int(getattr(me, "id", 0) or 0),
                "first_name": (me.first_name or ""),
                "last_name": (me.last_name or ""),
                "username": (getattr(me, "username", "") or ""),
                "phone": (getattr(me, "phone_number", None) or getattr(me, "phone", None) or ""),
            }

        try:
            return self._call(_get_me(), timeout)
        except Exception:
            return None

    # -------------------- loop lifecycle --------------------
    def start(self) -> None:
        if not self._enabled:
            log.info("[TG] Telegram disabled: missing pyrogram or telegram_api_id/hash in config.json (or env DRAGO_TG_API_*)")
            return
        if self._thread:
            return
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._enabled or not self._loop:
            return

        async def _shutdown():
            try:
                if self._initialized and self._client:
                    await self._client.terminate()
                    self._initialized = False
            finally:
                if self._connected and self._client:
                    try:
                        await self._client.disconnect()
                    except Exception:
                        pass
                if self._stop_event and not self._stop_event.is_set():
                    self._stop_event.set()

        fut = asyncio.run_coroutine_threadsafe(_shutdown(), self._loop)
        try:
            fut.result(5)
        except Exception:
            pass

        if self._thread:
            self._thread.join(timeout=5)
        self._thread = None
        self._loop = None

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        ensure_asyncio_exception_logging(self._loop)

        self._client = Client(
            self._session_name,
            api_id=int(self._api_id),
            api_hash=str(self._api_hash),
            workdir=str(self._workdir),
            sleep_threshold=60,
        )

        async def _connect_only():
            if self._connected:
                return
            await self._client.connect()
            self._connected = True
            log.info("[TG] connected (non-interactive)")

        async def _main():
            await _connect_only()
            # если уже авторизованы — поднимем апдейты
            try:
                _ = await self._client.get_me()
                await self._initialize_updates()
            except Exception:
                pass

            self._stop_event = asyncio.Event()
            await self._stop_event.wait()

            try:
                if self._initialized:
                    await self._client.terminate()
            finally:
                try:
                    await self._client.disconnect()
                except Exception:
                    pass

        try:
            self._loop.run_until_complete(_main())
        finally:
            pending = asyncio.all_tasks(loop=self._loop)
            for t in pending:
                t.cancel()
            try:
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            self._loop.stop()

    # -------------------- helpers --------------------
    @staticmethod
    def _discard_coroutine(coro: Any) -> None:
        try:
            if asyncio.iscoroutine(coro):
                coro.close()
                return
        except Exception:
            return
        try:
            cancel = getattr(coro, "cancel", None)
            if callable(cancel):
                cancel()
        except Exception:
            pass

    def _call(self, coro, timeout: float):
        if not (self._loop and self._client and self._enabled):
            self._discard_coroutine(coro)
            return None
        if not self._connected:
            self._discard_coroutine(coro)
            return None
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=timeout)
        except FuturesCancelledError:
            # Loop is stopping or the task was cancelled: treat as no result.
            return None
        except FuturesTimeoutError:
            # Timeout is expected under unstable network conditions.
            # Return None instead of raising to avoid noisy "Unhandled error in _call"
            # from error_guard wrappers.
            try:
                fut.cancel()
            except Exception:
                pass
            self._throttled_warning(
                "_last_call_timeout_at",
                4.0,
                "[TG] Async call timed out (timeout=%.1fs)",
                float(timeout),
            )
            return None
        except Exception as exc:
            if self._is_auth_issue(exc):
                self._auth_invalid = True
                self._notify_auth_issue(exc)
                return None
            if isinstance(exc, ConnectionError) and "not been started" in str(exc).lower():
                return None
            # If the caller timed out (or errored), ensure the coroutine doesn't
            # keep running in the background and blocking the shared loop.
            try:
                fut.cancel()
            except Exception:
                pass
            raise

    @staticmethod
    def _is_auth_issue(exc: BaseException) -> bool:
        msg = str(exc).upper()
        return ("AUTH_KEY_UNREGISTERED" in msg) or ("SESSION_PASSWORD_NEEDED" in msg) or ("UNAUTHORIZED" in msg)

    def _throttled_warning(self, stamp_attr: str, interval_sec: float, text: str, *args: Any) -> None:
        now = time.monotonic()
        last = float(getattr(self, stamp_attr, 0.0) or 0.0)
        if now - last < max(0.5, float(interval_sec)):
            return
        setattr(self, stamp_attr, now)
        try:
            log.warning(text, *args)
        except Exception:
            pass

    def _notify_auth_issue(self, exc: BaseException) -> None:
        previous = float(getattr(self, "_last_auth_error_at", 0.0) or 0.0)
        self._throttled_warning(
            "_last_auth_error_at",
            15.0,
            "[TG] History sync skipped: Telegram session is not authorized (%s)",
            exc,
        )
        if float(getattr(self, "_last_auth_error_at", 0.0) or 0.0) == previous:
            return
        if self._server:
            try:
                self._server.events.put({
                    "type": "gui_info",
                    "text": "Сессия Telegram не авторизована. Войдите заново.",
                })
            except Exception:
                pass

    @staticmethod
    def _as_int_list(items: List[str]) -> List[int]:
        out: List[int] = []
        for x in items:
            try:
                out.append(int(x))
            except Exception:
                pass
        return out

    @staticmethod
    def _as_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return bool(default)
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            raw = value.strip().lower()
            if raw in {"1", "true", "yes", "y", "on"}:
                return True
            if raw in {"0", "false", "no", "n", "off", ""}:
                return False
            return bool(default)
        return bool(default)

    @staticmethod
    def _normalize_api_id(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, int):
            return value if value > 0 else None
        text = str(value).strip()
        if not text or not text.isdigit():
            return None
        try:
            parsed = int(text)
            return parsed if parsed > 0 else None
        except Exception:
            return None

    @staticmethod
    def _normalize_api_hash(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        upper = text.upper()
        if upper in {"YOUR_API_HASH", "ENTER_API_HASH"}:
            return None
        return text

    def _remember_local_outgoing(self, message_id: Optional[int]) -> None:
        if message_id is None:
            return
        try:
            mid = int(message_id)
        except Exception:
            return
        with self._local_outgoing_lock:
            if len(self._local_outgoing_ids) >= self._local_outgoing_limit:
                try:
                    oldest = self._local_outgoing_ids.popleft()
                    self._local_outgoing_lookup.discard(oldest)
                except IndexError:
                    pass
            self._local_outgoing_ids.append(mid)
            self._local_outgoing_lookup.add(mid)

    def _consume_local_outgoing(self, message_id: Optional[int]) -> bool:
        if message_id is None:
            return False
        try:
            mid = int(message_id)
        except Exception:
            return False
        with self._local_outgoing_lock:
            if mid not in self._local_outgoing_lookup:
                return False
            self._local_outgoing_lookup.discard(mid)
            try:
                self._local_outgoing_ids.remove(mid)
            except ValueError:
                pass
            return True

    def _is_allowed(self, user_id: str, username: str) -> bool:
        if not (self._allowed_users or self._admin_users):
            return True
        return (user_id in self._allowed_users) or (username in self._allowed_users) or \
               (user_id in self._admin_users) or (username in self._admin_users)

    async def _ensure_me_cached(self) -> None:
        """
        В Pyrogram 2.x self.me может быть None до первого get_me().
        А save_file() смотрит self.me.is_premium -> без этого падает.
        """
        if getattr(self._client, "me", None) is None:
            me = await self._client.get_me()
            setattr(self._client, "me", me)

    async def _get_me_id_cached(self, ttl_sec: float = 60.0) -> Optional[str]:
        try:
            now = time.time()
            if self._me_cache and (now - self._me_cache[1] < ttl_sec):
                return self._me_cache[0]
            me = await self._client.get_me()  # внутри users.GetFullUser
            mid = str(me.id)
            self._me_cache = (mid, now)
            setattr(self._client, "me", me)  # прогреваем для save_file()
            return mid
        except sqlite3.ProgrammingError as exc:
            if "closed database" in str(exc).lower():
                log.warning("[TG] storage connection closed, reconnecting client")
                try:
                    await self._client.disconnect()
                except Exception:
                    pass
                try:
                    await self._client.connect()
                    return await self._get_me_id_cached(ttl_sec=ttl_sec)  # retry once
                except Exception:
                    return None
        except Exception:
            return None

    # -------- ffmpeg helpers: convert to OGG/Opus & probe duration --------
    @staticmethod
    def _which_ffmpeg() -> Optional[str]:
        return shutil.which("ffmpeg")

    @staticmethod
    def _which_ffprobe() -> Optional[str]:
        return shutil.which("ffprobe")

    def _convert_to_ogg_opus(self,
                             src_path: str,
                             *,
                             bitrate: str = "24k",
                             ar: int = 48000,
                             mono: bool = True) -> Optional[str]:
        """
        Конвертирует входной аудио-файл в OGG/Opus (для голосовых).
        Возвращает путь к временному .ogg или None при ошибке.
        """
        ffmpeg = self._which_ffmpeg()
        if not ffmpeg:
            log.warning("ffmpeg is not found in PATH — skip voice conversion")
            return None

        if not os.path.isfile(src_path):
            return None

        tmp_dir = tempfile.mkdtemp(prefix="tg_voice_")
        out_path = os.path.join(tmp_dir, "voice.ogg")

        cmd = [
            ffmpeg, "-hide_banner", "-nostdin",
            "-i", src_path,
            "-vn",
            "-c:a", "libopus",
            "-b:a", bitrate,
            "-vbr", "on",
            "-compression_level", "10",
            "-application", "voip",
            "-frame_duration", "60",
            "-ar", str(ar),
        ]
        if mono:
            cmd += ["-ac", "1"]
        cmd += ["-y", out_path]

        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            if os.path.isfile(out_path):
                return out_path
        except Exception as e:
            log.exception("ffmpeg conversion failed: %s", e)
        return None

    def _probe_duration_sec(self, path: str) -> Optional[int]:
        """
        Возвращает длительность в секундах (округлённую вниз) через ffprobe, если доступен.
        Если недоступен/ошибка — None (длительность не передаём).
        """
        ffprobe = self._which_ffprobe()
        if not ffprobe:
            return None
        try:
            cmd = [
                ffprobe, "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                path,
            ]
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            s = out.decode("utf-8", errors="ignore").strip()
            if not s:
                return None
            dur = float(s)
            if dur >= 0:
                return int(dur)
        except Exception:
            return None
        return None

    # -------------------- init updates/handlers --------------------
    async def _initialize_updates(self) -> None:
        if self._initialized:
            return

        await self._client.initialize()
        self._initialized = True

        await self._ensure_me_cached()
        m = getattr(self._client, "me", None)

        # фильтр по чатам (если указан allowlist)
        chats = self._as_int_list(self._chat_ids)
        chats_filter = filters.chat(chats) if chats else filters.all
        # Do not restrict by incoming/outgoing flags here: channel posts and some
        # service updates may not be classified as either, but we still need them
        # for real-time dialog sorting and UI refresh.
        flt = chats_filter

        # внутри async def _initialize_updates(self):

        @self._client.on_message(flt)
        async def _on_message(client: "Client", message: "Message"):
            try:
                text = str(message.text or "")
                caption = str(message.caption or "")
                uid = str(message.from_user.id) if message.from_user else "unknown"
                reply_to_id = self._extract_reply_to_id(message)
                forward_info = self._extract_forward_info(message)
                file_name = self._extract_file_name(message)
                sender_label = self._sender_display_name(message, uid)

                # allowlist (если настроен)
                if self._strict_live_filter and (self._allowed_users or self._admin_users):
                    username = message.from_user.username if message.from_user else ""
                    if not self._is_allowed(uid, username):
                        return

                # Обновим display-name
                if self._server and message.from_user:
                    uname = (message.from_user.username or "").strip()
                    fname = (message.from_user.first_name or "").strip()
                    lname = (message.from_user.last_name or "").strip()
                    disp = uname or f"{fname} {lname}".strip() or uid
                    self._server.set_user_display_name(uid, disp)
                # Keep the outgoing-id set bounded, but do not suppress GUI events:
                # we need real Telegram message IDs to reconcile local echoes and keep
                # context menu/actions working on freshly sent messages.
                self._consume_local_outgoing(getattr(message, "id", None))


                media_type, _media_id, media_size, mime, duration, waveform = self._extract_media_meta(message)
                media_group_id = self._extract_media_group_id(message)
                ts = int(getattr(message, "date", 0).timestamp()) if getattr(message, "date", None) else None

                cached: Optional[str] = None
                if self._server:
                    if media_type == "text":
                        if text:
                            entities = self._entities_to_dicts(getattr(message, "entities", None))
                            self._server.tg_incoming_message(
                                chat_id=str(message.chat.id),
                                user_id=uid,
                                text=text,
                                date_ts=ts,
                                message_id=int(message.id),
                                reply_to=reply_to_id,
                                forward_info=forward_info,
                                entities=entities,
                                sender_name=sender_label,
                            )
                    else:
                        media_root = pathlib.Path(self._media_root) / str(message.chat.id)
                        cached = self._find_cached_media_file(media_root, message.id)
                        entities = self._entities_to_dicts(getattr(message, "caption_entities", None))
                        self._server.tg_incoming_media(
                            chat_id=str(message.chat.id),
                            user_id=uid,
                            message_id=int(message.id),
                            mtype=media_type,
                            text=caption,
                            date_ts=ts,
                            entities=entities,
                            file_path=cached,
                            thumb_path=None,
                            reply_to=reply_to_id,
                            forward_info=forward_info,
                            file_name=file_name,
                            sender_name=sender_label,
                            file_size=media_size,
                            mime=mime,
                            duration=duration,
                            waveform=waveform,
                            media_group_id=media_group_id,
                        )
                self._store_message_record(message, file_path=cached)
                if (
                    not self._ghost_mode_enabled
                    and self._client
                    and not getattr(message, "outgoing", False)
                ):
                    try:
                        await self._client.read_history(message.chat.id, message.id)
                    except Exception:
                        log.debug("[TG] read_history skipped for %s/%s", message.chat.id, message.id)
            except Exception as e:
                log.exception("on_message failed: %s", e)

        @self._client.on_edited_message(flt)
        async def _on_edited_message(client: "Client", message: "Message"):
            try:
                text = str(message.text or "")
                caption = str(message.caption or "")
                uid = str(message.from_user.id) if message.from_user else "unknown"
                reply_to_id = self._extract_reply_to_id(message)
                forward_info = self._extract_forward_info(message)
                file_name = self._extract_file_name(message)
                sender_label = self._sender_display_name(message, uid)

                if self._strict_live_filter and (self._allowed_users or self._admin_users):
                    username = message.from_user.username if message.from_user else ""
                    if not self._is_allowed(uid, username):
                        return

                media_type, _media_id, media_size, mime, duration, waveform = self._extract_media_meta(message)
                media_group_id = self._extract_media_group_id(message)
                ts = int(getattr(message, "date", 0).timestamp()) if getattr(message, "date", None) else None

                # Если было медиа — переотрисовать превью/подпись
                cached_path: Optional[str] = None
                if self._server:
                    if media_type == "text":
                        if text:
                            entities = self._entities_to_dicts(getattr(message, "entities", None))
                            self._server.tg_incoming_message(
                                chat_id=str(message.chat.id),
                                user_id=uid,
                                text=text,
                                date_ts=ts,
                                message_id=int(message.id),
                                reply_to=reply_to_id,
                                forward_info=forward_info,
                                entities=entities,
                                sender_name=sender_label,
                            )
                    else:
                        media_root = pathlib.Path(self._media_root) / str(message.chat.id)
                        cached_path = self._find_cached_media_file(media_root, message.id)
                        entities = self._entities_to_dicts(getattr(message, "caption_entities", None))
                        self._server.tg_incoming_media(
                            chat_id=str(message.chat.id),
                            user_id=uid,
                            message_id=int(message.id),
                            mtype=media_type,
                            text=caption,
                            date_ts=ts,
                            entities=entities,
                            file_path=cached_path,
                            thumb_path=None,
                            reply_to=reply_to_id,
                            forward_info=forward_info,
                            file_name=file_name,
                            sender_name=sender_label,
                            file_size=media_size,
                            mime=mime,
                            duration=duration,
                            waveform=waveform,
                            media_group_id=media_group_id,
                        )
                self._store_message_record(message, file_path=cached_path)
            except Exception as e:
                log.exception("on_edited_message failed: %s", e)

        @self._client.on_deleted_messages()
        async def _on_deleted_messages(client: "Client", messages: List["Message"]):
            try:
                by_chat: Dict[int, List[int]] = {}
                unknown_chat_ids: List[int] = []
                for msg in messages or []:
                    try:
                        mid = int(msg.id)
                    except Exception:
                        continue
                    chat = getattr(msg, "chat", None)
                    if not chat:
                        unknown_chat_ids.append(mid)
                        continue
                    try:
                        chat_id = int(chat.id)
                    except Exception:
                        unknown_chat_ids.append(mid)
                        continue
                    by_chat.setdefault(chat_id, []).append(mid)

                # For private/group deletions Telegram doesn't always provide chat id.
                # Resolve affected peers via local storage.
                if unknown_chat_ids and self._storage:
                    try:
                        mapped = self._storage.find_peers_for_message_ids(unknown_chat_ids)
                        for peer_id, mids in mapped.items():
                            if not mids:
                                continue
                            by_chat.setdefault(int(peer_id), []).extend([int(mid) for mid in mids])
                    except Exception:
                        log.exception("[TG] Failed to resolve deleted message peers for ids=%s", unknown_chat_ids)

                for peer_id, mids in by_chat.items():
                    if not mids:
                        continue
                    uniq_mids = sorted({int(mid) for mid in mids if int(mid) > 0})
                    if not uniq_mids:
                        continue
                    if self._storage:
                        try:
                            self._storage.mark_messages_deleted(peer_id, uniq_mids, deleted=True)
                        except Exception:
                            log.exception("[TG] Failed to update storage for deleted messages %s/%s", peer_id, uniq_mids)
                    if self._server:
                        try:
                            self._server.tg_messages_deleted(str(peer_id), uniq_mids)
                        except Exception:
                            log.exception("[TG] Failed to notify server about deletions %s/%s", peer_id, uniq_mids)
            except Exception as exc:
                log.exception("on_deleted_messages failed: %s", exc)

        log.info("[TG] updates initialized (me=%s, premium=%s)",
                 getattr(m, "id", None), getattr(m, "is_premium", None))

    # -------------------- state / identity --------------------
    def is_authorized_sync(self, timeout: float = 5.0) -> bool:
        if not (self._enabled and self._client and self._loop):
            return False
        if self._auth_invalid:
            return False

        # чуть подождать подключение
        end = time.time() + 3.0
        while not self._connected and time.time() < end:
            time.sleep(0.05)

        async def _check():
            mid = await self._get_me_id_cached(ttl_sec=60.0)
            return bool(mid)

        try:
            return bool(self._call(_check(), timeout))
        except Exception:
            return False

    def get_self_id_sync(self, timeout: float = 10.0) -> Optional[str]:
        if not (self._enabled and self._client and self._loop):
            return None
        try:
            return self._call(self._get_me_id_cached(ttl_sec=60.0), timeout)
        except Exception:
            return None

    def resolve_username_sync(self, username: str, timeout: float = 10.0) -> Optional[str]:
        if not (self._enabled and self._client and self._loop):
            return None
        name = str(username or "").strip().lstrip("@")
        if not name:
            return None

        async def _run() -> Optional[str]:
            user = await self._client.get_users(name)
            if isinstance(user, list):
                user = user[0] if user else None
            if not user:
                return None
            return str(getattr(user, "id", "") or "")

        try:
            return self._call(_run(), timeout)
        except Exception:
            return None

    def get_contacts_sync(self, timeout: float = 15.0) -> List[Dict[str, Any]]:
        if not (self._enabled and self._client and self._loop):
            return []

        async def _run() -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            users = await self._client.get_contacts()
            for user in users:
                try:
                    uid = getattr(user, "id", None)
                except Exception:
                    uid = None
                if not uid:
                    continue
                username = str(getattr(user, "username", "") or "").strip()
                first = str(getattr(user, "first_name", "") or "").strip()
                last = str(getattr(user, "last_name", "") or "").strip()
                title = " ".join(p for p in (first, last) if p).strip()
                if not title:
                    title = username or str(uid)
                phone = str(getattr(user, "phone_number", None) or getattr(user, "phone", None) or "")
                out.append(
                    {
                        "id": str(uid),
                        "title": title,
                        "username": username,
                        "phone": phone,
                    }
                )
            return out

        try:
            return list(self._call(_run(), timeout) or [])
        except Exception:
            return []

    def create_group_sync(self, title: str, users: List[str], timeout: float = 20.0) -> Optional[str]:
        if not (self._enabled and self._client and self._loop):
            return None
        group_title = str(title or "").strip()
        if not group_title or not users:
            return None
        cleaned = [u.strip() for u in users if str(u).strip()]
        if not cleaned:
            return None

        async def _run() -> Optional[str]:
            chat = await self._client.create_group(group_title, cleaned)
            if not chat:
                return None
            return str(getattr(chat, "id", "") or "")

        try:
            return self._call(_run(), timeout)
        except Exception:
            return None

    def create_channel_sync(self, title: str, about: str = "", timeout: float = 20.0) -> Optional[str]:
        if not (self._enabled and self._client and self._loop):
            return None
        channel_title = str(title or "").strip()
        if not channel_title:
            return None
        description = str(about or "").strip()

        async def _run() -> Optional[str]:
            chat = await self._client.create_channel(channel_title, description)
            if not chat:
                return None
            return str(getattr(chat, "id", "") or "")

        try:
            return self._call(_run(), timeout)
        except Exception:
            return None

    def list_archived_chats_sync(self, limit: int = 200, timeout: float = 20.0) -> List[Dict[str, Any]]:
        if not (self._enabled and self._client and self._loop):
            return []
        total = int(limit or 0)
        if total <= 0:
            total = (1 << 31) - 1

        async def _collect() -> List[Dict[str, Any]]:
            from pyrogram import raw, types, utils

            out: List[Dict[str, Any]] = []
            current = 0
            page = min(100, total)
            offset_date = 0
            offset_id = 0
            offset_peer = raw.types.InputPeerEmpty()

            while True:
                r = await self._client.invoke(
                    raw.functions.messages.GetDialogs(
                        offset_date=offset_date,
                        offset_id=offset_id,
                        offset_peer=offset_peer,
                        limit=page,
                        hash=0,
                        folder_id=1,
                    ),
                    sleep_threshold=60,
                )
                users = {i.id: i for i in r.users}
                chats = {i.id: i for i in r.chats}
                messages: Dict[int, types.Message] = {}
                for message in r.messages:
                    if isinstance(message, raw.types.MessageEmpty):
                        continue
                    chat_id = utils.get_peer_id(message.peer_id)
                    messages[chat_id] = await types.Message._parse(self._client, message, users, chats)
                dialogs: List[types.Dialog] = []
                for dialog in r.dialogs:
                    if not isinstance(dialog, raw.types.Dialog):
                        continue
                    dialogs.append(types.Dialog._parse(self._client, dialog, messages, users, chats))
                if not dialogs:
                    return out
                last = dialogs[-1]
                offset_id = last.top_message.id
                offset_date = utils.datetime_to_timestamp(last.top_message.date)
                offset_peer = await self._client.resolve_peer(last.chat.id)
                for dialog in dialogs:
                    info = self._dialog_to_dict(dialog)
                    if info:
                        out.append(info)
                        current += 1
                        if current >= total:
                            return out
            return out

        try:
            return list(self._call(_collect(), timeout) or [])
        except Exception:
            return []

    # -------------------- AUTH: phone + code (+2FA) --------------------
    def send_login_code_sync(self, phone: str, timeout: float = 20.0) -> str:
        async def _send():
            sent = await self._client.send_code(phone)
            self._current_phone_hash = sent.phone_code_hash
            return self._current_phone_hash
        return self._call(_send(), timeout)

    def sign_in_with_code_sync(
        self, phone: str, code: str, password: Optional[str] = None, timeout: float = 30.0
    ) -> bool:
        async def _signin():
            if not self._current_phone_hash:
                raise RuntimeError("Сначала отправьте код на телефон (нет phone_code_hash).")
            try:
                await self._client.sign_in(
                    phone_number=phone,
                    phone_code_hash=self._current_phone_hash,
                    phone_code=code
                )
            except SessionPasswordNeeded:
                if not password:
                    raise
                await self._client.check_password(password=password)
            await self._initialize_updates()
            return True
        ok = bool(self._call(_signin(), timeout))
        if ok:
            self._auth_invalid = False
        return ok

    # -------------------- AUTH: QR login (+2FA) --------------------
    def submit_qr_2fa_password_sync(self, password: str, timeout: float = 5.0) -> bool:
        async def _set():
            self._qr_pwd_value = password
            if self._qr_pwd_event and not self._qr_pwd_event.is_set():
                self._qr_pwd_event.set()
            return True
        return bool(self._call(_set(), timeout))

    def start_qr_login_sync(
        self,
        on_qr_png: Callable[[bytes], None],
        on_status: Callable[[str], None],
        timeout_total: float = 180.0
    ) -> bool:
        if not HAVE_QR:
            raise RuntimeError("qrcode не установлен (pip install qrcode pillow)")

        if not self._thread:
            self.start()

        try:
            on_status("QR: подключаем Telegram-клиент...")
        except Exception:
            pass

        # дождаться подключения клиента (он стартуется в отдельном потоке)
        deadline = time.time() + 45.0
        while not self._connected and time.time() < deadline:
            time.sleep(0.05)
        if not self._connected:
            raise RuntimeError("Client has not been started yet (timeout waiting for connection)")

        async def _qr_flow():
            from io import BytesIO

            async def _on_raw(_, update, users, chats):
                # сигнал, что QR отсканирован (UpdateLoginToken)
                if isinstance(update, raw_types.UpdateLoginToken):
                    if self._qr_event:
                        self._qr_event.set()

            self._qr_event = asyncio.Event()
            self._qr_pwd_event = None
            self._qr_pwd_value = None
            self._qr_handler = RawUpdateHandler(_on_raw)
            self._client.add_handler(self._qr_handler)

            try:
                deadline2 = time.time() + timeout_total
                while time.time() < deadline2:
                    # 1) экспорт токена и показ QR
                    try:
                        login_token = await self._client.invoke(
                            raw_fn.auth.ExportLoginToken(
                                api_id=int(self._api_id),
                                api_hash=str(self._api_hash),
                                except_ids=[],
                            )
                        )
                    except SessionPasswordNeeded:
                        # иногда сервер требует пароль уже тут
                        on_status("QR: включена двухэтапная защита — введите пароль 2FA и нажмите «Войти».")
                        self._qr_pwd_event = asyncio.Event()
                        try:
                            await asyncio.wait_for(self._qr_pwd_event.wait(), timeout=120.0)
                        except asyncio.TimeoutError:
                            on_status("QR: ожидание пароля 2FA истекло")
                            return False
                        finally:
                            self._qr_pwd_event = None
                        pwd = self._qr_pwd_value or ""
                        self._qr_pwd_value = None
                        await self._client.check_password(password=pwd)
                        # повторим цикл
                        continue

                    if isinstance(login_token, raw_types.auth.LoginTokenSuccess):
                        on_status("QR: авторизация успешна")
                        await self._initialize_updates()
                        return True

                    if isinstance(login_token, raw_types.auth.LoginTokenMigrateTo):
                        mt = await self._client.invoke(raw_fn.auth.ImportLoginToken(token=login_token.token))
                        if isinstance(mt, (raw_types.auth.LoginTokenSuccess, raw_types.auth.Authorization)):
                            on_status("QR: авторизация успешна (migrate)")
                            await self._initialize_updates()
                            return True
                        continue

                    # 2) рендер QR tg://login?token=...
                    token_bytes = login_token.token
                    import base64
                    b64 = base64.urlsafe_b64encode(token_bytes).rstrip(b"=").decode("ascii")
                    tg_url = f"tg://login?token={b64}"

                    img = qrcode.make(tg_url)
                    buf = BytesIO(); img.save(buf, format="PNG")
                    on_qr_png(buf.getvalue())
                    on_status("Сканируйте QR: Telegram → Настройки → Устройства → Подключить устройство")

                    # 3) ждём скан (UpdateLoginToken)
                    try:
                        await asyncio.wait_for(self._qr_event.wait(), timeout=28.0)
                    except asyncio.TimeoutError:
                        continue  # истёк — новый токен
                    finally:
                        if self._qr_event:
                            self._qr_event.clear()

                    # 4) импорт токена (НЕ повторный Export!)
                    try:
                        res = await self._client.invoke(raw_fn.auth.ImportLoginToken(token=token_bytes))

                        if isinstance(res, raw_types.auth.LoginTokenMigrateTo):
                            res2 = await self._client.invoke(raw_fn.auth.ImportLoginToken(token=res.token))
                            if isinstance(res2, (raw_types.auth.LoginTokenSuccess, raw_types.auth.Authorization)):
                                on_status("QR: авторизация успешна (migrate)")
                                await self._initialize_updates()
                                return True
                            continue

                        if isinstance(res, (raw_types.auth.LoginTokenSuccess, raw_types.auth.Authorization)):
                            on_status("QR: авторизация успешна")
                            await self._initialize_updates()
                            return True

                    except SessionPasswordNeeded:
                        # 5) требуется пароль 2FA
                        on_status("QR: включена двухэтапная защита — введите пароль 2FA и нажмите «Войти».")
                        self._qr_pwd_event = asyncio.Event()
                        try:
                            await asyncio.wait_for(self._qr_pwd_event.wait(), timeout=120.0)
                        except asyncio.TimeoutError:
                            on_status("QR: ожидание пароля 2FA истекло")
                            return False
                        finally:
                            self._qr_pwd_event = None
                        pwd = self._qr_pwd_value or ""
                        self._qr_pwd_value = None
                        await self._client.check_password(password=pwd)
                        on_status("QR: пароль принят, завершаем вход…")
                        await self._initialize_updates()
                        return True

                return False
            finally:
                try:
                    if self._qr_handler:
                        self._client.remove_handler(self._qr_handler)
                except Exception:
                    pass
                self._qr_handler = None
                self._qr_event = None
                self._qr_pwd_event = None
                self._qr_pwd_value = None

        ok = bool(self._call(_qr_flow(), timeout_total))
        if ok:
            self._auth_invalid = False
        return ok

    # -------------------- dialogs / messaging --------------------
    @staticmethod
    def _dialog_to_dict(dialog) -> Optional[Dict[str, Any]]:
        try:
            chat = dialog.chat
        except Exception:
            return None

        try:
            cid = str(chat.id)
        except Exception:
            return None

        try:
            title = (
                chat.title
                or ((chat.first_name or "") + " " + (chat.last_name or "")).strip()
                or (chat.username or "")
                or cid
            )
        except Exception:
            title = cid

        try:
            tname = (chat.type.name or "").lower()
        except Exception:
            tname = ""

        try:
            username = (chat.username or "").strip()
        except Exception:
            username = ""

        photo_small = None
        photo_big = None
        try:
            photo = getattr(chat, "photo", None)
            if photo:
                photo_small = getattr(photo, "small_file_id", None)
                photo_big = getattr(photo, "big_file_id", None)
        except Exception:
            photo_small = photo_small or None
            photo_big = photo_big or None

        try:
            top_message = getattr(dialog, "top_message", None)
            last_ts = int(top_message.date.timestamp()) if (top_message and getattr(top_message, "date", None)) else 0
        except Exception:
            last_ts = 0

        try:
            unread_count = int(getattr(dialog, "unread_messages_count", 0) or 0)
        except Exception:
            unread_count = 0

        try:
            pinned = bool(getattr(dialog, "is_pinned", False) or getattr(dialog, "pinned", False))
        except Exception:
            pinned = False

        return {
            "id": cid,
            "title": title,
            "type": tname,
            "username": username,
            "photo_small_id": photo_small,
            "photo_big_id": photo_big,
            "last_ts": last_ts,
            "unread_count": unread_count,
            "pinned": pinned,
        }

    def _chat_to_peer_row(self, chat) -> Optional[Dict[str, Any]]:
        if not chat:
            return None
        try:
            cid = int(getattr(chat, "id"))
        except Exception:
            return None
        username = (getattr(chat, "username", "") or "").strip()
        title = (
            getattr(chat, "title", None)
            or ((getattr(chat, "first_name", "") or "") + " " + (getattr(chat, "last_name", "") or "")).strip()
            or username
            or str(cid)
        )
        ctype = getattr(chat, "type", None)
        if hasattr(ctype, "name"):
            ctype = (ctype.name or "").lower()
        else:
            ctype = str(ctype or "").lower()
        photo = getattr(chat, "photo", None)
        photo_small = getattr(photo, "small_file_id", None) if photo else None
        photo_big = getattr(photo, "big_file_id", None) if photo else None
        return {
            "id": cid,
            "type": ctype or "chat",
            "username": username or None,
            "title": title,
            "photo_small": photo_small,
            "photo_big": photo_big,
        }

    def _user_to_peer_row(self, user) -> Optional[Dict[str, Any]]:
        if not user:
            return None
        try:
            uid = int(getattr(user, "id"))
        except Exception:
            return None
        username = (getattr(user, "username", "") or "").strip()
        first = getattr(user, "first_name", "") or ""
        last = getattr(user, "last_name", "") or ""
        title = username or f"{first} {last}".strip() or str(uid)
        photo = getattr(user, "photo", None)
        photo_small = getattr(photo, "small_file_id", None) if photo else None
        photo_big = getattr(photo, "big_file_id", None) if photo else None
        return {
            "id": uid,
            "type": "user",
            "username": username or None,
            "title": title,
            "photo_small": photo_small,
            "photo_big": photo_big,
        }

    def _store_dialog_info(self, dialog, info: Dict[str, Any]) -> None:
        if not self._storage:
            return
        try:
            peer_id = int(info.get("id"))
        except Exception:
            return
        peer_row = {
            "id": peer_id,
            "type": info.get("type"),
            "username": info.get("username"),
            "title": info.get("title"),
            "photo_small": info.get("photo_small_id"),
            "photo_big": info.get("photo_big_id"),
        }
        self._storage.upsert_peers([peer_row])

        top_msg = getattr(dialog, "top_message", None)
        last_ts = None
        if top_msg and getattr(top_msg, "date", None):
            try:
                last_ts = int(top_msg.date.timestamp())  # type: ignore[arg-type]
            except Exception:
                last_ts = None
        dialog_row = {
            "peer_id": peer_id,
            "top_message_id": getattr(dialog, "top_message_id", None),
            "last_message_date": last_ts,
            "unread_count": int(getattr(dialog, "unread_messages_count", 0) or 0),
            "pinned": bool(getattr(dialog, "is_pinned", False)),
            "last_read_inbox_id": getattr(dialog, "read_inbox_max_id", None),
            "last_read_outbox_id": getattr(dialog, "read_outbox_max_id", None),
        }
        self._storage.upsert_dialogs([dialog_row])

    def _message_to_storage_dict(
        self,
        peer_id: int,
        message: "Message",
        *,
        file_path: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            message_id = int(getattr(message, "id"))
        except Exception:
            return None
        date_obj = getattr(message, "date", None)
        try:
            date_ts = int(date_obj.timestamp()) if date_obj else int(time.time())
        except Exception:
            date_ts = int(time.time())
        from_id = None
        if getattr(message, "from_user", None):
            try:
                from_id = int(message.from_user.id)
            except Exception:
                from_id = None
        elif getattr(message, "sender_chat", None):
            try:
                from_id = int(message.sender_chat.id)
            except Exception:
                from_id = None
        reply_to = self._extract_reply_to_id(message)
        text = (getattr(message, "text", None) or getattr(message, "caption", None) or "") or ""
        media_type, media_id, file_size, mime, duration, waveform = self._extract_media_meta(message)
        media_group_id = self._extract_media_group_id(message)
        forward_info = self._extract_forward_info(message)
        file_name = self._extract_file_name(message)
        entities = self._entities_to_dicts(
            getattr(message, "entities", None) if media_type == "text" else getattr(message, "caption_entities", None)
        )
        return {
            "id": message_id,
            "date": date_ts,
            "from_id": from_id,
            "reply_to": reply_to,
            "message": text,
            "media_type": media_type,
            "media_id": media_id,
            "file_path": file_path,
            "file_size": file_size,
            "mime": mime,
            "is_deleted": False,
            "forward_info": forward_info,
            "file_name": file_name,
            "entities": entities,
            "duration": duration,
            "waveform": waveform,
            "media_group_id": media_group_id,
        }

    def _store_message_record(
        self,
        message: "Message",
        *,
        file_path: Optional[str] = None,
    ) -> None:
        if not self._storage:
            return
        chat_obj = getattr(message, "chat", None)
        if not chat_obj:
            return
        try:
            peer_id = int(chat_obj.id)
        except Exception:
            return

        try:
            peer_row = self._chat_to_peer_row(chat_obj)
            if peer_row:
                self._storage.upsert_peers([peer_row])

            sender_rows: List[Dict[str, Any]] = []
            user_row = self._user_to_peer_row(getattr(message, "from_user", None))
            if user_row:
                sender_rows.append(user_row)
            sender_chat_row = self._chat_to_peer_row(getattr(message, "sender_chat", None))
            if sender_chat_row:
                sender_rows.append(sender_chat_row)
            if sender_rows:
                self._storage.upsert_peers(sender_rows)

            record = self._message_to_storage_dict(peer_id, message, file_path=file_path)
            if not record:
                return
            self._storage.upsert_messages(peer_id, [record])
            self._storage.update_dialog_last_ts(peer_id, record.get("date"), top_message_id=record.get("id"))
        except Exception as exc:
            err = str(exc).lower()
            if "closed database" in err:
                return
            log.exception("[TG] Failed to persist message %s/%s to storage", getattr(message, "chat", None), getattr(message, "id", None))

    @staticmethod
    def _sender_display_name(message: "Message", fallback: str) -> str:
        user = getattr(message, "from_user", None)
        if user:
            username = getattr(user, "username", "") or ""
            parts = [
                getattr(user, "first_name", "") or "",
                getattr(user, "last_name", "") or "",
            ]
            name = " ".join([p for p in parts if p]).strip()
            return name or username or fallback
        sender_chat = getattr(message, "sender_chat", None)
        if sender_chat:
            return getattr(sender_chat, "title", "") or getattr(sender_chat, "username", "") or fallback
        return fallback

    def _extract_media_meta(
        self, message: "Message"
    ) -> Tuple[str, Optional[str], Optional[int], Optional[str], Optional[int], Optional[List[int]]]:
        for attr, mtype in (
            ("video_note", "video_note"),
            ("photo", "image"),
            ("video", "video"),
            ("animation", "animation"),
            ("voice", "voice"),
            ("audio", "audio"),
            ("document", "document"),
            ("sticker", "sticker"),
        ):
            media_obj = getattr(message, attr, None)
            if not media_obj:
                continue
            file_id = getattr(media_obj, "file_id", None)
            file_size = getattr(media_obj, "file_size", None)
            mime = getattr(media_obj, "mime_type", None)
            media_kind = mtype
            if attr == "photo" and not mime:
                mime = "image/jpeg"
            if attr == "sticker" and not mime:
                mime = getattr(media_obj, "mime_type", None) or "image/webp"
            if attr == "audio":
                mime_l = str(mime or "").lower()
                file_name_l = str(getattr(media_obj, "file_name", None) or "").lower()
                title = getattr(media_obj, "title", None)
                performer = getattr(media_obj, "performer", None)
                is_probably_voice = (
                    ("ogg" in mime_l or "opus" in mime_l)
                    and not title
                    and not performer
                    and (not file_name_l or file_name_l.endswith((".ogg", ".oga", ".opus")))
                )
                if is_probably_voice:
                    media_kind = "voice"
            if attr == "document":
                mime_l = str(mime or "").lower()
                file_name_l = str(getattr(media_obj, "file_name", None) or "").lower()
                if (
                    "audio/ogg" in mime_l
                    or "audio/opus" in mime_l
                    or "application/ogg" in mime_l
                    or file_name_l.endswith((".ogg", ".oga", ".opus"))
                ):
                    media_kind = "voice"
            duration = getattr(media_obj, "duration", None)
            waveform = None
            if media_kind == "voice":
                waveform = self._normalize_waveform(getattr(media_obj, "waveform", None))
            return (
                media_kind,
                file_id,
                int(file_size) if file_size else None,
                mime,
                int(duration) if duration else None,
                waveform,
            )
        return "text", None, None, None, None, None

    @staticmethod
    def _extract_media_group_id(message: "Message") -> Optional[str]:
        raw = getattr(message, "media_group_id", None)
        if raw is None:
            return None
        value = str(raw).strip()
        return value or None

    def _entities_to_dicts(self, entities: Any) -> Optional[List[Dict[str, Any]]]:
        if not entities:
            return None
        out: List[Dict[str, Any]] = []
        for ent in entities:
            try:
                ent_type = getattr(ent, "type", None)
                name = getattr(ent_type, "name", None) or str(ent_type or "")
                key = str(name).strip().lower()
                mapping = {
                    "bold": "bold",
                    "italic": "italic",
                    "underline": "underline",
                    "strikethrough": "strikethrough",
                    "spoiler": "spoiler",
                    "code": "code",
                    "pre": "pre",
                    "text_link": "text_link",
                    "text_mention": "text_link",
                    "mention": "mention",
                    "url": "url",
                    "email": "email",
                    "hashtag": "hashtag",
                    "cashtag": "hashtag",
                    "bot_command": "bot_command",
                    "phone_number": "url",
                    "blockquote": "blockquote",
                }
                etype = mapping.get(key)
                if not etype:
                    continue

                offset = int(getattr(ent, "offset", 0) or 0)
                length = int(getattr(ent, "length", 0) or 0)
                if length <= 0:
                    continue
                payload: Dict[str, Any] = {"type": etype, "offset": offset, "length": length}
                url = getattr(ent, "url", None)
                if etype == "text_link" and url:
                    payload["url"] = str(url)
                if key == "text_mention":
                    user_obj = getattr(ent, "user", None)
                    user_id = getattr(user_obj, "id", None)
                    if user_id:
                        payload["url"] = f"tg://user?id={int(user_id)}"
                language = getattr(ent, "language", None)
                if etype == "pre" and language:
                    payload["language"] = str(language)
                out.append(payload)
            except Exception:
                continue
        return out or None

    @staticmethod
    def _extract_reply_to_id(message: "Message") -> Optional[int]:
        reply_to = getattr(message, "reply_to_message_id", None)
        if reply_to is None:
            reply_obj = getattr(message, "reply_to_message", None)
            if reply_obj is not None:
                try:
                    reply_to = int(reply_obj.id)
                except Exception:
                    reply_to = None
        if reply_to is None:
            return None
        try:
            return int(reply_to)
        except Exception:
            return None

    @staticmethod
    def _extract_forward_info(message: "Message") -> Optional[Dict[str, str]]:
        if not getattr(message, "forward_date", None):
            return None
        info: Dict[str, str] = {}
        sender = getattr(message, "forward_sender_name", None)
        user = getattr(message, "forward_from", None)
        chat = getattr(message, "forward_from_chat", None)
        if user:
            parts = [
                getattr(user, "first_name", "") or "",
                getattr(user, "last_name", "") or "",
            ]
            name = " ".join([p for p in parts if p]).strip() or getattr(user, "username", "") or str(getattr(user, "id", ""))
            info["sender"] = name
        elif sender:
            info["sender"] = sender
        if chat:
            title = getattr(chat, "title", "") or getattr(chat, "username", "") or str(getattr(chat, "id", ""))
            info["chat"] = title
        return info or None

    @staticmethod
    def _extract_file_name(message: "Message") -> Optional[str]:
        for attr in ("document", "audio", "video", "voice", "video_note"):
            media_obj = getattr(message, attr, None)
            if media_obj:
                name = getattr(media_obj, "file_name", None)
                if name:
                    return str(name)
        return None

    @staticmethod
    def _normalize_waveform(raw: Any) -> Optional[List[int]]:
        if raw is None:
            return None
        data: List[int] = []
        if isinstance(raw, (bytes, bytearray)):
            data = list(raw)
        elif isinstance(raw, list):
            try:
                data = [int(v) & 0xFF for v in raw]
            except Exception:
                data = []
        elif isinstance(raw, str):
            try:
                data = list(bytes.fromhex(raw))
            except Exception:
                data = []
        if not data:
            return None
        if len(data) > 120:
            import math

            step = len(data) / 120.0
            collapsed: List[int] = []
            for i in range(120):
                start = int(math.floor(i * step))
                end = int(math.floor((i + 1) * step))
                if end <= start:
                    end = min(len(data), start + 1)
                segment = data[start:end]
                collapsed.append(sum(segment) // len(segment))
            data = collapsed
        return data

    async def _iter_dialogs(self, limit: Optional[int]) -> AsyncIterator[Dict[str, Any]]:
        await self._ensure_me_cached()
        async for dialog in self._client.get_dialogs(limit=limit):
            info = self._dialog_to_dict(dialog)
            if info:
                self._store_dialog_info(dialog, info)
                yield info

    async def _ensure_avatar(
        self, *, file_id: Optional[str], fetcher: Callable[[], Awaitable[Optional[str]]], prefix: str, size: str
    ) -> Optional[str]:
        if not (self._client and self._avatar_dir):
            return None
        target_id = file_id
        if not target_id:
            try:
                target_id = await fetcher()
            except Exception:
                target_id = None
        if not target_id:
            return None

        digest = hashlib.sha1(f"{target_id}:{size}".encode("utf-8", "ignore")).hexdigest()
        dest = self._avatar_dir / f"{prefix}_{digest}.jpg"
        if dest.exists():
            return str(dest)

        try:
            result_path = await self._client.download_media(target_id, file_name=str(dest))
            if not result_path:
                return None
            if result_path != str(dest):
                dest = Path(result_path)
            return str(dest)
        except Exception as exc:
            log.debug("[TG] avatar download failed (%s): %s", prefix, exc)
            if dest.exists():
                with contextlib.suppress(Exception):
                    dest.unlink()
            return None

    async def _ensure_chat_avatar(self, chat_id: str, *, file_id: Optional[str], size: str) -> Optional[str]:
        if not self._client:
            return None

        async def _fetch() -> Optional[str]:
            try:
                chat = await self._client.get_chat(int(chat_id))
            except Exception:
                return None
            photo = getattr(chat, "photo", None)
            if not photo:
                return None
            attr = "small_file_id" if size == "small" else "big_file_id"
            return getattr(photo, attr, None)

        prefix = f"chat_{chat_id}"
        return await self._ensure_avatar(file_id=file_id, fetcher=_fetch, prefix=prefix, size=size)

    async def _ensure_user_avatar(self, user_id: str, *, file_id: Optional[str], size: str) -> Optional[str]:
        if not self._client:
            return None

        async def _fetch() -> Optional[str]:
            try:
                if user_id in {"me", "self"}:
                    user = await self._client.get_me()
                else:
                    try:
                        uid = int(user_id)
                        user = await self._client.get_users(uid)
                    except Exception:
                        user = await self._client.get_users(user_id)
            except Exception:
                return None
            photo = getattr(user, "photo", None)
            if not photo:
                return None
            attr = "small_file_id" if size == "small" else "big_file_id"
            return getattr(photo, attr, None)

        prefix = f"user_{user_id}"
        return await self._ensure_avatar(file_id=file_id, fetcher=_fetch, prefix=prefix, size=size)

    def ensure_chat_avatar_sync(
        self, chat_id: str, *, file_id: Optional[str] = None, size: str = "small"
    ) -> Optional[str]:
        if not (self._enabled and self._client and self._loop):
            return None

        async def _run() -> Optional[str]:
            return await self._ensure_chat_avatar(chat_id, file_id=file_id, size=size)

        try:
            return self._call(_run(), 20.0)
        except Exception:
            return None

    def ensure_user_avatar_sync(
        self, user_id: str, *, file_id: Optional[str] = None, size: str = "small"
    ) -> Optional[str]:
        if not (self._enabled and self._client and self._loop):
            return None

        async def _run() -> Optional[str]:
            return await self._ensure_user_avatar(user_id, file_id=file_id, size=size)

        try:
            return self._call(_run(), 20.0)
        except Exception:
            return None

    def list_all_chats_sync(self, limit: Optional[int] = 400, timeout: float = 20.0) -> List[Dict[str, Any]]:
        if not (self._enabled and self._client and self._loop):
            return []

        async def _collect():
            out: List[Dict[str, Any]] = []
            try:
                async for info in self._iter_dialogs(limit):
                    out.append(info)
            except Exception:
                pass
            return out

        try:
            return list(self._call(_collect(), timeout) or [])
        except Exception:
            return []

    def stream_dialogs(
        self, on_batch: Callable[[List[Dict[str, Any]]], None], on_done: Optional[Callable[[], None]] = None,
        limit: Optional[int] = 400, batch_size: int = 50
    ) -> None:
        if not (self._enabled and self._client and self._loop):
            if on_done:
                try:
                    on_done()
                except Exception:
                    pass
            return

        async def _stream():
            batch: List[Dict[str, Any]] = []
            try:
                async for info in self._iter_dialogs(limit):
                    batch.append(info)
                    if len(batch) >= batch_size:
                        try:
                            on_batch(list(batch))
                        finally:
                            batch.clear()
            except Exception:
                pass
            finally:
                if batch:
                    try:
                        on_batch(list(batch))
                    except Exception:
                        pass
                if on_done:
                    try:
                        on_done()
                    except Exception:
                        pass

        asyncio.run_coroutine_threadsafe(_stream(), self._loop)

    def send_text_sync(
        self,
        chat_id: str,
        text: str,
        reply_to: Optional[int] = None,
        *,
        entities: Optional[List[Dict[str, Any]]] = None,
        timeout: float = 15.0,
    ) -> bool:
        if not (self._enabled and self._client and self._loop):
            return False

        async def _send():
            try:
                kwargs: Dict[str, Any] = {}
                if entities:
                    py_entities: List[MessageEntity] = []
                    for ent in entities:
                        if not isinstance(ent, dict):
                            continue
                        etype_raw = str(ent.get("type") or "").strip().lower()
                        mapping = {
                            "bold": "BOLD",
                            "italic": "ITALIC",
                            "underline": "UNDERLINE",
                            "strikethrough": "STRIKETHROUGH",
                            "spoiler": "SPOILER",
                            "code": "CODE",
                            "pre": "PRE",
                            "text_link": "TEXT_LINK",
                            "url": "URL",
                            "email": "EMAIL",
                            "hashtag": "HASHTAG",
                            "blockquote": "BLOCKQUOTE",
                        }
                        me_name = mapping.get(etype_raw)
                        if not me_name:
                            continue
                        me_type = getattr(enums.MessageEntityType, me_name, None)
                        if me_type is None:
                            continue
                        try:
                            offset = int(ent.get("offset") or 0)
                            length = int(ent.get("length") or 0)
                        except Exception:
                            continue
                        if length <= 0:
                            continue
                        extra: Dict[str, Any] = {"type": me_type, "offset": offset, "length": length}
                        if me_type == enums.MessageEntityType.TEXT_LINK and ent.get("url"):
                            extra["url"] = str(ent.get("url"))
                        if me_type == enums.MessageEntityType.PRE and ent.get("language"):
                            extra["language"] = str(ent.get("language"))
                        py_entities.append(MessageEntity(**extra))
                    if py_entities:
                        kwargs["entities"] = py_entities

                msg_obj = await self._client.send_message(
                    int(chat_id),
                    text,
                    reply_to_message_id=int(reply_to) if reply_to else None,
                    **kwargs,
                )
                if msg_obj is not None:
                    self._remember_local_outgoing(getattr(msg_obj, "id", None))
                return True
            except Exception:
                return False

        return bool(self._call(_send(), timeout))

    def delete_messages_sync(self, chat_id: str, message_ids: List[int], timeout: float = 15.0) -> bool:
        if not (self._enabled and self._client and self._loop):
            return False
        mids = [int(mid) for mid in message_ids if mid is not None]
        if not mids:
            return False

        async def _delete():
            try:
                await self._client.delete_messages(int(chat_id), mids, revoke=True)
                return True
            except Exception:
                return False

        return bool(self._call(_delete(), timeout))

    def edit_message_text_sync(self, chat_id: str, message_id: int, text: str, timeout: float = 15.0) -> bool:
        if not (self._enabled and self._client and self._loop):
            return False

        async def _edit():
            try:
                await self._client.edit_message_text(int(chat_id), int(message_id), text)
                return True
            except Exception:
                return False

        return bool(self._call(_edit(), timeout))

    def forward_message_sync(
        self,
        *,
        from_chat_id: str,
        message_id: int,
        to_chat_id: str,
        timeout: float = 15.0,
    ) -> bool:
        if not (self._enabled and self._client and self._loop):
            return False

        async def _forward():
            try:
                await self._client.forward_messages(
                    int(to_chat_id),
                    int(from_chat_id),
                    [int(message_id)],
                )
                return True
            except Exception:
                return False

        return bool(self._call(_forward(), timeout))

    def send_reaction_sync(
        self,
        *,
        chat_id: str,
        message_id: int,
        reaction: str,
        timeout: float = 15.0,
    ) -> bool:
        if not (self._enabled and self._client and self._loop):
            return False
        emoji = str(reaction or "").strip()
        if not emoji:
            return False

        async def _react():
            try:
                # Pyrogram v2 API uses send_reaction(chat_id, message_id, emoji=...).
                await self._client.send_reaction(
                    chat_id=int(chat_id),
                    message_id=int(message_id),
                    emoji=emoji,
                )
                return True
            except Exception as exc:
                log.debug("send_reaction failed: %s", exc)
                return False

        return bool(self._call(_react(), timeout))

    def mark_chat_read_sync(self, chat_id: str, timeout: float = 15.0) -> bool:
        if not (self._enabled and self._client and self._loop):
            return False

        async def _mark():
            try:
                await self._client.read_history(int(chat_id))
                return True
            except Exception:
                return False

        return bool(self._call(_mark(), timeout))

    def _send_media_sync(
            self,
            *,
            method: str,
            payload_key: str,
            chat_id: str,
            path: str,
            caption: Optional[str],
            timeout: float,
            extra_kwargs: Optional[Dict[str, Any]] = None,
            log_tag: str = "send_media",
    ) -> bool:
        if not (self._enabled and self._client and self._loop):
            return False
        if not path:
            return False

        async def _send():
            msg_obj = None
            try:
                await self._ensure_me_cached()
                kwargs: Dict[str, Any] = {
                    "chat_id": int(chat_id),
                    payload_key: path,
                }
                if caption:
                    kwargs["caption"] = caption
                if extra_kwargs:
                    kwargs.update(extra_kwargs)

                # ВАЖНО: вызов должен быть всегда
                msg_obj = await getattr(self._client, method)(**kwargs)

                if msg_obj is not None:
                    self._remember_local_outgoing(getattr(msg_obj, "id", None))
                return True
            except Exception as e:
                log.exception("%s failed: %s", log_tag, e)
                return False

        fut = asyncio.run_coroutine_threadsafe(_send(), self._loop)
        try:
            return bool(fut.result(timeout=timeout))
        except Exception:
            return False

    def send_voice_sync(
            self,
            chat_id: str,
            voice_path: str | None = None,
            *,
            file_path: str | None = None,  # поддержка старого имени
            duration: int | None = None,
            caption: str | None = None,
            reply_to: int | None = None,
            timeout: float = 120.0,
    ) -> bool:
        """
        Отправить голосовое (OGG/Opus). Важно: прогреть self.me (см. _ensure_me_cached),
        и НЕ передавать duration, если он None (Pyrogram падает).
        """
        path = voice_path or file_path
        if not path:
            return False

        extra: Dict[str, Any] = {}
        if isinstance(duration, int):
            extra["duration"] = int(duration)
        if reply_to is not None:
            try:
                extra["reply_to_message_id"] = int(reply_to)
            except Exception:
                pass

        return self._send_media_sync(
            method="send_voice",
            payload_key="voice",
            chat_id=chat_id,
            path=path,
            caption=caption,
            timeout=timeout,
            extra_kwargs=extra if extra else None,
            log_tag="send_voice",
        )

    def send_audio_sync(
            self,
            chat_id: str,
            audio_path: str | None = None,
            *,
            file_path: str | None = None,  # поддержка старого имени
            title: str | None = None,
            performer: str | None = None,
            duration: int | None = None,
            caption: str | None = None,
            timeout: float = 120.0,
            # --- новое ---
            also_voice: bool = False,
            voice_bitrate: str = "24k",
            voice_ar: int = 48000,
            voice_mono: bool = True,
    ) -> bool:
        """
        Отправить обычное аудио (музыка/MP3/FLAC/WAV).
        Важный момент: прогреть self.me для получения лимита аплоада; duration обязателен int или None.

        Если also_voice=True — дополнительно конвертируем тот же файл в OGG/Opus и отправляем
        как голосовое сообщение (voice) следом. Если ffmpeg/ffprobe недоступны — голосовое пропустим.
        """
        path = audio_path or file_path
        if not path:
            return False

        # 1) отправляем обычное аудио
        extra: Dict[str, Any] = {}
        if title:
            extra["title"] = title
        if performer:
            extra["performer"] = performer
        if isinstance(duration, int):
            extra["duration"] = int(duration)

        ok_audio = self._send_media_sync(
            method="send_audio",
            payload_key="audio",
            chat_id=chat_id,
            path=path,
            caption=caption,
            timeout=timeout,
            extra_kwargs=extra if extra else None,
            log_tag="send_audio",
        )

        # 2) при необходимости — отправляем то же как voice
        if also_voice:
            ogg_path: Optional[str] = None
            try:
                # если уже .ogg/.opus — можно отправить как есть
                ext = os.path.splitext(path)[1].lower()
                if ext in (".ogg", ".opus", ".oga"):
                    ogg_path = path
                else:
                    ogg_path = self._convert_to_ogg_opus(
                        path,
                        bitrate=voice_bitrate,
                        ar=voice_ar,
                        mono=voice_mono,
                    )

                if ogg_path:
                    voice_dur = self._probe_duration_sec(ogg_path)
                    ok_voice = self.send_voice_sync(
                        chat_id=chat_id,
                        voice_path=ogg_path,
                        duration=voice_dur if isinstance(voice_dur, int) else None,
                        caption=None,  # обычно для voice подпись не нужна; при желании можно продублировать
                        timeout=max(30.0, timeout),
                    )
                else:
                    ok_voice = False
            finally:
                # чистим временный файл, если мы его создавали
                if ogg_path and ogg_path != path:
                    try:
                        tmp_dir = os.path.dirname(ogg_path)
                        os.remove(ogg_path)
                        # сносим директорию, если пустая
                        try:
                            os.rmdir(tmp_dir)
                        except Exception:
                            pass
                    except Exception:
                        pass
            return ok_audio and ok_voice

        return ok_audio

    def send_photo_sync(
            self,
            chat_id: str,
            photo_path: str,
            *,
            caption: str | None = None,
            reply_to: int | None = None,
            timeout: float = 180.0,
    ) -> bool:
        """
        Отправка изображения с подписью.
        """
        extra: Dict[str, Any] = {}
        if reply_to is not None:
            try:
                extra["reply_to_message_id"] = int(reply_to)
            except Exception:
                pass
        return self._send_media_sync(
            method="send_photo",
            payload_key="photo",
            chat_id=chat_id,
            path=photo_path,
            caption=caption,
            timeout=timeout,
            extra_kwargs=extra or None,
            log_tag="send_photo",
        )

    def send_video_sync(
            self,
            chat_id: str,
            video_path: str,
            *,
            caption: str | None = None,
            reply_to: int | None = None,
            duration: int | None = None,
            timeout: float = 240.0,
    ) -> bool:
        """
        Отправка обычного видео (не video note).
        """
        extra: Dict[str, Any] = {}
        if reply_to is not None:
            try:
                extra["reply_to_message_id"] = int(reply_to)
            except Exception:
                pass
        if isinstance(duration, int):
            extra["duration"] = int(duration)
        return self._send_media_sync(
            method="send_video",
            payload_key="video",
            chat_id=chat_id,
            path=video_path,
            caption=caption,
            timeout=timeout,
            extra_kwargs=extra or None,
            log_tag="send_video",
        )

    def send_media_group_sync(
        self,
        *,
        chat_id: str,
        items: List[Dict[str, Any]],
        reply_to: Optional[int] = None,
        timeout: float = 300.0,
    ) -> List[int]:
        if not (self._enabled and self._client and self._loop):
            return []
        prepared = list(items or [])
        if not prepared:
            return []

        async def _send_group() -> List[int]:
            media: List[Any] = []
            for idx, item in enumerate(prepared):
                kind = str(item.get("kind") or "").strip().lower()
                path = str(item.get("path") or "").strip()
                caption = str(item.get("caption") or "").strip() or None
                if not path:
                    continue
                if kind in {"image", "photo"}:
                    if idx == 0 and caption:
                        media.append(InputMediaPhoto(media=path, caption=caption))
                    else:
                        media.append(InputMediaPhoto(media=path))
                    continue
                if kind == "video":
                    if idx == 0 and caption:
                        media.append(InputMediaVideo(media=path, caption=caption))
                    else:
                        media.append(InputMediaVideo(media=path))
                    continue
            if not media:
                return []
            try:
                await self._ensure_me_cached()
                kwargs: Dict[str, Any] = {}
                if reply_to is not None:
                    kwargs["reply_to_message_id"] = int(reply_to)
                sent = await self._client.send_media_group(
                    chat_id=int(chat_id),
                    media=media,
                    **kwargs,
                )
            except Exception as exc:
                log.exception("send_media_group failed: %s", exc)
                return []

            mids: List[int] = []
            for message in sent or []:
                try:
                    mid = int(getattr(message, "id", 0) or 0)
                except Exception:
                    mid = 0
                if mid <= 0:
                    continue
                mids.append(mid)
                self._remember_local_outgoing(mid)
            return mids

        result = self._call(_send_group(), timeout)
        return list(result or [])

    def send_sticker_sync(
            self,
            chat_id: str,
            sticker_path: str,
            *,
            reply_to: int | None = None,
            timeout: float = 180.0,
    ) -> bool:
        """Отправить стикер (webp/tgs/webm)."""
        extra: Dict[str, Any] = {}
        if reply_to is not None:
            try:
                extra["reply_to_message_id"] = int(reply_to)
            except Exception:
                pass
        return self._send_media_sync(
            method="send_sticker",
            payload_key="sticker",
            chat_id=chat_id,
            path=sticker_path,
            caption=None,
            timeout=timeout,
            extra_kwargs=extra or None,
            log_tag="send_sticker",
        )

    def send_sticker_id_with_id_sync(
        self,
        chat_id: str,
        sticker_file_id: str,
        *,
        reply_to: int | None = None,
        timeout: float = 15.0,
    ) -> Optional[int]:
        """Отправить стикер по file_id и вернуть message_id (если удалось)."""
        if not (self._enabled and self._client and self._loop):
            return None
        sticker_file_id = str(sticker_file_id or "").strip()
        if not sticker_file_id:
            return None

        async def _send() -> Optional[int]:
            try:
                kwargs: Dict[str, Any] = {}
                if reply_to is not None:
                    kwargs["reply_to_message_id"] = int(reply_to)
                msg_obj = await self._client.send_sticker(int(chat_id), sticker_file_id, **kwargs)
                mid = int(getattr(msg_obj, "id", 0) or 0) if msg_obj is not None else 0
                if mid:
                    self._remember_local_outgoing(mid)
                    return mid
                return None
            except Exception:
                return None

        try:
            return self._call(_send(), timeout)
        except Exception:
            return None

    def send_sticker_id_sync(
        self,
        chat_id: str,
        sticker_file_id: str,
        *,
        reply_to: int | None = None,
        timeout: float = 15.0,
    ) -> bool:
        """Отправить стикер по file_id (из наборов/недавних)."""
        return bool(self.send_sticker_id_with_id_sync(chat_id, sticker_file_id, reply_to=reply_to, timeout=timeout))

    def send_animation_sync(
            self,
            chat_id: str,
            animation_path: str,
            *,
            caption: str | None = None,
            reply_to: int | None = None,
            timeout: float = 180.0,
    ) -> bool:
        """Отправить GIF/анимацию (как animation)."""
        extra: Dict[str, Any] = {}
        if reply_to is not None:
            try:
                extra["reply_to_message_id"] = int(reply_to)
            except Exception:
                pass
        return self._send_media_sync(
            method="send_animation",
            payload_key="animation",
            chat_id=chat_id,
            path=animation_path,
            caption=caption,
            timeout=timeout,
            extra_kwargs=extra or None,
            log_tag="send_animation",
        )

    def list_sticker_sets_sync(self, timeout: float = 15.0) -> List[Dict[str, Any]]:
        """Список установленных стикерпаков (raw GetAllStickers)."""
        if not (self._enabled and self._client and self._loop):
            return []

        async def _run() -> List[Dict[str, Any]]:
            try:
                res = await self._client.invoke(raw_fn.messages.GetAllStickers(hash=0))
            except Exception:
                return []
            if isinstance(res, raw_types.messages.AllStickersNotModified):
                return []
            sets = getattr(res, "sets", None) or []
            out: List[Dict[str, Any]] = []
            for s in sets:
                try:
                    out.append(
                        {
                            "id": int(getattr(s, "id", 0) or 0),
                            "access_hash": int(getattr(s, "access_hash", 0) or 0),
                            "title": str(getattr(s, "title", "") or ""),
                            "short_name": str(getattr(s, "short_name", "") or ""),
                            "count": int(getattr(s, "count", 0) or 0),
                            "animated": bool(getattr(s, "animated", False)),
                            "videos": bool(getattr(s, "videos", False)),
                            "emojis": bool(getattr(s, "emojis", False)),
                        }
                    )
                except Exception:
                    continue
            return out

        try:
            return list(self._call(_run(), timeout) or [])
        except Exception:
            return []

    def get_recent_stickers_sync(self, *, attached: bool = False, timeout: float = 15.0) -> List[Dict[str, Any]]:
        """Недавние стикеры (raw GetRecentStickers)."""
        if not (self._enabled and self._client and self._loop):
            return []

        async def _run() -> List[Dict[str, Any]]:
            try:
                res = await self._client.invoke(raw_fn.messages.GetRecentStickers(hash=0, attached=bool(attached)))
            except Exception:
                return []
            if isinstance(res, raw_types.messages.RecentStickersNotModified):
                return []
            docs = getattr(res, "stickers", None) or []
            packs = getattr(res, "packs", None) or []
            emoticons: Dict[int, str] = {}
            for p in packs:
                try:
                    emoji = str(getattr(p, "emoticon", "") or "")
                    for did in getattr(p, "documents", None) or []:
                        try:
                            emoticons[int(did)] = emoji
                        except Exception:
                            continue
                except Exception:
                    continue

            out: List[Dict[str, Any]] = []
            try:
                from pyrogram.file_id import FileId, FileType
            except Exception:
                return []

            for doc in docs:
                try:
                    did = int(getattr(doc, "id", 0) or 0)
                    fid = FileId(
                        file_type=FileType.STICKER,
                        dc_id=int(getattr(doc, "dc_id", 0) or 0),
                        media_id=did,
                        access_hash=int(getattr(doc, "access_hash", 0) or 0),
                        file_reference=bytes(getattr(doc, "file_reference", b"") or b""),
                    ).encode()
                    mime = str(getattr(doc, "mime_type", "") or "")
                    out.append(
                        {
                            "file_id": fid,
                            "doc_id": did,
                            "emoji": emoticons.get(did, ""),
                            "mime": mime,
                        }
                    )
                except Exception:
                    continue
            return out

        try:
            return list(self._call(_run(), timeout) or [])
        except Exception:
            return []

    def get_sticker_set_items_sync(
        self,
        *,
        set_id: int,
        access_hash: int,
        timeout: float = 20.0,
    ) -> List[Dict[str, Any]]:
        """Получить стикеры набора (raw GetStickerSet)."""
        if not (self._enabled and self._client and self._loop):
            return []

        async def _run() -> List[Dict[str, Any]]:
            try:
                req = raw_fn.messages.GetStickerSet(
                    stickerset=raw_types.InputStickerSetID(id=int(set_id), access_hash=int(access_hash)),
                    hash=0,
                )
                res = await self._client.invoke(req)
            except Exception:
                return []
            if isinstance(res, raw_types.messages.StickerSetNotModified):
                return []
            docs = getattr(res, "documents", None) or []
            packs = getattr(res, "packs", None) or []
            emoticons: Dict[int, str] = {}
            for p in packs:
                try:
                    emoji = str(getattr(p, "emoticon", "") or "")
                    for did in getattr(p, "documents", None) or []:
                        try:
                            emoticons[int(did)] = emoji
                        except Exception:
                            continue
                except Exception:
                    continue

            try:
                from pyrogram.file_id import FileId, FileType
            except Exception:
                return []

            out: List[Dict[str, Any]] = []
            for doc in docs:
                try:
                    did = int(getattr(doc, "id", 0) or 0)
                    fid = FileId(
                        file_type=FileType.STICKER,
                        dc_id=int(getattr(doc, "dc_id", 0) or 0),
                        media_id=did,
                        access_hash=int(getattr(doc, "access_hash", 0) or 0),
                        file_reference=bytes(getattr(doc, "file_reference", b"") or b""),
                    ).encode()
                    mime = str(getattr(doc, "mime_type", "") or "")
                    out.append(
                        {
                            "file_id": fid,
                            "doc_id": did,
                            "emoji": emoticons.get(did, ""),
                            "mime": mime,
                        }
                    )
                except Exception:
                    continue
            return out

        try:
            return list(self._call(_run(), timeout) or [])
        except Exception:
            return []

    def download_file_id_sync(self, file_id: str, *, file_name: str, timeout: float = 20.0) -> Optional[str]:
        """Скачать медиа по file_id (используется для превью стикеров)."""
        if not (self._enabled and self._client and self._loop):
            return None
        file_id = str(file_id or "").strip()
        if not file_id:
            return None
        target = str(file_name or "").strip()
        if not target:
            return None

        async def _dl() -> Optional[str]:
            try:
                coro = self._client.download_media(file_id, file_name=target)
                path = await asyncio.wait_for(coro, timeout=max(1.0, float(timeout)))
                if path:
                    return str(path)
                return str(target) if os.path.isfile(target) else None
            except Exception:
                return None

        try:
            return self._call(_dl(), timeout)
        except Exception:
            return None

    def send_document_sync(
            self,
            chat_id: str,
            document_path: str,
            *,
            caption: str | None = None,
            reply_to: int | None = None,
            filename: str | None = None,
            timeout: float = 180.0,
    ) -> bool:
        extra: Dict[str, Any] = {}
        if reply_to is not None:
            try:
                extra["reply_to_message_id"] = int(reply_to)
            except Exception:
                pass
        if filename:
            extra["filename"] = filename
        return self._send_media_sync(
            method="send_document",
            payload_key="document",
            chat_id=chat_id,
            path=document_path,
            caption=caption,
            timeout=timeout,
            extra_kwargs=extra or None,
            log_tag="send_document",
        )

    # -------------------- history / downloads --------------------
    @staticmethod
    def _find_cached_media_file(media_root: pathlib.Path, message_id: int) -> Optional[str]:
        try:
            mid = int(message_id)
        except Exception:
            return None
        pattern = f"{mid}*"
        for candidate in media_root.glob(pattern):
            if candidate.is_file():
                return str(candidate)
        return None

    def send_video_note_sync(
            self,
            chat_id: str,
            video_note_path: str | None = None,
            *,
            file_path: str | None = None,
            duration: int | None = None,
            length: int | None = 480,
            caption: str | None = None,
            reply_to: int | None = None,
            timeout: float = 180.0,
    ) -> bool:
        """
        Отправить видео-кружок (video note).
        Требование: квадратное видео до 1 мин, h264 + aac.
        """
        path = file_path or video_note_path
        if not path:
            return False

        extra: Dict[str, Any] = {}
        if isinstance(duration, int):
            extra["duration"] = int(duration)
        if isinstance(length, int):
            extra["length"] = int(length)
        if reply_to is not None:
            try:
                extra["reply_to_message_id"] = int(reply_to)
            except Exception:
                pass

        return self._send_media_sync(
            method="send_video_note",
            payload_key="video_note",
            chat_id=chat_id,
            path=path,
            caption=caption,
            timeout=timeout,
            extra_kwargs=extra if extra else None,
            log_tag="send_video_note",
        )

    def get_history_sync(
        self, chat_id: str, limit: int = 80, download_media: bool = False, timeout: float = 45.0
    ) -> List[Dict[str, Any]]:
        if not (self._enabled and self._client and self._loop):
            return []
        if self._auth_invalid or not self._connected:
            return []

        media_root = pathlib.Path(self._media_root) / str(chat_id)
        media_root.mkdir(parents=True, exist_ok=True)

        async def _hist():
            out: List[Dict[str, Any]] = []
            async for m in self._client.get_chat_history(int(chat_id), limit=limit):
                media_type, _media_id, file_size, _mime, duration, waveform = self._extract_media_meta(m)
                item = {
                    "id": m.id,
                    "date": int(m.date.timestamp()) if m.date else None,
                    "sender_id": str(m.from_user.id) if m.from_user else "",
                    "sender": (m.from_user.username if (m.from_user and m.from_user.username) else
                               (m.from_user.first_name if m.from_user else "")),
                    "type": media_type or "text",
                    "text": (m.text or m.caption or "") or "",
                    "entities": self._entities_to_dicts(
                        getattr(m, "entities", None) if (media_type or "text") == "text" else getattr(m, "caption_entities", None)
                    ),
                    "file_path": None,
                    "thumb_path": None,
                    "file_size": int(file_size or 0),
                    "reply_to": self._extract_reply_to_id(m),
                    "forward_info": self._extract_forward_info(m),
                    "file_name": self._extract_file_name(m),
                    "is_deleted": False,
                    "duration": int(duration) if duration else None,
                    "waveform": waveform,
                    "media_group_id": self._extract_media_group_id(m),
                }

                if item["type"] != "text":
                    cached = self._find_cached_media_file(media_root, m.id)
                    if cached:
                        item["file_path"] = cached
                    if not item["file_size"]:
                        size = self._estimate_media_size(m)
                        if size:
                            item["file_size"] = int(size)

                if download_media and item["type"] != "text" and not item["file_path"]:
                    try:
                        fp = await m.download(file_name=str(media_root / f"{m.id}"))
                        item["file_path"] = fp
                    except Exception:
                        pass
                self._store_message_record(m, file_path=item.get("file_path"))
                out.append(item)
            return out

        try:
            result = self._call(_hist(), timeout)
            return list(result or [])
        except TimeoutError:
            self._throttled_warning(
                "_last_history_timeout_at",
                8.0,
                "[TG] get_history timeout chat=%s limit=%s timeout=%.1f",
                chat_id,
                limit,
                float(timeout),
            )
            return []
        except Exception as exc:
            if self._is_auth_issue(exc):
                self._notify_auth_issue(exc)
                return []
            if "closed database" in str(exc).lower():
                return []
            raise

    def download_media_sync(self, chat_id: str, message_id: int, timeout: float = 180.0) -> Optional[str]:
        if not (self._enabled and self._client and self._loop):
            return None

        media_root = pathlib.Path(self._media_root) / str(chat_id)
        media_root.mkdir(parents=True, exist_ok=True)

        try:
            mid = int(message_id)
        except Exception:
            mid = None

        if mid is not None:
            cached = self._find_cached_media_file(media_root, mid)
            if cached:
                return cached

        async def _dl():
            msg_obj: Optional["Message"] = None
            try:
                msg_obj = await self._client.get_messages(int(chat_id), int(message_id))
                file_name = media_root / f"{message_id}"
                path = await msg_obj.download(file_name=str(file_name))
                self._store_message_record(msg_obj, file_path=path)
                return path
            except Exception:
                if msg_obj is not None:
                    self._store_message_record(msg_obj)
                return None

        try:
            return self._call(_dl(), timeout)
        except TimeoutError:
            log.warning("[TG] download_media timeout chat=%s message=%s", chat_id, message_id)
            return None
        except Exception as exc:
            if "closed database" in str(exc).lower():
                return None
            raise

    def download_thumb_sync(self, chat_id: str, message_id: int, timeout: float = 30.0) -> Optional[str]:
        """
        Скачивает thumbnail для видео/анимации.
        Возвращает путь к превью или None.
        """

        async def _do():
            msg = await self._client.get_messages(int(chat_id), int(message_id))
            media_dir = Path(self._media_root) / str(chat_id)
            media_dir.mkdir(parents=True, exist_ok=True)

            photo = getattr(msg, "photo", None)
            if photo:
                out = (media_dir / f"{message_id}_thumb").with_suffix(".jpg")
                if out.exists():
                    return str(out)
                try:
                    path = await self._client.download_media(photo, file_name=str(out))
                except Exception:
                    path = await msg.download(file_name=str(out))
                return path

            media_obj = None
            if getattr(msg, "video", None):
                media_obj = msg.video
            elif getattr(msg, "animation", None):
                media_obj = msg.animation
            elif getattr(msg, "video_note", None):
                media_obj = msg.video_note
            elif getattr(msg, "document", None) and getattr(msg.document, "thumbs", None):
                media_obj = msg.document

            if not media_obj:
                return None

            thumbs = getattr(media_obj, "thumbs", None) or []
            tn = getattr(media_obj, "thumbnail", None)
            if tn:
                thumbs = [tn] if not thumbs else thumbs

            if not thumbs:
                return None

            target = sorted(thumbs, key=lambda t: (getattr(t, "width", 99999) or 99999))[0]
            out = media_dir / f"{message_id}_thumb"
            if out.exists():
                return str(out)
            path = await self._client.download_media(target, file_name=str(out))
            return path

        return self._call(_do(), timeout)

    def start_media_download(
        self,
        *,
        chat_id: str,
        message_id: int,
        progress_cb: Callable[[str, Dict[str, Any]], None],
    ) -> Optional[str]:
        if not (self._enabled and self._client and self._loop):
            return None

        media_root = pathlib.Path(self._media_root) / str(chat_id)
        media_root.mkdir(parents=True, exist_ok=True)

        try:
            mid = int(message_id)
        except Exception:
            mid = None

        if mid is not None:
            cached = self._find_cached_media_file(media_root, mid)
            if cached:
                if self._storage:
                    try:
                        self._storage.update_message_file_path(int(chat_id), int(mid), path=cached, size=None, mime=None)
                    except Exception:
                        log.exception("[TG] Failed to update storage with cached media path")
                if progress_cb:
                    try:
                        progress_cb(
                            "",
                            {
                                "state": "completed",
                                "current": 1,
                                "total": 1,
                                "file_path": cached,
                                "cached": True,
                            },
                        )
                    except Exception as exc:
                        log.exception("[TG] cached media callback failed: %s", exc)
                return ""

        job_id = f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
        job = _DownloadJob(
            job_id=job_id,
            chat_id=str(chat_id),
            message_id=int(message_id),
            target_dir=media_root,
            progress_cb=progress_cb,
        )

        with self._download_lock:
            existing = self._message_to_job.get((job.chat_id, job.message_id))
            if existing:
                return existing
            self._download_jobs[job_id] = job
            self._message_to_job[(job.chat_id, job.message_id)] = job_id

        if progress_cb:
            try:
                progress_cb(job_id, {"state": "queued", "current": 0, "total": 0, "file_path": None})
            except Exception as exc:
                log.exception("[TG] queued progress callback failed: %s", exc)

        def _launch() -> None:
            if not self._loop or not self._client:
                return
            job.resume_event = asyncio.Event()
            job.resume_event.set()
            job.cancel_event = asyncio.Event()
            job.future = self._loop.create_task(self._run_download_job(job))

        self._loop.call_soon_threadsafe(_launch)
        return job_id

    def pause_media_download(self, job_id: str) -> bool:
        loop = self._loop
        if not loop:
            return False
        job = self._download_jobs.get(job_id)
        if not job or not job.resume_event:
            return False

        def _do_pause() -> None:
            if job.resume_event and job.resume_event.is_set():
                job.resume_event.clear()
                job.state = "paused"
                loop.create_task(self._emit_download_event(job, "paused"))

        loop.call_soon_threadsafe(_do_pause)
        return True

    def resume_media_download(self, job_id: str) -> bool:
        loop = self._loop
        if not loop:
            return False
        job = self._download_jobs.get(job_id)
        if not job or not job.resume_event:
            return False

        def _do_resume() -> None:
            if job.resume_event and not job.resume_event.is_set():
                job.resume_event.set()
                job.state = "downloading"
                loop.create_task(self._emit_download_event(job, "resumed"))

        loop.call_soon_threadsafe(_do_resume)
        return True

    def cancel_media_download(self, job_id: str) -> bool:
        loop = self._loop
        if not loop:
            return False
        job = self._download_jobs.get(job_id)
        if not job:
            return False

        def _do_cancel() -> None:
            if job.cancel_event and not job.cancel_event.is_set():
                job.cancel_event.set()
            if job.future and not job.future.done():
                job.future.cancel()

        loop.call_soon_threadsafe(_do_cancel)
        return True

    async def _emit_download_event(self, job: _DownloadJob, state: str, **extra: Any) -> None:
        if not job.progress_cb:
            return
        now = time.time()
        if state == "progress":
            if job.last_emit and (now - job.last_emit) < 0.12 and job.current not in (0, job.total):
                return
            job.last_emit = now

        payload: Dict[str, Any] = {
            "state": state,
            "current": int(job.current),
            "total": int(job.total),
            "file_path": job.file_path,
        }
        payload.update(extra)
        try:
            job.progress_cb(job.job_id, payload)
        except Exception as exc:
            log.exception("[TG] download progress callback error: %s", exc)

    async def _run_download_job(self, job: _DownloadJob) -> None:
        assert self._client is not None

        target_path = job.target_dir / f"{job.message_id}"
        with contextlib.suppress(Exception):
            job.target_dir.mkdir(parents=True, exist_ok=True)

        try:
            job.state = "downloading"
            await self._emit_download_event(job, "downloading")
            msg = await self._client.get_messages(int(job.chat_id), int(job.message_id))
            if not self._message_has_downloadable_media(msg):
                job.state = "error"
                await self._emit_download_event(job, "error", error="В сообщении нет медиа для загрузки")
                return
            total_est = self._estimate_media_size(msg)
            if total_est:
                job.total = int(total_est)

            async def _progress(current: int, total: int) -> None:
                if job.cancel_event and job.cancel_event.is_set():
                    raise asyncio.CancelledError()
                if job.resume_event:
                    while not job.resume_event.is_set():
                        if job.cancel_event and job.cancel_event.is_set():
                            raise asyncio.CancelledError()
                        await asyncio.sleep(0.12)
                job.current = int(current)
                if total:
                    job.total = int(total)
                await self._emit_download_event(job, "progress")

            path = await self._client.download_media(
                msg,
                file_name=str(target_path),
                progress=_progress if job.progress_cb else None,
            )
            if job.cancel_event and job.cancel_event.is_set():
                raise asyncio.CancelledError()

            job.file_path = path or str(target_path)
            job.current = job.total or job.current
            job.state = "completed"
            self._store_message_record(msg, file_path=job.file_path)
            await self._emit_download_event(job, "completed", file_path=job.file_path)
        except asyncio.CancelledError:
            job.state = "cancelled"
            await self._emit_download_event(job, "cancelled")
            with contextlib.suppress(Exception):
                if target_path.exists():
                    target_path.unlink()
            return
        except Exception as exc:
            job.state = "error"
            await self._emit_download_event(job, "error", error=str(exc))
            log.exception("[TG] download job failed (%s/%s): %s", job.chat_id, job.message_id, exc)
        finally:
            with self._download_lock:
                self._download_jobs.pop(job.job_id, None)
                self._message_to_job.pop((job.chat_id, job.message_id), None)

    @staticmethod
    def _estimate_media_size(message: "Message") -> int:
        for attr in ("video", "video_note", "animation", "document", "audio", "voice"):
            media_obj = getattr(message, attr, None)
            size = getattr(media_obj, "file_size", None) if media_obj else None
            if size:
                return int(size)

        photo = getattr(message, "photo", None)
        if photo and getattr(photo, "sizes", None):
            sizes = sorted(photo.sizes, key=lambda s: getattr(s, "file_size", 0) or 0)
            if sizes:
                size_val = getattr(sizes[-1], "file_size", None)
                if size_val:
                    return int(size_val)

        sticker = getattr(message, "sticker", None)
        if sticker and getattr(sticker, "file_size", None):
            return int(sticker.file_size)

        return 0

    @staticmethod
    def _message_has_downloadable_media(message: "Message") -> bool:
        media_attrs = (
            "photo",
            "video",
            "video_note",
            "animation",
            "audio",
            "voice",
            "document",
            "sticker",
        )
        for attr in media_attrs:
            if getattr(message, attr, None):
                return True
        return False


guard_module(globals())
