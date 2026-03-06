from __future__ import annotations

import json
import logging
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import ThreadPoolExecutor
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from ai import AIService
from utils.error_guard import guard_module
from utils import app_paths

from storage import Storage


log = logging.getLogger("server")


CHATS_DIR = str(app_paths.chats_dir())
SETTINGS_PATH = str(app_paths.chats_dir() / "hat_setting.json")


def _ensure_dirs() -> None:
    os.makedirs(CHATS_DIR, exist_ok=True)
    os.makedirs(str(app_paths.media_dir()), exist_ok=True)
    os.makedirs(str(app_paths.temp_dir()), exist_ok=True)


class ServerCore:
    """
    Ядро приложения: мост между Telegram (локальный адаптер), GUI и встроенным AI-сервисом.
    Совместимо с прежним форматом кадров для AI: {"user_id": "<chat_id>", "message": "..."}.
    """

    def __init__(self, service_token: Optional[str] = None):
        _ensure_dirs()

        self._storage: Optional[Storage] = None
        try:
            self._storage = Storage.open_default()
            self._storage.init_schema()
        except Exception:
            log.exception("[SERVER] Failed to initialise storage; legacy persistence will be used")
            self._storage = None

        # ===== Конфиг/Auth WS =====
        self._service_token = service_token or os.getenv("DRAGO_SERVICE_TOKEN", "dev-service-token")

        # ===== Внешние ссылки =====
        self._tg_adapter = None  # type: Optional["TelegramAdapter"]

        # ===== Очередь событий для GUI (если используется) =====
        from queue import Queue
        self.events: "Queue[Dict[str, Any]]" = Queue()

        # ===== AI сервис в рамках процесса =====
        try:
            workers_env = int(os.getenv("DRAGO_AI_WORKERS", "1") or 1)
        except ValueError:
            workers_env = 1
        workers = max(1, workers_env)
        self._ai_service = AIService(storage=self._storage)
        self._ai_executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="ai-service")
        self._ai_executor_shutdown = False


        # флаги AI/автоответа по чатам
        self._ai_flags: Dict[str, Dict[str, bool]] = self._load_flags()
        self._flags_lock = threading.Lock()

        # кэш имён пользователей (для GUI)
        self._user_names: Dict[str, str] = {}
        self._name_lock = threading.Lock()

        # Media download coordination (chat_id, message_id) -> job_id
        self._download_jobs: Dict[Tuple[str, int], str] = {}
        self._download_index: Dict[str, Tuple[str, int]] = {}
        self._download_lock = threading.Lock()
        self._tg_auth_cached: bool = False
        self._tg_auth_cache_until: float = 0.0
        self._history_timeout_warn_at: float = 0.0
        self._local_echo_lock = threading.Lock()
        self._local_echo_seq: int = 0

    # ---------------------------------------------------------------------
    #                        Telegram bridge (локальный)
    # ---------------------------------------------------------------------
    def set_telegram_adapter(self, tg: "TelegramAdapter") -> None:
        self._tg_adapter = tg
        self._tg_auth_cache_until = 0.0
        if self._storage:
            try:
                tg.set_storage(self._storage)
            except Exception:
                log.exception("[SERVER] Unable to attach storage to TelegramAdapter")

    def _is_tg_authorized(self, *, ttl: float = 4.0) -> bool:
        tg = self._tg_adapter
        if not tg:
            return False
        now = time.monotonic()
        if now < self._tg_auth_cache_until:
            return self._tg_auth_cached

        checker = getattr(tg, "is_authorized_sync", None)
        if not callable(checker):
            self._tg_auth_cached = True
        else:
            try:
                self._tg_auth_cached = bool(checker(timeout=5.0))
            except TypeError:
                self._tg_auth_cached = bool(checker())
            except Exception:
                self._tg_auth_cached = False
        self._tg_auth_cache_until = now + max(0.5, float(ttl))
        return self._tg_auth_cached

    def list_all_telegram_chats(self, limit: Optional[int] = 400, timeout: float = 20.0) -> List[Dict[str, Any]]:
        if not self._tg_adapter:
            return []
        dialogs = self._tg_adapter.list_all_chats_sync(limit=limit, timeout=timeout)
        if self._storage:
            try:
                cached = self._storage.get_dialogs_for_ui(limit=limit or 400)
                if cached:
                    return cached
            except Exception:
                log.exception("[SERVER] Failed to load dialogs from storage")
        return dialogs

    def list_cached_dialogs(self, limit: int = 400) -> List[Dict[str, Any]]:
        if not self._storage:
            return []
        try:
            return self._storage.get_dialogs_for_ui(limit=limit)
        except Exception:
            log.exception("[SERVER] Failed to load cached dialogs")
            return []

    def stream_telegram_chats(
        self,
        on_batch,
        on_done=None,
        limit: Optional[int] = 400,
        batch_size: int = 60
    ) -> None:
        if not self._tg_adapter:
            if on_done:
                try: on_done()
                except Exception: pass
            return
        self._tg_adapter.stream_dialogs(on_batch=on_batch, on_done=on_done, limit=limit, batch_size=batch_size)

    def fetch_chat_history(
        self,
        chat_id: str,
        limit: int = 80,
        download_media: bool = False,
        timeout: float = 45.0,
        *,
        include_deleted: bool = False,
    ) -> List[Dict[str, Any]]:
        remote: List[Dict[str, Any]] = []
        if self._tg_adapter and self._is_tg_authorized():
            try:
                remote = self._tg_adapter.get_history_sync(
                    chat_id=chat_id,
                    limit=limit,
                    download_media=download_media,
                    timeout=timeout,
                )
            except (TimeoutError, FuturesTimeoutError):
                remote = []
                now = time.monotonic()
                if now - self._history_timeout_warn_at >= 8.0:
                    self._history_timeout_warn_at = now
                    log.warning("[SERVER] Telegram history timeout for chat %s (timeout=%.1fs)", chat_id, float(timeout))
            except Exception as exc:
                remote = []
                err = str(exc).upper()
                if "AUTH_KEY_UNREGISTERED" in err or "SESSION_PASSWORD_NEEDED" in err:
                    self._tg_auth_cached = False
                    self._tg_auth_cache_until = time.monotonic() + 5.0
                    log.warning("[SERVER] Telegram session is not authorized for history sync (chat %s)", chat_id)
                else:
                    log.exception("[SERVER] Failed to fetch remote history for chat %s", chat_id)

        storage_ready = self._storage and str(chat_id).lstrip("-").isdigit()
        peer_id: Optional[int] = None
        cached: List[Dict[str, Any]] = []
        if storage_ready:
            try:
                peer_id = int(chat_id)
                cached = self._storage.get_messages_for_ui(peer_id, limit=limit, include_deleted=bool(include_deleted))
            except Exception:
                cached = []
                log.exception("[SERVER] Failed to load history from storage for chat %s", chat_id)

        if remote:
            if storage_ready and cached:
                try:
                    remote_ids = {int(item["id"]) for item in remote if item.get("id") is not None}
                except Exception:
                    remote_ids = set()

                cached_map: Dict[int, Dict[str, Any]] = {}
                for entry in cached:
                    try:
                        cached_map[int(entry.get("id"))] = dict(entry)
                    except Exception:
                        continue

                min_remote_id = min(remote_ids) if remote_ids else None
                missing: List[int] = []
                merged: List[Dict[str, Any]] = []
                for item in remote:
                    merged_item = dict(item)
                    try:
                        mid = int(item.get("id"))
                    except Exception:
                        mid = None
                    if mid is not None:
                        cached_entry = cached_map.get(mid)
                        if cached_entry:
                            for key in (
                                "file_path",
                                "file_size",
                                "mime",
                                "duration",
                                "waveform",
                                "forward_info",
                                "media_group_id",
                                "entities",
                                "reply_markup",
                                "reactions",
                                "poll",
                                "views",
                                "forwards",
                            ):
                                if merged_item.get(key) in (None, "", 0):
                                    merged_item[key] = cached_entry.get(key)
                    merged.append(merged_item)

                if cached_map and remote_ids:
                    for mid, cached_entry in cached_map.items():
                        if mid in remote_ids:
                            continue
                        if min_remote_id is None or mid >= min_remote_id:
                            missing.append(mid)

                if peer_id is not None and missing:
                    try:
                        self._storage.mark_messages_deleted(peer_id, missing, deleted=True)
                    except Exception:
                        log.exception("[SERVER] Failed to mark stale messages deleted for chat %s", chat_id)

                    if include_deleted:
                        missing_set = {int(mid) for mid in missing}
                        for mid in list(missing_set):
                            cached_entry = cached_map.get(mid)
                            if not cached_entry:
                                continue
                            deleted_item = dict(cached_entry)
                            deleted_item["is_deleted"] = True
                            merged.append(deleted_item)

                if include_deleted and cached_map and remote_ids:
                    # Preserve already deleted cached messages in the visible history.
                    for mid, cached_entry in cached_map.items():
                        if mid in remote_ids:
                            continue
                        if not bool(cached_entry.get("is_deleted")):
                            continue
                        merged.append(dict(cached_entry))

                merged.sort(key=lambda x: int(x.get("id", 0)), reverse=True)
                if limit:
                    merged = merged[:limit]
                return merged

            return remote

        if cached:
            return cached
        return remote

    def fetch_chat_history_cached(self, chat_id: str, limit: int = 80, *, include_deleted: bool = False) -> List[Dict[str, Any]]:
        """Return locally cached history without hitting Telegram."""
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return []
        try:
            return self._storage.get_messages_for_ui(int(chat_id), limit=limit, include_deleted=bool(include_deleted))
        except Exception:
            log.exception("[SERVER] Failed to load cached history for chat %s", chat_id)
            return []

    def download_media(self, chat_id: str, message_id: int, timeout: float = 180.0) -> Optional[str]:
        if not self._tg_adapter:
            return None
        return self._tg_adapter.download_media_sync(chat_id=chat_id, message_id=message_id, timeout=timeout)

    def download_thumb(self, chat_id: str, message_id: int, timeout: float = 20.0) -> Optional[str]:
        if not self._tg_adapter:
            return None
        return self._tg_adapter.download_thumb_sync(chat_id=chat_id, message_id=message_id, timeout=timeout)

    def get_message_details_for_ui(self, chat_id: str, message_id: int) -> Optional[Dict[str, Any]]:
        if not self._storage:
            return None
        try:
            peer_id = int(chat_id)
            mid = int(message_id)
        except Exception:
            return None
        try:
            return self._storage.get_message_by_id(peer_id, mid)
        except Exception:
            log.exception("[SERVER] Failed to load message %s/%s from storage", chat_id, message_id)
            return None

    def get_messages_details_for_ui(self, chat_id: str, message_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        if not self._storage:
            return {}
        try:
            peer_id = int(chat_id)
        except Exception:
            return {}
        try:
            return self._storage.get_messages_by_ids(peer_id, message_ids)
        except Exception:
            log.exception("[SERVER] Failed to load messages %s/%s from storage", chat_id, message_ids)
            return {}

    def get_recent_emojis(self, limit: int = 48) -> List[str]:
        if not self._tg_adapter:
            return []
        getter = getattr(self._tg_adapter, "get_recent_emojis_sync", None)
        if not callable(getter):
            return []
        try:
            return list(getter(limit=limit) or [])
        except Exception:
            return []

    def search_public_peers(self, query: str, limit: int = 24) -> List[Dict[str, Any]]:
        if not self._tg_adapter:
            return []
        getter = getattr(self._tg_adapter, "search_public_peers_sync", None)
        if not callable(getter):
            return []
        try:
            rows = list(getter(query=query, limit=limit) or [])
        except Exception:
            return []
        cleaned: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            cid = str(row.get("id") or "").strip()
            if not cid:
                continue
            cleaned.append(
                {
                    "id": cid,
                    "title": str(row.get("title") or cid),
                    "type": str(row.get("type") or "private"),
                    "username": str(row.get("username") or ""),
                    "photo_small_id": row.get("photo_small_id"),
                    "last_ts": int(row.get("last_ts") or 0),
                    "unread_count": int(row.get("unread_count") or 0),
                    "pinned": bool(row.get("pinned", False)),
                }
            )
        return cleaned

    def get_saved_gifs(self, limit: int = 32) -> List[Dict[str, Any]]:
        if not self._tg_adapter:
            return []
        getter = getattr(self._tg_adapter, "get_saved_gifs_sync", None)
        if not callable(getter):
            return []
        try:
            return list(getter(limit=limit) or [])
        except Exception:
            return []

    def get_chat_full_info(self, chat_id: str) -> Optional[Dict[str, Any]]:
        if not self._tg_adapter:
            return None
        getter = getattr(self._tg_adapter, "get_chat_full_info_sync", None)
        if not callable(getter):
            return None
        try:
            return getter(chat_id=chat_id)
        except Exception:
            log.exception("[SERVER] Failed to load chat profile for %s", chat_id)
            return None

    def get_chat_profile_sections(
        self,
        chat_id: str,
        *,
        media_limit: int = 80,
        file_limit: int = 80,
        link_limit: int = 120,
        members_limit: int = 80,
    ) -> Dict[str, Any]:
        if not str(chat_id or "").lstrip("-").isdigit():
            return {}
        peer_id = int(chat_id)
        out: Dict[str, Any] = {
            "media": [],
            "files": [],
            "links": [],
            "members": [],
        }
        if self._storage:
            try:
                out["media"] = self._storage.get_chat_shared_media(peer_id, limit=max(1, int(media_limit)))
            except Exception:
                log.exception("[SERVER] Failed to load shared media for %s", chat_id)
            try:
                out["files"] = self._storage.get_chat_shared_files(peer_id, limit=max(1, int(file_limit)))
            except Exception:
                log.exception("[SERVER] Failed to load shared files for %s", chat_id)
            try:
                out["links"] = self._storage.get_chat_links(peer_id, limit=max(1, int(link_limit)))
            except Exception:
                log.exception("[SERVER] Failed to load links for %s", chat_id)
            try:
                out["members"] = self._storage.get_chat_members_activity(peer_id, limit=max(1, int(members_limit)))
            except Exception:
                log.exception("[SERVER] Failed to load local members activity for %s", chat_id)

        tg_members: List[Dict[str, Any]] = []
        if self._tg_adapter:
            getter = getattr(self._tg_adapter, "get_chat_members_preview_sync", None)
            if callable(getter):
                try:
                    tg_members = list(getter(chat_id=chat_id, limit=max(1, int(members_limit))) or [])
                except Exception:
                    log.exception("[SERVER] Failed to load Telegram members preview for %s", chat_id)
        if tg_members:
            local_map: Dict[int, Dict[str, Any]] = {}
            for row in list(out.get("members") or []):
                try:
                    local_map[int(row.get("id"))] = dict(row)
                except Exception:
                    continue
            merged: List[Dict[str, Any]] = []
            seen: set[int] = set()
            for row in tg_members:
                try:
                    uid = int(row.get("id") or 0)
                except Exception:
                    uid = 0
                if uid <= 0:
                    continue
                seen.add(uid)
                local = local_map.get(uid, {})
                merged.append(
                    {
                        "id": uid,
                        "name": str(row.get("name") or local.get("name") or uid),
                        "username": str(row.get("username") or local.get("username") or ""),
                        "type": str(row.get("type") or local.get("type") or ""),
                        "status": str(row.get("status") or ""),
                        "messages": int(local.get("messages") or 0),
                        "last_date": int(local.get("last_date") or 0),
                        "deleted_messages": int(local.get("deleted_messages") or 0),
                    }
                )
            for uid, local in local_map.items():
                if uid in seen:
                    continue
                merged.append(dict(local))
            merged.sort(key=lambda item: int(item.get("messages") or 0), reverse=True)
            out["members"] = merged[: max(1, int(members_limit))]
        return out

    def leave_chat(self, chat_id: str) -> bool:
        if not self._tg_adapter:
            return False
        leaver = getattr(self._tg_adapter, "leave_chat_sync", None)
        if not callable(leaver):
            return False
        try:
            return bool(leaver(chat_id=chat_id))
        except Exception:
            log.exception("[SERVER] leave_chat failed for %s", chat_id)
            return False

    def get_chat_statistics(self, chat_id: str, *, limit: int = 500) -> Dict[str, Any]:
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return {}
        try:
            return self._storage.get_chat_statistics(int(chat_id), limit=limit)
        except Exception:
            log.exception("[SERVER] Failed to load chat statistics for %s", chat_id)
            return {}

    def get_message_statistics(self, chat_id: str, message_id: int) -> Dict[str, Any]:
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return {}
        try:
            return self._storage.get_message_statistics(int(chat_id), int(message_id))
        except Exception:
            log.exception("[SERVER] Failed to load message statistics for %s/%s", chat_id, message_id)
            return {}

    def start_media_download(self, chat_id: str, message_id: int) -> Optional[str]:
        if not self._tg_adapter:
            return None

        key = (str(chat_id), int(message_id))

        with self._download_lock:
            existing = self._download_jobs.get(key)
            if existing:
                return existing

        def _on_progress(job_id: str, payload: Dict[str, Any]) -> None:
            evt = dict(payload)
            evt.setdefault("chat_id", str(chat_id))
            evt.setdefault("message_id", int(message_id))
            evt.setdefault("job_id", job_id)
            evt["type"] = "gui_media_progress"
            self.events.put(evt)

            if evt.get("state") in {"completed", "error", "cancelled"}:
                with self._download_lock:
                    self._download_jobs.pop(key, None)
                    self._download_index.pop(job_id, None)

        job_id = self._tg_adapter.start_media_download(
            chat_id=str(chat_id),
            message_id=int(message_id),
            progress_cb=_on_progress,
        )
        if job_id:
            with self._download_lock:
                self._download_jobs[key] = job_id
                self._download_index[job_id] = key
        return job_id

    def pause_media_download(self, job_id: str) -> bool:
        if not self._tg_adapter:
            return False
        return self._tg_adapter.pause_media_download(job_id)

    def resume_media_download(self, job_id: str) -> bool:
        if not self._tg_adapter:
            return False
        return self._tg_adapter.resume_media_download(job_id)

    def cancel_media_download(self, job_id: str) -> bool:
        if not self._tg_adapter:
            return False
        success = self._tg_adapter.cancel_media_download(job_id)
        if success:
            with self._download_lock:
                key = self._download_index.pop(job_id, None)
                if key:
                    self._download_jobs.pop(key, None)
        return success

    def send_text_to_telegram(
        self,
        chat_id: str,
        text: str,
        reply_to: Optional[int] = None,
        *,
        entities: Optional[List[Dict[str, Any]]] = None,
    ) -> bool:
        if not self._tg_adapter:
            return False
        sender = getattr(self._tg_adapter, "send_text_sync", None)
        if not callable(sender):
            return False
        try:
            return bool(sender(chat_id=chat_id, text=text, reply_to=reply_to, entities=entities))
        except TypeError:
            return bool(sender(chat_id=chat_id, text=text, reply_to=reply_to))

    def ensure_chat_avatar(self, chat_id: str, *, file_id: Optional[str] = None, size: str = "small") -> Optional[str]:
        if not self._tg_adapter:
            return None
        return self._tg_adapter.ensure_chat_avatar_sync(chat_id=chat_id, file_id=file_id, size=size)

    def ensure_user_avatar(self, user_id: str, *, file_id: Optional[str] = None, size: str = "small") -> Optional[str]:
        if not self._tg_adapter:
            return None
        return self._tg_adapter.ensure_user_avatar_sync(user_id=user_id, file_id=file_id, size=size)

    # ---------------------------------------------------------------------
    #                              Флаги AI
    # ---------------------------------------------------------------------
    def _load_flags(self) -> Dict[str, Dict[str, bool]]:
        if os.path.isfile(SETTINGS_PATH):
            try:
                with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                    d = json.load(f)
                    if isinstance(d, dict):
                        return d
            except Exception:
                pass
        return {}

    def _save_flags(self) -> None:
        try:
            with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(self._ai_flags, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def get_ai_flags(self, chat_id: str) -> Dict[str, bool]:
        with self._flags_lock:
            flags = self._ai_flags.get(chat_id, {"ai": True, "auto": False})
            return {"ai": bool(flags.get("ai", True)), "auto": bool(flags.get("auto", False))}

    def set_ai_flags(self, chat_id: str, *, ai: Optional[bool] = None, auto: Optional[bool] = None) -> Dict[str, bool]:
        with self._flags_lock:
            flags = self._ai_flags.setdefault(chat_id, {"ai": True, "auto": False})
            if ai is not None:
                flags["ai"] = bool(ai)
                if not flags["ai"]:
                    flags["auto"] = False
            if auto is not None:
                flags["auto"] = bool(auto)
            self._ai_flags[chat_id] = flags
            self._save_flags()
            log.info("[SERVER] AI flags: chat=%s ai=%s auto=%s", chat_id, flags.get("ai"), flags.get("auto"))
            return dict(flags)

    def should_use_ai_for_gui(self, chat_id: str) -> bool:
        return self.get_ai_flags(chat_id).get("ai", True)

    def should_autoreply(self, chat_id: str) -> bool:
        flags = self.get_ai_flags(chat_id)
        return bool(flags.get("ai", True) and flags.get("auto", False))

    # ---------------------------------------------------------------------
    #                 Входящие из Telegram → GUI / AI
    # ---------------------------------------------------------------------
    def set_user_display_name(self, user_id: str, name: str) -> None:
        if not user_id or not name:
            return
        with self._name_lock:
            self._user_names[str(user_id)] = name

    def get_user_display_name(self, user_id: str) -> str:
        with self._name_lock:
            return self._user_names.get(str(user_id), "")

    def get_self_user_id(self) -> str:
        try:
            return self._tg_adapter.get_self_id_sync() or ""
        except Exception:
            return ""

    def tg_incoming_message(
        self,
        chat_id: str,
        user_id: str,
        text: str,
        date_ts: Optional[int] = None,
        *,
        message_id: Optional[int] = None,
        reply_to: Optional[int] = None,
        forward_info: Optional[Dict[str, Any]] = None,
        entities: Optional[List[Dict[str, Any]]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        sender_name: Optional[str] = None,
    ) -> None:
        ts = int(date_ts or time.time())
        payload = {
            "id": int(message_id or 0),
            "type": "text",
            "text": text,
            "sender_id": user_id,
            "sender": sender_name or self.get_user_display_name(user_id) or user_id,
            "entities": entities or None,
            "reply_markup": reply_markup or None,
            "reply_to": reply_to,
            "forward_info": forward_info,
            "file_name": None,
            "is_deleted": False,
            "ts": ts,
        }
        self.events.put({"type": "gui_message", "chat_id": chat_id, "payload": payload})
        self.events.put({"type": "gui_touch_dialog", "chat_id": chat_id, "ts": ts})

        my_id = self.get_self_user_id()
        if text.strip() and self.should_autoreply(chat_id) and str(user_id) != str(my_id or ""):
            self._schedule_ai_reply(str(chat_id), text)



    def tg_incoming_media(
        self,
        *,
        chat_id: str,
        user_id: str,
        message_id: int,
        mtype: str,
        text: str = "",
        date_ts: Optional[int] = None,
        file_path: Optional[str] = None,
        thumb_path: Optional[str] = None,
        reply_to: Optional[int] = None,
        forward_info: Optional[Dict[str, Any]] = None,
        entities: Optional[List[Dict[str, Any]]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        file_name: Optional[str] = None,
        sender_name: Optional[str] = None,
        file_size: Optional[int] = None,
        mime: Optional[str] = None,
        duration: Optional[int] = None,
        waveform: Optional[List[int]] = None,
        media_group_id: Optional[str] = None,
    ) -> None:
        ts = int(date_ts or time.time())
        self.events.put({
            "type": "gui_media",
            "chat_id": chat_id,
            "user_id": user_id,
            "id": int(message_id),
            "mtype": mtype,
            "text": text or "",
            "entities": entities or None,
            "reply_markup": reply_markup or None,
            "file_path": file_path,
            "thumb_path": thumb_path,
            "ts": ts,
            "reply_to": reply_to,
            "forward_info": forward_info,
            "file_name": file_name,
            "sender": sender_name or self.get_user_display_name(user_id) or user_id,
            "is_deleted": False,
            "file_size": file_size,
            "mime": mime,
            "duration": duration,
            "waveform": waveform,
            "media_group_id": media_group_id,
        })
        self.events.put({"type": "gui_touch_dialog", "chat_id": chat_id, "ts": ts})

    def tg_messages_deleted(self, chat_id: str, message_ids: List[int]) -> None:
        if not message_ids:
            return
        self.events.put({
            "type": "gui_messages_deleted",
            "chat_id": chat_id,
            "message_ids": [int(mid) for mid in message_ids],
        })

    # ---------------------------------------------------------------------
    #                        GUI → Отправка в Telegram/AI
    # ---------------------------------------------------------------------
    def gui_send_message(
        self,
        chat_id: str,
        user_id: str,
        text: str,
        *,
        reply_to: Optional[int] = None,
        entities: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        ts = int(time.time())
        with self._local_echo_lock:
            self._local_echo_seq += 1
            local_id = -int(self._local_echo_seq)
        payload = {
            "type": "gui_user_echo",
            "id": local_id,
            "chat_id": chat_id,
            "user_id": user_id,
            "text": text,
            "entities": entities or None,
            "local_origin": (user_id == "me"),
            "ts": ts,
        }
        if reply_to is not None:
            try:
                payload["reply_to"] = int(reply_to)
            except Exception:
                pass
        self.events.put(payload)
        self.events.put({"type": "gui_touch_dialog", "chat_id": chat_id, "ts": ts})
        # Do not block GUI thread on Telegram round-trip.
        def _send_task() -> None:
            try:
                self.send_text_to_telegram(chat_id=chat_id, text=text, reply_to=reply_to, entities=entities)
            except Exception:
                log.exception("[SERVER] Failed to send text to Telegram chat %s", chat_id)

        try:
            threading.Thread(target=_send_task, daemon=True, name="tg-send-text").start()
        except Exception:
            # Last-resort sync fallback.
            try:
                self.send_text_to_telegram(chat_id=chat_id, text=text, reply_to=reply_to, entities=entities)
            except Exception:
                log.exception("[SERVER] Failed to send text to Telegram chat %s", chat_id)
        if user_id != "me" and self.should_use_ai_for_gui(chat_id):
            self._schedule_ai_reply(str(chat_id), text)

    def delete_message(self, chat_id: str, message_id: int) -> bool:
        if not self._tg_adapter:
            return False
        result = bool(self._tg_adapter.delete_messages_sync(chat_id=chat_id, message_ids=[int(message_id)]))
        self.mark_local_deleted(chat_id, [message_id])
        return result

    def delete_messages(self, chat_id: str, message_ids: List[int]) -> bool:
        if not self._tg_adapter:
            return False
        mids: List[int] = []
        for mid in message_ids:
            try:
                mids.append(int(mid))
            except Exception:
                continue
        if not mids:
            return False
        result = bool(self._tg_adapter.delete_messages_sync(chat_id=chat_id, message_ids=mids))
        self.mark_local_deleted(chat_id, mids)
        return result

    def mark_local_deleted(self, chat_id: str, message_ids: list[int]) -> None:
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return
        mids: list[int] = []
        for mid in message_ids:
            try:
                mids.append(int(mid))
            except Exception:
                continue
        if not mids:
            return
        try:
            self._storage.mark_messages_deleted(int(chat_id), mids, deleted=True)
        except Exception:
            log.exception("[SERVER] Failed to mark messages deleted locally for %s/%s", chat_id, mids)
            return
        try:
            self.events.put(
                {
                    "type": "gui_messages_deleted",
                    "chat_id": str(chat_id),
                    "message_ids": [int(mid) for mid in mids],
                }
            )
        except Exception:
            pass

    def purge_local_messages(self, chat_id: str, message_ids: List[int]) -> bool:
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return False
        mids: List[int] = []
        for mid in message_ids:
            try:
                val = int(mid)
            except Exception:
                continue
            if val <= 0:
                continue
            mids.append(val)
        if not mids:
            return False
        try:
            self._storage.purge_messages(int(chat_id), mids)
            return True
        except Exception:
            log.exception("[SERVER] Failed to purge local messages for %s/%s", chat_id, mids)
            return False

    def edit_message(self, chat_id: str, message_id: int, text: str) -> bool:
        if not self._tg_adapter:
            return False
        return bool(self._tg_adapter.edit_message_text_sync(chat_id=chat_id, message_id=int(message_id), text=text))

    def forward_message(self, from_chat_id: str, message_id: int, to_chat_id: str) -> bool:
        if not self._tg_adapter:
            return False
        return bool(self._tg_adapter.forward_message_sync(from_chat_id=from_chat_id, message_id=int(message_id), to_chat_id=to_chat_id))

    def forward_messages(self, from_chat_id: str, message_ids: List[int], to_chat_id: str) -> bool:
        if not self._tg_adapter:
            return False
        mids: List[int] = []
        for mid in message_ids:
            try:
                mids.append(int(mid))
            except Exception:
                continue
        if not mids:
            return False
        ok_all = True
        for mid in mids:
            try:
                ok = bool(
                    self._tg_adapter.forward_message_sync(
                        from_chat_id=from_chat_id,
                        message_id=int(mid),
                        to_chat_id=to_chat_id,
                    )
                )
            except Exception:
                ok = False
            ok_all = ok_all and ok
        return ok_all

    def set_message_reaction(self, chat_id: str, message_id: int, reaction: str) -> bool:
        if not self._tg_adapter:
            return False
        sender = getattr(self._tg_adapter, "send_reaction_sync", None)
        if not callable(sender):
            return False
        try:
            return bool(sender(chat_id=chat_id, message_id=int(message_id), reaction=str(reaction or "").strip()))
        except Exception:
            return False

    def press_inline_button(self, chat_id: str, message_id: int, row: int, col: int) -> Dict[str, Any]:
        if not self._tg_adapter:
            return {"ok": False, "error": "telegram unavailable"}
        sender = getattr(self._tg_adapter, "press_inline_button_sync", None)
        if not callable(sender):
            return {"ok": False, "error": "inline buttons unsupported"}
        try:
            return dict(
                sender(
                    chat_id=str(chat_id),
                    message_id=int(message_id),
                    row=int(row),
                    col=int(col),
                )
                or {"ok": False}
            )
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    # ---------------------------------------------------------------------
    #                    AI helpers (in-process)
    # ---------------------------------------------------------------------
    def start(self, host: str = "127.0.0.1", port: int = 8765) -> None:
        """Retained for backward compatibility; AI now runs in-process."""
        log.debug("[SERVER] start() called (no websocket server; host=%s port=%s)", host, port)

    def stop(self) -> None:
        if getattr(self, "_ai_executor", None) and not self._ai_executor_shutdown:
            try:
                self._ai_executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                self._ai_executor.shutdown(wait=False)
            self._ai_executor_shutdown = True
        if self._tg_adapter and hasattr(self._tg_adapter, "set_storage"):
            try:
                self._tg_adapter.set_storage(None)
            except Exception:
                pass
        if self._storage:
            try:
                self._storage.close()
            except Exception:
                log.exception("[SERVER] Failed to close storage cleanly")

    def _schedule_ai_reply(self, chat_id: str, text: str) -> None:
        if not text or not text.strip():
            return
        if getattr(self, "_ai_executor_shutdown", False):
            return

        def _run() -> None:
            try:
                reply = self._ai_service.generate_reply(chat_id, text)
            except Exception:
                log.exception("[SERVER] AI generation failed for chat %s", chat_id)
                return
            reply_text = (reply or "").strip()
            if not reply_text:
                return
            self._handle_ai_reply(chat_id, reply_text)

        try:
            self._ai_executor.submit(_run)
        except Exception:
            log.exception("[SERVER] Unable to submit AI task for chat %s", chat_id)

    def _handle_ai_reply(self, chat_id: str, text: str) -> None:
        self.events.put({"type": "gui_ai_message", "chat_id": chat_id, "text": text})
        if self.send_text_to_telegram(chat_id=chat_id, text=text):
            self.events.put({"type": "gui_touch_dialog", "chat_id": chat_id, "ts": int(time.time())})
        else:
            log.warning("[SERVER] Failed to forward AI reply to Telegram chat %s", chat_id)

guard_module(globals())
