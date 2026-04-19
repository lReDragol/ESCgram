from __future__ import annotations

import csv
import json
import logging
import re
from collections import Counter
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import shutil
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from ai import AIService
from utils.error_guard import guard_module
from utils import app_paths
from utils.chat_stats_report import build_chat_statistics_report
from utils.chat_export import export_chat_to_files
from utils.community_scan_report import build_community_scan_report, build_community_scan_stylesheet
from utils.telegram_links import parse_telegram_reference

from storage import Storage


log = logging.getLogger("server")


CHATS_DIR = str(app_paths.chats_dir())
SETTINGS_PATH = str(app_paths.chats_dir() / "hat_setting.json")
_SCAN_SLUG_RE = re.compile(r"[^a-z0-9_]+")
_DISCOVERY_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9_]{4,}")
_DISCOVERY_STOPWORDS = {
    "chat",
    "group",
    "discussion",
    "channel",
    "forum",
    "mix",
    "чат",
    "группа",
    "канал",
    "официальный",
    "official",
}


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
        self._send_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="tg-send")
        self._send_executor_shutdown = False
        self._profile_scan_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="profile-sections")
        self._profile_scan_shutdown = False
        self._profile_scan_lock = threading.Lock()
        self._profile_scan_inflight: set[int] = set()

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

    @staticmethod
    def _slugify_scan_key(value: str) -> str:
        raw = str(value or "").strip().lower()
        ref = parse_telegram_reference(raw)
        base = str(ref.username or ref.invite or raw or "scan")
        base = _SCAN_SLUG_RE.sub("_", base).strip("_")
        return base or "scan"

    @staticmethod
    def _iter_telegram_refs(*values: Any) -> List[str]:
        found: List[str] = []
        seen: set[str] = set()
        for value in values:
            text = str(value or "")
            if not text:
                continue
            direct_ref = parse_telegram_reference(text)
            if direct_ref.is_username or direct_ref.is_invite:
                canonical = str(direct_ref.canonical or direct_ref.raw or "").strip()
                if canonical and canonical not in seen:
                    seen.add(canonical)
                    found.append(canonical)
            for token in re.findall(r"(?:https?://t\.me/[^\s]+|t\.me/[^\s]+|@[A-Za-z0-9_]{3,})", text):
                ref = parse_telegram_reference(token)
                canonical = str(ref.canonical or ref.raw or "").strip()
                if not canonical or canonical in seen or not (ref.is_username or ref.is_invite):
                    continue
                seen.add(canonical)
                found.append(canonical)
        return found

    @staticmethod
    def _normalize_scan_identity(value: Any) -> str:
        return re.sub(r"[^a-zа-яё0-9]+", "", str(value or "").strip().casefold())

    @classmethod
    def _is_exact_scan_query_match(cls, row: Optional[Dict[str, Any]], raw_query: str) -> bool:
        if not isinstance(row, dict):
            return False
        ref = parse_telegram_reference(raw_query)
        username = str(row.get("username") or "").strip().casefold()
        title = str(row.get("title") or "").strip().casefold()
        if ref.username and username == ref.username.casefold():
            return True
        q_norm = cls._normalize_scan_identity(ref.username or raw_query)
        if not q_norm:
            return False
        if cls._normalize_scan_identity(username) == q_norm:
            return True
        if cls._normalize_scan_identity(title) == q_norm:
            return True
        return False

    @staticmethod
    def _copy_asset_file(source_path: str, dest_dir: Path, *, prefix: str) -> str:
        src = Path(str(source_path or "")).expanduser()
        if not src.is_file():
            return ""
        dest_dir.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", src.name)
        target = dest_dir / f"{prefix}_{safe_name}"
        if not target.exists():
            shutil.copy2(src, target)
        return str(target.name)

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
        dialogs: List[Dict[str, Any]] = []
        if self._tg_adapter:
            try:
                dialogs = list(self._tg_adapter.list_all_chats_sync(limit=limit, timeout=timeout) or [])
            except Exception:
                dialogs = []
        cached: List[Dict[str, Any]] = []
        if self._storage:
            try:
                cached = self._storage.get_dialogs_for_ui(limit=limit or 400)
            except Exception:
                log.exception("[SERVER] Failed to load dialogs from storage")
                cached = []
        if dialogs:
            cached_by_id: Dict[str, Dict[str, Any]] = {}
            for row in cached:
                if not isinstance(row, dict):
                    continue
                cid = str(row.get("id") or "").strip()
                if cid:
                    cached_by_id[cid] = dict(row)
            merged: List[Dict[str, Any]] = []
            for row in dialogs:
                if not isinstance(row, dict):
                    continue
                cid = str(row.get("id") or "").strip()
                if not cid:
                    continue
                prev = dict(cached_by_id.get(cid, {}))
                prev.update({k: v for k, v in row.items() if v not in (None, "")})
                merged.append(prev)
            return merged
        return cached

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
                remote_only_fetched_top = len(remote) < limit
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

                if cached_map and remote_ids and not remote_only_fetched_top:
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
                    "members_count": row.get("members_count"),
                    "last_ts": int(row.get("last_ts") or 0),
                    "unread_count": int(row.get("unread_count") or 0),
                    "pinned": bool(row.get("pinned", False)),
                }
            )
        return cleaned

    def resolve_chat_reference(self, query: str, *, join: bool = False) -> Optional[Dict[str, Any]]:
        if not self._tg_adapter:
            return None
        getter = getattr(self._tg_adapter, "resolve_chat_reference_sync", None)
        if not callable(getter):
            return None
        try:
            row = getter(query, join=join) or {}
        except Exception:
            return None
        if not isinstance(row, dict):
            return None
        cid = str(row.get("id") or "").strip()
        if not cid:
            return None
        return {
            "id": cid,
            "title": str(row.get("title") or cid),
            "type": str(row.get("type") or "private"),
            "username": str(row.get("username") or ""),
            "photo_small_id": row.get("photo_small_id") or row.get("photo_small"),
            "members_count": row.get("members_count"),
            "last_ts": int(row.get("last_ts") or 0),
            "unread_count": int(row.get("unread_count") or 0),
            "pinned": bool(row.get("pinned", False)),
        }

    @staticmethod
    def _build_discovery_queries(raw_query: str, root_row: Optional[Dict[str, Any]]) -> List[str]:
        seen: set[str] = set()
        out: List[str] = []

        def _add(value: Any) -> None:
            text = str(value or "").strip()
            key = text.casefold()
            if not text or key in seen:
                return
            seen.add(key)
            out.append(text)

        ref = parse_telegram_reference(raw_query)
        if ref.username:
            _add(ref.username)
            _add(ref.username.upper())
            if "_" not in ref.username and len(ref.username) >= 8:
                mid = max(3, len(ref.username) // 2)
                _add(ref.username[:mid] + "_" + ref.username[mid:])
        title = str((root_row or {}).get("title") or "").strip()
        for token in _DISCOVERY_TOKEN_RE.findall(title):
            lowered = token.casefold()
            if lowered in _DISCOVERY_STOPWORDS:
                continue
            _add(token)
        if title:
            compact = " ".join(
                token
                for token in _DISCOVERY_TOKEN_RE.findall(title)
                if token.casefold() not in _DISCOVERY_STOPWORDS
            ).strip()
            if compact:
                _add(compact)
        return out[:6]

    @staticmethod
    def _row_matches_root_query(row: Dict[str, Any], root_row: Dict[str, Any], raw_query: str, full_info: Optional[Dict[str, Any]]) -> bool:
        ref = parse_telegram_reference(raw_query)
        query_terms = [str(raw_query or "").strip().casefold()]
        if ref.username:
            query_terms.extend(
                {
                    ref.username.casefold(),
                    f"@{ref.username.casefold()}",
                    f"t.me/{ref.username.casefold()}",
                    f"https://t.me/{ref.username.casefold()}",
                }
            )
        about = str((full_info or {}).get("about") or "").casefold()
        username = str(row.get("username") or "").casefold()
        title = str(row.get("title") or "").casefold()
        if any(term and term in about for term in query_terms):
            return True
        if ref.username and username == ref.username.casefold():
            return True
        root_tokens = [
            token.casefold()
            for token in _DISCOVERY_TOKEN_RE.findall(str(root_row.get("title") or ""))
            if token.casefold() not in _DISCOVERY_STOPWORDS
        ]
        if not root_tokens:
            return False
        overlap = sum(1 for token in root_tokens if token and token in title)
        required_overlap = 1 if len(root_tokens) == 1 else 2
        return overlap >= required_overlap

    @staticmethod
    def _score_scan_root_candidate(row: Dict[str, Any], raw_query: str) -> int:
        ref = parse_telegram_reference(raw_query)
        username = str(row.get("username") or "").strip().casefold()
        title = str(row.get("title") or "").strip().casefold()
        q_norm = re.sub(r"[^a-zа-яё0-9]+", "", str(ref.username or raw_query or "").casefold())
        username_norm = re.sub(r"[^a-zа-яё0-9]+", "", username)
        title_norm = re.sub(r"[^a-zа-яё0-9]+", "", title)
        try:
            members_count = int(row.get("members_count") or 0)
        except Exception:
            members_count = 0
        score = 0
        if ref.username and username == ref.username.casefold():
            score += 120
        if q_norm and username_norm == q_norm:
            score += 260
        elif q_norm and username_norm.startswith(q_norm):
            score += 120
        elif q_norm and q_norm in username_norm:
            score += 70
        if q_norm and q_norm in title_norm:
            score += 50
        if bool(row.get("accessible")):
            score += 140
        if str(row.get("type") or "").strip().lower() == "channel":
            score += 35
        if str(row.get("source") or "").startswith("discovery:"):
            score += 20
        if bool(row.get("is_root")):
            score += 10
        if members_count > 0:
            score += min(500, members_count // 250)
        return int(score)

    def get_community_scan(self, scan_key: str) -> Dict[str, Any]:
        if not self._storage:
            return {}
        try:
            return self._storage.get_community_scan_run(scan_key)
        except Exception:
            return {}

    def scan_selected_community(
        self,
        query: str,
        *,
        max_related: int = 28,
        max_depth: int = 2,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        if not self._storage:
            return {"ok": False, "error": "Хранилище недоступно."}
        if not self._tg_adapter or not self._is_tg_authorized():
            return {"ok": False, "error": "Telegram не авторизован."}
        raw_query = str(query or "").strip()
        if not raw_query:
            return {"ok": False, "error": "Укажите ссылку или username сообщества."}
        query_ref = parse_telegram_reference(raw_query)

        scan_key = self._slugify_scan_key(raw_query)
        scan_dir = app_paths.scans_dir() / scan_key
        scan_dir.mkdir(parents=True, exist_ok=True)
        started_at = int(time.time())
        try:
            target_history_messages = max(
                120,
                int(os.getenv("DRAGO_COMMUNITY_SCAN_TARGET_MESSAGES", "1200") or 1200),
            )
        except Exception:
            target_history_messages = 1200
        try:
            max_backfill_messages = max(
                target_history_messages,
                int(os.getenv("DRAGO_COMMUNITY_SCAN_MAX_BACKFILL", "2500") or 2500),
            )
        except Exception:
            max_backfill_messages = max(target_history_messages, 2500)
        try:
            history_scan_timeout = float(os.getenv("DRAGO_HISTORY_SCAN_TIMEOUT", "900") or 900.0)
        except Exception:
            history_scan_timeout = 900.0

        def _progress(done: int, total: int, text: str) -> None:
            if callable(progress_callback):
                try:
                    progress_callback(int(done), int(total), str(text or ""))
                except Exception:
                    pass

        tg = self._tg_adapter
        search_cache: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
        resolve_cache: Dict[Tuple[str, bool], Optional[Dict[str, Any]]] = {}
        full_info_cache: Dict[str, Dict[str, Any]] = {}

        def _search_cached(text: str, *, limit: int) -> List[Dict[str, Any]]:
            key = (str(text or "").strip(), int(limit))
            if key not in search_cache:
                search_cache[key] = list(self.search_public_peers(key[0], limit=key[1]) or [])
            return [dict(row) for row in list(search_cache.get(key) or []) if isinstance(row, dict)]

        def _resolve_cached(text: str, *, join: bool) -> Optional[Dict[str, Any]]:
            key = (str(text or "").strip(), bool(join))
            if key not in resolve_cache:
                resolved = self.resolve_chat_reference(key[0], join=key[1]) or {}
                resolve_cache[key] = dict(resolved) if isinstance(resolved, dict) else None
            cached = resolve_cache.get(key)
            return dict(cached) if isinstance(cached, dict) else None

        def _full_info_cached(chat_id: str) -> Dict[str, Any]:
            cid = str(chat_id or "").strip()
            if not cid:
                return {}
            if cid not in full_info_cache:
                raw = self.get_chat_full_info(cid) or {}
                full_info_cache[cid] = dict(raw) if isinstance(raw, dict) else {}
            return dict(full_info_cache.get(cid) or {})

        notes: List[str] = []
        search_rows = _search_cached(raw_query, limit=max(6, int(max_related) * 2))
        resolved_root = _resolve_cached(raw_query, join=False)
        if not resolved_root:
            resolved_root = _resolve_cached(raw_query, join=True)
        strict_search_rows = [
            dict(row)
            for row in search_rows
            if self._is_exact_scan_query_match(row, raw_query)
        ]
        if not resolved_root and strict_search_rows and (query_ref.is_username or query_ref.is_invite):
            resolved_root = dict(
                max(strict_search_rows, key=lambda row: self._score_scan_root_candidate(row, raw_query))
            )
        elif not resolved_root and search_rows and (query_ref.is_username or query_ref.is_invite):
            username = str(query_ref.username or "").strip().lower()
            if username:
                for row in search_rows:
                    if str(row.get("username") or "").strip().lower() == username:
                        resolved_root = dict(row)
                        break
            if not resolved_root:
                notes.append(
                    f"Точное совпадение для {raw_query} не найдено; похожие результаты не используются как корень скана."
                )
        elif not resolved_root and search_rows:
            resolved_root = dict(search_rows[0])

        if not resolved_root:
            lookup_status: Dict[str, Any] = {}
            getter = getattr(tg, "get_public_lookup_status", None)
            if callable(getter):
                try:
                    lookup_status = dict(getter() or {})
                except Exception:
                    lookup_status = {}

            cached_payload = self.get_community_scan(scan_key)
            cached_chats = [
                dict(item)
                for item in list((cached_payload or {}).get("chats") or [])
                if isinstance(item, dict)
            ]
            if cached_chats:
                payload = dict(cached_payload)
                cached_notes = [str(item) for item in list(payload.get("notes") or []) if str(item or "").strip()]
                fallback_note = "Свежий scan не выполнен: использован локально сохранённый результат."
                if bool(lookup_status.get("blocked")):
                    fallback_note = (
                        "Свежий scan не выполнен: Telegram временно блокирует public lookup API, "
                        f"использован локально сохранённый результат ({int(lookup_status.get('remaining_seconds') or 0)}s remaining)."
                    )
                if fallback_note not in cached_notes:
                    cached_notes.append(fallback_note)
                payload["notes"] = cached_notes
                payload["ok"] = True
                payload["used_cached_scan"] = True
                payload["lookup_status"] = lookup_status
                return payload

            error = f"Не удалось разрешить ссылку или username сообщества: {raw_query}."
            if bool(lookup_status.get("blocked")):
                remaining = int(lookup_status.get("remaining_seconds") or 0)
                reason = str(lookup_status.get("reason") or "").strip()
                error += (
                    " Telegram временно заблокировал public lookup API "
                    f"примерно на {remaining} секунд."
                )
                if reason:
                    error += f" Причина: {reason}."
            else:
                error += " Текущий аккаунт не видит этот чат и локального cached scan для него нет."
            return {
                "ok": False,
                "error": error,
                "scan_key": scan_key,
                "query": raw_query,
                "lookup_status": lookup_status,
            }

        candidates: Dict[str, Dict[str, Any]] = {}
        edges: List[Dict[str, Any]] = []
        unresolved_refs: List[str] = []
        temporary_joins: set[str] = set()
        processed: set[str] = set()
        root_channel_id = str((resolved_root or {}).get("id") or "").strip()
        analysis_root = dict(resolved_root or {})
        if resolved_root:
            root_full_info = _full_info_cached(root_channel_id)
            linked_row = dict(root_full_info.get("linked_chat") or {})
            linked_id = str(root_full_info.get("linked_chat_id") or linked_row.get("id") or "").strip()
            linked_type = str(linked_row.get("type") or "").strip().lower()
            if linked_id and linked_type in {"group", "supergroup"}:
                analysis_root = dict(linked_row)
                notes.append(
                    f"Основной анализ переведён на привязанную discussion-группу {str(linked_row.get('title') or linked_id)}."
                )
        root_candidate_id = str((analysis_root or {}).get("id") or root_channel_id or "").strip()

        def _remember_edge(source_chat_id: Optional[str], target_chat_id: Optional[str], label: str) -> None:
            src = str(source_chat_id or "").strip()
            dst = str(target_chat_id or "").strip()
            if not src or not dst or src == dst:
                return
            edge = {"source": src, "target": dst, "label": str(label or "").strip()}
            if edge not in edges:
                edges.append(edge)

        def _add_candidate(row: Optional[Dict[str, Any]], *, source: str, depth: int, is_root: bool = False) -> None:
            if not isinstance(row, dict):
                return
            cid = str(row.get("id") or "").strip()
            if not cid:
                return
            row_type = str(row.get("type") or "").strip().lower()
            if row_type == "private" and not is_root:
                return
            prev = dict(candidates.get(cid, {}))
            merged = dict(prev)
            merged.update({key: value for key, value in row.items() if value not in (None, "")})
            merged["chat_id"] = cid
            merged["source"] = source if not prev.get("source") else prev.get("source")
            merged["depth"] = min(int(prev.get("depth") or depth), int(depth))
            merged["is_root"] = bool(prev.get("is_root")) or bool(is_root)
            merged.setdefault("joined_temporarily", False)
            merged.setdefault("accessible", False)
            merged.setdefault("new_messages", 0)
            candidates[cid] = merged

        if resolved_root:
            _add_candidate(
                resolved_root,
                source="root",
                depth=0,
                is_root=bool(root_channel_id and root_channel_id == root_candidate_id),
            )
        if analysis_root and str(analysis_root.get("id") or "").strip() and str(analysis_root.get("id") or "").strip() != root_channel_id:
            analysis_root_id = str(analysis_root.get("id") or "").strip()
            _add_candidate(analysis_root, source="linked-discussion", depth=0, is_root=True)
            _remember_edge(root_channel_id, analysis_root_id, "linked-discussion")
        for row in search_rows:
            row_id = str(row.get("id") or "").strip()
            _add_candidate(row, source="search", depth=0, is_root=bool(root_candidate_id and row_id == root_candidate_id))

        if resolved_root:
            for discover_query in self._build_discovery_queries(raw_query, resolved_root):
                for related_row in _search_cached(discover_query, limit=max(8, int(max_related))):
                    if not isinstance(related_row, dict):
                        continue
                    related_id = str(related_row.get("id") or "").strip()
                    if not related_id or related_id == root_candidate_id:
                        continue
                    full_info = _full_info_cached(related_id)
                    if not self._row_matches_root_query(related_row, resolved_root, raw_query, full_info):
                        continue
                    enriched = dict(related_row)
                    enriched.update({key: value for key, value in full_info.items() if value not in (None, "")})
                    _add_candidate(enriched, source=f"discovery:{discover_query}", depth=1, is_root=False)
                    _remember_edge(root_candidate_id, related_id, "root-related")

        queue = sorted(candidates.keys(), key=lambda cid: 0 if bool(candidates[cid].get("is_root")) else 1)
        while queue and len(processed) < max(1, int(max_related)):
            chat_id = queue.pop(0)
            if chat_id in processed:
                continue
            processed.add(chat_id)
            row = candidates.get(chat_id, {})
            title = str(row.get("title") or chat_id)
            username = str(row.get("username") or "").strip()
            row_type = str(row.get("type") or "").strip().lower()
            can_probe_remote = row_type in {"group", "supergroup", "channel"}
            probe = []
            if can_probe_remote:
                try:
                    probe = list(getattr(tg, "get_history_sync")(chat_id, limit=3, timeout=20.0) or [])
                except Exception:
                    probe = []
            accessible = bool(probe)
            if can_probe_remote and not accessible and username:
                before_join = _resolve_cached(username, join=False)
                after_join = _resolve_cached(username, join=True)
                if not before_join and after_join and str(after_join.get("id") or "") == chat_id:
                    temporary_joins.add(chat_id)
                    row["joined_temporarily"] = True
                if after_join:
                    row.update({key: value for key, value in after_join.items() if value not in (None, "")})
                try:
                    probe = list(getattr(tg, "get_history_sync")(chat_id, limit=3, timeout=20.0) or [])
                except Exception:
                    probe = []
                accessible = bool(probe)
            row["accessible"] = accessible

            full_info = _full_info_cached(chat_id) if can_probe_remote else {}
            if full_info:
                row.update({key: value for key, value in full_info.items() if value not in (None, "")})
                title = str(row.get("title") or title)
            row["title"] = title
            row["probe_messages"] = len(probe)
            row["last_probe_at"] = int(time.time())

            _progress(len(processed), max(1, min(len(candidates), int(max_related))), f"Проверяю {title}")

            related_refs = self._iter_telegram_refs(
                full_info.get("about"),
                full_info.get("username"),
                full_info.get("invite_link"),
            )
            linked_chat_id = str(full_info.get("linked_chat_id") or "").strip()
            linked_chat_row = dict(full_info.get("linked_chat") or {})
            if linked_chat_id and linked_chat_id != chat_id and int(row.get("depth") or 0) < int(max_depth):
                if not linked_chat_row:
                    linked_chat_row = _resolve_cached(linked_chat_id, join=False) or _full_info_cached(linked_chat_id) or {}
                if linked_chat_row:
                    _add_candidate(linked_chat_row, source=f"linked:{chat_id}", depth=int(row.get("depth") or 0) + 1)
                    _remember_edge(chat_id, linked_chat_id, "linked-discussion")
                    if linked_chat_id not in processed and linked_chat_id not in queue:
                        queue.insert(0, linked_chat_id)
            if accessible:
                try:
                    new_messages = int(
                        getattr(tg, "scan_history_to_storage_sync")(
                            chat_id,
                            limit=0,
                            chunk_size=250,
                            timeout=history_scan_timeout,
                            incremental=True,
                        )
                        or 0
                    )
                except Exception:
                    new_messages = 0
                stored_messages = 0
                try:
                    peer_id = int(chat_id)
                except Exception:
                    peer_id = 0
                try:
                    stored_messages = int(self._storage.get_chat_message_count(peer_id) or 0) if peer_id else 0
                except Exception:
                    stored_messages = 0
                if stored_messages < target_history_messages:
                    backfill_limit = max(
                        target_history_messages,
                        min(max_backfill_messages, max(target_history_messages, stored_messages * 2)),
                    )
                    _progress(
                        len(processed),
                        max(1, min(len(candidates), int(max_related))),
                        f"Добираю историю {title} ({stored_messages}/{backfill_limit})",
                    )
                    try:
                        new_messages += int(
                            getattr(tg, "scan_history_to_storage_sync")(
                                chat_id,
                                limit=backfill_limit,
                                chunk_size=250,
                                timeout=history_scan_timeout,
                                incremental=False,
                            )
                            or 0
                        )
                    except Exception:
                        pass
                    try:
                        stored_messages = int(self._storage.get_chat_message_count(peer_id) or 0) if peer_id else 0
                    except Exception:
                        stored_messages = stored_messages
                row["new_messages"] = new_messages
                row["stored_messages"] = int(stored_messages)
                row["history_target_messages"] = int(target_history_messages)
                row["last_scanned_at"] = int(time.time())
                try:
                    for link_row in list(self._storage.get_chat_links(int(chat_id), limit=160) or []):
                        if not isinstance(link_row, dict):
                            continue
                        related_refs.extend(
                            self._iter_telegram_refs(
                                link_row.get("url"),
                                link_row.get("raw"),
                                link_row.get("context"),
                                link_row.get("text"),
                                link_row.get("message"),
                            )
                        )
                except Exception:
                    pass
            elif row.get("is_root"):
                notes.append(f"Корневой чат {title} найден, но его история недоступна текущему аккаунту.")

            if int(row.get("depth") or 0) < int(max_depth):
                for ref_value in related_refs:
                    related = _resolve_cached(ref_value, join=False) or {}
                    if not related:
                        related_rows = _search_cached(ref_value, limit=6)
                        if related_rows:
                            related = dict(related_rows[0])
                    if not related:
                        parsed_ref = parse_telegram_reference(ref_value)
                        lookup_blocked = getattr(tg, "_public_lookups_blocked", None)
                        is_lookup_blocked = False
                        if callable(lookup_blocked):
                            try:
                                is_lookup_blocked = bool(lookup_blocked())
                            except Exception:
                                is_lookup_blocked = False
                        if is_lookup_blocked and parsed_ref.is_username:
                            canonical = str(parsed_ref.canonical or ref_value or "").strip()
                            if canonical and canonical not in unresolved_refs:
                                unresolved_refs.append(canonical)
                        continue
                    target_id = str(related.get("id") or "").strip()
                    if not target_id or target_id == chat_id:
                        continue
                    _add_candidate(related, source=f"link:{chat_id}", depth=int(row.get("depth") or 0) + 1)
                    _remember_edge(chat_id, target_id, "telegram-link")
                    if target_id not in processed and target_id not in queue and len(candidates) <= int(max_related) * 2:
                        queue.append(target_id)

            candidates[chat_id] = row

        leave_chat = getattr(tg, "leave_chat_sync", None)
        if callable(leave_chat):
            for joined_id in list(temporary_joins):
                try:
                    leave_chat(joined_id, timeout=20.0)
                except Exception:
                    notes.append(f"Не удалось автоматически выйти из {joined_id}.")

        primary_root = max(
            list(candidates.values()),
            key=lambda item: self._score_scan_root_candidate(item, raw_query),
            default=None,
        )
        primary_root_id = str((primary_root or {}).get("chat_id") or (primary_root or {}).get("id") or "").strip()
        if primary_root_id:
            if root_candidate_id and primary_root_id != root_candidate_id:
                notes.append(
                    f"Основной узел сообщества переназначен на {str((primary_root or {}).get('title') or primary_root_id)}."
                )
                _remember_edge(root_candidate_id, primary_root_id, "community-primary")
            for item in candidates.values():
                item["is_root"] = str(item.get("chat_id") or item.get("id") or "").strip() == primary_root_id

        visible_chats = [
            dict(item)
            for item in list(candidates.values())
            if bool(item.get("is_root"))
            or str(item.get("type") or "").strip().lower() in {"channel", "group", "supergroup"}
        ]
        chats = sorted(
            visible_chats,
            key=lambda item: (not bool(item.get("is_root")), not bool(item.get("accessible")), str(item.get("title") or "")),
        )[: max(1, int(max_related))]
        accessible_chats = [row for row in chats if bool(row.get("accessible"))]
        total_new_messages = sum(int(row.get("new_messages") or 0) for row in accessible_chats)
        root_row = next((row for row in chats if bool(row.get("is_root"))), None)
        if root_row and len(accessible_chats) <= 1:
            try:
                stored_messages = int(root_row.get("stored_messages") or 0)
            except Exception:
                stored_messages = 0
            if stored_messages and stored_messages < int(target_history_messages):
                notes.append(
                    f"Покрытие выгрузки ограничено корневым каналом: доступно {stored_messages} сообщений, discussion-группа или связанные ветки не были получены этим аккаунтом."
                )
        if unresolved_refs:
            notes.append(
                "Не удалось разрешить часть Telegram-ссылок сообщества из-за текущего FLOOD_WAIT на public lookup API: "
                + ", ".join(unresolved_refs[:6])
            )

        payload: Dict[str, Any] = {
            "ok": True,
            "scan_key": scan_key,
            "query": raw_query,
            "title": str((root_row or {}).get("title") or raw_query),
            "root_chat_id": int(str((root_row or {}).get("chat_id") or "0") or 0) if root_row else None,
            "started_at": started_at,
            "completed_at": int(time.time()),
            "notes": notes,
            "unresolved_refs": unresolved_refs,
            "chats": chats,
            "edges": edges,
            "temporary_joins": sorted(list(temporary_joins)),
            "summary": {
                "total_chats": len(chats),
                "accessible_chats": len(accessible_chats),
                "new_messages": int(total_new_messages),
                "unresolved_refs": len(unresolved_refs),
                "root_title": str((root_row or {}).get("title") or raw_query),
                "root_access": "ok" if bool((root_row or {}).get("accessible")) else "forbidden",
            },
        }
        self._storage.save_community_scan_run(
            scan_key,
            query=raw_query,
            payload=payload,
            title=str(payload.get("title") or raw_query),
            root_chat_id=payload.get("root_chat_id"),
            updated_at=int(payload.get("completed_at") or time.time()),
        )
        manifest_path = scan_dir / "scan_manifest.json"
        manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        payload["manifest_path"] = str(manifest_path)
        return payload

    def export_community_scan(self, scan_key: str, *, output_dir: str) -> Dict[str, Any]:
        if not self._storage:
            return {"ok": False, "error": "Хранилище недоступно."}
        payload = self.get_community_scan(scan_key)
        if not payload:
            return {"ok": False, "error": "Скан сообщества не найден."}

        chats = [dict(item) for item in list(payload.get("chats") or []) if isinstance(item, dict)]
        peer_ids: List[int] = []
        for row in chats:
            try:
                peer_ids.append(int(row.get("chat_id") or row.get("id") or 0))
            except Exception:
                continue
        peer_ids = [pid for pid in peer_ids if pid != 0]
        if not peer_ids:
            return {"ok": False, "error": "В скане нет доступных чатов для выгрузки."}

        out_dir = Path(str(output_dir or "")).expanduser()
        out_dir.mkdir(parents=True, exist_ok=True)
        base_name = self._slugify_scan_key(str(payload.get("query") or scan_key))
        html_path = out_dir / f"{base_name}.html"
        css_path = out_dir / f"{base_name}.css"
        sql_path = out_dir / f"{base_name}.sqlite"
        assets_dir = out_dir / f"{base_name}.assets"
        avatars_dir = assets_dir / "avatars"
        avatars_dir.mkdir(parents=True, exist_ok=True)

        chat_reports: List[Dict[str, Any]] = []
        aggregated_users: Dict[int, Dict[str, Any]] = {}
        total_messages = 0
        risk_sum = 0
        avatar_cache: Dict[Tuple[str, str], str] = {}
        community_link_counter: Counter[str] = Counter()
        community_link_context: Dict[str, Dict[str, Any]] = {}
        community_post_anomalies: List[Dict[str, Any]] = []
        user_reason_counter: Counter[str] = Counter()

        for chat in chats:
            chat_id = str(chat.get("chat_id") or chat.get("id") or "").strip()
            if not chat_id or not chat_id.lstrip("-").isdigit():
                continue
            try:
                peer_id = int(chat_id)
            except Exception:
                continue
            stats = self._storage.get_chat_statistics(
                peer_id,
                limit=0,
                sender_limit=0,
                daily_limit=0,
                include_sender_daily=True,
            )
            stats = self._attach_chat_statistics_snapshots(peer_id, stats)
            total_messages += int(stats.get("total_messages") or 0)
            risk_sum += int((stats.get("risk") or {}).get("score") or 0) if isinstance(stats.get("risk"), dict) else 0
            chat_avatar = ""
            peer_meta = self._storage.get_peer(peer_id)
            try:
                avatar_key = ("chat", str(peer_id))
                avatar_src = avatar_cache.get(avatar_key, "")
                if not avatar_src:
                    chat_photo_id = str(chat.get("photo_small_id") or peer_meta.get("photo_small") or "").strip()
                    avatar_src = (
                        str(self.get_cached_chat_avatar(chat_id, file_id=chat_photo_id, size="small") or "")
                        if chat_photo_id
                        else ""
                    )
                    if avatar_src:
                        avatar_cache[avatar_key] = avatar_src
            except Exception:
                avatar_src = ""
            if avatar_src:
                copied = self._copy_asset_file(str(avatar_src), avatars_dir, prefix=f"chat_{abs(peer_id)}")
                if copied:
                    chat_avatar = f"{assets_dir.name}/avatars/{copied}"

            chat_links = list(self._storage.get_chat_links(peer_id, limit=80) or [])
            link_counts: Counter[str] = Counter()
            for link_row in chat_links:
                if not isinstance(link_row, dict):
                    continue
                url = str(link_row.get("url") or "").strip()
                if not url:
                    continue
                link_counts[url] += 1
                community_link_counter[url] += 1
                community_link_context.setdefault(
                    url,
                    {
                        "url": url,
                        "chat_titles": set(),
                        "chat_usernames": set(),
                        "sample_context": str(link_row.get("context") or "")[:220],
                    },
                )
                community_link_context[url]["chat_titles"].add(str(chat.get("title") or chat_id))
                username_value = str(chat.get("username") or "").strip()
                if username_value:
                    community_link_context[url]["chat_usernames"].add(username_value)

            top_links = [
                {"url": url, "count": int(count)}
                for url, count in link_counts.most_common(16)
            ]

            for anomaly in list((stats.get("post_performance") or {}).get("anomalies") or []):
                if not isinstance(anomaly, dict):
                    continue
                enriched_anomaly = dict(anomaly)
                enriched_anomaly["chat_id"] = peer_id
                enriched_anomaly["chat_title"] = str(chat.get("title") or chat_id)
                enriched_anomaly["chat_username"] = str(chat.get("username") or "")
                community_post_anomalies.append(enriched_anomaly)

            for sender in list(stats.get("sender_details") or []):
                if not isinstance(sender, dict):
                    continue
                try:
                    sender_id = int(sender.get("sender_id") or 0)
                except Exception:
                    sender_id = 0
                if sender_id <= 0:
                    continue
                target = aggregated_users.setdefault(
                    sender_id,
                    {
                        "sender_id": sender_id,
                        "name": str(sender.get("name") or sender_id),
                        "username": str(sender.get("username") or ""),
                        "type": str(sender.get("type") or ""),
                        "count": 0,
                        "chat_count": 0,
                        "media_messages": 0,
                        "deleted_messages": 0,
                        "total_reactions": 0,
                        "last_date": 0,
                        "risk_score": 0,
                        "risk_flags": [],
                        "avatar": "",
                        "photo_small_id": str(sender.get("photo_small_id") or ""),
                    },
                )
                target["count"] = int(target.get("count") or 0) + int(sender.get("count") or 0)
                target["chat_count"] = int(target.get("chat_count") or 0) + 1
                target["media_messages"] = int(target.get("media_messages") or 0) + int(sender.get("media_messages") or 0)
                target["deleted_messages"] = int(target.get("deleted_messages") or 0) + int(sender.get("deleted_messages") or 0)
                target["total_reactions"] = int(target.get("total_reactions") or 0) + int(sender.get("total_reactions") or 0)
                target["last_date"] = max(int(target.get("last_date") or 0), int(sender.get("last_date") or 0))
                target["risk_score"] = max(int(target.get("risk_score") or 0), int(sender.get("risk_score") or 0))
                flags = set(str(item) for item in list(target.get("risk_flags") or []) if str(item).strip())
                flags.update(str(item) for item in list(sender.get("risk_flags") or []) if str(item).strip())
                target["risk_flags"] = sorted(flags)
                target["chat_titles"] = sorted(set(list(target.get("chat_titles") or [])) | {str(chat.get("title") or chat_id)})
                target["chat_usernames"] = sorted(
                    set(list(target.get("chat_usernames") or []))
                    | ({str(chat.get("username") or "").strip()} if str(chat.get("username") or "").strip() else set())
                )
                for flag in list(sender.get("risk_flags") or []):
                    if str(flag).strip():
                        user_reason_counter[str(flag)] += 1

                if not target.get("avatar"):
                    avatar_src = ""
                    try:
                        avatar_key = ("user", str(sender_id))
                        avatar_src = avatar_cache.get(avatar_key, "")
                        if not avatar_src:
                            user_photo_id = str(sender.get("photo_small_id") or "").strip()
                            avatar_src = (
                                str(self.get_cached_user_avatar(str(sender_id), file_id=user_photo_id, size="small") or "")
                                if user_photo_id
                                else ""
                            )
                            if avatar_src:
                                avatar_cache[avatar_key] = avatar_src
                    except Exception:
                        avatar_src = ""
                    if avatar_src:
                        copied = self._copy_asset_file(str(avatar_src), avatars_dir, prefix=f"user_{sender_id}")
                        if copied:
                            target["avatar"] = f"{assets_dir.name}/avatars/{copied}"

            chat_reports.append(
                {
                    **chat,
                    "stats": stats,
                    "avatar": chat_avatar,
                    "top_links": top_links,
                }
            )

        users = sorted(
            list(aggregated_users.values()),
            key=lambda item: (-int(item.get("count") or 0), -int(item.get("last_date") or 0)),
        )
        coordinated_users = [
            dict(item)
            for item in users
            if int(item.get("chat_count") or 0) >= 2 and int(item.get("risk_score") or 0) >= 18
        ]
        community_top_links = []
        for url, count in community_link_counter.most_common(24):
            meta = dict(community_link_context.get(url) or {})
            community_top_links.append(
                {
                    "url": url,
                    "count": int(count),
                    "chat_titles": sorted(list(meta.get("chat_titles") or [])),
                    "chat_usernames": sorted(list(meta.get("chat_usernames") or [])),
                    "sample_context": str(meta.get("sample_context") or ""),
                }
            )
        community_post_anomalies.sort(
            key=lambda item: (
                -len(list(item.get("flags") or [])),
                -int(item.get("views") or 0),
                -int(item.get("reactions_total") or 0),
            )
        )

        report_payload = {
            "scan_key": str(payload.get("scan_key") or scan_key),
            "query": str(payload.get("query") or ""),
            "title": str(payload.get("title") or payload.get("query") or scan_key),
            "completed_at": int(payload.get("completed_at") or 0),
            "generated_at": int(time.time()),
            "notes": list(payload.get("notes") or []),
            "edges": list(payload.get("edges") or []),
            "chats": chat_reports,
            "users": users,
            "community": {
                "top_links": community_top_links,
                "post_anomalies": community_post_anomalies[:32],
                "coordinated_users": coordinated_users[:32],
                "risk_reason_breakdown": [
                    {"reason": reason, "count": int(count)}
                    for reason, count in user_reason_counter.most_common(20)
                ],
            },
            "summary": {
                "total_chats": len(chat_reports),
                "accessible_chats": sum(1 for chat in chat_reports if bool(chat.get("accessible"))),
                "new_messages": int(sum(int(chat.get("new_messages") or 0) for chat in chat_reports)),
                "total_messages": int(total_messages),
                "users": len(users),
                "suspicious_users": int(sum(1 for row in users if int(row.get("risk_score") or 0) >= 18)),
                "risk_score_avg": round(float(risk_sum) / float(max(1, len(chat_reports)))),
                "temporary_joins": len(list(payload.get("temporary_joins") or [])),
                "root_title": str((payload.get("summary") or {}).get("root_title") or payload.get("title") or ""),
                "root_access": str((payload.get("summary") or {}).get("root_access") or "unknown"),
                "coordinated_users": int(len(coordinated_users)),
                "top_links": int(len(community_top_links)),
                "post_anomalies": int(len(community_post_anomalies)),
            },
        }

        css_path.write_text(build_community_scan_stylesheet(), encoding="utf-8")
        html_path.write_text(
            build_community_scan_report(
                str(report_payload.get("title") or report_payload.get("query") or scan_key),
                report_payload,
                css_href=css_path.name,
            ),
            encoding="utf-8",
        )
        self._storage.export_subset_sqlite(str(sql_path), peer_ids=peer_ids, scan_key=str(payload.get("scan_key") or scan_key))
        return {
            "ok": True,
            "html_path": str(html_path),
            "css_path": str(css_path),
            "sql_path": str(sql_path),
            "assets_dir": str(assets_dir),
        }

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

    def export_chat_history(
        self,
        chat_id: str,
        *,
        output_dir: Optional[str] = None,
        date_from: Optional[int] = None,
        date_to: Optional[int] = None,
        include_deleted: bool = False,
        copy_media: bool = True,
        formats: Sequence[str] = ("html", "json"),
        sync_remote: bool = True,
        remote_limit: int = 0,
        media_types: Optional[Sequence[str]] = None,
        max_media_size_mb: int = 8,
        dark_theme: bool = True,
    ) -> Dict[str, Any]:
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return {"ok": False, "error": "Экспорт недоступен: чат не является числовым ID или хранилище недоступно."}
        try:
            peer_id = int(chat_id)
        except Exception:
            return {"ok": False, "error": "Невалидный chat_id."}

        if sync_remote and self._tg_adapter and self._is_tg_authorized():
            try:
                syncer = getattr(self._tg_adapter, "scan_history_to_storage_sync", None)
                if callable(syncer):
                    limit_val = int(remote_limit) if int(remote_limit) > 0 else 0
                    syncer(
                        chat_id=chat_id,
                        limit=limit_val,
                        chunk_size=300,
                        timeout=float(os.getenv("DRAGO_HISTORY_EXPORT_TIMEOUT", "900") or 900.0),
                    )
            except Exception:
                log.warning("[SERVER] Remote sync before export failed for chat %s", chat_id, exc_info=True)

        peer_meta = self._storage.get_peer(peer_id)
        chat_title = str(peer_meta.get("title") or peer_meta.get("username") or chat_id)
        chat_type = str(peer_meta.get("type") or "chat")

        total_stored = 0
        try:
            total_stored = int(self._storage.get_chat_message_count(peer_id))
        except Exception:
            pass

        all_messages: List[Dict[str, Any]] = []
        chunk_size = 5000
        offset = 0
        while True:
            chunk = self._storage.get_all_messages_for_export(
                peer_id,
                date_from=date_from,
                date_to=date_to,
                include_deleted=include_deleted,
                offset=offset,
                limit=chunk_size,
            )
            if not chunk:
                break
            all_messages.extend(chunk)
            if len(chunk) < chunk_size:
                break
            offset += chunk_size

        if not all_messages:
            return {"ok": False, "error": "Нет сообщений для экспорта в указанном диапазоне."}

        my_id: Optional[int] = None
        try:
            raw_id = self.get_self_user_id()
            if raw_id:
                my_id = int(raw_id)
        except Exception:
            my_id = None

        target_dir = str(output_dir or "").strip()
        if not target_dir:
            target_dir = str(app_paths.exports_dir() / chat_title)

        def _progress(done: int, total: int, text: str) -> None:
            try:
                self.events.put({
                    "type": "gui_export_progress",
                    "chat_id": chat_id,
                    "done": int(done),
                    "total": int(total),
                    "text": str(text or ""),
                })
            except Exception:
                pass

        result = export_chat_to_files(
            target_dir,
            chat_title,
            all_messages,
            my_id=my_id,
            chat_type=chat_type,
            chat_id=chat_id,
            total_count=total_stored,
            date_from=date_from,
            date_to=date_to,
            copy_media=copy_media,
            formats=formats,
            media_types=media_types,
            max_media_size_mb=max_media_size_mb,
            dark_theme=dark_theme,
            progress_cb=_progress,
        )

        self.events.put({
            "type": "gui_export_done",
            "chat_id": chat_id,
            "result": result,
        })
        return result

    def get_chat_full_info(self, chat_id: str) -> Optional[Dict[str, Any]]:
        if not self._tg_adapter:
            return None
        getter = getattr(self._tg_adapter, "get_chat_full_info_sync", None)
        if not callable(getter):
            return None
        try:
            try:
                timeout = float(os.getenv("DRAGO_PROFILE_FULL_INFO_TIMEOUT", "8.0") or 8.0)
            except Exception:
                timeout = 8.0
            try:
                return getter(chat_id=chat_id, timeout=timeout)
            except TypeError:
                return getter(chat_id=chat_id)
        except Exception:
            log.exception("[SERVER] Failed to load chat profile for %s", chat_id)
            return None

    def _schedule_profile_sections_scan(self, peer_id: int, *, full_scan: bool = False) -> None:
        if not self._storage or self._profile_scan_shutdown:
            return
        pid = int(peer_id)
        with self._profile_scan_lock:
            if pid in self._profile_scan_inflight:
                return
            self._profile_scan_inflight.add(pid)

        def _run() -> None:
            try:
                self._storage.refresh_chat_profile_sections_cache(
                    pid,
                    chunk_size=420,
                    full_scan=bool(full_scan),
                )
            except Exception:
                log.exception("[SERVER] Failed to refresh profile sections cache for %s", pid)
            finally:
                with self._profile_scan_lock:
                    self._profile_scan_inflight.discard(pid)

        try:
            self._profile_scan_executor.submit(_run)
        except Exception:
            with self._profile_scan_lock:
                self._profile_scan_inflight.discard(pid)

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
        out: Dict[str, Any] = {
            "media": [],
            "files": [],
            "links": [],
            "members": [],
        }
        out["media"] = self.get_chat_profile_section(chat_id, "media", limit=media_limit)
        out["files"] = self.get_chat_profile_section(chat_id, "files", limit=file_limit)
        out["links"] = self.get_chat_profile_section(chat_id, "links", limit=link_limit)
        out["members"] = self.get_chat_profile_section(chat_id, "members", limit=members_limit)
        return out

    def get_chat_profile_section(self, chat_id: str, section: str, *, limit: int = 500) -> List[Dict[str, Any]]:
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return []
        section_key = str(section or "").strip().lower()
        if section_key not in {"media", "files", "links", "members", "voice", "music", "gifs"}:
            return []
        peer_id = int(chat_id)
        target_limit = max(1, int(limit or 500))

        latest_message_id = 0
        last_scanned_id = 0
        try:
            latest_message_id = int(self._storage.get_chat_latest_message_id(peer_id))
        except RuntimeError as exc:
            if "Storage not connected" in str(exc):
                return []
        except Exception:
            latest_message_id = 0
        try:
            scan_state = self._storage.get_chat_profile_scan_state(peer_id)
            last_scanned_id = int(scan_state.get("last_message_id") or 0)
        except RuntimeError as exc:
            if "Storage not connected" in str(exc):
                return []
        except Exception:
            last_scanned_id = 0

        if section_key == "members":
            return self._load_profile_members(chat_id, peer_id, limit=target_limit)

        rows: List[Dict[str, Any]] = []
        used_cache = False
        try:
            rows = self._storage.get_cached_chat_profile_section(peer_id, section_key, limit=target_limit)
            used_cache = bool(rows)
        except RuntimeError as exc:
            if "Storage not connected" in str(exc):
                return []
            log.exception("[SERVER] Failed to load cached profile section %s for %s", section_key, chat_id)
        except Exception:
            log.exception("[SERVER] Failed to load cached profile section %s for %s", section_key, chat_id)

        if not used_cache:
            try:
                if section_key == "media":
                    rows = self._storage.get_chat_shared_media(peer_id, limit=target_limit)
                elif section_key == "files":
                    rows = self._storage.get_chat_shared_files(peer_id, limit=target_limit)
                elif section_key == "links":
                    rows = self._storage.get_chat_links(peer_id, limit=target_limit)
                elif section_key == "voice":
                    rows = self._storage.get_chat_shared_voice(peer_id, limit=target_limit)
                elif section_key == "music":
                    rows = self._storage.get_chat_shared_music(peer_id, limit=target_limit)
                elif section_key == "gifs":
                    rows = self._storage.get_chat_shared_gifs(peer_id, limit=target_limit)
            except RuntimeError as exc:
                if "Storage not connected" in str(exc):
                    return []
                log.exception("[SERVER] Failed to load profile section %s for %s", section_key, chat_id)
            except Exception:
                log.exception("[SERVER] Failed to load profile section %s for %s", section_key, chat_id)

        if latest_message_id > 0 and latest_message_id > last_scanned_id:
            self._schedule_profile_sections_scan(peer_id, full_scan=False)
        elif latest_message_id > 0 and not used_cache:
            self._schedule_profile_sections_scan(peer_id, full_scan=True)
        return [dict(row) for row in list(rows or []) if isinstance(row, dict)]

    def _load_profile_members(self, chat_id: str, peer_id: int, *, limit: int) -> List[Dict[str, Any]]:
        if not self._storage:
            return []
        try:
            local_rows = self._storage.get_chat_members_activity(peer_id, limit=max(1, int(limit)))
        except RuntimeError as exc:
            if "Storage not connected" in str(exc):
                return []
            log.exception("[SERVER] Failed to load local members activity for %s", chat_id)
            local_rows = []
        except Exception:
            log.exception("[SERVER] Failed to load local members activity for %s", chat_id)
            local_rows = []

        tg_members: List[Dict[str, Any]] = []
        enable_remote_members = str(os.getenv("DRAGO_PROFILE_REMOTE_MEMBERS", "0")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if enable_remote_members and self._tg_adapter:
            getter = getattr(self._tg_adapter, "get_chat_members_preview_sync", None)
            if callable(getter):
                remote_limit = min(max(1, int(limit)), 50)
                try:
                    timeout = float(os.getenv("DRAGO_PROFILE_MEMBERS_TIMEOUT", "8.0") or 8.0)
                except Exception:
                    timeout = 8.0
                try:
                    tg_members = list(getter(chat_id=chat_id, limit=remote_limit, timeout=timeout) or [])
                except TypeError:
                    try:
                        tg_members = list(getter(chat_id=chat_id, limit=remote_limit) or [])
                    except Exception:
                        log.exception("[SERVER] Failed to load Telegram members preview for %s", chat_id)
                except Exception:
                    log.exception("[SERVER] Failed to load Telegram members preview for %s", chat_id)
        if not tg_members:
            return [dict(row) for row in list(local_rows or []) if isinstance(row, dict)]

        local_map: Dict[int, Dict[str, Any]] = {}
        for row in list(local_rows or []):
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
        return merged[: max(1, int(limit))]

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

    @staticmethod
    def _write_csv_rows(path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})

    def get_chat_statistics(self, chat_id: str, *, limit: int = 500, sender_limit: int = 24) -> Dict[str, Any]:
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return {}
        try:
            peer_id = int(chat_id)
            stats = self._storage.get_chat_statistics(
                peer_id,
                limit=limit,
                sender_limit=sender_limit,
                daily_limit=30,
                include_sender_daily=False,
            )
            return self._attach_chat_statistics_snapshots(peer_id, stats)
        except RuntimeError as exc:
            if "Storage not connected" in str(exc):
                return {}
            log.exception("[SERVER] Failed to load chat statistics for %s", chat_id)
            return {}
        except Exception:
            log.exception("[SERVER] Failed to load chat statistics for %s", chat_id)
            return {}

    def scan_chat_statistics(self, chat_id: str, *, limit: int = 0, sender_limit: int = 24) -> Dict[str, Any]:
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return {}
        try:
            peer_id = int(chat_id)
            if limit <= 0 and self._tg_adapter and self._is_tg_authorized():
                try:
                    syncer = getattr(self._tg_adapter, "scan_history_to_storage_sync", None)
                    if callable(syncer):
                        syncer(
                            chat_id=chat_id,
                            limit=0,
                            chunk_size=200,
                            timeout=float(os.getenv("DRAGO_HISTORY_SCAN_TIMEOUT", "600") or 600.0),
                        )
                except Exception:
                    log.exception("[SERVER] Failed to sync full history before statistics scan for %s", chat_id)
            stats = self._storage.get_chat_statistics(
                peer_id,
                limit=limit,
                sender_limit=sender_limit,
                daily_limit=30,
                include_sender_daily=False,
            )
            scanned_at = self._storage.save_chat_statistics_snapshot(peer_id, stats)
            stats["scanned_at"] = int(scanned_at)
            return self._attach_chat_statistics_snapshots(peer_id, stats)
        except RuntimeError as exc:
            if "Storage not connected" in str(exc):
                return {}
            log.exception("[SERVER] Failed to scan chat statistics for %s", chat_id)
            return {}
        except Exception:
            log.exception("[SERVER] Failed to scan chat statistics for %s", chat_id)
            return {}

    def export_chat_statistics_report(
        self,
        chat_id: str,
        *,
        output_path: str,
        title: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not (self._storage and str(chat_id or "").lstrip("-").isdigit()):
            return {"ok": False, "error": "Статистика недоступна для этого чата."}
        target_raw = str(output_path or "").strip()
        if not target_raw:
            return {"ok": False, "error": "Не указан путь выгрузки."}
        try:
            peer_id = int(chat_id)
            stats = self._storage.get_chat_statistics(
                peer_id,
                limit=0,
                sender_limit=0,
                daily_limit=0,
                include_sender_daily=True,
            )
            stats["exported_at"] = int(time.time())
            stats = self._attach_chat_statistics_snapshots(peer_id, stats)
            report_title = str(title or chat_id)

            html_path = Path(target_raw).expanduser()
            if html_path.suffix.lower() != ".html":
                html_path = html_path.with_suffix(".html")
            html_path.parent.mkdir(parents=True, exist_ok=True)

            html_path.write_text(
                build_chat_statistics_report(report_title, chat_id, stats),
                encoding="utf-8",
            )

            json_path = html_path.with_name(html_path.stem + ".data.json")
            json_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

            sender_rows = [dict(row) for row in list(stats.get("sender_details") or []) if isinstance(row, dict)]
            daily_rows = [dict(row) for row in list(stats.get("daily_activity") or []) if isinstance(row, dict)]
            hourly_rows = [dict(row) for row in list(stats.get("hourly_activity") or []) if isinstance(row, dict)]
            suspicious_rows = [dict(row) for row in list(stats.get("suspicious_senders") or []) if isinstance(row, dict)]
            risk_payload = stats.get("risk") if isinstance(stats.get("risk"), dict) else {}
            risk_factor_rows = [dict(row) for row in list(risk_payload.get("factors") or []) if isinstance(row, dict)]

            sender_daily = stats.get("sender_daily_activity") if isinstance(stats.get("sender_daily_activity"), dict) else {}
            sender_lookup: Dict[str, Dict[str, Any]] = {
                str(row.get("sender_id") or ""): row
                for row in sender_rows
                if str(row.get("sender_id") or "").strip()
            }
            sender_daily_rows: List[Dict[str, Any]] = []
            for sender_id, rows in sender_daily.items():
                meta = sender_lookup.get(str(sender_id), {})
                for item in list(rows or []):
                    if not isinstance(item, dict):
                        continue
                    sender_daily_rows.append(
                        {
                            "sender_id": sender_id,
                            "name": str(meta.get("name") or sender_id),
                            "username": str(meta.get("username") or ""),
                            "day": str(item.get("day") or ""),
                            "count": int(item.get("count") or 0),
                        }
                    )

            users_csv = html_path.with_name(html_path.stem + ".users.csv")
            daily_csv = html_path.with_name(html_path.stem + ".daily.csv")
            hourly_csv = html_path.with_name(html_path.stem + ".hourly.csv")
            sender_daily_csv = html_path.with_name(html_path.stem + ".user_daily.csv")
            suspicious_csv = html_path.with_name(html_path.stem + ".suspicious.csv")
            risk_csv = html_path.with_name(html_path.stem + ".risk_factors.csv")

            self._write_csv_rows(
                users_csv,
                [
                    "sender_id",
                    "name",
                    "username",
                    "type",
                    "count",
                    "share",
                    "media_messages",
                    "deleted_messages",
                    "total_views",
                    "total_forwards",
                    "last_date",
                ],
                sender_rows,
            )
            self._write_csv_rows(daily_csv, ["day", "count"], daily_rows)
            self._write_csv_rows(hourly_csv, ["hour", "count"], hourly_rows)
            self._write_csv_rows(
                sender_daily_csv,
                ["sender_id", "name", "username", "day", "count"],
                sender_daily_rows,
            )
            self._write_csv_rows(
                suspicious_csv,
                [
                    "sender_id",
                    "name",
                    "username",
                    "type",
                    "count",
                    "share",
                    "risk_score",
                    "deleted_share",
                    "media_share",
                    "off_hours_share",
                    "peak_day_share",
                ],
                suspicious_rows,
            )
            self._write_csv_rows(
                risk_csv,
                ["key", "label", "severity", "score", "detail"],
                risk_factor_rows,
            )
            return {
                "ok": True,
                "html_path": str(html_path),
                "json_path": str(json_path),
                "users_csv_path": str(users_csv),
                "daily_csv_path": str(daily_csv),
                "hourly_csv_path": str(hourly_csv),
                "sender_daily_csv_path": str(sender_daily_csv),
                "suspicious_csv_path": str(suspicious_csv),
                "risk_csv_path": str(risk_csv),
            }
        except RuntimeError as exc:
            if "Storage not connected" in str(exc):
                return {"ok": False, "error": "Хранилище недоступно."}
            log.exception("[SERVER] Failed to export chat statistics for %s", chat_id)
            return {"ok": False, "error": str(exc)}
        except Exception as exc:
            log.exception("[SERVER] Failed to export chat statistics for %s", chat_id)
            return {"ok": False, "error": str(exc)}

    def _attach_chat_statistics_snapshots(self, peer_id: int, stats: Dict[str, Any]) -> Dict[str, Any]:
        if not self._storage:
            return stats
        data = dict(stats or {})
        try:
            snapshots = self._storage.get_chat_statistics_snapshots(peer_id, limit=2)
        except Exception:
            snapshots = []
        latest = snapshots[0] if len(snapshots) >= 1 else None
        previous = snapshots[1] if len(snapshots) >= 2 else None
        delta: Dict[str, Any] = {}
        if isinstance(latest, dict) and isinstance(previous, dict):
            for key in ("total_messages", "media_messages", "deleted_messages", "total_views", "total_forwards", "total_reactions"):
                try:
                    delta[key] = int(latest.get(key) or 0) - int(previous.get(key) or 0)
                except Exception:
                    continue
        data["snapshot_meta"] = {
            "latest": latest,
            "previous": previous,
            "delta": delta,
        }
        return data

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
    ) -> Optional[int]:
        if not self._tg_adapter:
            return None
        sender = getattr(self._tg_adapter, "send_text_sync", None)
        if not callable(sender):
            return None
        try:
            result = sender(chat_id=chat_id, text=text, reply_to=reply_to, entities=entities)
        except TypeError:
            result = sender(chat_id=chat_id, text=text, reply_to=reply_to)
        if isinstance(result, bool):
            return None
        try:
            message_id = int(result) if result is not None else 0
        except Exception:
            message_id = 0
        return message_id if message_id > 0 else None

    def ensure_chat_avatar(self, chat_id: str, *, file_id: Optional[str] = None, size: str = "small") -> Optional[str]:
        if not self._tg_adapter:
            return None
        return self._tg_adapter.ensure_chat_avatar_sync(chat_id=chat_id, file_id=file_id, size=size)

    def get_cached_chat_avatar(self, chat_id: str, *, file_id: Optional[str] = None, size: str = "small") -> Optional[str]:
        if not self._tg_adapter:
            return None
        getter = getattr(self._tg_adapter, "get_cached_chat_avatar_sync", None)
        if not callable(getter):
            return None
        try:
            return getter(chat_id=chat_id, file_id=file_id, size=size)
        except Exception:
            return None

    def ensure_user_avatar(self, user_id: str, *, file_id: Optional[str] = None, size: str = "small") -> Optional[str]:
        if not self._tg_adapter:
            return None
        return self._tg_adapter.ensure_user_avatar_sync(user_id=user_id, file_id=file_id, size=size)

    def get_cached_user_avatar(self, user_id: str, *, file_id: Optional[str] = None, size: str = "small") -> Optional[str]:
        if not self._tg_adapter:
            return None
        getter = getattr(self._tg_adapter, "get_cached_user_avatar_sync", None)
        if not callable(getter):
            return None
        try:
            return getter(user_id=user_id, file_id=file_id, size=size)
        except Exception:
            return None

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
        reactions: Optional[List[Dict[str, Any]]] = None,
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
            "reactions": reactions or None,
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
        reactions: Optional[List[Dict[str, Any]]] = None,
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
            "reactions": reactions or None,
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
                message_id = self.send_text_to_telegram(
                    chat_id=chat_id,
                    text=text,
                    reply_to=reply_to,
                    entities=entities,
                )
                if message_id:
                    self.events.put(
                        {
                            "type": "gui_message_sent",
                            "chat_id": chat_id,
                            "local_id": int(local_id),
                            "message_id": int(message_id),
                            "ts": int(time.time()),
                        }
                    )
            except Exception:
                log.exception("[SERVER] Failed to send text to Telegram chat %s", chat_id)

        try:
            if not self._send_executor_shutdown:
                self._send_executor.submit(_send_task)
            else:
                _send_task()
        except Exception:
            try:
                _send_task()
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

    def pin_message(self, chat_id: str, message_id: int) -> bool:
        if not self._tg_adapter:
            return False
        pinner = getattr(self._tg_adapter, "pin_message_sync", None)
        if not callable(pinner):
            return False
        try:
            return bool(pinner(chat_id=str(chat_id), message_id=int(message_id)))
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
        if getattr(self, "_send_executor", None) and not self._send_executor_shutdown:
            try:
                self._send_executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                self._send_executor.shutdown(wait=False)
            self._send_executor_shutdown = True
        if getattr(self, "_profile_scan_executor", None) and not self._profile_scan_shutdown:
            try:
                self._profile_scan_executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                self._profile_scan_executor.shutdown(wait=False)
            self._profile_scan_shutdown = True
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
