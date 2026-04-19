# storage.py — единый слой БД (SQLite/APSW), схема, WAL/PRAGMA + DAO
from __future__ import annotations

import os
import re
import time
import threading
import logging
import contextlib
import shutil
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
import json

# --- драйвер: предпочитаем APSW (лучше контролирует транзакции/WAL), fallback -> sqlite3
try:
    import apsw  # type: ignore
    HAVE_APSW = True
except Exception:
    apsw = None  # type: ignore[assignment]
    HAVE_APSW = False
import sqlite3


from utils.app_paths import db_path as _default_db_path


DEFAULT_DB_DIR = os.getenv("DRAGO_DB_DIR", "")
DEFAULT_DB_PATH = os.getenv("DRAGO_DB_PATH", os.path.join(DEFAULT_DB_DIR, "drago.db") if DEFAULT_DB_DIR else str(_default_db_path()))

# Рекомендованные параметры кеша (≈64 MiB) и mmap (≈256 MiB) — безопасные дефолты для desktop
CACHE_PAGES_KIB = 64 * 1024    # PRAGMA cache_size = -65536  (отрицательное -> KiB)
MMAP_SIZE_BYTES = 256 * 1024 * 1024
EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001FAFF"
    "\U00002600-\U000027BF"
    "\U0001F1E6-\U0001F1FF"
    "]",
    re.UNICODE,
)
URL_RE = re.compile(r"(https?://[^\s]+|t\.me/[^\s]+)", re.IGNORECASE)
ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]")
COMBINING_MARK_RE = re.compile(r"[\u0300-\u036f\u0483-\u0489]")
SPECIAL_SYMBOL_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff\u0300-\u036f\u0483-\u0489\u00ad]")
PROFILE_MEDIA_TYPES = {
    "image",
    "photo",
    "video",
    "animation",
    "gif",
    "video_note",
    "sticker",
}
PROFILE_FILE_TYPES = {
    "document",
    "audio",
    "voice",
}
log = logging.getLogger("storage")

_stats_src_seq_lock = threading.Lock()


_MIXED_SCRIPT_RE = re.compile(r"[\u0400-\u052F]|[A-Za-z]")


def _contains_mixed_cyrillic_latin(text: str) -> bool:
    raw = str(text or "")
    has_cyrillic = False
    has_latin = False
    for m in _MIXED_SCRIPT_RE.finditer(raw):
        ch = m.group(0)
        code = ord(ch)
        if 0x0400 <= code <= 0x052F:
            has_cyrillic = True
        else:
            has_latin = True
        if has_cyrillic and has_latin:
            return True
    return False


def _safe_json_loads(raw: Optional[str], fallback: Any = None) -> Any:
    if not raw:
        return fallback
    try:
        value = json.loads(raw)
        return value if value is not None else fallback
    except Exception:
        return fallback


def _parse_message_json_fields(row: tuple, field_map: Dict[str, int]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for field_name, index in field_map.items():
        raw = row[index] if index < len(row) else None
        result[field_name] = _safe_json_loads(raw)
    return result


def _normalize_message_fingerprint(text: str) -> str:
    raw = str(text or "").strip().lower()
    if not raw:
        return ""
    raw = ZERO_WIDTH_RE.sub("", raw)
    raw = COMBINING_MARK_RE.sub("", raw)
    raw = re.sub(r"https?://\S+|t\.me/\S+", " url ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"@[a-z0-9_]{3,}", " mention ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\d+", "0", raw)
    raw = re.sub(r"\s+", " ", raw)
    return raw[:240]


def _median_value(values: Sequence[float]) -> float:
    ordered = sorted(float(v) for v in list(values or []))
    if not ordered:
        return 0.0
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return float(ordered[mid - 1] + ordered[mid]) / 2.0


def _reaction_total_from_json(raw_value: Any) -> int:
    try:
        items = json.loads(str(raw_value or "[]"))
    except Exception:
        items = []
    if not isinstance(items, list):
        return 0
    total = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            total += int(item.get("count") or 0)
        except Exception:
            continue
    return int(total)


def _ensure_db_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(str(path)))
    if parent:
        os.makedirs(parent, exist_ok=True)


def _normalize_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    url = raw if raw.lower().startswith(("http://", "https://")) else f"https://{raw}"
    # Trim common trailing punctuation from plain-text links.
    while url and url[-1] in ".,;:!?)]}>":
        url = url[:-1]
    return url


def _iter_emoji_sequences(text: str) -> Iterator[str]:
    raw = str(text or "")
    if not raw:
        return
    try:
        import emoji as emoji_lib  # type: ignore

        emoji_list = getattr(emoji_lib, "emoji_list", None)
        if callable(emoji_list):
            rows = list(emoji_list(raw) or [])
            if rows:
                for item in rows:
                    if not isinstance(item, dict):
                        continue
                    value = str(item.get("emoji") or "").strip()
                    if value:
                        yield value
                return
    except Exception:
        pass

    length = len(raw)
    pos = 0
    while pos < length:
        match = EMOJI_RE.search(raw, pos)
        if match is None:
            break
        start, end = match.span()
        while end < length:
            ch = raw[end]
            code = ord(ch)
            if ch in {"\ufe0f", "\ufe0e"}:
                end += 1
                continue
            if 0x1F3FB <= code <= 0x1F3FF:
                end += 1
                continue
            if ch == "\u200d" and (end + 1) < length:
                next_match = EMOJI_RE.match(raw, end + 1)
                if next_match is not None:
                    end = next_match.end()
                    continue
            break
        value = raw[start:end].strip()
        if value:
            yield value
        pos = max(end, start + 1)


@dataclass
class _ConnWrap:
    is_apsw: bool
    conn: object  # apsw.Connection | sqlite3.Connection


def _serialize_json_field(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return None


class Storage:
    """
    Единая БД для диалогов/пиров/сообщений/файлов.
    Поддерживает APSW (если установлен) или стандартный sqlite3.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH) -> None:
        self.db_path = db_path
        self._cx: Optional[_ConnWrap] = None
        self._lock = threading.RLock()
        self._closing = False

    # --------------------------- lifecycle ---------------------------

    @classmethod
    def open_default(cls) -> "Storage":
        _ensure_db_parent_dir(DEFAULT_DB_PATH)
        st = cls(DEFAULT_DB_PATH)
        st.connect()
        return st

    def connect(self) -> None:
        if self._cx:
            return
        self._closing = False

        _ensure_db_parent_dir(self.db_path)

        apsw_error: Optional[Exception] = None
        if HAVE_APSW:
            try:
                conn = apsw.Connection(self.db_path)  # type: ignore[union-attr,assignment]
                conn.setbusytimeout(5_000)  # мс
                cur = conn.cursor()
                # WAL и базовые pragma
                cur.execute("PRAGMA foreign_keys=ON;")
                cur.execute("PRAGMA journal_mode=WAL;")              # включаем WAL
                cur.execute("PRAGMA synchronous=NORMAL;")            # в WAL это «безопасно» и быстро
                cur.execute("PRAGMA temp_store=MEMORY;")
                cur.execute("PRAGMA cache_size=-%d;" % CACHE_PAGES_KIB)
                cur.execute("PRAGMA wal_autocheckpoint=1000;")       # ~1000 страниц между чекпойнтами
                cur.execute("PRAGMA mmap_size=%d;" % MMAP_SIZE_BYTES)
                self._cx = _ConnWrap(True, conn)
                return
            except Exception as exc:
                apsw_error = exc
                self._cx = None
                log.warning("APSW init failed for %s; falling back to sqlite3: %s", self.db_path, exc)

        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute("PRAGMA foreign_keys=ON;")
            # Важно выполнять PRAGMA после открытия:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-%d;" % CACHE_PAGES_KIB)
            conn.execute("PRAGMA wal_autocheckpoint=1000;")
            conn.execute("PRAGMA mmap_size=%d;" % MMAP_SIZE_BYTES)
            self._cx = _ConnWrap(False, conn)
        except Exception:
            if apsw_error is not None:
                log.exception("sqlite3 fallback also failed after APSW init error for %s", self.db_path)
            raise

    def close(self) -> None:
        with self._lock:
            if not self._cx:
                return
            try:
                if self._cx.is_apsw:
                    self._cx.conn.close()  # type: ignore[attr-defined]
                else:
                    self._cx.conn.close()  # type: ignore[attr-defined]
            finally:
                self._cx = None
                self._closing = True

    def switch_db_path(self, db_path: str, *, init_schema: bool = True) -> bool:
        target = os.path.abspath(str(db_path or ""))
        if not target:
            raise ValueError("db_path is required")
        with self._lock:
            current = os.path.abspath(str(self.db_path or ""))
            if current == target and self._cx is not None:
                return False
            if self._cx is not None:
                try:
                    if self._cx.is_apsw:
                        self._cx.conn.close()  # type: ignore[attr-defined]
                    else:
                        self._cx.conn.close()  # type: ignore[attr-defined]
                except Exception:
                    pass
            self._cx = None
            self._closing = False
            self.db_path = target
            _ensure_db_parent_dir(target)
        self.connect()
        if init_schema:
            self.init_schema()
        return True

    def _get_cx_locked(self) -> _ConnWrap:
        cx = self._cx
        if cx is None and not self._closing:
            try:
                self.connect()
            except Exception:
                cx = None
            else:
                cx = self._cx
        if cx is None:
            raise RuntimeError("Storage not connected")
        return cx

    # --------------------------- helpers ---------------------------

    @contextmanager
    def _cursor(self):
        if not self._cx:
            raise RuntimeError("Storage not connected")
        if self._cx.is_apsw:
            cur = self._cx.conn.cursor()  # type: ignore[attr-defined]
            yield cur
        else:
            cur = self._cx.conn.cursor()  # type: ignore[attr-defined]
            try:
                yield cur
            finally:
                cur.close()

    def _execmany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        with self._lock:
            cx = self._get_cx_locked()
            row_list = list(rows)
            if not row_list:
                return
            if cx.is_apsw:
                cur = cx.conn.cursor()  # type: ignore[attr-defined]
                try:
                    cur.execute("BEGIN")
                    cur.executemany(sql, row_list)
                    cur.execute("COMMIT")
                except Exception:
                    with contextlib.suppress(Exception):
                        cur.execute("ROLLBACK")
                    raise
            else:
                conn = cx.conn  # type: ignore[attr-defined]
                try:
                    conn.execute("BEGIN")
                    conn.executemany(sql, row_list)
                    conn.execute("COMMIT")
                except Exception:
                    with contextlib.suppress(Exception):
                        conn.execute("ROLLBACK")
                    raise

    def _exec(self, sql: str, params: Sequence[Any] = ()) -> None:
        with self._lock:
            cx = self._get_cx_locked()
            if cx.is_apsw:
                cur = cx.conn.cursor()  # type: ignore[attr-defined]
                cur.execute(sql, params)
            else:
                cx.conn.execute(sql, params)  # type: ignore[attr-defined]

    def _query(self, sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
        with self._lock:
            cx = self._get_cx_locked()
            if cx.is_apsw:
                cur = cx.conn.cursor()  # type: ignore[attr-defined]
                cur.execute(sql, params)
                return list(cur.fetchall())
            cur = cx.conn.execute(sql, params)  # type: ignore[attr-defined]
            try:
                return list(cur.fetchall())
            finally:
                cur.close()

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self._query(f"PRAGMA table_info({table_name})")
        columns: set[str] = set()
        for row in rows:
            if len(row) > 1 and row[1]:
                columns.add(str(row[1]))
        return columns

    def _ensure_columns(self, table_name: str, columns: Sequence[Tuple[str, str]]) -> None:
        existing = self._table_columns(table_name)
        for column_name, ddl in columns:
            if column_name in existing:
                continue
            self._exec(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")
            existing.add(column_name)

    # --------------------------- schema ---------------------------

    def init_schema(self) -> None:
        """
        Создаёт минимально необходимую схему (идемпотентно).
        """
        ddl = [
            # peers: пользователи/чаты/каналы
            """
            CREATE TABLE IF NOT EXISTS peers (
              id          INTEGER PRIMARY KEY,
              type        TEXT NOT NULL,           -- user|chat|channel
              username    TEXT,
              title       TEXT,
              photo_small TEXT,
              photo_big   TEXT,
              photo_hash  TEXT,
              updated_at  INTEGER
            );
            """,
            # dialogs: верхние сообщения и счётчики по диалогу
            """
            CREATE TABLE IF NOT EXISTS dialogs (
              peer_id             INTEGER PRIMARY KEY REFERENCES peers(id) ON DELETE CASCADE,
              top_message_id      INTEGER,
              last_message_date   INTEGER,
              unread_count        INTEGER DEFAULT 0,
              pinned              INTEGER DEFAULT 0,
              last_read_inbox_id  INTEGER,
              last_read_outbox_id INTEGER,
              updated_at          INTEGER
            );
            """,
            # messages: история сообщений (по peer_id)
            """
            CREATE TABLE IF NOT EXISTS messages (
              peer_id      INTEGER NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
              id           INTEGER NOT NULL,
              date         INTEGER NOT NULL,
              from_id      INTEGER,
              reply_to     INTEGER,
              message      TEXT,
              media_type   TEXT,
              media_id     TEXT,
              is_deleted   INTEGER DEFAULT 0,
              forward_info TEXT,
              file_name    TEXT,
              entities     TEXT,
              reply_markup TEXT,
              duration     INTEGER,
              waveform     TEXT,
              reactions    TEXT,
              poll         TEXT,
              views        INTEGER,
              forwards     INTEGER,
              media_group_id TEXT,
              PRIMARY KEY (peer_id, id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS deleted_message_events (
              peer_id        INTEGER NOT NULL,
              message_id     INTEGER NOT NULL,
              deleted_at     INTEGER NOT NULL,
              snapshot_text  TEXT,
              media_type     TEXT,
              sender_id      INTEGER,
              source         TEXT,
              PRIMARY KEY (peer_id, message_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS edited_message_revisions (
              id             INTEGER PRIMARY KEY AUTOINCREMENT,
              peer_id        INTEGER NOT NULL,
              message_id     INTEGER NOT NULL,
              edited_at      INTEGER NOT NULL,
              snapshot_text  TEXT,
              media_type     TEXT,
              sender_id      INTEGER
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_edited_revisions_peer_msg
              ON edited_message_revisions(peer_id, message_id, edited_at DESC);
            """,
            """
            CREATE TABLE IF NOT EXISTS chat_statistics_snapshots (
              peer_id     INTEGER NOT NULL,
              scanned_at  INTEGER NOT NULL,
              payload     TEXT NOT NULL,
              PRIMARY KEY (peer_id, scanned_at)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS chat_profile_sections_cache (
              peer_id      INTEGER NOT NULL,
              section      TEXT NOT NULL,
              dedupe_key   TEXT NOT NULL,
              message_id   INTEGER NOT NULL,
              message_date INTEGER NOT NULL,
              payload      TEXT NOT NULL,
              updated_at   INTEGER NOT NULL,
              PRIMARY KEY (peer_id, section, dedupe_key)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS chat_profile_scan_state (
              peer_id         INTEGER PRIMARY KEY,
              last_message_id INTEGER NOT NULL DEFAULT 0,
              updated_at      INTEGER NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS community_scan_runs (
              scan_key     TEXT PRIMARY KEY,
              query        TEXT NOT NULL,
              title        TEXT,
              root_chat_id INTEGER,
              payload      TEXT NOT NULL,
              updated_at   INTEGER NOT NULL
            );
            """,
            # files: медиа-кэш по file_id (в т.ч. avatar/media пути)
            """
            CREATE TABLE IF NOT EXISTS files (
              file_id   TEXT PRIMARY KEY,   -- уникальный id или составной peer:msg
              path      TEXT,               -- локальный путь в кэше
              size      INTEGER,
              mime      TEXT,
              crc32     INTEGER,
              ttl       INTEGER,
              added_at  INTEGER
            );
            """,
            # индексы для запросов истории
            """
            CREATE TABLE IF NOT EXISTS ai_history (
              chat_id   TEXT NOT NULL,
              id        INTEGER NOT NULL,
              role      TEXT NOT NULL,
              content   TEXT,
              timestamp TEXT,
              reply_to  INTEGER,
              is_edited INTEGER DEFAULT 0,
              is_deleted INTEGER DEFAULT 0,
              PRIMARY KEY (chat_id, id)
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_messages_peer_date ON messages(peer_id, date DESC);",
            "CREATE INDEX IF NOT EXISTS idx_messages_peer_id   ON messages(peer_id, id DESC);",
            "CREATE INDEX IF NOT EXISTS idx_messages_peer_sender_date ON messages(peer_id, from_id, date DESC);",
            "CREATE INDEX IF NOT EXISTS idx_deleted_events_peer_date ON deleted_message_events(peer_id, deleted_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_chat_statistics_snapshots_peer_time ON chat_statistics_snapshots(peer_id, scanned_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_profile_sections_cache_peer_section_date ON chat_profile_sections_cache(peer_id, section, message_date DESC, message_id DESC);",
            "CREATE INDEX IF NOT EXISTS idx_community_scan_runs_updated_at ON community_scan_runs(updated_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_ai_history_chat_id ON ai_history(chat_id, id DESC);",
        ]
        with self._lock:
            for sql in ddl:
                self._exec(sql)
            self._ensure_columns(
                "messages",
                [
                    ("is_deleted", "INTEGER DEFAULT 0"),
                    ("forward_info", "TEXT"),
                    ("file_name", "TEXT"),
                    ("entities", "TEXT"),
                    ("reply_markup", "TEXT"),
                    ("duration", "INTEGER"),
                    ("waveform", "TEXT"),
                    ("reactions", "TEXT"),
                    ("poll", "TEXT"),
                    ("views", "INTEGER"),
                    ("forwards", "INTEGER"),
                    ("media_group_id", "TEXT"),
                ],
            )

    # --------------------------- DAO: UPSERT ---------------------------

    @staticmethod
    def _now() -> int:
        return int(time.time())

    def upsert_peers(self, peers: Iterable[Dict[str, Any]]) -> None:
        """
        peers: {id, type, username, title, photo_small, photo_big}
        """
        rows = []
        ts = self._now()
        for p in peers:
            try:
                pid = int(p.get("id"))
            except Exception:
                continue
            rows.append((
                pid,
                str(p.get("type") or ""),
                str(p.get("username") or ""),
                str(p.get("title") or ""),
                str(p.get("photo_small") or p.get("photo_small_id") or "") or None,
                str(p.get("photo_big") or p.get("photo_big_id") or "") or None,
                None,
                ts,
            ))
        if not rows:
            return
        self._execmany(
            """
            INSERT INTO peers(id,type,username,title,photo_small,photo_big,photo_hash,updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              type=excluded.type,
              username=excluded.username,
              title=excluded.title,
              photo_small=COALESCE(excluded.photo_small, peers.photo_small),
              photo_big=COALESCE(excluded.photo_big, peers.photo_big),
              updated_at=excluded.updated_at
            """,
            rows,
        )

    def upsert_dialogs(self, dialogs: Iterable[Dict[str, Any]]) -> None:
        """
        dialogs: {peer_id, top_message_id?, last_message_date? ...}
        """
        rows = []
        ts = self._now()
        for d in dialogs:
            try:
                pid = int(d.get("peer_id"))
            except Exception:
                continue
            rows.append((
                pid,
                d.get("top_message_id"),
                d.get("last_message_date"),
                int(d.get("unread_count") or 0),
                1 if d.get("pinned") else 0,
                d.get("last_read_inbox_id"),
                d.get("last_read_outbox_id"),
                ts,
            ))
        if not rows:
            return
        self._execmany(
            """
            INSERT INTO dialogs(peer_id,top_message_id,last_message_date,unread_count,pinned,last_read_inbox_id,last_read_outbox_id,updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(peer_id) DO UPDATE SET
              top_message_id=COALESCE(excluded.top_message_id, dialogs.top_message_id),
              last_message_date=COALESCE(excluded.last_message_date, dialogs.last_message_date),
              unread_count=excluded.unread_count,
              pinned=excluded.pinned,
              last_read_inbox_id=COALESCE(excluded.last_read_inbox_id, dialogs.last_read_inbox_id),
              last_read_outbox_id=COALESCE(excluded.last_read_outbox_id, dialogs.last_read_outbox_id),
              updated_at=excluded.updated_at
            """,
            rows,
        )

    def update_dialog_last_ts(self, peer_id: int, last_ts: Optional[int], top_message_id: Optional[int] = None) -> None:
        self._exec(
            """
            INSERT INTO dialogs(peer_id,last_message_date,top_message_id,updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(peer_id) DO UPDATE SET
              last_message_date=COALESCE(excluded.last_message_date, dialogs.last_message_date),
              top_message_id=COALESCE(excluded.top_message_id, dialogs.top_message_id),
              updated_at=excluded.updated_at
            """,
            (int(peer_id), last_ts, top_message_id, self._now()),
        )

    def upsert_messages(self, peer_id: int, msgs: Iterable[Dict[str, Any]]) -> None:
        """
        msgs: {id,date,from_id,reply_to,message,media_type,media_id,file_path,file_size,mime}
        """
        m_rows: List[Tuple] = []
        f_rows: List[Tuple] = []

        for m in msgs:
            try:
                mid = int(m.get("id"))
                dt = int(m.get("date") or 0)
            except Exception:
                continue

            deleted_flag = m.get("is_deleted")
            if deleted_flag is None:
                deleted_flag = None
            else:
                deleted_flag = 1 if bool(deleted_flag) else 0

            forward_serialized = _serialize_json_field(m.get("forward_info"))
            waveform_serialized = _serialize_json_field(m.get("waveform"))
            entities_serialized = _serialize_json_field(m.get("entities"))
            reply_markup_serialized = _serialize_json_field(m.get("reply_markup"))
            reactions_serialized = _serialize_json_field(m.get("reactions"))
            poll_serialized = _serialize_json_field(m.get("poll"))

            duration_val = m.get("duration")
            try:
                duration_int = int(duration_val) if duration_val is not None else None
            except Exception:
                duration_int = None

            views_val = m.get("views")
            try:
                views_int = int(views_val) if views_val is not None else None
            except Exception:
                views_int = None

            forwards_val = m.get("forwards")
            try:
                forwards_int = int(forwards_val) if forwards_val is not None else None
            except Exception:
                forwards_int = None

            m_rows.append((
                int(peer_id),
                mid,
                dt,
                int(m.get("from_id") or m.get("sender_id") or 0) or None,
                m.get("reply_to"),
                m.get("message") or m.get("text") or "",
                m.get("media_type") or m.get("type") or None,
                m.get("media_id") or None,
                deleted_flag,
                forward_serialized,
                m.get("file_name"),
                entities_serialized,
                reply_markup_serialized,
                duration_int,
                waveform_serialized,
                reactions_serialized,
                poll_serialized,
                views_int,
                forwards_int,
                (str(m.get("media_group_id") or "").strip() or None),
            ))

            # если отдан путь к файлу — зафиксируем в files
            fp = m.get("file_path")
            if fp:
                fid = m.get("media_id") or f"{peer_id}:{mid}"
                f_rows.append((
                    str(fid),
                    str(fp),
                    int(m.get("file_size") or 0) or None,
                    str(m.get("mime") or "") or None,
                    None,
                    None,
                    self._now(),
                ))

        if m_rows:
            edit_snapshots: List[Tuple[Any, ...]] = []
            for row in m_rows:
                r_pid, r_mid = row[0], row[1]
                r_text = str(row[5] or "")
                r_media_type = str(row[6] or "text")
                r_from_id = row[3]
                try:
                    existing = self._query(
                        "SELECT message, media_type FROM messages WHERE peer_id=? AND id=?",
                        (int(r_pid), int(r_mid)),
                    )
                    if existing:
                        old_text = str(existing[0][0] or "")
                        old_media_type = str(existing[0][1] or "text")
                        if old_text != r_text or old_media_type != r_media_type:
                            edit_snapshots.append((int(r_pid), int(r_mid), int(self._now()), old_text, old_media_type, int(r_from_id or 0) if r_from_id else None))
                except Exception:
                    pass
            if edit_snapshots:
                self._execmany(
                    """
                    INSERT INTO edited_message_revisions(peer_id, message_id, edited_at, snapshot_text, media_type, sender_id)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    edit_snapshots,
                )

            self._execmany(
                """
                INSERT INTO messages(peer_id,id,date,from_id,reply_to,message,media_type,media_id,is_deleted,forward_info,file_name,entities,reply_markup,duration,waveform,reactions,poll,views,forwards,media_group_id)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(peer_id,id) DO UPDATE SET
                  date=excluded.date,
                  from_id=COALESCE(excluded.from_id, messages.from_id),
                  reply_to=COALESCE(excluded.reply_to, messages.reply_to),
                  message=excluded.message,
                  media_type=excluded.media_type,
                  media_id=COALESCE(excluded.media_id, messages.media_id),
                  is_deleted=CASE
                    WHEN messages.is_deleted=1 THEN 1
                    ELSE COALESCE(excluded.is_deleted, messages.is_deleted, 0)
                  END,
                  forward_info=COALESCE(excluded.forward_info, messages.forward_info),
                  file_name=COALESCE(excluded.file_name, messages.file_name),
                  entities=COALESCE(excluded.entities, messages.entities),
                  reply_markup=COALESCE(excluded.reply_markup, messages.reply_markup),
                  duration=COALESCE(excluded.duration, messages.duration),
                  waveform=COALESCE(excluded.waveform, messages.waveform),
                  reactions=COALESCE(excluded.reactions, messages.reactions),
                  poll=COALESCE(excluded.poll, messages.poll),
                  views=COALESCE(excluded.views, messages.views),
                  forwards=COALESCE(excluded.forwards, messages.forwards),
                  media_group_id=COALESCE(excluded.media_group_id, messages.media_group_id)
                """,
                m_rows,
            )
        if f_rows:
            self._execmany(
                """
                INSERT INTO files(file_id,path,size,mime,crc32,ttl,added_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(file_id) DO UPDATE SET
                  path=excluded.path,
                  size=COALESCE(excluded.size, files.size),
                  mime=COALESCE(excluded.mime, files.mime),
                  added_at=excluded.added_at
                """,
                f_rows,
            )

    def update_message_file_path(
        self,
        peer_id: int,
        message_id: int,
        *,
        path: str,
        size: Optional[int] = None,
        mime: Optional[str] = None,
    ) -> None:
        """Гарантированно сохраняет путь к файлу для сообщения (peer_id, message_id)."""
        self._exec(
            """
            INSERT INTO files(file_id,path,size,mime,crc32,ttl,added_at)
            SELECT
              COALESCE(media_id, CAST(peer_id AS TEXT) || ':' || CAST(id AS TEXT)),
              ?,
              ?,
              ?,
              NULL,
              NULL,
              ?
            FROM messages
            WHERE peer_id = ? AND id = ?
            """,
            (str(path), size, mime, self._now(), int(peer_id), int(message_id)),
        )

    def mark_messages_deleted(self, peer_id: int, message_ids: Iterable[int], deleted: bool = True) -> None:
        rows: List[Tuple[int, int, int]] = []
        flag = 1 if deleted else 0
        for mid in message_ids:
            try:
                rows.append((flag, int(peer_id), int(mid)))
            except Exception:
                continue
        if not rows:
            return
        self._execmany(
            "UPDATE messages SET is_deleted=? WHERE peer_id=? AND id=?",
            rows,
        )

    def log_deleted_messages(
        self,
        peer_id: int,
        message_ids: Iterable[int],
        *,
        deleted_at: Optional[int] = None,
        source: str = "telegram",
    ) -> None:
        deleted_ts = int(deleted_at or self._now())
        mids: List[int] = []
        seen: set[int] = set()
        for mid in message_ids:
            try:
                val = int(mid)
            except Exception:
                continue
            if val <= 0 or val in seen:
                continue
            seen.add(val)
            mids.append(val)
        if not mids:
            return
        snapshots = self.get_messages_by_ids(int(peer_id), mids)
        rows: List[Tuple[Any, ...]] = []
        for mid in mids:
            item = snapshots.get(mid, {})
            rows.append(
                (
                    int(peer_id),
                    mid,
                    deleted_ts,
                    str(item.get("text") or ""),
                    str(item.get("type") or "text"),
                    int(item.get("from_id")) if item.get("from_id") is not None else None,
                    str(source or "telegram"),
                )
            )
        if not rows:
            return
        self._execmany(
            """
            INSERT INTO deleted_message_events(peer_id,message_id,deleted_at,snapshot_text,media_type,sender_id,source)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(peer_id,message_id) DO UPDATE SET
              deleted_at=excluded.deleted_at,
              snapshot_text=CASE
                WHEN excluded.snapshot_text <> '' THEN excluded.snapshot_text
                ELSE deleted_message_events.snapshot_text
              END,
              media_type=COALESCE(excluded.media_type, deleted_message_events.media_type),
              sender_id=COALESCE(excluded.sender_id, deleted_message_events.sender_id),
              source=COALESCE(excluded.source, deleted_message_events.source)
            """,
            rows,
        )

    def log_edited_message_revision(
        self,
        peer_id: int,
        message_id: int,
        *,
        snapshot_text: str = "",
        media_type: str = "",
        sender_id: Optional[int] = None,
        edited_at: Optional[int] = None,
    ) -> None:
        ts = int(edited_at or self._now())
        self._exec(
            """
            INSERT INTO edited_message_revisions(peer_id, message_id, edited_at, snapshot_text, media_type, sender_id)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (int(peer_id), int(message_id), ts, str(snapshot_text or ""), str(media_type or "text"), sender_id),
        )

    def get_edited_message_revisions(
        self,
        peer_id: int,
        message_id: int,
        *,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT edited_at, snapshot_text, media_type, sender_id
            FROM edited_message_revisions
            WHERE peer_id = ? AND message_id = ?
            ORDER BY edited_at DESC
            LIMIT ?
            """,
            (int(peer_id), int(message_id), int(max(limit, 1))),
        )
        out: List[Dict[str, Any]] = []
        for edited_at, snapshot_text, media_type, sender_id in rows:
            out.append({
                "edited_at": int(edited_at or 0),
                "text": str(snapshot_text or ""),
                "media_type": str(media_type or "text"),
                "sender_id": int(sender_id or 0),
            })
        return out

    def purge_messages(self, peer_id: int, message_ids: Iterable[int]) -> None:
        rows: List[Tuple[int, int]] = []
        for mid in message_ids:
            try:
                rows.append((int(peer_id), int(mid)))
            except Exception:
                continue
        if not rows:
            return
        self._execmany("DELETE FROM messages WHERE peer_id=? AND id=?", rows)

    # --------------------------- queries for UI ---------------------------

    def get_dialogs_for_ui(self, limit: int = 400) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT p.id, p.type, p.username, p.title, p.photo_small, d.last_message_date, d.unread_count, d.pinned
            FROM dialogs d
            JOIN peers p ON p.id = d.peer_id
            ORDER BY COALESCE(d.last_message_date, 0) DESC, p.id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "id": str(r[0]),
                "type": r[1],
                "username": r[2],
                "title": r[3],
                "photo_small_id": r[4],
                "last_message_date": r[5],
                "unread_count": r[6],
                "pinned": bool(r[7]),
            })
        return out

    def prune_dialogs_to_snapshot(self, peer_ids: Iterable[int]) -> None:
        snapshot: List[int] = []
        seen: set[int] = set()
        for peer_id in list(peer_ids or []):
            try:
                pid = int(peer_id)
            except Exception:
                continue
            if pid in seen:
                continue
            seen.add(pid)
            snapshot.append(pid)
        if snapshot:
            placeholders = ",".join("?" for _ in snapshot)
            self._exec(
                f"DELETE FROM dialogs WHERE peer_id NOT IN ({placeholders})",
                tuple(snapshot),
            )
            return
        self._exec("DELETE FROM dialogs")

    def get_peer(self, peer_id: int) -> Dict[str, Any]:
        rows = self._query(
            """
            SELECT id, type, username, title, photo_small, photo_big
            FROM peers
            WHERE id = ?
            LIMIT 1
            """,
            (int(peer_id),),
        )
        if not rows:
            return {}
        row = rows[0]
        return {
            "id": int(row[0]),
            "type": str(row[1] or ""),
            "username": str(row[2] or ""),
            "title": str(row[3] or ""),
            "photo_small": row[4],
            "photo_big": row[5],
        }

    def get_peer_by_username(self, username: str) -> Dict[str, Any]:
        handle = str(username or "").strip().lstrip("@")
        if not handle:
            return {}
        rows = self._query(
            """
            SELECT id, type, username, title, photo_small, photo_big
            FROM peers
            WHERE lower(COALESCE(username, '')) = lower(?)
            LIMIT 1
            """,
            (handle,),
        )
        if not rows:
            return {}
        row = rows[0]
        return {
            "id": int(row[0]),
            "type": str(row[1] or ""),
            "username": str(row[2] or ""),
            "title": str(row[3] or ""),
            "photo_small": row[4],
            "photo_big": row[5],
        }

    def get_messages_for_ui(self, peer_id: int, limit: int = 80, *, include_deleted: bool = False) -> List[Dict[str, Any]]:
        where_deleted = "" if include_deleted else "AND COALESCE(m.is_deleted, 0) = 0"
        rows = self._query(
            f"""
            SELECT
              m.id,
              m.date,
              m.from_id,
              m.reply_to,
              m.message,
              m.media_type,
              m.media_id,
              m.is_deleted,
              m.forward_info,
              m.file_name,
              m.entities,
              m.reply_markup,
              f.path,
              f.size,
              f.mime,
              COALESCE(p.title, p.username, CAST(m.from_id AS TEXT)),
              m.duration,
              m.waveform,
              m.reactions,
              m.poll,
              m.views,
              m.forwards,
              m.media_group_id
            FROM messages m
            LEFT JOIN files f
              ON f.file_id = COALESCE(m.media_id, CAST(m.peer_id AS TEXT) || ':' || CAST(m.id AS TEXT))
            LEFT JOIN peers p
              ON p.id = m.from_id
            WHERE m.peer_id = ? {where_deleted}
            ORDER BY m.id DESC
            LIMIT ?
            """,
            (int(peer_id), int(limit)),
        )
        _UI_JSON_FIELDS = {"forward_info": 8, "entities": 10, "reply_markup": 11, "waveform": 17, "reactions": 18, "poll": 19}
        out: List[Dict[str, Any]] = []
        for r in rows:
            sender_id = r[2]
            parsed = _parse_message_json_fields(r, _UI_JSON_FIELDS)
            forward_info = parsed.get("forward_info")
            if forward_info is None and r[8]:
                forward_info = {"sender": r[8]}
            out.append({
                "id": int(r[0]),
                "date": int(r[1]),
                "from_id": sender_id,
                "sender_id": str(sender_id) if sender_id is not None else "",
                "reply_to": r[3],
                "text": r[4] or "",
                "type": r[5] or "text",
                "media_id": r[6],
                "is_deleted": bool(r[7]),
                "forward_info": forward_info,
                "file_name": r[9],
                "entities": parsed.get("entities"),
                "reply_markup": parsed.get("reply_markup"),
                "file_path": r[12],
                "file_size": r[13],
                "mime": r[14],
                "sender": r[15] or (str(sender_id) if sender_id is not None else ""),
                "thumb_path": None,
                "duration": r[16],
                "waveform": parsed.get("waveform"),
                "reactions": parsed.get("reactions"),
                "poll": parsed.get("poll"),
                "views": r[20],
                "forwards": r[21],
                "media_group_id": r[22],
            })
        return out

    def get_message_by_id(self, peer_id: int, message_id: int) -> Optional[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT
              m.id,
              m.from_id,
              m.reply_to,
              m.message,
              m.media_type,
              m.is_deleted,
              m.forward_info,
              m.file_name,
              m.entities,
              m.reply_markup,
              COALESCE(p.title, p.username, CAST(m.from_id AS TEXT)),
              m.duration,
              m.waveform,
              m.reactions,
              m.poll,
              m.views,
              m.forwards,
              m.media_group_id
            FROM messages m
            LEFT JOIN peers p
              ON p.id = m.from_id
            WHERE m.peer_id = ? AND m.id = ?
            LIMIT 1
            """,
            (int(peer_id), int(message_id)),
        )
        _BYID_JSON_FIELDS = {"forward_info": 6, "entities": 8, "reply_markup": 9, "waveform": 12, "reactions": 13, "poll": 14}
        if not rows:
            return None
        r = rows[0]
        parsed = _parse_message_json_fields(r, _BYID_JSON_FIELDS)
        forward_info = parsed.get("forward_info")
        if forward_info is None and r[6]:
            forward_info = {"sender": r[6]}
        return {
            "id": int(r[0]),
            "from_id": r[1],
            "reply_to": r[2],
            "text": r[3] or "",
            "type": r[4] or "text",
            "is_deleted": bool(r[5]),
            "forward_info": forward_info,
            "file_name": r[7],
            "entities": parsed.get("entities"),
            "reply_markup": parsed.get("reply_markup"),
            "sender": r[10] or (str(r[1]) if r[1] is not None else ""),
            "duration": r[11],
            "waveform": parsed.get("waveform"),
            "reactions": parsed.get("reactions"),
            "poll": parsed.get("poll"),
            "views": r[15],
            "forwards": r[16],
            "media_group_id": r[17],
        }

    def get_messages_by_ids(self, peer_id: int, message_ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
        mids: List[int] = []
        seen: set[int] = set()
        for mid in message_ids:
            try:
                val = int(mid)
            except Exception:
                continue
            if val <= 0 or val in seen:
                continue
            seen.add(val)
            mids.append(val)
        if not mids:
            return {}
        out: Dict[int, Dict[str, Any]] = {}
        chunk_size = 400
        for idx in range(0, len(mids), chunk_size):
            chunk = mids[idx : idx + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            rows = self._query(
                f"""
                SELECT
                  m.id,
                  m.from_id,
                  m.reply_to,
                  m.message,
                  m.media_type,
                  m.is_deleted,
                  m.forward_info,
                  m.file_name,
                  m.entities,
                  m.reply_markup,
                  COALESCE(p.title, p.username, CAST(m.from_id AS TEXT)),
                  m.duration,
                  m.waveform,
                  m.reactions,
                  m.poll,
                  m.views,
                  m.forwards,
                  m.media_group_id
                FROM messages m
                LEFT JOIN peers p
                  ON p.id = m.from_id
                WHERE m.peer_id = ? AND m.id IN ({placeholders})
                """,
                (int(peer_id), *chunk),
            )
            _IDS_JSON_FIELDS = {"forward_info": 6, "entities": 8, "reply_markup": 9, "waveform": 12, "reactions": 13, "poll": 14}
            for row in rows:
                try:
                    mid = int(row[0])
                except Exception:
                    continue
                try:
                    parsed = _parse_message_json_fields(row, _IDS_JSON_FIELDS)
                    forward_info = parsed.get("forward_info")
                    if forward_info is None and row[6]:
                        forward_info = {"sender": row[6]}
                    out[mid] = {
                        "id": mid,
                        "from_id": row[1],
                        "reply_to": row[2],
                        "text": row[3] or "",
                        "type": row[4] or "text",
                        "is_deleted": bool(row[5]),
                        "forward_info": forward_info,
                        "file_name": row[7],
                        "entities": parsed.get("entities"),
                        "reply_markup": parsed.get("reply_markup"),
                        "sender": row[10] or (str(row[1]) if row[1] is not None else ""),
                        "duration": row[11],
                        "waveform": parsed.get("waveform"),
                        "reactions": parsed.get("reactions"),
                        "poll": parsed.get("poll"),
                        "views": row[15],
                        "forwards": row[16],
                        "media_group_id": row[17],
                    }
                except Exception:
                    continue
        return out

    def find_peers_for_message_ids(self, message_ids: Iterable[int]) -> Dict[int, List[int]]:
        mids: List[int] = []
        seen: set[int] = set()
        for mid in message_ids:
            try:
                val = int(mid)
            except Exception:
                continue
            if val <= 0 or val in seen:
                continue
            seen.add(val)
            mids.append(val)
        if not mids:
            return {}
        out: Dict[int, List[int]] = {}
        chunk_size = 400
        for idx in range(0, len(mids), chunk_size):
            chunk = mids[idx : idx + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            rows = self._query(
                f"""
                SELECT peer_id, id
                FROM messages
                WHERE id IN ({placeholders}) AND COALESCE(is_deleted, 0) = 0
                """,
                tuple(chunk),
            )
            for peer_id, msg_id in rows:
                try:
                    pid = int(peer_id)
                    mid = int(msg_id)
                except Exception:
                    continue
                out.setdefault(pid, []).append(mid)
        return out

    def get_recent_emojis(self, *, limit: int = 48, sender_id: Optional[int] = None) -> List[str]:
        params: List[Any] = []
        sender_sql = ""
        if sender_id is not None:
            sender_sql = "AND COALESCE(from_id, 0) = ?"
            params.append(int(sender_id))
        params.append(int(max(limit * 40, 200)))
        rows = self._query(
            f"""
            SELECT message
            FROM messages
            WHERE COALESCE(TRIM(message), '') <> '' {sender_sql}
            ORDER BY date DESC, id DESC
            LIMIT ?
            """,
            tuple(params),
        )
        ordered: List[str] = []
        seen: set[str] = set()
        for (message_text,) in rows:
            for emoji in _iter_emoji_sequences(str(message_text or "")):
                if emoji in seen:
                    continue
                seen.add(emoji)
                ordered.append(emoji)
                if len(ordered) >= int(limit):
                    return ordered
        return ordered

    def get_recent_custom_emoji_ids(self, *, limit: int = 48, sender_id: Optional[int] = None) -> List[int]:
        params: List[Any] = []
        sender_sql = ""
        if sender_id is not None:
            sender_sql = "AND COALESCE(from_id, 0) = ?"
            params.append(int(sender_id))
        params.append(int(max(limit * 40, 200)))
        rows = self._query(
            f"""
            SELECT entities
            FROM messages
            WHERE COALESCE(TRIM(entities), '') <> '' {sender_sql}
            ORDER BY date DESC, id DESC
            LIMIT ?
            """,
            tuple(params),
        )
        ordered: List[int] = []
        seen: set[int] = set()
        for (entities_raw,) in rows:
            try:
                entities = json.loads(str(entities_raw or "[]"))
            except Exception:
                entities = []
            if not isinstance(entities, list):
                continue
            for entity in entities:
                if not isinstance(entity, dict):
                    continue
                if str(entity.get("type") or "").strip().lower() != "custom_emoji":
                    continue
                try:
                    custom_id = int(entity.get("custom_emoji_id") or 0)
                except Exception:
                    custom_id = 0
                if custom_id <= 0 or custom_id in seen:
                    continue
                seen.add(custom_id)
                ordered.append(custom_id)
                if len(ordered) >= int(limit):
                    return ordered
        return ordered

    def get_chat_shared_media(self, peer_id: int, *, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT
              m.id,
              m.date,
              COALESCE(m.media_type, 'text'),
              COALESCE(m.message, ''),
              COALESCE(m.file_name, ''),
              COALESCE(f.size, 0),
              COALESCE(f.mime, ''),
              COALESCE(f.path, ''),
              COALESCE(m.from_id, 0)
            FROM messages m
            LEFT JOIN files f
              ON f.file_id = COALESCE(m.media_id, CAST(m.peer_id AS TEXT) || ':' || CAST(m.id AS TEXT))
            WHERE m.peer_id = ?
              AND COALESCE(m.media_type, 'text') IN ('image','photo','video','animation','gif','video_note','sticker')
            ORDER BY COALESCE(m.date, 0) DESC, m.id DESC
            LIMIT ?
            """,
            (int(peer_id), int(max(limit, 1))),
        )
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            file_path = str(row[7] or "")
            file_name = str(row[4] or "")
            file_size = int(row[5] or 0)
            if file_path.strip():
                key = f"path:{file_path.strip().lower()}"
            elif file_name.strip():
                key = f"name:{file_name.strip().lower()}|{file_size}"
            else:
                key = f"mid:{int(row[0])}"
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "id": int(row[0]),
                    "date": int(row[1] or 0),
                    "type": str(row[2] or "media"),
                    "text": str(row[3] or ""),
                    "file_name": file_name,
                    "file_size": file_size,
                    "mime": str(row[6] or ""),
                    "file_path": file_path,
                    "from_id": int(row[8] or 0),
                }
            )
        return out

    def get_chat_shared_files(self, peer_id: int, *, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT
              m.id,
              m.date,
              COALESCE(m.media_type, 'text'),
              COALESCE(m.message, ''),
              COALESCE(m.file_name, ''),
              COALESCE(f.size, 0),
              COALESCE(f.mime, ''),
              COALESCE(f.path, ''),
              COALESCE(m.from_id, 0)
            FROM messages m
            LEFT JOIN files f
              ON f.file_id = COALESCE(m.media_id, CAST(m.peer_id AS TEXT) || ':' || CAST(m.id AS TEXT))
            WHERE m.peer_id = ?
              AND (
                COALESCE(m.media_type, 'text') IN ('document','audio','voice')
                OR COALESCE(TRIM(m.file_name), '') <> ''
              )
            ORDER BY COALESCE(m.date, 0) DESC, m.id DESC
            LIMIT ?
            """,
            (int(peer_id), int(max(limit, 1))),
        )
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            file_path = str(row[7] or "")
            file_name = str(row[4] or "")
            file_size = int(row[5] or 0)
            if file_path.strip():
                key = f"path:{file_path.strip().lower()}"
            elif file_name.strip():
                key = f"name:{file_name.strip().lower()}|{file_size}"
            else:
                key = f"mid:{int(row[0])}"
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "id": int(row[0]),
                    "date": int(row[1] or 0),
                    "type": str(row[2] or "file"),
                    "text": str(row[3] or ""),
                    "file_name": file_name,
                    "file_size": file_size,
                    "mime": str(row[6] or ""),
                    "file_path": file_path,
                    "from_id": int(row[8] or 0),
                }
            )
        return out

    def get_chat_shared_voice(self, peer_id: int, *, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT
              m.id,
              m.date,
              COALESCE(m.media_type, 'text'),
              COALESCE(m.message, ''),
              COALESCE(m.file_name, ''),
              COALESCE(f.size, 0),
              COALESCE(f.mime, ''),
              COALESCE(f.path, ''),
              COALESCE(m.from_id, 0)
            FROM messages m
            LEFT JOIN files f
              ON f.file_id = COALESCE(m.media_id, CAST(m.peer_id AS TEXT) || ':' || CAST(m.id AS TEXT))
            WHERE m.peer_id = ?
              AND COALESCE(m.media_type, 'text') = 'voice'
            ORDER BY COALESCE(m.date, 0) DESC, m.id DESC
            LIMIT ?
            """,
            (int(peer_id), int(max(limit, 1))),
        )
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            file_path = str(row[7] or "")
            key = f"path:{file_path.strip().lower()}" if file_path.strip() else f"mid:{int(row[0])}"
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "id": int(row[0]), "date": int(row[1] or 0), "type": str(row[2] or "voice"),
                "text": str(row[3] or ""), "file_name": str(row[4] or ""),
                "file_size": int(row[5] or 0), "mime": str(row[6] or ""),
                "file_path": file_path, "from_id": int(row[8] or 0),
            })
        return out

    def get_chat_shared_music(self, peer_id: int, *, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT
              m.id,
              m.date,
              COALESCE(m.media_type, 'text'),
              COALESCE(m.message, ''),
              COALESCE(m.file_name, ''),
              COALESCE(f.size, 0),
              COALESCE(f.mime, ''),
              COALESCE(f.path, ''),
              COALESCE(m.from_id, 0)
            FROM messages m
            LEFT JOIN files f
              ON f.file_id = COALESCE(m.media_id, CAST(m.peer_id AS TEXT) || ':' || CAST(m.id AS TEXT))
            WHERE m.peer_id = ?
              AND COALESCE(m.media_type, 'text') = 'audio'
            ORDER BY COALESCE(m.date, 0) DESC, m.id DESC
            LIMIT ?
            """,
            (int(peer_id), int(max(limit, 1))),
        )
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            file_path = str(row[7] or "")
            file_name = str(row[4] or "")
            file_size = int(row[5] or 0)
            key = f"path:{file_path.strip().lower()}" if file_path.strip() else (f"name:{file_name.strip().lower()}|{file_size}" if file_name.strip() else f"mid:{int(row[0])}")
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "id": int(row[0]), "date": int(row[1] or 0), "type": str(row[2] or "audio"),
                "text": str(row[3] or ""), "file_name": file_name,
                "file_size": file_size, "mime": str(row[6] or ""),
                "file_path": file_path, "from_id": int(row[8] or 0),
            })
        return out

    def get_chat_shared_gifs(self, peer_id: int, *, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT
              m.id,
              m.date,
              COALESCE(m.media_type, 'text'),
              COALESCE(m.message, ''),
              COALESCE(m.file_name, ''),
              COALESCE(f.size, 0),
              COALESCE(f.mime, ''),
              COALESCE(f.path, ''),
              COALESCE(m.from_id, 0)
            FROM messages m
            LEFT JOIN files f
              ON f.file_id = COALESCE(m.media_id, CAST(m.peer_id AS TEXT) || ':' || CAST(m.id AS TEXT))
            WHERE m.peer_id = ?
              AND COALESCE(m.media_type, 'text') IN ('animation', 'gif')
            ORDER BY COALESCE(m.date, 0) DESC, m.id DESC
            LIMIT ?
            """,
            (int(peer_id), int(max(limit, 1))),
        )
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            file_path = str(row[7] or "")
            key = f"path:{file_path.strip().lower()}" if file_path.strip() else f"mid:{int(row[0])}"
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "id": int(row[0]), "date": int(row[1] or 0), "type": str(row[2] or "gif"),
                "text": str(row[3] or ""), "file_name": str(row[4] or ""),
                "file_size": int(row[5] or 0), "mime": str(row[6] or ""),
                "file_path": file_path, "from_id": int(row[8] or 0),
            })
        return out

    def get_chat_links(self, peer_id: int, *, limit: int = 150) -> List[Dict[str, Any]]:
        sample_rows = self._query(
            """
            SELECT
              id,
              date,
              COALESCE(message, ''),
              COALESCE(from_id, 0)
            FROM messages
            WHERE peer_id = ?
              AND COALESCE(TRIM(message), '') <> ''
            ORDER BY COALESCE(date, 0) DESC, id DESC
            LIMIT ?
            """,
            (int(peer_id), int(max(limit * 8, 300))),
        )
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for msg_id, msg_date, text, from_id in sample_rows:
            body = str(text or "")
            if not body:
                continue
            for match in URL_RE.finditer(body):
                raw = str(match.group(0) or "").strip()
                if not raw:
                    continue
                url = _normalize_url(raw)
                if not url:
                    continue
                key = url.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    {
                        "id": int(msg_id),
                        "date": int(msg_date or 0),
                        "url": url,
                        "raw": raw,
                        "context": body[:280],
                        "from_id": int(from_id or 0),
                    }
                )
                if len(out) >= int(limit):
                    return out
        return out

    def get_chat_latest_message_id(self, peer_id: int) -> int:
        rows = self._query(
            """
            SELECT COALESCE(MAX(id), 0)
            FROM messages
            WHERE peer_id = ?
            """,
            (int(peer_id),),
        )
        if not rows:
            return 0
        try:
            return int(rows[0][0] or 0)
        except Exception:
            return 0

    def get_chat_message_count(self, peer_id: int) -> int:
        rows = self._query(
            """
            SELECT COUNT(*)
            FROM messages
            WHERE peer_id = ?
            """,
            (int(peer_id),),
        )
        if not rows:
            return 0
        try:
            return int(rows[0][0] or 0)
        except Exception:
            return 0

    def get_chat_profile_scan_state(self, peer_id: int) -> Dict[str, int]:
        rows = self._query(
            """
            SELECT last_message_id, updated_at
            FROM chat_profile_scan_state
            WHERE peer_id = ?
            LIMIT 1
            """,
            (int(peer_id),),
        )
        if not rows:
            return {"last_message_id": 0, "updated_at": 0}
        row = rows[0]
        try:
            last_message_id = int(row[0] or 0)
        except Exception:
            last_message_id = 0
        try:
            updated_at = int(row[1] or 0)
        except Exception:
            updated_at = 0
        return {"last_message_id": last_message_id, "updated_at": updated_at}

    def _set_chat_profile_scan_state(self, peer_id: int, *, last_message_id: int) -> None:
        ts = self._now()
        self._exec(
            """
            INSERT INTO chat_profile_scan_state(peer_id,last_message_id,updated_at)
            VALUES(?,?,?)
            ON CONFLICT(peer_id) DO UPDATE SET
              last_message_id=excluded.last_message_id,
              updated_at=excluded.updated_at
            """,
            (int(peer_id), int(last_message_id), ts),
        )

    def _upsert_chat_profile_section_rows(
        self,
        peer_id: int,
        section: str,
        rows: List[Tuple[str, int, int, str]],
    ) -> None:
        if not rows:
            return
        ts = self._now()
        data: List[Tuple[Any, ...]] = []
        for dedupe_key, message_id, message_date, payload in rows:
            key = str(dedupe_key or "").strip()
            if not key:
                continue
            data.append(
                (
                    int(peer_id),
                    str(section or ""),
                    key,
                    int(message_id or 0),
                    int(message_date or 0),
                    str(payload or "{}"),
                    ts,
                )
            )
        if not data:
            return
        self._execmany(
            """
            INSERT INTO chat_profile_sections_cache(
              peer_id,section,dedupe_key,message_id,message_date,payload,updated_at
            )
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(peer_id,section,dedupe_key) DO UPDATE SET
              message_id=CASE
                WHEN excluded.message_id >= chat_profile_sections_cache.message_id THEN excluded.message_id
                ELSE chat_profile_sections_cache.message_id
              END,
              message_date=CASE
                WHEN excluded.message_id >= chat_profile_sections_cache.message_id THEN excluded.message_date
                ELSE chat_profile_sections_cache.message_date
              END,
              payload=CASE
                WHEN excluded.message_id >= chat_profile_sections_cache.message_id THEN excluded.payload
                ELSE chat_profile_sections_cache.payload
              END,
              updated_at=excluded.updated_at
            """,
            data,
        )

    def get_cached_chat_profile_section(
        self,
        peer_id: int,
        section: str,
        *,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT message_id, message_date, payload
            FROM chat_profile_sections_cache
            WHERE peer_id = ? AND section = ?
            ORDER BY message_date DESC, message_id DESC
            LIMIT ?
            """,
            (int(peer_id), str(section or ""), int(max(limit, 1))),
        )
        out: List[Dict[str, Any]] = []
        for message_id, message_date, payload_raw in rows:
            payload: Dict[str, Any]
            try:
                payload = json.loads(str(payload_raw or "{}"))
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}
            payload["id"] = int(message_id or payload.get("id") or 0)
            payload["date"] = int(message_date or payload.get("date") or 0)
            out.append(payload)
        return out

    def refresh_chat_profile_sections_cache(
        self,
        peer_id: int,
        *,
        chunk_size: int = 400,
        full_scan: bool = False,
    ) -> Dict[str, int]:
        pid = int(peer_id)
        scan_state = self.get_chat_profile_scan_state(pid)
        latest_before = self.get_chat_latest_message_id(pid)
        if latest_before <= 0:
            if full_scan:
                self._exec(
                    "DELETE FROM chat_profile_sections_cache WHERE peer_id = ?",
                    (pid,),
                )
                self._set_chat_profile_scan_state(pid, last_message_id=0)
            return {"processed": 0, "last_message_id": 0, "latest_message_id": 0}

        start_id = 0 if bool(full_scan) else int(scan_state.get("last_message_id") or 0)
        if bool(full_scan):
            self._exec(
                "DELETE FROM chat_profile_sections_cache WHERE peer_id = ?",
                (pid,),
            )

        processed = 0
        last_id = int(start_id)
        step = max(80, int(chunk_size or 0))

        while True:
            rows = self._query(
                """
                SELECT
                  m.id,
                  COALESCE(m.date, 0),
                  COALESCE(m.message, ''),
                  COALESCE(m.media_type, 'text'),
                  COALESCE(m.file_name, ''),
                  COALESCE(m.from_id, 0),
                  COALESCE(m.media_group_id, ''),
                  COALESCE(f.size, 0),
                  COALESCE(f.mime, ''),
                  COALESCE(f.path, '')
                FROM messages m
                LEFT JOIN files f
                  ON f.file_id = COALESCE(m.media_id, CAST(m.peer_id AS TEXT) || ':' || CAST(m.id AS TEXT))
                WHERE m.peer_id = ? AND m.id > ?
                ORDER BY m.id ASC
                LIMIT ?
                """,
                (pid, int(last_id), step),
            )
            if not rows:
                break

            media_batch: List[Tuple[str, int, int, str]] = []
            files_batch: List[Tuple[str, int, int, str]] = []
            links_batch: List[Tuple[str, int, int, str]] = []

            for row in rows:
                mid = int(row[0] or 0)
                if mid <= 0:
                    continue
                msg_date = int(row[1] or 0)
                text = str(row[2] or "")
                media_type_raw = str(row[3] or "text").strip().lower()
                media_type = {"photo": "image", "gif": "animation"}.get(media_type_raw, media_type_raw)
                file_name = str(row[4] or "")
                from_id = int(row[5] or 0)
                media_group_id = str(row[6] or "")
                file_size = int(row[7] or 0)
                mime = str(row[8] or "")
                file_path = str(row[9] or "")
                file_path_norm = file_path.strip().lower()

                if media_type in PROFILE_MEDIA_TYPES:
                    media_payload = {
                        "id": mid,
                        "date": msg_date,
                        "type": media_type or "media",
                        "text": text,
                        "file_name": file_name,
                        "file_size": file_size,
                        "mime": mime,
                        "file_path": file_path,
                        "from_id": from_id,
                        "media_group_id": media_group_id or None,
                    }
                    if file_path_norm:
                        media_key = f"path:{file_path_norm}"
                    elif file_name:
                        media_key = f"name:{file_name.strip().lower()}|{file_size}"
                    else:
                        media_key = f"mid:{mid}"
                    media_batch.append((media_key, mid, msg_date, json.dumps(media_payload, ensure_ascii=False)))

                if media_type in PROFILE_FILE_TYPES or bool(file_name.strip()):
                    file_payload = {
                        "id": mid,
                        "date": msg_date,
                        "type": media_type or "file",
                        "text": text,
                        "file_name": file_name,
                        "file_size": file_size,
                        "mime": mime,
                        "file_path": file_path,
                        "from_id": from_id,
                    }
                    if file_path_norm:
                        file_key = f"path:{file_path_norm}"
                    elif file_name:
                        file_key = f"name:{file_name.strip().lower()}|{file_size}"
                    else:
                        file_key = f"mid:{mid}"
                    files_batch.append((file_key, mid, msg_date, json.dumps(file_payload, ensure_ascii=False)))

                if text:
                    for match in URL_RE.finditer(text):
                        raw = str(match.group(0) or "").strip()
                        if not raw:
                            continue
                        url = _normalize_url(raw)
                        if not url:
                            continue
                        link_payload = {
                            "id": mid,
                            "date": msg_date,
                            "url": url,
                            "raw": raw,
                            "context": text[:320],
                            "from_id": from_id,
                        }
                        links_batch.append((url.lower(), mid, msg_date, json.dumps(link_payload, ensure_ascii=False)))

            self._upsert_chat_profile_section_rows(pid, "media", media_batch)
            self._upsert_chat_profile_section_rows(pid, "files", files_batch)
            self._upsert_chat_profile_section_rows(pid, "links", links_batch)

            processed += len(rows)
            last_id = int(rows[-1][0] or last_id)
            self._set_chat_profile_scan_state(pid, last_message_id=last_id)

            if len(rows) < step:
                break

        latest_after = self.get_chat_latest_message_id(pid)
        if latest_after > last_id:
            self._set_chat_profile_scan_state(pid, last_message_id=latest_after)
            last_id = latest_after
        return {
            "processed": int(processed),
            "last_message_id": int(last_id),
            "latest_message_id": int(latest_after),
        }

    def get_chat_members_activity(self, peer_id: int, *, limit: int = 80) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT
              COALESCE(m.from_id, 0),
              COUNT(1),
              MAX(COALESCE(m.date, 0)),
              SUM(CASE WHEN COALESCE(m.is_deleted, 0) = 1 THEN 1 ELSE 0 END),
              COALESCE(p.title, ''),
              COALESCE(p.username, ''),
              COALESCE(p.type, '')
            FROM messages m
            LEFT JOIN peers p
              ON p.id = m.from_id
            WHERE m.peer_id = ? AND COALESCE(m.from_id, 0) <> 0
            GROUP BY COALESCE(m.from_id, 0)
            ORDER BY COUNT(1) DESC, MAX(COALESCE(m.date, 0)) DESC
            LIMIT ?
            """,
            (int(peer_id), int(max(limit, 1))),
        )
        out: List[Dict[str, Any]] = []
        for from_id, count, last_date, deleted_count, title, username, ptype in rows:
            try:
                sender_id = int(from_id or 0)
            except Exception:
                sender_id = 0
            if sender_id == 0:
                continue
            out.append(
                {
                    "id": sender_id,
                    "name": str(title or username or sender_id),
                    "username": str(username or ""),
                    "type": str(ptype or ""),
                    "messages": int(count or 0),
                    "last_date": int(last_date or 0),
                    "deleted_messages": int(deleted_count or 0),
                }
            )
        return out

    def _chat_statistics_source_sql(self, peer_id: int, *, limit: int) -> Tuple[str, Tuple[Any, ...]]:
        pid = int(peer_id)
        if int(limit or 0) > 0:
            return (
                """
                (
                    SELECT
                      id,
                      COALESCE(date, 0) AS date,
                      COALESCE(message, '') AS message,
                      COALESCE(media_type, 'text') AS media_type,
                      COALESCE(is_deleted, 0) AS is_deleted,
                      COALESCE(reactions, '') AS reactions,
                      COALESCE(poll, '') AS poll,
                      COALESCE(views, 0) AS views,
                      COALESCE(forwards, 0) AS forwards,
                      COALESCE(from_id, 0) AS from_id
                    FROM messages
                    WHERE peer_id = ?
                    ORDER BY id DESC
                    LIMIT ?
                )
                """,
                (pid, int(max(limit, 1))),
            )
        return (
            """
            (
                SELECT
                  id,
                  COALESCE(date, 0) AS date,
                  COALESCE(message, '') AS message,
                  COALESCE(media_type, 'text') AS media_type,
                  COALESCE(is_deleted, 0) AS is_deleted,
                  COALESCE(reactions, '') AS reactions,
                  COALESCE(poll, '') AS poll,
                  COALESCE(views, 0) AS views,
                  COALESCE(forwards, 0) AS forwards,
                  COALESCE(from_id, 0) AS from_id
                FROM messages
                WHERE peer_id = ?
            )
            """,
            (pid,),
        )

    def get_chat_statistics(
        self,
        peer_id: int,
        *,
        limit: int = 500,
        sender_limit: int = 24,
        daily_limit: int = 30,
        include_sender_daily: bool = False,
    ) -> Dict[str, Any]:
        source_sql, source_params = self._chat_statistics_source_sql(int(peer_id), limit=int(limit or 0))

        _raw_sql, _raw_params = source_sql, source_params
        with _stats_src_seq_lock:
            _seq_val = int(getattr(Storage, '_stats_src_seq', 0)) + 1
            Storage._stats_src_seq = _seq_val
        _src_tbl = f'_st{_seq_val}'
        try:
            self._exec(f'DROP TABLE IF EXISTS {_src_tbl}')
            self._exec(f'CREATE TEMP TABLE {_src_tbl} AS SELECT * FROM {_raw_sql}', _raw_params)
            if int(limit or 0) <= 0 or int(limit or 0) > 2000:
                self._exec(f'CREATE INDEX _ix_{_src_tbl}_from ON {_src_tbl}(from_id)')
                self._exec(f'CREATE INDEX _ix_{_src_tbl}_date ON {_src_tbl}(date)')
            source_sql = _src_tbl
            source_params = ()
        except Exception:
            try:
                self._exec(f'DROP TABLE IF EXISTS {_src_tbl}')
            except Exception:
                pass
            source_sql, source_params = _raw_sql, _raw_params

        summary_rows = self._query(
            f"""
            SELECT
              COUNT(1),
              SUM(CASE WHEN COALESCE(m.media_type, 'text') <> 'text' THEN 1 ELSE 0 END),
              SUM(CASE WHEN COALESCE(m.is_deleted, 0) = 1 THEN 1 ELSE 0 END),
              SUM(COALESCE(m.views, 0)),
              SUM(COALESCE(m.forwards, 0)),
              MIN(CASE WHEN COALESCE(m.date, 0) > 0 THEN m.date END),
              MAX(CASE WHEN COALESCE(m.date, 0) > 0 THEN m.date END)
            FROM {source_sql} AS m
            """,
            source_params,
        )
        summary = summary_rows[0] if summary_rows else (0, 0, 0, 0, 0, 0, 0)
        total_messages = int(summary[0] or 0)
        media_messages = int(summary[1] or 0)
        deleted_messages = int(summary[2] or 0)
        total_views = int(summary[3] or 0)
        total_forwards = int(summary[4] or 0)
        first_message_date = int(summary[5] or 0)
        last_message_date = int(summary[6] or 0)

        hourly_rows = self._query(
            f"""
            SELECT
              CAST(strftime('%H', datetime(m.date, 'unixepoch', 'localtime')) AS INTEGER) AS hour_slot,
              COUNT(1)
            FROM {source_sql} AS m
            WHERE COALESCE(m.date, 0) > 0
            GROUP BY hour_slot
            ORDER BY hour_slot ASC
            """,
            source_params,
        )
        hourly_counter = {int(hour or 0): int(count or 0) for hour, count in hourly_rows}
        hourly_activity = [{"hour": hour, "count": int(hourly_counter.get(hour, 0))} for hour in range(24)]

        if int(daily_limit or 0) > 0:
            daily_rows = self._query(
                f"""
                SELECT day_slot, count_value
                FROM (
                    SELECT
                      strftime('%Y-%m-%d', datetime(m.date, 'unixepoch', 'localtime')) AS day_slot,
                      COUNT(1) AS count_value
                    FROM {source_sql} AS m
                    WHERE COALESCE(m.date, 0) > 0
                    GROUP BY day_slot
                    ORDER BY day_slot DESC
                    LIMIT ?
                )
                ORDER BY day_slot ASC
                """,
                (*source_params, int(max(daily_limit, 1))),
            )
        else:
            daily_rows = self._query(
                f"""
                SELECT
                  strftime('%Y-%m-%d', datetime(m.date, 'unixepoch', 'localtime')) AS day_slot,
                  COUNT(1) AS count_value
                FROM {source_sql} AS m
                WHERE COALESCE(m.date, 0) > 0
                GROUP BY day_slot
                ORDER BY day_slot ASC
                """,
                source_params,
            )
        daily_activity = [
            {"day": str(day or ""), "count": int(count or 0)}
            for day, count in daily_rows
            if str(day or "").strip()
        ]

        sender_rows = self._query(
            f"""
            SELECT
              COALESCE(m.from_id, 0),
              COUNT(1),
              MAX(COALESCE(m.date, 0)),
              SUM(CASE WHEN COALESCE(m.is_deleted, 0) = 1 THEN 1 ELSE 0 END),
              SUM(CASE WHEN COALESCE(m.media_type, 'text') <> 'text' THEN 1 ELSE 0 END),
              SUM(COALESCE(m.views, 0)),
              SUM(COALESCE(m.forwards, 0)),
              COALESCE(p.title, ''),
              COALESCE(p.username, ''),
              COALESCE(p.type, ''),
              COALESCE(p.photo_small, ''),
              COALESCE(p.photo_big, '')
            FROM {source_sql} AS m
            LEFT JOIN peers p
              ON p.id = m.from_id
            WHERE COALESCE(m.from_id, 0) <> 0
            GROUP BY COALESCE(m.from_id, 0)
            ORDER BY COUNT(1) DESC, MAX(COALESCE(m.date, 0)) DESC
            """,
            source_params,
        )

        sender_details_full: List[Dict[str, Any]] = []
        sender_lookup_full: Dict[int, Dict[str, Any]] = {}
        denominator = max(1, int(total_messages or 0))
        for sender_id, count, last_date, deleted_count, media_count, sender_views, sender_forwards, title, username, ptype, photo_small, photo_big in sender_rows:
            try:
                sid = int(sender_id or 0)
            except Exception:
                sid = 0
            if sid <= 0:
                continue
            messages_count = int(count or 0)
            sender_name = str(title or username or sid)
            sender_username = str(username or "")
            sender_type = str(ptype or "")
            share = float(messages_count) / float(denominator)
            row = {
                "sender_id": sid,
                "name": sender_name,
                "username": sender_username,
                "type": sender_type,
                "count": messages_count,
                "share": round(share, 4),
                "last_date": int(last_date or 0),
                "deleted_messages": int(deleted_count or 0),
                "media_messages": int(media_count or 0),
                "total_views": int(sender_views or 0),
                "total_forwards": int(sender_forwards or 0),
                "photo_small_id": str(photo_small or "").strip(),
                "photo_big_id": str(photo_big or "").strip(),
            }
            sender_details_full.append(row)
            sender_lookup_full[sid] = row

        sender_time_summary_rows = self._query(
            f"""
            SELECT
              day_agg.sender_id,
              day_agg.active_days,
              day_agg.peak_day_messages,
              hour_agg.off_hours_messages,
              hour_agg.peak_hour_messages
            FROM (
              SELECT sender_id, COUNT(1) AS active_days, MAX(day_count) AS peak_day_messages
              FROM (
                SELECT
                  COALESCE(m.from_id, 0) AS sender_id,
                  strftime('%Y-%m-%d', datetime(m.date, 'unixepoch', 'localtime')) AS day_slot,
                  COUNT(1) AS day_count
                FROM {source_sql} AS m
                WHERE COALESCE(m.from_id, 0) <> 0
                  AND COALESCE(m.date, 0) > 0
                GROUP BY sender_id, day_slot
              ) AS sender_days
              GROUP BY sender_id
            ) AS day_agg
            JOIN (
              SELECT
                sender_id,
                SUM(CASE WHEN hour_slot BETWEEN 0 AND 5 THEN hour_count ELSE 0 END) AS off_hours_messages,
                MAX(hour_count) AS peak_hour_messages
              FROM (
                SELECT
                  COALESCE(m.from_id, 0) AS sender_id,
                  CAST(strftime('%H', datetime(m.date, 'unixepoch', 'localtime')) AS INTEGER) AS hour_slot,
                  COUNT(1) AS hour_count
                FROM {source_sql} AS m
                WHERE COALESCE(m.from_id, 0) <> 0
                  AND COALESCE(m.date, 0) > 0
                GROUP BY sender_id, hour_slot
              ) AS sender_hours
              GROUP BY sender_id
            ) AS hour_agg ON hour_agg.sender_id = day_agg.sender_id
            """,
            source_params,
        )
        sender_day_summary: Dict[int, Dict[str, int]] = {}
        sender_hour_summary: Dict[int, Dict[str, int]] = {}
        for sender_id, active_days, peak_day_messages, off_hours_messages, peak_hour_messages in sender_time_summary_rows:
            try:
                sid = int(sender_id or 0)
            except Exception:
                sid = 0
            if sid <= 0:
                continue
            sender_day_summary[sid] = {
                "active_days": int(active_days or 0),
                "peak_day_messages": int(peak_day_messages or 0),
            }
            sender_hour_summary[sid] = {
                "off_hours_messages": int(off_hours_messages or 0),
                "peak_hour_messages": int(peak_hour_messages or 0),
            }

        sample_limit = 50000
        if total_messages > sample_limit:
            text_rows = self._query(
                f"""
                SELECT sender_id, message
                FROM (
                    SELECT
                      COALESCE(m.from_id, 0) AS sender_id,
                      COALESCE(m.message, '') AS message,
                      m.id AS message_id
                    FROM {source_sql} AS m
                    WHERE COALESCE(m.from_id, 0) <> 0
                      AND COALESCE(TRIM(m.message), '') <> ''
                    ORDER BY m.id DESC
                    LIMIT ?
                ) AS recent_messages
                """,
                (*source_params, int(sample_limit)),
            )
        else:
            text_rows = self._query(
                f"""
                SELECT
                  COALESCE(m.from_id, 0) AS sender_id,
                  COALESCE(m.message, '') AS message
                FROM {source_sql} AS m
                WHERE COALESCE(m.from_id, 0) <> 0
                  AND COALESCE(TRIM(m.message), '') <> ''
                """,
                source_params,
            )

        sender_text_flags: Dict[int, Dict[str, int]] = {}
        sender_fingerprints: Dict[int, Counter[str]] = {}
        for sender_id, message_text in text_rows:
            try:
                sid = int(sender_id or 0)
            except Exception:
                sid = 0
            if sid <= 0:
                continue
            raw_text = str(message_text or "")
            bucket = sender_text_flags.setdefault(
                sid,
                {
                    "text_messages": 0,
                    "special_symbol_messages": 0,
                    "zero_width_messages": 0,
                    "mixed_script_messages": 0,
                    "link_messages": 0,
                },
            )
            bucket["text_messages"] += 1
            if URL_RE.search(raw_text):
                bucket["link_messages"] += 1
            if ZERO_WIDTH_RE.search(raw_text):
                bucket["zero_width_messages"] += 1
            if SPECIAL_SYMBOL_RE.search(raw_text):
                bucket["special_symbol_messages"] += 1
            if _contains_mixed_cyrillic_latin(raw_text):
                bucket["mixed_script_messages"] += 1
            fingerprint = _normalize_message_fingerprint(raw_text)
            if fingerprint:
                counter = sender_fingerprints.setdefault(sid, Counter())
                counter[fingerprint] += 1

        reaction_rows = self._query(
            f"""
            SELECT COALESCE(m.from_id, 0), m.reactions
            FROM {source_sql} AS m
            WHERE COALESCE(TRIM(m.reactions), '') <> ''
            """,
            source_params,
        )
        total_reactions = 0
        reaction_counter: Counter[str] = Counter()
        sender_reactions_total: Dict[int, int] = {}
        for sender_id, reactions_raw in reaction_rows:
            try:
                sid = int(sender_id or 0)
            except Exception:
                sid = 0
            try:
                reactions = json.loads(str(reactions_raw or "[]"))
            except Exception:
                reactions = []
            if not isinstance(reactions, list):
                continue
            sender_count = 0
            for item in reactions:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("emoji") or item.get("title") or item.get("custom_emoji_id") or "").strip()
                try:
                    count = int(item.get("count") or 0)
                except Exception:
                    count = 0
                if not symbol or count <= 0:
                    continue
                reaction_counter[symbol] += count
                total_reactions += count
                sender_count += count
            if sid > 0 and sender_count > 0:
                sender_reactions_total[sid] = sender_reactions_total.get(sid, 0) + sender_count

        poll_rows = self._query(
            f"""
            SELECT m.poll
            FROM {source_sql} AS m
            WHERE COALESCE(TRIM(m.poll), '') <> ''
            ORDER BY m.id DESC
            """,
            source_params,
        )
        polls: List[Dict[str, Any]] = []
        poll_total_voters = 0
        poll_open = 0
        poll_closed = 0
        poll_top: List[Dict[str, Any]] = []
        for (poll_raw,) in poll_rows:
            try:
                poll = json.loads(str(poll_raw or "{}"))
            except Exception:
                poll = None
            if not isinstance(poll, dict):
                continue
            polls.append(poll)
            voters = int(poll.get("total_voter_count") or 0)
            poll_total_voters += voters
            if bool(poll.get("is_closed")):
                poll_closed += 1
            else:
                poll_open += 1
            poll_top.append(
                {
                    "question": str(poll.get("question") or "Опрос"),
                    "total_voter_count": voters,
                    "is_closed": bool(poll.get("is_closed")),
                }
            )

        post_rows = self._query(
            f"""
            SELECT
              m.id,
              COALESCE(m.date, 0),
              COALESCE(m.message, ''),
              COALESCE(m.views, 0),
              COALESCE(m.forwards, 0),
              COALESCE(m.reactions, ''),
              COALESCE(m.media_type, 'text')
            FROM {source_sql} AS m
            WHERE COALESCE(m.views, 0) > 0
               OR COALESCE(m.forwards, 0) > 0
               OR COALESCE(TRIM(m.reactions), '') <> ''
            ORDER BY COALESCE(m.date, 0) DESC, m.id DESC
            LIMIT 240
            """,
            source_params,
        )
        now_ts = int(time.time())
        post_items: List[Dict[str, Any]] = []
        post_daily_map: Dict[str, Dict[str, Any]] = {}
        age_bucket_views: Dict[str, List[float]] = {}
        for message_id, message_date, message_text, message_views, message_forwards, reactions_raw, media_type in post_rows:
            try:
                mid = int(message_id or 0)
            except Exception:
                mid = 0
            try:
                ts = int(message_date or 0)
            except Exception:
                ts = 0
            try:
                views = int(message_views or 0)
            except Exception:
                views = 0
            try:
                forwards_value = int(message_forwards or 0)
            except Exception:
                forwards_value = 0
            reactions_total = _reaction_total_from_json(reactions_raw)
            if ts <= 0 and views <= 0 and forwards_value <= 0 and reactions_total <= 0:
                continue
            text_preview = str(message_text or "").strip().replace("\r", " ").replace("\n", " ")
            text_preview = re.sub(r"\s+", " ", text_preview)[:220]
            engagement_rate = round(float(reactions_total) / float(max(1, views)), 4) if views > 0 else 0.0
            forward_rate = round(float(forwards_value) / float(max(1, views)), 4) if views > 0 else 0.0
            age_hours = round(float(max(0, now_ts - ts)) / 3600.0, 2) if ts > 0 else 0.0
            if age_hours <= 24:
                age_bucket = "0-24h"
            elif age_hours <= 72:
                age_bucket = "1-3d"
            elif age_hours <= 168:
                age_bucket = "3-7d"
            elif age_hours <= 720:
                age_bucket = "7-30d"
            else:
                age_bucket = "30d+"
            if views > 0:
                age_bucket_views.setdefault(age_bucket, []).append(float(views))
            day_slot = ""
            if ts > 0:
                try:
                    day_slot = time.strftime("%Y-%m-%d", time.localtime(ts))
                except Exception:
                    day_slot = ""
            if day_slot:
                bucket = post_daily_map.setdefault(
                    day_slot,
                    {"day": day_slot, "posts": 0, "views": 0, "reactions": 0, "forwards": 0},
                )
                bucket["posts"] += 1
                bucket["views"] += views
                bucket["reactions"] += reactions_total
                bucket["forwards"] += forwards_value
            post_items.append(
                {
                    "id": mid,
                    "date": ts,
                    "text_preview": text_preview,
                    "type": str(media_type or "text"),
                    "views": views,
                    "forwards": forwards_value,
                    "reactions_total": reactions_total,
                    "engagement_rate": engagement_rate,
                    "forward_rate": forward_rate,
                    "age_hours": age_hours,
                    "age_bucket": age_bucket,
                }
            )

        age_bucket_medians = {
            key: _median_value(values)
            for key, values in age_bucket_views.items()
            if values
        }
        post_anomalies: List[Dict[str, Any]] = []
        for post in post_items:
            flags: List[str] = []
            views = int(post.get("views") or 0)
            reactions_total = int(post.get("reactions_total") or 0)
            forwards_value = int(post.get("forwards") or 0)
            engagement_rate = float(post.get("engagement_rate") or 0.0)
            forward_rate = float(post.get("forward_rate") or 0.0)
            median_views = float(age_bucket_medians.get(str(post.get("age_bucket") or ""), 0.0) or 0.0)
            if views >= 1000 and median_views > 0 and views >= median_views * 2.4:
                flags.append("view_spike")
            if views >= 1000 and engagement_rate >= 0.08:
                flags.append("reaction_spike")
            if views >= 1000 and forward_rate >= 0.045:
                flags.append("forward_spike")
            if views > 0 and reactions_total > int(views * 0.15):
                flags.append("reaction_to_view_outlier")
            if views > 0 and forwards_value > int(views * 0.09):
                flags.append("forward_to_view_outlier")
            if not flags:
                continue
            anomaly = dict(post)
            anomaly["flags"] = flags
            anomaly["median_views_for_age_bucket"] = int(round(median_views))
            post_anomalies.append(anomaly)
        post_anomalies.sort(
            key=lambda item: (
                -len(list(item.get("flags") or [])),
                -int(item.get("views") or 0),
                -int(item.get("reactions_total") or 0),
            )
        )
        post_items_sorted = sorted(
            post_items,
            key=lambda item: (
                -int(item.get("views") or 0),
                -int(item.get("reactions_total") or 0),
                -int(item.get("forwards") or 0),
            ),
        )
        view_values = [float(item.get("views") or 0) for item in post_items if int(item.get("views") or 0) > 0]
        reaction_rates = [float(item.get("engagement_rate") or 0.0) for item in post_items if int(item.get("views") or 0) > 0]
        forward_rates = [float(item.get("forward_rate") or 0.0) for item in post_items if int(item.get("views") or 0) > 0]
        post_performance = {
            "total_posts": int(len(post_items)),
            "median_views": int(round(_median_value(view_values))) if view_values else 0,
            "median_reaction_rate": round(_median_value(reaction_rates), 4) if reaction_rates else 0.0,
            "median_forward_rate": round(_median_value(forward_rates), 4) if forward_rates else 0.0,
            "top_posts": post_items_sorted[:18],
            "daily": sorted(post_daily_map.values(), key=lambda item: str(item.get("day") or "")),
            "anomalies": post_anomalies[:18],
            "age_bucket_medians": {key: int(round(value)) for key, value in age_bucket_medians.items()},
        }

        sender_daily_activity: Dict[str, List[Dict[str, Any]]] = {}
        if include_sender_daily:
            sender_daily_rows = self._query(
                f"""
                SELECT
                  COALESCE(m.from_id, 0),
                  strftime('%Y-%m-%d', datetime(m.date, 'unixepoch', 'localtime')) AS day_slot,
                  COUNT(1)
                FROM {source_sql} AS m
                WHERE COALESCE(m.from_id, 0) <> 0
                  AND COALESCE(m.date, 0) > 0
                GROUP BY COALESCE(m.from_id, 0), day_slot
                ORDER BY COALESCE(m.from_id, 0) ASC, day_slot ASC
                """,
                source_params,
            )
            for sender_id, day_slot, count in sender_daily_rows:
                try:
                    sid = int(sender_id or 0)
                except Exception:
                    sid = 0
                day = str(day_slot or "").strip()
                if sid <= 0 or not day:
                    continue
                sender_daily_activity.setdefault(str(sid), []).append(
                    {"day": day, "count": int(count or 0)}
                )

        activity_days = int(len(daily_activity))
        peak_day = max(daily_activity, key=lambda item: int(item.get("count") or 0), default={"day": "", "count": 0})
        peak_hour = max(hourly_activity, key=lambda item: int(item.get("count") or 0), default={"hour": 0, "count": 0})
        off_hours_messages = sum(
            int(item.get("count") or 0)
            for item in hourly_activity
            if 0 <= int(item.get("hour") or 0) <= 5
        )
        activity_summary = {
            "active_days": activity_days,
            "messages_per_active_day": round(float(total_messages) / float(max(1, activity_days)), 2),
            "peak_day": {"day": str(peak_day.get("day") or ""), "count": int(peak_day.get("count") or 0)},
            "peak_hour": {"hour": int(peak_hour.get("hour") or 0), "count": int(peak_hour.get("count") or 0)},
            "off_hours_messages": int(off_hours_messages),
            "off_hours_share": round(float(off_hours_messages) / float(max(1, total_messages)), 4),
        }

        suspicious_senders: List[Dict[str, Any]] = []
        suspicious_message_total = 0
        bot_like_senders = 0
        bot_like_messages = 0
        numeric_username_senders = 0
        severity_rank = {"low": 1, "medium": 2, "high": 3, "critical": 4}
        sender_reason_weights = {
            "bot_like_sender": 18,
            "high_message_share": 14,
            "dominant_sender": 18,
            "high_deleted_sender_share": 10,
            "media_heavy_sender": 8,
            "burst_day_pattern": 8,
            "off_hours_cluster": 8,
            "numeric_username": 6,
            "special_symbol_sender": 12,
            "mixed_script_sender": 10,
            "repetitive_sender": 10,
        }
        for row in sender_details_full:
            count_value = int(row.get("count") or 0)
            sender_id = int(row.get("sender_id") or 0)
            username = str(row.get("username") or "")
            lower_user = username.lower()
            lower_type = str(row.get("type") or "").lower()
            deleted_sender = int(row.get("deleted_messages") or 0)
            media_sender = int(row.get("media_messages") or 0)
            day_meta = sender_day_summary.get(sender_id, {})
            hour_meta = sender_hour_summary.get(sender_id, {})
            active_sender_days = int(day_meta.get("active_days") or 0)
            peak_day_messages = int(day_meta.get("peak_day_messages") or 0)
            off_hours_sender = int(hour_meta.get("off_hours_messages") or 0)
            peak_hour_messages = int(hour_meta.get("peak_hour_messages") or 0)
            text_meta = sender_text_flags.get(sender_id, {})
            text_messages = int(text_meta.get("text_messages") or 0)
            special_symbol_messages = int(text_meta.get("special_symbol_messages") or 0)
            zero_width_messages = int(text_meta.get("zero_width_messages") or 0)
            mixed_script_messages = int(text_meta.get("mixed_script_messages") or 0)
            link_messages = int(text_meta.get("link_messages") or 0)
            fingerprint_counter = sender_fingerprints.get(sender_id, Counter())
            repetitive_messages = max(fingerprint_counter.values()) if fingerprint_counter else 0
            deleted_share = round(float(deleted_sender) / float(max(1, count_value)), 4)
            media_share = round(float(media_sender) / float(max(1, count_value)), 4)
            peak_day_share = round(float(peak_day_messages) / float(max(1, count_value)), 4)
            off_hours_share = round(float(off_hours_sender) / float(max(1, count_value)), 4)
            special_symbol_share = round(float(special_symbol_messages) / float(max(1, text_messages)), 4)
            zero_width_share = round(float(zero_width_messages) / float(max(1, text_messages)), 4)
            mixed_script_share = round(float(mixed_script_messages) / float(max(1, text_messages)), 4)
            repetitive_share = round(float(repetitive_messages) / float(max(1, text_messages)), 4)
            row["active_days"] = active_sender_days
            row["avg_per_active_day"] = round(float(count_value) / float(max(1, active_sender_days)), 2)
            row["peak_day_messages"] = peak_day_messages
            row["peak_day_share"] = peak_day_share
            row["peak_hour_messages"] = peak_hour_messages
            row["off_hours_messages"] = off_hours_sender
            row["off_hours_share"] = off_hours_share
            row["deleted_share"] = deleted_share
            row["media_share"] = media_share
            row["text_messages"] = text_messages
            row["link_messages"] = link_messages
            row["total_reactions"] = int(sender_reactions_total.get(sender_id, 0))
            row["special_symbol_messages"] = special_symbol_messages
            row["special_symbol_share"] = special_symbol_share
            row["zero_width_messages"] = zero_width_messages
            row["zero_width_share"] = zero_width_share
            row["mixed_script_messages"] = mixed_script_messages
            row["mixed_script_share"] = mixed_script_share
            row["repetitive_messages"] = repetitive_messages
            row["repetitive_share"] = repetitive_share

            reasons: List[str] = []
            if lower_type == "bot" or lower_user.endswith("bot"):
                reasons.append("bot_like_sender")
            if count_value >= 20 and float(row.get("share") or 0.0) >= 0.35:
                reasons.append("high_message_share")
            if count_value >= 50 and float(row.get("share") or 0.0) >= 0.50:
                reasons.append("dominant_sender")
            if count_value >= 10 and deleted_share >= 0.25:
                reasons.append("high_deleted_sender_share")
            if count_value >= 12 and media_share >= 0.90:
                reasons.append("media_heavy_sender")
            if count_value >= 15 and peak_day_share >= 0.70:
                reasons.append("burst_day_pattern")
            if count_value >= 12 and off_hours_share >= 0.65:
                reasons.append("off_hours_cluster")
            digit_count = sum(1 for ch in username if ch.isdigit())
            if username and digit_count >= 4 and float(digit_count) / float(max(1, len(username))) >= 0.4:
                reasons.append("numeric_username")
            if text_messages >= 6 and (special_symbol_share >= 0.35 or zero_width_share >= 0.15):
                reasons.append("special_symbol_sender")
            if text_messages >= 6 and mixed_script_share >= 0.25:
                reasons.append("mixed_script_sender")
            if text_messages >= 8 and repetitive_messages >= 4 and repetitive_share >= 0.40:
                reasons.append("repetitive_sender")
            row["risk_flags"] = list(reasons)
            row["risk_score"] = min(100, sum(int(sender_reason_weights.get(reason, 0)) for reason in reasons))
            if "bot_like_sender" in reasons:
                bot_like_senders += 1
                bot_like_messages += count_value
            if "numeric_username" in reasons:
                numeric_username_senders += 1
            if row["risk_score"] >= 18:
                suspicious_row = dict(row)
                suspicious_row["reasons"] = list(reasons)
                suspicious_senders.append(suspicious_row)
                suspicious_message_total += count_value

        shares = [float(row.get("share") or 0.0) for row in sender_details_full]
        single_message_senders = sum(1 for row in sender_details_full if int(row.get("count") or 0) == 1)
        distribution = {
            "top1_share": round(sum(shares[:1]), 4),
            "top3_share": round(sum(shares[:3]), 4),
            "top5_share": round(sum(shares[:5]), 4),
            "top10_share": round(sum(shares[:10]), 4),
            "concentration_index": round(sum(share * share for share in shares), 4),
            "single_message_senders": int(single_message_senders),
            "single_message_sender_share": round(float(single_message_senders) / float(max(1, len(sender_details_full))), 4),
            "suspicious_message_share": round(float(suspicious_message_total) / float(max(1, total_messages)), 4),
        }
        bot_summary = {
            "bot_like_senders": int(bot_like_senders),
            "bot_like_message_share": round(float(bot_like_messages) / float(max(1, total_messages)), 4),
            "numeric_username_senders": int(numeric_username_senders),
            "special_symbol_senders": int(sum(1 for row in sender_details_full if "special_symbol_sender" in list(row.get("risk_flags") or []))),
            "mixed_script_senders": int(sum(1 for row in sender_details_full if "mixed_script_sender" in list(row.get("risk_flags") or []))),
        }

        anomaly_flags: List[str] = []
        if total_views > 0 and total_reactions > int(total_views * 1.2):
            anomaly_flags.append("reactions_exceed_views")
        if total_views > 0 and total_forwards > int(total_views * 1.1):
            anomaly_flags.append("forwards_exceed_views")
        if len(suspicious_senders) >= 2:
            anomaly_flags.append("multiple_suspicious_senders")
        if total_messages > 0 and deleted_messages >= max(15, int(total_messages * 0.15)):
            anomaly_flags.append("high_deleted_share")
        if float(distribution.get("top1_share") or 0.0) >= 0.45:
            anomaly_flags.append("dominant_top_sender")
        if float(distribution.get("concentration_index") or 0.0) >= 0.18 and len(sender_details_full) >= 4:
            anomaly_flags.append("high_sender_concentration")
        if float(activity_summary.get("off_hours_share") or 0.0) >= 0.30 and total_messages >= 80:
            anomaly_flags.append("off_hours_activity_cluster")
        if int(activity_summary.get("peak_day", {}).get("count") or 0) >= max(25, int(total_messages * 0.28)):
            anomaly_flags.append("burst_activity_day")
        if float(bot_summary.get("bot_like_message_share") or 0.0) >= 0.25 and int(bot_summary.get("bot_like_senders") or 0) >= 1:
            anomaly_flags.append("bot_like_message_cluster")
        if int(bot_summary.get("special_symbol_senders") or 0) >= 2:
            anomaly_flags.append("special_symbol_cluster")

        risk_factors: List[Dict[str, Any]] = []

        def _add_risk_factor(key: str, label: str, *, severity: str, score: int, detail: str) -> None:
            risk_factors.append(
                {
                    "key": key,
                    "label": label,
                    "severity": severity,
                    "score": int(score),
                    "detail": detail,
                }
            )

        if "dominant_top_sender" in anomaly_flags:
            _add_risk_factor(
                "dominant_top_sender",
                "Один отправитель доминирует в потоке",
                severity="high",
                score=18,
                detail=f"Топ-1 отправитель формирует {float(distribution.get('top1_share') or 0.0) * 100.0:.1f}% сообщений.",
            )
        if "high_sender_concentration" in anomaly_flags:
            _add_risk_factor(
                "high_sender_concentration",
                "Высокая концентрация активности",
                severity="medium",
                score=12,
                detail=f"Индекс концентрации сообщений: {float(distribution.get('concentration_index') or 0.0):.3f}.",
            )
        if "bot_like_message_cluster" in anomaly_flags:
            _add_risk_factor(
                "bot_like_message_cluster",
                "Заметная доля bot-like активности",
                severity="high",
                score=16,
                detail=f"Bot-like отправители дали {float(bot_summary.get('bot_like_message_share') or 0.0) * 100.0:.1f}% сообщений.",
            )
        if "high_deleted_share" in anomaly_flags:
            _add_risk_factor(
                "high_deleted_share",
                "Высокая доля удалённых сообщений",
                severity="medium",
                score=12,
                detail=f"Удалено {float(deleted_messages) * 100.0 / float(max(1, total_messages)):.1f}% сообщений.",
            )
        if "off_hours_activity_cluster" in anomaly_flags:
            _add_risk_factor(
                "off_hours_activity_cluster",
                "Подозрительный кластер ночной активности",
                severity="medium",
                score=10,
                detail=f"В 00:00-05:59 отправлено {float(activity_summary.get('off_hours_share') or 0.0) * 100.0:.1f}% сообщений.",
            )
        if "burst_activity_day" in anomaly_flags:
            _add_risk_factor(
                "burst_activity_day",
                "Резкий всплеск активности за один день",
                severity="medium",
                score=10,
                detail=f"Пиковый день: {int(activity_summary.get('peak_day', {}).get('count') or 0)} сообщений.",
            )
        if "reactions_exceed_views" in anomaly_flags:
            _add_risk_factor(
                "reactions_exceed_views",
                "Реакции аномально опережают просмотры",
                severity="high",
                score=14,
                detail="Общий объём реакций выше ожидаемого относительно просмотров.",
            )
        if "forwards_exceed_views" in anomaly_flags:
            _add_risk_factor(
                "forwards_exceed_views",
                "Пересылки аномально опережают просмотры",
                severity="medium",
                score=10,
                detail="Общий объём пересылок выше ожидаемого относительно просмотров.",
            )
        if "special_symbol_cluster" in anomaly_flags:
            _add_risk_factor(
                "special_symbol_cluster",
                "Кластер сообщений со спецсимволами и скрытыми Unicode-знаками",
                severity="medium",
                score=10,
                detail=f"Отправители с suspicious Unicode-паттернами: {int(bot_summary.get('special_symbol_senders') or 0)}.",
            )
        if int(post_performance.get("total_posts") or 0) >= 8 and len(list(post_performance.get("anomalies") or [])) >= 3:
            _add_risk_factor(
                "post_performance_anomalies",
                "Есть аномалии по просмотрам, реакциям или пересылкам постов",
                severity="high",
                score=16,
                detail=f"Аномальных постов: {int(len(list(post_performance.get('anomalies') or [])))}.",
            )

        risk_score = min(100, sum(int(item.get("score") or 0) for item in risk_factors))
        if risk_score >= 70:
            risk_level = "critical"
        elif risk_score >= 50:
            risk_level = "high"
        elif risk_score >= 25:
            risk_level = "medium"
        else:
            risk_level = "low"
        max_severity = "low"
        for factor in risk_factors:
            severity = str(factor.get("severity") or "low")
            if severity_rank.get(severity, 0) > severity_rank.get(max_severity, 0):
                max_severity = severity

        top_senders = sender_details_full[:12]
        sender_details = list(sender_details_full if int(sender_limit or 0) <= 0 else sender_details_full[: int(sender_limit)])
        poll_top_sorted = sorted(poll_top, key=lambda row: int(row.get("total_voter_count") or 0), reverse=True)[:10]
        engagement = {
            "views_per_message": round(float(total_views) / float(max(1, total_messages)), 2),
            "reactions_per_100_messages": round(float(total_reactions) * 100.0 / float(max(1, total_messages)), 2),
            "forwards_per_100_messages": round(float(total_forwards) * 100.0 / float(max(1, total_messages)), 2),
        }
        payload: Dict[str, Any] = {
            "total_messages": total_messages,
            "media_messages": media_messages,
            "deleted_messages": deleted_messages,
            "total_views": total_views,
            "total_forwards": total_forwards,
            "total_reactions": total_reactions,
            "total_senders": len(sender_details_full),
            "top_reactions": [{"emoji": key, "count": value} for key, value in reaction_counter.most_common(8)],
            "polls": polls[:10],
            "top_senders": top_senders,
            "sender_details": sender_details,
            "suspicious_senders": suspicious_senders[:10],
            "anomaly_flags": anomaly_flags,
            "hourly_activity": hourly_activity,
            "daily_activity": daily_activity,
            "engagement": engagement,
            "post_performance": post_performance,
            "activity_summary": activity_summary,
            "distribution": distribution,
            "bot_summary": bot_summary,
            "risk": {
                "score": int(risk_score),
                "level": risk_level,
                "severity": max_severity,
                "factors": risk_factors,
            },
            "polls_summary": {
                "total_polls": int(len(polls)),
                "open_polls": int(poll_open),
                "closed_polls": int(poll_closed),
                "total_voters": int(poll_total_voters),
                "top_polls": poll_top_sorted,
            },
            "scope": {
                "peer_id": int(peer_id),
                "message_limit": int(limit or 0),
                "is_full_history": int(limit or 0) <= 0,
            },
            "date_range": {
                "first_message_date": first_message_date,
                "last_message_date": last_message_date,
            },
        }
        if include_sender_daily:
            payload["sender_daily_activity"] = sender_daily_activity
        try:
            self._exec(f'DROP TABLE IF EXISTS {_src_tbl}')
        except Exception:
            pass
        return payload

    def save_chat_statistics_snapshot(
        self,
        peer_id: int,
        payload: Dict[str, Any],
        *,
        scanned_at: Optional[int] = None,
    ) -> int:
        ts = int(scanned_at or time.time())
        data = dict(payload or {})
        data["scanned_at"] = ts
        self._exec(
            """
            INSERT OR REPLACE INTO chat_statistics_snapshots(peer_id, scanned_at, payload)
            VALUES (?, ?, ?)
            """,
            (int(peer_id), ts, json.dumps(data, ensure_ascii=False)),
        )
        return ts

    def get_chat_statistics_snapshots(self, peer_id: int, *, limit: int = 2) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT scanned_at, payload
            FROM chat_statistics_snapshots
            WHERE peer_id = ?
            ORDER BY scanned_at DESC
            LIMIT ?
            """,
            (int(peer_id), int(max(1, limit))),
        )
        out: List[Dict[str, Any]] = []
        for scanned_at, payload_raw in rows:
            try:
                payload = json.loads(payload_raw) if payload_raw else {}
            except Exception:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            payload.setdefault("scanned_at", int(scanned_at or 0))
            out.append(payload)
        return out

    def save_community_scan_run(
        self,
        scan_key: str,
        *,
        query: str,
        payload: Dict[str, Any],
        title: Optional[str] = None,
        root_chat_id: Optional[int] = None,
        updated_at: Optional[int] = None,
    ) -> int:
        key = str(scan_key or "").strip()
        if not key:
            raise ValueError("scan_key is required")
        ts = int(updated_at or time.time())
        data = dict(payload or {})
        data.setdefault("scan_key", key)
        data.setdefault("query", str(query or ""))
        data["updated_at"] = ts
        root_value = None
        try:
            root_value = int(root_chat_id) if root_chat_id is not None else None
        except Exception:
            root_value = None
        self._exec(
            """
            INSERT OR REPLACE INTO community_scan_runs(scan_key, query, title, root_chat_id, payload, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                key,
                str(query or ""),
                str(title or "") or None,
                root_value,
                json.dumps(data, ensure_ascii=False),
                ts,
            ),
        )
        return ts

    def get_community_scan_run(self, scan_key: str) -> Dict[str, Any]:
        key = str(scan_key or "").strip()
        if not key:
            return {}
        rows = self._query(
            """
            SELECT query, title, root_chat_id, payload, updated_at
            FROM community_scan_runs
            WHERE scan_key = ?
            """,
            (key,),
        )
        if not rows:
            return {}
        query, title, root_chat_id, payload_raw, updated_at = rows[0]
        try:
            payload = json.loads(str(payload_raw or "{}"))
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("scan_key", key)
        payload.setdefault("query", str(query or ""))
        payload.setdefault("title", str(title or ""))
        payload.setdefault("root_chat_id", int(root_chat_id or 0) if root_chat_id is not None else None)
        payload.setdefault("updated_at", int(updated_at or 0))
        return payload

    def list_community_scan_runs(self, *, limit: int = 24) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT scan_key, query, title, root_chat_id, updated_at, payload
            FROM community_scan_runs
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (int(max(1, limit or 24)),),
        )
        out: List[Dict[str, Any]] = []
        for scan_key, query, title, root_chat_id, updated_at, payload_raw in rows:
            try:
                payload = json.loads(str(payload_raw or "{}"))
            except Exception:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            payload.update(
                {
                    "scan_key": str(scan_key or ""),
                    "query": str(query or ""),
                    "title": str(title or ""),
                    "root_chat_id": int(root_chat_id or 0) if root_chat_id is not None else None,
                    "updated_at": int(updated_at or 0),
                }
            )
            out.append(payload)
        return out

    def export_subset_sqlite(
        self,
        dest_path: str,
        *,
        peer_ids: Sequence[int],
        scan_key: Optional[str] = None,
    ) -> str:
        target = os.path.abspath(str(dest_path or ""))
        if not target:
            raise ValueError("dest_path is required")
        peer_list = sorted({int(pid) for pid in list(peer_ids or []) if str(pid).strip()})
        if not peer_list:
            raise ValueError("peer_ids is required")
        _ensure_db_parent_dir(target)
        if os.path.exists(target):
            os.remove(target)

        target_storage = Storage(target)
        target_storage.connect()
        try:
            target_storage.init_schema()

            peer_id_values = set(peer_list)
            placeholders = ",".join("?" for _ in peer_list)

            sender_rows = self._query(
                f"""
                SELECT DISTINCT from_id
                FROM messages
                WHERE peer_id IN ({placeholders})
                  AND COALESCE(from_id, 0) <> 0
                """,
                tuple(peer_list),
            )
            for (sender_id,) in sender_rows:
                try:
                    sid = int(sender_id or 0)
                except Exception:
                    sid = 0
                if sid != 0:
                    peer_id_values.add(sid)

            peer_rows = self._query(
                f"""
                SELECT id, type, username, title, photo_small, photo_big
                FROM peers
                WHERE id IN ({",".join("?" for _ in peer_id_values)})
                """,
                tuple(sorted(peer_id_values)),
            )
            target_storage.upsert_peers(
                [
                    {
                        "id": int(pid),
                        "type": ptype,
                        "username": username,
                        "title": title,
                        "photo_small": photo_small,
                        "photo_big": photo_big,
                    }
                    for pid, ptype, username, title, photo_small, photo_big in peer_rows
                ]
            )

            dialog_rows = self._query(
                f"""
                SELECT peer_id, top_message_id, last_message_date, unread_count, pinned, last_read_inbox_id, last_read_outbox_id
                FROM dialogs
                WHERE peer_id IN ({placeholders})
                """,
                tuple(peer_list),
            )
            if dialog_rows:
                target_storage.upsert_dialogs(
                    [
                        {
                            "peer_id": int(peer_id),
                            "top_message_id": int(top_message_id or 0),
                            "last_message_date": int(last_message_date or 0),
                            "unread_count": int(unread_count or 0),
                            "pinned": int(pinned or 0),
                            "last_read_inbox_id": int(last_read_inbox_id or 0),
                            "last_read_outbox_id": int(last_read_outbox_id or 0),
                        }
                        for peer_id, top_message_id, last_message_date, unread_count, pinned, last_read_inbox_id, last_read_outbox_id in dialog_rows
                    ]
                )

            message_rows = self._query(
                f"""
                SELECT
                  peer_id, id, date, from_id, reply_to, message, media_type, media_id, is_deleted,
                  forward_info, file_name, entities, reply_markup, duration, waveform, reactions,
                  poll, views, forwards, media_group_id
                FROM messages
                WHERE peer_id IN ({placeholders})
                ORDER BY peer_id ASC, id ASC
                """,
                tuple(peer_list),
            )
            grouped_messages: Dict[int, List[Dict[str, Any]]] = {}
            for row in message_rows:
                peer_id = int(row[0])
                grouped_messages.setdefault(peer_id, []).append(
                    {
                        "id": int(row[1]),
                        "date": int(row[2] or 0),
                        "from_id": int(row[3] or 0) if row[3] is not None else None,
                        "reply_to": int(row[4] or 0) if row[4] is not None else None,
                        "message": row[5],
                        "media_type": row[6],
                        "media_id": row[7],
                        "is_deleted": int(row[8] or 0),
                        "forward_info": row[9],
                        "file_name": row[10],
                        "entities": row[11],
                        "reply_markup": row[12],
                        "duration": int(row[13] or 0) if row[13] is not None else None,
                        "waveform": row[14],
                        "reactions": row[15],
                        "poll": row[16],
                        "views": int(row[17] or 0) if row[17] is not None else None,
                        "forwards": int(row[18] or 0) if row[18] is not None else None,
                        "media_group_id": row[19],
                    }
                )
            for peer_id, items in grouped_messages.items():
                target_storage.upsert_messages(peer_id, items)

            for table_name, sql in (
                (
                    "deleted_message_events",
                    f"SELECT peer_id, message_id, deleted_at, snapshot_text, media_type, sender_id, source FROM deleted_message_events WHERE peer_id IN ({placeholders})",
                ),
                (
                    "chat_statistics_snapshots",
                    f"SELECT peer_id, scanned_at, payload FROM chat_statistics_snapshots WHERE peer_id IN ({placeholders})",
                ),
                (
                    "chat_profile_sections_cache",
                    f"SELECT peer_id, section, dedupe_key, message_id, message_date, payload, updated_at FROM chat_profile_sections_cache WHERE peer_id IN ({placeholders})",
                ),
                (
                    "chat_profile_scan_state",
                    f"SELECT peer_id, last_message_id, updated_at FROM chat_profile_scan_state WHERE peer_id IN ({placeholders})",
                ),
            ):
                rows = self._query(sql, tuple(peer_list))
                if rows:
                    marks = ",".join("?" for _ in rows[0])
                    target_storage._execmany(
                        f"INSERT OR REPLACE INTO {table_name} VALUES ({marks})",
                        rows,
                    )

            file_rows = self._query(
                f"""
                SELECT DISTINCT file_id, path, size, mime, crc32, ttl, added_at
                FROM files
                WHERE file_id IN (
                    SELECT DISTINCT COALESCE(media_id, CAST(peer_id AS TEXT) || ':' || CAST(id AS TEXT))
                    FROM messages
                    WHERE peer_id IN ({placeholders})
                )
                """,
                tuple(peer_list),
            )
            if file_rows:
                target_storage._execmany(
                    "INSERT OR REPLACE INTO files(file_id, path, size, mime, crc32, ttl, added_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    file_rows,
                )

            if scan_key:
                scan_row = self.get_community_scan_run(scan_key)
                if scan_row:
                    target_storage.save_community_scan_run(
                        str(scan_row.get("scan_key") or scan_key),
                        query=str(scan_row.get("query") or ""),
                        payload=scan_row,
                        title=str(scan_row.get("title") or ""),
                        root_chat_id=scan_row.get("root_chat_id"),
                        updated_at=int(scan_row.get("updated_at") or time.time()),
                    )
        finally:
            target_storage.close()
        return target

    def get_message_statistics(self, peer_id: int, message_id: int) -> Dict[str, Any]:
        item = self.get_message_by_id(int(peer_id), int(message_id)) or {}
        sender_profile = None
        sender_raw = item.get("from_id")
        try:
            sender_id = int(sender_raw) if sender_raw is not None else 0
        except Exception:
            sender_id = 0
        if sender_id:
            rows = self._query(
                """
                SELECT id, COALESCE(title, ''), COALESCE(username, ''), COALESCE(type, '')
                FROM peers
                WHERE id = ?
                LIMIT 1
                """,
                (sender_id,),
            )
            if rows:
                row = rows[0]
                sender_profile = {
                    "id": int(row[0]),
                    "name": str(row[1] or row[2] or row[0]),
                    "username": str(row[2] or ""),
                    "type": str(row[3] or ""),
                }
        deleted_rows = self._query(
            """
            SELECT deleted_at, snapshot_text, media_type, sender_id, source
            FROM deleted_message_events
            WHERE peer_id = ? AND message_id = ?
            LIMIT 1
            """,
            (int(peer_id), int(message_id)),
        )
        deleted_snapshot = None
        if deleted_rows:
            row = deleted_rows[0]
            deleted_snapshot = {
                "deleted_at": int(row[0] or 0),
                "snapshot_text": str(row[1] or ""),
                "media_type": str(row[2] or "text"),
                "sender_id": row[3],
                "source": str(row[4] or ""),
            }
        risk_flags: List[str] = []
        views = int(item.get("views") or 0)
        forwards = int(item.get("forwards") or 0)
        reactions_total = 0
        reactions = item.get("reactions") if isinstance(item.get("reactions"), list) else []
        for reaction in reactions:
            if not isinstance(reaction, dict):
                continue
            try:
                reactions_total += int(reaction.get("count") or 0)
            except Exception:
                continue
        if views > 0 and reactions_total > int(views * 1.2):
            risk_flags.append("reactions_exceed_views")
        if views > 0 and forwards > int(views * 1.1):
            risk_flags.append("forwards_exceed_views")
        if sender_profile:
            sender_type = str(sender_profile.get("type") or "").lower()
            sender_username = str(sender_profile.get("username") or "").lower()
            if sender_type == "bot" or sender_username.endswith("bot"):
                risk_flags.append("sender_bot_like")
        return {
            "message": item,
            "deleted_snapshot": deleted_snapshot,
            "sender_profile": sender_profile,
            "risk_flags": risk_flags,
        }

    # --------------------------- AI history ---------------------------

    def get_ai_history(self, chat_id: str, limit: int = 30) -> List[Dict[str, Any]]:
        rows = self._query(
            """
            SELECT id, role, content, timestamp, reply_to, is_edited, is_deleted
            FROM (
              SELECT *
              FROM ai_history
              WHERE chat_id = ?
              ORDER BY id DESC
              LIMIT ?
            )
            ORDER BY id ASC
            """,
            (str(chat_id), int(limit)),
        )
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "message_id": int(r[0]),
                "role": str(r[1] or "user"),
                "content": r[2] or "",
                "timestamp": str(r[3] or ""),
                "reply_to": r[4],
                "is_edited": bool(r[5]),
                "is_deleted": bool(r[6]),
            })
        return out

    def search_ai_history(
        self,
        terms: Iterable[str],
        *,
        exclude_chat_id: Optional[str] = None,
        limit: int = 6,
    ) -> List[Dict[str, Any]]:
        normalized: List[str] = []
        seen: set[str] = set()
        for raw in terms:
            token = str(raw or "").strip().lower()
            if len(token) < 2 or token in seen:
                continue
            seen.add(token)
            normalized.append(token)
        if not normalized or limit <= 0:
            return []

        like_params = [f"%{token}%" for token in normalized]
        where_terms = " OR ".join(["LOWER(content) LIKE ?"] * len(like_params))

        sql = f"""
            SELECT chat_id, id, role, content, timestamp
            FROM ai_history
            WHERE COALESCE(is_deleted, 0) = 0
              AND COALESCE(TRIM(content), '') <> ''
              {"AND chat_id <> ?" if exclude_chat_id else ""}
              AND ({where_terms})
            LIMIT ?
        """
        params: List[Any] = []
        if exclude_chat_id:
            params.append(str(exclude_chat_id))
        params.extend(like_params)
        # Read a wider candidate set, then rank in Python.
        params.append(int(max(limit * 12, 40)))
        rows = self._query(sql, tuple(params))

        scored: List[Tuple[int, str, int, Dict[str, Any]]] = []
        for row in rows:
            chat_id = str(row[0] or "")
            msg_id = int(row[1] or 0)
            role = str(row[2] or "user")
            content = str(row[3] or "")
            timestamp = str(row[4] or "")
            lowered = content.lower()
            score = sum(1 for token in normalized if token in lowered)
            if score <= 0:
                continue
            scored.append(
                (
                    score,
                    timestamp,
                    msg_id,
                    {
                        "chat_id": chat_id,
                        "message_id": msg_id,
                        "role": role,
                        "content": content,
                        "timestamp": timestamp,
                    },
                )
            )

        scored.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        return [entry for _, _, _, entry in scored[: int(limit)]]

    def append_ai_messages(
        self,
        chat_id: str,
        messages: Iterable[Dict[str, Any]],
        *,
        limit: int = 30,
    ) -> None:
        rows: List[Tuple] = []
        for msg in messages:
            try:
                mid = int(msg.get("message_id"))
            except Exception:
                continue
            rows.append((
                str(chat_id),
                mid,
                str(msg.get("role") or "user"),
                str(msg.get("content") or ""),
                str(msg.get("timestamp") or ""),
                msg.get("reply_to"),
                1 if msg.get("is_edited") else 0,
                1 if msg.get("is_deleted") else 0,
            ))
        if not rows:
            return
        self._execmany(
            """
            INSERT INTO ai_history(chat_id,id,role,content,timestamp,reply_to,is_edited,is_deleted)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(chat_id,id) DO UPDATE SET
              role=excluded.role,
              content=excluded.content,
              timestamp=excluded.timestamp,
              reply_to=excluded.reply_to,
              is_edited=excluded.is_edited,
              is_deleted=excluded.is_deleted
            """,
            rows,
        )
        self._exec(
            """
            DELETE FROM ai_history
            WHERE chat_id = ?
              AND id NOT IN (
                SELECT id
                FROM ai_history
                WHERE chat_id = ?
                ORDER BY id DESC
                LIMIT ?
              )
            """,
            (str(chat_id), str(chat_id), int(max(limit, 1))),
        )

    def get_all_messages_for_export(
        self,
        peer_id: int,
        *,
        date_from: Optional[int] = None,
        date_to: Optional[int] = None,
        include_deleted: bool = False,
        offset: int = 0,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        where_parts = ["m.peer_id = ?"]
        params: List[Any] = [int(peer_id)]
        if not include_deleted:
            where_parts.append("COALESCE(m.is_deleted, 0) = 0")
        if date_from is not None:
            where_parts.append("COALESCE(m.date, 0) >= ?")
            params.append(int(date_from))
        if date_to is not None:
            where_parts.append("COALESCE(m.date, 0) <= ?")
            params.append(int(date_to))
        where_clause = " AND ".join(where_parts)
        params.extend([int(offset), int(limit)])
        rows = self._query(
            f"""
            SELECT
              m.id,
              m.date,
              m.from_id,
              m.reply_to,
              m.message,
              m.media_type,
              m.media_id,
              m.is_deleted,
              m.forward_info,
              m.file_name,
              m.entities,
              m.reply_markup,
              f.path,
              f.size,
              f.mime,
              COALESCE(p.title, p.username, CAST(m.from_id AS TEXT)),
              m.duration,
              m.waveform,
              m.reactions,
              m.poll,
              m.views,
              m.forwards,
              m.media_group_id
            FROM messages m
            LEFT JOIN files f
              ON f.file_id = COALESCE(m.media_id, CAST(m.peer_id AS TEXT) || ':' || CAST(m.id AS TEXT))
            LEFT JOIN peers p
              ON p.id = m.from_id
            WHERE {where_clause}
            ORDER BY m.id ASC
            LIMIT ? OFFSET ?
            """,
            tuple(params),
        )
        _EXPORT_JSON_FIELDS = {"forward_info": 8, "entities": 10, "reply_markup": 11, "waveform": 17, "reactions": 18, "poll": 19}
        out: List[Dict[str, Any]] = []
        for r in rows:
            sender_id = r[2]
            parsed = _parse_message_json_fields(r, _EXPORT_JSON_FIELDS)
            forward_info = parsed.get("forward_info")
            if forward_info is None and r[8]:
                forward_info = {"sender": r[8]}
            out.append({
                "id": int(r[0]),
                "date": int(r[1] or 0),
                "from_id": sender_id,
                "sender_id": str(sender_id) if sender_id is not None else "",
                "sender": r[15] or (str(sender_id) if sender_id is not None else ""),
                "reply_to": r[3],
                "text": r[4] or "",
                "type": r[5] or "text",
                "media_id": r[6],
                "is_deleted": bool(r[7]),
                "forward_info": forward_info,
                "file_name": r[9],
                "entities": parsed.get("entities"),
                "reply_markup": parsed.get("reply_markup"),
                "file_path": r[12],
                "file_size": r[13],
                "mime": r[14],
                "duration": r[16],
                "waveform": parsed.get("waveform"),
                "reactions": parsed.get("reactions"),
                "poll": parsed.get("poll"),
                "views": r[20],
                "forwards": r[21],
                "media_group_id": r[22],
            })
        return out
