# storage.py — единый слой БД (SQLite/APSW), схема, WAL/PRAGMA + DAO
from __future__ import annotations

import os
import re
import time
import threading
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


@dataclass
class _ConnWrap:
    is_apsw: bool
    conn: object  # apsw.Connection | sqlite3.Connection


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
        os.makedirs(os.path.dirname(DEFAULT_DB_PATH), exist_ok=True)
        st = cls(DEFAULT_DB_PATH)
        st.connect()
        return st

    def connect(self) -> None:
        if self._cx:
            return
        self._closing = False

        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        if HAVE_APSW:
            conn = apsw.Connection(self.db_path)  # type: ignore[assignment]
            conn.setbusytimeout(5_000)  # мс
            self._cx = _ConnWrap(True, conn)
            cur = conn.cursor()
            # WAL и базовые pragma
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.execute("PRAGMA journal_mode=WAL;")              # включаем WAL
            cur.execute("PRAGMA synchronous=NORMAL;")            # в WAL это «безопасно» и быстро
            cur.execute("PRAGMA temp_store=MEMORY;")
            cur.execute("PRAGMA cache_size=-%d;" % CACHE_PAGES_KIB)
            cur.execute("PRAGMA wal_autocheckpoint=1000;")       # ~1000 страниц между чекпойнтами
            cur.execute("PRAGMA mmap_size=%d;" % MMAP_SIZE_BYTES)
        else:
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
            if cx.is_apsw:
                cur = cx.conn.cursor()  # type: ignore[attr-defined]
                cur.executemany(sql, list(rows))
            else:
                cx.conn.executemany(sql, list(rows))  # type: ignore[attr-defined]

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
            "CREATE INDEX IF NOT EXISTS idx_deleted_events_peer_date ON deleted_message_events(peer_id, deleted_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_ai_history_chat_id ON ai_history(chat_id, id DESC);",
        ]
        with self._lock:
            for sql in ddl:
                self._exec(sql)
            try:
                self._exec("ALTER TABLE messages ADD COLUMN is_deleted INTEGER DEFAULT 0")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN forward_info TEXT")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN file_name TEXT")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN entities TEXT")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN duration INTEGER")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN waveform TEXT")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN reactions TEXT")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN poll TEXT")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN views INTEGER")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN forwards INTEGER")
            except Exception:
                pass
            try:
                self._exec("ALTER TABLE messages ADD COLUMN media_group_id TEXT")
            except Exception:
                pass

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

            forward_raw = m.get("forward_info")
            if isinstance(forward_raw, str):
                forward_serialized = forward_raw
            elif forward_raw is not None:
                try:
                    forward_serialized = json.dumps(forward_raw, ensure_ascii=False)
                except Exception:
                    forward_serialized = None
            else:
                forward_serialized = None

            waveform_raw = m.get("waveform")
            if isinstance(waveform_raw, str):
                waveform_serialized = waveform_raw
            elif waveform_raw is not None:
                try:
                    waveform_serialized = json.dumps(waveform_raw, ensure_ascii=False)
                except Exception:
                    waveform_serialized = None
            else:
                waveform_serialized = None

            entities_raw = m.get("entities")
            if isinstance(entities_raw, str):
                entities_serialized = entities_raw
            elif entities_raw is not None:
                try:
                    entities_serialized = json.dumps(entities_raw, ensure_ascii=False)
                except Exception:
                    entities_serialized = None
            else:
                entities_serialized = None

            duration_val = m.get("duration")
            try:
                duration_int = int(duration_val) if duration_val is not None else None
            except Exception:
                duration_int = None

            reactions_raw = m.get("reactions")
            if isinstance(reactions_raw, str):
                reactions_serialized = reactions_raw
            elif reactions_raw is not None:
                try:
                    reactions_serialized = json.dumps(reactions_raw, ensure_ascii=False)
                except Exception:
                    reactions_serialized = None
            else:
                reactions_serialized = None

            poll_raw = m.get("poll")
            if isinstance(poll_raw, str):
                poll_serialized = poll_raw
            elif poll_raw is not None:
                try:
                    poll_serialized = json.dumps(poll_raw, ensure_ascii=False)
                except Exception:
                    poll_serialized = None
            else:
                poll_serialized = None

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
            self._execmany(
                """
                INSERT INTO messages(peer_id,id,date,from_id,reply_to,message,media_type,media_id,is_deleted,forward_info,file_name,entities,duration,waveform,reactions,poll,views,forwards,media_group_id)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
            FROM peers p
            LEFT JOIN dialogs d ON d.peer_id = p.id
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
        out: List[Dict[str, Any]] = []
        for r in rows:
            sender_id = r[2]
            fwd_raw = r[8]
            if fwd_raw:
                try:
                    forward_info = json.loads(fwd_raw)
                except Exception:
                    forward_info = {"sender": fwd_raw}
            else:
                forward_info = None
            entities_raw = r[10]
            if entities_raw:
                try:
                    entities = json.loads(entities_raw)
                except Exception:
                    entities = None
            else:
                entities = None
            waveform_raw = r[16]
            if waveform_raw:
                try:
                    waveform = json.loads(waveform_raw)
                except Exception:
                    waveform = None
            else:
                waveform = None
            reactions_raw = r[17]
            if reactions_raw:
                try:
                    reactions = json.loads(reactions_raw)
                except Exception:
                    reactions = None
            else:
                reactions = None
            poll_raw = r[18]
            if poll_raw:
                try:
                    poll = json.loads(poll_raw)
                except Exception:
                    poll = None
            else:
                poll = None
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
                "entities": entities,
                "file_path": r[11],
                "file_size": r[12],
                "mime": r[13],
                "sender": r[14] or (str(sender_id) if sender_id is not None else ""),
                "thumb_path": None,
                "duration": r[15],
                "waveform": waveform,
                "reactions": reactions,
                "poll": poll,
                "views": r[19],
                "forwards": r[20],
                "media_group_id": r[21],
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
        if not rows:
            return None
        r = rows[0]
        fwd_raw = r[6]
        if fwd_raw:
            try:
                forward_info = json.loads(fwd_raw)
            except Exception:
                forward_info = {"sender": fwd_raw}
        else:
            forward_info = None
        entities_raw = r[8]
        if entities_raw:
            try:
                entities = json.loads(entities_raw)
            except Exception:
                entities = None
        else:
            entities = None
        waveform_raw = r[11]
        if waveform_raw:
            try:
                waveform = json.loads(waveform_raw)
            except Exception:
                waveform = None
        else:
            waveform = None
        reactions_raw = r[12]
        if reactions_raw:
            try:
                reactions = json.loads(reactions_raw)
            except Exception:
                reactions = None
        else:
            reactions = None
        poll_raw = r[13]
        if poll_raw:
            try:
                poll = json.loads(poll_raw)
            except Exception:
                poll = None
        else:
            poll = None
        return {
            "id": int(r[0]),
            "from_id": r[1],
            "reply_to": r[2],
            "text": r[3] or "",
            "type": r[4] or "text",
            "is_deleted": bool(r[5]),
            "forward_info": forward_info,
            "file_name": r[7],
            "entities": entities,
            "sender": r[9] or (str(r[1]) if r[1] is not None else ""),
            "duration": r[10],
            "waveform": waveform,
            "reactions": reactions,
            "poll": poll,
            "views": r[14],
            "forwards": r[15],
            "media_group_id": r[16],
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
        placeholders = ",".join(["?"] * len(mids))
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
            (int(peer_id), *mids),
        )
        out: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            try:
                mid = int(row[0])
            except Exception:
                continue
            fwd_raw = row[6]
            if fwd_raw:
                try:
                    forward_info = json.loads(fwd_raw)
                except Exception:
                    forward_info = {"sender": fwd_raw}
            else:
                forward_info = None
            entities_raw = row[8]
            if entities_raw:
                try:
                    entities = json.loads(entities_raw)
                except Exception:
                    entities = None
            else:
                entities = None
            waveform_raw = row[11]
            if waveform_raw:
                try:
                    waveform = json.loads(waveform_raw)
                except Exception:
                    waveform = None
            else:
                waveform = None
            reactions_raw = row[12]
            if reactions_raw:
                try:
                    reactions = json.loads(reactions_raw)
                except Exception:
                    reactions = None
            else:
                reactions = None
            poll_raw = row[13]
            if poll_raw:
                try:
                    poll = json.loads(poll_raw)
                except Exception:
                    poll = None
            else:
                poll = None
            out[mid] = {
                "id": mid,
                "from_id": row[1],
                "reply_to": row[2],
                "text": row[3] or "",
                "type": row[4] or "text",
                "is_deleted": bool(row[5]),
                "forward_info": forward_info,
                "file_name": row[7],
                "entities": entities,
                "sender": row[9] or (str(row[1]) if row[1] is not None else ""),
                "duration": row[10],
                "waveform": waveform,
                "reactions": reactions,
                "poll": poll,
                "views": row[14],
                "forwards": row[15],
                "media_group_id": row[16],
            }
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

        placeholders = ",".join(["?"] * len(mids))
        rows = self._query(
            f"""
            SELECT peer_id, id
            FROM messages
            WHERE id IN ({placeholders}) AND COALESCE(is_deleted, 0) = 0
            """,
            tuple(mids),
        )
        out: Dict[int, List[int]] = {}
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
            for match in EMOJI_RE.finditer(str(message_text or "")):
                emoji = match.group(0)
                if emoji in seen:
                    continue
                seen.add(emoji)
                ordered.append(emoji)
                if len(ordered) >= int(limit):
                    return ordered
        return ordered

    def get_chat_statistics(self, peer_id: int, *, limit: int = 500) -> Dict[str, Any]:
        rows = self._query(
            """
            SELECT
              id,
              media_type,
              is_deleted,
              reactions,
              poll,
              COALESCE(views, 0),
              COALESCE(forwards, 0)
            FROM messages
            WHERE peer_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(peer_id), int(max(limit, 1))),
        )
        total_messages = len(rows)
        media_messages = 0
        deleted_messages = 0
        total_views = 0
        total_forwards = 0
        total_reactions = 0
        reaction_counter: Counter[str] = Counter()
        polls: List[Dict[str, Any]] = []
        for _, media_type, is_deleted, reactions_raw, poll_raw, views, forwards in rows:
            if str(media_type or "text") != "text":
                media_messages += 1
            if bool(is_deleted):
                deleted_messages += 1
            total_views += int(views or 0)
            total_forwards += int(forwards or 0)
            if reactions_raw:
                try:
                    reactions = json.loads(reactions_raw)
                except Exception:
                    reactions = []
                if isinstance(reactions, list):
                    for item in reactions:
                        if not isinstance(item, dict):
                            continue
                        symbol = str(item.get("emoji") or item.get("title") or item.get("custom_emoji_id") or "").strip()
                        count = int(item.get("count") or 0)
                        if not symbol or count <= 0:
                            continue
                        reaction_counter[symbol] += count
                        total_reactions += count
            if poll_raw:
                try:
                    poll = json.loads(poll_raw)
                except Exception:
                    poll = None
                if isinstance(poll, dict):
                    polls.append(poll)
        return {
            "total_messages": total_messages,
            "media_messages": media_messages,
            "deleted_messages": deleted_messages,
            "total_views": total_views,
            "total_forwards": total_forwards,
            "total_reactions": total_reactions,
            "top_reactions": [{"emoji": key, "count": value} for key, value in reaction_counter.most_common(8)],
            "polls": polls[:10],
        }

    def get_message_statistics(self, peer_id: int, message_id: int) -> Dict[str, Any]:
        item = self.get_message_by_id(int(peer_id), int(message_id)) or {}
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
        return {
            "message": item,
            "deleted_snapshot": deleted_snapshot,
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
