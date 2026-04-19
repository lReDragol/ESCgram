from __future__ import annotations

from pathlib import Path

from server import ServerCore
from storage import Storage
from utils.community_scan_report import build_community_scan_report, build_community_scan_stylesheet


def test_storage_roundtrip_community_scan_run(tmp_path: Path) -> None:
    db_path = tmp_path / "scan.db"
    storage = Storage(str(db_path))
    storage.connect()
    storage.init_schema()
    try:
        payload = {
            "scan_key": "strbypass",
            "query": "t.me/strbypass",
            "summary": {"total_chats": 2, "accessible_chats": 1},
            "chats": [{"chat_id": "-1001", "title": "Root"}],
        }
        storage.save_community_scan_run(
            "strbypass",
            query="t.me/strbypass",
            payload=payload,
            title="strbypass",
            root_chat_id=-1001,
            updated_at=123456,
        )

        loaded = storage.get_community_scan_run("strbypass")
    finally:
        storage.close()

    assert loaded["scan_key"] == "strbypass"
    assert loaded["query"] == "t.me/strbypass"
    assert loaded["root_chat_id"] == -1001
    assert loaded["summary"]["total_chats"] == 2


def test_build_community_scan_report_renders_russian_sections() -> None:
    html = build_community_scan_report(
        "Отчёт STR",
        {
            "scan_key": "strbypass",
            "query": "t.me/strbypass",
            "completed_at": 123456,
            "generated_at": 123457,
            "notes": ["Корневой чат закрыт"],
            "edges": [{"source": "1", "target": "2", "label": "telegram-link"}],
            "summary": {
                "total_chats": 2,
                "accessible_chats": 1,
                "new_messages": 32,
                "total_messages": 128,
                "users": 4,
                "suspicious_users": 1,
                "risk_score_avg": 24,
                "temporary_joins": 1,
                "root_title": "MIX",
                "root_access": "forbidden",
            },
            "chats": [
                {
                    "chat_id": "1",
                    "title": "MIX",
                    "type": "channel",
                    "accessible": False,
                    "is_root": True,
                    "stats": {"daily_activity": [], "hourly_activity": [], "top_senders": [], "risk": {"score": 22}},
                },
                {
                    "chat_id": "2",
                    "title": "Discussion",
                    "type": "supergroup",
                    "username": "strbypasss",
                    "accessible": True,
                    "new_messages": 32,
                    "stats": {
                        "total_messages": 128,
                        "total_senders": 4,
                        "daily_activity": [{"day": "2026-04-07", "count": 128}],
                        "hourly_activity": [{"hour": 10, "count": 64}],
                        "top_senders": [{"name": "user", "count": 64}],
                        "risk": {"score": 22},
                    },
                },
            ],
            "users": [
                {
                    "sender_id": 1,
                    "name": "User One",
                    "username": "user1",
                    "type": "private",
                    "count": 64,
                    "chat_count": 1,
                    "media_messages": 3,
                    "deleted_messages": 1,
                    "total_reactions": 4,
                    "last_date": 123456,
                    "risk_score": 20,
                    "risk_flags": ["special_symbol_sender"],
                }
            ],
        },
        css_href="report.css",
    )

    css = build_community_scan_stylesheet()

    assert "Аналитический workspace" in html
    assert "Чаты и каналы" in html
    assert "Боты и риск" in html
    assert "Граф связей сообщества" in html
    assert "report.css" in html
    assert ".layout" in css
    assert ".sidebar" in css
    assert ".workspace-body" in css


def test_iter_telegram_refs_ignores_plain_words() -> None:
    refs = ServerCore._iter_telegram_refs(
        "official portal community",
        "MIX КИБЕРПОРТАЛ",
        "@strbypass",
        "https://t.me/strbypasss",
    )

    assert refs == ["https://t.me/strbypass", "https://t.me/strbypasss"]


def test_exact_scan_query_match_rejects_fuzzy_username() -> None:
    assert ServerCore._is_exact_scan_query_match(
        {"username": "strbypasss", "title": "Discussion"},
        "strbypass",
    ) is False
    assert ServerCore._is_exact_scan_query_match(
        {"username": "STR_BYPASS", "title": "Киберпортал"},
        "strbypass",
    ) is True


def test_get_dialogs_for_ui_returns_only_real_dialog_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "dialogs.db"
    storage = Storage(str(db_path))
    storage.connect()
    storage.init_schema()
    try:
        storage.upsert_peers(
            [
                {"id": 1, "type": "private", "username": "one", "title": "One"},
                {"id": 2, "type": "bot", "username": "two_bot", "title": "Two"},
            ]
        )
        storage.upsert_dialogs(
            [
                {"peer_id": 1, "last_message_date": 123, "unread_count": 2, "pinned": 1},
            ]
        )

        rows = storage.get_dialogs_for_ui(limit=20)
    finally:
        storage.close()

    assert rows == [
        {
            "id": "1",
            "type": "private",
            "username": "one",
            "title": "One",
            "photo_small_id": None,
            "last_message_date": 123,
            "unread_count": 2,
            "pinned": True,
        }
    ]


def test_scan_selected_community_returns_error_when_root_unresolved(tmp_path: Path) -> None:
    db_path = tmp_path / "scan.db"
    storage = Storage(str(db_path))
    storage.connect()
    storage.init_schema()
    try:
        server = ServerCore()
        server._storage = storage
        server._tg_auth_cached = True
        server._tg_auth_cache_until = 10**9

        class _FakeAdapter:
            def get_public_lookup_status(self):
                return {"blocked": True, "remaining_seconds": 123, "reason": "contacts.ResolveUsername"}

        server._tg_adapter = _FakeAdapter()
        server.search_public_peers = lambda *_args, **_kwargs: []
        server.resolve_chat_reference = lambda *_args, **_kwargs: None
        server.get_chat_full_info = lambda *_args, **_kwargs: {}

        result = server.scan_selected_community("https://t.me/deeper_connect_ru_chat")
    finally:
        storage.close()

    assert result["ok"] is False
    assert "deeper_connect_ru_chat" in result["error"]
    assert "123" in result["error"]
