from __future__ import annotations

import json
from pathlib import Path

from server import ServerCore
from storage import Storage


def _make_storage(tmp_path: Path) -> Storage:
    db_path = tmp_path / "test_stats.db"
    st = Storage(str(db_path))
    st.connect()
    st.init_schema()
    return st


def test_storage_chat_statistics_supports_sender_breakdown_and_daily_series(tmp_path) -> None:
    st = _make_storage(tmp_path)
    try:
        st.upsert_peers(
            [
                {"id": 555, "type": "group", "title": "Big Group"},
                {"id": 1, "type": "user", "title": "Alice", "username": "alice"},
                {"id": 2, "type": "user", "title": "Bob", "username": "bob"},
            ]
        )
        st.upsert_messages(
            555,
            [
                {
                    "id": 1,
                    "date": 1710000000,
                    "from_id": 1,
                    "message": "one",
                    "media_type": "text",
                    "views": 10,
                    "reactions": [{"emoji": "👍", "count": 2}],
                },
                {
                    "id": 2,
                    "date": 1710003600,
                    "from_id": 1,
                    "message": "two",
                    "media_type": "image",
                    "forwards": 1,
                },
                {
                    "id": 3,
                    "date": 1710086400,
                    "from_id": 2,
                    "message": "three",
                    "media_type": "text",
                    "is_deleted": True,
                    "poll": {
                        "question": "Vote",
                        "is_closed": False,
                        "total_voter_count": 7,
                        "options": [{"text": "A", "voter_count": 4}],
                    },
                },
                {
                    "id": 4,
                    "date": 1710090000,
                    "from_id": 1,
                    "message": "four",
                    "media_type": "text",
                    "reactions": [{"emoji": "🔥", "count": 1}],
                },
            ],
        )

        stats = st.get_chat_statistics(
            555,
            limit=0,
            sender_limit=0,
            daily_limit=0,
            include_sender_daily=True,
        )

        assert stats["total_messages"] == 4
        assert stats["total_senders"] == 2
        assert stats["top_senders"][0]["sender_id"] == 1
        assert stats["top_senders"][0]["count"] == 3
        assert stats["sender_details"][0]["media_messages"] == 1
        assert stats["sender_details"][1]["deleted_messages"] == 1
        assert stats["total_reactions"] == 3
        assert stats["polls_summary"]["total_polls"] == 1
        assert [row["count"] for row in stats["daily_activity"]] == [2, 2]
        assert stats["sender_daily_activity"]["1"][0]["count"] == 2
        assert stats["sender_daily_activity"]["1"][1]["count"] == 1
        assert "distribution" in stats
        assert "risk" in stats
        assert "activity_summary" in stats
        assert stats["sender_details"][0]["risk_score"] >= 0
    finally:
        st.close()


def test_server_exports_chat_statistics_bundle_with_html_and_csv(tmp_path) -> None:
    st = _make_storage(tmp_path)
    try:
        st.upsert_peers(
            [
                {"id": 888, "type": "group", "title": "Export Group"},
                {"id": 10, "type": "user", "title": "Dora", "username": "dora"},
            ]
        )
        st.upsert_messages(
            888,
            [
                {
                    "id": 1,
                    "date": 1711000000,
                    "from_id": 10,
                    "message": "hello",
                    "media_type": "text",
                    "reactions": [{"emoji": "❤️", "count": 5}],
                },
                {
                    "id": 2,
                    "date": 1711003600,
                    "from_id": 10,
                    "message": "world",
                    "media_type": "video",
                },
            ],
        )

        server = ServerCore.__new__(ServerCore)
        server._storage = st
        server._tg_adapter = None

        result = ServerCore.export_chat_statistics_report(
            server,
            "888",
            output_path=str(tmp_path / "export_report.html"),
            title="Export Group",
        )

        assert result["ok"] is True
        html_path = Path(result["html_path"])
        json_path = Path(result["json_path"])
        users_csv_path = Path(result["users_csv_path"])
        sender_daily_csv_path = Path(result["sender_daily_csv_path"])
        suspicious_csv_path = Path(result["suspicious_csv_path"])
        risk_csv_path = Path(result["risk_csv_path"])

        assert html_path.is_file()
        assert json_path.is_file()
        assert users_csv_path.is_file()
        assert sender_daily_csv_path.is_file()
        assert suspicious_csv_path.is_file()
        assert risk_csv_path.is_file()
        html_text = html_path.read_text(encoding="utf-8")
        assert "<svg" in html_text
        assert "Export Group" in html_text
        assert "Dora" in html_text
        assert "Risk score" in html_text
        exported = json.loads(json_path.read_text(encoding="utf-8"))
        assert exported["total_messages"] == 2
        assert "risk" in exported
        users_csv = users_csv_path.read_text(encoding="utf-8-sig")
        assert "sender_id,name,username" in users_csv
        assert "Dora" in users_csv
    finally:
        st.close()
