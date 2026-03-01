from __future__ import annotations

from typing import Any, Dict, List

from ui.dialog_workers import HistoryWorker


class _FakeServer:
    def fetch_chat_history_cached(self, chat_id: str, limit: int = 60) -> List[Dict[str, Any]]:
        _ = (chat_id, limit)
        return [
            {"id": 5, "text": "old", "type": "text", "is_deleted": False},
            {"id": 4, "text": "same", "type": "text", "is_deleted": False},
        ]

    def fetch_chat_history(self, chat_id: str, limit: int = 80, download_media: bool = False, timeout: float = 25.0) -> List[Dict[str, Any]]:
        _ = (chat_id, limit, download_media, timeout)
        return [
            {"id": 6, "text": "new", "type": "text", "is_deleted": False},
            {"id": 5, "text": "updated", "type": "text", "is_deleted": False},
            {"id": 4, "text": "same", "type": "text", "is_deleted": False},
        ]


def test_history_worker_skips_unchanged_remote_messages() -> None:
    worker = HistoryWorker(_FakeServer(), "123", limit=20, batch_size=20)
    batches: List[List[Dict[str, Any]]] = []
    done_called = {"value": False}

    worker.batch.connect(lambda chunk: batches.append(list(chunk)))
    worker.finished.connect(lambda: done_called.__setitem__("value", True))

    worker.run()

    ids = [int(item.get("id") or 0) for chunk in batches for item in chunk]
    assert ids.count(4) == 1  # unchanged id=4 only from cached phase
    assert ids.count(5) == 2  # cached + changed remote update
    assert ids.count(6) == 1  # new remote message
    assert done_called["value"] is True


class _FakeServerEntities:
    def fetch_chat_history_cached(self, chat_id: str, limit: int = 60) -> List[Dict[str, Any]]:
        _ = (chat_id, limit)
        return [
            {"id": 10, "text": "hello @user", "type": "text", "entities": None, "is_deleted": False},
        ]

    def fetch_chat_history(
        self,
        chat_id: str,
        limit: int = 80,
        download_media: bool = False,
        timeout: float = 25.0,
    ) -> List[Dict[str, Any]]:
        _ = (chat_id, limit, download_media, timeout)
        return [
            {
                "id": 10,
                "text": "hello @user",
                "type": "text",
                "entities": [{"type": "mention", "offset": 6, "length": 5}],
                "is_deleted": False,
            },
        ]


def test_history_worker_emits_entity_updates_even_if_text_is_same() -> None:
    worker = HistoryWorker(_FakeServerEntities(), "777", limit=20, batch_size=20)
    batches: List[List[Dict[str, Any]]] = []

    worker.batch.connect(lambda chunk: batches.append(list(chunk)))
    worker.run()

    ids = [int(item.get("id") or 0) for chunk in batches for item in chunk]
    assert ids.count(10) == 2  # cached + remote entity refresh
