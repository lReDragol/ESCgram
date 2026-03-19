from __future__ import annotations

import telegram as telegram_module
from telegram import TelegramAdapter


class _DummyThread:
    def __init__(self, *, alive: bool = True) -> None:
        self._alive = alive
        self.started = False
        self.join_calls = []

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout: float | None = None) -> None:
        self.join_calls.append(timeout)
        self._alive = False


def test_telegram_adapter_restarts_when_previous_thread_is_stale(monkeypatch) -> None:
    created_threads: list[_DummyThread] = []

    def _make_thread(*_args, **_kwargs):
        thread = _DummyThread(alive=True)
        created_threads.append(thread)
        return thread

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._thread = _DummyThread(alive=False)

    monkeypatch.setattr(telegram_module.threading, "Thread", _make_thread)

    adapter.start()

    assert created_threads
    assert adapter._thread is created_threads[0]
    assert created_threads[0].started is True


def test_telegram_adapter_stop_joins_thread_even_before_loop_ready() -> None:
    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._thread = _DummyThread(alive=True)
    adapter._loop_ready.set()

    adapter.stop()

    assert adapter._thread is None
