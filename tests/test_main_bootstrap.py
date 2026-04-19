from __future__ import annotations

import sys
import types

import pytest

import main as main_module


class _DummyLock:
    def __init__(self) -> None:
        self.unlocked = False

    def unlock(self) -> None:
        self.unlocked = True


class _FakeServer:
    last_instance = None

    def __init__(self, service_token=None) -> None:
        self.service_token = service_token
        self.started = False
        self.stopped = False
        _FakeServer.last_instance = self

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def set_telegram_adapter(self, _tg) -> None:
        return None


class _FailingTelegramAdapter:
    last_instance = None

    def __init__(self) -> None:
        self.server = None
        self.stop_called = False
        _FailingTelegramAdapter.last_instance = self

    def set_server(self, server) -> None:
        self.server = server

    def start(self) -> None:
        raise RuntimeError("telegram start failed")

    def stop(self) -> None:
        self.stop_called = True


def test_main_cleans_up_server_and_lock_when_telegram_start_fails(monkeypatch, tmp_path) -> None:
    lock = _DummyLock()

    monkeypatch.setattr(main_module, "_acquire_single_instance_lock", lambda: lock)
    monkeypatch.setattr(main_module, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_module.app_paths, "logs_dir", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["main.py"])

    monkeypatch.setitem(sys.modules, "server", types.SimpleNamespace(ServerCore=_FakeServer))
    monkeypatch.setitem(sys.modules, "telegram", types.SimpleNamespace(TelegramAdapter=_FailingTelegramAdapter))
    monkeypatch.setitem(sys.modules, "gui_chat", types.SimpleNamespace(run_gui=lambda *_args, **_kwargs: None))

    with pytest.raises(RuntimeError, match="telegram start failed"):
        main_module.main()

    assert _FakeServer.last_instance is not None
    assert _FakeServer.last_instance.started is True
    assert _FakeServer.last_instance.stopped is True
    assert _FailingTelegramAdapter.last_instance is not None
    assert _FailingTelegramAdapter.last_instance.stop_called is True
    assert lock.unlocked is True
