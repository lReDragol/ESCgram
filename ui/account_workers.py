from __future__ import annotations

from typing import Any, Dict

from PySide6.QtCore import QObject, Signal, Slot


class AccountProfileWorker(QObject):
    done = Signal(dict)  # meta

    def __init__(self, tg_adapter: Any):
        super().__init__()
        self.tg = tg_adapter

    @Slot()
    def run(self) -> None:
        meta: Dict[str, object] = {}
        try:
            if self.tg and hasattr(self.tg, "refresh_active_account_profile"):
                meta = self.tg.refresh_active_account_profile() or {}
            elif self.tg and hasattr(self.tg, "get_active_account_meta"):
                meta = self.tg.get_active_account_meta() or {}
        except Exception:
            meta = {}
        self.done.emit(dict(meta))

