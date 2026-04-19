from __future__ import annotations

from typing import Any, Callable, Optional

from PySide6.QtCore import QCoreApplication, QObject, Qt, QThread, QTimer, Signal, Slot


class _GuiInvoker(QObject):
    invoke = Signal(object)

    def __init__(self) -> None:
        super().__init__(None)
        self.invoke.connect(self._run, Qt.ConnectionType.QueuedConnection)

    @Slot(object)
    def _run(self, callback: object) -> None:
        if callable(callback):
            try:
                callback()
            except Exception:
                pass


_GUI_INVOKER: Optional[_GuiInvoker] = None


def _get_gui_thread() -> Optional[QThread]:
    app = QCoreApplication.instance()
    if app is None:
        return None
    return app.thread()


def _get_gui_invoker() -> Optional[_GuiInvoker]:
    app = QCoreApplication.instance()
    if app is None:
        return None
    global _GUI_INVOKER
    if _GUI_INVOKER is None:
        _GUI_INVOKER = _GuiInvoker()
        try:
            _GUI_INVOKER.moveToThread(app.thread())
        except Exception:
            pass
    return _GUI_INVOKER


def invoke_in_gui_thread(callback: Callable[[], Any]) -> None:
    if not callable(callback):
        return
    gui_thread = _get_gui_thread()
    if gui_thread is None:
        callback()
        return
    if QThread.currentThread() == gui_thread:
        callback()
        return
    invoker = _get_gui_invoker()
    if invoker is None:
        callback()
        return
    invoker.invoke.emit(callback)


def single_shot_in_gui(delay_ms: int, callback: Callable[[], Any]) -> None:
    delay = max(0, int(delay_ms or 0))

    def _schedule() -> None:
        QTimer.singleShot(delay, callback)

    invoke_in_gui_thread(_schedule)
