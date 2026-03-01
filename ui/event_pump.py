from __future__ import annotations

from PySide6.QtCore import QObject, Signal, Slot


class EventPump(QObject):
    gui_ai_message = Signal(str, str)          # chat_id, text
    gui_user_echo = Signal(str, str, str, object)     # chat_id, text, user_id, payload
    gui_info = Signal(str)
    gui_touch_dialog = Signal(str, int)       # chat_id, ts
    gui_media = Signal(str, dict)             # chat_id, payload
    gui_media_progress = Signal(str, dict)    # chat_id, payload
    gui_peer_message = Signal(str, dict)      # chat_id, payload
    gui_messages_deleted = Signal(str, list)  # chat_id, message_ids

    def __init__(self, server):
        super().__init__()
        self._server = server
        self._running = True

    @Slot()
    def run(self):
        from queue import Empty

        q = self._server.events
        while self._running:
            try:
                evt = q.get(timeout=0.2)
            except Empty:
                continue
            t = evt.get("type")
            if t == "gui_ai_message":
                self.gui_ai_message.emit(str(evt.get("chat_id", "")), str(evt.get("text", "")))
            elif t == "gui_user_echo":
                self.gui_user_echo.emit(
                    str(evt.get("chat_id", "")),
                    str(evt.get("text", "")),
                    str(evt.get("user_id", "")),
                    dict(evt),
                )
            elif t == "gui_info":
                self.gui_info.emit(str(evt.get("text", "")))
            elif t == "gui_touch_dialog":
                self.gui_touch_dialog.emit(str(evt.get("chat_id", "")), int(evt.get("ts") or 0))
            elif t == "gui_media":
                self.gui_media.emit(str(evt.get("chat_id", "")), dict(evt))
            elif t == "gui_media_progress":
                self.gui_media_progress.emit(str(evt.get("chat_id", "")), dict(evt))
            elif t == "gui_message":
                payload = evt.get("payload") or {}
                if not isinstance(payload, dict):
                    payload = {}
                self.gui_peer_message.emit(str(evt.get("chat_id", "")), payload)
            elif t == "gui_messages_deleted":
                mids = evt.get("message_ids")
                if not isinstance(mids, list):
                    mids = []
                self.gui_messages_deleted.emit(str(evt.get("chat_id", "")), [int(m) for m in mids])
