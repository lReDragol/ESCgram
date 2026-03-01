from __future__ import annotations

from PySide6.QtCore import QObject, Signal, Slot


class ThumbWorker(QObject):
    done = Signal(int, str)  # message_id, path

    def __init__(self, server, chat_id: str, msg_id: int):
        super().__init__()
        self.server = server
        self.chat_id = chat_id
        self.msg_id = msg_id

    @Slot()
    def run(self):
        try:
            path = self.server.download_thumb(self.chat_id, self.msg_id) or ""
        except Exception:
            path = ""
        self.done.emit(self.msg_id, path)


class DownloadWorker(QObject):
    done = Signal(int, str)  # message_id, path

    def __init__(self, server, chat_id: str, msg_id: int):
        super().__init__()
        self.server = server
        self.chat_id = chat_id
        self.msg_id = msg_id

    @Slot()
    def run(self):
        try:
            path = self.server.download_media(self.chat_id, self.msg_id) or ""
        except Exception:
            path = ""
        self.done.emit(self.msg_id, path)

