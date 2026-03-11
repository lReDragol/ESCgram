from __future__ import annotations

import os
import subprocess
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QObject, Signal, Slot


class FfmpegConvertWorker(QObject):
    done = Signal(dict)

    def __init__(self, cmd: List[str], output_path: str, timeout_sec: float = 300.0):
        super().__init__()
        self.cmd = list(cmd)
        self.output_path = output_path
        self.timeout_sec = max(1.0, float(timeout_sec))

    @Slot()
    def run(self) -> None:
        payload: Dict[str, Any] = {
            "ok": False,
            "output_path": self.output_path,
            "error": "",
        }
        try:
            subprocess.run(
                self.cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
                timeout=self.timeout_sec,
            )
            if not (os.path.isfile(self.output_path) and os.path.getsize(self.output_path) > 0):
                raise RuntimeError("Конвертация не дала результата")
            payload["ok"] = True
        except subprocess.TimeoutExpired:
            payload["error"] = "Конвертация заняла слишком много времени и была прервана."
        except subprocess.CalledProcessError as exc:
            payload["error"] = f"ffmpeg завершился с ошибкой (код {exc.returncode})."
        except Exception as exc:
            payload["error"] = str(exc)
        self.done.emit(payload)


class MediaSendWorker(QObject):
    done = Signal(dict)

    def __init__(
        self,
        tg_adapter: Any,
        *,
        kind: str,
        chat_id: str,
        media_path: str,
        reply_to: int | None = None,
        timeout_sec: float = 180.0,
    ):
        super().__init__()
        self.tg = tg_adapter
        self.kind = str(kind or "").strip().lower()
        self.chat_id = str(chat_id or "").strip()
        self.media_path = media_path
        self.reply_to = int(reply_to) if reply_to is not None else None
        self.timeout_sec = max(1.0, float(timeout_sec))

    @Slot()
    def run(self) -> None:
        payload: Dict[str, Any] = {
            "ok": False,
            "kind": self.kind,
            "error": "",
        }
        try:
            if not self.chat_id:
                raise RuntimeError("Chat ID is empty")
            if self.kind == "voice":
                payload["ok"] = bool(
                    self.tg.send_voice_sync(
                        chat_id=self.chat_id,
                        voice_path=self.media_path,
                        reply_to=self.reply_to,
                        timeout=self.timeout_sec,
                    )
                )
            elif self.kind == "video_note":
                payload["ok"] = bool(
                    self.tg.send_video_note_sync(
                        chat_id=self.chat_id,
                        video_note_path=self.media_path,
                        length=480,
                        reply_to=self.reply_to,
                        timeout=self.timeout_sec,
                    )
                )
            else:
                raise RuntimeError(f"Unsupported media kind: {self.kind}")
            if not payload["ok"]:
                payload["error"] = "Отправка не подтверждена Telegram API."
        except Exception as exc:
            payload["error"] = str(exc)
        self.done.emit(payload)


class MediaBatchSendWorker(QObject):
    done = Signal(dict)

    def __init__(
        self,
        tg_adapter: Any,
        *,
        chat_id: str,
        items: List[Dict[str, Any]],
        reply_to: Optional[int] = None,
        timeout_sec: float = 240.0,
    ):
        super().__init__()
        self.tg = tg_adapter
        self.chat_id = str(chat_id or "").strip()
        self.items = list(items or [])
        self.reply_to = int(reply_to) if reply_to is not None else None
        self.timeout_sec = max(1.0, float(timeout_sec))

    @staticmethod
    def _normalize_kind(raw: Any) -> str:
        kind = str(raw or "").strip().lower()
        mapping = {
            "photo": "image",
            "gif": "animation",
            "file": "document",
        }
        return mapping.get(kind, kind or "document")

    def _send_one(self, item: Dict[str, Any], *, reply_to: Optional[int]) -> bool:
        kind = self._normalize_kind(item.get("kind"))
        path = str(item.get("path") or "")
        caption = str(item.get("caption") or "").strip() or None
        if not (path and os.path.isfile(path)):
            return False

        try:
            if kind == "image":
                sender = getattr(self.tg, "send_photo_sync", None)
                if callable(sender):
                    ok = bool(
                        sender(
                            chat_id=self.chat_id,
                            photo_path=path,
                            caption=caption,
                            reply_to=reply_to,
                            timeout=self.timeout_sec,
                        )
                    )
                    if ok:
                        return True
                sender = getattr(self.tg, "send_document_sync", None)
                if callable(sender):
                    return bool(
                        sender(
                            chat_id=self.chat_id,
                            document_path=path,
                            caption=caption,
                            reply_to=reply_to,
                            timeout=self.timeout_sec,
                        )
                    )
                return False

            if kind == "video":
                sender = getattr(self.tg, "send_video_sync", None)
                if callable(sender):
                    ok = bool(
                        sender(
                            chat_id=self.chat_id,
                            video_path=path,
                            caption=caption,
                            reply_to=reply_to,
                            timeout=self.timeout_sec,
                        )
                    )
                    if ok:
                        return True
                sender = getattr(self.tg, "send_document_sync", None)
                if callable(sender):
                    return bool(
                        sender(
                            chat_id=self.chat_id,
                            document_path=path,
                            caption=caption,
                            reply_to=reply_to,
                            timeout=self.timeout_sec,
                        )
                    )
                return False

            if kind == "animation":
                sender = getattr(self.tg, "send_animation_sync", None)
                if callable(sender):
                    ok = bool(
                        sender(
                            chat_id=self.chat_id,
                            animation_path=path,
                            caption=caption,
                            reply_to=reply_to,
                            timeout=self.timeout_sec,
                        )
                    )
                    if ok:
                        return True
                sender = getattr(self.tg, "send_document_sync", None)
                if callable(sender):
                    return bool(
                        sender(
                            chat_id=self.chat_id,
                            document_path=path,
                            caption=caption,
                            reply_to=reply_to,
                            timeout=self.timeout_sec,
                        )
                    )
                return False

            if kind == "voice":
                sender = getattr(self.tg, "send_voice_sync", None)
                if callable(sender):
                    return bool(
                        sender(
                            chat_id=self.chat_id,
                            voice_path=path,
                            reply_to=reply_to,
                            timeout=self.timeout_sec,
                        )
                    )
                return False

            if kind == "video_note":
                sender = getattr(self.tg, "send_video_note_sync", None)
                if callable(sender):
                    return bool(
                        sender(
                            chat_id=self.chat_id,
                            video_note_path=path,
                            reply_to=reply_to,
                            timeout=self.timeout_sec,
                        )
                    )
                return False

            if kind == "audio":
                sender = getattr(self.tg, "send_audio_sync", None)
                if callable(sender):
                    ok = bool(
                        sender(
                            chat_id=self.chat_id,
                            audio_path=path,
                            caption=caption,
                            timeout=self.timeout_sec,
                        )
                    )
                    if ok:
                        return True
                sender = getattr(self.tg, "send_document_sync", None)
                if callable(sender):
                    return bool(
                        sender(
                            chat_id=self.chat_id,
                            document_path=path,
                            caption=caption,
                            reply_to=reply_to,
                            timeout=self.timeout_sec,
                        )
                    )
                return False

            sender = getattr(self.tg, "send_document_sync", None)
            if callable(sender):
                return bool(
                    sender(
                        chat_id=self.chat_id,
                        document_path=path,
                        caption=caption,
                        reply_to=reply_to,
                        timeout=self.timeout_sec,
                    )
                )
        except Exception:
            return False
        return False

    @Slot()
    def run(self) -> None:
        payload: Dict[str, Any] = {
            "ok": False,
            "error": "",
            "results": [],
            "grouped": False,
            "message_ids": [],
        }
        try:
            if not self.chat_id:
                raise RuntimeError("Chat ID is empty")
            prepared: List[Dict[str, Any]] = []
            for index, raw in enumerate(self.items):
                path = str(raw.get("path") or "")
                if not (path and os.path.isfile(path)):
                    payload["results"].append(
                        {
                            "index": index,
                            "kind": self._normalize_kind(raw.get("kind")),
                            "path": path,
                            "ok": False,
                            "message_id": None,
                        }
                    )
                    continue
                prepared.append(
                    {
                        "index": index,
                        "kind": self._normalize_kind(raw.get("kind")),
                        "path": path,
                        "caption": str(raw.get("caption") or ""),
                    }
                )

            if not prepared:
                raise RuntimeError("No files to send")

            can_group = (
                len(prepared) > 1
                and all(item["kind"] in {"image", "video"} for item in prepared)
                and callable(getattr(self.tg, "send_media_group_sync", None))
            )

            grouped_success_count = 0
            if can_group:
                mids = self.tg.send_media_group_sync(
                    chat_id=self.chat_id,
                    items=prepared,
                    reply_to=self.reply_to,
                    timeout=self.timeout_sec,
                )
                if isinstance(mids, list) and mids:
                    payload["grouped"] = True
                    payload["message_ids"] = [int(mid) for mid in mids if mid is not None]
                    grouped_success_count = min(len(prepared), len(payload["message_ids"]))
                    for idx, item in enumerate(prepared):
                        mid = int(mids[idx]) if idx < len(mids) and mids[idx] is not None else None
                        payload["results"].append(
                            {
                                "index": int(item["index"]),
                                "kind": item["kind"],
                                "path": item["path"],
                                "ok": mid is not None,
                                "message_id": mid,
                            }
                        )
                    if len(mids) == len(prepared):
                        payload["ok"] = True
                        self.done.emit(payload)
                        return

            first_reply = self.reply_to if grouped_success_count == 0 else None
            sent_all = True
            for item in prepared[grouped_success_count:]:
                ok = bool(self._send_one(item, reply_to=first_reply))
                payload["results"].append(
                    {
                        "index": int(item["index"]),
                        "kind": item["kind"],
                        "path": item["path"],
                        "ok": ok,
                        "message_id": None,
                    }
                )
                sent_all = sent_all and ok
                first_reply = None
            payload["ok"] = sent_all
            if not sent_all:
                payload["error"] = "Не все файлы отправлены"
        except Exception as exc:
            payload["error"] = str(exc)
        self.done.emit(payload)
