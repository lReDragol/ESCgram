from __future__ import annotations

import json
from typing import Optional

from PySide6.QtCore import QObject, Signal, Slot


class OllamaPullWorker(QObject):
    # Use object for counters: int overflows on very large model layers in shiboken int conversion.
    progress = Signal(str, object, object)  # status, completed, total
    finished = Signal(bool, str)      # success, message

    def __init__(self, model: str, *, base_url: str = "http://localhost:11434") -> None:
        super().__init__()
        self.model = str(model or "").strip()
        self.base_url = str(base_url or "http://localhost:11434").rstrip("/")

    @Slot()
    def run(self) -> None:
        if not self.model:
            self.finished.emit(False, "Model name is empty")
            return
        try:
            import requests

            url = f"{self.base_url}/api/pull"
            resp = requests.post(url, json={"name": self.model}, stream=True, timeout=(5, None))
            resp.raise_for_status()

            last_status = ""
            last_total = 0
            last_done = 0
            success = False
            error_message = ""

            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                stream_error = str(payload.get("error") or "").strip()
                if stream_error:
                    error_message = stream_error
                    break
                status = str(payload.get("status") or "")
                if status:
                    last_status = status
                total = payload.get("total")
                completed = payload.get("completed")
                try:
                    if total is not None:
                        last_total = int(total)
                    if completed is not None:
                        last_done = int(completed)
                except Exception:
                    pass

                self.progress.emit(last_status, last_done, last_total)

                if payload.get("status") == "success":
                    success = True

            if error_message:
                self.finished.emit(False, error_message)
                return
            if success:
                self.finished.emit(True, f"{self.model} downloaded")
            else:
                fallback = last_status or f"{self.model} pull finished"
                self.finished.emit(False, fallback)
        except Exception as exc:
            self.finished.emit(False, str(exc))


class OllamaTagsWorker(QObject):
    done = Signal(list)     # List[str]
    failed = Signal(str)

    def __init__(self, *, base_url: str = "http://localhost:11434") -> None:
        super().__init__()
        self.base_url = str(base_url or "http://localhost:11434").rstrip("/")

    @Slot()
    def run(self) -> None:
        try:
            import requests

            url = f"{self.base_url}/api/tags"
            resp = requests.get(url, timeout=(0.6, 2.0))
            resp.raise_for_status()
            data = resp.json()
            models = [m.get("name") for m in data.get("models", []) if m.get("name")]
            self.done.emit(list(models))
        except Exception as exc:
            self.failed.emit(str(exc))
            self.done.emit([])
