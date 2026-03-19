from __future__ import annotations

from PySide6.QtWidgets import QApplication

from ui.media_render import _can_generate_local_video_preview
from ui.message_widgets import RichTextLabel, _CUSTOM_EMOJI_BUS


def _ensure_app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_custom_emoji_bus_accepts_large_ids_without_overflow(monkeypatch) -> None:
    _ensure_app()
    label = RichTextLabel("")
    large_id = 5285396701601881062
    label._custom_emoji_ids = {large_id}
    calls = {"count": 0}

    monkeypatch.setattr(
        label,
        "_render_current",
        lambda: calls.__setitem__("count", int(calls["count"]) + 1),
    )

    _CUSTOM_EMOJI_BUS.resolved.emit(str(large_id))

    assert calls["count"] == 1


def test_local_video_preview_skips_temp_files(tmp_path) -> None:
    temp_file = tmp_path / "clip.temp"
    temp_file.write_bytes(b"not-a-real-video")
    mp4_file = tmp_path / "clip.mp4"
    mp4_file.write_bytes(b"not-a-real-video")

    assert _can_generate_local_video_preview(str(temp_file)) is False
    assert _can_generate_local_video_preview(str(mp4_file)) is True
