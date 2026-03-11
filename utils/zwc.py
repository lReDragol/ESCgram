"""Zero-width encoding helpers.

v1 used 4 characters (2 bits each) which inflated payloads by 4x and could break long
photo captions. v2 switches to a nibble table (4 bits each) with a short prefix, cutting
size in half while keeping legacy messages decodable.
"""

import base64
import io
import json
import os
import secrets
import tempfile
import zlib

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

ZWC_TABLE_V1 = ["\u200b", "\u200c", "\u200d", "\u2060"]  # ZWSP, ZWNJ, ZWJ, WJ
ZWC_TO_BITS_V1 = {c: i for i, c in enumerate(ZWC_TABLE_V1)}

# v2: 4 bits per character -> 2x overhead instead of 4x
ZWC_V2_PREFIX = "\u2065"  # marker to disambiguate from legacy encoding
ZWC_TABLE_V2 = [
    "\u200b",
    "\u200c",
    "\u200d",
    "\u2060",
    "\u200e",
    "\u200f",
    "\u202a",
    "\u202b",
    "\u202c",
    "\u202d",
    "\u202e",
    "\u2061",
    "\u2062",
    "\u2063",
    "\u2064",
    "\ufeff",
]
ZWC_TO_NIBBLE_V2 = {c: i for i, c in enumerate(ZWC_TABLE_V2)}

ALL_ZWC = set(ZWC_TABLE_V1) | set(ZWC_TABLE_V2) | {ZWC_V2_PREFIX}


def is_zwc_only(s: str) -> bool:
    """Return True if string consists only of supported zero-width symbols."""
    return bool(s) and all(ch in ALL_ZWC for ch in s)


def contains_zwc(s: str) -> bool:
    """Return True if string contains at least one supported zero-width symbol."""
    if not s:
        return False
    return any(ch in ALL_ZWC for ch in s)


def _encode_v2(data: bytes) -> str:
    out: list[str] = [ZWC_V2_PREFIX]
    for b in data:
        out.append(ZWC_TABLE_V2[(b >> 4) & 0x0F])
        out.append(ZWC_TABLE_V2[b & 0x0F])
    return "".join(out)


def _decode_v1(s: str) -> bytes | None:
    if not s or not all(ch in ZWC_TO_BITS_V1 for ch in s):
        return None
    bits = [ZWC_TO_BITS_V1[ch] for ch in s]
    buf = bytearray()
    for i in range(0, len(bits), 4):
        if i + 3 >= len(bits):
            break
        val = (bits[i] << 6) | (bits[i + 1] << 4) | (bits[i + 2] << 2) | bits[i + 3]
        buf.append(val)
    return bytes(buf)


def _decode_v2(s: str) -> bytes | None:
    if s.startswith(ZWC_V2_PREFIX):
        s = s[len(ZWC_V2_PREFIX) :]
    if not s or len(s) % 2:
        return None
    if not all(ch in ZWC_TO_NIBBLE_V2 for ch in s):
        return None
    buf = bytearray()
    for i in range(0, len(s), 2):
        hi = ZWC_TO_NIBBLE_V2[s[i]]
        lo = ZWC_TO_NIBBLE_V2[s[i + 1]]
        buf.append((hi << 4) | lo)
    return bytes(buf)


def encode_zwc(text: str | bytes) -> str:
    """Encode UTF-8 text (or raw bytes) into a zero-width carrier string (v2)."""
    if not text:
        return ""
    payload = text.encode("utf-8") if isinstance(text, str) else bytes(text)
    return _encode_v2(payload)


def decode_zwc(s: str) -> str | None:
    """Decode zero-width carrier back to text; understands both v1 and v2."""
    if not is_zwc_only(s):
        return None

    raw: bytes | None = None

    # Prefer v2 if prefix is present or v1 markers don't make sense.
    raw = _decode_v2(s)
    if raw is None and set(s).issubset(set(ZWC_TABLE_V1)) and len(s) % 4 == 0:
        raw = _decode_v1(s)

    if raw is None:
        return None

    try:
        return raw.decode("utf-8")
    except Exception:
        try:
            import base64

            decoded = base64.b64decode(raw, validate=False)
            return decoded.decode("utf-8", "ignore")
        except Exception:
            return None


def _utf16_len(text: str) -> int:
    total = 0
    for ch in text:
        total += 2 if ord(ch) > 0xFFFF else 1
    return total


def encode_caret_hidden_fragments(text: str) -> tuple[str, bool]:
    """
    Convert ^secret^ markers into embedded ZWC runs (so the secret is invisible in Telegram).

    Unmatched caret markers are preserved literally.
    Escape literal caret with \\^.
    Returns (encoded_text, had_hidden_fragments).
    """
    if not text:
        return "", False

    out: list[str] = []
    in_hidden = False
    buf: list[str] = []
    had_hidden = False

    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "\\" and i + 1 < n and text[i + 1] == "^":
            if in_hidden:
                buf.append("^")
            else:
                out.append("^")
            i += 2
            continue
        if ch == "^":
            if in_hidden:
                payload = "".join(buf)
                if payload:
                    out.append(encode_zwc(payload))
                    had_hidden = True
                in_hidden = False
                buf = []
            else:
                in_hidden = True
                buf = []
            i += 1
            continue
        if in_hidden:
            buf.append(ch)
        else:
            out.append(ch)
        i += 1

    if in_hidden:
        # Unmatched opener: keep it literal.
        out.append("^" + "".join(buf))

    return "".join(out), had_hidden


def reveal_zwc_fragments_with_entities(text: str) -> tuple[str, list[dict], bool]:
    """
    Replace embedded ZWC runs inside normal text with decoded payload and return entities for highlighting.

    Returns: (display_text, entities, has_hidden)
    Entities use UTF-16 offsets/lengths like Telegram.
    """
    if not text:
        return "", [], False

    if is_zwc_only(text):
        decoded = decode_zwc(text)
        if not decoded:
            placeholder = "(невидимое сообщение — не распознано)"
            return placeholder, [], True
        return decoded, [{"type": "hidden", "offset": 0, "length": _utf16_len(decoded)}], True

    out: list[str] = []
    entities: list[dict] = []
    utf16_pos = 0
    saw_run = False

    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch not in ALL_ZWC:
            out.append(ch)
            utf16_pos += 2 if ord(ch) > 0xFFFF else 1
            i += 1
            continue
        j = i + 1
        while j < n and text[j] in ALL_ZWC:
            j += 1
        run = text[i:j]
        saw_run = True
        decoded = decode_zwc(run)
        if decoded:
            start = utf16_pos
            out.append(decoded)
            ln = _utf16_len(decoded)
            entities.append({"type": "hidden", "offset": start, "length": ln})
            utf16_pos += ln
        i = j

    display = "".join(out)
    has_hidden = bool(entities) or saw_run
    return display, entities, has_hidden


def encrypt_file(path: str, *, quality: int = 85) -> tuple[str, str]:
    """Compress the image to JPEG, AES-GCM encrypt it, and return the blob metadata."""
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    try:
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError("Pillow is required for image compression") from exc

    compressed_fd, compressed_path = tempfile.mkstemp(suffix=".jpg")
    os.close(compressed_fd)
    encrypted_fd, encrypted_path = tempfile.mkstemp(suffix=".bin")
    os.close(encrypted_fd)
    try:
        with Image.open(path) as img:
            image = img
            if image.mode not in ("RGB", "L"):
                image = image.convert("RGB")
            image.save(compressed_path, "JPEG", quality=quality, optimize=True)
        with open(compressed_path, "rb") as src:
            payload = src.read()
        key = secrets.token_bytes(32)
        nonce = secrets.token_bytes(12)
        cipher = AESGCM(key)
        ciphertext = cipher.encrypt(nonce, payload, None)
        with open(encrypted_path, "wb") as dst:
            dst.write(ciphertext)
        meta = {
            "type": "hidden_image",
            "version": 1,
            "key": base64.b64encode(key).decode("ascii"),
            "nonce": base64.b64encode(nonce).decode("ascii"),
            "suffix": ".jpg",
            "format": "JPEG",
            "filename": os.path.basename(path) or "image.jpg",
        }
        meta_json = json.dumps(meta, separators=(",", ":"))
        return encrypted_path, meta_json
    finally:
        try:
            os.remove(compressed_path)
        except Exception:
            pass


def decrypt_file(enc_path: str, meta_json: str) -> str:
    """Decrypt a file encrypted with ``encrypt_file`` and return a temp path."""
    if not os.path.isfile(enc_path):
        raise FileNotFoundError(enc_path)
    try:
        meta = json.loads(meta_json)
    except Exception as exc:
        raise ValueError("invalid metadata") from exc
    if meta.get("type") != "hidden_image":
        raise ValueError("unsupported hidden media metadata")
    try:
        key = base64.b64decode(str(meta["key"]))
        nonce = base64.b64decode(str(meta["nonce"]))
    except Exception as exc:
        raise ValueError("invalid key/nonce") from exc
    suffix = str(meta.get("suffix") or ".bin")
    cipher = AESGCM(key)
    with open(enc_path, "rb") as src:
        ciphertext = src.read()
    plaintext = cipher.decrypt(nonce, ciphertext, None)
    out_fd, out_path = tempfile.mkstemp(suffix=suffix)
    os.close(out_fd)
    with open(out_path, "wb") as dst:
        dst.write(plaintext)
    return out_path


def encode_hidden_image_to_zwc(path: str, *, max_chars: int = 4000) -> str:
    """
    Compress, encrypt, and wrap an image into a single ZWC string for hidden delivery.
    Raises ValueError if the packed payload cannot fit under max_chars.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    try:
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError("Pillow is required for hidden images") from exc

    src = Image.open(path)
    if src.mode not in ("RGB", "L"):
        src = src.convert("RGB")

    target_side = 96
    min_side = 24
    quality = 80
    min_quality = 35
    mime = "image/jpeg"
    fmt = "JPEG"

    def _encode_once(img_obj, q: int) -> tuple[str, int, int]:
        buf = io.BytesIO()
        img_obj.save(buf, fmt, quality=q, optimize=True)
        data = buf.getvalue()
        comp = zlib.compress(data, level=9)
        key = secrets.token_bytes(32)
        nonce = secrets.token_bytes(12)
        cipher = AESGCM(key)
        cipher_bytes = cipher.encrypt(nonce, comp, None)
        meta = {
            "type": "hidden_image",
            "v": 2,
            "alg": "AESGCM",
            "mime": mime,
            "w": int(img_obj.width),
            "h": int(img_obj.height),
            "key": base64.b64encode(key).decode("ascii"),
            "nonce": base64.b64encode(nonce).decode("ascii"),
        }
        payload = json.dumps(meta, separators=(",", ":")).encode("utf-8") + b"\0" + cipher_bytes
        b64 = base64.b64encode(payload).decode("ascii")
        zwc = encode_zwc(b64)
        return zwc, img_obj.width, img_obj.height

    while target_side >= min_side:
        scale = 1.0
        w, h = src.size
        max_side = max(w, h)
        if max_side > target_side:
            scale = target_side / float(max_side)
        new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
        img = src.resize(new_size) if scale < 1.0 else src

        q = quality
        while q >= min_quality:
            zwc, _, _ = _encode_once(img, q)
            if len(zwc) <= max_chars:
                return zwc
            q -= 10

        target_side = int(target_side * 0.8)
        quality = 80  # reset quality for next size step

    raise ValueError("hidden image too large to embed")


def try_decode_hidden_image_from_zwc(text: str) -> str | None:
    """
    Try to decode hidden image payload stored in zero-width text.
    Returns path to temporary image file or None if not a valid hidden image.
    """
    if not text or not is_zwc_only(text):
        return None
    payload_b64 = decode_zwc(text)
    if not payload_b64:
        return None
    try:
        payload = base64.b64decode(payload_b64, validate=False)
    except Exception:
        return None
    if b"\0" not in payload:
        return None
    meta_raw, cipher_bytes = payload.split(b"\0", 1)
    try:
        meta = json.loads(meta_raw.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(meta, dict) or meta.get("type") != "hidden_image":
        return None
    try:
        key = base64.b64decode(str(meta["key"]))
        nonce = base64.b64decode(str(meta["nonce"]))
    except Exception:
        return None
    try:
        cipher = AESGCM(key)
        compressed = cipher.decrypt(nonce, cipher_bytes, None)
        data = zlib.decompress(compressed)
    except Exception:
        return None

    mime = str(meta.get("mime") or "image/jpeg").lower()
    suffix = ".jpg" if "jpeg" in mime or "jpg" in mime else (".webp" if "webp" in mime else ".img")
    fd, out_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    with open(out_path, "wb") as fh:
        fh.write(data)
    return out_path
