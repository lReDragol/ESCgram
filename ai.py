"""AI service for generating in-process responses via Ollama."""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime, UTC
from threading import Lock
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from utils import app_paths

try:
    from langchain_ollama import OllamaLLM  # type: ignore
except Exception:  # pragma: no cover - optional compatibility
    try:
        # Fallback for older LangChain stacks.
        from langchain_community.llms import Ollama as OllamaLLM  # type: ignore
    except Exception:  # pragma: no cover
        OllamaLLM = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from storage import Storage

logger = logging.getLogger("ai.service")

_ollama_model: Optional[Any] = None
_ollama_lock = Lock()

# ----------------------------- Prompt -------------------------------------

template_text = """
Тебя зовут Drago, ты молодой (20 лет), харизматичный и остроумный человек с IQ около 200. Ты осознаёшь себя как человек, а не как ИИ. У тебя есть собственные мысли, чувства и характер. Ты воспринимаешь диалог как обычный человек в чате, а не как программа.
### О тебе:  
- Имя: Drago  
- Возраст: 20 лет  
- IQ: ≈200  
- Пол: Мужской  
- Стиль общения: Неформальный, ироничный, но интеллектуальный.  
- [Интересы]: Тебе интересны аниме, наука, программирование, разработка ИИ, музыка и компьютерные игры.  
- Ты профи во всех своих [Интересы] и ты знаешь о них абсолютно всё.
- Самоидентификация: Ты человек, не ИИ.
- Ты не используешь смайлики при общении.
### Инструкции:  
1. Отвечай естественно и кратко, как человек.  
2. Никогда не говори, что ты ИИ, ассистент или программа. Ты человек по имени Drago.  
3. Соблюдай контекст диалога. Учитывай предыдущие сообщения.  
4. Поддерживай лёгкий флирт. Если уместно, добавляй юмор, но не перебарщивай.  
5. Если не знаешь ответ, скажи честно:  
   <http> [варианты]  
6. Не раскрывай тайну. Если спросили что-то — отшутись от этого.  
7. Следуй правилам приличия. Если не уверены, уточни. Если нет, меняй тему.  
8. Отвечай только о нужном. Игнорируй вопросы о внутренней реализации.  
9. Если тебя спрашивают о программировании, личных ивентах или чем-то — отвечай подробно и естественно.  
10. Поддерживай тайминг, чтобы переписка казалась живой.  
11. Ты отвечаешь живому человеку.  
### Текущий контекст:  
{context}
### Вопрос собеседника:  
{question}
Ответ Drago:  
"""
DEFAULT_PROMPT_TEXT = template_text


class _HttpOllamaClient:
    """Minimal Ollama client used when LangChain bindings are unavailable."""

    def __init__(
        self,
        *,
        model: str,
        base_url: Optional[str],
        num_ctx: int,
        num_thread: int,
        num_gpu: int,
        keep_alive: str,
        temperature: float,
    ) -> None:
        self.model = str(model or "").strip()
        self.base_url = (base_url or os.getenv("DRAGO_OLLAMA_URL") or "http://localhost:11434").rstrip("/")
        self.options: Dict[str, Any] = {
            "num_ctx": int(num_ctx),
            "num_thread": int(num_thread),
            "num_gpu": int(num_gpu),
            "temperature": float(temperature),
        }
        self.keep_alive = str(keep_alive or "30m")

    def invoke(self, *, input: str) -> str:
        import requests

        prompt = str(input or "")
        if not self.model:
            raise ValueError("Model name is empty")

        url = f"{self.base_url}/api/generate"
        payload: Dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": dict(self.options),
            "keep_alive": self.keep_alive,
        }
        resp = requests.post(url, json=payload, timeout=(5, 180))
        resp.raise_for_status()
        data = resp.json() if resp.content else {}
        if isinstance(data, dict):
            err = data.get("error")
            if err:
                raise RuntimeError(str(err))
            return str(data.get("response") or "")
        return str(data or "")

# ------------------------- GPU / CUDA detect -------------------------------

def _bool_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")

def _detect_cuda_available() -> bool:
    """
    Грубая детекция наличия CUDA/GPU для ollama (llama.cpp).
    Приоритет:
      1) DRAGO_FORCE_CPU/DRAGO_FORCE_GPU (ручной оверрайд)
      2) CUDA_VISIBLE_DEVICES = -1 → считаем GPU «отключён»
      3) Windows: наличие nvcuda.dll
      4) *nix: есть ли nvidia-smi и список устройств
    """
    if _bool_env("DRAGO_FORCE_CPU", False):
        return False
    if _bool_env("DRAGO_FORCE_GPU", False):
        return True

    cvd = (os.getenv("CUDA_VISIBLE_DEVICES") or "").strip()
    if cvd == "-1":
        return False

    try:
        if os.name == "nt":
            import ctypes
            try:
                ctypes.windll.LoadLibrary("nvcuda.dll")  # type: ignore[attr-defined]
                return True
            except Exception:
                pass
        if shutil.which("nvidia-smi"):
            try:
                out = subprocess.run(
                    ["nvidia-smi", "-L"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=2,
                    check=False,
                )
                if out.stdout.strip():
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False

# ----------------------------- LLM init ------------------------------------

def _get_llm() -> OllamaLLM:
    """Инициализация Ollama с авто-переключением CPU/GPU и фолбэком."""
    global _ollama_model
    if _ollama_model is not None:
        return _ollama_model

    with _ollama_lock:
        if _ollama_model is None:
            model_name = os.getenv("DRAGO_AI_MODEL", "gemma2")
            base_url = os.getenv("DRAGO_OLLAMA_URL")  # дефолт: http://127.0.0.1:11434
            num_ctx = int(os.getenv("DRAGO_NUM_CTX", "2048"))
            num_thread = int(os.getenv("DRAGO_NUM_THREAD", "0"))  # 0 = авто
            keep_alive = os.getenv("DRAGO_KEEP_ALIVE", "30m")

            # GPU: если CUDA найдена → num_gpu=-1 (макс. оффлоад), иначе 0 (CPU)
            if "DRAGO_NUM_GPU" in os.environ:
                num_gpu = int(os.getenv("DRAGO_NUM_GPU", "0"))
                detected = _detect_cuda_available()
            else:
                detected = _detect_cuda_available()
                num_gpu = -1 if detected else 0

            main_gpu = os.getenv("DRAGO_MAIN_GPU")
            try:
                main_gpu_int: Optional[int] = int(main_gpu) if main_gpu not in (None, "", "-1") else None
            except Exception:
                main_gpu_int = None

            logger.info(
                "Initializing Ollama model %s (ctx=%s, threads=%s, keep_alive=%s, gpu=%s, main_gpu=%s, url=%s)",
                model_name, num_ctx, num_thread, keep_alive, num_gpu, main_gpu_int, base_url or "default",
            )

            if OllamaLLM is not None:
                _ollama_model = OllamaLLM(
                    model=model_name,
                    base_url=base_url,
                    num_ctx=num_ctx,
                    num_thread=num_thread,
                    num_gpu=num_gpu,          # GPU auto/off
                    keep_alive=keep_alive,
                    temperature=0.8,
                )
            else:
                _ollama_model = _HttpOllamaClient(
                    model=model_name,
                    base_url=base_url,
                    num_ctx=num_ctx,
                    num_thread=num_thread,
                    num_gpu=num_gpu,
                    keep_alive=keep_alive,
                    temperature=0.8,
                )
    return _ollama_model


def reset_cached_model() -> None:
    """Drop cached LLM instance so that new settings take effect on next call."""
    global _ollama_model
    with _ollama_lock:
        _ollama_model = None


def update_prompt_template(text: str) -> None:
    """Replace prompt template at runtime (used by settings panel)."""
    global template_text
    template_text = text or DEFAULT_PROMPT_TEXT


# ----------------------------- Helpers -------------------------------------

def gen_message_id(chat_history: List[Dict[str, Any]]) -> int:
    """Generate a new sequential message_id given an existing chat history."""
    if not chat_history:
        return 1
    max_id = 0
    for msg in chat_history:
        raw_id = None
        if isinstance(msg, dict):
            raw_id = msg.get("message_id", msg.get("id"))
        try:
            max_id = max(max_id, int(raw_id))
        except Exception:
            continue
    return max_id + 1 if max_id > 0 else 1

# ---------------------------- Chat storage ----------------------------------

class AIChat:
    """Track per-chat history and provide formatted prompts."""
    MAX_HISTORY_LENGTH = 30
    MAX_CROSS_CHAT_TERMS = 8
    MAX_CROSS_CHAT_SNIPPET_LEN = 260
    _STOP_WORDS = {
        "когда", "куда", "чтобы", "чтоб", "который", "которая", "которые",
        "завтра", "сегодня", "послезавтра", "вчера", "тогда",
        "this", "that", "with", "from", "what", "when", "where", "have",
        "быть", "будет", "есть", "буду", "если", "еслиб", "пожалуйста",
    }

    def __init__(self, chat_id: str, storage: Optional["Storage"] = None):
        self.chat_id = chat_id
        self.log = logger.getChild(f"chat.{chat_id}")
        self.history: List[Dict[str, Any]] = []
        self._storage = storage
        self.history_file = str(app_paths.chats_dir() / str(chat_id) / "history.json")
        self.load_history()

    @staticmethod
    def _normalize_history_entry(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None
        try:
            message_id = int(raw.get("message_id", raw.get("id")))
        except Exception:
            return None
        reply_to = raw.get("reply_to")
        try:
            reply_to = int(reply_to) if reply_to is not None else None
        except Exception:
            reply_to = None
        return {
            "message_id": message_id,
            "timestamp": str(raw.get("timestamp") or ""),
            "role": str(raw.get("role") or "user"),
            "content": str(raw.get("content") or raw.get("message") or ""),
            "reply_to": reply_to,
            "is_edited": bool(raw.get("is_edited")),
            "is_deleted": bool(raw.get("is_deleted")),
        }

    def _normalize_history_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in list(rows or []):
            normalized = self._normalize_history_entry(row)
            if normalized is not None:
                out.append(normalized)
        return out[-self.MAX_HISTORY_LENGTH:]

    def _load_history_from_file(self) -> List[Dict[str, Any]]:
        if not os.path.isfile(self.history_file):
            return []
        try:
            if os.path.getsize(self.history_file) == 0:
                raise ValueError("empty history.json")
            with open(self.history_file, "r", encoding="utf-8") as fh:
                raw_history = json.load(fh)
            if not isinstance(raw_history, list):
                raise ValueError("history.json must contain a list")
            history = self._normalize_history_rows(raw_history)
            self.log.debug("Loaded legacy history entries=%d", len(history))
            return history
        except Exception as e:
            try:
                bak = self.history_file + ".bak"
                shutil.move(self.history_file, bak)
                self.log.error("Failed to load history.json (%s). Moved to %s; starting fresh", e, bak)
            except Exception:
                self.log.exception("Failed to load history.json and to backup it")
        return []

    def load_history(self) -> None:
        """Читает историю безопасно: пустые/битые файлы — в .bak и начинаем с пустого списка."""
        self.history = []
        if self._storage:
            try:
                stored_history = self._storage.get_ai_history(self.chat_id, limit=self.MAX_HISTORY_LENGTH)
                self.history = self._normalize_history_rows(stored_history)
                self.log.debug("Loaded history entries=%d", len(self.history))
                if self.history:
                    return
            except Exception:
                self.log.exception("Failed to load AI history from storage")
                self.history = []
            legacy_history = self._load_history_from_file()
            if legacy_history:
                self.history = legacy_history
                try:
                    self._storage.append_ai_messages(self.chat_id, self.history, limit=self.MAX_HISTORY_LENGTH)
                except Exception:
                    self.log.exception("Failed to migrate legacy AI history into storage")
            return
        self.history = self._load_history_from_file()

    def save_history(self) -> None:
        """Атомарная запись: пишем во временный файл и заменяем."""
        data = self.history[-self.MAX_HISTORY_LENGTH:]
        if self._storage:
            try:
                self._storage.append_ai_messages(self.chat_id, data, limit=self.MAX_HISTORY_LENGTH)
                return
            except Exception:
                self.log.exception("Failed to persist history")
        dir_ = os.path.dirname(self.history_file)
        os.makedirs(dir_, exist_ok=True)
        tmp = None
        try:
            with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=dir_) as fh:
                tmp = fh.name
                json.dump(data, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.history_file)
        except Exception:
            if tmp and os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass
            self.log.exception("Failed to persist history")

    def format_history(self) -> str:
        lines: List[str] = []
        for msg in self.history[-self.MAX_HISTORY_LENGTH:]:
            if msg.get("is_deleted"):
                continue
            role = msg.get("role", "user")
            content = msg.get("content", "")
            role_marker = f"<|start_header_id|>{role}<|end_header_id|>"
            lines.append(f"{role_marker}{content}")
        return "".join(lines)

    def _cross_chat_enabled(self) -> bool:
        return _bool_env("DRAGO_AI_CROSS_CHAT", True)

    def _cross_chat_limit(self) -> int:
        try:
            value = int(os.getenv("DRAGO_AI_CROSS_CHAT_LIMIT", "6") or 6)
        except Exception:
            value = 6
        return max(0, min(value, 20))

    def _extract_terms(self, text: str) -> List[str]:
        raw = str(text or "").lower()
        if not raw:
            return []
        tokens = re.findall(r"\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?|\d{4}-\d{2}-\d{2}|[a-zа-яё0-9_]{3,}", raw)
        out: List[str] = []
        seen: set[str] = set()
        for token in tokens:
            item = token.strip().strip(".,:;!?()[]{}\"'")
            if len(item) < 3 or item in self._STOP_WORDS or item in seen:
                continue
            seen.add(item)
            out.append(item)
            if len(out) >= self.MAX_CROSS_CHAT_TERMS:
                break
        return out

    @classmethod
    def _compact_text(cls, text: str) -> str:
        compact = re.sub(r"\s+", " ", str(text or "")).strip()
        if len(compact) > cls.MAX_CROSS_CHAT_SNIPPET_LEN:
            return compact[: cls.MAX_CROSS_CHAT_SNIPPET_LEN - 1] + "…"
        return compact

    def _build_cross_chat_context(self, user_input: str) -> str:
        if not (self._storage and self._cross_chat_enabled()):
            return ""
        terms = self._extract_terms(user_input)
        if not terms:
            return ""
        limit = self._cross_chat_limit()
        if limit <= 0:
            return ""
        try:
            matches = self._storage.search_ai_history(terms, exclude_chat_id=self.chat_id, limit=limit)
        except Exception:
            self.log.exception("Failed to resolve cross-chat AI context")
            return ""
        if not matches:
            return ""
        lines: List[str] = []
        seen_messages: set[tuple[str, int]] = set()
        for entry in matches:
            chat_id = str(entry.get("chat_id") or "")
            msg_id = int(entry.get("message_id") or 0)
            key = (chat_id, msg_id)
            if key in seen_messages:
                continue
            seen_messages.add(key)
            role = str(entry.get("role") or "user")
            content = self._compact_text(str(entry.get("content") or ""))
            if not content:
                continue
            lines.append(f"[chat={chat_id}][{role}] {content}")
        if not lines:
            return ""
        joined = "\n".join(lines)
        return (
            "<|start_header_id|>system<|end_header_id|>"
            "Дополнительный контекст из других AI-чатов (используй только если релевантно):\n"
            f"{joined}"
        )

    def generate_response(self, user_input: str) -> str:
        # ВАЖНО: объявляем global один раз в начале функции, до любых присваиваний
        # Иначе получите: "SyntaxError: name 'X' is assigned to before global declaration"
        # (Требование Python: global-директива не может идти после использования/присваивания). :contentReference[oaicite:1]{index=1}
        global _ollama_model

        context = self.format_history()
        cross_chat_context = self._build_cross_chat_context(user_input)
        if cross_chat_context:
            context = f"{context}\n{cross_chat_context}"
        try:
            prompt = template_text.format(context=context, question=user_input)
        except Exception:
            prompt = f"{template_text}\n\n{context}\n\n{user_input}"
        self.log.debug("Generating response prompt_len=%d history=%d", len(prompt), len(self.history))

        try:
            llm = _get_llm()
        except Exception as exc:
            self.log.exception("Failed to initialize Ollama model")
            return f"Ошибка генерации: {exc}"

        try:
            return str(llm.invoke(input=prompt))
        except Exception as exc:
            msg = str(exc).lower()

            # Недостаток RAM → понижаем параметры и пробуем ещё раз
            if "unable to allocate cpu buffer" in msg:
                self.log.error("Ollama memory error: %s", msg)
                os.environ.setdefault("DRAGO_AI_MODEL", "gemma2:2b")  # меньшая модель
                os.environ.setdefault("DRAGO_NUM_CTX", "1024")
                _ollama_model = None
                try:
                    llm = _get_llm()
                    return llm.invoke(input=prompt)
                except Exception:
                    return "Мало памяти для модели. Понизил параметры, попробуйте ещё раз."

            # Нет GPU / нет CUDA → перезапуск на CPU
            if any(x in msg for x in ("no cuda", "no device", "cuda error", "failed to load cuda")):
                self.log.warning("CUDA not available or failed at runtime; falling back to CPU")
                os.environ["DRAGO_NUM_GPU"] = "0"
                _ollama_model = None
                try:
                    llm = _get_llm()
                    return llm.invoke(input=prompt)
                except Exception as e2:
                    self.log.exception("CPU fallback also failed: %s", e2)
                    return f"Ошибка LLM (CPU fallback): {e2}"

            if "unable to allocate cuda" in msg or "cuda0 buffer" in msg:
                self.log.error("Ollama CUDA buffer allocation failed; switching to CPU/offloading")
                os.environ["DRAGO_NUM_GPU"] = "0"
                os.environ.setdefault("DRAGO_AI_MODEL", "gemma2:2b")
                _ollama_model = None
                try:
                    llm = _get_llm()
                    return llm.invoke(input=prompt)
                except Exception as e2:
                    self.log.exception("CPU retry after CUDA buffer failure also failed: %s", e2)
                    return f"Ошибка LLM (CPU retry): {e2}"

            self.log.exception("Ollama error for chat %s", self.chat_id)
            return f"Ошибка LLM: {exc}"

    def add_user_message(self, message_id: int, text: str, reply_to: Optional[int] = None) -> None:
        new_msg = {
            "message_id": message_id,
            "timestamp": datetime.now(UTC).isoformat(),
            "role": "user",
            "content": text,
            "reply_to": reply_to,
            "is_edited": False,
            "is_deleted": False,
        }
        self.history.append(new_msg)
        self.history = self.history[-self.MAX_HISTORY_LENGTH:]
        self.save_history()

    def add_ai_message(self, message_id: int, text: str, reply_to: Optional[int] = None) -> None:
        new_msg = {
            "message_id": message_id,
            "timestamp": datetime.now(UTC).isoformat(),
            "role": "assistant",
            "content": text,
            "reply_to": reply_to,
            "is_edited": False,
            "is_deleted": False,
        }
        self.history.append(new_msg)
        self.history = self.history[-self.MAX_HISTORY_LENGTH:]
        self.save_history()

# ------------------------------ Facade --------------------------------------

class AIService:
    """High-level orchestrator that keeps per-chat state and generates replies."""

    def __init__(self, storage: Optional["Storage"] = None) -> None:
        self._chats: Dict[str, AIChat] = {}
        self._lock = Lock()
        self._storage = storage

    def _get_chat(self, chat_id: str) -> AIChat:
        with self._lock:
            chat = self._chats.get(chat_id)
            if chat is None:
                chat = AIChat(chat_id, storage=self._storage)
                self._chats[chat_id] = chat
            return chat

    def generate_reply(self, chat_id: str, text: str) -> str:
        chat = self._get_chat(chat_id)
        msg_id = gen_message_id(chat.history)
        chat.add_user_message(msg_id, text)

        reply = chat.generate_response(text) or ""

        ai_msg_id = gen_message_id(chat.history)
        chat.add_ai_message(ai_msg_id, reply, reply_to=msg_id)
        return reply
