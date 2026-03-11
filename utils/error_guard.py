# utils/error_guard.py
"""
Utility helpers to wrap callables with a defensive exception logger.

Every wrapped callable keeps the original signature while ensuring that
unexpected exceptions are logged with a clear module/function qualifier
before being re-raised. This makes it easier to diagnose crashes without
sprinkling explicit try/except blocks everywhere.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import types
from concurrent.futures import CancelledError as FuturesCancelledError
from typing import Any, Callable, Dict, Iterable, Optional

LOG = logging.getLogger("error_guard")


def _log_exception(fn: Callable[..., Any], exc: BaseException) -> None:
    """Log exception with the fully qualified function name and re-raise."""
    if isinstance(exc, (asyncio.CancelledError, FuturesCancelledError)):
        return
    try:
        qualifier = getattr(fn, "__qualname__", getattr(fn, "__name__", repr(fn)))
        module = getattr(fn, "__module__", None)
        qualified = f"{module}.{qualifier}" if module else qualifier
    except Exception:
        qualified = repr(fn)
    LOG.exception("Unhandled error in %s: %s", qualified, exc, exc_info=True)


def guard_callable(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a callable so that unhandled exceptions are logged before re-raise."""
    if getattr(fn, "__wrapped_by_error_guard__", False):
        return fn

    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                _log_exception(fn, exc)
                raise

        setattr(_async_wrapper, "__wrapped_by_error_guard__", True)
        return _async_wrapper

    if inspect.isasyncgenfunction(fn):
        @functools.wraps(fn)
        async def _async_gen_wrapper(*args: Any, **kwargs: Any) -> Iterable[Any]:
            try:
                async for item in fn(*args, **kwargs):
                    yield item
            except Exception as exc:  # noqa: BLE001
                _log_exception(fn, exc)
                raise

        setattr(_async_gen_wrapper, "__wrapped_by_error_guard__", True)
        return _async_gen_wrapper

    if inspect.isgeneratorfunction(fn):
        @functools.wraps(fn)
        def _gen_wrapper(*args: Any, **kwargs: Any) -> Iterable[Any]:
            try:
                yield from fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                _log_exception(fn, exc)
                raise

        setattr(_gen_wrapper, "__wrapped_by_error_guard__", True)
        return _gen_wrapper

    @functools.wraps(fn)
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            _log_exception(fn, exc)
            raise

    setattr(_wrapper, "__wrapped_by_error_guard__", True)
    return _wrapper


def _is_signal(attr: Any) -> bool:
    """Rudimentary check for Qt Signal descriptors to avoid wrapping them."""
    # PySide6 signals expose `.connect` and `.emit` on the instance but behave
    # like special descriptors on the class. We skip wrapping them to avoid
    # breaking Qt's meta-object behaviour.
    return hasattr(attr, "connect") and hasattr(attr, "emit") and not callable(attr)


def _is_user_defined(obj: Any, module_name: Optional[str]) -> bool:
    """Only touch objects defined in the same module that calls guard_module()."""
    try:
        return getattr(obj, "__module__", None) == module_name
    except Exception:
        return False


def guard_class(cls: type) -> None:
    """Wrap methods of a class in-place (only pure Python callables)."""
    for name, attr in list(vars(cls).items()):
        if name.startswith("__"):
            continue

        # staticmethod / classmethod
        if isinstance(attr, (staticmethod, classmethod)):
            fn = attr.__func__  # type: ignore[attr-defined]
            wrapped = guard_callable(fn)
            if isinstance(attr, staticmethod):
                setattr(cls, name, staticmethod(wrapped))
            else:
                setattr(cls, name, classmethod(wrapped))
            continue

        # обычные методы — только функции, чтобы не трогать C-дескрипторы
        if inspect.isfunction(attr) and not _is_signal(attr):
            setattr(cls, name, guard_callable(attr))


def guard_module(globals_dict: Dict[str, Any]) -> None:
    """
    Apply guard_callable **only** to module-local callables/classes.

        guard_module(globals())

    Это важно: не трогаем импортированные/встроенные типы (deque, list и т.п.),
    иначе получим TypeError при попытке заменить их методы.
    """
    module_name = globals_dict.get("__name__")

    for name, obj in list(globals_dict.items()):
        if name.startswith("__"):
            continue

        if inspect.isclass(obj):
            if _is_user_defined(obj, module_name):
                guard_class(obj)
            continue

        if callable(obj):
            if _is_user_defined(obj, module_name):
                globals_dict[name] = guard_callable(obj)


def ensure_asyncio_exception_logging(loop: Optional[asyncio.AbstractEventLoop]) -> None:
    """Attach an exception handler to the loop that logs uncaught exceptions."""
    if not loop:
        return

    def _handler(_loop: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
        exc = context.get("exception")
        if exc:
            LOG.exception("Unhandled error in asyncio task: %s", exc, exc_info=True)
        else:
            msg = context.get("message", "Unknown asyncio exception")
            LOG.error("Unhandled asyncio context: %s", msg)

    try:
        loop.set_exception_handler(_handler)
    except Exception as exc:  # noqa: BLE001
        LOG.debug("Unable to install asyncio exception handler: %s", exc)
