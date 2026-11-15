import logging

from django.core.exceptions import PermissionDenied
from django.http import HttpRequest
from ipware import get_client_ip

logger = logging.getLogger(__name__)
import functools
import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar

logger = logging.getLogger(__name__)

P = ParamSpec('P')
R = TypeVar('R')


def _timed(f: Callable[P, R], label: Callable[..., str]) -> Callable[P, R]:
    if inspect.iscoroutinefunction(f):
        async_f: Callable[P, Awaitable[Any]] = f

        @functools.wraps(f)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            start = time.perf_counter()
            try:
                return await async_f(*args, **kwargs)
            finally:
                logger.info(f"{label(*args)} runs in {time.perf_counter() - start:.2f}s")

        return async_wrapper  # type: ignore[return-value]

    @functools.wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        start = time.perf_counter()
        try:
            return f(*args, **kwargs)
        finally:
            logger.info(f"{label(*args)} runs in {time.perf_counter() - start:.2f}s")

    return wrapper


def log_time(f: Callable[P, R]) -> Callable[P, R]:
    """Time a synchronous or asynchronous function."""
    return _timed(f, lambda *args: f.__name__)


def log_time_class(f: Callable[P, R]) -> Callable[P, R]:
    """Time a method, prefixing the log message with its instance."""
    return _timed(f, lambda *args: f"{args[0]}: {f.__name__}")


def check_ip(white_list: list[str]) -> Callable[[Callable[..., R]], Callable[..., R]]:
    def dec(f: Callable[..., R]) -> Callable[..., R]:
        def wrapper(request: HttpRequest, *args: Any, **kwargs: Any) -> R:
            client_ip, is_routable = get_client_ip(request)
            if client_ip not in white_list and request.user.is_anonymous:
                logger.info(f"access from {client_ip}/{is_routable}")
                raise PermissionDenied
            return f(request, *args, **kwargs)

        return wrapper

    return dec
