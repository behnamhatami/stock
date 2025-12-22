import logging
import time

from django.core.exceptions import PermissionDenied
from ipware import get_client_ip

logger = logging.getLogger(__name__)


def log_time(f):
    def wrapper(*args, **kwargs):
        t = time.time()
        try:
            return f(*args, **kwargs)
        finally:
            logger.info(f"{f.__name__} runs in {time.time() - t}")

    return wrapper


def log_time_class(f):
    def wrapper(*args, **kwargs):
        t = time.time()
        try:
            return f(*args, **kwargs)
        finally:
            logger.info(f"{args[0]}: {f.__name__} runs in {round(time.time() - t, 2)}")

    return wrapper


def check_ip(white_list: list):
    def dec(f):
        def wrapper(request, *args, **kwargs):
            client_ip, is_routable = get_client_ip(request)
            if client_ip not in white_list and request.user.is_anonymous:
                logger.info(f"access from {client_ip}/{is_routable}")
                raise PermissionDenied
            return f(request, *args, **kwargs)

        return wrapper

    return dec
