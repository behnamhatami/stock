import logging
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def log_time(f):
    def wrapper(*args, **kwargs):
        t = time.time()
        try:
            return f(*args, **kwargs)
        finally:
            logger.info(f"{f.__name__} runs in {time.time() - t}")

    return wrapper
