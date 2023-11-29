from functools import wraps
import time
import logging
import random

from config.settings import app_settings

logging.basicConfig(level=app_settings.log_level.upper())
logger = logging.getLogger(__name__)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"{e}, retry in {t} seconds.")
                    time.sleep(t)
                    t = min(t * factor, border_sleep_time)
        return inner
    return func_wrapper


def backoff_with_jitter(retry_exceptions, start_sleep_time=0.1, factor=2, border_sleep_time=10, jitter=0.1):
    """
    Decorator for retrying a function with exponential delay and jitter.
    :param retry_exceptions: Exceptions under which to retry the function call.
    :param start_sleep_time: The initial wait time between attempts.
    :param factor: The factor by which the wait time is increased.
    :param border_sleep_time: Maximum wait time.
    :param jitter: Additional random wait time.
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    sleep_time = min(t, border_sleep_time) + random.uniform(0, jitter)
                    logger.error(f"{e}, retry in {sleep_time:.2f} seconds.")
                    time.sleep(sleep_time)
                    t *= factor
                except Exception as e:
                    raise e
        return inner
    return func_wrapper
