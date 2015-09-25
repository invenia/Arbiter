from datetime import timedelta
from functools import wraps, partial
from numbers import Integral
from time import sleep


class RetryCondition(object):
    """
    Defines 
    """
    __metaclass__ = ABCMeta

    def on_value(self, value):
        """
        Returns True or False as to whether or not the given value should trigger a retry event.

        Args:
            value: The value to check against.
        """
        return False

    def on_exception(self, exc):
        """
        Returns True or False as to whether or not the given exception should trigger a retry event.

        Args:
            exc (Exception): The exceptioin to check against.
        """
        return False


def retry_handler(retries=0, delay=timedelta(), conditions=[]):
    if not isinstance(retries, Integral):
        raise TypeError(retries)

    delay_in_seconds = delay.total_seconds()

    if delay_in_seconds < 0:
        raise TypeError(delay)

    return partial(retry_loop, retries, delay_in_seconds, conditions)


def retry(retries=0, delay=timedelta(), conditions=[]):
    """
    A decorator for making a function that retries on failure (failure
    is defined as raising an exception).
    The function's returned value will be passed through (and if on the
    final retry, an exception will be passed through).
    retries: The number of times to retry if a failure occurs.
    delay: (optional, 0 seconds) A timedelta representing the amount of
        time to delay between retries.
    """
    if not isinstance(retries, Integral):
        raise TypeError(retries)

    delay_in_seconds = delay.total_seconds()

    if delay_in_seconds < 0:
        raise TypeError(delay)

    def decorator(function):
        """
        The actual decorator for retrying.
        """
        @wraps(function)
        def wrapper(*args, **kwargs):
            """
            The actual wrapper for retrying.
            """
            func = partial(function, *args, **kwargs)
            return retry_loop(retries, delay_in_seconds, conditions, func)

        return wrapper

    return decorator


def retry_loop(retries, delay_in_seconds, conditions, function):
    attempts = 0

    while attempts <= retries:

        try:
            value = function()
            for condition in conditions:
                if condition.on_value(value):
                    break
            else:
                return value
        except Exception as exc:
            for condition in conditions:
                if condition.on_exception(exc):
                    break
            else:
                raise

        attempts += 1
        sleep(delay_in_seconds)

    return value
