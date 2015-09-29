from datetime import timedelta
from functools import wraps, partial
from numbers import Integral
from time import sleep


class RetryCondition(object):
    """
    Defines a retry condition, which by default doesn't trigger a retry
    on either a value or an exception.

    NOTE: I don't particularly like this solution.
    """

    def __init__(self, function, kind='exception'):
        """
        Returns a retry condition which will run the supplied function
        in either on_value or on_exception depending if kind == 'exception'
        or 'value'

        Args:
            function: The function to run.
            kind: the type of condition exception or value based.
        """
        self._function = function
        self._kind = kind
        if kind != 'exception' and kind != 'value':
            raise ValueError(kind)

    def on_value(self, value):
        """
        Returns True or False as to whether or not the given value
        should trigger a retry event (Defaults to False).

        Args:
            value: The value to check against.
        """
        if self._kind == 'value':
            return self._function(value)

        return False

    def on_exception(self, exc):
        """
        Returns True or False as to whether or not the given exception
        should trigger a retry event (Defaults to False).

        Args:
            exc (Exception): The exceptioin to check against.
        """
        if self._kind == 'exception':
            return self._function(exc)

        return False


def retry_handler(retries=0, delay=timedelta(), conditions=[]):
    """
    A simple wrapper function that creates a handler function by using
    on the retry_loop function.

    Args:
        retries (Integral): The number of times to retry if a failure occurs.
        delay (timedelta, optional, 0 seconds): A timedelta representing
            the amount time to delay between retries.
        conditions (list): A list of retry conditions.
    Returns:
        function: The retry_loop function partialed.
    """
    delay_in_seconds = delay.total_seconds()
    return partial(retry_loop, retries, delay_in_seconds, conditions)


def retry(retries=0, delay=timedelta(), conditions=[]):
    """
    A decorator for making a function that retries on failure.

    Args:
        retries (Integral): The number of times to retry if a failure occurs.
        delay (timedelta, optional, 0 seconds): A timedelta representing
            the amount of time to delay between retries.
        conditions (list): A list of retry conditions.
    """
    delay_in_seconds = delay.total_seconds()

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
    """
    Actually performs the retry loop used by the retry decorator
    and handler functions. Failures for retrying are defined by
    the RetryConditions passed in. If the maximum number of
    retries has been reached then it raises the most recent
    error or a ValueError on the most recent result value.

    Args:
        retries (Integral): Maximum number of times to retry.
        delay_in_seconds (Integral): Number of seconds to wait
            between retries.
        conditions (list): A list of retry conditions the can
            trigger a retry on a return value or exception.
        function (function): The function to wrap.

    Returns:
        value: The return value from function
    """
    if not isinstance(retries, Integral):
        raise TypeError(retries)

    if delay_in_seconds < 0:
        raise TypeError(delay_in_seconds)

    attempts = 0
    value = None
    err = None
    while attempts <= retries:
        try:
            value = function()
            for condition in conditions:
                if condition.on_value(value):
                    break
            else:
                return value
        except Exception as exc:
            err = exc
            for condition in conditions:
                if condition.on_exception(exc):
                    break
            else:
                raise

        attempts += 1
        sleep(delay_in_seconds)
    else:
        if err:
            raise err
        else:
            raise ValueError(
                "Max retries ({}) reached and return the value is still {}."
                .format(attempts, value)
            )

    return value
