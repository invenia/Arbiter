"""
Task Creation for arbiter
"""
from collections import namedtuple
from datetime import timedelta
from functools import wraps
from numbers import Integral
from time import sleep


# Task is implemented as a namedtuple not a class. Task is used as a
# description of how to accomplish a task, not the task itself (e.g.,
# the task's status, etc. are not contained within the task), and there
# aren't any methods associated with the task data. As such, it made
# more sense to view a task as just data. namedtuple was chosen over
# dict because the data is immutable, and attributes are completely
# known in advance.

# If you plan on creating a Task directly, instead of using create,
# function should be a function that raises an exception (that derives
# from Exception) on failure. dependencies should be a frozenset. args
# should be a tuple. kwargs should be a dict.
Task = namedtuple(
    'Task',
    ['name', 'function', 'dependencies', 'args', 'kwargs', 'chain']
)


def create_task(name, function, dependencies=None, args=(), kwargs=None,
                chain=False, retries=0, delay=timedelta()):
    """
    Create a new task to be run by arbiter.

    name: The name of the task being created. As the name is used for
        dependency-tracking, it must be unique among all tasks you are
        running at a given time.
    function: A function that raises an exception on failure.
    dependencies: (optional, None) An iterable of names of tasks that
        must be successfully completed in order for this task to be run.
    args: (optional, ()) A tuple of arguments to pass to function.
    kwargs: (optional, None) A dict of arguments to pass to function.
    chain: (optional, False) If true, the results of dependent tasks
        will be be passed through to the task as named arguments.
    retries: (optional, 0) The number of times to retry on failure.
    delay: (optional, 0 seconds) The amount of time to delay between
        retries.
    """
    if dependencies is None:
        dependencies = frozenset()
    else:
        dependencies = frozenset(dependencies)

    if kwargs is None:
        kwargs = {}

    if retries > 0:
        function = retry(retries, delay)(function)

    return Task(name, function, dependencies, args, kwargs, chain)


def retry(retries, delay=timedelta()):
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
            attempts = 0

            while attempts < retries:
                try:
                    return function(*args, **kwargs)
                except Exception:
                    pass

                attempts += 1
                sleep(delay_in_seconds)

            return function(*args, **kwargs)

        return wrapper

    return decorator
