"""
The actual task runner.
"""
from collections import namedtuple
from concurrent.futures import (as_completed, ProcessPoolExecutor,
                                ThreadPoolExecutor, TimeoutError)
from datetime import datetime

from arbiter.exceptions import (
    FailedDependencyError,
    UnsatisfiedDependencyError
)


Result = namedtuple('Result', ['success', 'value'])


def run_tasks(tasks, max_workers=1, processes=False, timeout=None):
    """
    Concurrently run a set of tasks, resolving dependencies as
    necessary. Returns a dict containing task results.

    tasks: An iterable of tasks.
    max_workers: (optional, 1) The maximum number of workers to use for
        executing tasks.
    processes: (optional, False) Run each task in a separate process
        instead of separate tasks.
    timeout: (optional, None) If given, a timedelta representing the
        maximum amount of time to allot to running tasks.
    """
    validate_task_names(tasks)

    if timeout is not None:
        finish_date = datetime.utcnow() + timeout

    results = {}
    futures = set()

    with get_executor(max_workers, processes=processes) as executor:
        updated = True
        while futures or (tasks and updated):
            updated = False

            remaining_tasks = []

            for task in tasks:
                for name in task.dependencies:
                    dependency = results.get(name)

                    if dependency is None:  # not yet complete
                        remaining_tasks.append(task)
                        break
                    elif not dependency.success:
                        updated = True
                        results[task.name] = Result(
                            False, FailedDependencyError(name)
                        )
                        break
                else:  # task can be run
                    updated = True
                    future = executor.submit(
                        task.function, *task.args, **task.kwargs
                    )

                    # as_completed takes a list of tasks, but we need
                    # to know which task the future represents. To get
                    # around this we just add a name to the future
                    future.name = task.name

                    futures.add(future)

            tasks = remaining_tasks

            if timeout is not None:
                timeout = (finish_date - datetime.utcnow()).total_seconds()

            try:
                for future in as_completed(futures, timeout=timeout):
                    try:
                        results[future.name] = Result(True, future.result())
                    except Exception as exception:
                        results[future.name] = Result(False, exception)

                    # remove the future from the set of futures
                    futures.remove(future)

                    if tasks:  # new task may be runnable
                        updated = True
                        break
            except TimeoutError as exception:
                for future in futures:
                    results[future.name] = Result(False, exception)
                    future.cancel()

                futures = []

                for task in tasks:
                    results[task.name] = Result(False, exception)

                tasks = []

    # some tasks may have been unrunnable
    names = set()

    for task in tasks:
        # later unrunnable tasks may have depended on this task
        names.add(task.name)

        results[task.name] = Result(
            False,
            UnsatisfiedDependencyError(
                {
                    dependency for dependency in task.dependencies
                    if dependency in names or dependency not in results
                }
            )
        )

    return results


def get_executor(max_workers, processes=False):
    """
    Get the appropriate Pool Executor.

    max_workers: The maximum number of workers to use for executing
        tasks.
    processes: (optional, False) Use a ProcessPoolExecutor instead of a
        ThreadPoolExecutor.
    """
    if processes:
        return ProcessPoolExecutor(max_workers=max_workers)
    else:
        return ThreadPoolExecutor(max_workers=max_workers)


def validate_task_names(tasks):
    """
    Ensure that none of the tasks have the same name
    """
    names = set()
    for task in tasks:
        if task.name in names:
            raise TypeError(task.name)
        else:
            names.add(task.name)
