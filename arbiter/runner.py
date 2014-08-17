"""
The actual task runner.
"""
from collections import namedtuple
from concurrent.futures import (as_completed, ProcessPoolExecutor,
                                ThreadPoolExecutor)

from arbiter.exceptions import (
    FailedDependencyError,
    UnsatisfiedDependencyError
)


Result = namedtuple('Result', ['success', 'value'])


def run_tasks(tasks, max_workers=1, processes=False):
    """
    Concurrently run a set of tasks, resolving dependencies as
    necessary. Returns a dict containing task results.

    tasks: An iterable of tasks.
    max_workers: (optional, 1) The maximum number of workers to use for
        executing tasks.
    processes: (optional, False) Run each task in a separate process
        instead of separate tasks.
    """
    names = set()
    for task in tasks:
        if task.name in names:
            raise TypeError(task.name)
        else:
            names.add(task.name)

    results = {}

    if processes:
        get_executor = ProcessPoolExecutor
    else:
        get_executor = ThreadPoolExecutor

    futures = set()

    with get_executor(max_workers) as executor:
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

            for future in as_completed(futures):

                try:
                    results[future.name] = Result(True, future.result())
                except Exception as exception:
                    results[future.name] = Result(False, exception)

                # remove the future from the set of futures
                futures.remove(future)

                if tasks:  # new task may be runnable
                    updated = True
                    break

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
