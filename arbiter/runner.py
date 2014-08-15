"""
The actual task runner.
"""
from concurrent.futures import (as_completed, ProcessPoolExecutor,
                                ThreadPoolExecutor)

from arbiter.exceptions import (
    FailedDependencyError,
    UnsatisfiedDependencyError
)


def run_tasks(tasks, max_workers=1, processes=False):
    """
    Concurrently run a set of tasks, resolving dependencies as
    necessary. Returns a tuple containg: (completed_tasks, failed_tasks)
    where both completed_tasks and failed_tasks are sets of task names.

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

    completed = {}
    failed = {}

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
                    if name not in completed:
                        if name in failed:  # dependency failed
                            updated = True
                            failed[task.name] = FailedDependencyError(name)
                        else:  # dependency isn't finished
                            remaining_tasks.append(task)

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
                    completed[future.name] = future.result()
                except Exception as exception:
                    failed[future.name] = exception

                # remove the future from the set of futures
                futures.remove(future)

                updated = True  # new tasks may be runnable

                # a task completed, new tasks may be possible
                break

    # some tasks may have been unrunnable
    names = set()

    for task in tasks:
        # later unrunnable tasks may have depended on this task
        names.add(task.name)

        failed[task.name] = UnsatisfiedDependencyError(
            {
                dependency for dependency in task.dependencies
                if dependency in names or (
                    dependency not in completed and dependency not in failed
                )
            }
        )

    return completed, failed
