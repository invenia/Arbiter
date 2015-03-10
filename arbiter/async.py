"""
Asynchronous task runner using concurrent futures
"""
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)

from arbiter.base import task_loop, TaskResult


__all__ = ('run_tasks',)


def run_tasks(tasks, max_workers=None, use_processes=False):
    """
    Run an iterable of tasks.

    tasks: The iterable of tasks
    max_workers: (optional, None) The maximum number of workers to use.
        As of Python 3.5, if None is passed to the thread executor will
        default to 5 * the number of processors and the process executor
        will default to the number of processors. If you are running an
        older version, consider this a non-optional value.
    use_processes: (optional, False) use a process pool instead of a
        thread pool.
    """
    futures = set()

    if use_processes:
        get_executor = ProcessPoolExecutor
    else:
        get_executor = ThreadPoolExecutor

    with get_executor(max_workers) as executor:
        def execute(task):
            """
            Submit a task to the pool
            """
            future = executor.submit(task.function)
            future.name = task.name

            futures.add(future)

        def wait():
            """
            Wait for at least one task to complete
            """
            results = []

            # Wait for at least one task to complete. Unfortunately,
            # the options we have are: busy wait or iterate over futures
            # as they completed.
            #
            # To non-busily wait until we have at least one completed
            # future, iterate over futures as they complete, but
            # immediately break.
            for _ in as_completed(futures):
                break

            # we can't delete as we're iterating over the futures
            completed = [
                future for future in futures if future.done()
            ]

            for future in completed:
                results.append(TaskResult(future.name, future.result()))
                futures.remove(future)

            return results

        return task_loop(tasks, execute, wait)
