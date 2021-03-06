"""
Asynchronous task runner using concurrent futures
"""
import concurrent.futures

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
        get_executor = concurrent.futures.ProcessPoolExecutor
    else:
        get_executor = concurrent.futures.ThreadPoolExecutor

    with get_executor(max_workers) as executor:
        def execute(function, name):
            """
            Submit a task to the pool
            """
            future = executor.submit(function)
            future.name = name

            futures.add(future)

        def wait():
            """
            Wait for at least one task to complete
            """
            results = []

            waited = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for future in waited.done:
                exc = future.exception()
                if exc is None:
                    results.append(
                        TaskResult(
                            future.name,
                            True,
                            None,
                            future.result()
                        )
                    )
                else:
                    results.append(TaskResult(future.name, False, exc, None))

                futures.remove(future)

            return results

        return task_loop(tasks, execute, wait)
