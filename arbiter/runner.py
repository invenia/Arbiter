"""
The actual task runner.
"""
from concurrent.futures import (as_completed, ProcessPoolExecutor,
                                ThreadPoolExecutor)


def run_tasks(tasks, max_workers=1, processes=False):
    """
    Concurrently run a set of tasks, resolving dependencies as
    necessary. Returns a tuple containg: (completed_tasks, failed_tasks)
    where both completed_tasks and failed_tasks are sets of task names.

    tasks: An iterable of tasks.
    max_workers: (optional, 1) The maximum number of workers to use for
        executing tasks.
    processes: (optional, False) Run each task in a separate process
        instead of separate tasks
    """
    futures = {}

    if processes:
        get_executor = ProcessPoolExecutor
    else:
        get_executor = ThreadPoolExecutor

    with get_executor(max_workers) as executor:
        updated = True
        while tasks and updated:
            updated = False

            remaining_tasks = []

            for task in tasks:
                # all dependencies matched. We can run the task
                if task.dependencies <= set(futures.keys()):
                    futures[task.name] = executor.submit(
                        task_runner,
                        [futures[name] for name in task.dependencies],
                        task.function,
                        task.args,
                        task.kwargs,
                    )
                    updated = True
                else:
                    remaining_tasks.append(task)

            tasks = remaining_tasks

        completed = set()
        failures = {task.name for task in tasks}

        for name, future in futures.items():
            if future.result():
                completed.add(name)
            else:
                failures.add(name)

    return completed, failures


def task_runner(dependent_futures, function, args=(), kwargs=None):
    """
    The function that runs a task if its dependencies are met

    dependent_futures: A list of futures that are prerequisites.
    function: The actual task to run.
    args: The function arguments.
    kwargs: The function keyword arguments.
    """
    if kwargs is None:
        kwargs = {}

    # wait on dependencies to finish
    for dependent_future in as_completed(dependent_futures):
            # if any dependency fails, the task fails. No need to wait
            if not dependent_future.result():
                return False

    try:
        return function(*args, **kwargs)
    except Exception:
        return False
