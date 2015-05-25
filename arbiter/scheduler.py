"""
The dependency scheduler.
"""
from arbiter.graph import Graph, Strategy


__all__ = ('Scheduler',)


class Scheduler(object):
    """
    A dependency scheduler.
    """
    def __init__(self, tasks=None):
        self._graph = Graph()
        self._tasks = {}
        self._running = set()
        self._completed = set()
        self._failed = set()

        if tasks is not None:
            for task in tasks:
                self.add_task(task)

    @property
    def tasks(self):
        """
        A copy of the set of tasks
        """
        return frozenset(self._tasks.values())

    @property
    def completed(self):
        """
        A copy of the set of successfully completed tasks.
        """
        return frozenset(self._completed)

    @property
    def failed(self):
        """
        A copy of the set of failed tasks.
        """
        return frozenset(self._failed)

    @property
    def running(self):
        """
        A copy of the set of running tasks.
        """
        return frozenset(self._running)

    @property
    def runnable(self):
        """
        Get the set of tasks that are currently runnable.
        """
        return self._graph.roots - self._running

    def is_finished(self):
        """
        Have all runnable tasks completed?
        """
        return not (self._graph.roots or self._running)

    def add_task(self, task):
        """
        Add a task to the scheduler.

        task: The task to add.
        """
        if not Graph.valid_name(task.name):
            raise TypeError(task.name)

        if task.name in self._tasks:
            raise ValueError(task.name)

        self._tasks[task.name] = task

        incomplete_dependencies = set()

        for dependency in task.dependencies:
            if not Graph.valid_name(dependency) or dependency in self._failed:
                self._cascade_failure(task.name)
                break

            if dependency not in self._completed:
                incomplete_dependencies.add(dependency)
        else:  # task hasn't failed
            try:
                self._graph.add(task.name, incomplete_dependencies)
            except:
                self._cascade_failure(task.name)
                raise

    def start_task(self, name=None):
        """
        Start a task.

        Returns the task that was started (or None if no task has been
            started).

        name: (optional, None) The task to start. If a name is given,
            Scheduler will attempt to start the task (and raise an
            exception if the task doesn't exist or isn't runnable). If
            no name is given, a task will be chosen arbitrarily
        """
        if name is None:
            for possibility in self._graph.roots:
                if possibility not in self._running:
                    name = possibility
                    break
            else:  # all tasks blocked/running/completed/failed
                return None
        else:
            if name not in self._graph.roots or name in self._running:
                raise ValueError(name)

        self._running.add(name)

        return self._tasks[name]

    def end_task(self, name, success=True):
        """
        End a running task. Raises an exception if the task isn't
        running.

        name: The name of the task to complete.
        success: (optional, True) Whether the task was successful.
        """
        self._running.remove(name)

        if success:
            self._completed.add(name)
            self._graph.remove(name, strategy=Strategy.orphan)
        else:
            self._cascade_failure(name)

    def remove_unrunnable(self):
        """
        Remove any tasks that are dependent on non-existent tasks.
        """
        self._failed.update(self._graph.prune())

    def fail_remaining(self):
        """
        Mark all unfinished tasks (including currently running ones) as
        failed.
        """
        self._failed.update(self._graph.nodes)
        self._graph = Graph()
        self._running = set()

    def _cascade_failure(self, name):
        """
        Mark a task (and anything that depends on it) as failed.

        name: The name of the offending task
        """
        if name in self._graph:
            self._failed.update(
                self._graph.remove(name, strategy=Strategy.remove)
            )
        elif Graph.valid_name(name):
            self._failed.add(name)

    def __enter__(self):
        """
        Remove all unrunnable tasks and enter a context manager. When
        the context manager is exited, all non-complete tasks will be
        failed.
        """
        self.remove_unrunnable()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context manager, stopping (and failing) any
        non-completed tasks.
        """
        self.fail_remaining()
