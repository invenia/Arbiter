"""
The dependency scheduler.
"""
from collections import Hashable

from arbiter.graph import DirectedGraph


__all__ = ('Scheduler',)


class Scheduler(object):
    """
    A dependency scheduler.
    """
    def __init__(self, tasks=None, completed=None, failed=None):
        if completed is None:
            completed = set()

        if failed is None:
            failed = set()

        self._graph = DirectedGraph(acyclic=True)
        self._tasks = set()
        self._running = set()
        self._completed = completed
        self._failed = failed

        if tasks is not None:
            for name, dependencies in tasks.items():
                self.add_task(name, dependencies=dependencies)

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

    def runnable(self):
        """
        Get the set of tasks that are currently runnable.
        """
        return self._graph.roots - self._running

    def add_task(self, name, dependencies=None):
        """
        Add a task to the scheduler.

        name: The name of the task being added. The name must be unique,
            hashable, and cannot be None.
        dependencies: (optional, None) An iterable of dependencies that
            must complete successfully prior to running this task. If
            any of these dependencies fail, the task will also fail.
        """
        if not self._valid_name(name):
            raise ValueError(name)

        self._tasks.add(name)

        incomplete_dependencies = set()

        for dependency in (dependencies or ()):
            if not self._valid_name(dependency) or dependency in self._failed:
                # there may already be tasks dependent on this one.
                self._cascade_failure(name)

                break

            if dependency not in self._completed:
                incomplete_dependencies.add(dependency)
        else:  # task hasn't failed
            try:
                self._graph.add_node(name, incomplete_dependencies)
            except ValueError:
                self._cascade_failure(name)

    def start_task(self, name=None):
        """
        Start a task.

        Returns the name of the task that was started (or None if no
            task has been started).

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

        return name

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
            self._graph.remove_node(name, remove_children=False)
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
        self._graph = DirectedGraph(acyclic=True)
        self._running = set()

    def _valid_name(self, name):
        """
        Check whether a name is valid as a task name.
        """
        return name is not None and isinstance(name, Hashable)

    def _cascade_failure(self, name):
        """
        Mark a task (and anything that depends on it) as failed.

        name: The name of the offending task
        """
        if name in self._graph:
            self._failed.update(
                self._graph.remove_node(name, remove_children=True)
            )
        else:
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
