"""
Arbiter is a 2.6/3.3+ compatible task-dependency solver.
"""
from arbiter.graph import DirectedGraph


__all__ = ('Arbiter',)


class Arbiter(object):
    """
    A task-dependency solver.
    """
    def __init__(self, tasks=None, completed=None, failed=None):
        if tasks is None:
            tasks = {}

        if completed is None:
            completed = set()
        elif not isinstance(completed, set):
            raise TypeError(completed)

        if failed is None:
            failed = set()
        elif not isinstance(failed, set):
            raise TypeError(failed)

        self._graph = DirectedGraph(acyclic=True)

        self._tasks = set()
        self._running = set()
        self._completed = completed
        self._failed = failed

        for name, dependencies in tasks.items():
            self.add_task(name, dependencies=dependencies)

    @property
    def completed(self):
        """
        Read-only access to the completed set
        """
        return frozenset(self._completed)

    @property
    def failed(self):
        """
        Read-only access to the failed set
        """
        return frozenset(self._failed)

    def add_task(self, name, dependencies=None):
        """
        Add a new task to the solver.

        name: The name of the task being added. The name must be unique,
            and cannot be None.
        dependencies: (optional, None) An iterable of dependencies that
            must complete successfully prior to running this task. If
            any of these dependencies fail, the task will also fail.

        NOTE: An exception will be thrown if the solver is no longer
        running
        """
        if self._graph is None:
            raise ValueError  # solver no longer running

        if name is None:
            raise ValueError(name)

        remaining_dependencies = {}

        for dependency in (dependencies or ()):
            if dependency in self._failed:
                # are there already tasks dependent on this task?
                self._cascade_failure(name)

                self._tasks.add(name)

                break

            if dependency not in self._completed:
                remaining_dependencies.add(dependency)
        else:  # not a failure
            self._graph.add_node(name, dependencies)
            self._tasks.add(name)

    def start_task(self, name=None):
        """
        Start a task.

        Returns the name of the started task (or None if no task is
            started).

        If a specific task is given and it doesn't exist, or isn't
        runnable, an exception will be thrown.

        name: (optional, None) The task to start. If no task is given, a
            task will be arbitrarily chosen from the set of runnable
            tasks (if any are available).

        NOTE: An exception will be thrown if the solver is no longer
        running
        """
        if self._graph is None:
            raise ValueError  # solver no longer running

        if name is None:
            for possible_name in self._graph.roots:
                if possible_name not in self._running:
                    name = possible_name
                    break
            else:  # all tasks block/running/completed/failed
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
        success: (optional, True) Whether the task successfully
            completed.

        NOTE: An exception will be thrown if the solver is no longer
        running
        """
        if self._graph is None:
            raise ValueError  # solver no longer running

        self._running.remove(name)

        if success:
            self._completed.add(name)
            self._graph.remove_node(name, remove_children=False)
        else:
            self._cascade_failure(name)

    def _cascade_failure(self, name):
        """
        Mark a task (and any of it's decendants as failed)

        name: The name of the offending task.
        """
        if name in self._graph:
            self._failed.update(
                self._graph.remove_node(name, remove_children=True)
            )
        else:
            self._failed.add(name)

    def remove_dead_tasks(self):
        """
        Remove any tasks that are dependent on non-existent tasks.
        """
        self._failed.update(self._graph.prune())

    def stop(self):
        """
        Stop the solver, pushing any unfinished tasks into failed.
        """
        if self._graph:
            self._running = frozenset()
            self._failed.update(self._graph.nodes)

            self._graph = None

    def __enter__(self):
        """
        Enter a context manager. On entering, remove_dead_tasks will be
        called, and on exit, stop will be called.
        """
        self.remove_dead_tasks()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context manager, stopping any non-completed tasks.
        """
        self.stop()
