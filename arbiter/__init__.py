"""
Arbiter is a 2.7/3.3+ compatible task-dependency solver.
"""
__all__ = ('Arbiter',)

from collections import namedtuple
from itertools import chain


class Arbiter(object):
    """
    A task-dependency solver.
    """
    def __init__(self, tasks=None):
        if tasks is None:
            tasks = {}

        self._tasks = set()
        self._blocked = {}
        self._runnable = set()
        self._running = set()
        self._completed = set()
        self._failed = set()

        for name, dependencies in tasks.items():
            self.add_task(name, dependencies=dependencies)

    @property
    def tasks(self):
        """
        Read-only access to the tasks set
        """
        return frozenset(self._tasks)

    @property
    def blocked(self):
        """
        Read-only access to the blocked dict
        """
        return dict(self._blocked)

    @property
    def runnable(self):
        """
        Read-only access to the runnable set
        """
        return frozenset(self._runnable)

    @property
    def running(self):
        """
        Read-only access to the running set
        """
        return frozenset(self._running)

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

    def remove_dead_tasks(self, keep_orphans=False):
        """
        Remove any tasks (by moving them to failed) that won't ever be
        runnable. A task is dead if:

         *  It forms a circular dependency with one or more other tasks.
         *  It is dependent on a non-existent task (and keep_orphans is
            set to True).
         *  It is dependent on a task that has been marked as dead.

        NOTE: The end_task method already handles removing dead
            nodes created by a task failing, so we don't need to worry
            about them here. It also handles updating dependencies on
            the blocked list when a task completes successfully, so we
            can assume all dependencies are up-to-date.

        keep_orphans: (optional, False) If True, tasks that are
            dependent on non-existent tasks won't be considered dead.
        """
        dead = set()
        resolvable = self._runnable.union(self._running)

        for task, dependencies in self._blocked.items():
            if not (task in resolvable or task in dead):
                stack = [StackItem(task, {task}, iter(dependencies))]
                living = True

                while stack and living:
                    curr = stack[-1]

                    try:
                        dependency = next(curr.dependencies)

                        if dependency in dead or dependency in curr.visited:
                            living = False
                        elif dependency not in resolvable:
                            child_dependencies = self._blocked.get(dependency)

                            if child_dependencies:
                                stack.append(StackItem(
                                    dependency,
                                    curr.visited.union({dependency}),
                                    iter(child_dependencies),
                                ))
                            else:
                                # We know that curr.task is an orphan
                                # because a valid dependency has to be
                                # in runnable, running, or blocked.
                                if not keep_orphans:
                                    living = False
                    except StopIteration:
                        resolvable.add(curr.task)
                        stack.pop()

                    if not living:
                        while stack:
                            dead.add(stack.pop().task)

        for task in dead:
            del self._blocked[task]
            self._failed.add(task)


    def add_task(self, name, dependencies=None):
        """
        Add a new task to the solver.

        name: The name of the task being added. The name must be unique,
            and cannot be None.
        dependencies: (optional, None) An iterable of dependencies that
            must complete successfully prior to running this task. If
            any of these dependencies fail, the task will also fail.
        """
        if name is None or name in self._tasks:
            raise ValueError(name)

        if dependencies is None:
            dependencies = ()

        self._tasks.add(name)

        blockers = set()

        for dependency in dependencies:
            if dependency in self._failed:
                self._failed.add(name)

                # there may already be tasks reliant on this task
                self._cascade_failure(name)

                break

            if dependency not in self._completed:
                blockers.add(dependency)
        else:  # not a failed task
            if blockers:
                self._blocked[name] = blockers
            else:
                self._runnable.add(name)

    def start_task(self, name=None):
        """
        Move a task from runnable to running. Returns the name of the
        started task (or None if no task is started). If a name of a
        task is given and it is not runnable, a ValueError will be
        thrown.

        name: (optional, None) The name of the task to start. If not
            given, a task will be arbitrarily chosen.
        """
        if name is None:  # arbitrary task
            if self._runnable:
                name = self._runnable.pop()
            else:
                return None  # no more runnable tasks remain
        else:  # task better be runnable
            if name not in self._runnable:
                raise ValueError(name)

            self._runnable.remove(name)

        self._running.add(name)

        return name

    def end_task(self, name, success=True):
        """
        End a running task. Raises a ValueError if the task isn't
        running.

        name: The name of the task to complete.
        success: (optional, True) Whether the task successfully
            completed.
        """
        if name in self._running:
            self._running.remove(name)
        else:  # This task can't be completed
            raise ValueError(name)

        if success:
            self._completed.add(name)

            # There may be tasks that are no longer blocked
            now_runnable = set()

            for task, dependencies in self._blocked.items():
                if name in dependencies:
                    dependencies.remove(name)

                    if not dependencies:
                        now_runnable.add(task)

            for task in now_runnable:
                del self._blocked[task]
                self._runnable.add(task)
        else:
            self._failed.add(name)
            self._cascade_failure(name)

    def _cascade_failure(self, name):
        """
        Mark any descendents of a task as failed.

        name: The name of the offending task.
        """
        failures = {name}
        remaining = list(self._blocked.items())
        finished = False

        while not finished:
            finished = True
            new_remaining = list()

            for task, dependencies in remaining:
                # TODO: Is intersection overkill? We don't actually care
                # what the overlap is, just that it exists
                if failures.intersection(dependencies):
                    del self._blocked[task]
                    self._failed.add(task)

                    failures.add(task)
                    finished = False
                else:
                    new_remaining.append((task, dependencies))

            remaining = new_remaining

    def stop(self):
        """
        Stop the solver, pushing any unfinished tasks into failed.
        Returns (set of successful tasks, set of failed tasks).
        """
        self._failed = set(
            chain(
                self._failed, self._blocked, self._runnable, self._running
            )
        )

        self._blocked = set()
        self._runnable = set()
        self._running = set()

        return self._completed, self._failed

    def __enter__(self):
        """
        Enter a context manager. Presently, the context manager just
        calls stop on exit, so there is nothing to do here.

        Enter does return self so you can do:

        with Arbiter(tasks=tasks) as arbiter:
            # do stuff

        I would recommend against it because you won't be able to check
        completed/failure afterward.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context manager, stopping any non-completed tasks.
        """
        self.stop()


# Used by remove_dead_tasks
StackItem = namedtuple('StackItem', ('task', 'visited', 'dependencies'))
