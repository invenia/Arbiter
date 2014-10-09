"""
Arbiter is a 2.6, 2.7, 3.3+ compatible task-dependency solver.
"""
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
        self._running = set()  # Optional, can runnable -> complete
        self._completed = set()
        self._failed = set()

        for name, dependencies in tasks:
            self.add_task(name, dependencies=dependencies)

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
        """failed
        Read-only access to the failed set
        """
        return frozenset(self._failed)

    def resolve_dependencies(self, fail_on_missing=True):
        """
        Resolve all circular dependencies (by marking those tasks as
        failed).

        fail_on_missing: (Optional, True) Consider a task failed if it
            is blocked on a non-existent task. You would not want to do
            this if you may be adding more tasks after calling finished.
        """
        updated = True

        # everything that can run
        resolvable = self._runnable.union(self._running)
        unresolvable = set()

        unresolved = dict(self._blocked)

        while updated:
            updated = False

            still_unresolved = {}

            for task, dependencies in unresolved:
                remaining_dependencies = set()

                for dependency in dependencies:
                    if dependency not in resolvable:
                        if (dependency in unresolvable or
                                dependency not in self._tasks):
                            unresolvable.add(task)
                            updated = True
                            break
                        else:
                            remaining_dependencies.add(dependency)
                else:  # might still be resolvable
                    if remaining_dependencies:
                        still_unresolved[task] = remaining_dependencies
                    else:
                        resolvable.add(task)
                        updated = True

            unresolved = still_unresolved

        # any remaining tasks in unresolved have circular dependencies
        for task in chain(unresolvable, unresolved):
            del self._blocked[task]
            self._failed.add(task)


    def add_task(self, name, dependencies=None):
        """
        Add a dependency to the solver.

        name: The name of the task (must be unique).
        dependencies: (Optional, None) A list of dependencies the task
            is dependant on.
        """
        if name in self._tasks:
            raise ValueError(name)

        if dependencies is None:
            dependencies = []

        self._tasks.add(name)

        blockers = set()

        for dependency in dependencies:
            if dependency in self._failed:
                self._failed.add(name)
                break

            if dependency not in self._completed:
                blockers.add(dependency)
        else:  # not a failed task
            if blockers:
                self._blocked[name] = blockers
            else:
                self._runnable.add(name)

    def runnable_tasks(self):
        """
        Get a set of all tasks that are runnable.
        """
        return frozenset(self._runnable)

    def start_task(self, name=None):
        """
        Start a task. Returns None if no task can be started. Raises
        ValueErrors if the name of a task that isn't runnable is given.

        name: (Optional, None) The name of the task to start. If not
            given, a task will be arbitrarily chosen.

        NOTE: start_task is not required to run complete_task on a task,
        it is designed as a convenience to remove a task from runnable/
        get a task to run. Depending on your setup, grabbing from
        runnable_tasks may work better for you.
        """
        if name is None:  # arbitrary task
            try:
                name = self._runnable.pop()
            except KeyError:  # no runnable tasks left
                return None
        else:  # task better be runnable
            if name not in self._runnable:
                raise ValueError(name)

            self._runnable.remove(name)

        self._running.add(name)

        return name

    def complete_task(self, name, success=True):
        """
        Complete a task. Raises a ValueError if the task isn't runnbale
        (or currently listed as running)

        name: The name of the task to complete.
        success: (Optional, True) Did the task complete successfully?
        """
        if name in self._running:
            self._running.remove(name)
        elif name in self._runnable:
            self._runnable.remove(name)
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

            # There may be tasks that are unrunnable
            now_unrunnable = set()

            for task, dependencies in self._blocked.items():
                if name in dependencies:
                    now_unrunnable.add(task)

            for task in now_unrunnable:
                del self._blocked[task]
                self._failed.add(task)

    def stop(self):
        """
        Stop the solver, pushing any unfinished tasks into failed.
        Returns (set of successful tasks, set of failed tasks).
        """
        self._failed += (self._blocked + self._runnable + self._running)
        self._blocked = set()
        self._runnable = set()
        self._running = set()

        return self._completed, self._failed

    def results(self):
        """
        Returns (set of successful tasks, set of failed tasks).
        """

        return self._completed, self._failed
