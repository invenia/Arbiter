"""
Task creation/generation
"""
from collections import namedtuple
from uuid import uuid4
from functools import partial


Task = namedtuple(
    'Task',
    ('name', 'function', 'handler', 'dependencies', 'args', 'kwargs'),
)


def create_task(function, *args, **kwargs):
    """
    Create a task object
    name: The name of the task.
    function: The actual task function. It should take no arguments,
        and return a False-y value if it fails.
    dependencies: (optional, ()) Any dependencies that this task relies
        on.
    """
    name = "{}".format(uuid4())
    handler = None
    deps = set()

    if 'name' in kwargs:
        name = kwargs['name']
        del kwargs['name']
    elif function is not None:
        name = "{}-{}".format(function.__name__, name)

    if 'handler' in kwargs:
        handler = kwargs['handler']
        del kwargs['handler']

    if 'dependencies' in kwargs:
        for dep in kwargs['dependencies']:
            deps.add(dep)
        del kwargs['dependencies']

    for arg in args:
        if isinstance(arg, Task):
            deps.add(arg.name)

    for key in kwargs:
        if isinstance(kwargs[key], Task):
            deps.add(kwargs[key].name)

    return Task(name, function, handler, frozenset(deps), args, kwargs)


class TaskStore(object):
    
    def __init__(self):
        self._results = {}

    def get(self, name):
        """
        Retrieve a task result given its unique task name.
        """
        return self._results[name]

    def put(self, name, value):
        """
        Retrieve a task result given its unique task name.
        """
        self._results[name] = value

    
    

