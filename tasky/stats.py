# Copyright 2016 John Reese
# Licensed under the MIT license

import logging

from collections import defaultdict

from .tasks import Task

Log = logging.getLogger('tasky.stats')


class DictWrapper(object):
    '''Provides a dict-like interface to another dict, where all key lookups
    prepend the given prefix string before passing through to the underlying
    data structure.'''

    def __init__(self, data: dict, prefix: str='') -> None:
        assert isinstance(data, dict)

        self.data = data

        if prefix:
            self.template = '{}.{{}}'.format(prefix)
        else:
            self.template = '{}'

    def __bool__(self) -> bool:
        return bool(self.data)

    def __len__(self) -> int:
        return self.data.__len__()

    def __iter__(self):
        return self.data.__iter__()

    def __contains__(self, key):
        return self.data.__contains__(self.template.format(key))

    def __getitem__(self, key):
        return self.data.__getitem__(self.template.format(key))

    def __setitem__(self, key, value):
        return self.data.__setitem__(self.template.format(key), value)

    def __delitem__(self, key):
        return self.data.__delitem__(self.template.format(key))

    def __missing__(self, key):
        return self.data.__missing__(self.template.format(key))

    def clear(self):
        return self.data.clear()

    def copy(self):
        return self.data.copy()

    def get(self, key, default=None):
        return self.data.get(self.template.format(key), default)

    def items(self):
        return self.data.items()

    def keys(self):
        return self.data.keys()

    def pop(self, key, default=None):
        return self.data.pop(self.template.format(key), default)

    def popitem(self):
        return self.data.popitem()

    def setdefault(self, key, default=None):
        return self.data.setdefault(self.template.format(key), default)

    def values(self):
        return self.data.values()


class Stats(Task):
    '''Mechanism for providing key-based counters across all tasks.  These
    counters could then be periodically aggregated, exported, or reset.
    Base implementation just tracks counters without processing them.'''

    async def init(self) -> None:
        self.data = defaultdict(int)

    def counter(self, prefix: str='') -> DictWrapper:
        return DictWrapper(self.data, prefix)

    def global_counter(self) -> DictWrapper:
        return self.counter()

    def task_counter(self, task: Task) -> DictWrapper:
        return self.counter(task.__class__.__name__)

    def dump(self) -> dict:
        return self.data.copy()

    def __bool__(self) -> bool:
        return len(self.data) > 0
