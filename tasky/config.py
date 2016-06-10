# Copyright 2016 John Reese
# Licensed under the MIT license

import json
import logging

from typing import Any

from .tasks import Task

Log = logging.getLogger('tasky')


class Config(Task):
    '''Mechanism for providing read-only configuration values to a service,
    as well as individual tasks in that service, from either a local source
    or an external configuration service.  Base implementation simply stores
    a static dictionary, and emulates a read-only container interface.'''

    def __init__(self, data: dict=None) -> None:
        if not data:
            data = {}

        self.data = data

    def get(self, key: Any, default: Any=None) -> Any:
        '''Return the configured value for the given key name, or `default` if
        no value is available or key is invalid.'''

        return self.data.get(key, default)

    def global_config(self) -> Any:
        '''Return the global service configuration.'''

        return self.data

    def task_config(self, task: Task) -> Any:
        '''Return the task-specific configuration.'''

        return self.get(task.__class__.__name__)

    async def init(self) -> None:
        '''Gather initial configuration data from the backing.'''

        pass

    async def run(self) -> None:
        '''Potentially run any amount of one-shot or ongoing async code
        necessary to maintain configuration data.'''

        pass

    def __repr__(self):
        return '{}(data={})'.format(self.name, self.data)


class JsonConfig(Config):
    '''Provide configuration from a local JSON file.'''

    def __init__(self, json_path: str=None, json_data: str=None) -> None:
        self.json_path = json_path
        self.json_data = json_data
        self.data = None

    @property
    def enabled(self) -> bool:
        '''Enabled until data is fetched, then disabled.'''
        return self.data is None

    async def init(self) -> None:
        '''Load configuration in JSON format from either a file or
        a raw data string.'''

        if self.data:
            return

        if self.json_data:
            try:
                self.data = json.loads(self.json_data)

            except Exception:
                Log.exception('Falied to load raw configuration')

        else:
            try:
                with open(self.json_path, 'r') as f:
                    self.data = json.load(f)

            except Exception:
                Log.exception('Failed to load configuration from %s',
                              self.json_path)
                self.data = {}
