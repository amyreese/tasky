# Copyright 2016 John Reese
# Licensed under the MIT license
# flake8: noqa

from concurrent.futures import CancelledError

from .tasks import Task, PeriodicTask, TimerTask, QueueTask
from .config import Config, JsonConfig
from .stats import Stats
from .loop import Tasky

__version__ = '0.7.1'
