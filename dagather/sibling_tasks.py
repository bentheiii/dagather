from __future__ import annotations

from asyncio import Task
from enum import Enum, auto
from typing import MutableMapping, Any, Mapping, Collection, Optional, MutableSet

from dagather.exceptions import DiscardedTask
from dagather.tasktemplate import TaskTemplate
from dagather.util import remove_keys_transitively


class SiblingTaskState(Enum):
    discarded = auto()
    waiting = auto()
    running = auto()
    done = auto()


class SiblingTasks:
    def __init__(self, created_tasks: MutableMapping[TaskTemplate, Task],
                 waiting: MutableMapping[TaskTemplate, Any],
                 discarded: MutableSet[TaskTemplate],
                 dependency_relation: Mapping[TaskTemplate, Collection[TaskTemplate]]):
        self._created_tasks = created_tasks
        self._waiting = waiting
        self._discarded = discarded
        self._dependency_relation = dependency_relation

    def state_of(self, key) -> SiblingTaskState:
        try:
            task = self.task(key)
        except DiscardedTask:
            return SiblingTaskState.discarded
        if not task:
            return SiblingTaskState.waiting
        if task.done():
            return SiblingTaskState.done
        return SiblingTaskState.running

    def task(self, key: TaskTemplate) -> Optional[Task]:
        if key in self._discarded:
            raise DiscardedTask(f'{key!r} has been discarded')
        return self._created_tasks.get(key)

    def cancel(self, key: TaskTemplate):
        """
        Cancel a running task, or discard a waiting one
        """
        task = self.task(key)
        if task:
            task.cancel()
        else:
            remove_keys_transitively(self._waiting, self._dependency_relation, key, self._discarded)
