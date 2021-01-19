from asyncio import Task
from typing import Mapping, Any, Container

from dagather.exceptions import DiscardedTask
from dagather.tasktemplate import TaskTemplate, ContinueResult


class DagatherResult(Mapping[TaskTemplate, Any]):
    def __init__(self, tasks: Mapping[TaskTemplate, Task], discarded: Container[TaskTemplate]):
        self.tasks = tasks
        self.discarded = discarded

    def __getitem__(self, item):
        try:
            ret = self.tasks[item].result()
        except KeyError:
            if item in self.discarded:
                raise DiscardedTask(item) from None
            raise
        if isinstance(ret, ContinueResult):
            return ret.return_value
        return ret

    def __iter__(self):
        return iter(self.tasks)

    def keys(self):
        return self.tasks.keys()

    def __len__(self):
        return len(self.tasks)

    def kwargs(self):
        """
        :return: A dict of the results as names rather than templates
        """
        return {tt.name: v for (tt, v) in self.items()}
