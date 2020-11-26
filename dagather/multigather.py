from __future__ import annotations

import sys
from asyncio import wait, Task, create_task, FIRST_COMPLETED
from collections import defaultdict, namedtuple
from enum import Enum, auto
from functools import update_wrapper, partial
from inspect import signature, Parameter
from typing import Set, Callable, TypeVar, Coroutine, Dict, Generic

from dagather.exceptions import CycleError
from dagather.util import remove_keys_transitively, filter_dict

T = TypeVar('T')

if sys.version_info < (3, 8, 0):
    _ct = create_task

    def create_task(coro, *, name=None):
        return _ct(coro)


class ExceptionPolicy(Enum):
    cancel_not_started = auto()
    cancel_children = auto()
    continue_all = auto()


_DelayedException = namedtuple('_DelayedException', 'exc')
_CANCEL_NOT_STARTED = object()
_CANCEL_CHILDREN = object()
_CONTINUE_ALL = object()


class Subtask(Generic[T]):
    def __init__(self, name: str,
                 callback: Callable[..., Coroutine[None, None, T]],
                 dependencies: Set[Subtask],
                 exception_policy: ExceptionPolicy,
                 return_exception: bool
                 ):
        self.name = name
        self.callback = callback
        self.dependencies = dependencies
        self.exception_policy = exception_policy
        self.return_exception = return_exception
        update_wrapper(self, callback)

    async def _safe_call(self, args, kwargs):
        continual = _CONTINUE_ALL
        try:
            result = await self(*args, **kwargs)
        except Exception as e:
            if not self.return_exception:
                result = _DelayedException(e)
            else:
                result = e

            if self.exception_policy is ExceptionPolicy.cancel_not_started:
                continual = _CANCEL_NOT_STARTED
            elif self.exception_policy is ExceptionPolicy.cancel_children:
                continual = _CANCEL_CHILDREN

        return continual, result

    def __call__(self, *args, **kwargs):
        return self.callback(*args, **kwargs)

    def __repr__(self):
        return f'Subtask({self.name})'


param_kind_ignore = frozenset((
    Parameter.POSITIONAL_ONLY, Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD
))


class Dagather:
    def __init__(self):
        self._kwargs: Dict[str, Set[Subtask]] = defaultdict(set)
        self._subtasks: Dict[str, Subtask] = {}

    def register(self, func=..., exception_policy: ExceptionPolicy = ExceptionPolicy.cancel_not_started,
                 return_exceptions: bool = False):
        if func is ...:
            return partial(self.register, exception_policy=exception_policy, return_exceptions=return_exceptions)
        name = func.__name__
        if name in self._subtasks:
            raise ValueError(f'duplicate sub-task name {name}')
        kwargs = set()
        dependencies = set()

        sign = signature(func)
        for param in sign.parameters.values():
            if param.kind in param_kind_ignore:
                continue
            parent_task = self._subtasks.get(param.name)
            if parent_task:
                dependencies.add(parent_task)
            else:
                kwargs.add(param.name)

        subtask = Subtask(name, func, dependencies, exception_policy=exception_policy,
                          return_exception=return_exceptions)
        self._subtasks[name] = subtask
        for kw in kwargs:
            self._kwargs[kw].add(subtask)

        dependants = self._kwargs.pop(name, ())
        for dependant in dependants:
            dependant.dependencies.add(subtask)

        return subtask

    async def __call__(self, *args, **kwargs):
        delayed_exception = None
        intermediary = {}
        results = {}

        bad_keys = kwargs.keys() & self._subtasks.keys()
        if bad_keys:
            raise TypeError(f'cannot accept keywords arguments of subtask names {list(bad_keys)}')

        def mk_task(st: Subtask):
            kw = {**filter_dict(intermediary, (d.name for d in st.dependencies)), **kwargs}
            coroutine = st._safe_call(args, kw)
            task = create_task(
                coroutine,
                name=st.name
            )
            tasks[task] = st
            return task

        tasks: Dict[Task, Subtask] = {}

        pending: Set[Task] = set()
        not_ready: Dict[Subtask, Set[Subtask]] = {}
        dependants: Dict[Subtask, Set[Subtask]] = defaultdict(set)

        for st in self._subtasks.values():
            for dependancy in st.dependencies:
                dependants[dependancy].add(st)

            if not st.dependencies:
                pending.add(mk_task(st))
            else:
                not_ready[st] = set(st.dependencies)

        while True:
            if not pending:
                if not_ready:
                    raise CycleError(f"cyclic dependancy between multiple subtasks: {list(not_ready)}")
                break

            # pytype: disable=annotation-type-mismatch #in future versions, pending will 100% return tasks
            done_tasks, pending = await wait(pending, return_when=FIRST_COMPLETED)
            # pytype: enable=annotation-type-mismatch

            done: Task
            for done in done_tasks:
                st = tasks[done]

                continual, result = done.result()

                if isinstance(result, _DelayedException):
                    if not delayed_exception:
                        delayed_exception = result.exc
                    result = result.exc

                results[st] = intermediary[st.name] = result

                if continual is _CANCEL_NOT_STARTED:
                    not_ready.clear()
                elif continual is _CANCEL_CHILDREN:
                    remove_keys_transitively(not_ready, dependants, st)
                else:
                    for dependant in dependants[st]:
                        if dependant not in not_ready:
                            # the sub-task may have been removed by another cancelled task
                            continue
                        not_ready[dependant].remove(st)
                        if not not_ready[dependant]:
                            del not_ready[dependant]
                            pending.add(mk_task(dependant))

        if delayed_exception:
            raise delayed_exception
        return results
