from __future__ import annotations

import sys
from asyncio import wait, Task, create_task, FIRST_COMPLETED, CancelledError
from collections import defaultdict
from contextvars import ContextVar
from functools import partial
from inspect import signature, Parameter
from typing import Set, Dict, Callable, overload, TypeVar

from dagather.sibling_tasks import SiblingTasks
from dagather.exceptions import CycleError
from dagather.result import DagatherResult
from dagather.tasktemplate import TaskTemplate, ExceptionHandler, CancelPolicy, PropagateError, ContinueResult, \
    PostErrorResult
from dagather.util import remove_keys_transitively, filter_dict

if sys.version_info < (3, 9, 0):
    def cancel_task(task, msg):
        return task.cancel()
else:
    def cancel_task(task, msg):
        return task.cancel(msg)

if sys.version_info < (3, 8, 0):
    # prior to 3.8, create_task did not accept name parameter
    _ct = create_task

    def create_task(coro, *, name=None):
        return _ct(coro)

param_kind_ignore = frozenset((
    Parameter.POSITIONAL_ONLY, Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD
))

T = TypeVar('T')

sibling_tasks: ContextVar[SiblingTasks] = ContextVar('sibling_tasks')
"""
A context variable to store all the tasks currently running or completed in a single Dagather run
"""


class Dagather:
    """
    A collection of tasks templates.
    """

    def __init__(self, default_exception_handler: ExceptionHandler = PropagateError):
        """
        :param default_exception_handler: The default exception handler for new task templates
        """
        self._kwarg_users: Dict[str, Set[TaskTemplate]] = defaultdict(set)
        # maps parameter names to templates that use them. For use for when new subtasks are added,
        # and we want to assign dependencies. parameters that are templates names will not appear here.
        self._templates: Dict[str, TaskTemplate] = {}
        # a mapping of templates by their names
        self.default_exception_handler = default_exception_handler

    @overload
    def register(self, func: Callable[..., T], **kwargs) -> TaskTemplate[T]:
        pass

    @overload
    def register(self, **kwargs) -> Callable[..., TaskTemplate]:
        pass

    def register(self, func: Callable = ..., exception_handler: ExceptionHandler = ...):
        """
        Create a new task template, and register it to the dagather.
        :param func: The callable to wrap in a TaskTemplate. If missing, a partial function is returned.
        :param exception_handler: The exception handler of the task template, default value is to use the
            dagather's default exception handler.
        :return: The task template.

        .. note::
            This method can be used as a decorator.
        """
        if func is ...:
            return partial(self.register, exception_handler=exception_handler)
        name = func.__name__

        if exception_handler is ...:
            exception_handler = self.default_exception_handler

        if name in self._templates:
            raise ValueError(f'duplicate sub-task name {name}')

        # split keyword arguments into parent template and potential future parent subtasks
        kwargs = set()
        dependencies = set()

        sign = signature(func)
        for param in sign.parameters.values():
            if param.kind in param_kind_ignore:
                continue
            parent_task = self._templates.get(param.name)
            if parent_task:
                dependencies.add(parent_task)
            else:
                kwargs.add(param.name)

        template = TaskTemplate(name, func, dependencies, exception_handler=exception_handler)
        self._templates[name] = template
        for kw in kwargs:
            self._kwarg_users[kw].add(template)

        # any previous template that use the current template as a keyword will now use it as a dependency
        dependants = self._kwarg_users.pop(name, ())
        for dependant in dependants:
            dependant.dependencies.add(template)

        return template

    async def __call__(self, *args, **kwargs) -> DagatherResult:
        """
        Call all the task templates in topological order
        :param args: forwarded to all tasks as positional arguments
        :param kwargs: forwarded to all tasks as keyword arguments
        :return: a dict, mapping completed task templates to their result value, or their raised exception,
         if an exception was raised.
        """
        delayed_exception = None
        intermediary = {}

        bad_keys = kwargs.keys() & self._templates.keys()
        if bad_keys:
            raise TypeError(f'cannot accept keywords arguments of subtask names {list(bad_keys)}')

        # helper function to create a task from a template
        def mk_task(st: TaskTemplate):
            kw = {**filter_dict(intermediary, (d.name for d in st.dependencies)), **kwargs}
            coroutine = st._safe_call(args, kw)
            task = create_task(
                coroutine,
                name=st.name
            )
            tasks[task] = st
            inv_tasks[st] = task
            return task

        tasks: Dict[Task, TaskTemplate] = {}
        # mapping created tasks to their original template
        inv_tasks: Dict[TaskTemplate, Task] = {}

        pending: Set[Task] = set()
        # a set of all currently running templates
        not_ready: Dict[TaskTemplate, Set[TaskTemplate]] = {}
        # a mapping of all templates that are waiting for other tasks to complete
        dependants: Dict[TaskTemplate, Set[TaskTemplate]] = defaultdict(set)
        # a mapping of all templates to all other subtasks that *might* be waiting for them directly
        discarded: Set[TaskTemplate] = set()
        # a collection of all the discarded tasks

        sibling_tasks.set(SiblingTasks(inv_tasks, not_ready, discarded, dependants))

        for st in self._templates.values():
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

            try:
                # pytype: disable=annotation-type-mismatch #in future versions, pending will 100% return tasks
                done_tasks, pending = await wait(pending, return_when=FIRST_COMPLETED)
                # pytype: enable=annotation-type-mismatch
            except CancelledError:
                for task in tasks:
                    cancel_task(task, 'cancelled by caller')
                raise

            done: Task
            for done in done_tasks:
                st = tasks[done]

                result = done.result()

                if isinstance(result, PostErrorResult):
                    if result.cancel_policy is CancelPolicy.cancel_all:
                        discarded.update(not_ready)
                        not_ready.clear()
                        for task in tasks:
                            cancel_task(task, f'cancelled by sibling task "{st.name}"')
                    elif result.cancel_policy is CancelPolicy.discard_not_started:
                        discarded.update(not_ready)
                        not_ready.clear()
                    elif result.cancel_policy is CancelPolicy.discard_children:
                        remove_keys_transitively(not_ready, dependants, st, discarded)

                    if isinstance(result, PropagateError):
                        if not delayed_exception:
                            delayed_exception = result.exception
                        result = result.exception
                    else:
                        assert isinstance(result, ContinueResult)
                        result = result.return_value

                intermediary[st.name] = result

                for dependant in dependants[st]:
                    if dependant not in not_ready:
                        # the template may have been removed by another cancelled task
                        continue
                    not_ready[dependant].remove(st)
                    if not not_ready[dependant]:
                        del not_ready[dependant]
                        pending.add(mk_task(dependant))

        if delayed_exception:
            raise delayed_exception
        return DagatherResult(inv_tasks, discarded)
