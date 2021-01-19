from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from functools import update_wrapper, partial
from typing import Callable, TypeVar, Generic, Coroutine, Set, Any, Union, Type, Mapping


class CancelPolicy(Enum):
    """
    Which templates to cancel if an exception is raised.
    """
    cancel_all = auto()
    """
    Cancel all tasks, causing all started tasks to raise a CancelledError and discarding all others
    """
    discard_not_started = auto()
    """
    Discard all tasks that have not yet been started.
    """
    discard_children = auto()
    """
    Discard all subtasks that rely on the result of the failed task.
    """
    continue_all = auto()
    """
    No subtasks are canceled.
    """


class PostErrorResult:
    cancel_policy: CancelPolicy

    @classmethod
    def exception_handler(cls, cancel_policy: CancelPolicy):
        return partial(cls, cancel_policy=cancel_policy)


@dataclass
class ContinueResult(PostErrorResult):
    return_value: Any
    cancel_policy: CancelPolicy = CancelPolicy.continue_all


@dataclass
class PropagateError(PostErrorResult):
    exception: BaseException
    cancel_policy: CancelPolicy = CancelPolicy.cancel_all


class Abort(BaseException):
    """
    A special exception type, handleable by the task templates.
    If raised inside a dagather task, the dagather acts according to the post_error_result.
    """

    def __init__(self, post_error_result: PostErrorResult):
        super().__init__(post_error_result)


# pytype: disable=not-supported-yet
ExceptionHandler = Union[
    PostErrorResult,
    Callable[[BaseException], 'ExceptionHandler'],
    Mapping[Type[BaseException], 'ExceptionHandler']
]
# pytype: enable=not-supported-yet
"""
A protocol for exception handlers, that dictate how subtasks should behave if they fail.
"""


def handle_exception(handler: ExceptionHandler, exc: BaseException, base_explicit=False):
    if isinstance(handler, PostErrorResult):
        if not base_explicit and not isinstance(exc, Exception):
            return PropagateError(exc)
        return handler
    if callable(handler):
        return handle_exception(handler(exc), exc, base_explicit)
    for k, v in handler.items():
        if isinstance(exc, k):
            return handle_exception(v, exc, True)
    return PropagateError(exc)


T = TypeVar('T')


class TaskTemplate(Generic[T]):
    """
    A template for a sub-task in a dagather instance
    """

    def __init__(self, name: str,
                 callback: Callable[..., Coroutine[None, None, T]],
                 dependencies: Set[TaskTemplate],
                 exception_handler: ExceptionHandler):
        """
        :param name: the name of the template
        :param callback: the async function to call when executing the task
        :param dependencies: a set of task templates that must be completed before this task is started.
        :param exception_handler: the exception handler to use if the task raises an exception
        """
        self.name = name
        self.callback = callback
        self.dependencies = dependencies
        self.exception_handler = exception_handler
        update_wrapper(self, callback)

    async def _safe_call(self, args, kwargs):
        """
        call the inner function while catching any errors and using the template's exception wrapper.
        """
        try:
            result = await self(*args, **kwargs)
        except Abort as e:
            return e.args[0]
        except BaseException as e:
            return handle_exception(self.exception_handler, e)

        # pytype: disable=name-error
        if isinstance(result, PostErrorResult):
            raise TypeError('subtask must not return a PostErrorResult, raise an AbortSubtask instead')
        return result
        # pytype: enable=name-error

    def __call__(self, *args, **kwargs) -> Coroutine[None, None, T]:
        """
        call the base callable of the template.
        """
        return self.callback(*args, **kwargs)

    def __repr__(self):
        return f'TaskTemplate({self.name})'
