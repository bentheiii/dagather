from __future__ import annotations

from enum import Enum, auto
from functools import update_wrapper
from typing import Callable, NamedTuple, TypeVar, Generic, Coroutine, Set, Any


class CancelPolicy(Enum):
    """
    Which template to cancel if an exception is raised.
    """
    cancel_not_started = auto()
    """
    Cancel all tasks that have not yet been started.
    """
    cancel_children = auto()
    """
    Cancel all subtasks that rely on the result of the failed task.
    """
    continue_all = auto()
    """
    No subtasks are canceled.
    """


class PostErrorResult(NamedTuple):
    """
    Behavioral specifications after a task raised an exception.
    .. warning::
        PostErrorResult is handled especially inside dagather. Returning it directly will cause unexpected behaviour.
    """
    result: Any
    """
    The result to be recorded as the output of the task. This is usually the exception raised.
    """
    cancel_policy: CancelPolicy
    """
    The cancel policy to employ.
    """
    return_exception: bool
    """
    Whether to suppress the exception and not raise it in the parent coroutine (after all non-cancelled tasks are
     completed).
    """


class Abort(BaseException):
    """
    A special exception type, handleable by the default exception handler.
    If raised inside a dagather task, the dagather acts according to the post_error_result.
    """

    def __init__(self, post_error_result: PostErrorResult):
        super().__init__(post_error_result)


ExceptionHandler = Callable[[Exception, 'TaskTemplate'], PostErrorResult]
"""
A protocol for exception handlers, that dictate how subtasks should behave if they fail.
"""


class ErrorHandler(ExceptionHandler):
    """
    The standard default exception handler
    """

    def __init__(self, cancel_policy: CancelPolicy = CancelPolicy.cancel_not_started, return_exception: bool = False):
        """
        :param cancel_policy: The cancel policy to apply on exceptions
        :param return_exception: Whether to suppress the exception from the main dagather.
        """
        self.cancel_policy = cancel_policy
        self.return_exception = return_exception

    def __call__(self, exc, template) -> PostErrorResult:
        return PostErrorResult(exc, self.cancel_policy, self.return_exception)


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
        except Exception as e:
            return self.exception_handler(e, self)

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
