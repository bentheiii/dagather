# Dagather
dagather (**d**irected **a**cyclic **gather**) is a new way to plan out and schedule asynchronous tasks. The tasks are organized, with each task specifying the tasks that come before it. The tasks are then run in topological order, ensuring that each operation will start as soon as it is able to without waiting for routines it does not need.

```python
from asyncio import sleep
from dagather import Dagather

foo = Dagather()

@foo.register
# add a new task to the task list
async def a():
    await sleep(1)
    return 12

@foo.register
async def b(a):
    # we now specify that a is a requirement for this task,
    # meaning that b will not be called until a has finished.
    # during runtime, a's value will be the return value of the
    # a task
    assert a == 12
    await sleep(2)

@foo.register
async def c(a):
    await sleep(1)
    return 'testing'

@foo.register
async def d():
    await sleep(1)

@foo.register
async def e(d, c):
    await sleep(1)

result = await foo()
# when foo is called, it runs each of its registered tasks 
# as soon as all its dependencies are finished.
# once all the tasks are finished, it will return a dict
# mapping each task to its return value.
assert result == {
    a: 12,
    b: None,
    c: 'testing',
    d: None,
    e: None
}
```