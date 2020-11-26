from asyncio import sleep

from pytest import mark, raises

from dagather import Dagather, ExceptionPolicy
from dagather.exceptions import CycleError

atest = mark.asyncio


@atest
async def test_basic():
    dag = Dagather()

    @dag.register
    async def a(t, x):
        return t + x

    assert await dag(10, x=2) == {a: 12}


@atest
async def test_seq():
    dag = Dagather()

    execution_order = []

    @dag.register
    async def b(a, x):
        execution_order.append('b')
        return a + 1

    @dag.register
    async def a(x):
        execution_order.append('a')
        return x

    @dag.register
    async def c(b, x):
        execution_order.append('c')
        return b + x

    assert await dag(x=2) == {a: 2, b: 3, c: 5}
    assert execution_order == ['a', 'b', 'c']


@atest
async def test_layers():
    dag = Dagather()

    """
    a->b->c
    d->e->c,g
    f->c
    """

    execution_order = []

    @dag.register
    async def a():
        await sleep(0.05)
        execution_order.append('1')

    @dag.register
    async def b(a):
        await sleep(0.05)
        execution_order.append('2')

    @dag.register
    async def c(b, d, e, f):
        await sleep(0.02)
        execution_order.append('3')

    @dag.register
    async def d():
        await sleep(0.06)
        execution_order.append('1')

    @dag.register
    async def e(d):
        await sleep(0.05)
        execution_order.append('2')

    @dag.register
    async def f():
        await sleep(0.07)
        execution_order.append('1')

    @dag.register
    async def g(e):
        await sleep(0.05)
        execution_order.append('3')

    await dag()
    assert execution_order == list('1112233')


@atest
async def test_cyclic():
    dag = Dagather()

    @dag.register
    async def a(b):
        pass

    @dag.register
    async def b(a):
        pass

    with raises(CycleError):
        await dag()


@atest
async def test_missing_args():
    dag = Dagather()

    @dag.register
    async def a(x, y=1):
        return x + y

    with raises(TypeError):
        await dag(x=1, y=1, z=1)

    assert await dag(x=1) == {a: 2}


@atest
async def test_nameof_subtask():
    dag = Dagather()

    @dag.register
    async def a():
        pass

    @dag.register
    async def b(a):
        pass

    with raises(TypeError):
        await dag(a=1)


@atest
async def test_args():
    dag = Dagather()

    @dag.register
    async def a(x, *y, z):
        return x + sum(y) * z

    assert await dag(1, 2, z=3) == {a: 7}


@atest
async def test_call_sub():
    dag = Dagather()

    @dag.register
    async def a(x, *y, z):
        return x + sum(y) * z

    assert await a(1, 2, z=3) == 7


@atest
async def test_same_name():
    dag = Dagather()

    @dag.register
    async def a():
        pass

    with raises(ValueError):
        @dag.register
        async def a():
            pass

    assert await dag() == {a: None}


@atest
async def test_cyclic_single():
    dag = Dagather()

    @dag.register
    async def a(a):
        pass

    with raises(CycleError):
        await dag()


@atest
async def test_error():
    dag = Dagather()

    @dag.register
    async def a():
        raise ValueError('foobar')

    with raises(ValueError, match='foobar'):
        await dag()


@atest
async def test_error_no_rollback():
    dag = Dagather()

    ex = []

    @dag.register
    async def a():
        ex.append('a')

    @dag.register
    async def b(a):
        raise ValueError('foobar')

    with raises(ValueError, match='foobar'):
        await dag()

    assert ex == ['a']


def make_error_prone(return_exceptions, exception_policy):
    dag = Dagather()

    ex = []
    ex2 = []

    @dag.register
    async def a():
        ex.append('a')

    @dag.register(return_exceptions=return_exceptions, exception_policy=exception_policy)
    async def b(a):
        ex2.append('b0')
        raise ValueError('foobar')
        ex2.append('b1')

    @dag.register
    async def c(a):
        ex.append('c0')
        await sleep(0.1)
        ex.append('c1')

    @dag.register
    async def d(c):
        ex.append('d')

    @dag.register
    async def e(b):
        ex2.append('e')

    return dag, ex, ex2, (a, b, c, d, e)


@atest
async def test_error_cancels():
    dag, ex1, ex2, _ = make_error_prone(False, ExceptionPolicy.cancel_not_started)

    with raises(ValueError, match='foobar'):
        await dag()

    assert ex1 == ['a', 'c0', 'c1']
    assert ex2 == ['b0']


@atest
async def test_return_errors():
    dag, ex1, ex2, (a, b, c, d, e) = make_error_prone(True, ExceptionPolicy.cancel_not_started)

    result = await dag()
    assert result == {a: None, b: result[b], c: None}

    assert isinstance(result[b], ValueError) and result[b].args == ('foobar',)
    assert ex1 == ['a', 'c0', 'c1']
    assert ex2 == ['b0']


@atest
async def test_return_errors_continue():
    dag, ex1, ex2, (a, b, c, d, e) = make_error_prone(return_exceptions=True,
                                                      exception_policy=ExceptionPolicy.continue_all)

    result = await dag()
    assert result == {a: None, b: result[b], c: None, d: None, e: None}

    assert isinstance(result[b], ValueError) and result[b].args == ('foobar',)
    assert ex1 == ['a', 'c0', 'c1', 'd']
    assert ex2 == ['b0', 'e']


@atest
async def test_return_errors_cancel_branch():
    dag, ex1, ex2, (a, b, c, d, e) = make_error_prone(return_exceptions=True,
                                                      exception_policy=ExceptionPolicy.cancel_children)

    result = await dag()
    assert result == {a: None, b: result[b], c: None, d: None}

    assert isinstance(result[b], ValueError) and result[b].args == ('foobar',)
    assert ex1 == ['a', 'c0', 'c1', 'd']
    assert ex2 == ['b0']


@atest
async def test_raise_continue():
    dag, ex1, ex2, _ = make_error_prone(return_exceptions=False,
                                        exception_policy=ExceptionPolicy.continue_all)

    with raises(ValueError, match='foobar'):
        await dag()

    assert ex1 == ['a', 'c0', 'c1', 'd']
    assert ex2 == ['b0', 'e']


@atest
async def test_raise_cancel_branch():
    dag, ex1, ex2, _ = make_error_prone(return_exceptions=False,
                                        exception_policy=ExceptionPolicy.cancel_children)

    with raises(ValueError, match='foobar'):
        await dag()

    assert ex1 == ['a', 'c0', 'c1', 'd']
    assert ex2 == ['b0']
