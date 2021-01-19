__all__ = ['CycleError', 'DiscardedTask']


class CycleError(ValueError):
    """
    Exception raised if a dependency cycle is detected
    """


class DiscardedTask(Exception):
    """
    Exception raised if a attempt is made to access a discarded task template
    """
