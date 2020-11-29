__all__ = ['CycleError']


class CycleError(ValueError):
    """
    Exception raised if a dependency cycle is detected
    """
    pass
