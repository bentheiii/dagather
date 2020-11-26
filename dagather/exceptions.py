try:
    from graphlib import CycleError
except ImportError:
    class CycleError(ValueError):
        pass
