"""Module resolution helpers for tests."""

import functools
from importlib.util import find_spec

__all__ = ("module_is_available",)


@functools.cache
def module_is_available(module: str) -> bool:
    """Return True when a module specification can be resolved without importing it."""
    try:
        return find_spec(module) is not None
    except ModuleNotFoundError:
        return False
