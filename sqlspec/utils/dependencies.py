"""Runtime optional dependency detection utilities."""

from importlib.util import find_spec

_dependency_cache: dict[str, bool] = {}


def module_available(module_name: str) -> bool:
    """Return True if the given module can be resolved.

    The result is cached per interpreter session. Call
    :func:`reset_dependency_cache` to invalidate cached entries when
    tests manipulate ``sys.path``.

    Args:
        module_name: Dotted module path to check.

    Returns:
        True if :mod:`importlib` can find the module, False otherwise.
    """

    cached = _dependency_cache.get(module_name)
    if cached is not None:
        return cached

    try:
        is_available = find_spec(module_name) is not None
    except ModuleNotFoundError:
        is_available = False

    _dependency_cache[module_name] = is_available
    return is_available


def reset_dependency_cache(module_name: str | None = None) -> None:
    """Clear cached availability for one module or the entire cache.

    Args:
        module_name: Specific dotted module path to drop from the cache.
            Clears the full cache when ``None``.
    """

    if module_name is None:
        _dependency_cache.clear()
        return

    _dependency_cache.pop(module_name, None)


class OptionalDependencyFlag:
    """Boolean-like wrapper that evaluates module availability lazily."""

    __slots__ = ("module_name",)

    def __init__(self, module_name: str) -> None:
        self.module_name = module_name

    def __bool__(self) -> bool:
        return module_available(self.module_name)

    def __repr__(self) -> str:
        status = "available" if module_available(self.module_name) else "missing"
        return f"OptionalDependencyFlag(module='{self.module_name}', status='{status}')"


def dependency_flag(module_name: str) -> "OptionalDependencyFlag":
    """Return a lazily evaluated flag for the supplied module name.

    Args:
        module_name: Dotted module path to guard.

    Returns:
        :class:`OptionalDependencyFlag` tracking the module.
    """

    return OptionalDependencyFlag(module_name)


__all__ = ("OptionalDependencyFlag", "dependency_flag", "module_available", "reset_dependency_cache")
