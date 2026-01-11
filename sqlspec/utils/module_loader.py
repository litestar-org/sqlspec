"""Module loading utilities for SQLSpec.

Provides functions for dynamic module imports, path resolution, and dependency
availability checking. Used for loading modules from dotted paths, converting
module paths to filesystem paths, and ensuring optional dependencies are installed.
"""

import importlib
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import MissingDependencyError

if TYPE_CHECKING:
    from types import ModuleType

__all__ = (
    "OptionalDependencyFlag",
    "dependency_flag",
    "ensure_attrs",
    "ensure_cattrs",
    "ensure_fsspec",
    "ensure_litestar",
    "ensure_msgspec",
    "ensure_numpy",
    "ensure_obstore",
    "ensure_opentelemetry",
    "ensure_orjson",
    "ensure_pandas",
    "ensure_pgvector",
    "ensure_polars",
    "ensure_prometheus",
    "ensure_pyarrow",
    "ensure_pydantic",
    "ensure_uvloop",
    "import_string",
    "module_available",
    "module_to_os_path",
    "reset_dependency_cache",
)


# =============================================================================
# Dependency Availability Checking
# =============================================================================

_dependency_cache: "dict[str, bool]" = {}


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


# =============================================================================
# Module Loading and Import Utilities
# =============================================================================


def _require_dependency(
    module_name: str, *, package_name: str | None = None, install_package: str | None = None
) -> None:
    """Raise MissingDependencyError when an optional dependency is absent."""

    if module_available(module_name):
        return

    package = package_name or module_name
    install = install_package or package
    raise MissingDependencyError(package=package, install_package=install)


def _raise_import_error(msg: str, exc: "Exception | None" = None) -> None:
    """Raise an ImportError with optional exception chaining."""
    if exc is not None:
        raise ImportError(msg) from exc
    raise ImportError(msg)


def _resolve_import_attr(obj: Any, attr: str, module: "ModuleType | None", dotted_path: str) -> Any:
    """Resolve a dotted attribute path segment on a module or object."""
    try:
        return obj.__getattribute__(attr)
    except AttributeError as exc:
        module_name = module.__name__ if module is not None else "unknown"
        _raise_import_error(f"Module '{module_name}' has no attribute '{attr}' in '{dotted_path}'", exc)
        raise


def module_to_os_path(dotted_path: str = "app") -> "Path":
    """Convert a module dotted path to filesystem path.

    Args:
        dotted_path: The path to the module.

    Raises:
        TypeError: The module could not be found.

    Returns:
        The path to the module.
    """
    try:
        if (src := find_spec(dotted_path)) is None:  # pragma: no cover
            msg = f"Couldn't find the path for {dotted_path}"
            raise TypeError(msg)
    except ModuleNotFoundError as e:
        msg = f"Couldn't find the path for {dotted_path}"
        raise TypeError(msg) from e

    path = Path(str(src.origin))
    return path.parent if path.is_file() else path


def import_string(dotted_path: str) -> "Any":
    """Import a module or attribute from a dotted path string.

    Args:
        dotted_path: The path of the module to import.

    Returns:
        The imported object.
    """

    obj: Any = None
    try:
        parts = dotted_path.split(".")
        module = None
        i = len(parts)

        for i in range(len(parts), 0, -1):
            module_path = ".".join(parts[:i])
            try:
                module = importlib.import_module(module_path)
                break
            except ModuleNotFoundError:
                continue
        else:
            _raise_import_error(f"{dotted_path} doesn't look like a module path")

        if module is None:
            _raise_import_error(f"Failed to import any module from {dotted_path}")

        obj = module
        attrs = parts[i:]
        if not attrs and i == len(parts) and len(parts) > 1:
            parent_module_path = ".".join(parts[:-1])
            attr = parts[-1]
            try:
                parent_module = importlib.import_module(parent_module_path)
            except Exception:
                return obj
            if attr not in parent_module.__dict__:
                _raise_import_error(f"Module '{parent_module_path}' has no attribute '{attr}' in '{dotted_path}'")

        for attr in attrs:
            obj = _resolve_import_attr(obj, attr, module, dotted_path)
    except Exception as e:  # pylint: disable=broad-exception-caught
        _raise_import_error(f"Could not import '{dotted_path}': {e}", e)
    return obj


def ensure_attrs() -> None:
    """Ensure attrs is available."""
    _require_dependency("attrs")


def ensure_cattrs() -> None:
    """Ensure cattrs is available."""
    _require_dependency("cattrs")


def ensure_fsspec() -> None:
    """Ensure fsspec is available for filesystem operations."""
    _require_dependency("fsspec")


def ensure_litestar() -> None:
    """Ensure Litestar is available."""
    _require_dependency("litestar")


def ensure_msgspec() -> None:
    """Ensure msgspec is available for serialization."""
    _require_dependency("msgspec")


def ensure_numpy() -> None:
    """Ensure NumPy is available for array operations."""
    _require_dependency("numpy")


def ensure_obstore() -> None:
    """Ensure obstore is available for object storage operations."""
    _require_dependency("obstore")


def ensure_opentelemetry() -> None:
    """Ensure OpenTelemetry is available for tracing."""
    _require_dependency("opentelemetry", package_name="opentelemetry-api", install_package="opentelemetry")


def ensure_orjson() -> None:
    """Ensure orjson is available for fast JSON operations."""
    _require_dependency("orjson")


def ensure_pandas() -> None:
    """Ensure pandas is available for DataFrame operations."""
    _require_dependency("pandas")


def ensure_pgvector() -> None:
    """Ensure pgvector is available for vector operations."""
    _require_dependency("pgvector")


def ensure_polars() -> None:
    """Ensure Polars is available for DataFrame operations."""
    _require_dependency("polars")


def ensure_prometheus() -> None:
    """Ensure Prometheus client is available for metrics."""
    _require_dependency("prometheus_client", package_name="prometheus-client", install_package="prometheus")


def ensure_pyarrow() -> None:
    """Ensure PyArrow is available for Arrow operations."""
    _require_dependency("pyarrow")


def ensure_pydantic() -> None:
    """Ensure Pydantic is available for data validation."""
    _require_dependency("pydantic")


def ensure_uvloop() -> None:
    """Ensure uvloop is available for fast event loops."""
    _require_dependency("uvloop")
