"""Module loading utilities for SQLSpec.

Provides functions for dynamic module imports, path resolution, and dependency
availability checking. Used for loading modules from dotted paths, converting
module paths to filesystem paths, and ensuring optional dependencies are installed.
"""

import importlib
from importlib.util import find_spec
from pathlib import Path
from typing import Any

from sqlspec.exceptions import MissingDependencyError
from sqlspec.typing import (
    AIOSQL_INSTALLED,
    ATTRS_INSTALLED,
    CATTRS_INSTALLED,
    FSSPEC_INSTALLED,
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
    NUMPY_INSTALLED,
    OBSTORE_INSTALLED,
    OPENTELEMETRY_INSTALLED,
    ORJSON_INSTALLED,
    PANDAS_INSTALLED,
    PGVECTOR_INSTALLED,
    POLARS_INSTALLED,
    PROMETHEUS_INSTALLED,
    PYARROW_INSTALLED,
    PYDANTIC_INSTALLED,
)

__all__ = (
    "ensure_aiosql",
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
    "import_string",
    "module_to_os_path",
)


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

    def _raise_import_error(msg: str, exc: "Exception | None" = None) -> None:
        if exc is not None:
            raise ImportError(msg) from exc
        raise ImportError(msg)

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
            if not hasattr(parent_module, attr):
                _raise_import_error(f"Module '{parent_module_path}' has no attribute '{attr}' in '{dotted_path}'")

        for attr in attrs:
            if not hasattr(obj, attr):
                _raise_import_error(
                    f"Module '{module.__name__ if module is not None else 'unknown'}' has no attribute '{attr}' in '{dotted_path}'"
                )
            obj = getattr(obj, attr)
    except Exception as e:  # pylint: disable=broad-exception-caught
        _raise_import_error(f"Could not import '{dotted_path}': {e}", e)
    return obj


def ensure_aiosql() -> None:
    """Ensure aiosql is available.

    Raises:
        MissingDependencyError: If aiosql is not installed.
    """
    if not AIOSQL_INSTALLED:
        raise MissingDependencyError(package="aiosql", install_package="aiosql")


def ensure_attrs() -> None:
    """Ensure attrs is available.

    Raises:
        MissingDependencyError: If attrs is not installed.
    """
    if not ATTRS_INSTALLED:
        raise MissingDependencyError(package="attrs", install_package="attrs")


def ensure_cattrs() -> None:
    """Ensure cattrs is available.

    Raises:
        MissingDependencyError: If cattrs is not installed.
    """
    if not CATTRS_INSTALLED:
        raise MissingDependencyError(package="cattrs", install_package="cattrs")


def ensure_fsspec() -> None:
    """Ensure fsspec is available for filesystem operations.

    Raises:
        MissingDependencyError: If fsspec is not installed.
    """
    if not FSSPEC_INSTALLED:
        raise MissingDependencyError(package="fsspec", install_package="fsspec")


def ensure_litestar() -> None:
    """Ensure Litestar is available.

    Raises:
        MissingDependencyError: If litestar is not installed.
    """
    if not LITESTAR_INSTALLED:
        raise MissingDependencyError(package="litestar", install_package="litestar")


def ensure_msgspec() -> None:
    """Ensure msgspec is available for serialization.

    Raises:
        MissingDependencyError: If msgspec is not installed.
    """
    if not MSGSPEC_INSTALLED:
        raise MissingDependencyError(package="msgspec", install_package="msgspec")


def ensure_numpy() -> None:
    """Ensure NumPy is available for array operations.

    Raises:
        MissingDependencyError: If numpy is not installed.
    """
    if not NUMPY_INSTALLED:
        raise MissingDependencyError(package="numpy", install_package="numpy")


def ensure_obstore() -> None:
    """Ensure obstore is available for object storage operations.

    Raises:
        MissingDependencyError: If obstore is not installed.
    """
    if not OBSTORE_INSTALLED:
        raise MissingDependencyError(package="obstore", install_package="obstore")


def ensure_opentelemetry() -> None:
    """Ensure OpenTelemetry is available for tracing.

    Raises:
        MissingDependencyError: If opentelemetry-api is not installed.
    """
    if not OPENTELEMETRY_INSTALLED:
        raise MissingDependencyError(package="opentelemetry-api", install_package="opentelemetry")


def ensure_orjson() -> None:
    """Ensure orjson is available for fast JSON operations.

    Raises:
        MissingDependencyError: If orjson is not installed.
    """
    if not ORJSON_INSTALLED:
        raise MissingDependencyError(package="orjson", install_package="orjson")


def ensure_pandas() -> None:
    """Ensure pandas is available for DataFrame operations.

    Raises:
        MissingDependencyError: If pandas is not installed.
    """
    if not PANDAS_INSTALLED:
        raise MissingDependencyError(package="pandas", install_package="pandas")


def ensure_pgvector() -> None:
    """Ensure pgvector is available for vector operations.

    Raises:
        MissingDependencyError: If pgvector is not installed.
    """
    if not PGVECTOR_INSTALLED:
        raise MissingDependencyError(package="pgvector", install_package="pgvector")


def ensure_polars() -> None:
    """Ensure Polars is available for DataFrame operations.

    Raises:
        MissingDependencyError: If polars is not installed.
    """
    if not POLARS_INSTALLED:
        raise MissingDependencyError(package="polars", install_package="polars")


def ensure_prometheus() -> None:
    """Ensure Prometheus client is available for metrics.

    Raises:
        MissingDependencyError: If prometheus-client is not installed.
    """
    if not PROMETHEUS_INSTALLED:
        raise MissingDependencyError(package="prometheus-client", install_package="prometheus")


def ensure_pyarrow() -> None:
    """Ensure PyArrow is available for Arrow operations.

    Raises:
        MissingDependencyError: If pyarrow is not installed.
    """
    if not PYARROW_INSTALLED:
        raise MissingDependencyError(package="pyarrow", install_package="pyarrow")


def ensure_pydantic() -> None:
    """Ensure Pydantic is available for data validation.

    Raises:
        MissingDependencyError: If pydantic is not installed.
    """
    if not PYDANTIC_INSTALLED:
        raise MissingDependencyError(package="pydantic", install_package="pydantic")
