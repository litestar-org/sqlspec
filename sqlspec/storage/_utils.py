"""Shared optional-dependency utilities for storage backends."""

from typing import Any

from sqlspec.storage._paths import FILE_PROTOCOL, FILE_SCHEME_PREFIX, resolve_storage_path
from sqlspec.utils.module_loader import ensure_pyarrow

__all__ = (
    "FILE_PROTOCOL",
    "FILE_SCHEME_PREFIX",
    "import_pyarrow",
    "import_pyarrow_csv",
    "import_pyarrow_parquet",
    "resolve_storage_path",
)


def import_pyarrow() -> "Any":
    """Import PyArrow with optional dependency guard.

    Returns:
        PyArrow module.
    """

    ensure_pyarrow()
    import pyarrow as pa

    return pa


def import_pyarrow_parquet() -> "Any":
    """Import PyArrow parquet module with optional dependency guard.

    Returns:
        PyArrow parquet module.
    """

    ensure_pyarrow()
    import pyarrow.parquet as pq

    return pq


def import_pyarrow_csv() -> "Any":
    """Import PyArrow CSV module with optional dependency guard.

    Returns:
        PyArrow CSV module.
    """

    ensure_pyarrow()
    import pyarrow.csv as pa_csv

    return pa_csv
