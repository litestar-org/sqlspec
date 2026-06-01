"""Shared optional-dependency utilities for storage backends."""

import logging
from typing import Any

from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.module_loader import ensure_pyarrow

__all__ = ("_log_storage_event", "import_pyarrow", "import_pyarrow_csv", "import_pyarrow_parquet")

logger = get_logger(__name__)


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


def _log_storage_event(
    event: str,
    *,
    backend_type: str,
    protocol: str,
    operation: str | None = None,
    mode: str | None = None,
    path: str | None = None,
    source_path: str | None = None,
    destination_path: str | None = None,
    count: int | None = None,
    exists: bool | None = None,
) -> None:
    fields: dict[str, Any] = {"backend_type": backend_type, "protocol": protocol}
    if operation is not None:
        fields["operation"] = operation
    if mode is not None:
        fields["mode"] = mode
    if path is not None:
        fields["path"] = path
    if source_path is not None:
        fields["source_path"] = source_path
    if destination_path is not None:
        fields["destination_path"] = destination_path
    if count is not None:
        fields["count"] = count
    if exists is not None:
        fields["exists"] = exists
    log_with_context(logger, logging.DEBUG, event, **fields)
