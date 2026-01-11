"""Pure helper functions for storage operations.

These functions are extracted from StorageDriverMixin to eliminate
cross-trait attribute access that causes mypyc segmentation faults.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.storage import StorageBridgeJob, StorageTelemetry, create_storage_bridge_job
from sqlspec.utils.arrow_helpers import arrow_table_to_rows as _arrow_table_to_rows_impl
from sqlspec.utils.arrow_helpers import build_ingest_telemetry as _build_ingest_telemetry_impl
from sqlspec.utils.arrow_helpers import coerce_arrow_table as _coerce_arrow_table_impl

if TYPE_CHECKING:
    from sqlspec.core.result import ArrowResult
    from sqlspec.storage import StorageDestination
    from sqlspec.typing import ArrowTable


__all__ = (
    "CAPABILITY_HINTS",
    "arrow_table_to_rows",
    "attach_partition_telemetry",
    "build_ingest_telemetry",
    "coerce_arrow_table",
    "create_storage_job",
    "stringify_storage_target",
)


CAPABILITY_HINTS: Final[dict[str, str]] = {
    "arrow_export_enabled": "native Arrow export",
    "arrow_import_enabled": "native Arrow import",
    "parquet_export_enabled": "native Parquet export",
    "parquet_import_enabled": "native Parquet import",
}


def stringify_storage_target(target: "StorageDestination | None") -> str | None:
    """Convert storage target to string representation.

    Args:
        target: Storage destination path or None.

    Returns:
        String representation of the path or None.

    """
    if target is None:
        return None
    if isinstance(target, Path):
        return target.as_posix()
    return str(target)


def coerce_arrow_table(source: "ArrowResult | Any") -> "ArrowTable":
    """Coerce various sources to a PyArrow Table.

    Args:
        source: ArrowResult, PyArrow Table, RecordBatch, or iterable of dicts.

    Returns:
        PyArrow Table.

    Raises:
        TypeError: If source type is not supported.

    """
    return _coerce_arrow_table_impl(source)


def arrow_table_to_rows(
    table: "ArrowTable", columns: "list[str] | None" = None
) -> "tuple[list[str], list[tuple[Any, ...]]]":
    """Convert Arrow table to column names and row tuples.

    Args:
        table: Arrow table to convert.
        columns: Optional list of columns to extract. Defaults to all columns.

    Returns:
        Tuple of (column_names, list of row tuples).

    Raises:
        ValueError: If table has no columns to import.

    """
    return _arrow_table_to_rows_impl(table, columns)


def build_ingest_telemetry(table: "ArrowTable", *, format_label: str = "arrow") -> "StorageTelemetry":
    """Build telemetry dict from Arrow table statistics.

    Args:
        table: Arrow table to extract statistics from.
        format_label: Format label for telemetry.

    Returns:
        StorageTelemetry dict with row/byte counts.

    """
    telemetry = _build_ingest_telemetry_impl(table, format_label=format_label)
    return cast("StorageTelemetry", telemetry)


def attach_partition_telemetry(telemetry: "StorageTelemetry", partitioner: "dict[str, object] | None") -> None:
    """Attach partitioner info to telemetry dict (mutates in place).

    Args:
        telemetry: Telemetry dict to update.
        partitioner: Partitioner configuration or None.

    """
    if not partitioner:
        return
    extra = dict(telemetry.get("extra", {}))
    extra["partitioner"] = partitioner
    telemetry["extra"] = extra


def create_storage_job(
    produced: "StorageTelemetry", provided: "StorageTelemetry | None" = None, *, status: str = "completed"
) -> "StorageBridgeJob":
    """Create a StorageBridgeJob from telemetry data.

    Args:
        produced: Telemetry from the production side of the operation.
        provided: Optional telemetry from the source side.
        status: Job status string.

    Returns:
        StorageBridgeJob instance.

    """
    merged = cast("StorageTelemetry", dict(produced))
    if provided:
        source_bytes = provided.get("bytes_processed")
        if source_bytes is not None:
            merged["bytes_processed"] = int(merged.get("bytes_processed", 0)) + int(source_bytes)
        extra = dict(merged.get("extra", {}))
        extra["source"] = provided
        merged["extra"] = extra
    return create_storage_bridge_job(status, merged)
