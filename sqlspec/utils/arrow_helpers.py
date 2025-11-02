"""Arrow conversion helpers for dict-to-Arrow transformations.

This module provides utilities for converting Python dictionaries to Apache Arrow
format, handling empty results, NULL values, and type inference.
"""

from typing import TYPE_CHECKING, Any, Literal, overload

from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("convert_dict_to_arrow",)


@overload
def convert_dict_to_arrow(
    data: "list[dict[str, Any]]", return_format: Literal["table", "reader"] = "table", batch_size: int | None = None
) -> "ArrowTable": ...


@overload
def convert_dict_to_arrow(
    data: "list[dict[str, Any]]", return_format: Literal["batch"], batch_size: int | None = None
) -> "ArrowRecordBatch": ...


@overload
def convert_dict_to_arrow(
    data: "list[dict[str, Any]]", return_format: Literal["batches"], batch_size: int | None = None
) -> "list[ArrowRecordBatch]": ...


def convert_dict_to_arrow(
    data: "list[dict[str, Any]]",
    return_format: Literal["table", "reader", "batch", "batches"] = "table",
    batch_size: int | None = None,
) -> "ArrowTable | ArrowRecordBatch | list[ArrowRecordBatch]":
    """Convert list of dictionaries to Arrow Table or RecordBatch.

    Handles empty results, NULL values, and automatic type inference.
    Used by adapters that don't have native Arrow support to convert
    dict-based results to Arrow format.

    Args:
        data: List of dictionaries (one per row).
        return_format: Output format - "table" for Table, "batch"/"batches" for RecordBatch.
            "reader" is converted to "table" (streaming handled at driver level).
        batch_size: Chunk size for batching (used when return_format="batch"/"batches").

    Returns:
        ArrowTable or ArrowRecordBatch depending on return_format.


    Examples:
        >>> data = [
        ...     {"id": 1, "name": "Alice"},
        ...     {"id": 2, "name": "Bob"},
        ... ]
        >>> table = convert_dict_to_arrow(data, return_format="table")
        >>> print(table.num_rows)
        2

        >>> batch = convert_dict_to_arrow(data, return_format="batch")
        >>> print(batch.num_rows)
        2
    """

    ensure_pyarrow()

    import pyarrow as pa

    if not data:
        empty_schema = pa.schema([])
        empty_table = pa.Table.from_pydict({}, schema=empty_schema)

        if return_format in {"batch", "batches"}:
            batches = empty_table.to_batches()
            return batches[0] if batches else pa.RecordBatch.from_pydict({})

        return empty_table

    columns: dict[str, list[Any]] = {key: [row.get(key) for row in data] for key in data[0]}

    arrow_table = pa.Table.from_pydict(columns)

    if return_format == "batches":
        return arrow_table.to_batches(max_chunksize=batch_size)

    if return_format == "batch":
        batches = arrow_table.to_batches()
        return batches[0] if batches else pa.RecordBatch.from_pydict({})

    return arrow_table
