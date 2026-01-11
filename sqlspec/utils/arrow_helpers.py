"""Arrow conversion helpers for dict-to-Arrow transformations.

This module provides utilities for converting Python dictionaries to Apache Arrow
format, handling empty results, NULL values, and type inference.
"""

from typing import TYPE_CHECKING, Any, Literal, overload

from sqlspec.utils.arrow_impl import convert_dict_to_arrow as _convert_dict_to_arrow

if TYPE_CHECKING:
    from sqlspec.typing import ArrowRecordBatch, ArrowRecordBatchReader, ArrowTable

__all__ = ("convert_dict_to_arrow",)


@overload
def convert_dict_to_arrow(
    data: "list[dict[str, Any]]", return_format: Literal["table"] = "table", batch_size: int | None = None
) -> "ArrowTable": ...


@overload
def convert_dict_to_arrow(
    data: "list[dict[str, Any]]", return_format: Literal["reader"], batch_size: int | None = None
) -> "ArrowRecordBatchReader": ...


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
) -> "ArrowTable | ArrowRecordBatch | ArrowRecordBatchReader | list[ArrowRecordBatch]":
    """Convert list of dictionaries to Arrow Table or RecordBatch.

    Handles empty results, NULL values, and automatic type inference.
    Used by adapters that don't have native Arrow support to convert
    dict-based results to Arrow format.

    Args:
        data: List of dictionaries (one per row).
        return_format: Output format - "table" for Table, "batch"/"batches" for RecordBatch.
            "reader" returns a RecordBatchReader.
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

    return _convert_dict_to_arrow(data, return_format=return_format, batch_size=batch_size)
