"""Shared helpers for bounded Parquet batch streaming."""

from pathlib import PurePath
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlspec.typing import ArrowRecordBatch

__all__ = ("iter_parquet_row_groups", "validate_parquet_stream_options")

_NON_PARQUET_SUFFIXES = frozenset({".arrow", ".csv", ".feather", ".ipc", ".json", ".jsonl", ".ndjson"})


def validate_parquet_stream_options(pattern: str, file_format: str, batch_size: int) -> None:
    """Validate a Parquet streaming request before storage is accessed."""
    if file_format != "parquet":
        msg = f"Arrow batch streaming supports only Parquet files; received file_format={file_format!r}"
        raise ValueError(msg)
    if batch_size <= 0:
        msg = f"batch_size must be greater than zero; received {batch_size}"
        raise ValueError(msg)

    suffix = PurePath(pattern).suffix.lower()
    if suffix in _NON_PARQUET_SUFFIXES:
        msg = f"Arrow batch streaming supports only Parquet files; pattern {pattern!r} selects {suffix} files"
        raise ValueError(msg)


def iter_parquet_row_groups(parquet_file: Any, *, batch_size: int, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
    """Yield batches while limiting each PyArrow read to one row group."""
    for row_group in range(parquet_file.num_row_groups):
        yield from parquet_file.iter_batches(batch_size=batch_size, row_groups=[row_group], **kwargs)
