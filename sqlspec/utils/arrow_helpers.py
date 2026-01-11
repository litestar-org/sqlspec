"""Arrow conversion helpers for dict-to-Arrow transformations.

This module provides utilities for converting Python dictionaries to Apache Arrow
format, handling empty results, NULL values, and type inference.

NOTE: This module is excluded from mypyc compilation to avoid segmentation faults
when compiled drivers touch Arrow objects.
"""

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal, overload

from sqlspec.utils.module_loader import ensure_pandas, ensure_polars, ensure_pyarrow
from sqlspec.utils.type_guards import has_arrow_table_stats, has_get_data

if TYPE_CHECKING:
    from sqlspec.core.result import ArrowResult
    from sqlspec.typing import ArrowRecordBatch, ArrowRecordBatchReader, ArrowTable, PandasDataFrame, PolarsDataFrame

__all__ = (
    "arrow_table_column_names",
    "arrow_table_num_columns",
    "arrow_table_num_rows",
    "arrow_table_to_pandas",
    "arrow_table_to_polars",
    "arrow_table_to_pylist",
    "arrow_table_to_return_format",
    "arrow_table_to_rows",
    "build_ingest_telemetry",
    "cast_arrow_table_schema",
    "coerce_arrow_table",
    "convert_dict_to_arrow",
    "convert_dict_to_arrow_with_schema",
    "ensure_arrow_table",
)


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
    ensure_pyarrow()
    import pyarrow as pa

    if not data:
        empty_schema = pa.schema([])
        empty_table = pa.Table.from_pydict({}, schema=empty_schema)

        if return_format == "reader":
            return pa.RecordBatchReader.from_batches(empty_table.schema, empty_table.to_batches())

        if return_format in {"batch", "batches"}:
            batches = empty_table.to_batches(max_chunksize=batch_size)
            return batches[0] if batches else pa.RecordBatch.from_pydict({})

        return empty_table

    columns: dict[str, list[Any]] = {key: [row.get(key) for row in data] for key in data[0]}
    arrow_table = pa.Table.from_pydict(columns)

    if return_format == "reader":
        batches = arrow_table.to_batches(max_chunksize=batch_size)
        return pa.RecordBatchReader.from_batches(arrow_table.schema, batches)

    if return_format == "batches":
        return arrow_table.to_batches(max_chunksize=batch_size)

    if return_format == "batch":
        batches = arrow_table.to_batches(max_chunksize=batch_size)
        return batches[0] if batches else pa.RecordBatch.from_pydict({})

    return arrow_table


def convert_dict_to_arrow_with_schema(
    data: "list[dict[str, Any]]",
    return_format: Literal["table", "reader", "batch", "batches"] = "table",
    batch_size: int | None = None,
    arrow_schema: Any = None,
) -> "ArrowTable | ArrowRecordBatch | ArrowRecordBatchReader | list[ArrowRecordBatch]":
    """Convert dict rows to Arrow and optionally cast to a schema."""
    table = convert_dict_to_arrow(data, return_format="table", batch_size=batch_size)
    if arrow_schema is not None:
        ensure_pyarrow()
        import pyarrow as pa

        if not isinstance(arrow_schema, pa.Schema):
            msg = f"arrow_schema must be a pyarrow.Schema, got {type(arrow_schema).__name__}"
            raise TypeError(msg)
        if not data:
            table = pa.Table.from_pydict({name: [] for name in arrow_schema.names}, schema=arrow_schema)
        else:
            table = table.cast(arrow_schema)

    if return_format == "table":
        return table

    ensure_pyarrow()
    import pyarrow as pa

    if return_format == "reader":
        batches = table.to_batches(max_chunksize=batch_size)
        return pa.RecordBatchReader.from_batches(table.schema, batches)
    if return_format == "batches":
        return table.to_batches(max_chunksize=batch_size)
    batches = table.to_batches(max_chunksize=batch_size)
    return batches[0] if batches else pa.RecordBatch.from_pydict({})


def coerce_arrow_table(source: "ArrowResult | Any") -> "ArrowTable":
    """Coerce various sources to a PyArrow Table."""
    ensure_pyarrow()
    import pyarrow as pa

    if has_get_data(source):
        table = source.get_data()
        if isinstance(table, pa.Table):
            return table
        msg = "ArrowResult did not return a pyarrow.Table instance"
        raise TypeError(msg)
    if isinstance(source, pa.Table):
        return source
    if isinstance(source, pa.RecordBatch):
        return pa.Table.from_batches([source])
    if isinstance(source, Iterable):
        return pa.Table.from_pylist(list(source))
    msg = f"Unsupported Arrow source type: {type(source).__name__}"
    raise TypeError(msg)


def ensure_arrow_table(data: Any) -> "ArrowTable":
    """Ensure data is a PyArrow Table."""
    ensure_pyarrow()
    import pyarrow as pa

    if isinstance(data, pa.Table):
        return data
    msg = f"Expected an Arrow Table, but got {type(data).__name__}"
    raise TypeError(msg)


def cast_arrow_table_schema(table: "ArrowTable", arrow_schema: Any) -> "ArrowTable":
    """Cast an Arrow table to a provided schema.

    Args:
        table: Arrow table to cast.
        arrow_schema: Optional pyarrow.Schema for casting.

    Returns:
        Arrow table with updated schema.

    Raises:
        TypeError: If arrow_schema is not a pyarrow.Schema instance.
    """
    if arrow_schema is None:
        return table

    ensure_pyarrow()
    import pyarrow as pa

    if not isinstance(arrow_schema, pa.Schema):
        msg = f"arrow_schema must be a pyarrow.Schema, got {type(arrow_schema).__name__}"
        raise TypeError(msg)
    return table.cast(arrow_schema)


def arrow_table_to_return_format(
    table: "ArrowTable",
    return_format: Literal["table", "reader", "batch", "batches"] = "table",
    batch_size: int | None = None,
) -> "ArrowTable | ArrowRecordBatch | ArrowRecordBatchReader | list[ArrowRecordBatch]":
    """Convert an Arrow table into the requested return format.

    Args:
        table: Arrow table to convert.
        return_format: Output format (table, reader, batch, batches).
        batch_size: Batch size for reader/batch outputs.

    Returns:
        Converted Arrow data in the requested format.
    """
    ensure_pyarrow()
    import pyarrow as pa

    if return_format == "table":
        return table

    batches = table.to_batches(max_chunksize=batch_size)
    if return_format == "reader":
        return pa.RecordBatchReader.from_batches(table.schema, batches)
    if return_format == "batches":
        return batches
    return batches[0] if batches else pa.RecordBatch.from_pydict({})


def arrow_table_to_rows(
    table: "ArrowTable", columns: "list[str] | None" = None
) -> "tuple[list[str], list[tuple[Any, ...]]]":
    """Convert Arrow table to column names and row tuples."""
    ensure_pyarrow()
    resolved_columns = columns or list(table.column_names)
    if not resolved_columns:
        msg = "Arrow table has no columns to import"
        raise ValueError(msg)
    batches = table.to_pylist()
    records: list[tuple[Any, ...]] = []
    for row in batches:
        record = tuple(row.get(col) for col in resolved_columns)
        records.append(record)
    return resolved_columns, records


def arrow_table_to_pylist(table: "ArrowTable") -> "list[dict[str, Any]]":
    """Convert Arrow table to list of dictionaries."""
    return table.to_pylist()


def arrow_table_column_names(table: "ArrowTable") -> "list[str]":
    """Return Arrow table column names."""
    return list(table.column_names)


def arrow_table_num_rows(table: "ArrowTable") -> int:
    """Return Arrow table row count."""
    return int(table.num_rows)


def arrow_table_num_columns(table: "ArrowTable") -> int:
    """Return Arrow table column count."""
    return int(table.num_columns)


def arrow_table_to_pandas(table: "ArrowTable") -> "PandasDataFrame":
    """Convert Arrow table to pandas DataFrame."""
    ensure_pandas()
    import pandas as pd

    result = table.to_pandas()
    if not isinstance(result, pd.DataFrame):
        msg = f"Expected a pandas DataFrame, but got {type(result).__name__}"
        raise TypeError(msg)
    return result


def arrow_table_to_polars(table: "ArrowTable") -> "PolarsDataFrame":
    """Convert Arrow table to Polars DataFrame."""
    ensure_polars()
    import polars as pl

    result = pl.from_arrow(table)
    if not isinstance(result, pl.DataFrame):
        msg = f"Expected a Polars DataFrame, but got {type(result).__name__}"
        raise TypeError(msg)
    return result


def build_ingest_telemetry(table: "ArrowTable", *, format_label: str = "arrow") -> "dict[str, int | str]":
    """Build telemetry dict from Arrow table statistics."""
    if has_arrow_table_stats(table):
        rows = int(table.num_rows)
        bytes_processed = int(table.nbytes)
    else:
        rows = 0
        bytes_processed = 0
    return {"rows_processed": rows, "bytes_processed": bytes_processed, "format": format_label}
