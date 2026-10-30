"""Arrow conversion helpers for dict-to-Arrow transformations.

This module provides utilities for converting Python dictionaries to Apache Arrow
format, handling empty results, NULL values, and type inference.

NOTE: This module is excluded from mypyc compilation to avoid segmentation faults
when compiled drivers touch Arrow objects.
"""

import contextlib
import struct
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Final, Literal, cast, overload

from sqlspec.data_dictionary._registry import get_dialect_config
from sqlspec.exceptions import ImproperConfigurationError, SQLBuilderError
from sqlspec.utils.dispatch import TypeDispatcher
from sqlspec.utils.module_loader import ensure_pandas, ensure_polars, ensure_pyarrow
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_arrow_table_stats, has_get_data
from sqlspec.utils.uuids import uuid_from_bytes

if TYPE_CHECKING:
    from sqlspec.core.result import ArrowResult
    from sqlspec.data_dictionary._types import DialectConfig
    from sqlspec.storage import StorageTelemetry
    from sqlspec.typing import ArrowRecordBatch, ArrowRecordBatchReader, ArrowTable, PandasDataFrame, PolarsDataFrame

__all__ = (
    "PG_BINARY_COPY_HEADER",
    "PG_BINARY_COPY_TRAILER",
    "arrow_batch_to_rows",
    "arrow_reader_to_return_format",
    "arrow_reader_with_deferred_close",
    "arrow_table_column_names",
    "arrow_table_needs_parameter_preparation",
    "arrow_table_num_columns",
    "arrow_table_num_rows",
    "arrow_table_to_pandas",
    "arrow_table_to_polars",
    "arrow_table_to_pylist",
    "arrow_table_to_return_format",
    "arrow_table_to_rows",
    "arrow_type_from_token",
    "build_ingest_telemetry",
    "cast_arrow_table_schema",
    "coerce_arrow_batches",
    "coerce_arrow_table",
    "convert_dict_to_arrow",
    "convert_dict_to_arrow_with_schema",
    "encode_arrow_batch_pg_binary",
    "ensure_arrow_table",
    "iter_pgcopy_binary_chunks",
    "iter_pgcopy_binary_chunks_async",
    "records_to_arrow_table",
    "sql_type_for_category",
    "sql_type_from_arrow_type",
)
PG_BINARY_COPY_HEADER: Final[bytes] = b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00"
PG_BINARY_COPY_TRAILER: Final[bytes] = b"\xff\xff"
_PG_DATE_EPOCH_DAYS: Final[int] = 10957
_PG_EPOCH_OFFSET_US: Final[int] = 946_684_800_000_000
_PG_NULL_MARKER: Final[bytes] = b"\xff\xff\xff\xff"
_ARROW_TABLE_COERCER: "TypeDispatcher[Any] | None" = None
_ARROW_SCHEMA_DECISION_CACHE_SIZE = 512
_CONFIG_CATEGORY_DEFAULTS: "dict[str, str]" = {
    "uuid": "UUID",
    "boolean": "BOOLEAN",
    "blob": "BLOB",
    "json": "JSON",
    "text": "TEXT",
}
_PLAIN_CATEGORY_TYPES: "dict[str, str]" = {
    "smallint": "SMALLINT",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "real": "REAL",
    "date": "DATE",
    "time": "TIME",
    "interval": "INTERVAL",
}
_NAIVE_TIMESTAMP_TYPES: "dict[str, str]" = {"mssql": "DATETIME2(6)", "bigquery": "DATETIME"}
_AWARE_TIMESTAMP_TYPES: "dict[str, str]" = {
    "postgres": "TIMESTAMP WITH TIME ZONE",
    "cockroachdb": "TIMESTAMPTZ",
    "duckdb": "TIMESTAMP WITH TIME ZONE",
    "oracle": "TIMESTAMP WITH TIME ZONE",
    "mssql": "DATETIMEOFFSET(6)",
}
_BOUNDED_KEY_TEXT_DIALECTS: "frozenset[str]" = frozenset({"mysql", "mariadb", "oracle", "mssql", "db2"})
_KEY_TEXT_LENGTH = 255
_DEFAULT_DECIMAL_PRECISION = 38
_DEFAULT_DECIMAL_SCALE = 9
_DECIMAL_MAX_PRECISION: "dict[str, int]" = {
    "duckdb": 38,
    "mssql": 38,
    "oracle": 38,
    "db2": 31,
    "mysql": 65,
    "mariadb": 65,
    "postgres": 1000,
    "cockroachdb": 1000,
    "bigquery": 76,
}
_SPANNER_NUMERIC_SCALE = 9
_SPANNER_NUMERIC_INTEGER_DIGITS = 29
_BIGQUERY_NUMERIC_MAX_PRECISION = 38
_BIGQUERY_NUMERIC_MAX_SCALE = 9
_BIGQUERY_BIGNUMERIC_MAX_PRECISION = 76
_ARROW_TOKEN_FACTORIES: "dict[str, Callable[[Any], Any]]" = {
    "bool": lambda pa: pa.bool_(),
    "int16": lambda pa: pa.int16(),
    "int32": lambda pa: pa.int32(),
    "int64": lambda pa: pa.int64(),
    "float32": lambda pa: pa.float32(),
    "float64": lambda pa: pa.float64(),
    "decimal": lambda pa: pa.decimal128(38, 9),
    "string": lambda pa: pa.string(),
    "binary": lambda pa: pa.binary(),
    "date": lambda pa: pa.date32(),
    "time": lambda pa: pa.time64("us"),
    "timestamp": lambda pa: pa.timestamp("us"),
    "timestamptz": lambda pa: pa.timestamp("us", tz="UTC"),
    "duration": lambda pa: pa.duration("us"),
}


@overload
def convert_dict_to_arrow(
    data: "list[dict[str, Any]]",
    return_format: Literal["table"] = "table",
    batch_size: int | None = None,
    column_types: "Mapping[str, str] | None" = None,
) -> "ArrowTable": ...


@overload
def convert_dict_to_arrow(
    data: "list[dict[str, Any]]",
    return_format: Literal["reader"],
    batch_size: int | None = None,
    column_types: "Mapping[str, str] | None" = None,
) -> "ArrowRecordBatchReader": ...


@overload
def convert_dict_to_arrow(
    data: "list[dict[str, Any]]",
    return_format: Literal["batch"],
    batch_size: int | None = None,
    column_types: "Mapping[str, str] | None" = None,
) -> "ArrowRecordBatch": ...


@overload
def convert_dict_to_arrow(
    data: "list[dict[str, Any]]",
    return_format: Literal["batches"],
    batch_size: int | None = None,
    column_types: "Mapping[str, str] | None" = None,
) -> "list[ArrowRecordBatch]": ...


def convert_dict_to_arrow(
    data: "list[dict[str, Any]]",
    return_format: Literal["table", "reader", "batch", "batches"] = "table",
    batch_size: int | None = None,
    column_types: "Mapping[str, str] | None" = None,
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
        column_types: Optional column name to neutral type-token map reported by the
            adapter. Used to type columns that are NULL in every row and would
            otherwise infer as Arrow ``null``.

    Returns:
        ArrowTable or ArrowRecordBatch depending on return_format.
    """
    ensure_pyarrow()
    import pyarrow as pa

    if not data:
        empty_schema = pa.schema([])
        empty_table = pa.Table.from_pydict({}, schema=empty_schema)

        if return_format == "reader":
            return pa.RecordBatchReader.from_batches(empty_table.schema, empty_table.to_batches())

        if return_format == "batches":
            return []

        if return_format == "batch":
            batches = empty_table.to_batches(max_chunksize=batch_size)
            return batches[0] if batches else pa.RecordBatch.from_pydict({})

        return empty_table
    arrow_table = _resolve_null_columns(pa.Table.from_pylist(data), column_types)

    if return_format == "reader":
        batches = arrow_table.to_batches(max_chunksize=batch_size)
        return pa.RecordBatchReader.from_batches(arrow_table.schema, batches)

    if return_format == "batches":
        return arrow_table.to_batches(max_chunksize=batch_size)

    if return_format == "batch":
        batches = arrow_table.to_batches(max_chunksize=batch_size)
        return batches[0] if batches else pa.RecordBatch.from_pydict({})

    return arrow_table


def arrow_type_from_token(token: str) -> Any:
    """Return the pyarrow type for a neutral column-type token.

    Unknown tokens resolve to ``string`` so an adapter reporting a type this
    module does not model degrades to the safe fallback rather than raising.
    """
    import pyarrow as pa

    factory = _ARROW_TOKEN_FACTORIES.get(token)
    if factory is None:
        return pa.string()
    return factory(pa)


def sql_type_from_arrow_type(arrow_type: Any, dialect: str | None = None) -> str:
    """Return a SQL column type string for a PyArrow DataType.

    Args:
        arrow_type: PyArrow DataType instance.
        dialect: Optional target SQL dialect name.

    Returns:
        Dialect-optimal or standard SQL type string.
    """
    ensure_pyarrow()
    import pyarrow as pa

    if _arrow_type_is_uuid(arrow_type):
        return sql_type_for_category("uuid", dialect)

    unwrapped = arrow_type
    if pa.types.is_dictionary(unwrapped):
        unwrapped = unwrapped.value_type
    is_extension = getattr(pa.types, "is_extension", None)
    if is_extension is not None and is_extension(unwrapped) and not _arrow_type_is_uuid(unwrapped):
        unwrapped = unwrapped.storage_type

    if _arrow_type_is_uuid(unwrapped):
        return sql_type_for_category("uuid", dialect)
    if pa.types.is_decimal(unwrapped):
        return sql_type_for_category("decimal", dialect, precision=unwrapped.precision, scale=unwrapped.scale)
    if pa.types.is_timestamp(unwrapped):
        return sql_type_for_category("timestamptz" if getattr(unwrapped, "tz", None) else "timestamp", dialect)
    return sql_type_for_category(_arrow_type_category(pa, unwrapped), dialect)


def sql_type_for_category(
    category: str, dialect: str | None = None, *, precision: int | None = None, scale: int | None = None
) -> str:
    """Return a dialect-aware SQL column type for a neutral value category.

    This is the single mapping shared by Arrow schema conversion and Python value
    inference. Categories are ``uuid``, ``boolean``, ``smallint``, ``integer``,
    ``bigint``, ``real``, ``double``, ``decimal``, ``date``, ``time``,
    ``timestamp`` (naive), ``timestamptz`` (timezone-aware), ``interval``,
    ``blob``, ``json``, ``text``, and ``key_text`` (text used in a primary key,
    which must be bounded on engines that cannot index unbounded text).

    Args:
        category: Neutral value category.
        dialect: Optional target SQL dialect name.
        precision: Decimal precision for the ``decimal`` category.
        scale: Decimal scale for the ``decimal`` category.

    Returns:
        Dialect-specific SQL type string.

    Raises:
        SQLBuilderError: When a decimal precision exceeds what the dialect can store exactly.
    """
    config = _resolve_dialect_config(dialect)
    name = config.name if config is not None else None
    if category in _CONFIG_CATEGORY_DEFAULTS:
        return config.get_optimal_type(category) if config is not None else _CONFIG_CATEGORY_DEFAULTS[category]
    if category == "key_text":
        if name in _BOUNDED_KEY_TEXT_DIALECTS and config is not None:
            return config.get_optimal_type("varchar", length=_KEY_TEXT_LENGTH)
        return config.get_optimal_type("text") if config is not None else "TEXT"
    if category == "double":
        if name in {"duckdb", "mysql", "mariadb"}:
            return "DOUBLE"
        if name in {"bigquery", "spanner"} and config is not None:
            return config.get_optimal_type("float")
        return "DOUBLE PRECISION"
    if category == "decimal":
        return _decimal_sql_type(name, precision, scale)
    if category == "timestamp":
        return _NAIVE_TIMESTAMP_TYPES.get(name or "", "TIMESTAMP")
    if category == "timestamptz":
        if name is None:
            return "TIMESTAMP WITH TIME ZONE"
        return _AWARE_TIMESTAMP_TYPES.get(name, "TIMESTAMP")
    if category in _PLAIN_CATEGORY_TYPES:
        return _PLAIN_CATEGORY_TYPES[category]
    msg = f"Unknown SQL type category: {category!r}"
    raise SQLBuilderError(msg)


def convert_dict_to_arrow_with_schema(
    data: "list[dict[str, Any]]",
    return_format: Literal["table", "reader", "batch", "batches"] = "table",
    batch_size: int | None = None,
    arrow_schema: Any = None,
    column_types: "Mapping[str, str] | None" = None,
) -> "ArrowTable | ArrowRecordBatch | ArrowRecordBatchReader | list[ArrowRecordBatch]":
    """Convert dict rows to Arrow, optionally casting to an explicit schema.

    An explicit ``arrow_schema`` always wins; ``column_types`` only fills in
    columns that would otherwise infer as Arrow ``null``.
    """
    table = convert_dict_to_arrow(data, return_format="table", batch_size=batch_size, column_types=column_types)
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


def records_to_arrow_table(
    records: "Iterable[Mapping[str, Any]] | Iterable[Iterable[Any]]", columns: "list[str] | None"
) -> "ArrowTable":
    """Normalize mapping or positional records into a PyArrow table for ingest."""
    ensure_pyarrow()
    import pyarrow as pa

    materialized = list(records)
    if not materialized:
        msg = "load_from_records requires at least one record."
        raise ImproperConfigurationError(msg)

    first = materialized[0]
    if isinstance(first, Mapping):
        resolved = columns if columns is not None else list(first.keys())
        expected = set(resolved)
        for record in materialized:
            if not isinstance(record, Mapping):
                msg = "load_from_records mapping records must all be mappings."
                raise ImproperConfigurationError(msg)
            if record.keys() != expected:
                msg = "load_from_records mapping records must all share the same keys."
                raise ImproperConfigurationError(msg)
        mapping_records = cast("list[Mapping[str, Any]]", materialized)
        table = pa.Table.from_pylist(mapping_records)
        return table if columns is None else table.select(resolved)

    if columns is None:
        msg = "load_from_records requires columns when records are positional sequences."
        raise ImproperConfigurationError(msg)

    row_values = [tuple(record) for record in materialized]
    if any(len(row) != len(columns) for row in row_values):
        msg = "load_from_records positional records must match the number of columns."
        raise ImproperConfigurationError(msg)
    try:
        arrays = [pa.array(values) for values in zip(*row_values, strict=True)]
    except ValueError as exc:
        msg = "load_from_records positional records must match the number of columns."
        raise ImproperConfigurationError(msg) from exc
    return pa.Table.from_arrays(arrays, names=columns)


def coerce_arrow_table(source: "ArrowResult | Any") -> "ArrowTable":
    """Coerce various sources to a PyArrow Table."""
    ensure_pyarrow()

    if has_get_data(source):
        table = source.get_data()
        coercer = _get_arrow_table_coercer().get(table)
        if coercer is not None:
            return cast("ArrowTable", coercer(table))
        msg = "ArrowResult did not return a pyarrow.Table instance"
        raise TypeError(msg)
    coercer = _get_arrow_table_coercer().get(source)
    if coercer is not None:
        return cast("ArrowTable", coercer(source))
    if hasattr(source, "__arrow_c_stream__"):
        import pyarrow as pa

        return pa.RecordBatchReader.from_stream(source).read_all()
    if isinstance(source, Mapping):
        import pyarrow as pa

        return pa.Table.from_pydict(source)
    if isinstance(source, Iterable):
        import pyarrow as pa

        return pa.Table.from_pylist(list(source))
    msg = f"Unsupported Arrow source type: {type(source).__name__}"
    raise TypeError(msg)


def coerce_arrow_batches(
    source: "ArrowResult | Any", *, batch_size: int = 65_536
) -> "tuple[list[str], Iterator[ArrowRecordBatch]]":
    """Coerce an Arrow source into column names and a lazy RecordBatch iterator.

    Streams ``pyarrow.RecordBatchReader`` and ``__arrow_c_stream__`` sources one
    batch at a time without materializing the full table into memory.

    Args:
        source: ArrowResult, RecordBatchReader, RecordBatch, Table, C stream, or dict/row iterable.
        batch_size: Maximum rows per batch when splitting an in-memory Table.

    Returns:
        Tuple of ``(column_names, batch_iterator)``.
    """
    ensure_pyarrow()
    import pyarrow as pa

    unwrapped = source.get_data() if has_get_data(source) else source
    if isinstance(unwrapped, pa.RecordBatchReader):
        return list(unwrapped.schema.names), iter(unwrapped)
    if isinstance(unwrapped, pa.RecordBatch):
        return list(unwrapped.schema.names), iter((unwrapped,))
    if isinstance(unwrapped, pa.Table):
        return list(unwrapped.schema.names), iter(unwrapped.to_batches(max_chunksize=batch_size))
    if hasattr(unwrapped, "__arrow_c_stream__"):
        reader = pa.RecordBatchReader.from_stream(unwrapped)
        return list(reader.schema.names), iter(reader)
    table = coerce_arrow_table(unwrapped)
    return list(table.schema.names), iter(table.to_batches(max_chunksize=batch_size))


def arrow_batch_to_rows(batch: "ArrowRecordBatch", columns: "list[str] | None" = None) -> "list[tuple[Any, ...]]":
    """Convert a single Arrow RecordBatch to a list of row tuples.

    Args:
        batch: Arrow RecordBatch to extract rows from.
        columns: Optional subset or ordering of column names.

    Returns:
        List of row tuples for the batch.
    """
    ensure_pyarrow()
    resolved_columns = columns or list(batch.schema.names)
    if not resolved_columns or batch.num_rows == 0:
        return []
    col_data = [batch.column(col).to_pylist() for col in resolved_columns]
    if not col_data or not col_data[0]:
        return []
    return [tuple(row) for row in zip(*col_data, strict=False)]


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

    col_data = [table.column(col).to_pylist() for col in resolved_columns]
    if not col_data or not col_data[0]:
        return resolved_columns, []

    records: list[tuple[Any, ...]] = [tuple(row) for row in zip(*col_data, strict=False)]
    return resolved_columns, records


def arrow_table_needs_parameter_preparation(table: "ArrowTable") -> bool:
    """Return whether Arrow rows may emit nested values needing driver preparation."""
    return _arrow_schema_needs_preparation(table.schema)


def arrow_table_to_pylist(
    table: "ArrowTable | ArrowRecordBatch", *, decode_arrow_extension_types: bool = False
) -> "list[dict[str, Any]]":
    """Convert Arrow data to dictionaries and optionally decode opaque UUID fields."""
    if not decode_arrow_extension_types or not _arrow_schema_has_opaque_uuid(table.schema):
        return table.to_pylist()

    column_values = [
        _arrow_uuid_column_to_pylist(table.column(index), field.type) for index, field in enumerate(table.schema)
    ]
    return [dict(zip(table.column_names, row, strict=False)) for row in zip(*column_values, strict=False)]


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


def build_ingest_telemetry(table: "ArrowTable", *, format_label: str = "arrow") -> "StorageTelemetry":
    """Build telemetry dict from Arrow table statistics."""
    if has_arrow_table_stats(table):
        rows = int(table.num_rows)
        bytes_processed = int(table.nbytes)
    else:
        rows = 0
        bytes_processed = 0
    return {"rows_processed": rows, "bytes_processed": bytes_processed, "format": format_label}


def arrow_reader_with_deferred_close(reader: Any, close_callback: "Callable[[], None]") -> "ArrowRecordBatchReader":
    """Wrap a RecordBatchReader so close_callback fires when it is exhausted or errors."""
    ensure_pyarrow()
    import pyarrow as pa

    return pa.RecordBatchReader.from_batches(reader.schema, _DeferredCloseBatchIterator(reader, close_callback))


def arrow_reader_to_return_format(
    reader: Any,
    return_format: Literal["table", "reader", "batch", "batches"] = "reader",
    batch_size: "int | None" = None,
    arrow_schema: Any = None,
) -> "tuple[Any, int]":
    """Shape a RecordBatchReader into the requested Arrow payload and row count.

    Returns -1 as the row count for lazily-streamed reader payloads.
    """
    ensure_pyarrow()
    import pyarrow as pa

    if not isinstance(reader, pa.RecordBatchReader):
        msg = f"Expected a pyarrow.RecordBatchReader, got {type(reader).__name__}"
        raise TypeError(msg)
    if arrow_schema is not None:
        if not isinstance(arrow_schema, pa.Schema):
            msg = f"arrow_schema must be a pyarrow.Schema, got {type(arrow_schema).__name__}"
            raise TypeError(msg)
        reader = reader.cast(arrow_schema)
    if return_format == "reader":
        return reader, -1
    if return_format == "batches":
        batches = list(reader)
        return batches, sum(batch.num_rows for batch in batches)
    table = reader.read_all()
    shaped = arrow_table_to_return_format(table, return_format=return_format, batch_size=batch_size)
    return shaped, int(table.num_rows)


def encode_arrow_batch_pg_binary(
    batch: "ArrowRecordBatch",
    *,
    columns: "Sequence[str] | None" = None,
    vector_columns: frozenset[str] = frozenset(),
    jsonb_columns: frozenset[str] = frozenset(),
    include_header: bool = False,
    include_trailer: bool = False,
) -> bytes:
    """Encode an Arrow RecordBatch into PostgreSQL COPY BINARY format.

    Uses zero-loop structured NumPy dtype packing when all columns have fixed
    wire widths and no nulls, falling back to direct Arrow buffer slicing for
    variable-width and nullable columns.

    Args:
        batch: Arrow RecordBatch to encode.
        columns: Optional subset or order of columns to encode.
        vector_columns: Column names using pgvector binary wire format.
        jsonb_columns: Column names requiring PostgreSQL jsonb version byte prefix.
        include_header: Whether to prepend ``PG_BINARY_COPY_HEADER``.
        include_trailer: Whether to append ``PG_BINARY_COPY_TRAILER``.

    Returns:
        PostgreSQL ``COPY ... WITH (FORMAT BINARY)`` byte payload.
    """
    ensure_pyarrow()
    resolved_columns = list(columns) if columns is not None else list(batch.schema.names)
    body = b""
    if resolved_columns and batch.num_rows > 0:
        fast_body = _encode_pg_binary_fast_path_1(batch, resolved_columns, jsonb_columns)
        if fast_body is not None:
            body = fast_body
        else:
            body = _encode_pg_binary_fast_path_2(batch, resolved_columns, vector_columns, jsonb_columns)
    parts: list[bytes] = []
    if include_header:
        parts.append(PG_BINARY_COPY_HEADER)
    if body:
        parts.append(body)
    if include_trailer:
        parts.append(PG_BINARY_COPY_TRAILER)
    return b"".join(parts)


def iter_pgcopy_binary_chunks(
    source: "ArrowResult | Any",
    *,
    columns: "Sequence[str] | None" = None,
    batch_size: int = 65_536,
    chunk_bytes: int = 8_388_608,
    vector_columns: frozenset[str] = frozenset(),
    jsonb_columns: frozenset[str] = frozenset(),
    metrics: "dict[str, int] | None" = None,
) -> "Iterator[bytes]":
    """Yield PostgreSQL COPY BINARY chunks from an Arrow source in bounded memory.

    Args:
        source: Arrow Table, RecordBatch, RecordBatchReader, ArrowResult, or RecordBatch iterator.
        columns: Optional explicit column order.
        batch_size: Maximum rows per batch when splitting a Table.
        chunk_bytes: Target byte size for yielded chunks (default 8 MiB).
        vector_columns: Column names using pgvector binary wire format.
        jsonb_columns: Column names requiring PostgreSQL jsonb version byte prefix.
        metrics: Optional mutable dict updated in-place with ``rows_processed`` and ``bytes_processed``.

    Yields:
        Byte chunks forming a complete ``PGCOPY`` binary stream.
    """
    ensure_pyarrow()
    import pyarrow as pa

    if columns is not None and isinstance(source, Iterator) and not isinstance(source, pa.RecordBatchReader):
        resolved_columns = list(columns)
        batches: Iterator[ArrowRecordBatch] = source
    else:
        inferred_columns, batches = coerce_arrow_batches(source, batch_size=batch_size)
        resolved_columns = list(columns) if columns is not None else inferred_columns

    buffer = bytearray(PG_BINARY_COPY_HEADER)
    threshold = max(1, int(chunk_bytes))
    total_rows = 0
    total_bytes = 0
    for batch in batches:
        if batch.num_rows == 0:
            continue
        total_rows += int(batch.num_rows)
        total_bytes += int(batch.nbytes)
        encoded = encode_arrow_batch_pg_binary(
            batch,
            columns=resolved_columns,
            vector_columns=vector_columns,
            jsonb_columns=jsonb_columns,
            include_header=False,
            include_trailer=False,
        )
        buffer.extend(encoded)
        if len(buffer) >= threshold:
            yield bytes(buffer)
            buffer.clear()
    if metrics is not None:
        metrics["rows_processed"] = total_rows
        metrics["bytes_processed"] = total_bytes
    buffer.extend(PG_BINARY_COPY_TRAILER)
    yield bytes(buffer)


def iter_pgcopy_binary_chunks_async(
    source: "ArrowResult | Any",
    *,
    columns: "Sequence[str] | None" = None,
    batch_size: int = 65_536,
    chunk_bytes: int = 8_388_608,
    vector_columns: frozenset[str] = frozenset(),
    jsonb_columns: frozenset[str] = frozenset(),
    metrics: "dict[str, int] | None" = None,
) -> Any:
    """Return an async byte-chunk iterator of PostgreSQL COPY BINARY payloads."""
    return _AsyncBytesIterator(
        iter_pgcopy_binary_chunks(
            source,
            columns=columns,
            batch_size=batch_size,
            chunk_bytes=chunk_bytes,
            vector_columns=vector_columns,
            jsonb_columns=jsonb_columns,
            metrics=metrics,
        )
    )


def _extract_pg_fixed_column_array(col: Any, dtype: Any, num_rows: int) -> "tuple[int, str, Any] | None":
    """Return ``(byte_width, numpy_dtype_str, numpy_array)`` for fixed-width scalar columns."""
    import numpy as np
    import pyarrow as pa

    is_extension = getattr(pa.types, "is_extension", None)
    if is_extension is not None and is_extension(dtype):
        dtype = dtype.storage_type
        col = col.storage if hasattr(col, "storage") else col

    if pa.types.is_boolean(dtype):
        filled = col.fill_null(False) if col.null_count > 0 else col
        arr = filled.to_numpy(zero_copy_only=False).astype("|u1", copy=False)
        return 1, "|u1", arr
    if pa.types.is_int8(dtype):
        raw = np.frombuffer(col.buffers()[1], dtype=np.int8, count=col.offset + num_rows)[col.offset :]
        return 2, ">i2", raw.astype(">i2")
    if pa.types.is_uint8(dtype):
        raw = np.frombuffer(col.buffers()[1], dtype=np.uint8, count=col.offset + num_rows)[col.offset :]
        return 2, ">i2", raw.astype(">i2")
    if pa.types.is_int16(dtype) or pa.types.is_uint16(dtype):
        raw = np.frombuffer(col.buffers()[1], dtype=np.int16, count=col.offset + num_rows)[col.offset :]
        return 2, ">i2", raw.astype(">i2", copy=False)
    if pa.types.is_int32(dtype) or pa.types.is_uint32(dtype):
        raw = np.frombuffer(col.buffers()[1], dtype=np.int32, count=col.offset + num_rows)[col.offset :]
        return 4, ">i4", raw.astype(">i4", copy=False)
    if pa.types.is_int64(dtype) or pa.types.is_uint64(dtype):
        raw = np.frombuffer(col.buffers()[1], dtype=np.int64, count=col.offset + num_rows)[col.offset :]
        return 8, ">i8", raw.astype(">i8", copy=False)
    if pa.types.is_float16(dtype):
        raw = np.frombuffer(col.buffers()[1], dtype=np.float16, count=col.offset + num_rows)[col.offset :]
        return 4, ">f4", raw.astype(">f4")
    if pa.types.is_float32(dtype):
        raw = np.frombuffer(col.buffers()[1], dtype=np.float32, count=col.offset + num_rows)[col.offset :]
        return 4, ">f4", raw.astype(">f4", copy=False)
    if pa.types.is_float64(dtype):
        raw = np.frombuffer(col.buffers()[1], dtype=np.float64, count=col.offset + num_rows)[col.offset :]
        return 8, ">f8", raw.astype(">f8", copy=False)
    if pa.types.is_date32(dtype):
        buf = col.buffers()[1]
        raw = np.frombuffer(buf, dtype=np.int32, count=col.offset + num_rows)[col.offset :]
        return 4, ">i4", (raw - _PG_DATE_EPOCH_DAYS).astype(">i4")
    if pa.types.is_date64(dtype):
        buf = col.buffers()[1]
        raw = np.frombuffer(buf, dtype=np.int64, count=col.offset + num_rows)[col.offset :]
        return 4, ">i4", ((raw // 86_400_000) - _PG_DATE_EPOCH_DAYS).astype(">i4")
    if pa.types.is_timestamp(dtype):
        buf = col.buffers()[1]
        raw = np.frombuffer(buf, dtype=np.int64, count=col.offset + num_rows)[col.offset :]
        unit = dtype.unit
        if unit == "us":
            shifted = raw - _PG_EPOCH_OFFSET_US
        elif unit == "ms":
            shifted = raw * 1_000 - _PG_EPOCH_OFFSET_US
        elif unit == "s":
            shifted = raw * 1_000_000 - _PG_EPOCH_OFFSET_US
        else:
            shifted = (raw // 1_000) - _PG_EPOCH_OFFSET_US
        return 8, ">i8", shifted.astype(">i8")
    if pa.types.is_fixed_size_binary(dtype):
        width = int(dtype.byte_width)
        buf = col.buffers()[1]
        arr = np.frombuffer(buf, dtype=f"|V{width}", count=col.offset + num_rows)[col.offset :]
        return width, f"|V{width}", arr
    return None


def _encode_pg_binary_fast_path_1(
    batch: "ArrowRecordBatch", columns: "list[str]", jsonb_columns: frozenset[str]
) -> bytes | None:
    """Pack non-null fixed-width and FixedSizeList float vector columns via structured NumPy dtype."""
    import numpy as np
    import pyarrow as pa

    num_rows = int(batch.num_rows)
    dtype_fields: list[Any] = [("ncols", ">i2")]
    assignments: list[tuple[str, Any]] = [("ncols", len(columns))]

    for idx, col_name in enumerate(columns):
        if col_name in jsonb_columns:
            return None
        col = batch.column(col_name)
        if col.null_count > 0:
            return None
        if isinstance(col, pa.ChunkedArray):
            col = col.combine_chunks()
        field_type = col.type
        fixed_info = _extract_pg_fixed_column_array(col, field_type, num_rows)
        if fixed_info is not None:
            byte_width, np_dtype, arr = fixed_info
            len_key = f"c{idx}_len"
            val_key = f"c{idx}_val"
            dtype_fields.append((len_key, ">i4"))
            dtype_fields.append((val_key, np_dtype))
            assignments.append((len_key, byte_width))
            assignments.append((val_key, arr))
            continue

        if pa.types.is_fixed_size_list(field_type) and pa.types.is_floating(field_type.value_type):
            dim = int(field_type.list_size)
            flat_values = col.values[col.offset * dim : (col.offset + num_rows) * dim]
            if flat_values.null_count > 0:
                return None
            is_f16 = pa.types.is_float16(field_type.value_type)
            elem_dtype = ">f2" if is_f16 else ">f4"
            elem_size = 2 if is_f16 else 4
            arr_2d = flat_values.to_numpy(zero_copy_only=False).astype(elem_dtype, copy=False).reshape(num_rows, dim)
            len_key = f"c{idx}_len"
            dim_key = f"c{idx}_dim"
            unused_key = f"c{idx}_unused"
            val_key = f"c{idx}_val"
            dtype_fields.extend([
                (len_key, ">i4"),
                (dim_key, ">u2"),
                (unused_key, ">u2"),
                (val_key, elem_dtype, (dim,)),
            ])
            assignments.extend([
                (len_key, 4 + elem_size * dim),
                (dim_key, dim),
                (unused_key, 0),
                (val_key, arr_2d),
            ])
            continue

        return None

    row_dtype = np.dtype(dtype_fields)
    out = np.empty(num_rows, dtype=row_dtype)
    for field_key, value in assignments:
        out[field_key] = value
    return out.tobytes()


def _encode_pg_binary_fast_path_2(
    batch: "ArrowRecordBatch",
    columns: "list[str]",
    vector_columns: frozenset[str],
    jsonb_columns: frozenset[str],
) -> bytes:
    """Encode variable-width and nullable columns by slicing Arrow buffers directly."""
    import numpy as np
    import pyarrow as pa

    num_rows = int(batch.num_rows)
    row_header = struct.pack(">h", len(columns))
    col_encoders: list[Callable[[int, bytearray], None]] = []

    for col_name in columns:
        col = batch.column(col_name)
        if isinstance(col, pa.ChunkedArray):
            col = col.combine_chunks()
        field_type = col.type
        has_nulls = col.null_count > 0
        null_mask = col.is_null().to_numpy(zero_copy_only=False) if has_nulls else None

        if col_name not in jsonb_columns:
            fixed_info = _extract_pg_fixed_column_array(col, field_type, num_rows)
            if fixed_info is not None:
                byte_width, np_dtype, arr = fixed_info
                packed = np.empty(num_rows, dtype=np.dtype([("len", ">i4"), ("val", np_dtype)]))
                packed["len"] = byte_width
                packed["val"] = arr
                packed_mv = memoryview(packed.tobytes())
                stride = 4 + byte_width

                def _emit_fixed(
                    row_idx: int,
                    out_buf: bytearray,
                    _mv: memoryview = packed_mv,
                    _stride: int = stride,
                    _nulls: Any = null_mask,
                ) -> None:
                    if _nulls is not None and _nulls[row_idx]:
                        out_buf.extend(_PG_NULL_MARKER)
                    else:
                        start = row_idx * _stride
                        out_buf.extend(_mv[start : start + _stride])

                col_encoders.append(_emit_fixed)
                continue

            if pa.types.is_fixed_size_list(field_type) and pa.types.is_floating(field_type.value_type):
                dim = int(field_type.list_size)
                flat_values = col.values[col.offset * dim : (col.offset + num_rows) * dim]
                is_f16 = pa.types.is_float16(field_type.value_type)
                elem_dtype = ">f2" if is_f16 else ">f4"
                elem_size = 2 if is_f16 else 4
                arr_2d = (
                    flat_values.to_numpy(zero_copy_only=False).astype(elem_dtype, copy=False).reshape(num_rows, dim)
                )
                packed = np.empty(
                    num_rows,
                    dtype=np.dtype([
                        ("len", ">i4"),
                        ("dim", ">u2"),
                        ("unused", ">u2"),
                        ("val", elem_dtype, (dim,)),
                    ]),
                )
                packed["len"] = 4 + elem_size * dim
                packed["dim"] = dim
                packed["unused"] = 0
                packed["val"] = arr_2d
                packed_mv = memoryview(packed.tobytes())
                stride = 8 + elem_size * dim

                def _emit_fixed_vec(
                    row_idx: int,
                    out_buf: bytearray,
                    _mv: memoryview = packed_mv,
                    _stride: int = stride,
                    _nulls: Any = null_mask,
                ) -> None:
                    if _nulls is not None and _nulls[row_idx]:
                        out_buf.extend(_PG_NULL_MARKER)
                    else:
                        start = row_idx * _stride
                        out_buf.extend(_mv[start : start + _stride])

                col_encoders.append(_emit_fixed_vec)
                continue

        if (
            pa.types.is_string(field_type)
            or pa.types.is_large_string(field_type)
            or pa.types.is_binary(field_type)
            or pa.types.is_large_binary(field_type)
        ):
            offset_dtype = (
                np.int64 if (pa.types.is_large_string(field_type) or pa.types.is_large_binary(field_type)) else np.int32
            )
            offsets = np.frombuffer(col.buffers()[1], dtype=offset_dtype, count=col.offset + num_rows + 1)[
                col.offset :
            ]
            data_buf = col.buffers()[2]
            data_mv = memoryview(data_buf) if data_buf is not None else memoryview(b"")
            is_jsonb = col_name in jsonb_columns
            diffs = np.diff(offsets)
            lengths_mv = memoryview((diffs + (1 if is_jsonb else 0)).astype(">i4").tobytes())

            def _emit_varlen(
                row_idx: int,
                out_buf: bytearray,
                _offsets: Any = offsets,
                _data_mv: memoryview = data_mv,
                _len_mv: memoryview = lengths_mv,
                _nulls: Any = null_mask,
                _is_jsonb: bool = is_jsonb,
            ) -> None:
                if _nulls is not None and _nulls[row_idx]:
                    out_buf.extend(_PG_NULL_MARKER)
                    return
                out_buf.extend(_len_mv[row_idx * 4 : (row_idx + 1) * 4])
                if _is_jsonb:
                    out_buf.append(1)
                start = int(_offsets[row_idx])
                end = int(_offsets[row_idx + 1])
                if end > start:
                    out_buf.extend(_data_mv[start:end])

            col_encoders.append(_emit_varlen)
            continue

        if (pa.types.is_list(field_type) or pa.types.is_large_list(field_type)) and (
            col_name in vector_columns or pa.types.is_floating(field_type.value_type)
        ):
            offset_dtype = np.int64 if pa.types.is_large_list(field_type) else np.int32
            offsets = np.frombuffer(col.buffers()[1], dtype=offset_dtype, count=col.offset + num_rows + 1)[
                col.offset :
            ]
            is_f16 = pa.types.is_float16(field_type.value_type)
            elem_dtype = ">f2" if is_f16 else ">f4"
            elem_size = 2 if is_f16 else 4
            flat_bytes = col.values.to_numpy(zero_copy_only=False).astype(elem_dtype, copy=False).tobytes()
            flat_mv = memoryview(flat_bytes)
            dims = np.diff(offsets)
            headers = np.empty(num_rows, dtype=np.dtype([("len", ">i4"), ("dim", ">u2"), ("unused", ">u2")]))
            headers["len"] = 4 + elem_size * dims
            headers["dim"] = dims
            headers["unused"] = 0
            headers_mv = memoryview(headers.tobytes())

            def _emit_var_vec(
                row_idx: int,
                out_buf: bytearray,
                _offsets: Any = offsets,
                _flat_mv: memoryview = flat_mv,
                _hdr_mv: memoryview = headers_mv,
                _elem_size: int = elem_size,
                _nulls: Any = null_mask,
            ) -> None:
                if _nulls is not None and _nulls[row_idx]:
                    out_buf.extend(_PG_NULL_MARKER)
                    return
                out_buf.extend(_hdr_mv[row_idx * 8 : (row_idx + 1) * 8])
                start = int(_offsets[row_idx]) * _elem_size
                end = int(_offsets[row_idx + 1]) * _elem_size
                if end > start:
                    out_buf.extend(_flat_mv[start:end])

            col_encoders.append(_emit_var_vec)
            continue

        py_values = col.to_pylist()
        is_jsonb = col_name in jsonb_columns

        def _emit_fallback(
            row_idx: int,
            out_buf: bytearray,
            _vals: list[Any] = py_values,
            _is_jsonb: bool = is_jsonb,
        ) -> None:
            val = _vals[row_idx]
            if val is None:
                out_buf.extend(_PG_NULL_MARKER)
                return
            if isinstance(val, (bytes, bytearray, memoryview)):
                raw = bytes(val)
            elif isinstance(val, str):
                raw = val.encode("utf-8")
            elif isinstance(val, (dict, list, tuple)):
                raw = to_json(val).encode("utf-8")
            else:
                raw = str(val).encode("utf-8")
            if _is_jsonb:
                out_buf.extend(struct.pack(">iB", len(raw) + 1, 1))
            else:
                out_buf.extend(struct.pack(">i", len(raw)))
            out_buf.extend(raw)

        col_encoders.append(_emit_fallback)

    out_buf = bytearray()
    for row_idx in range(num_rows):
        out_buf.extend(row_header)
        for encoder in col_encoders:
            encoder(row_idx, out_buf)
    return bytes(out_buf)


@lru_cache(maxsize=_ARROW_SCHEMA_DECISION_CACHE_SIZE)
def _arrow_schema_has_opaque_uuid(schema: Any) -> bool:
    return any(
        _arrow_type_is_opaque_uuid(field.type) or _arrow_type_is_list_of_opaque_uuid(field.type) for field in schema
    )


def _resolve_dialect_config(dialect: str | None) -> "DialectConfig | None":
    if not dialect:
        return None
    with contextlib.suppress(ValueError):
        return get_dialect_config(str(dialect))
    return None


def _arrow_type_category(pa: Any, data_type: Any) -> str:
    """Map a non-decimal, non-timestamp PyArrow DataType to a neutral SQL type category."""
    types = pa.types
    if types.is_boolean(data_type):
        return "boolean"
    if types.is_int8(data_type) or types.is_int16(data_type) or types.is_uint8(data_type):
        return "smallint"
    if types.is_int32(data_type) or types.is_uint16(data_type):
        return "integer"
    if types.is_int64(data_type) or types.is_uint32(data_type) or types.is_uint64(data_type):
        return "bigint"
    if types.is_float16(data_type) or types.is_float32(data_type):
        return "real"
    if types.is_float64(data_type):
        return "double"
    if types.is_date(data_type):
        return "date"
    if types.is_time(data_type):
        return "time"
    if types.is_duration(data_type) or types.is_interval(data_type):
        return "interval"
    if (
        types.is_binary(data_type)
        or types.is_large_binary(data_type)
        or types.is_fixed_size_binary(data_type)
        or getattr(types, "is_binary_view", lambda _: False)(data_type)
    ):
        return "blob"
    if (
        types.is_list(data_type)
        or types.is_large_list(data_type)
        or types.is_fixed_size_list(data_type)
        or types.is_struct(data_type)
        or types.is_map(data_type)
        or getattr(types, "is_list_view", lambda _: False)(data_type)
        or getattr(types, "is_large_list_view", lambda _: False)(data_type)
    ):
        return "json"
    return "text"


def _decimal_sql_type(dialect_name: str | None, precision: int | None, scale: int | None) -> str:
    """Render an exact decimal type, refusing precisions the dialect cannot store exactly."""
    resolved_precision = _DEFAULT_DECIMAL_PRECISION if precision is None else int(precision)
    resolved_scale = _DEFAULT_DECIMAL_SCALE if scale is None else int(scale)
    if dialect_name == "spanner":
        if (
            resolved_scale > _SPANNER_NUMERIC_SCALE
            or resolved_precision - resolved_scale > _SPANNER_NUMERIC_INTEGER_DIGITS
        ):
            msg = (
                f"DECIMAL({resolved_precision}, {resolved_scale}) does not fit Spanner NUMERIC "
                f"(at most {_SPANNER_NUMERIC_INTEGER_DIGITS} integer digits and {_SPANNER_NUMERIC_SCALE} fractional digits)"
            )
            raise SQLBuilderError(msg)
        return "NUMERIC"
    if dialect_name == "bigquery":
        if resolved_precision <= _BIGQUERY_NUMERIC_MAX_PRECISION and resolved_scale <= _BIGQUERY_NUMERIC_MAX_SCALE:
            return f"NUMERIC({resolved_precision}, {resolved_scale})"
        if resolved_precision <= _BIGQUERY_BIGNUMERIC_MAX_PRECISION:
            return f"BIGNUMERIC({resolved_precision}, {resolved_scale})"
    max_precision = _DECIMAL_MAX_PRECISION.get(dialect_name or "")
    if max_precision is not None and resolved_precision > max_precision:
        msg = (
            f"DECIMAL({resolved_precision}, {resolved_scale}) exceeds the maximum precision {max_precision} "
            f"supported by {dialect_name}; cast the column to a narrower decimal or a text type explicitly"
        )
        raise SQLBuilderError(msg)
    return f"DECIMAL({resolved_precision}, {resolved_scale})"


def _arrow_type_is_uuid(data_type: Any) -> bool:
    if _arrow_type_is_opaque_uuid(data_type):
        return True
    ext_name = getattr(data_type, "extension_name", None)
    return isinstance(ext_name, str) and "uuid" in ext_name.casefold()


def _arrow_type_is_opaque_uuid(data_type: Any) -> bool:
    return (
        getattr(data_type, "extension_name", None) == "arrow.opaque"
        and getattr(data_type, "type_name", "").casefold() == "uuid"
    )


def _arrow_type_is_list_of_opaque_uuid(data_type: Any) -> bool:
    ensure_pyarrow()
    import pyarrow as pa

    return pa.types.is_list(data_type) and _arrow_type_is_opaque_uuid(data_type.value_type)


def _arrow_uuid_column_to_pylist(column: Any, data_type: Any) -> "list[Any]":
    if _arrow_type_is_opaque_uuid(data_type):
        ensure_pyarrow()
        import pyarrow as pa

        array = cast("Any", column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column)
        storage_values = cast("list[Any]", array.storage.to_pylist())
        return [uuid_from_bytes(value) if value is not None else None for value in storage_values]
    if _arrow_type_is_list_of_opaque_uuid(data_type):
        nested_values = cast("list[Any]", column.to_pylist())
        return [
            [uuid_from_bytes(item) if item is not None else None for item in value] if value is not None else None
            for value in nested_values
        ]
    return cast("list[Any]", column.to_pylist())


def _resolve_null_columns(table: "ArrowTable", column_types: "Mapping[str, str] | None" = None) -> "ArrowTable":
    """Give value-inferred Arrow ``null`` columns their real type.

    ``pa.Table.from_pylist`` infers column types from values alone, so a column
    that is ``NULL`` in every row arrives as Arrow ``null`` and loses the type
    the database declared. When the adapter reported that column's type, use it.
    Otherwise fall back to ``string``, which holds the nulls without breaking
    string operations downstream.
    """
    import pyarrow as pa

    if not any(pa.types.is_null(field.type) for field in table.schema):
        return table

    hints = column_types or {}
    fields = [
        field.with_type(arrow_type_from_token(hints[field.name]))
        if pa.types.is_null(field.type) and field.name in hints
        else (field.with_type(pa.string()) if pa.types.is_null(field.type) else field)
        for field in table.schema
    ]
    return table.cast(pa.schema(fields))


def _coerce_arrow_table_identity(source: Any) -> Any:
    return source


def _coerce_arrow_record_batch(source: Any) -> Any:
    import pyarrow as pa

    return pa.Table.from_batches([source])


def _coerce_arrow_record_batch_reader(source: Any) -> Any:
    return source.read_all()


def _get_arrow_table_coercer() -> "TypeDispatcher[Any]":
    global _ARROW_TABLE_COERCER
    if _ARROW_TABLE_COERCER is None:
        ensure_pyarrow()
        import pyarrow as pa

        dispatcher = TypeDispatcher[Any]()
        dispatcher.register(pa.Table, _coerce_arrow_table_identity)
        dispatcher.register(pa.RecordBatch, _coerce_arrow_record_batch)
        dispatcher.register(pa.RecordBatchReader, _coerce_arrow_record_batch_reader)
        _ARROW_TABLE_COERCER = dispatcher
    return _ARROW_TABLE_COERCER


def _arrow_type_needs_preparation(data_type: Any) -> bool:
    ensure_pyarrow()
    import pyarrow as pa

    if pa.types.is_dictionary(data_type):
        return _arrow_type_needs_preparation(data_type.value_type)

    is_extension = getattr(pa.types, "is_extension", None)
    if is_extension is not None and is_extension(data_type):
        return _arrow_type_needs_preparation(data_type.storage_type)

    nested_type_checks = (
        "is_struct",
        "is_list",
        "is_large_list",
        "is_fixed_size_list",
        "is_map",
        "is_union",
        "is_list_view",
        "is_large_list_view",
    )
    return any(getattr(pa.types, check, lambda _: False)(data_type) for check in nested_type_checks)


@lru_cache(maxsize=_ARROW_SCHEMA_DECISION_CACHE_SIZE)
def _arrow_schema_needs_preparation(schema: Any) -> bool:
    return any(_arrow_type_needs_preparation(field.type) for field in schema)


class _DeferredCloseBatchIterator:
    """Iterate RecordBatches from a reader and invoke a callback on exhaustion or error."""

    __slots__ = ("_close_callback", "_closed", "_reader")

    def __init__(self, reader: Any, close_callback: "Callable[[], None]") -> None:
        self._reader = reader
        self._close_callback = close_callback
        self._closed = False

    def __del__(self) -> None:
        self._finalize()

    def __iter__(self) -> "_DeferredCloseBatchIterator":
        return self

    def __next__(self) -> Any:
        try:
            return self._reader.read_next_batch()
        except StopIteration:
            self._finalize()
            raise
        except Exception:
            self._finalize()
            raise

    def close(self) -> None:
        self._finalize()

    def _finalize(self) -> None:
        if not self._closed:
            self._closed = True
            with contextlib.suppress(Exception):
                self._close_callback()


class _AsyncBytesIterator:
    """Wrap a synchronous byte-chunk iterator as an async iterator."""

    __slots__ = ("_iterator",)

    def __init__(self, iterator: "Iterator[bytes]") -> None:
        self._iterator = iterator

    def __aiter__(self) -> "_AsyncBytesIterator":
        return self

    async def __anext__(self) -> bytes:
        try:
            return next(self._iterator)
        except StopIteration as exc:
            raise StopAsyncIteration from exc
