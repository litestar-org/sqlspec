"""Fixture loading utilities for SQLSpec.

Provides functions for writing, loading and parsing JSON fixture files
used in testing and development, and for loading and exporting per-table
fixture files against a database driver. Supports both sync and async operations.
"""

import array
import base64
import gzip
import importlib
import os
import re
import secrets
import struct
import zipfile
from collections.abc import Callable, Mapping, Sequence
from datetime import time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, NamedTuple

from sqlglot import Dialect, exp

from sqlspec import dialects as _dialects
from sqlspec.core.parameters import TypedParameter
from sqlspec.core.type_converter import convert_iso_date, convert_iso_datetime, convert_uuid
from sqlspec.storage import storage_registry
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json as decode_json
from sqlspec.utils.serializers import schema_dump
from sqlspec.utils.serializers import to_json as encode_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.typing import SupportedSchemaModel

_ = _dialects

__all__ = (
    "export_table_fixtures_async",
    "export_table_fixtures_sync",
    "load_table_fixtures_async",
    "load_table_fixtures_sync",
    "open_fixture_async",
    "open_fixture_sync",
    "write_fixture_async",
    "write_fixture_sync",
)

_TABLE_FIXTURE_EXTENSIONS: Final["tuple[str, ...]"] = (".jsonl.gz", ".json.gz", ".jsonl", ".json")
_SHARD_STEM_PATTERN: Final["re.Pattern[str]"] = re.compile(r"^(.+)\.(\d+)$")
_COLUMN_NAME_PATTERN: Final["re.Pattern[str]"] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TABLE_NAME_PATTERN: Final["re.Pattern[str]"] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?")
_SEQUENCE_IDENTIFIER_PATTERN: Final["re.Pattern[str]"] = re.compile(
    r"[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)?"
)
_NEXTVAL_CALL_PATTERN: Final["re.Pattern[str]"] = re.compile(r"""(?i)\bnextval\(\s*(?:'([^']+)'|([`A-Za-z0-9_.$]+))""")
_NEXT_VALUE_FOR_PATTERN: Final["re.Pattern[str]"] = re.compile(
    r"""(?i)\b(?:next\s+value|nextval)\s+for\s+([A-Za-z0-9_.$"\[\]`]+)"""
)
_ORACLE_NEXTVAL_PATTERN: Final["re.Pattern[str]"] = re.compile(
    r"""(?i)^\s*\(*\s*([A-Za-z0-9_.$"]+)\.\s*"?nextval"?\s*\)*\s*$"""
)
_INSERT_VALUES_CLAUSE_PATTERN: Final["re.Pattern[str]"] = re.compile(r"\)\s+VALUES\s+\(")
_POSTGRES_DIALECTS: Final["frozenset[str]"] = frozenset({"cockroach", "cockroachdb", "postgres", "postgresql"})
_PG_SEQUENCE_DIALECTS: Final["frozenset[str]"] = frozenset({"postgres", "postgresql"})
_SEQUENCE_RESYNC_DIALECTS: Final["frozenset[str]"] = frozenset({
    "cockroach",
    "cockroachdb",
    "db2",
    "duckdb",
    "mariadb",
    "mssql",
    "mysql",
    "oracle",
    "postgres",
    "postgresql",
    "sqlanywhere",
    "sqlite",
    "sybase",
    "tsql",
})
_MYSQL_DIALECTS: Final["frozenset[str]"] = frozenset({"mariadb", "mysql"})
_ON_CONFLICT_FAMILIES: Final["frozenset[str]"] = frozenset({"duckdb", "postgres", "sqlite"})
_JSON_VALUE_FAMILIES: Final["frozenset[str]"] = frozenset({"duckdb", "mysql", "postgres", "sqlite"})
_JSON_TYPE_NAMES: Final["frozenset[str]"] = frozenset({"json", "jsonb"})
_BINARY_TYPES: Final["frozenset[str]"] = frozenset({
    "binary",
    "blob",
    "bytea",
    "image",
    "long binary",
    "long raw",
    "longblob",
    "mediumblob",
    "raw",
    "tinyblob",
    "varbinary",
})
_UNORDERABLE_POSTGRES_TYPES: Final["frozenset[str]"] = frozenset({
    "box",
    "circle",
    "halfvec",
    "json",
    "line",
    "lseg",
    "path",
    "point",
    "polygon",
    "sparsevec",
    "vector",
    "xml",
})
_UNORDERABLE_FAMILY_TYPES: Final["Mapping[str, frozenset[str]]"] = {
    "bigquery": frozenset({"geography", "json"}),
    "db2": frozenset({"blob", "clob", "dbclob", "long varchar", "long vargraphic", "xml"}),
    "mysql": frozenset({"blob", "geometry", "json", "longblob", "mediumblob", "point", "tinyblob", "vector"}),
    "oracle": frozenset({"bfile", "blob", "clob", "long", "long raw", "nclob", "vector", "xmltype"}),
    "spanner": frozenset({"json", "tokenlist"}),
    "sqlanywhere": frozenset({"long binary", "long nvarchar", "long varchar", "xml"}),
    "sybase": frozenset({"image", "long binary", "long nvarchar", "long varchar", "text", "unitext"}),
    "tsql": frozenset({"geography", "geometry", "image", "ntext", "text", "xml"}),
}
_IDENTITY_GENERATION_VALUES: Final["frozenset[str]"] = frozenset({"always", "by default", "by default on null"})
_ALWAYS_IDENTITY_GENERATION_VALUES: Final["frozenset[str]"] = frozenset({"always"})
_ISO_DURATION_PATTERN: Final["re.Pattern[str]"] = re.compile(
    r"(-)?P(?:(\d+(?:\.\d+)?)W)?(?:(\d+(?:\.\d+)?)D)?"
    r"(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?"
)
_DUCKDB_INTERVAL_PARTS: Final[int] = 3
_NANOSECONDS_PER_MICROSECOND: Final[int] = 1000
_TEMPORARY_FILE_MODE: Final[int] = 0o666
_MAX_ZIP_FIXTURE_BYTES: Final[int] = 256 * 1024 * 1024
_FLOAT32_BYTES: Final[int] = 4
_PG_VECTOR_HEADER_BYTES: Final[int] = 4

logger = get_logger("sqlspec.utils.fixtures")


def open_fixture_sync(fixtures_path: Any, fixture_name: str) -> Any:
    """Load and parse a JSON fixture file with compression support.

    Supports reading from:
        - Regular JSON files (.json)
        - Gzipped JSON files (.json.gz)
        - Zipped JSON files (.json.zip)

    Args:
        fixtures_path: The path to look for fixtures (pathlib.Path)
        fixture_name: The fixture name to load.

    Returns:
        The parsed JSON data
    """
    fixture_path = _find_fixture_file(fixtures_path, fixture_name)

    if fixture_path.suffix in {".gz", ".zip"}:
        f_data = _read_compressed_file(fixture_path)
    else:
        with fixture_path.open(mode="r", encoding="utf-8") as f:
            f_data = f.read()

    return decode_json(f_data)


async def open_fixture_async(fixtures_path: Any, fixture_name: str) -> Any:
    """Load and parse a JSON fixture file asynchronously with compression support.

    Supports reading from:
        - Regular JSON files (.json)
        - Gzipped JSON files (.json.gz)
        - Zipped JSON files (.json.zip)

    For compressed files, uses sync reading in a thread pool since gzip and zipfile
    don't have native async equivalents.

    Args:
        fixtures_path: The path to look for fixtures (pathlib.Path)
        fixture_name: The fixture name to load.

    Returns:
        The parsed JSON data
    """
    fixture_path = _find_fixture_file(fixtures_path, fixture_name)

    if fixture_path.suffix in {".gz", ".zip"}:
        f_data = await _async_read_compressed(fixture_path)
    else:
        f_data = await _async_read_text(fixture_path)

    return decode_json(f_data)


def write_fixture_sync(
    fixtures_path: str,
    table_name: str,
    data: "list[SupportedSchemaModel] | list[dict[str, Any]] | SupportedSchemaModel",
    storage_backend: str = "local",
    compress: bool = False,
    **storage_kwargs: Any,
) -> None:
    """Write fixture data to storage using SQLSpec storage backend.

    Args:
        fixtures_path: Base path where fixtures should be stored
        table_name: Name of the table/fixture (used as filename)
        data: Data to write - can be list of dicts, models, or single model
        storage_backend: Storage backend to use (default: "local")
        compress: Whether to gzip compress the output
        **storage_kwargs: Additional arguments for the storage backend

    Raises:
        ValueError: If storage backend is not found or table_name is invalid
    """
    _validated_fixture_name(table_name)
    if storage_backend == "local":
        uri = "file://"
        storage_kwargs["base_path"] = _resolve_path_str(fixtures_path)
    else:
        uri = storage_backend

    try:
        storage = storage_registry.get(uri, **storage_kwargs)
    except Exception as exc:
        msg = f"Failed to get storage backend for '{storage_backend}': {exc}"
        raise ValueError(msg) from exc

    json_content = _serialize_data(data)

    if compress:
        file_path = f"{table_name}.json.gz"
        content = gzip.compress(json_content.encode("utf-8"))
        storage.write_bytes_sync(file_path, content)
    else:
        file_path = f"{table_name}.json"
        storage.write_text_sync(file_path, json_content)


async def write_fixture_async(
    fixtures_path: str,
    table_name: str,
    data: "list[SupportedSchemaModel] | list[dict[str, Any]] | SupportedSchemaModel",
    storage_backend: str = "local",
    compress: bool = False,
    **storage_kwargs: Any,
) -> None:
    """Write fixture data to storage using SQLSpec storage backend asynchronously.

    Args:
        fixtures_path: Base path where fixtures should be stored
        table_name: Name of the table/fixture (used as filename)
        data: Data to write - can be list of dicts, models, or single model
        storage_backend: Storage backend to use (default: "local")
        compress: Whether to gzip compress the output
        **storage_kwargs: Additional arguments for the storage backend

    Raises:
        ValueError: If storage backend is not found or table_name is invalid
    """
    _validated_fixture_name(table_name)
    if storage_backend == "local":
        uri = "file://"
        storage_kwargs["base_path"] = _resolve_path_str(fixtures_path)
    else:
        uri = storage_backend

    try:
        storage = storage_registry.get(uri, **storage_kwargs)
    except Exception as exc:
        msg = f"Failed to get storage backend for '{storage_backend}': {exc}"
        raise ValueError(msg) from exc

    json_content = await _async_serialize(data)

    if compress:
        file_path = f"{table_name}.json.gz"
        content = await _async_compress(json_content)
        await storage.write_bytes_async(file_path, content)
    else:
        file_path = f"{table_name}.json"
        await storage.write_text_async(file_path, json_content)


def load_table_fixtures_sync(
    driver: "SyncDriverAdapterBase",
    fixtures_path: "str | Path",
    *,
    tables: "Sequence[str] | None" = None,
    table_order: "Sequence[str] | None" = None,
    conflict_keys: "Mapping[str, Sequence[str] | str] | None" = None,
    batch_size: int = 500,
    resync_sequences: bool = False,
    ignore_unknown_columns: bool = False,
    exclude_update_columns: "Sequence[str] | Mapping[str, Sequence[str] | str] | None" = None,
) -> "dict[str, int]":
    """Load per-table fixture files into database tables.

    Each table reads from one or more files named after it: ``<table>.json`` holding a JSON
    array of row objects, or ``<table>.jsonl`` holding one row object per line, either
    optionally gzipped (``.gz``). Tables split into multiple shards (e.g. ``<table>.001.jsonl.gz``,
    ``<table>.002.jsonl.gz``) are loaded in ascending numeric shard order. Sparse row objects
    are normalized across the ordered union of column keys in the file, defaulting omitted keys
    to ``None``. Table and column names are quoted, so they must match the database spelling
    exactly. Rows are inserted in batches with ``execute_many``; each file is read into memory.
    Transaction control stays with the caller.

    The target table's columns are read from the driver's data dictionary first, and
    JSON values of these column types are converted: ISO 8601 strings in timestamp,
    datetime, date (datetimes on Oracle, whose DATE carries a time), and (except on
    MySQL) time columns to datetimes, dates, and times; ISO 8601 durations without year
    or month parts and numbers of seconds in interval columns to ``timedelta``, and
    DuckDB ``[months, days, nanoseconds]`` interval lists to interval text; strings and
    numbers in numeric and decimal columns to ``Decimal``; strings in uuid columns to
    ``UUID``; base64 strings in bytea, blob, raw, image, binary, and varbinary columns
    to bytes; and PostgreSQL, MySQL, DuckDB, and SQLite json and jsonb values to JSON
    text. Vector column values are converted according to the driver family and data
    type. Values of generated (computed) columns are ignored, while identity column
    values are loaded. On PostgreSQL, ``GENERATED ALWAYS`` identity columns receive
    the loaded values through ``OVERRIDING SYSTEM VALUE`` and are never updated by upserts;
    on SQL Server, inserts into identity columns run between ``SET IDENTITY_INSERT ... ON``
    and ``OFF``. Other databases receive identity values as given and reject them for
    identity columns that only allow generated values.

    Args:
        driver: Sync driver that runs the inserts.
        fixtures_path: Directory containing the table fixture files.
        tables: Tables to load. Defaults to every table fixture file in the directory;
            files whose names are not table identifiers are skipped with a debug log, and
            a dot in a file name marks a schema-qualified table.
        table_order: Tables to load first, in this order. Remaining tables load
            afterwards in alphabetical order; names not being loaded are ignored.
        conflict_keys: Mapping of table name to a column name or sequence of key column
            names of a unique constraint; tables not being loaded are ignored. Rows for
            matched tables are upserted, updating every non-key, non-excluded column
            when a row with the same key already exists. Supported on PostgreSQL-family,
            SQLite, and DuckDB drivers (``ON CONFLICT``) and MySQL
            (``ON DUPLICATE KEY UPDATE``, which matches any unique key of the table).
        batch_size: Maximum number of rows per ``execute_many`` call.
        resync_sequences: On supported database drivers (PostgreSQL, CockroachDB,
            DuckDB, Oracle, SQL Server, SQLite, IBM Db2, MySQL, MariaDB), set the
            sequence, identity column, or auto-increment counter behind each loaded
            table to follow the column's maximum value, or back to its initial
            minimum/seed value for an empty table, so subsequent generated values follow
            the loaded rows. On dialects where monotonic sequence resynchronization does
            not apply (Cloud Spanner, BigQuery), the option is skipped with a debug log.
        ignore_unknown_columns: When ``True`` and table column metadata is available,
            strip fixture columns not present in the target table before inserting.
        exclude_update_columns: Column names (or per-table mapping of column names) to
            omit from the ``SET`` clause of upserts so existing values are preserved on
            conflict.

    Returns:
        Mapping of table name to the number of rows loaded, in load order.

    Raises:
        ValueError: If ``batch_size`` is below 1, ``conflict_keys`` is given for an
            unsupported dialect, a table or column name is not a plain SQL identifier,
            a table has both unsharded and sharded fixture files, mixed extensions across
            shards, duplicate shard indices, a row in a fixture file is empty, or a fixture
            file only holds generated columns.
        TypeError: If a fixture file does not hold a list of row objects, or
            ``conflict_keys`` / ``exclude_update_columns`` has an invalid type.
        FileNotFoundError: If a requested table has no fixture file.
    """
    dialect = driver.statement_config.dialect
    dialect_name = _dialect_name(dialect)
    family = _dialect_family(dialect_name)
    _validate_load_arguments(conflict_keys, batch_size, dialect_name, family, exclude_update_columns)
    table_files = _ordered_table_files(fixtures_path, tables, table_order)
    resync = _sequence_resync_enabled(resync_sequences, dialect_name, family)
    counts: dict[str, int] = {}
    for table, file_paths in table_files:
        columns: list[_TableColumn] | None = _table_columns_sync(driver, family, table) if resync else None
        total_loaded = 0
        for file_path in file_paths:
            rows = _table_fixture_rows(file_path, _read_table_fixture_text(file_path))
            if columns is None and rows:
                columns = _table_columns_sync(driver, family, table)
            loaded_count = _insert_table_rows_sync(
                driver,
                dialect,
                family,
                table,
                rows,
                columns or [],
                _conflict_keys_for(conflict_keys, table),
                _excluded_update_columns_for(exclude_update_columns, table),
                batch_size,
                ignore_unknown_columns=ignore_unknown_columns,
            )
            total_loaded += loaded_count
        if resync and columns:
            _resync_table_sequences_sync(driver, family, dialect_name, table, columns)
        counts[table] = total_loaded
    return counts


async def load_table_fixtures_async(
    driver: "AsyncDriverAdapterBase",
    fixtures_path: "str | Path",
    *,
    tables: "Sequence[str] | None" = None,
    table_order: "Sequence[str] | None" = None,
    conflict_keys: "Mapping[str, Sequence[str] | str] | None" = None,
    batch_size: int = 500,
    resync_sequences: bool = False,
    ignore_unknown_columns: bool = False,
    exclude_update_columns: "Sequence[str] | Mapping[str, Sequence[str] | str] | None" = None,
) -> "dict[str, int]":
    """Load per-table fixture files into database tables asynchronously.

    Each table reads from one or more files named after it: ``<table>.json`` holding a JSON
    array of row objects, or ``<table>.jsonl`` holding one row object per line, either
    optionally gzipped (``.gz``). Tables split into multiple shards (e.g. ``<table>.001.jsonl.gz``,
    ``<table>.002.jsonl.gz``) are loaded in ascending numeric shard order. Sparse row objects
    are normalized across the ordered union of column keys in the file, defaulting omitted keys
    to ``None``. Table and column names are quoted, so they must match the database spelling
    exactly. Rows are inserted in batches with ``execute_many``; each file is read into memory in a
    worker thread. Transaction control stays with the caller.

    The target table's columns are read from the driver's data dictionary first, and
    JSON values of these column types are converted: ISO 8601 strings in timestamp,
    datetime, date (datetimes on Oracle, whose DATE carries a time), and (except on
    MySQL) time columns to datetimes, dates, and times; ISO 8601 durations without year
    or month parts and numbers of seconds in interval columns to ``timedelta``, and
    DuckDB ``[months, days, nanoseconds]`` interval lists to interval text; strings and
    numbers in numeric and decimal columns to ``Decimal``; strings in uuid columns to
    ``UUID``; base64 strings in bytea, blob, raw, image, binary, and varbinary columns
    to bytes; and PostgreSQL, MySQL, DuckDB, and SQLite json and jsonb values to JSON
    text. Vector column values are converted according to the driver family and data
    type. Values of generated (computed) columns are ignored, while identity column
    values are loaded. On PostgreSQL, ``GENERATED ALWAYS`` identity columns receive
    the loaded values through ``OVERRIDING SYSTEM VALUE`` and are never updated by upserts;
    on SQL Server, inserts into identity columns run between ``SET IDENTITY_INSERT ... ON``
    and ``OFF``. Other databases receive identity values as given and reject them for
    identity columns that only allow generated values.

    Args:
        driver: Async driver that runs the inserts.
        fixtures_path: Directory containing the table fixture files.
        tables: Tables to load. Defaults to every table fixture file in the directory;
            files whose names are not table identifiers are skipped with a debug log, and
            a dot in a file name marks a schema-qualified table.
        table_order: Tables to load first, in this order. Remaining tables load
            afterwards in alphabetical order; names not being loaded are ignored.
        conflict_keys: Mapping of table name to a column name or sequence of key column
            names of a unique constraint; tables not being loaded are ignored. Rows for
            matched tables are upserted, updating every non-key, non-excluded column
            when a row with the same key already exists. Supported on PostgreSQL-family,
            SQLite, and DuckDB drivers (``ON CONFLICT``) and MySQL
            (``ON DUPLICATE KEY UPDATE``, which matches any unique key of the table).
        batch_size: Maximum number of rows per ``execute_many`` call.
        resync_sequences: On supported database drivers (PostgreSQL, CockroachDB,
            DuckDB, Oracle, SQL Server, SQLite, IBM Db2, MySQL, MariaDB), set the
            sequence, identity column, or auto-increment counter behind each loaded
            table to follow the column's maximum value, or back to its initial
            minimum/seed value for an empty table, so subsequent generated values follow
            the loaded rows. On dialects where monotonic sequence resynchronization does
            not apply (Cloud Spanner, BigQuery), the option is skipped with a debug log.
        ignore_unknown_columns: When ``True`` and table column metadata is available,
            strip fixture columns not present in the target table before inserting.
        exclude_update_columns: Column names (or per-table mapping of column names) to
            omit from the ``SET`` clause of upserts so existing values are preserved on
            conflict.

    Returns:
        Mapping of table name to the number of rows loaded, in load order.

    Raises:
        ValueError: If ``batch_size`` is below 1, ``conflict_keys`` is given for an
            unsupported dialect, a table or column name is not a plain SQL identifier,
            a table has both unsharded and sharded fixture files, mixed extensions across
            shards, duplicate shard indices, a row in a fixture file is empty, or a fixture
            file only holds generated columns.
        TypeError: If a fixture file does not hold a list of row objects, or
            ``conflict_keys`` / ``exclude_update_columns`` has an invalid type.
        FileNotFoundError: If a requested table has no fixture file.
    """
    dialect = driver.statement_config.dialect
    dialect_name = _dialect_name(dialect)
    family = _dialect_family(dialect_name)
    _validate_load_arguments(conflict_keys, batch_size, dialect_name, family, exclude_update_columns)
    table_files = await _async_ordered_table_files(fixtures_path, tables, table_order)
    resync = _sequence_resync_enabled(resync_sequences, dialect_name, family)
    counts: dict[str, int] = {}
    for table, file_paths in table_files:
        columns: list[_TableColumn] | None = await _table_columns_async(driver, family, table) if resync else None
        total_loaded = 0
        for file_path in file_paths:
            rows = _table_fixture_rows(file_path, await _async_read_table_fixture_text(file_path))
            if columns is None and rows:
                columns = await _table_columns_async(driver, family, table)
            loaded_count = await _insert_table_rows_async(
                driver,
                dialect,
                family,
                table,
                rows,
                columns or [],
                _conflict_keys_for(conflict_keys, table),
                _excluded_update_columns_for(exclude_update_columns, table),
                batch_size,
                ignore_unknown_columns=ignore_unknown_columns,
            )
            total_loaded += loaded_count
        if resync and columns:
            await _resync_table_sequences_async(driver, family, dialect_name, table, columns)
        counts[table] = total_loaded
    return counts


def export_table_fixtures_sync(
    driver: "SyncDriverAdapterBase",
    fixtures_path: "str | Path",
    tables: "Sequence[str]",
    *,
    compress: bool = True,
    jsonl: bool = False,
    max_shard_bytes: "int | None" = None,
    max_rows_per_shard: "int | None" = None,
) -> "dict[str, int]":
    """Export database tables to per-table fixture files.

    Writes every row of each table to ``<table>.json`` (a JSON array) or, with
    ``jsonl``, ``<table>.jsonl`` (one JSON object per line), gzipped with a ``.gz``
    suffix when ``compress`` is set. When ``max_shard_bytes`` or ``max_rows_per_shard``
    is provided and the table exceeds the threshold, rows are split across numbered
    shards ``<table>.001<ext>``, ``<table>.002<ext>``, etc. Generated columns are left out.
    Rows are ordered by the primary key, or by every orderable column when the table has none,
    and each table is read into memory. Dates, times, and datetimes are written as ISO 8601
    strings, ``Decimal`` and ``UUID`` values as strings, bytes as base64 strings, and vector
    values as lists of numbers, which :func:`load_table_fixtures_sync` converts back for typed
    columns.

    Each file is written to a temporary file in the directory, created with the
    process umask, and then moved into place; after that, stale table fixture files for the
    same table in the directory are removed, so the next load reads the exported files.
    The directory is created when missing. Table names are quoted and must match the database
    spelling exactly.

    Args:
        driver: Sync driver that reads the tables.
        fixtures_path: Directory to write the fixture files to.
        tables: Tables to export.
        compress: Whether to gzip the written files.
        jsonl: Whether to write one JSON object per line instead of a JSON array.
        max_shard_bytes: Maximum uncompressed bytes per shard file before splitting.
        max_rows_per_shard: Maximum rows per shard file before splitting.

    Returns:
        Mapping of table name to the number of rows written.

    Raises:
        ValueError: If a table name is not a plain SQL identifier, or shard limits are invalid.
    """
    if max_shard_bytes is not None and max_shard_bytes <= 0:
        msg = f"max_shard_bytes must be greater than 0, got {max_shard_bytes}"
        raise ValueError(msg)
    if max_rows_per_shard is not None and max_rows_per_shard <= 0:
        msg = f"max_rows_per_shard must be greater than 0, got {max_rows_per_shard}"
        raise ValueError(msg)
    table_names = [_validated_table_name(table) for table in tables]
    dialect = driver.statement_config.dialect
    family = _dialect_family(_dialect_name(dialect))
    counts: dict[str, int] = {}
    for table in table_names:
        columns = _table_columns_sync(driver, family, table)
        rows = _without_generated_values(driver.select(_table_export_query(dialect, family, table, columns)), columns)
        _write_table_fixture(
            Path(fixtures_path),
            table,
            rows,
            compress,
            jsonl,
            max_shard_bytes=max_shard_bytes,
            max_rows_per_shard=max_rows_per_shard,
        )
        counts[table] = len(rows)
    return counts


async def export_table_fixtures_async(
    driver: "AsyncDriverAdapterBase",
    fixtures_path: "str | Path",
    tables: "Sequence[str]",
    *,
    compress: bool = True,
    jsonl: bool = False,
    max_shard_bytes: "int | None" = None,
    max_rows_per_shard: "int | None" = None,
) -> "dict[str, int]":
    """Export database tables to per-table fixture files asynchronously.

    Writes every row of each table to ``<table>.json`` (a JSON array) or, with
    ``jsonl``, ``<table>.jsonl`` (one JSON object per line), gzipped with a ``.gz``
    suffix when ``compress`` is set. When ``max_shard_bytes`` or ``max_rows_per_shard``
    is provided and the table exceeds the threshold, rows are split across numbered
    shards ``<table>.001<ext>``, ``<table>.002<ext>``, etc. Generated columns are left out.
    Rows are ordered by the primary key, or by every orderable column when the table has none,
    and each table is read into memory. Dates, times, and datetimes are written as ISO 8601
    strings, ``Decimal`` and ``UUID`` values as strings, bytes as base64 strings, and vector
    values as lists of numbers, which :func:`load_table_fixtures_async` converts back for typed
    columns.

    Each file is written to a temporary file in the directory, created with the
    process umask, and then moved into place; after that, stale table fixture files for the
    same table in the directory are removed, so the next load reads the exported files.
    The directory is created when missing, and file writes run in a worker thread.
    Table names are quoted and must match the database spelling exactly.

    Args:
        driver: Async driver that reads the tables.
        fixtures_path: Directory to write the fixture files to.
        tables: Tables to export.
        compress: Whether to gzip the written files.
        jsonl: Whether to write one JSON object per line instead of a JSON array.
        max_shard_bytes: Maximum uncompressed bytes per shard file before splitting.
        max_rows_per_shard: Maximum rows per shard file before splitting.

    Returns:
        Mapping of table name to the number of rows written.

    Raises:
        ValueError: If a table name is not a plain SQL identifier, or shard limits are invalid.
    """
    if max_shard_bytes is not None and max_shard_bytes <= 0:
        msg = f"max_shard_bytes must be greater than 0, got {max_shard_bytes}"
        raise ValueError(msg)
    if max_rows_per_shard is not None and max_rows_per_shard <= 0:
        msg = f"max_rows_per_shard must be greater than 0, got {max_rows_per_shard}"
        raise ValueError(msg)
    table_names = [_validated_table_name(table) for table in tables]
    dialect = driver.statement_config.dialect
    family = _dialect_family(_dialect_name(dialect))
    counts: dict[str, int] = {}
    for table in table_names:
        columns = await _table_columns_async(driver, family, table)
        rows = _without_generated_values(
            await driver.select(_table_export_query(dialect, family, table, columns)), columns
        )
        await _async_write_table_fixture(
            Path(fixtures_path), table, rows, compress, jsonl, max_shard_bytes, max_rows_per_shard
        )
        counts[table] = len(rows)
    return counts


class _TableColumn(NamedTuple):
    """Column metadata used to convert, insert, order, and resync table fixture rows.

    Attributes:
        name: Column name as spelled in the catalog.
        data_type: Lower-case catalog data type name.
        is_primary: Whether the column is part of the primary key.
        is_identity: Whether the column is an identity column.
        identity_always: Whether the catalog reports the identity as ``GENERATED ALWAYS``.
        sequence_name: Sequence owned by or defaulted on the column.
        is_generated: Whether the column is a computed column that never accepts values.
        identity_on_null: Whether the catalog reports the identity as ``GENERATED BY DEFAULT ON NULL``.
    """

    name: str
    data_type: str
    is_primary: bool
    is_identity: bool
    identity_always: bool
    sequence_name: "str | None"
    is_generated: bool
    identity_on_null: bool = False


def _sqlglot_dialect(dialect: "DialectType", family: str) -> "DialectType":
    """Return a SQLGlot-compatible dialect string or object for the driver family.

    SQLGlot does not recognize dialect aliases such as cockroach, cockroachdb,
    postgresql, mariadb, or mssql, so dialect families are mapped to standard
    SQLGlot identifiers.
    """
    if family in {"postgres", "mysql", "tsql"}:
        return family
    if family in {"sybase", "sqlanywhere"} and family not in Dialect.classes:
        importlib.import_module("sqlspec.dialects.sybase")
    return dialect


def _postgres_vector_kind(data_type: str) -> "str | None":
    """Return the vector kind if the PostgreSQL column is a pgvector type."""
    base = data_type.partition("(")[0].strip().lower()
    if base in {"vector", "halfvec", "sparsevec"}:
        return base
    return None


def _mysql_vector_kind(data_type: str) -> "str | None":
    """Return the vector kind if the MySQL column is a vector type."""
    base = data_type.partition("(")[0].strip().lower()
    if base == "vector":
        return "vector"
    return None


def _is_vector_column(data_type: str) -> bool:
    """Return whether a column data type represents a vector."""
    normalized = data_type.lower().strip()
    base = normalized.partition("(")[0].partition("[")[0].strip()
    if base in {"vector", "halfvec", "sparsevec", "f32_blob", "float32_blob"}:
        return True
    if base in {"float", "double"} and "[" in normalized:
        return True
    return normalized.startswith(("array<float32>", "array<float64>"))


def _truthy_metadata_flag(value: Any) -> bool:
    """Interpret a catalog flag value (bool, number, or yes/true/1 text) as a boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y", "always"}
    return bool(value)


def _read_text_sync(path: "Path") -> str:
    return path.read_text(encoding="utf-8")


def _resolve_path_str(path: str) -> str:
    return str(Path(path).resolve())


def _compress_text(content: str) -> bytes:
    return gzip.compress(content.encode("utf-8"))


def _read_zip_member(zf: zipfile.ZipFile, member_name: str) -> str:
    """Read and decode a single JSON member from a ZIP archive with size bounds checking."""
    info = zf.getinfo(member_name)
    if info.file_size > _MAX_ZIP_FIXTURE_BYTES:
        msg = f"ZIP fixture member {member_name!r} exceeds maximum allowed size ({_MAX_ZIP_FIXTURE_BYTES} bytes)"
        raise ValueError(msg)
    with zf.open(member_name) as member_file:
        data = member_file.read(_MAX_ZIP_FIXTURE_BYTES + 1)
    if len(data) > _MAX_ZIP_FIXTURE_BYTES:
        msg = f"ZIP fixture member {member_name!r} exceeds maximum allowed size ({_MAX_ZIP_FIXTURE_BYTES} bytes)"
        raise ValueError(msg)
    return data.decode("utf-8")


def _read_compressed_file(file_path: Path) -> str:
    """Read and decompress a file based on its extension.

    Args:
        file_path: Path to the file to read

    Returns:
        The decompressed file content as a string

    Raises:
        ValueError: If the file format is not supported
    """
    if file_path.suffix == ".gz":
        with gzip.open(file_path, mode="rt", encoding="utf-8") as f:
            return f.read()
    elif file_path.suffix == ".zip":
        with zipfile.ZipFile(file_path, "r") as zf:
            json_name = file_path.stem + ".json"
            if json_name in zf.namelist():
                return _read_zip_member(zf, json_name)
            json_files = [name for name in zf.namelist() if name.endswith(".json")]
            if json_files:
                return _read_zip_member(zf, json_files[0])
            msg = f"No JSON file found in ZIP archive: {file_path}"
            raise ValueError(msg)
    else:
        msg = f"Unsupported compression format: {file_path.suffix}"
        raise ValueError(msg)


def _find_fixture_file(fixtures_path: Any, fixture_name: str) -> Path:
    """Find a fixture file with various extensions.

    Args:
        fixtures_path: The path to look for fixtures
        fixture_name: The fixture name to load

    Returns:
        Path to the found fixture file

    Raises:
        FileNotFoundError: If no fixture file is found
        ValueError: If fixture_name contains path traversal segments
    """
    validated_name = _validated_fixture_name(fixture_name)
    base_path = Path(fixtures_path).resolve()

    for extension in [".json", ".json.gz", ".json.zip"]:
        fixture_path = (base_path / f"{validated_name}{extension}").resolve()
        if not fixture_path.is_relative_to(base_path):
            msg = f"Invalid fixture name: {fixture_name!r}"
            raise ValueError(msg)
        if fixture_path.exists():
            return fixture_path

    msg = f"Could not find the {fixture_name} fixture"
    raise FileNotFoundError(msg)


def _serialize_data(data: Any) -> str:
    """Serialize data to JSON string, handling different input types.

    Args:
        data: Data to serialize. Can be dict, list, or SQLSpec model types

    Returns:
        JSON string representation of the data
    """
    if isinstance(data, (list, tuple)):
        serialized_items: list[Any] = []

        for item in data:
            if isinstance(item, (str, int, float, bool, type(None))):
                serialized_items.append(item)
            else:
                serialized_items.append(schema_dump(item))

        return encode_json(serialized_items)
    if isinstance(data, (str, int, float, bool, type(None))):
        return encode_json(data)

    return encode_json(schema_dump(data))


def _validated_fixture_name(name: str) -> str:
    """Return the fixture name when it is a safe single-segment file stem.

    Raises:
        ValueError: If the name is empty or contains path separators or traversal segments.
    """
    candidate = Path(name)
    if (
        not name
        or candidate.is_absolute()
        or len(candidate.parts) != 1
        or name in {".", ".."}
        or "/" in name
        or "\\" in name
    ):
        msg = f"Invalid fixture name: {name!r}"
        raise ValueError(msg)
    return name


def _validated_table_name(name: str) -> str:
    """Return the table name when it is a plain or schema-qualified SQL identifier.

    Raises:
        ValueError: If the name is not a safe identifier.
    """
    if not _TABLE_NAME_PATTERN.fullmatch(name):
        msg = f"Invalid table name for table fixtures: {name!r}"
        raise ValueError(msg)
    return name


def _validated_column_name(name: str) -> str:
    """Return the column name when it is a plain SQL identifier.

    Raises:
        ValueError: If the name is not a safe identifier.
    """
    if not _COLUMN_NAME_PATTERN.fullmatch(name):
        msg = f"Invalid column name for table fixtures: {name!r}"
        raise ValueError(msg)
    return name


def _dialect_name(dialect: "DialectType") -> str:
    """Return the lower-case name of a dialect, or an empty string when unset."""
    if dialect is None:
        return ""
    if isinstance(dialect, str):
        return dialect.lower()
    if isinstance(dialect, type):
        return dialect.__name__.lower()
    return type(dialect).__name__.lower()


def _dialect_family(dialect_name: str) -> str:
    """Return canonical dialect family for a dialect name."""
    if dialect_name in _POSTGRES_DIALECTS:
        return "postgres"
    if dialect_name in _MYSQL_DIALECTS:
        return "mysql"
    if dialect_name == "ase":
        return "sybase"
    if dialect_name == "sa":
        return "sqlanywhere"
    return dialect_name


def _normalize_column_sequence(columns: "Sequence[str] | str", parameter_name: str) -> "tuple[str, ...]":
    """Normalize a single column name or sequence of column names to a validated non-empty tuple."""
    if isinstance(columns, (bytes, bytearray)):
        msg = f"{parameter_name} must be a column name or sequence of column names, not bytes"
        raise TypeError(msg)
    items = (columns,) if isinstance(columns, str) else tuple(columns)
    if not items:
        msg = f"{parameter_name} must not be empty"
        raise ValueError(msg)
    return tuple(_validated_column_name(item) for item in items)


def _validate_load_arguments(
    conflict_keys: "Mapping[str, Sequence[str] | str] | None",
    batch_size: int,
    dialect_name: str,
    family: str,
    exclude_update_columns: "Sequence[str] | Mapping[str, Sequence[str] | str] | None" = None,
) -> None:
    """Validate the batch size, conflict keys, and excluded update columns of a table fixture load.

    Raises:
        ValueError: If ``batch_size`` is below 1, conflict keys are unsupported for the
            dialect, or a table or column name is unsafe or empty.
        TypeError: If ``conflict_keys`` or ``exclude_update_columns`` has an invalid type.
    """
    if batch_size < 1:
        msg = f"batch_size must be at least 1, got {batch_size}"
        raise ValueError(msg)
    if exclude_update_columns is not None:
        if isinstance(exclude_update_columns, (str, bytes, bytearray)):
            msg = (
                "exclude_update_columns must be a sequence of column names or a mapping of table names to column names"
            )
            raise TypeError(msg)
        if isinstance(exclude_update_columns, Mapping):
            for table, cols in exclude_update_columns.items():
                _validated_table_name(table)
                if cols or isinstance(cols, (str, bytes, bytearray)):
                    _normalize_column_sequence(cols, f"exclude_update_columns[{table!r}]")
        else:
            for column in exclude_update_columns:
                if not isinstance(column, str):
                    msg = "exclude_update_columns items must be column name strings"
                    raise TypeError(msg)
                _validated_column_name(column)
    if not conflict_keys:
        return
    if family not in _ON_CONFLICT_FAMILIES and family != "mysql":
        msg = (
            f"conflict_keys is not supported for dialect {dialect_name!r}; "
            "supported dialects are PostgreSQL-family, SQLite, DuckDB, and MySQL"
        )
        raise ValueError(msg)
    for table, keys in conflict_keys.items():
        _validated_table_name(table)
        _normalize_column_sequence(keys, f"conflict_keys[{table!r}]")


def _conflict_keys_for(conflict_keys: "Mapping[str, Sequence[str] | str] | None", table: str) -> "tuple[str, ...]":
    """Return the conflict key columns configured for a table."""
    if not conflict_keys or table not in conflict_keys:
        return ()
    return _normalize_column_sequence(conflict_keys[table], f"conflict_keys[{table!r}]")


def _excluded_update_columns_for(
    exclude_update_columns: "Sequence[str] | Mapping[str, Sequence[str] | str] | None", table: str
) -> "set[str]":
    """Return the column names excluded from upsert updates for a table."""
    if not exclude_update_columns:
        return set()
    if isinstance(exclude_update_columns, Mapping):
        raw = exclude_update_columns.get(table)
        if not raw:
            return set()
        return set(_normalize_column_sequence(raw, f"exclude_update_columns[{table!r}]"))
    return set(exclude_update_columns)


def _parse_table_fixture_filename(file_name: str) -> "tuple[str, int | None, str] | None":
    """Parse a table fixture file name into (table, shard_index, extension).

    Returns ``None`` if the file does not have a recognized table fixture extension.
    For sharded files like ``users.001.jsonl.gz``, returns ``("users", 1, ".jsonl.gz")``.
    For unsharded files like ``users.jsonl.gz``, returns ``("users", None, ".jsonl.gz")``.
    """
    for extension in _TABLE_FIXTURE_EXTENSIONS:
        if file_name.endswith(extension):
            stem = file_name[: -len(extension)]
            match = _SHARD_STEM_PATTERN.fullmatch(stem)
            if match is not None:
                table, digits = match.groups()
                return table, int(digits), extension
            return stem, None, extension
    return None


def _resolve_table_fixture_paths(table: str, entries: "list[tuple[int | None, str, Path]]") -> "tuple[Path, ...]":
    """Validate and return ordered fixture file paths for a table.

    Raises:
        FileNotFoundError: If no fixture files exist for the table.
        ValueError: If unsharded and sharded files are mixed, multiple unsharded files exist,
            extensions differ across shards, or duplicate shard indices are present.
    """
    if not entries:
        msg = f"Could not find the {table} fixture"
        raise FileNotFoundError(msg)
    unsharded = [entry for entry in entries if entry[0] is None]
    sharded = [entry for entry in entries if entry[0] is not None]
    if unsharded and sharded:
        names = ", ".join(path.name for _, _, path in entries)
        msg = f"Table {table!r} has both unsharded and sharded fixture files: {names}"
        raise ValueError(msg)
    if unsharded:
        if len(unsharded) > 1:
            names = ", ".join(path.name for _, _, path in unsharded)
            msg = f"Table {table!r} has more than one fixture file: {names}"
            raise ValueError(msg)
        return (unsharded[0][2],)
    extensions = {ext for _, ext, _ in sharded}
    if len(extensions) > 1:
        names = ", ".join(path.name for _, _, path in sharded)
        msg = f"Table {table!r} has mixed fixture extensions across shards: {names}"
        raise ValueError(msg)
    seen_indices: set[int] = set()
    for idx, _, _path in sharded:
        if idx is not None and idx in seen_indices:
            names = ", ".join(p.name for _, _, p in sharded)
            msg = f"Table {table!r} has duplicate shard index {idx}: {names}"
            raise ValueError(msg)
        if idx is not None:
            seen_indices.add(idx)
    sorted_shards = sorted(sharded, key=lambda item: item[0] or 0)
    return tuple(path for _, _, path in sorted_shards)


def _discover_table_files(fixtures_path: "str | Path") -> "dict[str, tuple[Path, ...]]":
    """Map each table with fixture files in the directory to its ordered file paths.

    Raises:
        ValueError: If a table has both unsharded and sharded fixture files, mixed
            extensions across shards, or duplicate shard indices.
    """
    table_entries: dict[str, list[tuple[int | None, str, Path]]] = {}
    for entry in sorted(Path(fixtures_path).iterdir()):
        if not entry.is_file():
            continue
        parsed = _parse_table_fixture_filename(entry.name)
        if parsed is None:
            continue
        table, shard_idx, ext = parsed
        if not _TABLE_NAME_PATTERN.fullmatch(table):
            logger.debug("Skipping table fixture file %s: %r is not a table identifier", entry.name, table)
            continue
        if "." in table:
            logger.debug("Table fixture file %s names schema-qualified table %r", entry.name, table)
        table_entries.setdefault(table, []).append((shard_idx, ext, entry))
    return {table: _resolve_table_fixture_paths(table, entries) for table, entries in table_entries.items()}


def _ordered_table_files(
    fixtures_path: "str | Path", tables: "Sequence[str] | None", table_order: "Sequence[str] | None"
) -> "list[tuple[str, tuple[Path, ...]]]":
    """Resolve the tables to load and their fixture files in load order.

    Raises:
        ValueError: If a table name is not a safe identifier or a table has invalid fixture files.
        FileNotFoundError: If a requested table has no fixture file.
    """
    if tables is None:
        table_files = _discover_table_files(fixtures_path)
    else:
        for table in tables:
            _validated_table_name(table)
        all_files = _discover_table_files(fixtures_path)
        table_files = {}
        for table in tables:
            if table not in all_files:
                msg = f"Could not find the {table} fixture"
                raise FileNotFoundError(msg)
            table_files[table] = all_files[table]
    ordered = [table for table in dict.fromkeys(table_order or ()) if table in table_files]
    listed = set(ordered)
    ordered.extend(sorted(table for table in table_files if table not in listed))
    return [(table, table_files[table]) for table in ordered]


def _read_table_fixture_text(file_path: Path) -> str:
    """Read a table fixture file, decompressing gzipped files."""
    if file_path.suffix == ".gz":
        return _read_compressed_file(file_path)
    return file_path.read_text(encoding="utf-8")


def _table_fixture_rows(file_path: Path, content: str) -> "list[dict[str, Any]]":
    """Decode the row objects of a table fixture file and normalize sparse columns.

    Raises:
        TypeError: If the content is not a list of row objects.
        ValueError: If any row is empty or has an unsafe column name.
    """
    decoded: Any
    if file_path.name.endswith((".jsonl", ".jsonl.gz")):
        decoded = [decode_json(line) for line in content.splitlines() if line.strip()]
    else:
        decoded = decode_json(content)
        if not isinstance(decoded, list):
            msg = f"Table fixture {file_path} must contain a JSON array of row objects"
            raise TypeError(msg)
    raw_rows: list[dict[str, Any]] = []
    ordered_columns: list[str] = []
    seen_columns: set[str] = set()
    for index, row in enumerate(decoded):
        if not isinstance(row, dict):
            msg = f"Row {index} in table fixture {file_path} must be an object"
            raise TypeError(msg)
        if not row:
            msg = f"Row {index} in table fixture {file_path} must have at least one column"
            raise ValueError(msg)
        for column in row:
            if column not in seen_columns:
                _validated_column_name(column)
                seen_columns.add(column)
                ordered_columns.append(column)
        raw_rows.append(row)
    return [{column: raw_row.get(column) for column in ordered_columns} for raw_row in raw_rows]


def _quoted_identifier(name: str) -> exp.Identifier:
    """Return a quoted identifier expression for a name."""
    return exp.to_identifier(name, quoted=True)


def _quoted_identifier_sql(name: str) -> str:
    """Return the name as double-quoted SQL identifier text."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _quoted_table_name(table: str) -> str:
    """Return a validated plain or schema-qualified table name with each part double-quoted."""
    return ".".join(_quoted_identifier_sql(part) for part in table.split("."))


def _quoted_table(table: str) -> exp.Table:
    """Return a table expression with each part of a plain or schema-qualified name quoted."""
    schema_name, _, table_name = table.rpartition(".")
    return exp.Table(this=_quoted_identifier(table_name), db=_quoted_identifier(schema_name) if schema_name else None)


def _validated_sequence_identifier(name: str) -> str:
    """Return a plain or schema-qualified sequence identifier stripped of dialect quotes.

    Raises:
        ValueError: If the sequence name is not a safe SQL identifier.
    """
    cleaned = ".".join(part.strip('`"[] ') for part in name.strip().split("."))
    if not _SEQUENCE_IDENTIFIER_PATTERN.fullmatch(cleaned):
        msg = f"Invalid sequence name for table fixtures: {name!r}"
        raise ValueError(msg)
    return cleaned


def _extract_sequence_from_default(column_default: Any) -> "str | None":
    """Extract a backing sequence name from a column ``DEFAULT`` expression if present.

    Recognizes ``nextval('seq'::regclass)`` / ``nextval('seq')`` / ``nextval(`seq`)``
    (PostgreSQL, CockroachDB, DuckDB, MariaDB), ``NEXT VALUE FOR seq`` / ``NEXTVAL FOR seq``
    (SQL Server, IBM Db2, MariaDB), and ``seq.NEXTVAL`` (Oracle).
    """
    if not isinstance(column_default, str) or not column_default.strip():
        return None
    text = column_default.strip()
    nextval_match = _NEXTVAL_CALL_PATTERN.search(text)
    if nextval_match is not None:
        single_quoted, identifier_expr = nextval_match.groups()
        if single_quoted is not None:
            return single_quoted
        if identifier_expr is not None:
            cleaned = ".".join(part.strip("` ") for part in identifier_expr.split("."))
            if _SEQUENCE_IDENTIFIER_PATTERN.fullmatch(cleaned):
                return cleaned
    next_value_for_match = _NEXT_VALUE_FOR_PATTERN.search(text)
    if next_value_for_match is not None:
        raw_seq = next_value_for_match.group(1)
        cleaned = ".".join(part.strip('`"[] ') for part in raw_seq.split("."))
        if _SEQUENCE_IDENTIFIER_PATTERN.fullmatch(cleaned):
            return cleaned
    oracle_match = _ORACLE_NEXTVAL_PATTERN.match(text)
    if oracle_match is not None:
        raw_seq = oracle_match.group(1)
        cleaned = ".".join(part.strip('" ') for part in raw_seq.split("."))
        if _SEQUENCE_IDENTIFIER_PATTERN.fullmatch(cleaned):
            return cleaned
    return None


def _table_column(row: "Mapping[str, Any]") -> _TableColumn:
    """Return column metadata from a data dictionary column row.

    Identity columns are recognized from canonical ``is_identity`` (boolean, numeric,
    or truthy string flag), ``identity_generation`` (such as ``ALWAYS``, ``BY DEFAULT``,
    or ``BY DEFAULT ON NULL``), or ``auto_increment`` in ``extra`` (MySQL, MariaDB).
    Standalone sequence defaults are recognized from ``sequence_name`` or parsed from
    ``column_default``. An identity column is never treated as generated, because Db2
    also flags identity columns in ``is_generated``.
    """
    sequence_name = row.get("sequence_name")
    if sequence_name is None:
        sequence_name = _extract_sequence_from_default(row.get("column_default"))
    identity_generation = str(row.get("identity_generation") or "").strip().lower()
    extra = str(row.get("extra") or "").lower()
    is_identity = (
        identity_generation in _IDENTITY_GENERATION_VALUES
        or _truthy_metadata_flag(row.get("is_identity"))
        or "auto_increment" in extra
    )
    identity_always = identity_generation in _ALWAYS_IDENTITY_GENERATION_VALUES
    identity_on_null = identity_generation == "by default on null"
    return _TableColumn(
        name=str(row["column_name"]),
        data_type=str(row.get("data_type") or "").lower(),
        is_primary=_truthy_metadata_flag(row.get("is_primary")),
        is_identity=is_identity,
        identity_always=identity_always,
        sequence_name=None if sequence_name is None else str(sequence_name),
        is_generated=not is_identity and _is_generated_column(row),
        identity_on_null=identity_on_null,
    )


def _is_generated_column(row: "Mapping[str, Any]") -> bool:
    """Return whether a data dictionary column row describes a generated (computed) column.

    Recognized from canonical ``is_generated`` or ``generation_expression`` metadata,
    or dialect-specific ``extra`` flags containing the word ``generated``.
    """
    if _truthy_metadata_flag(row.get("is_generated")) or bool(row.get("generation_expression")):
        return True
    return "generated" in str(row.get("extra") or "").lower().split()


def _column_lookup_names(family: str, table: str) -> "tuple[str, str | None]":
    """Return the table and schema names to pass to the data dictionary for an exact-case table."""
    schema_name, _, table_name = table.rpartition(".")
    if family == "postgres":
        return _quoted_identifier_sql(table_name), _quoted_identifier_sql(schema_name) if schema_name else None
    return table_name, schema_name or None


def _table_columns_sync(driver: "SyncDriverAdapterBase", family: str, table: str) -> "list[_TableColumn]":
    """Read the column metadata of a table from the driver's data dictionary.

    An unqualified PostgreSQL-family table with no rows is looked up again in
    ``current_schema()``, because the data dictionary resolves unqualified names
    through the search path.
    """
    table_name, schema_name = _column_lookup_names(family, table)
    rows = driver.data_dictionary.get_columns(driver, table=table_name, schema=schema_name)
    if not rows and family == "postgres" and schema_name is None:
        active_schema = driver.select_value_or_none("SELECT current_schema()")
        if active_schema:
            rows = driver.data_dictionary.get_columns(
                driver, table=table_name, schema=_quoted_identifier_sql(str(active_schema))
            )
    return [_table_column(row) for row in rows]


async def _table_columns_async(driver: "AsyncDriverAdapterBase", family: str, table: str) -> "list[_TableColumn]":
    """Read the column metadata of a table from the driver's data dictionary.

    An unqualified PostgreSQL-family table with no rows is looked up again in
    ``current_schema()``, because the data dictionary resolves unqualified names
    through the search path.
    """
    table_name, schema_name = _column_lookup_names(family, table)
    rows = await driver.data_dictionary.get_columns(driver, table=table_name, schema=schema_name)
    if not rows and family == "postgres" and schema_name is None:
        active_schema = await driver.select_value_or_none("SELECT current_schema()")
        if active_schema:
            rows = await driver.data_dictionary.get_columns(
                driver, table=table_name, schema=_quoted_identifier_sql(str(active_schema))
            )
    return [_table_column(row) for row in rows]


def _generated_column_names(columns: "list[_TableColumn]") -> "set[str]":
    """Return the names of the generated columns of a table."""
    return {column.name for column in columns if column.is_generated}


def _drop_generated_values(
    rows: "list[dict[str, Any]]", columns: "list[_TableColumn]", *, ignore_unknown_columns: bool = False
) -> None:
    """Remove generated columns and optionally unknown columns from fixture rows in place.

    Raises:
        ValueError: If only generated columns remain in the rows.
    """
    if not rows or not columns:
        return
    generated = _generated_column_names(columns)
    unknown: set[str] = set()
    if ignore_unknown_columns:
        known = {column.name for column in columns}
        unknown = set(rows[0]) - known
    to_drop = (generated & set(rows[0])) | unknown
    if not to_drop:
        return
    for row in rows:
        for name in to_drop:
            row.pop(name, None)
    if not rows[0]:
        if unknown:
            rows.clear()
            return
        msg = f"Table fixture rows only contain generated columns: {sorted(generated)}"
        raise ValueError(msg)


def _decode_exported_json_value(value: Any) -> Any:
    """Decode a JSON string returned by a driver on a JSON/JSONB column, falling back to the raw string."""
    if not isinstance(value, str):
        return value
    try:
        return decode_json(value)
    except (ValueError, TypeError):
        return value


def _decode_exported_vector_value(value: Any, data_type: str) -> Any:
    """Normalize vector representations to a JSON-serializable list of floats."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [float(x) for x in value]
    if isinstance(value, array.array):
        return [float(x) for x in value]
    if hasattr(value, "tolist"):
        return [float(x) for x in value.tolist()]
    if isinstance(value, str):
        v = value.strip()
        if v.startswith("[") and v.endswith("]"):
            inner = v[1:-1].strip()
            if not inner:
                return []
            return [float(x.strip()) for x in inner.split(",") if x.strip()]
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        b = bytes(value)
        if "f32" in data_type or "float32" in data_type:
            n = len(b) // _FLOAT32_BYTES
            return list(struct.unpack(f"<{n}f", b[: n * _FLOAT32_BYTES]))
        if len(b) >= _PG_VECTOR_HEADER_BYTES:
            dim, _ = struct.unpack(">HH", b[:_PG_VECTOR_HEADER_BYTES])
            if _PG_VECTOR_HEADER_BYTES + dim * _FLOAT32_BYTES == len(b):
                return list(struct.unpack(f">{dim}f", b[_PG_VECTOR_HEADER_BYTES:]))
    return value


def _without_generated_values(rows: "list[dict[str, Any]]", columns: "list[_TableColumn]") -> "list[dict[str, Any]]":
    """Return exported rows without generated columns and with string JSON/vector cells normalized."""
    generated = _generated_column_names(columns)
    json_columns = {column.name for column in columns if column.data_type in _JSON_TYPE_NAMES}
    vector_columns = {column.name: column.data_type for column in columns if _is_vector_column(column.data_type)}
    if not generated and not json_columns and not vector_columns:
        return rows
    result: list[dict[str, Any]] = []
    for row in rows:
        row_dict: dict[str, Any] = {}
        for key, value in row.items():
            if key in generated:
                continue
            if key in json_columns:
                row_dict[key] = _decode_exported_json_value(value)
            elif key in vector_columns:
                row_dict[key] = _decode_exported_vector_value(value, vector_columns[key])
            else:
                row_dict[key] = value
        result.append(row_dict)
    return result


def _decode_bytes(value: Any) -> Any:
    """Decode a base64 string to bytes."""
    return base64.b64decode(value, validate=True) if isinstance(value, str) else value


def _decode_datetime(value: Any) -> Any:
    """Decode an ISO 8601 string to a datetime."""
    return convert_iso_datetime(value) if isinstance(value, str) else value


def _decode_date(value: Any) -> Any:
    """Decode an ISO 8601 string to a date."""
    return convert_iso_date(value) if isinstance(value, str) else value


def _decode_time(value: Any) -> Any:
    """Decode an ISO 8601 string to a time."""
    if not isinstance(value, str):
        return value
    return time.fromisoformat(f"{value[:-1]}+00:00" if value.endswith("Z") else value)


def _decode_interval(value: Any) -> Any:
    """Decode an ISO 8601 duration string or a number of seconds to a timedelta."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=value)
    if not isinstance(value, str):
        return value
    match = _ISO_DURATION_PATTERN.fullmatch(value)
    if match is None:
        return value
    sign, weeks, days, hours, minutes, seconds = match.groups()
    if not (weeks or days or hours or minutes or seconds):
        return value
    whole_seconds, _, fraction = (seconds or "0").partition(".")
    duration = timedelta(
        weeks=float(weeks or 0),
        days=float(days or 0),
        hours=float(hours or 0),
        minutes=float(minutes or 0),
        seconds=int(whole_seconds),
        microseconds=round(float(f"0.{fraction or 0}") * 1_000_000),
    )
    return -duration if sign else duration


def _decode_duckdb_interval(value: Any) -> Any:
    """Decode a ``[months, days, nanoseconds]`` list to DuckDB interval text and other values to a timedelta."""
    if (
        isinstance(value, list)
        and len(value) == _DUCKDB_INTERVAL_PARTS
        and all(isinstance(part, int) and not isinstance(part, bool) for part in value)
    ):
        months, days, nanoseconds = value
        return f"{months} months {days} days {nanoseconds // _NANOSECONDS_PER_MICROSECOND} microseconds"
    return _decode_interval(value)


def _decode_decimal(value: Any) -> Any:
    """Decode a numeric string or JSON number to a Decimal."""
    if isinstance(value, str):
        return Decimal(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return Decimal(str(value))
    return value


def _decode_uuid(value: Any) -> Any:
    """Decode a UUID string to a UUID."""
    return convert_uuid(value) if isinstance(value, str) else value


def _decode_pg_vector(value: Any) -> Any:
    """Format vector values as PostgreSQL/pgvector string literal."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return f"[{','.join(str(float(x)) for x in value)}]"
    if hasattr(value, "tolist"):
        return f"[{','.join(str(float(x)) for x in value.tolist())}]"
    return str(value)


def _decode_mysql_vector(value: Any) -> Any:
    """Format vector values as MySQL string literal."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return f"[{','.join(str(float(x)) for x in value)}]"
    if hasattr(value, "tolist"):
        return f"[{','.join(str(float(x)) for x in value.tolist())}]"
    return str(value)


def _decode_oracle_vector(data_type: str) -> "Callable[[Any], Any]":
    """Return decoder converting vector values to array.array for Oracle."""
    typecode = "d" if "64" in data_type else "f"

    def _oracle_vector_converter(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, array.array):
            return value
        if hasattr(value, "indices") and hasattr(value, "values"):
            return value
        if isinstance(value, (list, tuple)):
            return array.array(typecode, [float(x) for x in value])
        if hasattr(value, "tolist"):
            return array.array(typecode, [float(x) for x in value.tolist()])
        if isinstance(value, str):
            v = value.strip()
            if v.startswith("[") and v.endswith("]"):
                items = [float(x.strip()) for x in v[1:-1].split(",") if x.strip()]
                return array.array(typecode, items)
        return value

    return _oracle_vector_converter


def _decode_spanner_vector(data_type: str) -> "Callable[[Any], Any]":
    """Return decoder binding Spanner vector arrays with proper type hints."""
    is_float32 = "32" in data_type or data_type in {"real[]", "float4[]"}

    def _spanner_vector_converter(value: Any) -> Any:
        if value is None:
            return None
        items: list[float]
        if isinstance(value, (list, tuple)):
            items = [float(x) for x in value]
        elif hasattr(value, "tolist"):
            items = [float(x) for x in value.tolist()]
        elif isinstance(value, str):
            v = value.strip()
            if v.startswith("[") and v.endswith("]"):
                items = [float(x.strip()) for x in v[1:-1].split(",") if x.strip()]
            else:
                items = []
        else:
            return value
        if is_float32:
            return TypedParameter(items, list, semantic_name="ARRAY<FLOAT32>")
        return items

    return _spanner_vector_converter


def _decode_sqlite_vector(data_type: str) -> "Callable[[Any], Any]":
    """Return decoder converting vector values for SQLite."""
    is_blob = "blob" in data_type or "f32" in data_type

    def _sqlite_vector_converter(value: Any) -> Any:
        if value is None:
            return None
        items: list[float]
        if isinstance(value, (list, tuple)):
            items = [float(x) for x in value]
        elif hasattr(value, "tolist"):
            items = [float(x) for x in value.tolist()]
        elif isinstance(value, str):
            v = value.strip()
            if v.startswith("[") and v.endswith("]"):
                items = [float(x.strip()) for x in v[1:-1].split(",") if x.strip()]
            else:
                return value
        elif isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        else:
            return value
        if is_blob:
            return struct.pack(f"<{len(items)}f", *items)
        return encode_json(items)

    return _sqlite_vector_converter


def _decode_generic_vector(value: Any) -> Any:
    """Normalize vector values to a list of floats."""
    if value is None:
        return None
    if isinstance(value, list):
        return [float(x) for x in value]
    if isinstance(value, tuple):
        return [float(x) for x in value]
    if hasattr(value, "tolist"):
        return [float(x) for x in value.tolist()]
    if isinstance(value, str):
        v = value.strip()
        if v.startswith("[") and v.endswith("]"):
            return [float(x.strip()) for x in v[1:-1].split(",") if x.strip()]
    return value


def _column_value_decoder(family: str, data_type: str) -> "Callable[[Any], Any] | None":
    """Return the converter from a JSON value to the driver value for a column type, if any.

    Oracle ``DATE`` columns store a time of day, so their values decode to datetimes.
    """
    normalized_type = data_type.lower()
    if _is_vector_column(normalized_type):
        if family == "postgres":
            return _decode_pg_vector
        if family == "mysql":
            return _decode_mysql_vector
        if family == "oracle":
            return _decode_oracle_vector(normalized_type)
        if family in {"spanner", "spangres"}:
            return _decode_spanner_vector(normalized_type)
        if family == "sqlite":
            return _decode_sqlite_vector(normalized_type)
        if family == "duckdb":
            return _decode_pg_vector
        return _decode_generic_vector
    if data_type.endswith("]"):
        return None
    if data_type in _BINARY_TYPES or data_type.startswith(("binary(", "varbinary(")):
        return _decode_bytes
    if data_type in _JSON_TYPE_NAMES:
        return encode_json if family in _JSON_VALUE_FAMILIES else None
    if family == "sqlite":
        return None
    if data_type.startswith(("timestamp", "datetime", "bigdatetime", "smalldatetime", "datetimetz")):
        return _decode_datetime
    if data_type == "date":
        return _decode_datetime if family == "oracle" else _decode_date
    if data_type.startswith(("time", "bigtime")) and family != "mysql":
        return _decode_time
    if data_type.startswith("interval"):
        return _decode_duckdb_interval if family == "duckdb" else _decode_interval
    if data_type.startswith(("numeric", "decimal", "money", "smallmoney")):
        return _decode_decimal
    if data_type in {"uuid", "uniqueidentifier"}:
        return _decode_uuid
    return None


def _decode_row_values(rows: "list[dict[str, Any]]", family: str, columns: "list[_TableColumn]") -> None:
    """Convert the JSON values of typed columns in place."""
    decoders: list[tuple[str, Callable[[Any], Any]]] = []
    for column in columns:
        decoder = _column_value_decoder(family, column.data_type)
        if decoder is not None:
            decoders.append((column.name, decoder))
    if not decoders:
        return
    for row in rows:
        for name, decoder in decoders:
            value = row.get(name)
            if value is not None:
                row[name] = decoder(value)


def _conflict_clause(
    family: str,
    columns: "list[str]",
    conflict_keys: "tuple[str, ...]",
    always_identity: "set[str]",
    excluded_updates: "set[str] | None" = None,
) -> exp.OnConflict:
    """Return the upsert clause that updates non-key, non-identity, non-excluded columns on a key conflict."""
    excluded = excluded_updates or set()
    updates = [
        column
        for column in columns
        if column not in conflict_keys and column not in always_identity and column not in excluded
    ]
    if family == "mysql":
        assignments = [
            exp.EQ(
                this=exp.column(_quoted_identifier(column)),
                expression=exp.Anonymous(this="VALUES", expressions=[exp.column(_quoted_identifier(column))]),
            )
            for column in updates or list(conflict_keys)
        ]
        return exp.OnConflict(duplicate=True, action=exp.var("UPDATE"), expressions=assignments)
    keys = [_quoted_identifier(key) for key in conflict_keys]
    if not updates:
        return exp.OnConflict(conflict_keys=keys, action=exp.var("DO NOTHING"))
    assignments = [
        exp.EQ(
            this=exp.column(_quoted_identifier(column)),
            expression=exp.column(_quoted_identifier(column), table=_quoted_identifier("excluded")),
        )
        for column in updates
    ]
    return exp.OnConflict(conflict_keys=keys, action=exp.var("DO UPDATE"), expressions=assignments)


def _table_insert_statement(
    dialect: "DialectType",
    family: str,
    table: str,
    columns: "list[str]",
    conflict_keys: "tuple[str, ...]",
    table_columns: "list[_TableColumn]",
    excluded_updates: "set[str] | None" = None,
) -> str:
    """Return INSERT SQL text with a named placeholder per quoted column, upserting on conflict keys.

    The statement is rendered directly from the expression in the target dialect so
    quoted catalog names keep their exact spelling. On PostgreSQL, an insert into
    ``GENERATED ALWAYS`` identity columns carries ``OVERRIDING SYSTEM VALUE``.
    """
    col_map = {col.name: col for col in table_columns}
    value_expressions: list[exp.Expr] = []
    for column in columns:
        col = col_map.get(column)
        ph = exp.Placeholder(this=column)
        if col is not None and family == "postgres" and _postgres_vector_kind(col.data_type):
            target_type = exp.DataType.build(col.data_type, dialect="postgres", udt=True)
            cast_text = exp.Cast(this=ph, to=exp.DataType(this=exp.DataType.Type.TEXT))
            value_expressions.append(exp.Cast(this=cast_text, to=target_type))
        elif col is not None and family == "mysql" and _mysql_vector_kind(col.data_type):
            dialect_str = _dialect_name(dialect)
            if dialect_str == "mariadb":
                value_expressions.append(exp.Anonymous(this="VEC_FromText", expressions=[ph]))
            else:
                value_expressions.append(exp.Anonymous(this="STRING_TO_VECTOR", expressions=[ph]))
        else:
            value_expressions.append(ph)

    insert_expression = exp.Insert(
        this=exp.Schema(this=_quoted_table(table), expressions=[_quoted_identifier(column) for column in columns]),
        expression=exp.Values(expressions=[exp.Tuple(expressions=value_expressions)]),
    )
    always_identity = {column.name for column in table_columns if column.identity_always}
    if conflict_keys:
        insert_expression.set(
            "conflict", _conflict_clause(family, columns, conflict_keys, always_identity, excluded_updates)
        )
    rendered_sql = insert_expression.sql(dialect=_sqlglot_dialect(dialect, family), copy=False)
    if family != "postgres" or always_identity.isdisjoint(columns):
        return rendered_sql
    return _INSERT_VALUES_CLAUSE_PATTERN.sub(") OVERRIDING SYSTEM VALUE VALUES (", rendered_sql, count=1)


def _identity_insert_statements(
    dialect: "DialectType", family: str, table: str, columns: "list[str]", table_columns: "list[_TableColumn]"
) -> "tuple[str, str] | None":
    """Return the SQL Server and Sybase ASE statements that allow and then disallow explicit identity values.

    SQL Server and Sybase ASE reject values for identity columns unless ``IDENTITY_INSERT`` is on for
    the table, so inserts that include an identity column run between these statements.
    Other dialects return ``None``.
    """
    inserted = set(columns)
    if family not in {"tsql", "sybase"} or not any(
        column.is_identity and column.name in inserted for column in table_columns
    ):
        return None
    table_sql = _quoted_table(table).sql(dialect=_sqlglot_dialect(dialect, family))
    return f"SET IDENTITY_INSERT {table_sql} ON", f"SET IDENTITY_INSERT {table_sql} OFF"


def _insert_table_rows_sync(
    driver: "SyncDriverAdapterBase",
    dialect: "DialectType",
    family: str,
    table: str,
    rows: "list[dict[str, Any]]",
    columns: "list[_TableColumn]",
    conflict_keys: "tuple[str, ...]",
    excluded_updates: "set[str]",
    batch_size: int,
    *,
    ignore_unknown_columns: bool = False,
) -> int:
    """Prepare and insert fixture rows for a single table synchronously, returning the loaded row count."""
    if not rows:
        return 0
    _drop_generated_values(rows, columns, ignore_unknown_columns=ignore_unknown_columns)
    if not rows:
        return 0
    _decode_row_values(rows, family, columns)
    inserted_columns = list(rows[0])
    statement = _table_insert_statement(
        dialect, family, table, inserted_columns, conflict_keys, columns, excluded_updates
    )
    identity_insert = _identity_insert_statements(dialect, family, table, inserted_columns, columns)
    if identity_insert is not None:
        driver.execute(identity_insert[0])
    try:
        for start in range(0, len(rows), batch_size):
            driver.execute_many(statement, rows[start : start + batch_size])
    finally:
        if identity_insert is not None:
            driver.execute(identity_insert[1])
    return len(rows)


async def _insert_table_rows_async(
    driver: "AsyncDriverAdapterBase",
    dialect: "DialectType",
    family: str,
    table: str,
    rows: "list[dict[str, Any]]",
    columns: "list[_TableColumn]",
    conflict_keys: "tuple[str, ...]",
    excluded_updates: "set[str]",
    batch_size: int,
    *,
    ignore_unknown_columns: bool = False,
) -> int:
    """Prepare and insert fixture rows for a single table asynchronously, returning the loaded row count."""
    if not rows:
        return 0
    _drop_generated_values(rows, columns, ignore_unknown_columns=ignore_unknown_columns)
    if not rows:
        return 0
    _decode_row_values(rows, family, columns)
    inserted_columns = list(rows[0])
    statement = _table_insert_statement(
        dialect, family, table, inserted_columns, conflict_keys, columns, excluded_updates
    )
    identity_insert = _identity_insert_statements(dialect, family, table, inserted_columns, columns)
    if identity_insert is not None:
        await driver.execute(identity_insert[0])
    try:
        for start in range(0, len(rows), batch_size):
            await driver.execute_many(statement, rows[start : start + batch_size])
    finally:
        if identity_insert is not None:
            await driver.execute(identity_insert[1])
    return len(rows)


def _is_orderable_type(family: str, data_type: str) -> bool:
    """Return whether a column type, or the element type of an array type, can be used in ORDER BY.

    PostgreSQL types without a default sort order are skipped on every dialect, and
    large object, XML, and spatial types are skipped on Oracle, Db2, and SQL Server,
    which reject them in ORDER BY.
    """
    base_type = data_type.partition("[")[0].partition("(")[0].strip().lower()
    if base_type in _UNORDERABLE_POSTGRES_TYPES:
        return False
    if base_type.startswith(("array<", "struct<")):
        return False
    family_types = _UNORDERABLE_FAMILY_TYPES.get(family)
    return family_types is None or base_type not in family_types


def _table_export_query(dialect: "DialectType", family: str, table: str, table_columns: "list[_TableColumn]") -> str:
    """Return SELECT SQL text for every row ordered by the primary key, every orderable column, or position 1.

    The statement is rendered directly from the expression in the target dialect so
    quoted catalog names keep their exact spelling.
    """
    order_columns = [column.name for column in table_columns if column.is_primary] or [
        column.name for column in table_columns if _is_orderable_type(family, column.data_type)
    ]
    order_by: list[exp.Expr] = [exp.Column(this=_quoted_identifier(name)) for name in order_columns]
    if not order_by and not table_columns:
        order_by = [exp.Literal.number(1)]
    projections: list[exp.Expr] = []
    if family in _JSON_VALUE_FAMILIES and any(column.data_type in _JSON_TYPE_NAMES for column in table_columns):
        for column in table_columns:
            expression = exp.Column(this=_quoted_identifier(column.name))
            if column.data_type in _JSON_TYPE_NAMES:
                projections.append(
                    exp.Alias(
                        this=exp.Cast(this=expression, to=exp.DataType(this=exp.DataType.Type.TEXT)),
                        alias=_quoted_identifier(column.name),
                    )
                )
            else:
                projections.append(expression)
    query = exp.Select().select(*(projections or [exp.Star()])).from_(_quoted_table(table))
    if order_by:
        query = query.order_by(*order_by)
    return query.sql(dialect=_sqlglot_dialect(dialect, family))


def _sequence_resync_enabled(resync_sequences: bool, dialect_name: str, family: str) -> bool:
    """Return whether sequence resync applies, logging when it is skipped for the dialect."""
    if not resync_sequences:
        return False
    if dialect_name in _SEQUENCE_RESYNC_DIALECTS or family in _SEQUENCE_RESYNC_DIALECTS:
        return True
    logger.debug("Skipping table fixture sequence resync for unsupported dialect %r", dialect_name or None)
    return False


def _sequence_resync_statements(
    table: str, table_columns: "list[_TableColumn]", dialect_name: str = "postgres", family: str = "postgres"
) -> "list[tuple[str, dict[str, Any]]]":
    """Return statements aligning each owned sequence or identity column of a table with its maximum value."""
    statements: list[tuple[str, dict[str, Any]]] = []

    if family == "postgres" and dialect_name not in {"cockroach", "cockroachdb"}:
        quoted_table = _quoted_table_name(table)
        for column in table_columns:
            quoted_column = _quoted_identifier_sql(column.name)
            if column.sequence_name is not None:
                statement = (
                    "WITH seq_info AS ("
                    "SELECT to_regclass(:sequence_name) AS relid, seqmin "
                    "FROM pg_catalog.pg_sequence "
                    "WHERE seqrelid = to_regclass(:sequence_name)"
                    "), table_max AS ("
                    f"SELECT max({quoted_column}) AS max_val FROM {quoted_table}"
                    ") "
                    "SELECT setval("
                    "s.relid, coalesce(m.max_val, s.seqmin), m.max_val IS NOT NULL"
                    ") FROM seq_info s CROSS JOIN table_max m"
                )
                statements.append((statement, {"sequence_name": column.sequence_name}))
            elif column.is_identity:
                statement = (
                    "WITH seq_info AS ("
                    "SELECT to_regclass(pg_catalog.pg_get_serial_sequence(:table_name, :column_name)) AS relid, s.seqmin "
                    "FROM pg_catalog.pg_sequence s "
                    "WHERE s.seqrelid = to_regclass(pg_catalog.pg_get_serial_sequence(:table_name, :column_name))"
                    "), table_max AS ("
                    f"SELECT max({quoted_column}) AS max_val FROM {quoted_table}"
                    ") "
                    "SELECT setval("
                    "s.relid, coalesce(m.max_val, s.seqmin), m.max_val IS NOT NULL"
                    ") FROM seq_info s CROSS JOIN table_max m"
                )
                statements.append((statement, {"table_name": quoted_table, "column_name": column.name}))
        return statements

    if dialect_name in {"cockroach", "cockroachdb"} or family in {"cockroach", "cockroachdb"}:
        for column in table_columns:
            if column.sequence_name is None:
                continue
            quoted_column = _quoted_identifier_sql(column.name)
            statement = (
                f"SELECT setval(:sequence_name, coalesce(max({quoted_column}), 1), "
                f"max({quoted_column}) IS NOT NULL) FROM {_quoted_table_name(table)}"
            )
            statements.append((statement, {"sequence_name": column.sequence_name}))
        return statements

    if dialect_name == "duckdb" or family == "duckdb":
        for column in table_columns:
            if column.sequence_name is None:
                continue
            quoted_column = _quoted_identifier_sql(column.name)
            sequence_schema, _, sequence_name = column.sequence_name.rpartition(".")
            if not sequence_schema:
                table_schema, _, _ = table.rpartition(".")
                sequence_schema = table_schema or "main"
            quoted_seq = f"{sequence_schema}.{sequence_name}" if sequence_schema else sequence_name
            statement = (
                f"WITH seq_info AS ("
                f"SELECT coalesce(last_value, start_value - increment_by) AS cur_val, increment_by "
                f"FROM duckdb_sequences() "
                f"WHERE schema_name = :sequence_schema AND sequence_name = :sequence_name"
                f"), "
                f"table_max AS ("
                f"SELECT coalesce(max({quoted_column}), 0) AS max_val FROM {_quoted_table_name(table)}"
                f"), "
                f"calc AS ("
                f"SELECT CASE "
                f"WHEN table_max.max_val > seq_info.cur_val AND seq_info.increment_by > 0 "
                f"THEN cast(ceil((table_max.max_val - seq_info.cur_val) * 1.0 / seq_info.increment_by) AS bigint) "
                f"ELSE 0 "
                f"END AS steps "
                f"FROM seq_info CROSS JOIN table_max"
                f") "
                f"SELECT count(nextval(:quoted_seq)) FROM calc, range(calc.steps)"
            )
            statements.append((
                statement,
                {"sequence_schema": sequence_schema, "sequence_name": sequence_name, "quoted_seq": quoted_seq},
            ))
        return statements

    if family == "oracle":
        for column in table_columns:
            if column.is_identity:
                quoted_column = _quoted_identifier_sql(column.name)
                if column.identity_always:
                    generation_clause = "GENERATED ALWAYS AS IDENTITY"
                elif column.identity_on_null:
                    generation_clause = "GENERATED BY DEFAULT ON NULL AS IDENTITY"
                else:
                    generation_clause = "GENERATED BY DEFAULT AS IDENTITY"
                statement = (
                    f"ALTER TABLE {_quoted_table_name(table)} MODIFY ("
                    f"{quoted_column} {generation_clause} (START WITH LIMIT VALUE))"
                )
                statements.append((statement, {}))
        return statements

    if family == "tsql":
        if any(column.is_identity for column in table_columns):
            table_sql = _quoted_table(table).sql(dialect="tsql")
            statement = f"DBCC CHECKIDENT ('{table_sql}', RESEED)"
            statements.append((statement, {}))
        return statements

    if family == "sqlite":
        primary_columns = [
            col
            for col in table_columns
            if col.is_primary and (col.is_identity or "int" in col.data_type or col.data_type == "")
        ]
        if primary_columns:
            quoted_column = _quoted_identifier_sql(primary_columns[0].name)
            table_base = table.rsplit(".", maxsplit=1)[-1]
            statement = (
                "WITH max_val AS ("
                f"SELECT coalesce(max({quoted_column}), 0) AS val "
                f"FROM {_quoted_table_name(table)}"
                ") "
                "UPDATE sqlite_sequence SET seq = max_val.val "
                "FROM max_val WHERE sqlite_sequence.name = :table_name"
            )
            statements.append((statement, {"table_name": table_base}))
        return statements

    return statements


def _resync_sybase_sequences_sync(driver: "SyncDriverAdapterBase", table: str, columns: "list[_TableColumn]") -> None:
    """Resynchronize Sybase ASE identity columns for a table synchronously."""
    for column in columns:
        if column.is_identity:
            quoted_col = _quoted_identifier_sql(column.name)
            query = f"SELECT coalesce(max({quoted_col}), 0) FROM {_quoted_table_name(table)}"
            max_val = driver.select_value(query)
            next_val = int(max_val or 0)
            if next_val > 0:
                escaped_table = table.replace("'", "''")
                driver.execute(f"EXEC sp_chgattribute '{escaped_table}', 'identity_burn_max', 0, '{next_val}'")


def _resync_sqlanywhere_sequences_sync(
    driver: "SyncDriverAdapterBase", table: str, columns: "list[_TableColumn]"
) -> None:
    """Resynchronize SAP SQL Anywhere identity columns for a table synchronously."""
    for column in columns:
        if column.is_identity:
            quoted_col = _quoted_identifier_sql(column.name)
            query = f"SELECT coalesce(max({quoted_col}), 0) FROM {_quoted_table_name(table)}"
            max_val = driver.select_value(query)
            next_val = int(max_val or 0)
            schema_part, _, table_part = table.rpartition(".")
            escaped_table = table_part.replace("'", "''")
            escaped_schema = schema_part.replace("'", "''")
            schema_sql = f"'{escaped_schema}'" if schema_part else "NULL"
            driver.execute(f"CALL sa_reset_identity('{escaped_table}', {schema_sql}, {next_val})")


def _resync_table_sequences_sync(
    driver: "SyncDriverAdapterBase", family: str, dialect_name: str, table: str, columns: "list[_TableColumn]"
) -> None:
    """Resynchronize sequences, identity columns, or auto-increment counters for a table synchronously."""
    if family == "sybase":
        _resync_sybase_sequences_sync(driver, table, columns)
        return

    if family == "sqlanywhere":
        _resync_sqlanywhere_sequences_sync(driver, table, columns)
        return

    if family == "db2":
        quoted_table = _quoted_table_name(table)
        for column in columns:
            if column.is_identity:
                quoted_col = _quoted_identifier_sql(column.name)
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = driver.select_value(query)
                next_val = int(max_val or 1)
                driver.execute(f"ALTER TABLE {quoted_table} ALTER COLUMN {quoted_col} RESTART WITH {next_val}")
            elif column.sequence_name is not None:
                quoted_col = _quoted_identifier_sql(column.name)
                quoted_seq = _quoted_table_name(_validated_sequence_identifier(column.sequence_name))
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = driver.select_value(query)
                next_val = int(max_val or 1)
                driver.execute(f"ALTER SEQUENCE {quoted_seq} RESTART WITH {next_val}")
        return

    if family == "oracle":
        for statement, parameters in _sequence_resync_statements(
            table, columns, dialect_name=dialect_name, family=family
        ):
            driver.execute(statement, parameters)
        quoted_table = _quoted_table_name(table)
        for column in columns:
            if not column.is_identity and column.sequence_name is not None:
                quoted_col = _quoted_identifier_sql(column.name)
                quoted_seq = _quoted_table_name(_validated_sequence_identifier(column.sequence_name))
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = driver.select_value(query)
                next_val = int(max_val or 1)
                driver.execute(f"ALTER SEQUENCE {quoted_seq} RESTART START WITH {next_val}")
        return

    if family == "tsql":
        for statement, parameters in _sequence_resync_statements(
            table, columns, dialect_name=dialect_name, family=family
        ):
            driver.execute(statement, parameters)
        quoted_table = _quoted_table(table).sql(dialect="tsql")
        for column in columns:
            if not column.is_identity and column.sequence_name is not None:
                quoted_col = exp.column(_quoted_identifier(column.name)).sql(dialect="tsql")
                quoted_seq = _quoted_table(_validated_sequence_identifier(column.sequence_name)).sql(dialect="tsql")
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = driver.select_value(query)
                next_val = int(max_val or 1)
                driver.execute(f"ALTER SEQUENCE {quoted_seq} RESTART WITH {next_val}")
        return

    if family == "mysql":
        quoted_table = _quoted_table(table).sql(dialect="mysql")
        for column in columns:
            if not column.is_identity and column.sequence_name is not None:
                quoted_col = exp.column(_quoted_identifier(column.name)).sql(dialect="mysql")
                quoted_seq = _quoted_table(_validated_sequence_identifier(column.sequence_name)).sql(dialect="mysql")
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = driver.select_value(query)
                next_val = int(max_val or 1)
                driver.execute(f"ALTER SEQUENCE {quoted_seq} RESTART WITH {next_val}")
        auto_columns = [
            col
            for col in columns
            if col.is_identity
            or (col.is_primary and col.sequence_name is None and ("int" in col.data_type or col.data_type == ""))
        ]
        if auto_columns:
            column = auto_columns[0]
            quoted_col = exp.column(_quoted_identifier(column.name)).sql(dialect="mysql")
            query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
            max_val = driver.select_value(query)
            next_val = int(max_val or 1)
            driver.execute(f"ALTER TABLE {quoted_table} AUTO_INCREMENT = {next_val}")
        return

    if family == "sqlite":
        for statement, parameters in _sequence_resync_statements(
            table, columns, dialect_name=dialect_name, family=family
        ):
            try:
                driver.execute(statement, parameters)
            except Exception as exc:
                if "no such table: sqlite_sequence" in str(exc).lower():
                    logger.debug("Skipping sqlite_sequence update: sqlite_sequence table does not exist")
                else:
                    raise
        return

    statements = _sequence_resync_statements(table, columns, dialect_name=dialect_name, family=family)
    if not statements:
        logger.debug("Skipping table fixture sequence resync for table %r: no sequence or identity columns", table)
        return
    for statement, parameters in statements:
        driver.execute(statement, parameters)


async def _resync_sybase_sequences_async(
    driver: "AsyncDriverAdapterBase", table: str, columns: "list[_TableColumn]"
) -> None:
    """Resynchronize Sybase ASE identity columns for a table asynchronously."""
    for column in columns:
        if column.is_identity:
            quoted_col = _quoted_identifier_sql(column.name)
            query = f"SELECT coalesce(max({quoted_col}), 0) FROM {_quoted_table_name(table)}"
            max_val = await driver.select_value(query)
            next_val = int(max_val or 0)
            if next_val > 0:
                escaped_table = table.replace("'", "''")
                await driver.execute(f"EXEC sp_chgattribute '{escaped_table}', 'identity_burn_max', 0, '{next_val}'")


async def _resync_sqlanywhere_sequences_async(
    driver: "AsyncDriverAdapterBase", table: str, columns: "list[_TableColumn]"
) -> None:
    """Resynchronize SAP SQL Anywhere identity columns for a table asynchronously."""
    for column in columns:
        if column.is_identity:
            quoted_col = _quoted_identifier_sql(column.name)
            query = f"SELECT coalesce(max({quoted_col}), 0) FROM {_quoted_table_name(table)}"
            max_val = await driver.select_value(query)
            next_val = int(max_val or 0)
            schema_part, _, table_part = table.rpartition(".")
            escaped_table = table_part.replace("'", "''")
            escaped_schema = schema_part.replace("'", "''")
            schema_sql = f"'{escaped_schema}'" if schema_part else "NULL"
            await driver.execute(f"CALL sa_reset_identity('{escaped_table}', {schema_sql}, {next_val})")


async def _resync_table_sequences_async(
    driver: "AsyncDriverAdapterBase", family: str, dialect_name: str, table: str, columns: "list[_TableColumn]"
) -> None:
    """Resynchronize sequences, identity columns, or auto-increment counters for a table asynchronously."""
    if family == "sybase":
        await _resync_sybase_sequences_async(driver, table, columns)
        return

    if family == "sqlanywhere":
        await _resync_sqlanywhere_sequences_async(driver, table, columns)
        return

    if family == "db2":
        quoted_table = _quoted_table_name(table)
        for column in columns:
            if column.is_identity:
                quoted_col = _quoted_identifier_sql(column.name)
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = await driver.select_value(query)
                next_val = int(max_val or 1)
                await driver.execute(f"ALTER TABLE {quoted_table} ALTER COLUMN {quoted_col} RESTART WITH {next_val}")
            elif column.sequence_name is not None:
                quoted_col = _quoted_identifier_sql(column.name)
                quoted_seq = _quoted_table_name(_validated_sequence_identifier(column.sequence_name))
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = await driver.select_value(query)
                next_val = int(max_val or 1)
                await driver.execute(f"ALTER SEQUENCE {quoted_seq} RESTART WITH {next_val}")
        return

    if family == "oracle":
        for statement, parameters in _sequence_resync_statements(
            table, columns, dialect_name=dialect_name, family=family
        ):
            await driver.execute(statement, parameters)
        quoted_table = _quoted_table_name(table)
        for column in columns:
            if not column.is_identity and column.sequence_name is not None:
                quoted_col = _quoted_identifier_sql(column.name)
                quoted_seq = _quoted_table_name(_validated_sequence_identifier(column.sequence_name))
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = await driver.select_value(query)
                next_val = int(max_val or 1)
                await driver.execute(f"ALTER SEQUENCE {quoted_seq} RESTART START WITH {next_val}")
        return

    if family == "tsql":
        for statement, parameters in _sequence_resync_statements(
            table, columns, dialect_name=dialect_name, family=family
        ):
            await driver.execute(statement, parameters)
        quoted_table = _quoted_table(table).sql(dialect="tsql")
        for column in columns:
            if not column.is_identity and column.sequence_name is not None:
                quoted_col = exp.column(_quoted_identifier(column.name)).sql(dialect="tsql")
                quoted_seq = _quoted_table(_validated_sequence_identifier(column.sequence_name)).sql(dialect="tsql")
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = await driver.select_value(query)
                next_val = int(max_val or 1)
                await driver.execute(f"ALTER SEQUENCE {quoted_seq} RESTART WITH {next_val}")
        return

    if family == "mysql":
        quoted_table = _quoted_table(table).sql(dialect="mysql")
        for column in columns:
            if not column.is_identity and column.sequence_name is not None:
                quoted_col = exp.column(_quoted_identifier(column.name)).sql(dialect="mysql")
                quoted_seq = _quoted_table(_validated_sequence_identifier(column.sequence_name)).sql(dialect="mysql")
                query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
                max_val = await driver.select_value(query)
                next_val = int(max_val or 1)
                await driver.execute(f"ALTER SEQUENCE {quoted_seq} RESTART WITH {next_val}")
        auto_columns = [
            col
            for col in columns
            if col.is_identity
            or (col.is_primary and col.sequence_name is None and ("int" in col.data_type or col.data_type == ""))
        ]
        if auto_columns:
            column = auto_columns[0]
            quoted_col = exp.column(_quoted_identifier(column.name)).sql(dialect="mysql")
            query = f"SELECT coalesce(max({quoted_col}), 0) + 1 FROM {quoted_table}"
            max_val = await driver.select_value(query)
            next_val = int(max_val or 1)
            await driver.execute(f"ALTER TABLE {quoted_table} AUTO_INCREMENT = {next_val}")
        return

    if family == "sqlite":
        for statement, parameters in _sequence_resync_statements(
            table, columns, dialect_name=dialect_name, family=family
        ):
            try:
                await driver.execute(statement, parameters)
            except Exception as exc:
                if "no such table: sqlite_sequence" in str(exc).lower():
                    logger.debug("Skipping sqlite_sequence update: sqlite_sequence table does not exist")
                else:
                    raise
        return

    statements = _sequence_resync_statements(table, columns, dialect_name=dialect_name, family=family)
    if not statements:
        logger.debug("Skipping table fixture sequence resync for table %r: no sequence or identity columns", table)
        return
    for statement, parameters in statements:
        await driver.execute(statement, parameters)


def _exportable_value(value: Any) -> Any:
    """Return a JSON-ready value, encoding bytes as base64 text and vectors as lists."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, array.array):
        return [float(x) for x in value]
    if hasattr(value, "tolist"):
        return [float(x) for x in value.tolist()]
    return value


def _table_fixture_text(rows: "list[dict[str, Any]]", jsonl: bool) -> str:
    """Serialize table rows as a JSON array or as JSON lines, encoding bytes as base64 text."""
    exportable = [{key: _exportable_value(value) for key, value in row.items()} for row in rows]
    if not jsonl:
        return encode_json(exportable)
    return "".join(f"{encode_json(row)}\n" for row in exportable)


def _partition_table_fixture_shards(
    rows: "list[dict[str, Any]]",
    jsonl: bool,
    max_shard_bytes: "int | None" = None,
    max_rows_per_shard: "int | None" = None,
) -> "list[str]":
    """Partition table rows into serialized shard text strings."""
    if not rows:
        return ["" if jsonl else "[]"]
    if max_shard_bytes is None and max_rows_per_shard is None:
        return [_table_fixture_text(rows, jsonl)]

    exportable = [{key: _exportable_value(value) for key, value in row.items()} for row in rows]
    shards: list[str] = []

    if jsonl:
        current_lines: list[str] = []
        current_bytes = 0
        for row in exportable:
            line = f"{encode_json(row)}\n"
            line_bytes = len(line.encode("utf-8"))
            if current_lines and (
                (max_rows_per_shard is not None and len(current_lines) >= max_rows_per_shard)
                or (max_shard_bytes is not None and current_bytes + line_bytes > max_shard_bytes)
            ):
                shards.append("".join(current_lines))
                current_lines = [line]
                current_bytes = line_bytes
            else:
                current_lines.append(line)
                current_bytes += line_bytes
        if current_lines:
            shards.append("".join(current_lines))
    else:
        current_rows: list[str] = []
        current_bytes = 2
        for row in exportable:
            row_json = encode_json(row)
            row_bytes = len(row_json.encode("utf-8"))
            row_cost = row_bytes if not current_rows else (1 + row_bytes)
            if current_rows and (
                (max_rows_per_shard is not None and len(current_rows) >= max_rows_per_shard)
                or (max_shard_bytes is not None and current_bytes + row_cost > max_shard_bytes)
            ):
                shards.append(f"[{','.join(current_rows)}]")
                current_rows = [row_json]
                current_bytes = 2 + row_bytes
            else:
                current_rows.append(row_json)
                current_bytes += row_cost
        if current_rows:
            shards.append(f"[{','.join(current_rows)}]")

    return shards


def _write_table_fixture(
    base_path: Path,
    table: str,
    rows: "list[dict[str, Any]]",
    compress: bool,
    jsonl: bool,
    max_shard_bytes: "int | None" = None,
    max_rows_per_shard: "int | None" = None,
) -> None:
    """Atomically write table rows to fixture files, removing other fixture files of the table."""
    base_path.mkdir(parents=True, exist_ok=True)
    extension = (".jsonl" if jsonl else ".json") + (".gz" if compress else "")
    shards = _partition_table_fixture_shards(rows, jsonl, max_shard_bytes, max_rows_per_shard)

    temp_paths: list[Path] = []
    final_paths: list[Path] = []
    try:
        if len(shards) == 1:
            target_path = base_path / f"{table}{extension}"
            final_paths.append(target_path)
            temp_path = base_path / f".{table}.{secrets.token_hex(8)}.tmp"
            temp_paths.append(temp_path)
            payload = _compress_text(shards[0]) if compress else shards[0].encode("utf-8")
            descriptor = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, _TEMPORARY_FILE_MODE)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
        else:
            for index, shard_text in enumerate(shards, start=1):
                target_path = base_path / f"{table}.{index:03d}{extension}"
                final_paths.append(target_path)
                temp_path = base_path / f".{table}.{secrets.token_hex(8)}.tmp"
                temp_paths.append(temp_path)
                payload = _compress_text(shard_text) if compress else shard_text.encode("utf-8")
                descriptor = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, _TEMPORARY_FILE_MODE)
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(payload)

        for temp_path, final_path in zip(temp_paths, final_paths, strict=True):
            temp_path.replace(final_path)
    except BaseException:
        for temp_path in temp_paths:
            temp_path.unlink(missing_ok=True)
        raise

    written_paths = set(final_paths)
    for entry in base_path.iterdir():
        if not entry.is_file():
            continue
        parsed = _parse_table_fixture_filename(entry.name)
        if parsed is not None and parsed[0] == table and entry not in written_paths:
            entry.unlink(missing_ok=True)


_async_read_text = async_(_read_text_sync)
_async_read_compressed = async_(_read_compressed_file)
_async_serialize = async_(_serialize_data)
_async_compress = async_(_compress_text)
_async_ordered_table_files = async_(_ordered_table_files)
_async_read_table_fixture_text = async_(_read_table_fixture_text)
_async_write_table_fixture = async_(_write_table_fixture)
