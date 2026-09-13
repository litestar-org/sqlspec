"""Fixture loading utilities for SQLSpec.

Provides functions for writing, loading and parsing JSON fixture files
used in testing and development, and for loading and exporting per-table
fixture files against a database driver. Supports both sync and async operations.
"""

import base64
import gzip
import os
import re
import secrets
import zipfile
from datetime import time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, NamedTuple

from sqlglot import exp

from sqlspec.builder import Insert, Select
from sqlspec.core.type_converter import convert_iso_date, convert_iso_datetime, convert_uuid
from sqlspec.storage import storage_registry
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json as decode_json
from sqlspec.utils.serializers import schema_dump
from sqlspec.utils.serializers import to_json as encode_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.typing import SupportedSchemaModel

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

_TABLE_FIXTURE_EXTENSIONS: Final["tuple[str, ...]"] = (".json", ".json.gz", ".jsonl", ".jsonl.gz")
_COLUMN_NAME_PATTERN: Final["re.Pattern[str]"] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TABLE_NAME_PATTERN: Final["re.Pattern[str]"] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?")
_POSTGRES_DIALECTS: Final["frozenset[str]"] = frozenset({"postgres", "postgresql"})
_MYSQL_DIALECTS: Final["frozenset[str]"] = frozenset({"mariadb", "mysql"})
_ON_CONFLICT_FAMILIES: Final["frozenset[str]"] = frozenset({"duckdb", "postgres", "sqlite"})
_JSON_VALUE_FAMILIES: Final["frozenset[str]"] = frozenset({"mysql", "postgres"})
_METADATA_FAMILIES: Final["frozenset[str]"] = frozenset({"duckdb", "mysql", "postgres", "sqlite"})
_BINARY_TYPES: Final["frozenset[str]"] = frozenset({
    "binary",
    "blob",
    "bytea",
    "longblob",
    "mediumblob",
    "tinyblob",
    "varbinary",
})
_UNORDERABLE_POSTGRES_TYPES: Final["frozenset[str]"] = frozenset({
    "box",
    "circle",
    "json",
    "line",
    "lseg",
    "path",
    "point",
    "polygon",
    "xml",
})
_ISO_DURATION_PATTERN: Final["re.Pattern[str]"] = re.compile(
    r"(-)?P(?:(\d+(?:\.\d+)?)W)?(?:(\d+(?:\.\d+)?)D)?"
    r"(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?"
)
_DUCKDB_INTERVAL_PARTS: Final[int] = 3
_NANOSECONDS_PER_MICROSECOND: Final[int] = 1000
_TEMPORARY_FILE_MODE: Final[int] = 0o666

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
        ValueError: If storage backend is not found
    """
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
        ValueError: If storage backend is not found
    """
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
    conflict_keys: "Mapping[str, Sequence[str]] | None" = None,
    batch_size: int = 500,
    resync_sequences: bool = False,
) -> "dict[str, int]":
    """Load per-table fixture files into database tables.

    Each table reads from one file named after it: ``<table>.json`` holding a JSON
    array of row objects, or ``<table>.jsonl`` holding one row object per line, either
    optionally gzipped (``.gz``). Every row in a file must have the same keys, which
    become the inserted columns. Table and column names are quoted, so they must match
    the database spelling exactly. Rows are inserted in batches with ``execute_many``;
    each file is read into memory. Transaction control stays with the caller.

    On PostgreSQL-family, MySQL, DuckDB, and SQLite drivers the target table's columns
    are read from the driver's data dictionary first, and JSON values of these column
    types are converted: ISO 8601 strings in timestamp, datetime, date, and (except on
    MySQL) time columns to datetimes, dates, and times; ISO 8601 durations without year
    or month parts and numbers of seconds in interval columns to ``timedelta``, and
    DuckDB ``[months, days, nanoseconds]`` interval lists to interval text; strings and
    numbers in numeric and decimal columns to ``Decimal``; strings in uuid columns to
    ``UUID``; base64 strings in bytea, blob, binary, and varbinary columns to bytes; and
    PostgreSQL and MySQL json and jsonb values to JSON text. SQLite only converts binary
    columns. All other values, including array elements and durations with year or
    month parts such as ``P1M``, are passed to the driver as decoded from JSON. Values
    of generated columns are ignored. On PostgreSQL, ``GENERATED ALWAYS`` identity
    columns receive the loaded values through ``OVERRIDING SYSTEM VALUE`` and are never
    updated by upserts.

    Args:
        driver: Sync driver that runs the inserts.
        fixtures_path: Directory containing the table fixture files.
        tables: Tables to load. Defaults to every table fixture file in the directory;
            files whose names are not table identifiers are skipped with a debug log, and
            a dot in a file name marks a schema-qualified table.
        table_order: Tables to load first, in this order. Remaining tables load
            afterwards in alphabetical order; names not being loaded are ignored.
        conflict_keys: Mapping of loaded table name to the key columns of a unique
            constraint. Rows for these tables are upserted, updating every non-key column
            when a row with the same key already exists. Supported on PostgreSQL-family, SQLite,
            and DuckDB drivers (``ON CONFLICT``) and MySQL (``ON DUPLICATE KEY UPDATE``,
            which matches any unique key of the table).
        batch_size: Maximum number of rows per ``execute_many`` call.
        resync_sequences: On PostgreSQL-family drivers, set the sequence behind each
            serial or identity column of a loaded table to the column's maximum value,
            or back to the sequence's minimum value for an empty table, so the next
            generated value follows the loaded rows. On other dialects the option is
            skipped with a debug log.

    Returns:
        Mapping of table name to the number of rows loaded, in load order.

    Raises:
        ValueError: If ``batch_size`` is below 1, ``conflict_keys`` is given for an
            unsupported dialect or names a table that is not loaded, a table or column
            name is not a plain SQL identifier,
            a table has more than one fixture file, the rows of a fixture file have
            differing columns, or a fixture file only holds generated columns.
        TypeError: If a fixture file does not hold a list of row objects.
        FileNotFoundError: If a requested table has no fixture file.
    """
    dialect = driver.statement_config.dialect
    dialect_name = _dialect_name(dialect)
    family = _dialect_family(dialect_name)
    _validate_load_arguments(conflict_keys, batch_size, dialect_name, family)
    table_files = _ordered_table_files(fixtures_path, tables, table_order)
    _validate_conflict_tables(conflict_keys, table_files)
    resync = _sequence_resync_enabled(resync_sequences, dialect_name, family)
    counts: dict[str, int] = {}
    for table, file_path in table_files:
        rows = _table_fixture_rows(file_path, _read_table_fixture_text(file_path))
        columns = _table_columns_sync(driver, family, table) if rows or resync else []
        if rows:
            _drop_generated_values(rows, columns)
            _decode_row_values(rows, family, columns)
            statement = _table_insert_statement(
                dialect, family, table, list(rows[0]), _conflict_keys_for(conflict_keys, table), columns
            )
            for start in range(0, len(rows), batch_size):
                driver.execute_many(statement, rows[start : start + batch_size])
        if resync:
            for resync_statement, parameters in _sequence_resync_statements(table, columns):
                driver.execute(resync_statement, parameters)
        counts[table] = len(rows)
    return counts


async def load_table_fixtures_async(
    driver: "AsyncDriverAdapterBase",
    fixtures_path: "str | Path",
    *,
    tables: "Sequence[str] | None" = None,
    table_order: "Sequence[str] | None" = None,
    conflict_keys: "Mapping[str, Sequence[str]] | None" = None,
    batch_size: int = 500,
    resync_sequences: bool = False,
) -> "dict[str, int]":
    """Load per-table fixture files into database tables asynchronously.

    Each table reads from one file named after it: ``<table>.json`` holding a JSON
    array of row objects, or ``<table>.jsonl`` holding one row object per line, either
    optionally gzipped (``.gz``). Every row in a file must have the same keys, which
    become the inserted columns. Table and column names are quoted, so they must match
    the database spelling exactly. Rows are inserted in batches with ``execute_many``;
    each file is read into memory in a worker thread. Transaction control stays with
    the caller.

    On PostgreSQL-family, MySQL, DuckDB, and SQLite drivers the target table's columns
    are read from the driver's data dictionary first, and JSON values of these column
    types are converted: ISO 8601 strings in timestamp, datetime, date, and (except on
    MySQL) time columns to datetimes, dates, and times; ISO 8601 durations without year
    or month parts and numbers of seconds in interval columns to ``timedelta``, and
    DuckDB ``[months, days, nanoseconds]`` interval lists to interval text; strings and
    numbers in numeric and decimal columns to ``Decimal``; strings in uuid columns to
    ``UUID``; base64 strings in bytea, blob, binary, and varbinary columns to bytes; and
    PostgreSQL and MySQL json and jsonb values to JSON text. SQLite only converts binary
    columns. All other values, including array elements and durations with year or
    month parts such as ``P1M``, are passed to the driver as decoded from JSON. Values
    of generated columns are ignored. On PostgreSQL, ``GENERATED ALWAYS`` identity
    columns receive the loaded values through ``OVERRIDING SYSTEM VALUE`` and are never
    updated by upserts.

    Args:
        driver: Async driver that runs the inserts.
        fixtures_path: Directory containing the table fixture files.
        tables: Tables to load. Defaults to every table fixture file in the directory;
            files whose names are not table identifiers are skipped with a debug log, and
            a dot in a file name marks a schema-qualified table.
        table_order: Tables to load first, in this order. Remaining tables load
            afterwards in alphabetical order; names not being loaded are ignored.
        conflict_keys: Mapping of loaded table name to the key columns of a unique
            constraint. Rows for these tables are upserted, updating every non-key column
            when a row with the same key already exists. Supported on PostgreSQL-family, SQLite,
            and DuckDB drivers (``ON CONFLICT``) and MySQL (``ON DUPLICATE KEY UPDATE``,
            which matches any unique key of the table).
        batch_size: Maximum number of rows per ``execute_many`` call.
        resync_sequences: On PostgreSQL-family drivers, set the sequence behind each
            serial or identity column of a loaded table to the column's maximum value,
            or back to the sequence's minimum value for an empty table, so the next
            generated value follows the loaded rows. On other dialects the option is
            skipped with a debug log.

    Returns:
        Mapping of table name to the number of rows loaded, in load order.

    Raises:
        ValueError: If ``batch_size`` is below 1, ``conflict_keys`` is given for an
            unsupported dialect or names a table that is not loaded, a table or column
            name is not a plain SQL identifier,
            a table has more than one fixture file, the rows of a fixture file have
            differing columns, or a fixture file only holds generated columns.
        TypeError: If a fixture file does not hold a list of row objects.
        FileNotFoundError: If a requested table has no fixture file.
    """
    dialect = driver.statement_config.dialect
    dialect_name = _dialect_name(dialect)
    family = _dialect_family(dialect_name)
    _validate_load_arguments(conflict_keys, batch_size, dialect_name, family)
    table_files = await _async_ordered_table_files(fixtures_path, tables, table_order)
    _validate_conflict_tables(conflict_keys, table_files)
    resync = _sequence_resync_enabled(resync_sequences, dialect_name, family)
    counts: dict[str, int] = {}
    for table, file_path in table_files:
        rows = _table_fixture_rows(file_path, await _async_read_table_fixture_text(file_path))
        columns = await _table_columns_async(driver, family, table) if rows or resync else []
        if rows:
            _drop_generated_values(rows, columns)
            _decode_row_values(rows, family, columns)
            statement = _table_insert_statement(
                dialect, family, table, list(rows[0]), _conflict_keys_for(conflict_keys, table), columns
            )
            for start in range(0, len(rows), batch_size):
                await driver.execute_many(statement, rows[start : start + batch_size])
        if resync:
            for resync_statement, parameters in _sequence_resync_statements(table, columns):
                await driver.execute(resync_statement, parameters)
        counts[table] = len(rows)
    return counts


def export_table_fixtures_sync(
    driver: "SyncDriverAdapterBase",
    fixtures_path: "str | Path",
    tables: "Sequence[str]",
    *,
    compress: bool = True,
    jsonl: bool = False,
) -> "dict[str, int]":
    """Export database tables to per-table fixture files.

    Writes every row of each table to ``<table>.json`` (a JSON array) or, with
    ``jsonl``, ``<table>.jsonl`` (one JSON object per line), gzipped with a ``.gz``
    suffix when ``compress`` is set. Generated columns are left out. Rows are ordered by
    the primary key, or by every orderable column when the table has none, and each
    table is read into memory. Dates,
    times, and datetimes are written as ISO 8601 strings, ``Decimal`` and ``UUID``
    values as strings, and bytes as base64 strings, which
    :func:`load_table_fixtures_sync` converts back for typed columns.

    Each file is written to a temporary file in the directory, created with the
    process umask, and then moved into
    place; after that, other table fixture files for the same table in the directory
    are removed, so the next load reads the exported file. The directory is created
    when missing. Table names are quoted and must match the database spelling exactly.

    Args:
        driver: Sync driver that reads the tables.
        fixtures_path: Directory to write the fixture files to.
        tables: Tables to export.
        compress: Whether to gzip the written files.
        jsonl: Whether to write one JSON object per line instead of a JSON array.

    Returns:
        Mapping of table name to the number of rows written.

    Raises:
        ValueError: If a table name is not a plain SQL identifier.
    """
    table_names = [_validated_table_name(table) for table in tables]
    dialect = driver.statement_config.dialect
    family = _dialect_family(_dialect_name(dialect))
    counts: dict[str, int] = {}
    for table in table_names:
        columns = _table_columns_sync(driver, family, table)
        rows = _without_generated_values(driver.select(_table_export_query(dialect, table, columns)), columns)
        _write_table_fixture(Path(fixtures_path), table, rows, compress, jsonl)
        counts[table] = len(rows)
    return counts


async def export_table_fixtures_async(
    driver: "AsyncDriverAdapterBase",
    fixtures_path: "str | Path",
    tables: "Sequence[str]",
    *,
    compress: bool = True,
    jsonl: bool = False,
) -> "dict[str, int]":
    """Export database tables to per-table fixture files asynchronously.

    Writes every row of each table to ``<table>.json`` (a JSON array) or, with
    ``jsonl``, ``<table>.jsonl`` (one JSON object per line), gzipped with a ``.gz``
    suffix when ``compress`` is set. Generated columns are left out. Rows are ordered by
    the primary key, or by every orderable column when the table has none, and each
    table is read into memory. Dates,
    times, and datetimes are written as ISO 8601 strings, ``Decimal`` and ``UUID``
    values as strings, and bytes as base64 strings, which
    :func:`load_table_fixtures_async` converts back for typed columns.

    Each file is written to a temporary file in the directory, created with the
    process umask, and then moved into
    place; after that, other table fixture files for the same table in the directory
    are removed, so the next load reads the exported file. The directory is created
    when missing, and file writes run in a worker thread. Table names are quoted and
    must match the database spelling exactly.

    Args:
        driver: Async driver that reads the tables.
        fixtures_path: Directory to write the fixture files to.
        tables: Tables to export.
        compress: Whether to gzip the written files.
        jsonl: Whether to write one JSON object per line instead of a JSON array.

    Returns:
        Mapping of table name to the number of rows written.

    Raises:
        ValueError: If a table name is not a plain SQL identifier.
    """
    table_names = [_validated_table_name(table) for table in tables]
    dialect = driver.statement_config.dialect
    family = _dialect_family(_dialect_name(dialect))
    counts: dict[str, int] = {}
    for table in table_names:
        columns = await _table_columns_async(driver, family, table)
        rows = _without_generated_values(await driver.select(_table_export_query(dialect, table, columns)), columns)
        await _async_write_table_fixture(Path(fixtures_path), table, rows, compress, jsonl)
        counts[table] = len(rows)
    return counts


class _TableColumn(NamedTuple):
    """Column metadata used to convert, insert, order, and resync table fixture rows."""

    name: str
    data_type: str
    is_primary: bool
    identity_kind: str
    sequence_name: "str | None"
    is_generated: bool


def _read_text_sync(path: "Path") -> str:
    return path.read_text(encoding="utf-8")


def _resolve_path_str(path: str) -> str:
    return str(Path(path).resolve())


def _compress_text(content: str) -> bytes:
    return gzip.compress(content.encode("utf-8"))


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
                with zf.open(json_name) as f:
                    return f.read().decode("utf-8")
            json_files = [name for name in zf.namelist() if name.endswith(".json")]
            if json_files:
                with zf.open(json_files[0]) as f:
                    return f.read().decode("utf-8")
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
    """
    base_path = Path(fixtures_path)

    for extension in [".json", ".json.gz", ".json.zip"]:
        fixture_path = base_path / f"{fixture_name}{extension}"
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
    """Return ``postgres`` or ``mysql`` for those dialect families, otherwise the dialect name."""
    if dialect_name in _POSTGRES_DIALECTS:
        return "postgres"
    if dialect_name in _MYSQL_DIALECTS:
        return "mysql"
    return dialect_name


def _validate_load_arguments(
    conflict_keys: "Mapping[str, Sequence[str]] | None", batch_size: int, dialect_name: str, family: str
) -> None:
    """Validate the batch size and conflict keys of a table fixture load.

    Raises:
        ValueError: If ``batch_size`` is below 1, conflict keys are unsupported for the
            dialect, or a conflict key name is unsafe.
    """
    if batch_size < 1:
        msg = f"batch_size must be at least 1, got {batch_size}"
        raise ValueError(msg)
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
        for key in keys:
            _validated_column_name(key)


def _validate_conflict_tables(
    conflict_keys: "Mapping[str, Sequence[str]] | None", table_files: "list[tuple[str, Path]]"
) -> None:
    """Validate that every conflict key entry names a table being loaded.

    Raises:
        ValueError: If a conflict key table is not among the loaded tables.
    """
    if not conflict_keys:
        return
    loaded = {table for table, _ in table_files}
    unknown = sorted(table for table in conflict_keys if table not in loaded)
    if unknown:
        names = ", ".join(repr(table) for table in unknown)
        msg = f"conflict_keys names tables that are not loaded: {names}"
        raise ValueError(msg)


def _conflict_keys_for(conflict_keys: "Mapping[str, Sequence[str]] | None", table: str) -> "tuple[str, ...]":
    """Return the conflict key columns configured for a table."""
    if not conflict_keys or table not in conflict_keys:
        return ()
    return tuple(conflict_keys[table])


def _table_name_for_file(file_name: str) -> "str | None":
    """Return the table part of a table fixture file name, or None for other files."""
    for extension in _TABLE_FIXTURE_EXTENSIONS:
        if file_name.endswith(extension):
            return file_name[: -len(extension)]
    return None


def _single_table_file(table: str, paths: "list[Path]") -> Path:
    """Return the only fixture file of a table.

    Raises:
        FileNotFoundError: If the table has no fixture file.
        ValueError: If the table has more than one fixture file.
    """
    if not paths:
        msg = f"Could not find the {table} fixture"
        raise FileNotFoundError(msg)
    if len(paths) > 1:
        names = ", ".join(path.name for path in paths)
        msg = f"Table {table!r} has more than one fixture file: {names}"
        raise ValueError(msg)
    return paths[0]


def _discover_table_files(fixtures_path: "str | Path") -> "dict[str, Path]":
    """Map each table with a fixture file in the directory to its file.

    Raises:
        ValueError: If a table has more than one fixture file.
    """
    table_paths: dict[str, list[Path]] = {}
    for entry in sorted(Path(fixtures_path).iterdir()):
        table = _table_name_for_file(entry.name)
        if table is None or not entry.is_file():
            continue
        if not _TABLE_NAME_PATTERN.fullmatch(table):
            logger.debug("Skipping table fixture file %s: %r is not a table identifier", entry.name, table)
            continue
        if "." in table:
            logger.debug("Table fixture file %s names schema-qualified table %r", entry.name, table)
        table_paths.setdefault(table, []).append(entry)
    return {table: _single_table_file(table, paths) for table, paths in table_paths.items()}


def _ordered_table_files(
    fixtures_path: "str | Path", tables: "Sequence[str] | None", table_order: "Sequence[str] | None"
) -> "list[tuple[str, Path]]":
    """Resolve the tables to load and their fixture files in load order.

    Raises:
        ValueError: If a table name is not a safe identifier or a table has more than one fixture file.
        FileNotFoundError: If a requested table has no fixture file.
    """
    if tables is None:
        table_files = _discover_table_files(fixtures_path)
    else:
        base_path = Path(fixtures_path)
        table_files = {}
        for table in tables:
            _validated_table_name(table)
            candidates = [base_path / f"{table}{extension}" for extension in _TABLE_FIXTURE_EXTENSIONS]
            table_files[table] = _single_table_file(table, [path for path in candidates if path.is_file()])
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
    """Decode the row objects of a table fixture file and validate their columns.

    Raises:
        TypeError: If the content is not a list of row objects.
        ValueError: If the rows do not share the same non-empty set of safe column names.
    """
    decoded: Any
    if file_path.name.endswith((".jsonl", ".jsonl.gz")):
        decoded = [decode_json(line) for line in content.splitlines() if line.strip()]
    else:
        decoded = decode_json(content)
        if not isinstance(decoded, list):
            msg = f"Table fixture {file_path} must contain a JSON array of row objects"
            raise TypeError(msg)
    rows: list[dict[str, Any]] = []
    columns: set[str] = set()
    for index, row in enumerate(decoded):
        if not isinstance(row, dict):
            msg = f"Row {index} in table fixture {file_path} must be an object"
            raise TypeError(msg)
        if index == 0:
            if not row:
                msg = f"Row 0 in table fixture {file_path} must have at least one column"
                raise ValueError(msg)
            columns = {_validated_column_name(column) for column in row}
        elif set(row) != columns:
            msg = f"Row {index} in table fixture {file_path} has columns {sorted(row)} that do not match {sorted(columns)}"
            raise ValueError(msg)
        rows.append(row)
    return rows


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


def _table_column(row: "Mapping[str, Any]") -> _TableColumn:
    """Return column metadata from a data dictionary column row."""
    sequence_name = row.get("sequence_name")
    return _TableColumn(
        name=str(row["column_name"]),
        data_type=str(row.get("data_type") or "").lower(),
        is_primary=bool(row.get("is_primary")),
        identity_kind=str(row.get("identity_generation") or ""),
        sequence_name=None if sequence_name is None else str(sequence_name),
        is_generated=_is_generated_column(row),
    )


def _is_generated_column(row: "Mapping[str, Any]") -> bool:
    """Return whether a data dictionary column row describes a generated (computed) column.

    PostgreSQL rows carry ``generated_kind``, DuckDB rows ``is_generated``, CockroachDB
    rows ``generation_expression``, and MySQL, MariaDB, and SQLite rows an ``extra``
    value containing the word ``generated``.
    """
    if row.get("is_generated") or row.get("generated_kind") or row.get("generation_expression"):
        return True
    return "generated" in str(row.get("extra") or "").lower().split()


def _column_lookup_names(family: str, table: str) -> "tuple[str, str | None]":
    """Return the table and schema names to pass to the data dictionary for an exact-case table."""
    schema_name, _, table_name = table.rpartition(".")
    if family == "postgres":
        return _quoted_identifier_sql(table_name), _quoted_identifier_sql(schema_name) if schema_name else None
    return table_name, schema_name or None


def _table_columns_sync(driver: "SyncDriverAdapterBase", family: str, table: str) -> "list[_TableColumn]":
    """Read the column metadata of a table from the driver's data dictionary."""
    if family not in _METADATA_FAMILIES:
        return []
    table_name, schema_name = _column_lookup_names(family, table)
    rows = driver.data_dictionary.get_columns(driver, table=table_name, schema=schema_name)
    return [_table_column(row) for row in rows]


async def _table_columns_async(driver: "AsyncDriverAdapterBase", family: str, table: str) -> "list[_TableColumn]":
    """Read the column metadata of a table from the driver's data dictionary."""
    if family not in _METADATA_FAMILIES:
        return []
    table_name, schema_name = _column_lookup_names(family, table)
    rows = await driver.data_dictionary.get_columns(driver, table=table_name, schema=schema_name)
    return [_table_column(row) for row in rows]


def _generated_column_names(columns: "list[_TableColumn]") -> "set[str]":
    """Return the names of the generated columns of a table."""
    return {column.name for column in columns if column.is_generated}


def _drop_generated_values(rows: "list[dict[str, Any]]", columns: "list[_TableColumn]") -> None:
    """Remove the values of generated columns from fixture rows in place.

    Raises:
        ValueError: If only generated columns remain in the rows.
    """
    generated = _generated_column_names(columns)
    if not rows or generated.isdisjoint(rows[0]):
        return
    for row in rows:
        for name in generated:
            row.pop(name, None)
    if not rows[0]:
        msg = f"Table fixture rows only contain generated columns: {sorted(generated)}"
        raise ValueError(msg)


def _without_generated_values(rows: "list[dict[str, Any]]", columns: "list[_TableColumn]") -> "list[dict[str, Any]]":
    """Return exported rows without the values of generated columns."""
    generated = _generated_column_names(columns)
    if not generated:
        return rows
    return [{key: value for key, value in row.items() if key not in generated} for row in rows]


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


def _column_value_decoder(family: str, data_type: str) -> "Callable[[Any], Any] | None":
    """Return the converter from a JSON value to the driver value for a column type, if any."""
    if data_type.endswith("]"):
        return None
    if data_type in _BINARY_TYPES or data_type.startswith(("binary(", "varbinary(")):
        return _decode_bytes
    if family == "sqlite":
        return None
    if data_type in {"json", "jsonb"}:
        return encode_json if family in _JSON_VALUE_FAMILIES else None
    if data_type.startswith(("timestamp", "datetime")):
        return _decode_datetime
    if data_type == "date":
        return _decode_date
    if data_type.startswith("time") and family != "mysql":
        return _decode_time
    if data_type.startswith("interval"):
        return _decode_duckdb_interval if family == "duckdb" else _decode_interval
    if data_type.startswith(("numeric", "decimal")):
        return _decode_decimal
    if data_type == "uuid":
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
    family: str, columns: "list[str]", conflict_keys: "tuple[str, ...]", always_identity: "set[str]"
) -> exp.OnConflict:
    """Return the upsert clause that updates non-key, non-identity columns on a key conflict."""
    updates = [column for column in columns if column not in conflict_keys and column not in always_identity]
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
) -> "Insert | str":
    """Return an INSERT with a named placeholder per quoted column, upserting on conflict keys.

    On PostgreSQL, an insert into ``GENERATED ALWAYS`` identity columns is returned as SQL
    text with ``OVERRIDING SYSTEM VALUE``.
    """
    statement = (
        Insert(dialect=dialect)
        .into(_quoted_table_name(table))
        .columns(*[_quoted_identifier(column) for column in columns])
        .values({column: exp.Placeholder(this=column) for column in columns})
    )
    insert_expression = statement.get_insert_expression()
    always_identity = {column.name for column in table_columns if column.identity_kind == "a"}
    if conflict_keys:
        insert_expression.set("conflict", _conflict_clause(family, columns, conflict_keys, always_identity))
    if family != "postgres" or always_identity.isdisjoint(columns):
        return statement
    return insert_expression.sql(dialect=dialect).replace(") VALUES (", ") OVERRIDING SYSTEM VALUE VALUES (", 1)


def _is_orderable_type(data_type: str) -> bool:
    """Return whether a column type, or the element type of an array type, can be used in ORDER BY."""
    return data_type.partition("[")[0] not in _UNORDERABLE_POSTGRES_TYPES


def _table_export_query(dialect: "DialectType", table: str, table_columns: "list[_TableColumn]") -> Select:
    """Return a SELECT of every row ordered by the primary key, every orderable column, or position 1."""
    order_columns = [column.name for column in table_columns if column.is_primary] or [
        column.name for column in table_columns if _is_orderable_type(column.data_type)
    ]
    order_by = [_quoted_identifier_sql(name) for name in order_columns] or ["1"]
    return Select("*", dialect=dialect).from_(_quoted_table_name(table)).order_by(*order_by)


def _sequence_resync_enabled(resync_sequences: bool, dialect_name: str, family: str) -> bool:
    """Return whether sequence resync applies, logging when it is skipped for the dialect."""
    if not resync_sequences:
        return False
    if family == "postgres":
        return True
    logger.debug("Skipping table fixture sequence resync for non-PostgreSQL dialect %r", dialect_name or None)
    return False


def _sequence_resync_statements(table: str, table_columns: "list[_TableColumn]") -> "list[tuple[str, dict[str, str]]]":
    """Return statements aligning each owned sequence of a table with its column's maximum value."""
    statements: list[tuple[str, dict[str, str]]] = []
    for column in table_columns:
        if column.sequence_name is None:
            continue
        quoted_column = _quoted_identifier_sql(column.name)
        statement = (
            f"SELECT setval(to_regclass(:sequence_name), coalesce(max({quoted_column}), "
            "(SELECT seqmin FROM pg_catalog.pg_sequence WHERE seqrelid = to_regclass(:sequence_name))), "
            f"max({quoted_column}) IS NOT NULL) FROM {_quoted_table_name(table)}"
        )
        statements.append((statement, {"sequence_name": column.sequence_name}))
    return statements


def _exportable_value(value: Any) -> Any:
    """Return a JSON-ready value, encoding bytes as base64 text."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(value).decode("ascii")
    return value


def _table_fixture_text(rows: "list[dict[str, Any]]", jsonl: bool) -> str:
    """Serialize table rows as a JSON array or as JSON lines, encoding bytes as base64 text."""
    exportable = [{key: _exportable_value(value) for key, value in row.items()} for row in rows]
    if not jsonl:
        return encode_json(exportable)
    return "".join(f"{encode_json(row)}\n" for row in exportable)


def _write_table_fixture(
    base_path: Path, table: str, rows: "list[dict[str, Any]]", compress: bool, jsonl: bool
) -> None:
    """Atomically write table rows to their fixture file, then remove other fixture files of the table."""
    extension = (".jsonl" if jsonl else ".json") + (".gz" if compress else "")
    base_path.mkdir(parents=True, exist_ok=True)
    content = _table_fixture_text(rows, jsonl)
    payload = _compress_text(content) if compress else content.encode("utf-8")
    temporary_path = base_path / f".{table}.{secrets.token_hex(8)}.tmp"
    descriptor = os.open(temporary_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, _TEMPORARY_FILE_MODE)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
        temporary_path.replace(base_path / f"{table}{extension}")
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise
    for other_extension in _TABLE_FIXTURE_EXTENSIONS:
        if other_extension != extension:
            (base_path / f"{table}{other_extension}").unlink(missing_ok=True)


_async_read_text = async_(_read_text_sync)
_async_read_compressed = async_(_read_compressed_file)
_async_serialize = async_(_serialize_data)
_async_compress = async_(_compress_text)
_async_ordered_table_files = async_(_ordered_table_files)
_async_read_table_fixture_text = async_(_read_table_fixture_text)
_async_write_table_fixture = async_(_write_table_fixture)
