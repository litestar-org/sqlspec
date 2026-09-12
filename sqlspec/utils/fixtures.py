"""Fixture loading utilities for SQLSpec.

Provides functions for writing, loading and parsing JSON fixture files
used in testing and development, and for loading and exporting per-table
fixture files against a database driver. Supports both sync and async operations.
"""

import gzip
import re
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from sqlglot import exp

from sqlspec.builder import Insert, Select
from sqlspec.storage import storage_registry
from sqlspec.utils.serializers import from_json as decode_json
from sqlspec.utils.serializers import schema_dump
from sqlspec.utils.serializers import to_json as encode_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

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

_JSON_FIXTURE_EXTENSIONS: Final["tuple[str, ...]"] = (".json", ".json.gz", ".json.zip")
_TABLE_FIXTURE_EXTENSIONS: Final["tuple[str, ...]"] = (".json", ".json.gz", ".jsonl", ".jsonl.gz")
_COLUMN_NAME_PATTERN: Final["re.Pattern[str]"] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TABLE_NAME_PATTERN: Final["re.Pattern[str]"] = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?")


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
) -> "dict[str, int]":
    """Load per-table fixture files into database tables.

    Each table reads from one file named after it: ``<table>.json`` holding a JSON
    array of row objects, or ``<table>.jsonl`` holding one row object per line, either
    optionally gzipped (``.gz``). Every row in a file must have the same keys, which
    become the inserted columns. Rows are inserted in batches with ``execute_many``.
    Values are passed to the driver as decoded from JSON, so files written by
    :func:`export_table_fixtures_sync` load back unchanged. Transaction control stays
    with the caller.

    Args:
        driver: Sync driver that runs the inserts.
        fixtures_path: Directory containing the table fixture files.
        tables: Tables to load. Defaults to every table fixture file in the directory.
        table_order: Tables to load first, in this order. Remaining tables load
            afterwards in alphabetical order; names not being loaded are ignored.
        conflict_keys: Mapping of table name to the key columns of a unique constraint.
            Rows for these tables are upserted, updating every non-key column when a row
            with the same key already exists.
        batch_size: Maximum number of rows per ``execute_many`` call.

    Returns:
        Mapping of table name to the number of rows loaded, in load order.

    Raises:
        ValueError: If ``batch_size`` is below 1, a table or column name is not a plain
            SQL identifier, or the rows of a fixture file have differing columns.
        TypeError: If a fixture file does not hold a list of row objects.
        FileNotFoundError: If a requested table has no fixture file.
    """
    _validate_load_arguments(conflict_keys, batch_size)
    table_files = _ordered_table_files(fixtures_path, tables, table_order)
    dialect = driver.statement_config.dialect
    counts: dict[str, int] = {}
    for table, file_path in table_files:
        rows = _table_fixture_rows(file_path, _read_table_fixture_text(file_path))
        if rows:
            statement = _table_insert_statement(dialect, table, list(rows[0]), _conflict_keys_for(conflict_keys, table))
            for start in range(0, len(rows), batch_size):
                driver.execute_many(statement, rows[start : start + batch_size])
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
) -> "dict[str, int]":
    """Load per-table fixture files into database tables asynchronously.

    Each table reads from one file named after it: ``<table>.json`` holding a JSON
    array of row objects, or ``<table>.jsonl`` holding one row object per line, either
    optionally gzipped (``.gz``). Every row in a file must have the same keys, which
    become the inserted columns. Rows are inserted in batches with ``execute_many``.
    Values are passed to the driver as decoded from JSON, so files written by
    :func:`export_table_fixtures_async` load back unchanged. File reads run in a
    worker thread. Transaction control stays with the caller.

    Args:
        driver: Async driver that runs the inserts.
        fixtures_path: Directory containing the table fixture files.
        tables: Tables to load. Defaults to every table fixture file in the directory.
        table_order: Tables to load first, in this order. Remaining tables load
            afterwards in alphabetical order; names not being loaded are ignored.
        conflict_keys: Mapping of table name to the key columns of a unique constraint.
            Rows for these tables are upserted, updating every non-key column when a row
            with the same key already exists.
        batch_size: Maximum number of rows per ``execute_many`` call.

    Returns:
        Mapping of table name to the number of rows loaded, in load order.

    Raises:
        ValueError: If ``batch_size`` is below 1, a table or column name is not a plain
            SQL identifier, or the rows of a fixture file have differing columns.
        TypeError: If a fixture file does not hold a list of row objects.
        FileNotFoundError: If a requested table has no fixture file.
    """
    _validate_load_arguments(conflict_keys, batch_size)
    table_files = await _async_ordered_table_files(fixtures_path, tables, table_order)
    dialect = driver.statement_config.dialect
    counts: dict[str, int] = {}
    for table, file_path in table_files:
        rows = _table_fixture_rows(file_path, await _async_read_table_fixture_text(file_path))
        if rows:
            statement = _table_insert_statement(dialect, table, list(rows[0]), _conflict_keys_for(conflict_keys, table))
            for start in range(0, len(rows), batch_size):
                await driver.execute_many(statement, rows[start : start + batch_size])
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
    suffix when ``compress`` is set. Other table fixture files for the same table in
    the directory are removed, so :func:`load_table_fixtures_sync` reads the exported
    file. The directory is created when missing.

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
    counts: dict[str, int] = {}
    for table in table_names:
        rows = driver.select(Select("*", dialect=dialect).from_(table))
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
    suffix when ``compress`` is set. Other table fixture files for the same table in
    the directory are removed, so :func:`load_table_fixtures_async` reads the exported
    file. The directory is created when missing. File writes run in a worker thread.

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
    counts: dict[str, int] = {}
    for table in table_names:
        rows = await driver.select(Select("*", dialect=dialect).from_(table))
        await _async_write_table_fixture(Path(fixtures_path), table, rows, compress, jsonl)
        counts[table] = len(rows)
    return counts


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


def _find_fixture_file(
    fixtures_path: Any, fixture_name: str, extensions: "tuple[str, ...]" = _JSON_FIXTURE_EXTENSIONS
) -> Path:
    """Find a fixture file with various extensions.

    Args:
        fixtures_path: The path to look for fixtures
        fixture_name: The fixture name to load
        extensions: File extensions to try, in priority order

    Returns:
        Path to the found fixture file

    Raises:
        FileNotFoundError: If no fixture file is found
    """
    base_path = Path(fixtures_path)

    for extension in extensions:
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


def _validate_load_arguments(conflict_keys: "Mapping[str, Sequence[str]] | None", batch_size: int) -> None:
    """Validate the batch size and conflict key identifiers of a table fixture load.

    Raises:
        ValueError: If ``batch_size`` is below 1 or a conflict key name is unsafe.
    """
    if batch_size < 1:
        msg = f"batch_size must be at least 1, got {batch_size}"
        raise ValueError(msg)
    if conflict_keys:
        for table, keys in conflict_keys.items():
            _validated_table_name(table)
            for key in keys:
                _validated_column_name(key)


def _conflict_keys_for(conflict_keys: "Mapping[str, Sequence[str]] | None", table: str) -> "tuple[str, ...]":
    """Return the conflict key columns configured for a table."""
    if not conflict_keys or table not in conflict_keys:
        return ()
    return tuple(conflict_keys[table])


def _discover_table_files(fixtures_path: "str | Path") -> "dict[str, Path]":
    """Map each table with a fixture file in the directory to its file.

    Raises:
        ValueError: If a fixture file name is not a safe table identifier.
    """
    base_path = Path(fixtures_path)
    table_names: set[str] = set()
    for entry in base_path.iterdir():
        if not entry.is_file():
            continue
        for extension in _TABLE_FIXTURE_EXTENSIONS:
            if entry.name.endswith(extension):
                table_names.add(entry.name[: -len(extension)])
                break
    return {
        table: _find_fixture_file(base_path, _validated_table_name(table), _TABLE_FIXTURE_EXTENSIONS)
        for table in sorted(table_names)
    }


def _ordered_table_files(
    fixtures_path: "str | Path", tables: "Sequence[str] | None", table_order: "Sequence[str] | None"
) -> "list[tuple[str, Path]]":
    """Resolve the tables to load and their fixture files in load order.

    Raises:
        ValueError: If a table name is not a safe identifier.
        FileNotFoundError: If a requested table has no fixture file.
    """
    if tables is None:
        table_files = _discover_table_files(fixtures_path)
    else:
        table_files = {
            table: _find_fixture_file(fixtures_path, _validated_table_name(table), _TABLE_FIXTURE_EXTENSIONS)
            for table in tables
        }
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


def _table_insert_statement(
    dialect: "DialectType", table: str, columns: "list[str]", conflict_keys: "tuple[str, ...]"
) -> Insert:
    """Return an INSERT with a named placeholder per column, upserting on conflict keys."""
    statement = (
        Insert(table, dialect=dialect, enable_optimization=False)
        .columns(*columns)
        .values({column: exp.Placeholder(this=column) for column in columns})
    )
    if not conflict_keys:
        return statement
    updates = {column: exp.column(column, table="excluded") for column in columns if column not in conflict_keys}
    if not updates:
        return statement.on_conflict(*conflict_keys).do_nothing()
    return statement.on_conflict(*conflict_keys).do_update(**updates)


def _table_fixture_text(rows: "list[dict[str, Any]]", jsonl: bool) -> str:
    """Serialize table rows as a JSON array or as JSON lines."""
    if not jsonl:
        return _serialize_data(rows)
    return "".join(f"{encode_json(schema_dump(row))}\n" for row in rows)


def _write_table_fixture(
    base_path: Path, table: str, rows: "list[dict[str, Any]]", compress: bool, jsonl: bool
) -> None:
    """Write table rows to their fixture file, replacing other fixture files of the table."""
    extension = (".jsonl" if jsonl else ".json") + (".gz" if compress else "")
    base_path.mkdir(parents=True, exist_ok=True)
    content = _table_fixture_text(rows, jsonl)
    target = base_path / f"{table}{extension}"
    if compress:
        target.write_bytes(_compress_text(content))
    else:
        target.write_text(content, encoding="utf-8")
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
