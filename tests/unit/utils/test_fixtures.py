# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Tests for sqlspec.utils.fixtures module.

Tests fixture loading utilities including synchronous and asynchronous
JSON fixture file loading with compression support.
"""

import array
import asyncio
import gzip
import importlib
import json
import logging
import os
import struct
import sys
import zipfile
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import UUID

import pytest

import sqlspec.utils.fixtures as fixture_module
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core.parameters import TypedParameter
from sqlspec.utils.fixtures import (
    _async_compress,
    _async_read_compressed,
    _async_read_text,
    _async_serialize,
    _find_fixture_file,
    _read_compressed_file,
    _serialize_data,
    export_table_fixtures_async,
    export_table_fixtures_sync,
    load_table_fixtures_async,
    load_table_fixtures_sync,
    open_fixture_async,
    open_fixture_sync,
    write_fixture_async,
    write_fixture_sync,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import _AsyncWrapper

TABLE_FIXTURE_DDL = (
    "PRAGMA foreign_keys = ON",
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), title TEXT NOT NULL)",
)
USER_ROWS: "list[dict[str, Any]]" = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
POST_ROWS: "list[dict[str, Any]]" = [
    {"id": 10, "user_id": 1, "title": "Hello"},
    {"id": 11, "user_id": 2, "title": "World"},
    {"id": 12, "user_id": 1, "title": "Again"},
]


def test_fixture_helpers_use_explicit_sync_and_async_names() -> None:
    """The clean-break API names both execution modes explicitly."""
    assert fixture_module.__all__ == (
        "export_table_fixtures_async",
        "export_table_fixtures_sync",
        "load_table_fixtures_async",
        "load_table_fixtures_sync",
        "open_fixture_async",
        "open_fixture_sync",
        "write_fixture_async",
        "write_fixture_sync",
    )
    assert not hasattr(fixture_module, "open_fixture")
    assert not hasattr(fixture_module, "write_fixture")


def test_find_fixture_file_json(tmp_path: Path) -> None:
    """Test finding regular .json fixture file."""
    fixtures_path = tmp_path
    fixture_file = fixtures_path / "test.json"
    fixture_file.write_text('{"test": "data"}')

    result = _find_fixture_file(fixtures_path, "test")
    assert result == fixture_file


def test_find_fixture_file_gz_priority(tmp_path: Path) -> None:
    """Test .json.gz takes priority over .json when both exist."""
    fixtures_path = tmp_path
    json_file = fixtures_path / "test.json"
    gz_file = fixtures_path / "test.json.gz"

    json_file.write_text('{"test": "json"}')
    with gzip.open(gz_file, "wt") as f:
        json.dump({"test": "gz"}, f)

    result = _find_fixture_file(fixtures_path, "test")
    assert result == json_file  # .json has highest priority


def test_find_fixture_file_zip_fallback(tmp_path: Path) -> None:
    """Test .json.zip is found when .json and .json.gz don't exist."""
    fixtures_path = tmp_path
    zip_file = fixtures_path / "test.json.zip"

    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr("test.json", '{"test": "zip"}')

    result = _find_fixture_file(fixtures_path, "test")
    assert result == zip_file


def test_find_fixture_file_not_found(tmp_path: Path) -> None:
    """Test FileNotFoundError when no fixture file exists."""
    fixtures_path = tmp_path

    with pytest.raises(FileNotFoundError, match="Could not find the missing fixture"):
        _find_fixture_file(fixtures_path, "missing")


def test_read_gzip_file(tmp_path: Path) -> None:
    """Test reading gzipped JSON file."""
    gz_file = tmp_path / "test.json.gz"
    test_data = {"test": "gzipped data", "number": 42}

    with gzip.open(gz_file, "wt", encoding="utf-8") as f:
        json.dump(test_data, f)

    result = _read_compressed_file(gz_file)
    assert json.loads(result) == test_data


def test_read_zip_file_with_matching_name(tmp_path: Path) -> None:
    """Test reading ZIP file with matching JSON filename."""
    zip_file = tmp_path / "test.json.zip"
    test_data = {"test": "zipped data", "array": [1, 2, 3]}

    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr("test.json", json.dumps(test_data))

    result = _read_compressed_file(zip_file)
    assert json.loads(result) == test_data


def test_read_zip_file_first_json(tmp_path: Path) -> None:
    """Test reading ZIP file with first JSON file when no matching name."""
    zip_file = tmp_path / "archive.zip"
    test_data = {"test": "first json file"}

    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr("data.json", json.dumps(test_data))
        zf.writestr("other.txt", "not json")

    result = _read_compressed_file(zip_file)
    assert json.loads(result) == test_data


def test_read_zip_file_no_json(tmp_path: Path) -> None:
    """Test error when ZIP file contains no JSON files."""
    zip_file = tmp_path / "empty.zip"

    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr("data.txt", "not json")

    with pytest.raises(ValueError, match="No JSON file found in ZIP archive"):
        _read_compressed_file(zip_file)


def test_read_unsupported_format(tmp_path: Path) -> None:
    """Test error for unsupported compression format."""
    unsupported_file = tmp_path / "test.tar.gz"
    unsupported_file.write_text("data")

    # gzip module attempts to read .tar.gz files and raises BadGzipFile
    with pytest.raises(gzip.BadGzipFile):
        _read_compressed_file(unsupported_file)


def test_async_fixture_wrappers_are_hoisted() -> None:
    assert isinstance(_async_read_text, _AsyncWrapper)
    assert isinstance(_async_read_compressed, _AsyncWrapper)
    assert isinstance(_async_serialize, _AsyncWrapper)
    assert isinstance(_async_compress, _AsyncWrapper)


def test_serialize_dict() -> None:
    """Test serializing a simple dictionary."""
    data = {"name": "test", "value": 42}
    result = _serialize_data(data)
    assert json.loads(result) == data


def test_serialize_list_of_dicts() -> None:
    """Test serializing a list of dictionaries."""
    data = [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]
    result = _serialize_data(data)
    assert json.loads(result) == data


def test_serialize_tuple() -> None:
    """Test serializing a tuple (treated as list)."""
    data = ({"id": 1}, {"id": 2})
    result = _serialize_data(data)
    assert json.loads(result) == [{"id": 1}, {"id": 2}]


@pytest.mark.skipif(
    not hasattr(__import__("sys").modules.get("pydantic", None), "BaseModel"), reason="Pydantic not available"
)
def test_serialize_pydantic_model() -> None:
    """Test serializing a Pydantic model."""
    try:
        from pydantic import BaseModel

        class FixtureModel(BaseModel):
            name: str
            value: int

        model = FixtureModel(name="test", value=42)
        result = _serialize_data(model)
        assert json.loads(result) == {"name": "test", "value": 42}
    except ImportError:
        pytest.skip("Pydantic not available")


@pytest.mark.skipif(
    not hasattr(__import__("sys").modules.get("msgspec", None), "Struct"), reason="msgspec not available"
)
def test_serialize_msgspec_struct() -> None:
    """Test serializing a msgspec Struct."""
    try:
        import msgspec

        class FixtureStruct(msgspec.Struct):
            name: str
            value: int

        struct = FixtureStruct(name="test", value=42)
        result = _serialize_data(struct)
        assert json.loads(result) == {"name": "test", "value": 42}
    except ImportError:
        pytest.skip("msgspec not available")


def test_serialize_list_mixed_types() -> None:
    """Test serializing a list with mixed types."""
    data = [{"id": 1}, "string", 42, True, None]
    result = _serialize_data(data)
    assert json.loads(result) == data


def test_serialize_primitive_string() -> None:
    """Test serializing a primitive string."""
    data = "hello world"
    result = _serialize_data(data)
    assert json.loads(result) == data


def test_serialize_primitive_number() -> None:
    """Test serializing a primitive number."""
    data = 42
    result = _serialize_data(data)
    assert json.loads(result) == data


def test_serialize_primitive_boolean() -> None:
    """Test serializing a primitive boolean."""
    data = True
    result = _serialize_data(data)
    assert json.loads(result) == data


def test_serialize_primitive_none() -> None:
    """Test serializing None."""
    data = None
    result = _serialize_data(data)
    assert json.loads(result) == data


def test_open_fixture_valid_file(tmp_path: Path) -> None:
    """Test open_fixture_sync with valid JSON fixture file."""
    fixtures_path = tmp_path
    fixture_file = fixtures_path / "test_fixture.json"

    test_data = {"name": "test", "value": 42, "items": [1, 2, 3]}
    with fixture_file.open("w") as f:
        json.dump(test_data, f)

    result = open_fixture_sync(fixtures_path, "test_fixture")
    assert result == test_data


def test_open_fixture_gzipped(tmp_path: Path) -> None:
    """Test open_fixture_sync with gzipped JSON file."""
    fixtures_path = tmp_path
    fixture_file = fixtures_path / "test.json.gz"

    test_data = {"compressed": True, "data": [1, 2, 3]}
    with gzip.open(fixture_file, "wt", encoding="utf-8") as f:
        json.dump(test_data, f)

    result = open_fixture_sync(fixtures_path, "test")
    assert result == test_data


def test_open_fixture_zipped(tmp_path: Path) -> None:
    """Test open_fixture_sync with zipped JSON file."""
    fixtures_path = tmp_path
    fixture_file = fixtures_path / "test.json.zip"

    test_data = {"zipped": True, "values": ["a", "b", "c"]}
    with zipfile.ZipFile(fixture_file, "w") as zf:
        zf.writestr("test.json", json.dumps(test_data))

    result = open_fixture_sync(fixtures_path, "test")
    assert result == test_data


def test_open_fixture_missing_file(tmp_path: Path) -> None:
    """Test open_fixture_sync with missing fixture file."""
    fixtures_path = tmp_path

    with pytest.raises(FileNotFoundError, match="Could not find the nonexistent fixture"):
        open_fixture_sync(fixtures_path, "nonexistent")


def test_open_fixture_invalid_json(tmp_path: Path) -> None:
    """Test open_fixture_sync with invalid JSON."""
    fixtures_path = tmp_path
    fixture_file = fixtures_path / "invalid.json"

    with fixture_file.open("w") as f:
        f.write("{ invalid json content")

    with pytest.raises(Exception):
        open_fixture_sync(fixtures_path, "invalid")


async def test_open_fixture_async_valid_file(tmp_path: Path) -> None:
    """Test open_fixture_async with valid JSON fixture file."""
    fixtures_path = tmp_path
    fixture_file = fixtures_path / "test_async.json"

    test_data = {"async": True, "data": {"nested": "value"}}
    with fixture_file.open("w") as f:
        json.dump(test_data, f)

    result = await open_fixture_async(fixtures_path, "test_async")
    assert result == test_data


async def test_open_fixture_async_gzipped(tmp_path: Path) -> None:
    """Test open_fixture_async with gzipped file."""
    fixtures_path = tmp_path
    fixture_file = fixtures_path / "async_gz.json.gz"

    test_data = {"async_compressed": True, "numbers": [1, 2, 3, 4, 5]}
    with gzip.open(fixture_file, "wt", encoding="utf-8") as f:
        json.dump(test_data, f)

    result = await open_fixture_async(fixtures_path, "async_gz")
    assert result == test_data


async def test_open_fixture_async_zipped(tmp_path: Path) -> None:
    """Test open_fixture_async with zipped file."""
    fixtures_path = tmp_path
    fixture_file = fixtures_path / "async_zip.json.zip"

    test_data = {"async_zipped": True, "items": ["x", "y", "z"]}
    with zipfile.ZipFile(fixture_file, "w") as zf:
        zf.writestr("async_zip.json", json.dumps(test_data))

    result = await open_fixture_async(fixtures_path, "async_zip")
    assert result == test_data


async def test_open_fixture_async_missing_file(tmp_path: Path) -> None:
    """Test open_fixture_async with missing fixture file."""
    fixtures_path = tmp_path

    with pytest.raises(FileNotFoundError, match="Could not find the missing_async fixture"):
        await open_fixture_async(fixtures_path, "missing_async")


def test_write_fixture_dict(tmp_path: Path) -> None:
    """Test writing a dictionary fixture."""
    test_data: Any = {"name": "test", "value": 42, "active": True}

    write_fixture_sync(str(tmp_path), "test_dict", test_data)

    # Verify file was created
    fixture_file = tmp_path / "test_dict.json"
    assert fixture_file.exists()

    # Verify content
    loaded_data = open_fixture_sync(tmp_path, "test_dict")
    assert loaded_data == test_data


def test_write_fixture_list(tmp_path: Path) -> None:
    """Test writing a list fixture."""
    test_data: Any = [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]

    write_fixture_sync(str(tmp_path), "test_list", test_data)
    loaded_data = open_fixture_sync(tmp_path, "test_list")
    assert loaded_data == test_data


def test_write_fixture_compressed(tmp_path: Path) -> None:
    """Test writing a compressed fixture."""
    test_data: Any = {"compressed": True, "data": list(range(100))}

    write_fixture_sync(str(tmp_path), "test_compressed", test_data, compress=True)

    # Verify gzipped file was created
    fixture_file = tmp_path / "test_compressed.json.gz"
    assert fixture_file.exists()

    # Verify content can be read
    loaded_data = open_fixture_sync(tmp_path, "test_compressed")
    assert loaded_data == test_data


def test_write_fixture_storage_backend_error(tmp_path: Path) -> None:
    """Test error handling for invalid storage backend."""
    test_data: Any = {"test": "data"}

    with pytest.raises(ValueError, match="Failed to get storage backend"):
        write_fixture_sync(str(tmp_path), "test", test_data, storage_backend="invalid://backend")


@patch("sqlspec.utils.fixtures.storage_registry")
def test_write_fixture_with_custom_backend(mock_registry: Mock) -> None:
    """Test write_fixture_sync with custom storage backend."""
    mock_storage = Mock()
    mock_registry.get.return_value = mock_storage

    test_data: Any = {"custom": "backend"}
    write_fixture_sync("/tmp", "test", test_data, storage_backend="s3://bucket", custom_param="value")

    # Verify storage backend was called correctly
    mock_registry.get.assert_called_once_with("s3://bucket", custom_param="value")
    mock_storage.write_text_sync.assert_called_once()


async def test_write_fixture_async_dict(tmp_path: Path) -> None:
    """Test async writing a dictionary fixture."""
    test_data: Any = {"async_write": True, "value": 123}

    await write_fixture_async(str(tmp_path), "async_test", test_data)

    # Verify file was created and content is correct
    loaded_data = await open_fixture_async(tmp_path, "async_test")
    assert loaded_data == test_data


async def test_write_fixture_async_compressed(tmp_path: Path) -> None:
    """Test async writing a compressed fixture."""
    test_data: Any = {"async_compressed": True, "large_data": list(range(50))}

    await write_fixture_async(str(tmp_path), "async_compressed", test_data, compress=True)

    # Verify gzipped file was created
    fixture_file = tmp_path / "async_compressed.json.gz"
    assert fixture_file.exists()

    # Verify content
    loaded_data = await open_fixture_async(tmp_path, "async_compressed")
    assert loaded_data == test_data


async def test_write_fixture_async_storage_error(tmp_path: Path) -> None:
    """Test async error handling for invalid storage backend."""
    test_data: Any = {"test": "data"}

    with pytest.raises(ValueError, match="Failed to get storage backend"):
        await write_fixture_async(str(tmp_path), "test", test_data, storage_backend="invalid://backend")


@patch("sqlspec.utils.fixtures.storage_registry")
async def test_write_fixture_async_custom_backend(mock_registry: Mock) -> None:
    """Test write_fixture_async with a custom storage backend."""
    mock_storage = AsyncMock()
    mock_registry.get.return_value = mock_storage

    test_data: Any = {"async_custom": "backend"}
    await write_fixture_async(
        "/tmp", "async_test", test_data, storage_backend="gcs://bucket", custom_param="async_value"
    )

    # Verify storage backend was called correctly
    mock_registry.get.assert_called_once_with("gcs://bucket", custom_param="async_value")
    mock_storage.write_text_async.assert_called_once()


async def test_write_fixture_async_imports_without_anyio(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setitem(sys.modules, "anyio", None)
    sys.modules.pop("sqlspec.utils.fixtures", None)

    fixtures_module = importlib.import_module("sqlspec.utils.fixtures")
    await fixtures_module.write_fixture_async(str(tmp_path), "without_anyio", {"ok": True})

    assert (tmp_path / "without_anyio.json").exists()


def test_write_read_roundtrip(tmp_path: Path) -> None:
    """Test complete write and read roundtrip."""
    original_data: Any = {
        "users": [{"id": 1, "name": "Alice", "active": True}, {"id": 2, "name": "Bob", "active": False}],
        "metadata": {"version": "1.0", "created": "2024-01-01", "total_users": 2},
    }

    # Write fixture
    write_fixture_sync(str(tmp_path), "integration_test", original_data)

    # Read fixture back
    loaded_data = open_fixture_sync(tmp_path, "integration_test")

    # Verify data integrity
    assert loaded_data == original_data


async def test_async_write_read_roundtrip(tmp_path: Path) -> None:
    """Test complete async write and read roundtrip."""
    original_data: Any = {
        "async_test": True,
        "data": {"nested": {"deeply": {"value": 42}}},
        "list_data": [{"item": i} for i in range(10)],
    }

    # Write fixture async
    await write_fixture_async(str(tmp_path), "async_integration", original_data)

    # Read fixture back async
    loaded_data = await open_fixture_async(tmp_path, "async_integration")

    # Verify data integrity
    assert loaded_data == original_data


def test_compressed_roundtrip(tmp_path: Path) -> None:
    """Test write and read roundtrip with compression."""
    # Large data that benefits from compression
    original_data: Any = {
        "large_list": [{"id": i, "data": f"item_{i}" * 10} for i in range(100)],
        "repeated_data": ["same_string"] * 50,
    }

    # Write compressed
    write_fixture_sync(str(tmp_path), "compressed_test", original_data, compress=True)

    # Read back
    loaded_data = open_fixture_sync(tmp_path, "compressed_test")

    # Verify data integrity
    assert loaded_data == original_data

    # Verify file is actually compressed
    compressed_file = tmp_path / "compressed_test.json.gz"
    assert compressed_file.exists()
    assert compressed_file.suffix == ".gz"


def _write_table_fixture_files(fixtures_path: Path) -> None:
    fixtures_path.mkdir(parents=True, exist_ok=True)
    (fixtures_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    with gzip.open(fixtures_path / "posts.jsonl.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(row) for row in POST_ROWS) + "\n")


def _normalized(rows: "list[dict[str, Any]]") -> "list[dict[str, Any]]":
    return sorted(from_json(to_json(rows)), key=lambda row: row["id"])


def test_load_table_fixtures_orders_and_counts(tmp_path: Path) -> None:
    """Every discovered table loads in the requested order and reports row counts."""
    _write_table_fixture_files(tmp_path)
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        for ddl in TABLE_FIXTURE_DDL:
            driver.execute(ddl)

        counts = load_table_fixtures_sync(driver, tmp_path, table_order=["users", "posts"])

        assert counts == {"users": 2, "posts": 3}
        assert list(counts) == ["users", "posts"]
        assert _normalized(driver.select("SELECT * FROM users")) == USER_ROWS
        assert _normalized(driver.select("SELECT * FROM posts")) == POST_ROWS
    config.close_pool()


async def test_load_table_fixtures_orders_and_counts_async(tmp_path: Path) -> None:
    """The async loader discovers, orders, and counts tables like the sync loader."""
    _write_table_fixture_files(tmp_path)
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    async with config.provide_session() as driver:
        for ddl in TABLE_FIXTURE_DDL:
            await driver.execute(ddl)

        counts = await load_table_fixtures_async(driver, str(tmp_path), table_order=["users", "posts"], batch_size=2)

        assert counts == {"users": 2, "posts": 3}
        assert list(counts) == ["users", "posts"]
        assert _normalized(await driver.select("SELECT * FROM users")) == USER_ROWS
        assert _normalized(await driver.select("SELECT * FROM posts")) == POST_ROWS
    await config.close_pool()


def test_load_table_fixtures_subset_loads_unlisted_tables_alphabetically(tmp_path: Path) -> None:
    """Only requested tables load, listed ones first and the rest alphabetically."""
    _write_table_fixture_files(tmp_path)
    (tmp_path / "audit.jsonl").write_text('{"id": 1}\n\n{"id": 2}\n', encoding="utf-8")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        driver.execute("CREATE TABLE audit (id INTEGER PRIMARY KEY)")

        counts = load_table_fixtures_sync(
            driver, tmp_path, tables=["users", "audit"], table_order=["users", "missing"], batch_size=1
        )

        assert list(counts.items()) == [("users", 2), ("audit", 2)]
        assert driver.select_value("SELECT COUNT(*) FROM audit") == 2
    config.close_pool()


def test_load_upserts_with_conflict_keys(tmp_path: Path) -> None:
    """Conflict keys turn a reload into an update of existing rows."""
    _write_table_fixture_files(tmp_path)
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        for ddl in TABLE_FIXTURE_DDL:
            driver.execute(ddl)
        load_table_fixtures_sync(driver, tmp_path, tables=["users"])

        edited = [{"id": 1, "name": "Alicia"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Cara"}]
        (tmp_path / "users.json").write_text(json.dumps(edited), encoding="utf-8")
        counts = load_table_fixtures_sync(driver, tmp_path, tables=["users"], conflict_keys={"users": ["id"]})

        assert counts == {"users": 3}
        assert _normalized(driver.select("SELECT * FROM users")) == edited
    config.close_pool()


async def test_load_upserts_with_conflict_keys_async(tmp_path: Path) -> None:
    """The async loader upserts on conflict keys."""
    _write_table_fixture_files(tmp_path)
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    async with config.provide_session() as driver:
        for ddl in TABLE_FIXTURE_DDL:
            await driver.execute(ddl)
        await load_table_fixtures_async(driver, tmp_path, tables=["users"])

        edited = [{"id": 1, "name": "Alicia"}, {"id": 2, "name": "Bob"}]
        (tmp_path / "users.json").write_text(json.dumps(edited), encoding="utf-8")
        counts = await load_table_fixtures_async(driver, tmp_path, tables=["users"], conflict_keys={"users": ["id"]})

        assert counts == {"users": 2}
        assert _normalized(await driver.select("SELECT * FROM users")) == edited
    await config.close_pool()


def test_load_without_conflict_keys_rejects_duplicates(tmp_path: Path) -> None:
    """Reloading existing rows without conflict keys surfaces the database error."""
    _write_table_fixture_files(tmp_path)
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        for ddl in TABLE_FIXTURE_DDL:
            driver.execute(ddl)
        load_table_fixtures_sync(driver, tmp_path, tables=["users"])

        with pytest.raises(Exception, match="UNIQUE"):
            load_table_fixtures_sync(driver, tmp_path, tables=["users"])
    config.close_pool()


@pytest.mark.parametrize(("compress", "jsonl"), [(True, False), (False, True), (True, True), (False, False)])
def test_export_load_roundtrip(tmp_path: Path, compress: bool, jsonl: bool) -> None:
    """Exported table fixtures reload into a fresh database unchanged."""
    source_path = tmp_path / "source"
    export_path = tmp_path / "export"
    _write_table_fixture_files(source_path)
    source = SqliteConfig(connection_config={"database": ":memory:"})
    target = SqliteConfig(connection_config={"database": ":memory:"})
    with source.provide_session() as driver:
        for ddl in TABLE_FIXTURE_DDL:
            driver.execute(ddl)
        load_table_fixtures_sync(driver, source_path, table_order=["users", "posts"])

        written = export_table_fixtures_sync(driver, export_path, ["users", "posts"], compress=compress, jsonl=jsonl)

    suffix = (".jsonl" if jsonl else ".json") + (".gz" if compress else "")
    assert written == {"users": 2, "posts": 3}
    assert sorted(path.name for path in export_path.iterdir()) == [f"posts{suffix}", f"users{suffix}"]

    with target.provide_session() as driver:
        for ddl in TABLE_FIXTURE_DDL:
            driver.execute(ddl)
        counts = load_table_fixtures_sync(driver, export_path, table_order=["users", "posts"])

        assert counts == {"users": 2, "posts": 3}
        assert _normalized(driver.select("SELECT * FROM users")) == USER_ROWS
        assert _normalized(driver.select("SELECT * FROM posts")) == POST_ROWS
    source.close_pool()
    target.close_pool()


async def test_export_load_roundtrip_async(tmp_path: Path) -> None:
    """The async exporter writes fixtures the async loader reloads unchanged."""
    source_path = tmp_path / "source"
    export_path = tmp_path / "export"
    _write_table_fixture_files(source_path)
    source = AiosqliteConfig(connection_config={"database": ":memory:"})
    target = AiosqliteConfig(connection_config={"database": ":memory:"})
    async with source.provide_session() as driver:
        for ddl in TABLE_FIXTURE_DDL:
            await driver.execute(ddl)
        await load_table_fixtures_async(driver, source_path, table_order=["users", "posts"])

        written = await export_table_fixtures_async(driver, str(export_path), ["users", "posts"])

    assert written == {"users": 2, "posts": 3}
    assert sorted(path.name for path in export_path.iterdir()) == ["posts.json.gz", "users.json.gz"]

    async with target.provide_session() as driver:
        for ddl in TABLE_FIXTURE_DDL:
            await driver.execute(ddl)
        counts = await load_table_fixtures_async(driver, export_path, table_order=["users", "posts"])

        assert counts == {"users": 2, "posts": 3}
        assert _normalized(await driver.select("SELECT * FROM users")) == USER_ROWS
        assert _normalized(await driver.select("SELECT * FROM posts")) == POST_ROWS
    await source.close_pool()
    await target.close_pool()


def test_export_replaces_other_fixture_variants_of_the_table(tmp_path: Path) -> None:
    """Exporting a table removes its other fixture files so a reload reads the export."""
    (tmp_path / "users.json").write_text(json.dumps([{"id": 99, "name": "Stale"}]), encoding="utf-8")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        driver.execute_many("INSERT INTO users (id, name) VALUES (:id, :name)", USER_ROWS)

        export_table_fixtures_sync(driver, tmp_path, ["users"], compress=True)

    assert [path.name for path in tmp_path.iterdir()] == ["users.json.gz"]
    config.close_pool()


@pytest.mark.parametrize(
    ("file_name", "content", "tables", "match"),
    [
        ("users;drop.json", "[]", ["users;drop"], "Invalid table name"),
        ("users.jsonl", "", None, "more than one fixture file"),
        ("users.json", '[{"id": 1, "name) VALUES (1); --": "x"}]', None, "Invalid column name"),
        ("users.json", '[{"id": 1}, {}]', None, "at least one column"),
        ("users.json", "[1, 2]", None, "must be an object"),
        ("users.json", "[{}]", None, "at least one column"),
        ("users.json", '{"id": 1}', None, "JSON array"),
        ("users.json", "[]", ["other"], "Could not find the other fixture"),
    ],
)
def test_load_table_fixtures_rejects_invalid_input(
    tmp_path: Path, file_name: str, content: str, tables: "list[str] | None", match: str
) -> None:
    """Unsafe identifiers, malformed rows, and missing files raise before any insert."""
    (tmp_path / file_name).write_text(content, encoding="utf-8")
    if file_name == "users.jsonl":
        (tmp_path / "users.json.gz").write_bytes(gzip.compress(b"[]"))
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

        with pytest.raises((ValueError, TypeError, FileNotFoundError), match=match):
            load_table_fixtures_sync(driver, tmp_path, tables=tables)

        assert driver.select_value("SELECT COUNT(*) FROM users") == 0
    config.close_pool()


def test_table_fixture_helpers_reject_invalid_arguments(tmp_path: Path) -> None:
    """Invalid batch sizes, conflict keys, and export table names raise ValueError."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

        with pytest.raises(ValueError, match="batch_size"):
            load_table_fixtures_sync(driver, tmp_path, batch_size=0)
        with pytest.raises(ValueError, match="Invalid column name"):
            load_table_fixtures_sync(driver, tmp_path, conflict_keys={"users": ["id = 1 --"]})
        with pytest.raises(ValueError, match="Invalid table name"):
            export_table_fixtures_sync(driver, tmp_path, ["users where 1=1"])
    config.close_pool()


def test_resync_noop_on_duckdb(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Sequence resync is skipped with a debug log on dialects without monotonic table sequences."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    caplog.set_level(logging.DEBUG, logger="sqlspec.utils.fixtures")
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

        counts = load_table_fixtures_sync(driver, tmp_path, resync_sequences=True)

        assert counts == {"users": 2}
        assert _normalized(driver.select("SELECT * FROM users")) == USER_ROWS
    config.close_pool()
    assert any("resync" in record.getMessage() and record.levelno == logging.DEBUG for record in caplog.records)


def test_resync_on_sqlite(tmp_path: Path) -> None:
    """SQLite sequence resynchronization runs and updates sqlite_sequence."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")

        counts = load_table_fixtures_sync(driver, tmp_path, resync_sequences=True)

        assert counts == {"users": 2}
        assert _normalized(driver.select("SELECT * FROM users")) == USER_ROWS
        res = driver.select_value("SELECT seq FROM sqlite_sequence WHERE name = 'users'")
        assert res == 2
    config.close_pool()


async def test_resync_on_sqlite_async(tmp_path: Path) -> None:
    """The async loader resynchronizes SQLite sequences."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    async with config.provide_session() as driver:
        await driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")

        counts = await load_table_fixtures_async(driver, tmp_path, resync_sequences=True)

        assert counts == {"users": 2}
        res = await driver.select_value("SELECT seq FROM sqlite_sequence WHERE name = 'users'")
        assert res == 2
    await config.close_pool()


def test_load_table_fixtures_discovery_skips_non_identifier_files(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Discovery ignores files whose names are not table identifiers and logs them."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    (tmp_path / "notes-draft.json").write_text("[]", encoding="utf-8")
    caplog.set_level(logging.DEBUG, logger="sqlspec.utils.fixtures")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

        assert load_table_fixtures_sync(driver, tmp_path) == {"users": 2}
    config.close_pool()
    assert any("notes-draft.json" in record.getMessage() for record in caplog.records)


def test_table_fixtures_quote_reserved_and_mixed_case_identifiers(tmp_path: Path) -> None:
    """Reserved-word and mixed-case table and column names load, upsert, and export."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    rows = [{"id": 1, "group": "a", "userName": "Ann"}, {"id": 2, "group": "b", "userName": "Ben"}]
    (tmp_path / "order.json").write_text(json.dumps(rows), encoding="utf-8")
    with config.provide_session() as driver:
        driver.execute('CREATE TABLE "order" ("id" INTEGER PRIMARY KEY, "group" TEXT, "userName" TEXT)')

        load_table_fixtures_sync(driver, tmp_path)
        (tmp_path / "order.json").write_text(json.dumps([{"id": 1, "group": "z", "userName": "Ann"}]), encoding="utf-8")
        load_table_fixtures_sync(driver, tmp_path, conflict_keys={"order": ["id"]})
        written = export_table_fixtures_sync(driver, tmp_path / "out", ["order"], compress=False)

    assert written == {"order": 2}
    assert json.loads((tmp_path / "out" / "order.json").read_text()) == [
        {"id": 1, "group": "z", "userName": "Ann"},
        {"id": 2, "group": "b", "userName": "Ben"},
    ]
    config.close_pool()


def test_export_orders_rows_by_primary_key(tmp_path: Path) -> None:
    """Exported rows follow the primary key so repeated exports produce identical files."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE items (id INTEGER, code TEXT PRIMARY KEY)")
        driver.execute_many(
            "INSERT INTO items (id, code) VALUES (:id, :code)",
            [{"id": 1, "code": "c"}, {"id": 2, "code": "a"}, {"id": 3, "code": "b"}],
        )

        export_table_fixtures_sync(driver, tmp_path, ["items"], compress=False, jsonl=True)

    lines = (tmp_path / "items.jsonl").read_text().splitlines()
    assert [json.loads(line)["code"] for line in lines] == ["a", "b", "c"]
    config.close_pool()


def test_export_load_roundtrip_sqlite_blob(tmp_path: Path) -> None:
    """BLOB values are written as base64 text and decoded back to bytes on load."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE files (id INTEGER PRIMARY KEY, payload BLOB, amount NUMERIC)")
        driver.execute("INSERT INTO files VALUES (1, x'00ff10', 1.5)")
        before = driver.select("SELECT * FROM files")

        export_table_fixtures_sync(driver, tmp_path, ["files"], compress=False)
        driver.execute("DELETE FROM files")
        load_table_fixtures_sync(driver, tmp_path)

        assert json.loads((tmp_path / "files.json").read_text())[0]["payload"] == "AP8Q"
        assert driver.select("SELECT * FROM files") == before
        assert driver.select_value("SELECT typeof(payload) FROM files") == "blob"
    config.close_pool()


def test_export_load_roundtrip_duckdb_types(tmp_path: Path) -> None:
    """DuckDB timestamp, date, decimal, uuid, and blob columns round-trip and upsert."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "typed.duckdb")})
    ddl = (
        'CREATE TABLE "Typed" (id INTEGER PRIMARY KEY, ts TIMESTAMP, tstz TIMESTAMPTZ, d DATE, t TIME, '
        'amount DECIMAL(10, 2), u UUID, payload BLOB, "userName" VARCHAR)'
    )
    with config.provide_session() as driver:
        driver.execute(ddl)
        driver.execute(
            "INSERT INTO \"Typed\" VALUES (1, TIMESTAMP '2024-01-02 03:04:05.123456', "
            "TIMESTAMPTZ '2024-01-02 03:04:05+00', DATE '2024-01-02', TIME '03:04:05', 12.50, "
            "'7f3c1d2e-9a4b-4c5d-8e6f-0a1b2c3d4e5f', '\\x00\\xFF'::BLOB, 'Ann')"
        )
        before = driver.select('SELECT * FROM "Typed"')

        export_table_fixtures_sync(driver, tmp_path, ["Typed"])
        driver.execute('DELETE FROM "Typed"')
        load_table_fixtures_sync(driver, tmp_path)
        after = driver.select('SELECT * FROM "Typed"')
        load_table_fixtures_sync(driver, tmp_path, conflict_keys={"Typed": ["id"]})

        assert after == before
        assert isinstance(after[0]["amount"], Decimal)
        assert isinstance(after[0]["u"], UUID)
        assert after[0]["payload"] == b"\x00\xff"
        assert driver.select_value('SELECT count(*) FROM "Typed"') == 1
    config.close_pool()


def test_conflict_keys_rejected_for_unsupported_dialect(tmp_path: Path) -> None:
    """Conflict keys on a dialect without an upsert form raise before any statement runs."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    driver = MagicMock()
    driver.statement_config.dialect = "tsql"

    with pytest.raises(ValueError, match="conflict_keys is not supported for dialect 'tsql'"):
        load_table_fixtures_sync(driver, tmp_path, conflict_keys={"users": ["id"]})

    driver.execute_many.assert_not_called()
    driver.select.assert_not_called()
    driver.data_dictionary.get_columns.assert_not_called()


def test_conflict_keys_render_duplicate_key_update_for_mysql(tmp_path: Path) -> None:
    """MySQL upserts use ON DUPLICATE KEY UPDATE with quoted identifiers."""
    (tmp_path / "order.json").write_text(json.dumps([{"id": 1, "userName": "Ann"}]), encoding="utf-8")
    driver = MagicMock()
    driver.statement_config.dialect = "mysql"
    driver.data_dictionary.get_columns.return_value = []

    load_table_fixtures_sync(driver, tmp_path, conflict_keys={"order": ["id"]})

    driver.data_dictionary.get_columns.assert_called_once_with(driver, table="order", schema=None)
    driver.select.assert_not_called()

    statement = driver.execute_many.call_args.args[0]
    assert statement == (
        "INSERT INTO `order` (`id`, `userName`) VALUES (:id, :userName) "
        "ON DUPLICATE KEY UPDATE `userName` = VALUES(`userName`)"
    )


def test_export_keeps_existing_files_when_write_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed export leaves the previous fixture files in place and no temporary files."""
    (tmp_path / "users.json").write_text(json.dumps([{"id": 99, "name": "Stale"}]), encoding="utf-8")

    def _fail_replace(self: Path, target: Any) -> Path:
        raise OSError("disk full")

    monkeypatch.setattr(Path, "replace", _fail_replace)
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        driver.execute_many("INSERT INTO users (id, name) VALUES (:id, :name)", USER_ROWS)

        with pytest.raises(OSError, match="disk full"):
            export_table_fixtures_sync(driver, tmp_path, ["users"], compress=True)

    assert [path.name for path in tmp_path.iterdir()] == ["users.json"]
    config.close_pool()


def _postgres_mock_driver(columns: "list[dict[str, Any]]") -> MagicMock:
    driver = MagicMock()
    driver.statement_config.dialect = "postgres"
    driver.data_dictionary.get_columns.return_value = columns
    return driver


ALWAYS_IDENTITY_COLUMNS: "list[dict[str, Any]]" = [
    {
        "column_name": "id",
        "data_type": "integer",
        "is_primary": True,
        "identity_generation": "ALWAYS",
        "sequence_name": 'app."Users_id_seq"',
    },
    {
        "column_name": "email",
        "data_type": "text",
        "is_primary": False,
        "identity_generation": "",
        "sequence_name": None,
    },
    {"column_name": "name", "data_type": "text", "is_primary": False, "identity_generation": "", "sequence_name": None},
]


def test_postgres_always_identity_columns_use_overriding_and_are_not_updated(tmp_path: Path) -> None:
    """Explicit identity values use OVERRIDING SYSTEM VALUE and never appear in the upsert SET list."""
    rows = [{"id": 1, "email": "a@example.com", "name": "Ann"}]
    (tmp_path / "app.Users.json").write_text(json.dumps(rows), encoding="utf-8")
    driver = _postgres_mock_driver(ALWAYS_IDENTITY_COLUMNS)

    load_table_fixtures_sync(driver, tmp_path, conflict_keys={"app.Users": ["email"]}, resync_sequences=True)

    driver.data_dictionary.get_columns.assert_called_once_with(driver, table='"Users"', schema='"app"')
    statement = driver.execute_many.call_args.args[0]
    assert statement == (
        'INSERT INTO "app"."Users" ("id", "email", "name") OVERRIDING SYSTEM VALUE '
        "VALUES (%(id)s, %(email)s, %(name)s) "
        'ON CONFLICT("email") DO UPDATE SET "name" = "excluded"."name"'
    )
    resync_statement, resync_parameters = driver.execute.call_args.args
    assert 'max("id")' in resync_statement
    assert resync_parameters == {"sequence_name": 'app."Users_id_seq"'}


def test_postgres_upsert_with_only_identity_and_key_columns_does_nothing_on_conflict(tmp_path: Path) -> None:
    """An upsert whose only non-key column is an always-identity column skips updates on conflict."""
    (tmp_path / "users.json").write_text(json.dumps([{"id": 1, "email": "a@example.com"}]), encoding="utf-8")
    driver = _postgres_mock_driver(ALWAYS_IDENTITY_COLUMNS)

    load_table_fixtures_sync(driver, tmp_path, conflict_keys={"users": ["email"]})

    statement = driver.execute_many.call_args.args[0]
    assert statement.endswith('ON CONFLICT("email") DO NOTHING')


def test_conflict_keys_for_tables_not_loaded_raise(tmp_path: Path) -> None:
    """Conflict keys for unloaded tables are ignored while loaded tables still upsert."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    driver = _postgres_mock_driver([])

    counts = load_table_fixtures_sync(driver, tmp_path, conflict_keys={"users": "id", "Users": ["id"], "posts": ["id"]})

    assert counts == {"users": 2}
    assert driver.execute_many.call_args.args[0] == (
        'INSERT INTO "users" ("id", "name") VALUES (%(id)s, %(name)s) '
        'ON CONFLICT("id") DO UPDATE SET "name" = "excluded"."name"'
    )


def test_export_without_primary_key_orders_by_every_column(tmp_path: Path) -> None:
    """Tables without a primary key export rows ordered by all columns."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE pairs (a INTEGER, b TEXT)")
        driver.execute("INSERT INTO pairs VALUES (2, 'x'), (1, 'y'), (1, 'a')")

        export_table_fixtures_sync(driver, tmp_path, ["pairs"], compress=False)

    assert json.loads((tmp_path / "pairs.json").read_text()) == [
        {"a": 1, "b": "a"},
        {"a": 1, "b": "y"},
        {"a": 2, "b": "x"},
    ]
    config.close_pool()


def test_discovery_logs_schema_qualified_table_names(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Dotted fixture file names are treated as schema-qualified tables and logged."""
    (tmp_path / "users.backup.json").write_text("[]", encoding="utf-8")
    caplog.set_level(logging.DEBUG, logger="sqlspec.utils.fixtures")
    driver = MagicMock()
    driver.statement_config.dialect = "tsql"

    assert load_table_fixtures_sync(driver, tmp_path) == {"users.backup": 0}
    assert any("'users.backup'" in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize(("umask", "expected_mode"), [(0o022, 0o644), (0o077, 0o600)])
def test_export_file_mode_follows_umask(tmp_path: Path, umask: int, expected_mode: int) -> None:
    """Exported files get the default permissions allowed by the process umask."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    previous = os.umask(umask)
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            export_table_fixtures_sync(driver, tmp_path, ["users"], compress=False)
    finally:
        os.umask(previous)
        config.close_pool()

    assert (tmp_path / "users.json").stat().st_mode & 0o777 == expected_mode


def test_values_of_unlisted_types_pass_through_unchanged(tmp_path: Path) -> None:
    """Only known binary type names decode base64; other column types keep their JSON values."""
    (tmp_path / "users.json").write_text(json.dumps([{"id": 1, "name": "AAE="}]), encoding="utf-8")
    driver = _postgres_mock_driver([
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "name", "data_type": "blobfish", "is_primary": False},
    ])

    load_table_fixtures_sync(driver, tmp_path)

    assert driver.execute_many.call_args.args[1] == [{"id": 1, "name": "AAE="}]


@pytest.mark.parametrize(
    ("dialect", "generated_column", "expected_generated"),
    [
        ("postgres", {"is_generated": True}, True),
        ("postgres", {"is_generated": False}, False),
        ("postgres", {"generation_expression": "id * 2"}, True),
        ("postgres", {"generation_expression": ""}, False),
        ("duckdb", {"is_generated": True}, True),
        ("mysql", {"extra": "VIRTUAL GENERATED"}, True),
        ("mysql", {"extra": "STORED GENERATED"}, True),
        ("mysql", {"extra": "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"}, False),
        ("sqlite", {"extra": "generated"}, True),
        ("sqlite", {"extra": "hidden"}, False),
    ],
)
def test_generated_columns_are_detected_from_column_metadata(
    tmp_path: Path, dialect: str, generated_column: "dict[str, Any]", expected_generated: bool
) -> None:
    """Generated columns are left out of loaded inserts and exported rows for each dialect's metadata."""
    (tmp_path / "items.json").write_text(json.dumps([{"id": 1, "doubled": 2}]), encoding="utf-8")
    driver = MagicMock()
    driver.statement_config.dialect = dialect
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "doubled", "data_type": "integer", "is_primary": False, **generated_column},
    ]
    driver.select.return_value = [{"id": 1, "doubled": 2}]

    load_table_fixtures_sync(driver, tmp_path)
    export_table_fixtures_sync(driver, tmp_path / "out", ["items"], compress=False)

    expected_row = {"id": 1} if expected_generated else {"id": 1, "doubled": 2}
    assert driver.execute_many.call_args.args[1] == [expected_row]
    assert json.loads((tmp_path / "out" / "items.json").read_text()) == [expected_row]


def test_load_rejects_rows_with_only_generated_columns(tmp_path: Path) -> None:
    """A fixture file holding only generated column values raises before any insert."""
    (tmp_path / "items.json").write_text(json.dumps([{"doubled": 2}]), encoding="utf-8")
    driver = _postgres_mock_driver([
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "doubled", "data_type": "integer", "is_generated": True},
    ])

    with pytest.raises(ValueError, match="only contain generated columns"):
        load_table_fixtures_sync(driver, tmp_path)

    driver.execute_many.assert_not_called()


def test_sqlite_generated_columns_round_trip(tmp_path: Path) -> None:
    """SQLite virtual and stored generated columns are skipped on export and on load."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute(
            "CREATE TABLE gen (id INTEGER PRIMARY KEY, name TEXT, "
            "doubled INTEGER GENERATED ALWAYS AS (id * 2) VIRTUAL, "
            "label TEXT GENERATED ALWAYS AS (name || '!') STORED)"
        )
        driver.execute("INSERT INTO gen (id, name) VALUES (1, 'a'), (2, 'b')")
        before = driver.select("SELECT * FROM gen ORDER BY id")

        export_table_fixtures_sync(driver, tmp_path, ["gen"], compress=False)
        exported = json.loads((tmp_path / "gen.json").read_text())
        driver.execute("DELETE FROM gen")
        load_table_fixtures_sync(driver, tmp_path)
        after_export = driver.select("SELECT * FROM gen ORDER BY id")
        (tmp_path / "gen.json").write_text(json.dumps(before), encoding="utf-8")
        driver.execute("DELETE FROM gen")
        load_table_fixtures_sync(driver, tmp_path, conflict_keys={"gen": ["id"]})
        after_full_rows = driver.select("SELECT * FROM gen ORDER BY id")

    assert exported == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert after_export == before
    assert after_full_rows == before
    config.close_pool()


async def test_aiosqlite_generated_columns_round_trip(tmp_path: Path) -> None:
    """The async helpers skip generated columns on export and on load."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    async with config.provide_session() as driver:
        await driver.execute(
            "CREATE TABLE gen (id INTEGER PRIMARY KEY, doubled INTEGER GENERATED ALWAYS AS (id * 2) STORED)"
        )
        await driver.execute("INSERT INTO gen (id) VALUES (1), (2)")
        before = await driver.select("SELECT * FROM gen ORDER BY id")

        await export_table_fixtures_async(driver, tmp_path, ["gen"], compress=False)
        exported = json.loads((tmp_path / "gen.json").read_text())
        await driver.execute("DELETE FROM gen")
        (tmp_path / "gen.json").write_text(json.dumps(before), encoding="utf-8")
        await load_table_fixtures_async(driver, tmp_path)

        assert exported == [{"id": 1}, {"id": 2}]
        assert await driver.select("SELECT * FROM gen ORDER BY id") == before
    await config.close_pool()


def test_duckdb_generated_columns_round_trip(tmp_path: Path) -> None:
    """DuckDB generated columns are skipped on export and on load, including files that contain them."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "generated.duckdb")})
    fixtures_path = tmp_path / "fixtures"
    with config.provide_session() as driver:
        driver.execute(
            'CREATE TABLE "Gen" (id INTEGER PRIMARY KEY, "userName" VARCHAR, '
            "doubled INTEGER GENERATED ALWAYS AS (id * 2) VIRTUAL, "
            '"Shout" VARCHAR AS (upper("userName")))'
        )
        driver.execute("INSERT INTO \"Gen\" (id, \"userName\") VALUES (1, 'ann'), (2, 'ben')")
        before = driver.select('SELECT * FROM "Gen" ORDER BY id')

        export_table_fixtures_sync(driver, fixtures_path, ["Gen"], compress=False)
        exported = json.loads((fixtures_path / "Gen.json").read_text())
        driver.execute('DELETE FROM "Gen"')
        load_table_fixtures_sync(driver, fixtures_path)
        after_export = driver.select('SELECT * FROM "Gen" ORDER BY id')
        (fixtures_path / "Gen.json").write_text(json.dumps(before), encoding="utf-8")
        load_table_fixtures_sync(driver, fixtures_path, conflict_keys={"Gen": ["id"]})
        after_upsert = driver.select('SELECT * FROM "Gen" ORDER BY id')

    assert exported == [{"id": 1, "userName": "ann"}, {"id": 2, "userName": "ben"}]
    assert after_export == before
    assert after_upsert == before
    config.close_pool()


def test_duckdb_interval_columns_round_trip(tmp_path: Path) -> None:
    """DuckDB INTERVAL values exported as month, day, and nanosecond lists load back unchanged."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "interval.duckdb")})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE spans (id INTEGER PRIMARY KEY, span INTERVAL)")
        driver.execute(
            "INSERT INTO spans VALUES (1, INTERVAL '14 months 2 days 3 seconds'), "
            "(2, -INTERVAL '1 day 5 microseconds'), (3, NULL)"
        )
        before = driver.select("SELECT * FROM spans ORDER BY id")

        export_table_fixtures_sync(driver, tmp_path, ["spans"])
        driver.execute("DELETE FROM spans")
        load_table_fixtures_sync(driver, tmp_path)

        assert driver.select("SELECT * FROM spans ORDER BY id") == before
    config.close_pool()


def test_export_order_skips_arrays_of_unorderable_types(tmp_path: Path) -> None:
    """Arrays of unorderable PostgreSQL types are left out of the export ordering like their element types."""
    driver = _postgres_mock_driver([
        {"column_name": "id", "data_type": "integer", "is_primary": False},
        {"column_name": "docs", "data_type": "json[]", "is_primary": False},
        {"column_name": "points", "data_type": "point[]", "is_primary": False},
        {"column_name": "note", "data_type": "json", "is_primary": False},
        {"column_name": "names", "data_type": "text[]", "is_primary": False},
    ])
    driver.select.return_value = []

    export_table_fixtures_sync(driver, tmp_path, ["items"])

    assert driver.select.call_args.args[0] == (
        'SELECT "id", "docs", "points", CAST("note" AS TEXT) AS "note", "names" FROM "items" ORDER BY "id", "names"'
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("P1DT5S", timedelta(days=1, seconds=5)),
        ("-PT86395S", timedelta(days=-1, seconds=5)),
        ("PT7200.000005S", timedelta(hours=2, microseconds=5)),
        ("P2W", timedelta(weeks=2)),
        ("PT1H30M", timedelta(hours=1, minutes=30)),
        ("P0D", timedelta(0)),
        (90, timedelta(seconds=90)),
        (1.5, timedelta(seconds=1, microseconds=500000)),
        (True, True),
        ("1 day", "1 day"),
        ("P", "P"),
        ([0, 1, 0], [0, 1, 0]),
    ],
)
def test_decode_interval(value: Any, expected: Any) -> None:
    """ISO 8601 durations and numbers of seconds decode to timedeltas; other values pass through."""
    assert fixture_module._decode_interval(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ([14, 2, 3_000_000_000], "14 months 2 days 3000000 microseconds"),
        ([0, -1, -5000], "0 months -1 days -5 microseconds"),
        ("P1D", timedelta(days=1)),
        ([1, 2], [1, 2]),
    ],
)
def test_decode_duckdb_interval(value: Any, expected: Any) -> None:
    """DuckDB month-day-nanosecond lists decode to interval text and other values decode like intervals."""
    assert fixture_module._decode_duckdb_interval(value) == expected


def test_interval_and_time_with_time_zone_columns_are_decoded(tmp_path: Path) -> None:
    """PostgreSQL interval and time with time zone values decode to timedelta and offset-aware time."""
    rows = [{"id": 1, "span": "P1DT5.5S", "at": "03:04:05+02:00", "seconds": 30}]
    (tmp_path / "items.json").write_text(json.dumps(rows), encoding="utf-8")
    driver = _postgres_mock_driver([
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "span", "data_type": "interval", "is_primary": False},
        {"column_name": "at", "data_type": "time with time zone", "is_primary": False},
        {"column_name": "seconds", "data_type": "interval second", "is_primary": False},
    ])

    load_table_fixtures_sync(driver, tmp_path)

    assert driver.execute_many.call_args.args[1] == [
        {
            "id": 1,
            "span": timedelta(days=1, seconds=5, microseconds=500000),
            "at": time(3, 4, 5, tzinfo=timezone(timedelta(hours=2))),
            "seconds": timedelta(seconds=30),
        }
    ]


def test_export_load_roundtrip_duckdb_json_scalars_and_objects(tmp_path: Path) -> None:
    """DuckDB JSON columns with scalars, objects, arrays, and NULLs export without double-encoding and reload cleanly."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "json_test.duckdb")})
    export_dir = tmp_path / "exported"
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE app_config (id INTEGER PRIMARY KEY, key VARCHAR NOT NULL, val JSON)")
        driver.execute(
            "INSERT INTO app_config VALUES "
            "(1, 'theme', '\"console\"'::JSON), "
            "(2, 'enabled', 'true'::JSON), "
            "(3, 'count', '42'::JSON), "
            "(4, 'meta', '{\"nested\": true}'::JSON), "
            "(5, 'tags', '[\"a\", \"b\"]'::JSON), "
            "(6, 'empty', NULL)"
        )
        before = driver.select("SELECT * FROM app_config ORDER BY id")

        export_table_fixtures_sync(driver, export_dir, ["app_config"], compress=False, jsonl=True)
        exported_rows = [json.loads(line) for line in (export_dir / "app_config.jsonl").read_text().splitlines()]
        assert exported_rows == [
            {"id": 1, "key": "theme", "val": "console"},
            {"id": 2, "key": "enabled", "val": True},
            {"id": 3, "key": "count", "val": 42},
            {"id": 4, "key": "meta", "val": {"nested": True}},
            {"id": 5, "key": "tags", "val": ["a", "b"]},
            {"id": 6, "key": "empty", "val": None},
        ]

        driver.execute("DELETE FROM app_config")
        counts = load_table_fixtures_sync(driver, export_dir, conflict_keys={"app_config": "id"})
        after = driver.select("SELECT * FROM app_config ORDER BY id")

        assert counts == {"app_config": 6}
        assert [json.loads(row["val"]) if row["val"] is not None else None for row in after] == [
            json.loads(row["val"]) if row["val"] is not None else None for row in before
        ]
    config.close_pool()


async def test_export_decodes_json_and_jsonb_string_cells_with_fallback_async(tmp_path: Path) -> None:
    """Async export decodes valid JSON string cells on json/jsonb columns and preserves non-JSON strings."""
    driver = AsyncMock()
    driver.statement_config.dialect = "postgres"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "payload", "data_type": "jsonb", "is_primary": False},
        {"column_name": "raw_json", "data_type": "json", "is_primary": False},
    ]
    driver.select.return_value = [
        {"id": 1, "payload": '{"a": 1}', "raw_json": '"scalar"'},
        {"id": 2, "payload": "not-valid-json", "raw_json": None},
    ]

    await export_table_fixtures_async(driver, tmp_path, ["events"], compress=False)

    assert driver.select.call_args.args[0] == (
        'SELECT "id", CAST("payload" AS TEXT) AS "payload", CAST("raw_json" AS TEXT) AS "raw_json" '
        'FROM "events" ORDER BY "id"'
    )
    exported = json.loads((tmp_path / "events.json").read_text())
    assert exported == [
        {"id": 1, "payload": {"a": 1}, "raw_json": "scalar"},
        {"id": 2, "payload": "not-valid-json", "raw_json": None},
    ]


@pytest.mark.parametrize(
    ("raw_value", "expected_json_text"),
    [
        ("console", '"console"'),
        ("true", '"true"'),
        (True, "true"),
        (42, "42"),
        ({"k": "v"}, '{"k":"v"}'),
        ([1, 2], "[1,2]"),
        ('{"k": "v"}', json.dumps('{"k": "v"}')),
        ("  [1, 2] ", json.dumps("  [1, 2] ")),
        ("{not valid json", '"{not valid json"'),
    ],
)
def test_fixture_json_conversion_preserves_scalar_types(raw_value: Any, expected_json_text: str) -> None:
    """Fixture values represent JSON values, including strings that look like objects or arrays."""
    decoder = fixture_module._column_value_decoder("duckdb", "json")
    assert callable(decoder)
    encoded = decoder(raw_value)
    assert json.loads(encoded) == json.loads(expected_json_text)


def test_conflict_keys_accepts_bare_string_and_ignores_unloaded_tables(tmp_path: Path) -> None:
    """Bare string conflict_keys normalize to single-column tuples and unloaded tables are ignored."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        load_table_fixtures_sync(driver, tmp_path)

        updated = [{"id": 1, "name": "Alice Updated"}, {"id": 2, "name": "Bob"}]
        (tmp_path / "users.json").write_text(json.dumps(updated), encoding="utf-8")
        counts = load_table_fixtures_sync(
            driver, tmp_path, conflict_keys={"users": "id", "unloaded_table": "key", "other_table": ["a", "b"]}
        )

        assert counts == {"users": 2}
        assert _normalized(driver.select("SELECT * FROM users")) == updated
    config.close_pool()


async def test_conflict_keys_accepts_bare_string_and_ignores_unloaded_tables_async(tmp_path: Path) -> None:
    """Async loader normalizes bare string conflict_keys and ignores unloaded tables."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    async with config.provide_session() as driver:
        await driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        await load_table_fixtures_async(driver, tmp_path)

        updated = [{"id": 1, "name": "Alice Async"}, {"id": 2, "name": "Bob"}]
        (tmp_path / "users.json").write_text(json.dumps(updated), encoding="utf-8")
        counts = await load_table_fixtures_async(
            driver, tmp_path, conflict_keys={"users": "id", "unloaded_table": "key"}
        )

        assert counts == {"users": 2}
        assert _normalized(await driver.select("SELECT * FROM users")) == updated
    await config.close_pool()


def test_conflict_keys_rejects_empty_and_bytes_values(tmp_path: Path) -> None:
    """Empty sequences, empty strings, and bytes in conflict_keys raise before any statement runs."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    driver = _postgres_mock_driver([])

    with pytest.raises(ValueError, match="must not be empty"):
        load_table_fixtures_sync(driver, tmp_path, conflict_keys={"users": []})
    with pytest.raises(ValueError, match="Invalid column name"):
        load_table_fixtures_sync(driver, tmp_path, conflict_keys={"users": ""})
    with pytest.raises(TypeError, match="not bytes"):
        load_table_fixtures_sync(driver, tmp_path, conflict_keys=cast("Any", {"users": b"id"}))


def test_load_table_fixtures_supports_sparse_rows(tmp_path: Path) -> None:
    """Sparse JSONL and JSON rows normalize across the ordered union of columns with None defaults."""
    sparse_lines = [
        json.dumps({"id": 1, "name": "Alice"}),
        json.dumps({"id": 2, "name": "Bob", "bio": "Engineer"}),
        json.dumps({"id": 3, "role": "admin"}),
    ]
    (tmp_path / "profiles.jsonl").write_text("\n".join(sparse_lines) + "\n", encoding="utf-8")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE profiles (id INTEGER PRIMARY KEY, name TEXT, bio TEXT, role TEXT)")

        counts = load_table_fixtures_sync(driver, tmp_path)

        assert counts == {"profiles": 3}
        rows = driver.select("SELECT * FROM profiles ORDER BY id")
        assert rows == [
            {"id": 1, "name": "Alice", "bio": None, "role": None},
            {"id": 2, "name": "Bob", "bio": "Engineer", "role": None},
            {"id": 3, "name": None, "bio": None, "role": "admin"},
        ]
    config.close_pool()


def test_ignore_unknown_columns_strips_extra_columns_and_skips_when_none_remain(tmp_path: Path) -> None:
    """ignore_unknown_columns drops columns missing from table metadata and skips insert if all are unknown."""
    (tmp_path / "users.json").write_text(
        json.dumps([{"id": 1, "name": "Alice", "legacy_flag": True}, {"id": 2, "name": "Bob", "extra": "x"}]),
        encoding="utf-8",
    )
    (tmp_path / "obsolete.json").write_text(json.dumps([{"old_a": 1, "old_b": 2}]), encoding="utf-8")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        driver.execute("CREATE TABLE obsolete (id INTEGER PRIMARY KEY)")

        counts = load_table_fixtures_sync(
            driver, tmp_path, table_order=["users", "obsolete"], ignore_unknown_columns=True
        )

        assert counts == {"users": 2, "obsolete": 0}
        assert _normalized(driver.select("SELECT * FROM users")) == USER_ROWS
        assert driver.select("SELECT * FROM obsolete") == []
    config.close_pool()


async def test_ignore_unknown_columns_async(tmp_path: Path) -> None:
    """Async loader supports ignore_unknown_columns and still rejects rows with only generated columns."""
    (tmp_path / "users.json").write_text(json.dumps([{"id": 1, "name": "Alice", "removed_col": 99}]), encoding="utf-8")
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    async with config.provide_session() as driver:
        await driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")

        counts = await load_table_fixtures_async(driver, tmp_path, ignore_unknown_columns=True)

        assert counts == {"users": 1}
        assert await driver.select("SELECT * FROM users") == [{"id": 1, "name": "Alice"}]
    await config.close_pool()

    (tmp_path / "users.json").write_text(json.dumps([{"doubled": 2}]), encoding="utf-8")
    mock_driver = _postgres_mock_driver([
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "doubled", "data_type": "integer", "is_generated": True},
    ])
    with pytest.raises(ValueError, match="only contain generated columns"):
        load_table_fixtures_sync(mock_driver, tmp_path, ignore_unknown_columns=True)


def test_exclude_update_columns_omits_columns_from_upsert_and_falls_back_to_do_nothing(tmp_path: Path) -> None:
    """exclude_update_columns preserves specified columns on conflict and falls back to DO NOTHING when all are excluded."""
    initial = [{"id": 1, "name": "Alice", "created_at": "2024-01-01"}]
    updated = [{"id": 1, "name": "Alice Updated", "created_at": "2099-01-01"}]
    (tmp_path / "users.json").write_text(json.dumps(initial), encoding="utf-8")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL)")
        load_table_fixtures_sync(driver, tmp_path)

        (tmp_path / "users.json").write_text(json.dumps(updated), encoding="utf-8")
        load_table_fixtures_sync(driver, tmp_path, conflict_keys={"users": "id"}, exclude_update_columns=["created_at"])
        assert driver.select("SELECT * FROM users") == [{"id": 1, "name": "Alice Updated", "created_at": "2024-01-01"}]

        second_update = [{"id": 1, "name": "Ignored Name", "created_at": "2099-01-01"}]
        (tmp_path / "users.json").write_text(json.dumps(second_update), encoding="utf-8")
        load_table_fixtures_sync(
            driver,
            tmp_path,
            conflict_keys={"users": "id"},
            exclude_update_columns={"users": ["name", "created_at"], "other_table": "created_at"},
        )
        assert driver.select("SELECT * FROM users") == [{"id": 1, "name": "Alice Updated", "created_at": "2024-01-01"}]
    config.close_pool()


async def test_exclude_update_columns_async_and_validation(tmp_path: Path) -> None:
    """Async loader supports per-table bare string exclude_update_columns and validates invalid inputs."""
    (tmp_path / "users.json").write_text(
        json.dumps([{"id": 1, "name": "Alice", "created_at": "2024-01-01"}]), encoding="utf-8"
    )
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    async with config.provide_session() as driver:
        await driver.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        await load_table_fixtures_async(driver, tmp_path)

        (tmp_path / "users.json").write_text(
            json.dumps([{"id": 1, "name": "Alicia", "created_at": "2099-01-01"}]), encoding="utf-8"
        )
        await load_table_fixtures_async(
            driver, tmp_path, conflict_keys={"users": "id"}, exclude_update_columns={"users": "created_at"}
        )
        assert await driver.select("SELECT * FROM users") == [{"id": 1, "name": "Alicia", "created_at": "2024-01-01"}]

        with pytest.raises(TypeError, match="exclude_update_columns"):
            await load_table_fixtures_async(driver, tmp_path, exclude_update_columns="created_at")
        with pytest.raises(ValueError, match="Invalid column name"):
            await load_table_fixtures_async(driver, tmp_path, exclude_update_columns=["bad col"])
    await config.close_pool()


@pytest.mark.parametrize("config_type", [SqliteConfig, DuckDBConfig])
def test_json_fixture_roundtrip_preserves_string_scalar_types(
    tmp_path: Path, config_type: type[SqliteConfig] | type[DuckDBConfig]
) -> None:
    values = ["true", "42", "null", "[1, 2]", '{"key": 1}', True, 42, [1, 2], {"key": 1}, None]
    config = config_type(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE TABLE json_values (id INTEGER PRIMARY KEY, payload JSON)")
            driver.execute_many(
                "INSERT INTO json_values (id, payload) VALUES (:id, :payload)",
                [
                    {"id": index, "payload": None if value is None else json.dumps(value)}
                    for index, value in enumerate(values)
                ],
            )
            export_table_fixtures_sync(driver, tmp_path, ["json_values"], compress=False)
            assert [row["payload"] for row in json.loads((tmp_path / "json_values.json").read_text())] == values
            driver.execute("DELETE FROM json_values")
            load_table_fixtures_sync(driver, tmp_path)
            loaded = driver.select("SELECT CAST(payload AS TEXT) AS payload FROM json_values ORDER BY id")
            assert [None if row["payload"] is None else json.loads(row["payload"]) for row in loaded] == values
    finally:
        config.close_pool()


def test_ignore_unknown_fixture_columns_uses_exact_quoted_names(tmp_path: Path) -> None:
    (tmp_path / "users.json").write_text(json.dumps([{"id": 1, "Name": "wrong column"}]), encoding="utf-8")
    driver = MagicMock()
    driver.statement_config.dialect = "postgres"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "name", "data_type": "text", "is_primary": False},
    ]

    assert load_table_fixtures_sync(driver, tmp_path, ignore_unknown_columns=True) == {"users": 1}
    assert driver.execute_many.call_args.args[1] == [{"id": 1}]


@pytest.mark.parametrize("bad_name", ["../secret", "sub/table", "..\\secret", "", ".", ".."])
def test_fixture_name_rejects_path_traversal(tmp_path: Path, bad_name: str) -> None:
    """open_fixture_sync and write_fixture_sync reject fixture names with path separators or traversal."""
    with pytest.raises(ValueError, match="Invalid fixture name"):
        open_fixture_sync(tmp_path, bad_name)
    with pytest.raises(ValueError, match="Invalid fixture name"):
        write_fixture_sync(str(tmp_path), bad_name, [{"id": 1}])


def test_read_compressed_file_rejects_oversized_zip_member(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """_read_compressed_file rejects ZIP members larger than _MAX_ZIP_FIXTURE_BYTES."""
    zip_file = tmp_path / "bomb.json.zip"
    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.writestr("bomb.json", json.dumps([{"id": 1, "payload": "x" * 200}]))

    monkeypatch.setattr(fixture_module, "_MAX_ZIP_FIXTURE_BYTES", 50)
    with pytest.raises(ValueError, match="exceeds maximum allowed size"):
        _read_compressed_file(zip_file)


def test_postgres_table_columns_falls_back_to_current_schema() -> None:
    """Unqualified PostgreSQL table column lookup falls back to SELECT current_schema() when initial lookup is empty."""
    driver = MagicMock()
    driver.statement_config.dialect = "postgres"
    driver.select_value_or_none.return_value = "tenant_a"
    driver.data_dictionary.get_columns.side_effect = [
        [],
        [{"column_name": "id", "data_type": "integer", "is_primary": True}],
    ]

    cols = fixture_module._table_columns_sync(driver, "postgres", "users")
    assert len(cols) == 1
    assert cols[0].name == "id"
    assert driver.data_dictionary.get_columns.call_count == 2
    assert driver.data_dictionary.get_columns.call_args_list[1].kwargs["schema"] == '"tenant_a"'


def test_table_insert_statement_overriding_system_value_does_not_corrupt_conflict_clause() -> None:
    """OVERRIDING SYSTEM VALUE insertion targets only the top-level column list boundary."""
    cols = [
        fixture_module._TableColumn(
            name="id",
            data_type="integer",
            is_primary=True,
            is_identity=True,
            identity_always=True,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="note",
            data_type="text",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
    ]
    stmt = fixture_module._table_insert_statement(
        dialect="postgres",
        family="postgres",
        table="users",
        columns=["id", "note"],
        conflict_keys=("id",),
        table_columns=cols,
    )
    assert isinstance(stmt, str)
    assert stmt.count("OVERRIDING SYSTEM VALUE") == 1
    assert ") OVERRIDING SYSTEM VALUE VALUES (" in stmt


def test_sequence_resync_dialect_qualification() -> None:
    """Sequence resync enables for supported dialects and disables for unsupported ones."""
    assert fixture_module._dialect_family("cockroach") == "postgres"
    assert fixture_module._dialect_family("cockroachdb") == "postgres"
    assert fixture_module._sequence_resync_enabled(True, "postgres", "postgres") is True
    assert fixture_module._sequence_resync_enabled(True, "cockroach", "postgres") is True
    assert fixture_module._sequence_resync_enabled(True, "cockroachdb", "postgres") is True
    assert fixture_module._sequence_resync_enabled(True, "oracle", "oracle") is True
    assert fixture_module._sequence_resync_enabled(True, "tsql", "tsql") is True
    assert fixture_module._sequence_resync_enabled(True, "mssql", "tsql") is True
    assert fixture_module._sequence_resync_enabled(True, "sqlite", "sqlite") is True
    assert fixture_module._sequence_resync_enabled(True, "db2", "db2") is True
    assert fixture_module._sequence_resync_enabled(True, "mysql", "mysql") is True
    assert fixture_module._sequence_resync_enabled(True, "mariadb", "mysql") is True
    assert fixture_module._sequence_resync_enabled(True, "duckdb", "duckdb") is True
    assert fixture_module._sequence_resync_enabled(True, "spanner", "spanner") is False
    assert fixture_module._sequence_resync_enabled(True, "bigquery", "bigquery") is False


def test_sequence_resync_statements_cross_dialect() -> None:
    """_sequence_resync_statements generates appropriate DDL and DML across supported dialects."""
    col_seq = fixture_module._TableColumn(
        name="id",
        data_type="integer",
        is_primary=True,
        is_identity=False,
        identity_always=False,
        sequence_name="users_id_seq",
        is_generated=False,
    )
    col_ident = fixture_module._TableColumn(
        name="id",
        data_type="integer",
        is_primary=True,
        is_identity=True,
        identity_always=False,
        sequence_name=None,
        is_generated=False,
    )

    stmts_pg = fixture_module._sequence_resync_statements(
        "users", [col_seq], dialect_name="postgres", family="postgres"
    )
    assert len(stmts_pg) == 1
    assert "pg_catalog.pg_sequence" in stmts_pg[0][0]
    assert stmts_pg[0][1] == {"sequence_name": "users_id_seq"}

    stmts_crdb = fixture_module._sequence_resync_statements(
        "users", [col_seq], dialect_name="cockroach", family="postgres"
    )
    assert len(stmts_crdb) == 1
    assert 'setval(:sequence_name, coalesce(max("id"), 1)' in stmts_crdb[0][0]
    assert stmts_crdb[0][1] == {"sequence_name": "users_id_seq"}

    stmts_ora = fixture_module._sequence_resync_statements("users", [col_ident], dialect_name="oracle", family="oracle")
    assert len(stmts_ora) == 1
    assert (
        stmts_ora[0][0] == 'ALTER TABLE "users" MODIFY ("id" GENERATED BY DEFAULT AS IDENTITY (START WITH LIMIT VALUE))'
    )
    assert stmts_ora[0][1] == {}

    col_ora_always = fixture_module._TableColumn(
        name="id",
        data_type="number",
        is_primary=True,
        is_identity=True,
        identity_always=True,
        sequence_name="ISEQ$$_1",
        is_generated=False,
    )
    stmts_ora_always = fixture_module._sequence_resync_statements(
        "users", [col_ora_always], dialect_name="oracle", family="oracle"
    )
    assert (
        stmts_ora_always[0][0]
        == 'ALTER TABLE "users" MODIFY ("id" GENERATED ALWAYS AS IDENTITY (START WITH LIMIT VALUE))'
    )

    col_ora_on_null = fixture_module._TableColumn(
        name="id",
        data_type="number",
        is_primary=True,
        is_identity=True,
        identity_always=False,
        sequence_name="ISEQ$$_2",
        is_generated=False,
        identity_on_null=True,
    )
    stmts_ora_on_null = fixture_module._sequence_resync_statements(
        "users", [col_ora_on_null], dialect_name="oracle", family="oracle"
    )
    assert (
        stmts_ora_on_null[0][0]
        == 'ALTER TABLE "users" MODIFY ("id" GENERATED BY DEFAULT ON NULL AS IDENTITY (START WITH LIMIT VALUE))'
    )

    stmts_duckdb = fixture_module._sequence_resync_statements(
        "users", [col_seq], dialect_name="duckdb", family="duckdb"
    )
    assert len(stmts_duckdb) == 1
    assert "count(nextval(" in stmts_duckdb[0][0]
    assert stmts_duckdb[0][1] == {
        "sequence_schema": "main",
        "sequence_name": "users_id_seq",
        "quoted_seq": "main.users_id_seq",
    }

    stmts_tsql = fixture_module._sequence_resync_statements("users", [col_ident], dialect_name="tsql", family="tsql")
    assert len(stmts_tsql) == 1
    assert stmts_tsql[0][0] == "DBCC CHECKIDENT ('[users]', RESEED)"
    assert stmts_tsql[0][1] == {}

    stmts_sqlite = fixture_module._sequence_resync_statements(
        "users", [col_seq], dialect_name="sqlite", family="sqlite"
    )
    assert len(stmts_sqlite) == 1
    assert "UPDATE sqlite_sequence SET seq" in stmts_sqlite[0][0]
    assert stmts_sqlite[0][1] == {"table_name": "users"}


def test_resync_table_sequences_dynamic_db2_and_mysql() -> None:
    """Dynamic two-phase sequence resync queries max value and executes restart DDL on Db2 and MySQL."""
    col_ident = fixture_module._TableColumn(
        name="id",
        data_type="integer",
        is_primary=True,
        is_identity=True,
        identity_always=False,
        sequence_name=None,
        is_generated=False,
    )
    col_pk = fixture_module._TableColumn(
        name="id",
        data_type="integer",
        is_primary=True,
        is_identity=False,
        identity_always=False,
        sequence_name=None,
        is_generated=False,
    )

    mock_db2 = MagicMock()
    mock_db2.select_value.return_value = 42
    fixture_module._resync_table_sequences_sync(mock_db2, "db2", "db2", "users", [col_ident])
    mock_db2.execute.assert_called_with('ALTER TABLE "users" ALTER COLUMN "id" RESTART WITH 42')

    mock_mysql = MagicMock()
    mock_mysql.select_value.return_value = 100
    fixture_module._resync_table_sequences_sync(mock_mysql, "mysql", "mysql", "users", [col_pk])
    mock_mysql.select_value.assert_called_with("SELECT coalesce(max(`id`), 0) + 1 FROM `users`")
    mock_mysql.execute.assert_called_with("ALTER TABLE `users` AUTO_INCREMENT = 100")

    col_uuid_pk = fixture_module._TableColumn(
        name="id",
        data_type="varchar",
        is_primary=True,
        is_identity=False,
        identity_always=False,
        sequence_name=None,
        is_generated=False,
    )
    mock_mysql_uuid = MagicMock()
    fixture_module._resync_table_sequences_sync(mock_mysql_uuid, "mysql", "mysql", "users", [col_uuid_pk])
    mock_mysql_uuid.select_value.assert_not_called()
    mock_mysql_uuid.execute.assert_not_called()
    assert fixture_module._sequence_resync_statements("users", [col_uuid_pk], "sqlite", "sqlite") == []

    col_pg_ident_no_seq = fixture_module._TableColumn(
        name="id",
        data_type="integer",
        is_primary=True,
        is_identity=True,
        identity_always=False,
        sequence_name=None,
        is_generated=False,
    )
    stmts_pg = fixture_module._sequence_resync_statements("users", [col_pg_ident_no_seq], "postgres", "postgres")
    assert len(stmts_pg) == 1
    assert "pg_get_serial_sequence(:table_name, :column_name)" in stmts_pg[0][0]
    assert stmts_pg[0][1] == {"table_name": '"users"', "column_name": "id"}


def test_catalog_column_metadata_skips_computed_columns_per_dialect() -> None:
    """Computed and system-versioned period columns are flagged as generated across dialects."""
    row_mysql = {"column_name": "slug", "data_type": "varchar", "extra": "STORED GENERATED"}
    assert fixture_module._table_column(row_mysql).is_generated is True

    row_mysql_auto = {"column_name": "id", "data_type": "int", "extra": "auto_increment", "is_primary": 1}
    col_mysql_auto = fixture_module._table_column(row_mysql_auto)
    assert col_mysql_auto.is_identity is True
    assert col_mysql_auto.is_generated is False

    row_db2 = {
        "column_name": "id",
        "data_type": "integer",
        "is_identity": 1,
        "identity_generation": "BY DEFAULT",
        "is_generated": 1,
    }
    col_db2 = fixture_module._table_column(row_db2)
    assert col_db2.is_identity is True
    assert col_db2.is_generated is False

    row_db2_always = {"column_name": "id", "data_type": "integer", "identity_generation": "ALWAYS", "is_generated": 0}
    col_db2_always = fixture_module._table_column(row_db2_always)
    assert col_db2_always.is_identity is True
    assert col_db2_always.identity_always is True
    assert col_db2_always.is_generated is False

    row_tsql = {"column_name": "valid_to", "data_type": "datetime2", "is_generated": 1}
    assert fixture_module._table_column(row_tsql).is_generated is True

    row_spanner = {
        "column_name": "full_name",
        "data_type": "string",
        "is_generated": "ALWAYS",
        "generation_expression": "concat(first_name, ' ', last_name)",
    }
    assert fixture_module._table_column(row_spanner).is_generated is True


def test_db2_identity_columns_load_and_export_while_computed_columns_are_skipped(tmp_path: Path) -> None:
    """Db2 identity columns load explicit values, while computed columns are skipped."""
    driver = MagicMock()
    driver.statement_config.dialect = "db2"
    driver.data_dictionary.get_columns.return_value = [
        {
            "column_name": "id",
            "data_type": "integer",
            "is_primary": True,
            "is_identity": 1,
            "identity_generation": "BY DEFAULT",
            "is_generated": 1,
        },
        {"column_name": "name", "data_type": "varchar", "is_primary": False},
        {"column_name": "slug", "data_type": "varchar", "is_primary": False, "is_generated": "ALWAYS"},
    ]
    (tmp_path / "items.json").write_text(json.dumps([{"id": 1, "name": "Item A", "slug": "item-a"}]), encoding="utf-8")

    load_table_fixtures_sync(driver, tmp_path)

    insert_sql = driver.execute_many.call_args.args[0]
    assert insert_sql == 'INSERT INTO "items" ("id", "name") VALUES (:id, :name)'


def test_oracle_date_values_with_time_load_as_datetimes_and_identity_values_are_kept(tmp_path: Path) -> None:
    """Oracle DATE columns decode ISO timestamps as datetimes to preserve time components."""
    driver = MagicMock()
    driver.statement_config.dialect = "oracle"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "number", "identity_column": "YES", "is_primary": False},
        {"column_name": "created_at", "data_type": "date", "is_primary": False},
    ]
    (tmp_path / "events.json").write_text(
        json.dumps([{"id": 42, "created_at": "2026-10-30T14:30:00"}]), encoding="utf-8"
    )

    load_table_fixtures_sync(driver, tmp_path)

    batch_rows = driver.execute_many.call_args.args[1]
    assert batch_rows == [{"id": 42, "created_at": datetime(2026, 10, 30, 14, 30)}]


def test_oracle_export_orders_by_sortable_columns_and_keeps_identity_values(tmp_path: Path) -> None:
    """Oracle exports order by sortable columns when is_primary is absent, skipping unorderable LOBs."""
    driver = MagicMock()
    driver.statement_config.dialect = "oracle"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "number", "identity_column": "YES", "is_primary": False},
        {"column_name": "notes", "data_type": "clob", "is_primary": False},
        {"column_name": "title", "data_type": "varchar2", "is_primary": False},
    ]
    driver.select.return_value = [{"id": 1, "notes": "some notes", "title": "First"}]

    counts = export_table_fixtures_sync(driver, tmp_path, ["events"], compress=False)

    assert counts == {"events": 1}
    query = driver.select.call_args.args[0]
    assert query == 'SELECT * FROM "events" ORDER BY "id", "title"'


def test_db2_export_orders_by_sortable_columns(tmp_path: Path) -> None:
    """Db2 exports order by sortable columns, skipping unorderable LOBs and XML."""
    driver = MagicMock()
    driver.statement_config.dialect = "db2"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": False},
        {"column_name": "payload", "data_type": "blob", "is_primary": False},
        {"column_name": "doc", "data_type": "xml", "is_primary": False},
        {"column_name": "code", "data_type": "varchar", "is_primary": False},
    ]
    driver.select.return_value = []

    export_table_fixtures_sync(driver, tmp_path, ["records"], compress=False)

    query = driver.select.call_args.args[0]
    assert query == 'SELECT * FROM "records" ORDER BY "id", "code"'


def test_tsql_identity_values_load_between_identity_insert_statements(tmp_path: Path) -> None:
    """SQL Server inserts that include identity columns wrap execution with SET IDENTITY_INSERT ON and OFF."""
    driver = MagicMock()
    driver.statement_config.dialect = "tsql"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "int", "is_identity": 1, "is_primary": True},
        {"column_name": "name", "data_type": "nvarchar", "is_primary": False},
    ]
    (tmp_path / "accounts.json").write_text(json.dumps([{"id": 10, "name": "Acme"}]), encoding="utf-8")

    load_table_fixtures_sync(driver, tmp_path)

    calls = [call.args[0] for call in driver.execute.call_args_list]
    assert calls == ["SET IDENTITY_INSERT [accounts] ON", "SET IDENTITY_INSERT [accounts] OFF"]


async def test_tsql_identity_insert_is_disabled_when_an_insert_fails_async(tmp_path: Path) -> None:
    """Async SQL Server inserts guarantee SET IDENTITY_INSERT OFF runs in finally even on insert error."""
    driver = AsyncMock()
    driver.statement_config.dialect = "tsql"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "int", "is_identity": 1, "is_primary": True},
        {"column_name": "name", "data_type": "nvarchar", "is_primary": False},
    ]
    driver.execute_many.side_effect = RuntimeError("network partition")
    (tmp_path / "accounts.json").write_text(json.dumps([{"id": 10, "name": "Acme"}]), encoding="utf-8")

    with pytest.raises(RuntimeError, match="network partition"):
        await load_table_fixtures_async(driver, tmp_path)

    calls = [call.args[0] for call in driver.execute.call_args_list]
    assert calls == ["SET IDENTITY_INSERT [accounts] ON", "SET IDENTITY_INSERT [accounts] OFF"]


def test_tsql_rows_without_identity_values_skip_identity_insert(tmp_path: Path) -> None:
    """SQL Server inserts that omit the identity column do not trigger SET IDENTITY_INSERT."""
    driver = MagicMock()
    driver.statement_config.dialect = "tsql"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "int", "is_identity": 1, "is_primary": True},
        {"column_name": "name", "data_type": "nvarchar", "is_primary": False},
    ]
    (tmp_path / "accounts.json").write_text(json.dumps([{"name": "NoIdentity"}]), encoding="utf-8")

    load_table_fixtures_sync(driver, tmp_path)

    driver.execute.assert_not_called()


def test_tsql_export_orders_by_sortable_columns_and_skips_computed_and_period_columns(tmp_path: Path) -> None:
    """SQL Server exports omit computed and system-versioned columns and skip image/text in ORDER BY."""
    driver = MagicMock()
    driver.statement_config.dialect = "tsql"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "int", "is_primary": True, "is_identity": 1},
        {"column_name": "data", "data_type": "image", "is_primary": False},
        {"column_name": "computed", "data_type": "varchar", "is_generated": 1, "is_primary": False},
        {"column_name": "row_start", "data_type": "datetime2", "is_generated": 1, "is_primary": False},
        {"column_name": "tag", "data_type": "nvarchar", "is_primary": False},
    ]
    driver.select.return_value = [
        {"id": 1, "data": b"\x01\x02", "computed": "skip", "row_start": "2026-01-01", "tag": "test"}
    ]

    export_table_fixtures_sync(driver, tmp_path, ["entities"], compress=False)

    exported = json.loads((tmp_path / "entities.json").read_text())
    assert exported == [{"id": 1, "data": "AQI=", "tag": "test"}]
    query = driver.select.call_args.args[0]
    assert query == "SELECT * FROM [entities] ORDER BY [id]"


def test_column_lookup_errors_propagate(tmp_path: Path) -> None:
    """Exceptions raised during catalog column metadata queries propagate directly."""
    driver = MagicMock()
    driver.statement_config.dialect = "postgres"
    driver.data_dictionary.get_columns.side_effect = RuntimeError("connection reset")
    (tmp_path / "users.json").write_text(json.dumps([{"id": 1, "name": "A"}]), encoding="utf-8")

    with pytest.raises(RuntimeError, match="connection reset"):
        load_table_fixtures_sync(driver, tmp_path)


async def test_column_lookup_async_reads_schema_qualified_catalog_rows_and_propagates_errors() -> None:
    """Async catalog lookup passes schema and table names and propagates errors."""
    failing_driver = AsyncMock()
    failing_driver.statement_config.dialect = "postgres"
    failing_driver.data_dictionary.get_columns.side_effect = RuntimeError("catalog unavailable")

    with pytest.raises(RuntimeError, match="catalog unavailable"):
        await fixture_module._table_columns_async(failing_driver, "postgres", "orders")


def test_load_table_fixtures_sync_sharded_jsonl_gz(tmp_path: Path) -> None:
    """Sharded jsonl.gz table fixtures load in ascending numeric order and query metadata once."""
    shard1 = b"".join(json.dumps(r).encode("utf-8") + b"\n" for r in [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
    shard2 = b"".join(json.dumps(r).encode("utf-8") + b"\n" for r in [{"id": 3, "name": "C"}])
    shard10 = b"".join(json.dumps(r).encode("utf-8") + b"\n" for r in [{"id": 10, "name": "J"}])

    (tmp_path / "users.001.jsonl.gz").write_bytes(gzip.compress(shard1))
    (tmp_path / "users.002.jsonl.gz").write_bytes(gzip.compress(shard2))
    (tmp_path / "users.010.jsonl.gz").write_bytes(gzip.compress(shard10))

    driver = MagicMock()
    driver.statement_config.dialect = "postgres"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "name", "data_type": "text", "is_primary": False},
    ]

    counts = load_table_fixtures_sync(driver, tmp_path)

    assert counts == {"users": 4}
    assert driver.data_dictionary.get_columns.call_count == 1
    assert driver.execute_many.call_count == 3
    executed_batches = [call.args[1] for call in driver.execute_many.call_args_list]
    assert executed_batches[0] == [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
    assert executed_batches[1] == [{"id": 3, "name": "C"}]
    assert executed_batches[2] == [{"id": 10, "name": "J"}]


async def test_load_table_fixtures_async_sharded_json_gz(tmp_path: Path) -> None:
    """Async loader processes sharded json.gz files in correct shard order."""
    shard1 = json.dumps([{"id": 100, "val": "first"}])
    shard2 = json.dumps([{"id": 200, "val": "second"}])
    (tmp_path / "metrics.001.json.gz").write_bytes(gzip.compress(shard1.encode("utf-8")))
    (tmp_path / "metrics.002.json.gz").write_bytes(gzip.compress(shard2.encode("utf-8")))

    driver = AsyncMock()
    driver.statement_config.dialect = "sqlite"
    driver.data_dictionary.get_columns.return_value = []

    counts = await load_table_fixtures_async(driver, tmp_path)

    assert counts == {"metrics": 2}
    assert driver.execute_many.call_count == 2
    executed_batches = [call.args[1] for call in driver.execute_many.call_args_list]
    assert executed_batches[0] == [{"id": 100, "val": "first"}]
    assert executed_batches[1] == [{"id": 200, "val": "second"}]


def test_load_table_fixtures_sharded_schema_qualified(tmp_path: Path) -> None:
    """Schema-qualified sharded fixture files load with proper table naming."""
    rows = [{"id": 1, "data": "val"}]
    content = b"".join(json.dumps(r).encode("utf-8") + b"\n" for r in rows)
    (tmp_path / "crm.contacts.001.jsonl.gz").write_bytes(gzip.compress(content))

    driver = MagicMock()
    driver.statement_config.dialect = "postgres"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "data", "data_type": "text", "is_primary": False},
    ]

    counts = load_table_fixtures_sync(driver, tmp_path)

    assert counts == {"crm.contacts": 1}
    insert_sql = driver.execute_many.call_args.args[0]
    assert 'INSERT INTO "crm"."contacts"' in insert_sql


def test_load_table_fixtures_sharded_with_sequence_resync(tmp_path: Path) -> None:
    """PostgreSQL sequence resync executes once after all shards of a table are loaded."""
    shard1 = b"".join(json.dumps(r).encode("utf-8") + b"\n" for r in [{"id": 1}])
    shard2 = b"".join(json.dumps(r).encode("utf-8") + b"\n" for r in [{"id": 2}])
    (tmp_path / "logs.001.jsonl.gz").write_bytes(gzip.compress(shard1))
    (tmp_path / "logs.002.jsonl.gz").write_bytes(gzip.compress(shard2))

    driver = MagicMock()
    driver.statement_config.dialect = "postgres"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": True, "sequence_name": "logs_id_seq"}
    ]

    counts = load_table_fixtures_sync(driver, tmp_path, resync_sequences=True)

    assert counts == {"logs": 2}
    resync_calls = [call for call in driver.execute.call_args_list if "setval" in call.args[0]]
    assert len(resync_calls) == 1
    assert resync_calls[0].args[1] == {"sequence_name": "logs_id_seq"}


def test_load_table_fixtures_sharded_validation_errors(tmp_path: Path) -> None:
    """Loader rejects invalid shard configurations with clear ValueError messages."""
    (tmp_path / "users.json").write_text("[]")
    (tmp_path / "users.001.json").write_text("[]")
    driver = MagicMock()
    driver.statement_config.dialect = "sqlite"

    with pytest.raises(ValueError, match="both unsharded and sharded"):
        load_table_fixtures_sync(driver, tmp_path)

    (tmp_path / "users.json").unlink()
    (tmp_path / "users.002.jsonl").write_text("{}\n")

    with pytest.raises(ValueError, match="mixed fixture extensions"):
        load_table_fixtures_sync(driver, tmp_path)

    (tmp_path / "users.002.jsonl").unlink()
    (tmp_path / "users.01.json").write_text("[]")

    with pytest.raises(ValueError, match="duplicate shard index 1"):
        load_table_fixtures_sync(driver, tmp_path)


def test_export_table_fixtures_sync_sharded_by_max_rows(tmp_path: Path) -> None:
    """Exporting with max_rows_per_shard splits rows across numbered shards."""
    driver = MagicMock()
    driver.statement_config.dialect = "sqlite"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "val", "data_type": "text", "is_primary": False},
    ]
    driver.select.return_value = [{"id": i, "val": f"val_{i}"} for i in range(1, 11)]

    counts = export_table_fixtures_sync(driver, tmp_path, ["items"], compress=False, jsonl=True, max_rows_per_shard=4)

    assert counts == {"items": 10}
    shards = sorted(tmp_path.glob("items.*.jsonl"))
    assert len(shards) == 3
    assert shards[0].name == "items.001.jsonl"
    assert shards[1].name == "items.002.jsonl"
    assert shards[2].name == "items.003.jsonl"

    lines1 = [json.loads(line) for line in shards[0].read_text().splitlines() if line]
    lines2 = [json.loads(line) for line in shards[1].read_text().splitlines() if line]
    lines3 = [json.loads(line) for line in shards[2].read_text().splitlines() if line]
    assert len(lines1) == 4
    assert len(lines2) == 4
    assert len(lines3) == 2


def test_export_table_fixtures_sync_sharded_by_max_bytes(tmp_path: Path) -> None:
    """Exporting with max_shard_bytes splits rows into bounded byte chunks without breaking rows."""
    driver = MagicMock()
    driver.statement_config.dialect = "sqlite"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": True},
        {"column_name": "val", "data_type": "text", "is_primary": False},
    ]
    driver.select.return_value = [{"id": i, "val": "x" * 50} for i in range(1, 6)]

    counts = export_table_fixtures_sync(driver, tmp_path, ["docs"], compress=False, jsonl=False, max_shard_bytes=140)

    assert counts == {"docs": 5}
    shards = sorted(tmp_path.glob("docs.*.json"))
    assert len(shards) >= 2
    for shard in shards:
        content = shard.read_text()
        parsed = json.loads(content)
        assert isinstance(parsed, list)


def test_export_table_fixtures_async_sharded_json(tmp_path: Path) -> None:
    """Async export splits JSON array shards and writes gzipped files."""
    driver = AsyncMock()
    driver.statement_config.dialect = "sqlite"
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "integer", "is_primary": True}
    ]
    driver.select.return_value = [{"id": i} for i in range(1, 7)]

    counts = asyncio.run(
        export_table_fixtures_async(driver, tmp_path, ["users"], compress=True, jsonl=False, max_rows_per_shard=3)
    )

    assert counts == {"users": 6}
    shards = sorted(tmp_path.glob("users.*.json.gz"))
    assert len(shards) == 2
    assert shards[0].name == "users.001.json.gz"
    assert shards[1].name == "users.002.json.gz"

    decompressed1 = json.loads(gzip.decompress(shards[0].read_bytes()).decode("utf-8"))
    decompressed2 = json.loads(gzip.decompress(shards[1].read_bytes()).decode("utf-8"))
    assert decompressed1 == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert decompressed2 == [{"id": 4}, {"id": 5}, {"id": 6}]


def test_export_table_fixtures_single_shard_unsharded_filename(tmp_path: Path) -> None:
    """When exported rows fit in a single shard, unnumbered <table>.<ext> is written."""
    driver = MagicMock()
    driver.statement_config.dialect = "sqlite"
    driver.data_dictionary.get_columns.return_value = []
    driver.select.return_value = [{"id": 1}, {"id": 2}]

    counts = export_table_fixtures_sync(driver, tmp_path, ["users"], compress=False, jsonl=True, max_rows_per_shard=100)

    assert counts == {"users": 2}
    assert (tmp_path / "users.jsonl").exists()
    assert not (tmp_path / "users.001.jsonl").exists()


def test_export_table_fixtures_stale_shard_cleanup(tmp_path: Path) -> None:
    """Re-exporting a table with fewer shards or without sharding deletes old shard files."""
    (tmp_path / "orders.001.jsonl.gz").write_bytes(b"dummy")
    (tmp_path / "orders.002.jsonl.gz").write_bytes(b"dummy")
    (tmp_path / "orders.003.jsonl.gz").write_bytes(b"dummy")
    (tmp_path / "other.001.jsonl.gz").write_bytes(b"keep")

    driver = MagicMock()
    driver.statement_config.dialect = "sqlite"
    driver.data_dictionary.get_columns.return_value = []
    driver.select.return_value = [{"id": 1}]

    export_table_fixtures_sync(driver, tmp_path, ["orders"], compress=False, jsonl=True)

    assert (tmp_path / "orders.jsonl").exists()
    assert not (tmp_path / "orders.001.jsonl.gz").exists()
    assert not (tmp_path / "orders.002.jsonl.gz").exists()
    assert not (tmp_path / "orders.003.jsonl.gz").exists()
    assert (tmp_path / "other.001.jsonl.gz").exists()


def test_export_table_fixtures_invalid_shard_parameters(tmp_path: Path) -> None:
    """Invalid non-positive shard limits raise ValueError."""
    driver = MagicMock()
    driver.statement_config.dialect = "sqlite"

    with pytest.raises(ValueError, match="max_shard_bytes must be greater than 0"):
        export_table_fixtures_sync(driver, tmp_path, ["users"], max_shard_bytes=0)

    with pytest.raises(ValueError, match="max_rows_per_shard must be greater than 0"):
        export_table_fixtures_sync(driver, tmp_path, ["users"], max_rows_per_shard=-5)


def test_postgres_vector_column_insert_wrapping() -> None:
    """PostgreSQL vector columns wrap placeholders in double cast with udt parsing."""
    cols = [
        fixture_module._TableColumn(
            name="id",
            data_type="integer",
            is_primary=True,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="embedding",
            data_type="vector(3)",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="half",
            data_type="halfvec(1536)",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="sparse",
            data_type="sparsevec(5)",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
    ]
    stmt = fixture_module._table_insert_statement(
        dialect="postgres",
        family="postgres",
        table="items",
        columns=["id", "embedding", "half", "sparse"],
        conflict_keys=(),
        table_columns=cols,
    )
    assert "CAST(CAST(%(embedding)s AS TEXT) AS VECTOR(3))" in stmt
    assert "CAST(CAST(%(half)s AS TEXT) AS halfvec(1536))" in stmt
    assert "CAST(CAST(%(sparse)s AS TEXT) AS sparsevec(5))" in stmt


def test_mysql_vector_column_insert_wrapping() -> None:
    """MySQL and MariaDB vector columns wrap placeholders in their respective conversion functions."""
    cols = [
        fixture_module._TableColumn(
            name="id",
            data_type="int",
            is_primary=True,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="vec",
            data_type="vector",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
    ]
    stmt_mysql = fixture_module._table_insert_statement(
        dialect="mysql", family="mysql", table="items", columns=["id", "vec"], conflict_keys=(), table_columns=cols
    )
    assert "STRING_TO_VECTOR(:vec)" in stmt_mysql

    stmt_mariadb = fixture_module._table_insert_statement(
        dialect="mariadb", family="mysql", table="items", columns=["id", "vec"], conflict_keys=(), table_columns=cols
    )
    assert "VEC_FROMTEXT(:vec)" in stmt_mariadb


def test_vector_column_value_decoders_per_dialect() -> None:
    """Vector decoders prepare values matching the driver parameter contract."""
    pg_decoder = fixture_module._column_value_decoder("postgres", "vector(3)")
    assert pg_decoder is not None
    assert pg_decoder([1.0, 2.5, 3.0]) == "[1.0,2.5,3.0]"
    assert pg_decoder("[1.0,2.5,3.0]") == "[1.0,2.5,3.0]"

    mysql_decoder = fixture_module._column_value_decoder("mysql", "vector")
    assert mysql_decoder is not None
    assert mysql_decoder([0.1, 0.2]) == "[0.1,0.2]"

    oracle_decoder = fixture_module._column_value_decoder("oracle", "vector(3, float32)")
    assert oracle_decoder is not None
    val_oracle = oracle_decoder([1.0, 2.0, 3.0])
    assert isinstance(val_oracle, array.array)
    assert val_oracle.typecode == "f"
    assert list(val_oracle) == [1.0, 2.0, 3.0]

    spanner_decoder = fixture_module._column_value_decoder("spanner", "array<float32>")
    assert spanner_decoder is not None
    val_spanner = spanner_decoder([1.0, 2.0])
    assert isinstance(val_spanner, TypedParameter)
    assert val_spanner.semantic_name == "ARRAY<FLOAT32>"

    sqlite_decoder = fixture_module._column_value_decoder("sqlite", "f32_blob(2)")
    assert sqlite_decoder is not None
    val_sqlite = sqlite_decoder([1.0, 2.0])
    assert isinstance(val_sqlite, bytes)
    assert struct.unpack("<2f", val_sqlite) == (1.0, 2.0)

    duckdb_decoder = fixture_module._column_value_decoder("duckdb", "float[3]")
    assert duckdb_decoder is not None
    assert duckdb_decoder([1.0, 2.0, 3.0]) == "[1.0,2.0,3.0]"


def test_export_normalizes_vector_column_values() -> None:
    """Vector representations from various drivers normalize to JSON list of floats on export."""
    cols = [
        fixture_module._TableColumn(
            name="id",
            data_type="int",
            is_primary=True,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="v_tuple",
            data_type="float[3]",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="v_array",
            data_type="vector(2)",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="v_f32",
            data_type="f32_blob(2)",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="v_str",
            data_type="vector(2)",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
    ]
    raw_rows = [
        {
            "id": 1,
            "v_tuple": (1.0, 2.0, 3.0),
            "v_array": array.array("f", [4.0, 5.0]),
            "v_f32": struct.pack("<2f", 6.0, 7.0),
            "v_str": "[8.0, 9.0]",
        }
    ]
    normalized = fixture_module._without_generated_values(raw_rows, cols)
    assert normalized == [
        {"id": 1, "v_tuple": [1.0, 2.0, 3.0], "v_array": [4.0, 5.0], "v_f32": [6.0, 7.0], "v_str": [8.0, 9.0]}
    ]


def test_export_order_by_skips_vector_and_unorderable_columns() -> None:
    """Export SELECT excludes vector and unorderable columns from ORDER BY."""
    cols = [
        fixture_module._TableColumn(
            name="id",
            data_type="integer",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
        fixture_module._TableColumn(
            name="embedding",
            data_type="vector(1536)",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        ),
    ]
    query = fixture_module._table_export_query("postgres", "postgres", "items", cols)
    assert query == 'SELECT * FROM "items" ORDER BY "id"'

    cols_vector_only = [
        fixture_module._TableColumn(
            name="vec",
            data_type="vector(3)",
            is_primary=False,
            is_identity=False,
            identity_always=False,
            sequence_name=None,
            is_generated=False,
        )
    ]
    query_no_order = fixture_module._table_export_query("postgres", "postgres", "embeddings", cols_vector_only)
    assert "ORDER BY" not in query_no_order


def test_sqlite_vector_and_sharded_round_trip(tmp_path: Path) -> None:
    """SQLite round trip verifies sharded export and import of vector data."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, title TEXT, vec TEXT)")
        driver.execute("INSERT INTO embeddings VALUES (1, 'First', '[1.0, 2.0]'), (2, 'Second', '[3.0, 4.0]')")
        driver.execute("INSERT INTO embeddings VALUES (3, 'Third', '[5.0, 6.0]'), (4, 'Fourth', '[7.0, 8.0]')")

        counts = export_table_fixtures_sync(
            driver, tmp_path, ["embeddings"], compress=True, jsonl=True, max_rows_per_shard=2
        )
        assert counts == {"embeddings": 4}

    shards = sorted(tmp_path.glob("embeddings.*.jsonl.gz"))
    assert len(shards) == 2

    target = SqliteConfig(connection_config={"database": ":memory:"})
    with target.provide_session() as driver:
        driver.execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, title TEXT, vec TEXT)")
        load_counts = load_table_fixtures_sync(driver, tmp_path)
        assert load_counts == {"embeddings": 4}
        rows = driver.select("SELECT * FROM embeddings ORDER BY id")
        assert len(rows) == 4
        assert rows[0]["vec"] == "[1.0, 2.0]"
        assert rows[3]["vec"] == "[7.0, 8.0]"


def test_standalone_sequence_extraction_and_resync_across_dialects() -> None:
    """Standalone sequence defaults are parsed from column_default and resynchronized without converting to identity."""
    col_pg = fixture_module._table_column({
        "column_name": "id",
        "data_type": "integer",
        "column_default": "nextval('custom_user_seq'::regclass)",
        "is_primary": True,
    })
    assert col_pg.is_identity is False
    assert col_pg.sequence_name == "custom_user_seq"

    col_crdb = fixture_module._table_column({
        "column_name": "id",
        "data_type": "int8",
        "column_default": "nextval('public.orders_seq'::REGCLASS)",
        "is_primary": True,
    })
    assert col_crdb.is_identity is False
    assert col_crdb.sequence_name == "public.orders_seq"

    col_duckdb = fixture_module._table_column({
        "column_name": "id",
        "data_type": "integer",
        "column_default": "nextval('items_seq')",
        "is_primary": True,
    })
    assert col_duckdb.is_identity is False
    assert col_duckdb.sequence_name == "items_seq"

    col_ora_seq = fixture_module._table_column({
        "column_name": "id",
        "data_type": "number",
        "column_default": '"APP"."USERS_SEQ".NEXTVAL',
        "is_identity": 0,
        "is_primary": True,
    })
    assert col_ora_seq.is_identity is False
    assert col_ora_seq.sequence_name == "APP.USERS_SEQ"
    mock_ora = MagicMock()
    mock_ora.select_value.return_value = 55
    fixture_module._resync_table_sequences_sync(mock_ora, "oracle", "oracle", "users", [col_ora_seq])
    mock_ora.select_value.assert_called_once_with('SELECT coalesce(max("id"), 0) + 1 FROM "users"')
    mock_ora.execute.assert_called_once_with('ALTER SEQUENCE "APP"."USERS_SEQ" RESTART START WITH 55')

    col_ora_on_null = fixture_module._table_column({
        "column_name": "id",
        "data_type": "number",
        "is_identity": 1,
        "identity_generation": "BY DEFAULT ON NULL",
        "is_primary": True,
    })
    assert col_ora_on_null.is_identity is True
    assert col_ora_on_null.identity_always is False
    assert col_ora_on_null.identity_on_null is True

    col_tsql_seq = fixture_module._table_column({
        "column_name": "id",
        "data_type": "int",
        "column_default": "(NEXT VALUE FOR [dbo].[OrderSeq])",
        "is_identity": 0,
        "is_primary": True,
    })
    assert col_tsql_seq.is_identity is False
    assert col_tsql_seq.sequence_name == "dbo.OrderSeq"
    mock_tsql = MagicMock()
    mock_tsql.select_value.return_value = 77
    fixture_module._resync_table_sequences_sync(mock_tsql, "tsql", "mssql", "orders", [col_tsql_seq])
    mock_tsql.select_value.assert_called_once_with("SELECT coalesce(max([id]), 0) + 1 FROM [orders]")
    mock_tsql.execute.assert_called_once_with("ALTER SEQUENCE [dbo].[OrderSeq] RESTART WITH 77")

    col_db2_seq = fixture_module._table_column({
        "column_name": "id",
        "data_type": "integer",
        "column_default": 'NEXT VALUE FOR "APP"."INVOICE_SEQ"',
        "identity_generation": None,
        "is_primary": True,
    })
    assert col_db2_seq.is_identity is False
    assert col_db2_seq.sequence_name == "APP.INVOICE_SEQ"
    mock_db2 = MagicMock()
    mock_db2.select_value.return_value = 12
    fixture_module._resync_table_sequences_sync(mock_db2, "db2", "db2", "invoices", [col_db2_seq])
    mock_db2.select_value.assert_called_once_with('SELECT coalesce(max("id"), 0) + 1 FROM "invoices"')
    mock_db2.execute.assert_called_once_with('ALTER SEQUENCE "APP"."INVOICE_SEQ" RESTART WITH 12')

    col_mariadb_seq = fixture_module._table_column({
        "column_name": "id",
        "data_type": "int",
        "column_default": "(nextval(`account_seq`))",
        "extra": "",
        "is_primary": True,
    })
    assert col_mariadb_seq.is_identity is False
    assert col_mariadb_seq.sequence_name == "account_seq"
    mock_mariadb = MagicMock()
    mock_mariadb.select_value.return_value = 99
    fixture_module._resync_table_sequences_sync(mock_mariadb, "mysql", "mariadb", "accounts", [col_mariadb_seq])
    mock_mariadb.select_value.assert_called_once_with("SELECT coalesce(max(`id`), 0) + 1 FROM `accounts`")
    mock_mariadb.execute.assert_called_once_with("ALTER SEQUENCE `account_seq` RESTART WITH 99")


def test_duckdb_sequence_resync_round_trip(tmp_path: Path) -> None:
    """DuckDB table backed by CREATE SEQUENCE resynchronizes nextval past loaded fixture rows."""
    (tmp_path / "events.json").write_text(
        json.dumps([{"id": 10, "name": "ten"}, {"id": 25, "name": "twenty-five"}]), encoding="utf-8"
    )
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE SEQUENCE events_id_seq START 1")
            driver.execute(
                "CREATE TABLE events (id INTEGER DEFAULT nextval('events_id_seq') PRIMARY KEY, name VARCHAR)"
            )
            counts = load_table_fixtures_sync(driver, tmp_path, resync_sequences=True)
            assert counts == {"events": 2}
            driver.execute("INSERT INTO events (name) VALUES ('next-after-fixture')")
            next_id = driver.select_value("SELECT id FROM events WHERE name = 'next-after-fixture'")
            assert next_id == 26
    finally:
        config.close_pool()


def test_duckdb_sequence_resync_increment_and_schema(tmp_path: Path) -> None:
    """DuckDB sequence resync handles non-unit increment, schema qualification, and empty tables."""
    (tmp_path / "custom.items.json").write_text(json.dumps([{"id": 11, "name": "first"}]), encoding="utf-8")
    (tmp_path / "custom.empty_items.json").write_text(json.dumps([]), encoding="utf-8")
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("CREATE SCHEMA custom")
            driver.execute("CREATE SEQUENCE custom.items_id_seq INCREMENT BY 2 START 1")
            driver.execute("CREATE SEQUENCE custom.empty_id_seq START 1")
            driver.execute(
                "CREATE TABLE custom.items (id INTEGER DEFAULT nextval('custom.items_id_seq') PRIMARY KEY, name VARCHAR)"
            )
            driver.execute(
                "CREATE TABLE custom.empty_items (id INTEGER DEFAULT nextval('custom.empty_id_seq') PRIMARY KEY, name VARCHAR)"
            )
            counts = load_table_fixtures_sync(driver, tmp_path, resync_sequences=True)
            assert counts == {"custom.items": 1, "custom.empty_items": 0}
            driver.execute("INSERT INTO custom.items (name) VALUES ('next-after-fixture')")
            next_id = driver.select_value("SELECT id FROM custom.items WHERE name = 'next-after-fixture'")
            assert next_id == 13
            driver.execute("INSERT INTO custom.empty_items (name) VALUES ('first-after-empty')")
            empty_next_id = driver.select_value("SELECT id FROM custom.empty_items WHERE name = 'first-after-empty'")
            assert empty_next_id == 1
    finally:
        config.close_pool()
