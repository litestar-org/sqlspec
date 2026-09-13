# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Tests for sqlspec.utils.fixtures module.

Tests fixture loading utilities including synchronous and asynchronous
JSON fixture file loading with compression support.
"""

import gzip
import importlib
import json
import logging
import os
import sys
import zipfile
from datetime import time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import UUID

import pytest

import sqlspec.utils.fixtures as fixture_module
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.sqlite import SqliteConfig
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
        ("users.json", '[{"id": 1}, {"id": 2, "name": "extra"}]', None, "do not match"),
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


def test_resync_noop_on_sqlite(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Sequence resync is skipped with a debug log on non-PostgreSQL drivers."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    caplog.set_level(logging.DEBUG, logger="sqlspec.utils.fixtures")
    config = SqliteConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

        counts = load_table_fixtures_sync(driver, tmp_path, resync_sequences=True)

        assert counts == {"users": 2}
        assert _normalized(driver.select("SELECT * FROM users")) == USER_ROWS
    config.close_pool()
    assert any("resync" in record.getMessage() and record.levelno == logging.DEBUG for record in caplog.records)


async def test_resync_noop_on_sqlite_async(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """The async loader skips sequence resync on non-PostgreSQL drivers."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    caplog.set_level(logging.DEBUG, logger="sqlspec.utils.fixtures")
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    async with config.provide_session() as driver:
        await driver.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

        counts = await load_table_fixtures_async(driver, tmp_path, resync_sequences=True)

        assert counts == {"users": 2}
    await config.close_pool()
    assert any("resync" in record.getMessage() and record.levelno == logging.DEBUG for record in caplog.records)


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
    rendered = " ".join(statement.to_statement().sql.split())
    assert rendered.startswith("INSERT INTO `order` (`id`, `userName`)")
    assert rendered.endswith("ON DUPLICATE KEY UPDATE `userName` = VALUES(`userName`)")


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
        "identity_generation": "a",
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
    """Conflict keys must name tables that are being loaded, spelled exactly."""
    (tmp_path / "users.json").write_text(json.dumps(USER_ROWS), encoding="utf-8")
    driver = _postgres_mock_driver([])

    with pytest.raises(ValueError, match=r"not loaded: 'Users', 'posts'"):
        load_table_fixtures_sync(driver, tmp_path, conflict_keys={"Users": ["id"], "posts": ["id"]})

    driver.execute_many.assert_not_called()


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
        ("postgres", {"generated_kind": "s"}, True),
        ("postgres", {"generated_kind": ""}, False),
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
        {"column_name": "doubled", "data_type": "integer", "generated_kind": "s"},
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

    order_by = driver.select.call_args.args[0].to_statement().sql.partition("ORDER BY")[2]
    assert '"id"' in order_by
    assert '"names"' in order_by
    assert all(name not in order_by for name in ('"docs"', '"points"', '"note"'))


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
