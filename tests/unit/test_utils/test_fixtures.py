"""Tests for sqlspec.utils.fixtures module.

Tests fixture loading utilities including synchronous and asynchronous
JSON fixture file loading with compression support.
"""

import gzip
import json
import tempfile
import zipfile
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sqlspec.utils.fixtures import (
    _find_fixture_file,
    _read_compressed_file,
    _serialize_data,
    open_fixture,
    open_fixture_async,
    write_fixture,
    write_fixture_async,
)

pytestmark = pytest.mark.xdist_group("utils")


def test_find_fixture_file_json() -> None:
    """Test finding regular .json fixture file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        fixture_file = fixtures_path / "test.json"
        fixture_file.write_text('{"test": "data"}')

        result = _find_fixture_file(fixtures_path, "test")
        assert result == fixture_file


def test_find_fixture_file_gz_priority() -> None:
    """Test .json.gz takes priority over .json when both exist."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        json_file = fixtures_path / "test.json"
        gz_file = fixtures_path / "test.json.gz"

        json_file.write_text('{"test": "json"}')
        with gzip.open(gz_file, "wt") as f:
            json.dump({"test": "gz"}, f)

        result = _find_fixture_file(fixtures_path, "test")
        assert result == json_file  # .json has highest priority


def test_find_fixture_file_zip_fallback() -> None:
    """Test .json.zip is found when .json and .json.gz don't exist."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        zip_file = fixtures_path / "test.json.zip"

        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr("test.json", '{"test": "zip"}')

        result = _find_fixture_file(fixtures_path, "test")
        assert result == zip_file


def test_find_fixture_file_not_found() -> None:
    """Test FileNotFoundError when no fixture file exists."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)

        with pytest.raises(FileNotFoundError, match="Could not find the missing fixture"):
            _find_fixture_file(fixtures_path, "missing")


def test_read_gzip_file() -> None:
    """Test reading gzipped JSON file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        gz_file = Path(temp_dir) / "test.json.gz"
        test_data = {"test": "gzipped data", "number": 42}

        with gzip.open(gz_file, "wt", encoding="utf-8") as f:
            json.dump(test_data, f)

        result = _read_compressed_file(gz_file)
        assert json.loads(result) == test_data


def test_read_zip_file_with_matching_name() -> None:
    """Test reading ZIP file with matching JSON filename."""
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_file = Path(temp_dir) / "test.json.zip"
        test_data = {"test": "zipped data", "array": [1, 2, 3]}

        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr("test.json", json.dumps(test_data))

        result = _read_compressed_file(zip_file)
        assert json.loads(result) == test_data


def test_read_zip_file_first_json() -> None:
    """Test reading ZIP file with first JSON file when no matching name."""
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_file = Path(temp_dir) / "archive.zip"
        test_data = {"test": "first json file"}

        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr("data.json", json.dumps(test_data))
            zf.writestr("other.txt", "not json")

        result = _read_compressed_file(zip_file)
        assert json.loads(result) == test_data


def test_read_zip_file_no_json() -> None:
    """Test error when ZIP file contains no JSON files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_file = Path(temp_dir) / "empty.zip"

        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr("data.txt", "not json")

        with pytest.raises(ValueError, match="No JSON file found in ZIP archive"):
            _read_compressed_file(zip_file)


def test_read_unsupported_format() -> None:
    """Test error for unsupported compression format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        unsupported_file = Path(temp_dir) / "test.tar.gz"
        unsupported_file.write_text("data")

        # gzip module attempts to read .tar.gz files and raises BadGzipFile
        with pytest.raises(gzip.BadGzipFile):
            _read_compressed_file(unsupported_file)


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

        class TestModel(BaseModel):
            name: str
            value: int

        model = TestModel(name="test", value=42)
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

        class TestStruct(msgspec.Struct):
            name: str
            value: int

        struct = TestStruct(name="test", value=42)
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


def test_open_fixture_valid_file() -> None:
    """Test open_fixture with valid JSON fixture file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        fixture_file = fixtures_path / "test_fixture.json"

        test_data = {"name": "test", "value": 42, "items": [1, 2, 3]}
        with fixture_file.open("w") as f:
            json.dump(test_data, f)

        result = open_fixture(fixtures_path, "test_fixture")
        assert result == test_data


def test_open_fixture_gzipped() -> None:
    """Test open_fixture with gzipped JSON file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        fixture_file = fixtures_path / "test.json.gz"

        test_data = {"compressed": True, "data": [1, 2, 3]}
        with gzip.open(fixture_file, "wt", encoding="utf-8") as f:
            json.dump(test_data, f)

        result = open_fixture(fixtures_path, "test")
        assert result == test_data


def test_open_fixture_zipped() -> None:
    """Test open_fixture with zipped JSON file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        fixture_file = fixtures_path / "test.json.zip"

        test_data = {"zipped": True, "values": ["a", "b", "c"]}
        with zipfile.ZipFile(fixture_file, "w") as zf:
            zf.writestr("test.json", json.dumps(test_data))

        result = open_fixture(fixtures_path, "test")
        assert result == test_data


def test_open_fixture_missing_file() -> None:
    """Test open_fixture with missing fixture file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)

        with pytest.raises(FileNotFoundError, match="Could not find the nonexistent fixture"):
            open_fixture(fixtures_path, "nonexistent")


def test_open_fixture_invalid_json() -> None:
    """Test open_fixture with invalid JSON."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        fixture_file = fixtures_path / "invalid.json"

        with fixture_file.open("w") as f:
            f.write("{ invalid json content")

        with pytest.raises(Exception):
            open_fixture(fixtures_path, "invalid")


@pytest.mark.asyncio
async def test_open_fixture_async_valid_file() -> None:
    """Test open_fixture_async with valid JSON fixture file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        fixture_file = fixtures_path / "test_async.json"

        test_data = {"async": True, "data": {"nested": "value"}}
        with fixture_file.open("w") as f:
            json.dump(test_data, f)

        result = await open_fixture_async(fixtures_path, "test_async")
        assert result == test_data


@pytest.mark.asyncio
async def test_open_fixture_async_gzipped() -> None:
    """Test open_fixture_async with gzipped file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        fixture_file = fixtures_path / "async_gz.json.gz"

        test_data = {"async_compressed": True, "numbers": [1, 2, 3, 4, 5]}
        with gzip.open(fixture_file, "wt", encoding="utf-8") as f:
            json.dump(test_data, f)

        result = await open_fixture_async(fixtures_path, "async_gz")
        assert result == test_data


@pytest.mark.asyncio
async def test_open_fixture_async_zipped() -> None:
    """Test open_fixture_async with zipped file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        fixture_file = fixtures_path / "async_zip.json.zip"

        test_data = {"async_zipped": True, "items": ["x", "y", "z"]}
        with zipfile.ZipFile(fixture_file, "w") as zf:
            zf.writestr("async_zip.json", json.dumps(test_data))

        result = await open_fixture_async(fixtures_path, "async_zip")
        assert result == test_data


@pytest.mark.asyncio
async def test_open_fixture_async_missing_file() -> None:
    """Test open_fixture_async with missing fixture file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)

        with pytest.raises(FileNotFoundError, match="Could not find the missing_async fixture"):
            await open_fixture_async(fixtures_path, "missing_async")


def test_write_fixture_dict() -> None:
    """Test writing a dictionary fixture."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data: Any = {"name": "test", "value": 42, "active": True}

        write_fixture(temp_dir, "test_dict", test_data)

        # Verify file was created
        fixture_file = Path(temp_dir) / "test_dict.json"
        assert fixture_file.exists()

        # Verify content
        loaded_data = open_fixture(temp_dir, "test_dict")
        assert loaded_data == test_data


def test_write_fixture_list() -> None:
    """Test writing a list fixture."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data: Any = [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]

        write_fixture(temp_dir, "test_list", test_data)
        loaded_data = open_fixture(temp_dir, "test_list")
        assert loaded_data == test_data


def test_write_fixture_compressed() -> None:
    """Test writing a compressed fixture."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data: Any = {"compressed": True, "data": list(range(100))}

        write_fixture(temp_dir, "test_compressed", test_data, compress=True)

        # Verify gzipped file was created
        fixture_file = Path(temp_dir) / "test_compressed.json.gz"
        assert fixture_file.exists()

        # Verify content can be read
        loaded_data = open_fixture(temp_dir, "test_compressed")
        assert loaded_data == test_data


def test_write_fixture_storage_backend_error() -> None:
    """Test error handling for invalid storage backend."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data: Any = {"test": "data"}

        with pytest.raises(ValueError, match="Failed to get storage backend"):
            write_fixture(temp_dir, "test", test_data, storage_backend="invalid://backend")


@patch("sqlspec.utils.fixtures.storage_registry")
def test_write_fixture_with_custom_backend(mock_registry: Mock) -> None:
    """Test write_fixture with custom storage backend."""
    mock_storage = Mock()
    mock_registry.get.return_value = mock_storage

    test_data: Any = {"custom": "backend"}
    write_fixture("/tmp", "test", test_data, storage_backend="s3://bucket", custom_param="value")

    # Verify storage backend was called correctly
    mock_registry.get.assert_called_once_with("s3://bucket", custom_param="value")
    mock_storage.write_text.assert_called_once()


@pytest.mark.asyncio
async def test_write_fixture_async_dict() -> None:
    """Test async writing a dictionary fixture."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data: Any = {"async_write": True, "value": 123}

        await write_fixture_async(temp_dir, "async_test", test_data)

        # Verify file was created and content is correct
        loaded_data = await open_fixture_async(temp_dir, "async_test")
        assert loaded_data == test_data


@pytest.mark.asyncio
async def test_write_fixture_async_compressed() -> None:
    """Test async writing a compressed fixture."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data: Any = {"async_compressed": True, "large_data": list(range(50))}

        await write_fixture_async(temp_dir, "async_compressed", test_data, compress=True)

        # Verify gzipped file was created
        fixture_file = Path(temp_dir) / "async_compressed.json.gz"
        assert fixture_file.exists()

        # Verify content
        loaded_data = await open_fixture_async(temp_dir, "async_compressed")
        assert loaded_data == test_data


@pytest.mark.asyncio
async def test_write_fixture_async_storage_error() -> None:
    """Test async error handling for invalid storage backend."""
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data: Any = {"test": "data"}

        with pytest.raises(ValueError, match="Failed to get storage backend"):
            await write_fixture_async(temp_dir, "test", test_data, storage_backend="invalid://backend")


@pytest.mark.asyncio
@patch("sqlspec.utils.fixtures.storage_registry")
async def test_write_fixture_async_custom_backend(mock_registry: Mock) -> None:
    """Test async write_fixture with custom storage backend."""
    mock_storage = AsyncMock()
    mock_registry.get.return_value = mock_storage

    test_data: Any = {"async_custom": "backend"}
    await write_fixture_async(
        "/tmp", "async_test", test_data, storage_backend="gcs://bucket", custom_param="async_value"
    )

    # Verify storage backend was called correctly
    mock_registry.get.assert_called_once_with("gcs://bucket", custom_param="async_value")
    mock_storage.write_text_async.assert_called_once()


def test_write_read_roundtrip() -> None:
    """Test complete write and read roundtrip."""
    with tempfile.TemporaryDirectory() as temp_dir:
        original_data: Any = {
            "users": [{"id": 1, "name": "Alice", "active": True}, {"id": 2, "name": "Bob", "active": False}],
            "metadata": {"version": "1.0", "created": "2024-01-01", "total_users": 2},
        }

        # Write fixture
        write_fixture(temp_dir, "integration_test", original_data)

        # Read fixture back
        loaded_data = open_fixture(temp_dir, "integration_test")

        # Verify data integrity
        assert loaded_data == original_data


@pytest.mark.asyncio
async def test_async_write_read_roundtrip() -> None:
    """Test complete async write and read roundtrip."""
    with tempfile.TemporaryDirectory() as temp_dir:
        original_data: Any = {
            "async_test": True,
            "data": {"nested": {"deeply": {"value": 42}}},
            "list_data": [{"item": i} for i in range(10)],
        }

        # Write fixture async
        await write_fixture_async(temp_dir, "async_integration", original_data)

        # Read fixture back async
        loaded_data = await open_fixture_async(temp_dir, "async_integration")

        # Verify data integrity
        assert loaded_data == original_data


def test_compressed_roundtrip() -> None:
    """Test write and read roundtrip with compression."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Large data that benefits from compression
        original_data: Any = {
            "large_list": [{"id": i, "data": f"item_{i}" * 10} for i in range(100)],
            "repeated_data": ["same_string"] * 50,
        }

        # Write compressed
        write_fixture(temp_dir, "compressed_test", original_data, compress=True)

        # Read back
        loaded_data = open_fixture(temp_dir, "compressed_test")

        # Verify data integrity
        assert loaded_data == original_data

        # Verify file is actually compressed
        compressed_file = Path(temp_dir) / "compressed_test.json.gz"
        assert compressed_file.exists()
        assert compressed_file.suffix == ".gz"
