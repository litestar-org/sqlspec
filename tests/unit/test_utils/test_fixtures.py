"""Tests for sqlspec.utils.fixtures module."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import msgspec
import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.utils.fixtures import open_fixture, open_fixture_async


@pytest.fixture
def temp_fixtures_dir() -> Generator[Path, None, None]:
    """Create a temporary directory with test fixture files."""
    with TemporaryDirectory() as tmp_dir:
        fixtures_path = Path(tmp_dir)

        # Create test fixture files
        test_data = {"name": "test", "value": 42, "nested": {"key": "value"}}
        (fixtures_path / "test_fixture.json").write_text(json.dumps(test_data))

        empty_data: dict[str, Any] = {}
        (fixtures_path / "empty_fixture.json").write_text(json.dumps(empty_data))

        complex_data = {
            "users": [
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
                {"id": 2, "name": "Bob", "email": "bob@example.com"},
            ],
            "metadata": {"version": "1.0", "created": "2023-01-01"},
        }
        (fixtures_path / "complex_fixture.json").write_text(json.dumps(complex_data))

        # Create invalid JSON file
        (fixtures_path / "invalid_fixture.json").write_text("{ invalid json")

        yield fixtures_path


def test_open_fixture_basic_success(temp_fixtures_dir: Path) -> None:
    """Test basic successful fixture loading."""
    result = open_fixture(temp_fixtures_dir, "test_fixture")

    expected = {"name": "test", "value": 42, "nested": {"key": "value"}}
    assert result == expected


def test_open_fixture_empty_file(temp_fixtures_dir: Path) -> None:
    """Test loading an empty JSON fixture."""
    result = open_fixture(temp_fixtures_dir, "empty_fixture")

    assert result == {}


def test_open_fixture_complex_data(temp_fixtures_dir: Path) -> None:
    """Test loading a complex JSON fixture."""
    result = open_fixture(temp_fixtures_dir, "complex_fixture")

    assert "users" in result
    assert len(result["users"]) == 2
    assert result["users"][0]["name"] == "Alice"
    assert result["metadata"]["version"] == "1.0"


def test_open_fixture_file_not_found(temp_fixtures_dir: Path) -> None:
    """Test that FileNotFoundError is raised for non-existent fixtures."""
    with pytest.raises(FileNotFoundError, match="Could not find the nonexistent fixture"):
        open_fixture(temp_fixtures_dir, "nonexistent")


def test_open_fixture_invalid_json(temp_fixtures_dir: Path) -> None:
    """Test that JSON decode errors are raised for invalid JSON."""
    with pytest.raises((json.JSONDecodeError, msgspec.DecodeError)):
        open_fixture(temp_fixtures_dir, "invalid_fixture")


def test_open_fixture_with_pathlib_path(temp_fixtures_dir: Path) -> None:
    """Test that open_fixture works with pathlib.Path objects."""
    result = open_fixture(temp_fixtures_dir, "test_fixture")

    expected = {"name": "test", "value": 42, "nested": {"key": "value"}}
    assert result == expected


@patch("sqlspec.utils.fixtures.decode_json")
def test_open_fixture_uses_decode_json(mock_decode_json: Mock, temp_fixtures_dir: Path) -> None:
    """Test that open_fixture uses the decode_json function."""
    mock_decode_json.return_value = {"mocked": "data"}

    result = open_fixture(temp_fixtures_dir, "test_fixture")

    assert result == {"mocked": "data"}
    mock_decode_json.assert_called_once()
    # Verify the JSON content was passed to decode_json
    call_args = mock_decode_json.call_args[0][0]
    assert "test" in call_args
    assert "42" in call_args


def test_open_fixture_file_encoding(temp_fixtures_dir: Path) -> None:
    """Test that files are read with UTF-8 encoding."""
    # Create a fixture with UTF-8 characters
    utf8_data = {"message": "Hello 世界", "emoji": "🚀"}
    utf8_file = temp_fixtures_dir / "utf8_fixture.json"
    utf8_file.write_text(json.dumps(utf8_data), encoding="utf-8")

    result = open_fixture(temp_fixtures_dir, "utf8_fixture")

    assert result["message"] == "Hello 世界"
    assert result["emoji"] == "🚀"


@pytest.mark.asyncio
async def test_open_fixture_async_basic_success(temp_fixtures_dir: Path) -> None:
    """Test basic successful async fixture loading."""
    result = await open_fixture_async(temp_fixtures_dir, "test_fixture")

    expected = {"name": "test", "value": 42, "nested": {"key": "value"}}
    assert result == expected


@pytest.mark.asyncio
async def test_open_fixture_async_file_not_found(temp_fixtures_dir: Path) -> None:
    """Test that async version raises FileNotFoundError for non-existent fixtures."""
    with pytest.raises(FileNotFoundError, match="Could not find the nonexistent fixture"):
        await open_fixture_async(temp_fixtures_dir, "nonexistent")


@pytest.mark.asyncio
async def test_open_fixture_async_complex_data(temp_fixtures_dir: Path) -> None:
    """Test async loading of complex JSON fixture."""
    result = await open_fixture_async(temp_fixtures_dir, "complex_fixture")

    assert "users" in result
    assert len(result["users"]) == 2
    assert result["metadata"]["version"] == "1.0"


@pytest.mark.asyncio
async def test_open_fixture_async_invalid_json(temp_fixtures_dir: Path) -> None:
    """Test that async version raises JSON decode errors for invalid JSON."""
    with pytest.raises((json.JSONDecodeError, msgspec.DecodeError)):
        await open_fixture_async(temp_fixtures_dir, "invalid_fixture")


@pytest.mark.asyncio
@patch("sqlspec.utils.fixtures.decode_json")
async def test_open_fixture_async_uses_decode_json(mock_decode_json: Mock, temp_fixtures_dir: Path) -> None:
    """Test that async open_fixture uses the decode_json function."""
    mock_decode_json.return_value = {"async_mocked": "data"}

    result = await open_fixture_async(temp_fixtures_dir, "test_fixture")

    assert result == {"async_mocked": "data"}
    mock_decode_json.assert_called_once()


@pytest.mark.asyncio
async def test_open_fixture_async_utf8_encoding(temp_fixtures_dir: Path) -> None:
    """Test that async version handles UTF-8 encoding correctly."""
    # Create a fixture with UTF-8 characters
    utf8_data = {"async_message": "Async Hello 世界", "async_emoji": "⚡"}
    utf8_file = temp_fixtures_dir / "async_utf8_fixture.json"
    utf8_file.write_text(json.dumps(utf8_data), encoding="utf-8")

    result = await open_fixture_async(temp_fixtures_dir, "async_utf8_fixture")

    assert result["async_message"] == "Async Hello 世界"
    assert result["async_emoji"] == "⚡"


def test_open_fixture_async_missing_anyio_dependency() -> None:
    """Test that MissingDependencyError is raised when anyio is not available."""
    with patch("sqlspec.utils.fixtures.Path") as _:
        # Only patch sys.modules to simulate missing anyio
        with patch.dict("sys.modules", {"anyio": None}):
            with pytest.raises(MissingDependencyError, match="Package 'anyio' is not installed"):
                import asyncio

                async def test_coro() -> None:
                    await open_fixture_async(Path("/tmp"), "test")

                asyncio.run(test_coro())


@pytest.mark.asyncio
@patch("anyio.Path")
async def test_open_fixture_async_with_anyio_path(mock_async_path_class: Mock, temp_fixtures_dir: Path) -> None:
    """Test that async version works with anyio.Path objects."""
    # Mock anyio.Path behavior
    mock_async_path = AsyncMock()
    mock_async_path_class.side_effect = lambda *a: mock_async_path  # type: ignore
    mock_async_path.exists.return_value = True

    # Mock the file operations
    mock_file = AsyncMock()
    mock_file.read.return_value = '{"async_anyio": "test"}'
    mock_async_path.open.return_value.__aenter__.return_value = mock_file

    with patch("sqlspec.utils.fixtures.decode_json", return_value={"async_anyio": "test"}):
        result = await open_fixture_async(temp_fixtures_dir, "test_fixture")

    assert result == {"async_anyio": "test"}
    mock_async_path_class.assert_called_once_with(temp_fixtures_dir / "test_fixture.json")
    mock_async_path.open.assert_called_once_with(mode="r", encoding="utf-8")


@pytest.mark.parametrize(
    ("fixture_name", "expected_filename"),
    [
        ("simple", "simple.json"),
        ("with_underscore", "with_underscore.json"),
        ("with-hyphen", "with-hyphen.json"),
        ("123numeric", "123numeric.json"),
        ("", ".json"),  # Edge case: empty name
    ],
    ids=["simple_name", "with_underscore", "with_hyphen", "numeric_name", "empty_name"],
)
def test_open_fixture_filename_construction(temp_fixtures_dir: Path, fixture_name: str, expected_filename: str) -> None:
    """Test that fixture filenames are constructed correctly."""
    # Create the expected file
    test_data = {"fixture_name": fixture_name}
    file_path = temp_fixtures_dir / expected_filename
    file_path.write_text(json.dumps(test_data))

    if fixture_name == "":
        # Remove the file to ensure FileNotFoundError is raised
        file_path.unlink()
        with pytest.raises(FileNotFoundError):
            open_fixture(temp_fixtures_dir, fixture_name)
    else:
        result = open_fixture(temp_fixtures_dir, fixture_name)
        assert result["fixture_name"] == fixture_name


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fixture_name", "expected_filename"),
    [("async_simple", "async_simple.json"), ("async_with_underscore", "async_with_underscore.json")],
    ids=["async_simple_name", "async_with_underscore"],
)
async def test_open_fixture_async_filename_construction(
    temp_fixtures_dir: Path, fixture_name: str, expected_filename: str
) -> None:
    """Test that async fixture filenames are constructed correctly."""
    # Create the expected file
    test_data = {"async_fixture_name": fixture_name}
    (temp_fixtures_dir / expected_filename).write_text(json.dumps(test_data))

    result = await open_fixture_async(temp_fixtures_dir, fixture_name)
    assert result["async_fixture_name"] == fixture_name


def test_open_fixture_path_traversal_security(temp_fixtures_dir: Path) -> None:
    """Test that path traversal attempts are handled safely."""
    # Try to access a file outside the fixtures directory
    with pytest.raises(FileNotFoundError):
        open_fixture(temp_fixtures_dir, "../../../etc/passwd")


@pytest.mark.asyncio
async def test_open_fixture_async_path_traversal_security(temp_fixtures_dir: Path) -> None:
    """Test that async version handles path traversal attempts safely."""
    with pytest.raises(FileNotFoundError):
        await open_fixture_async(temp_fixtures_dir, "../../../etc/passwd")


def test_open_fixture_large_file_handling(temp_fixtures_dir: Path) -> None:
    """Test handling of larger JSON files."""
    # Create a larger fixture with nested data
    large_data = {
        "items": [{"id": i, "name": f"item_{i}", "data": list(range(10))} for i in range(100)],
        "metadata": {"count": 100, "description": "Large test fixture"},
    }
    (temp_fixtures_dir / "large_fixture.json").write_text(json.dumps(large_data))

    result = open_fixture(temp_fixtures_dir, "large_fixture")

    assert len(result["items"]) == 100
    assert result["items"][0]["name"] == "item_0"
    assert result["metadata"]["count"] == 100


@pytest.mark.asyncio
async def test_open_fixture_async_large_file_handling(temp_fixtures_dir: Path) -> None:
    """Test async handling of larger JSON files."""
    # Create a larger fixture with nested data
    large_data = {
        "async_items": [{"id": i, "name": f"async_item_{i}"} for i in range(50)],
        "async_metadata": {"count": 50},
    }
    (temp_fixtures_dir / "async_large_fixture.json").write_text(json.dumps(large_data))

    result = await open_fixture_async(temp_fixtures_dir, "async_large_fixture")

    assert len(result["async_items"]) == 50
    assert result["async_items"][0]["name"] == "async_item_0"
    assert result["async_metadata"]["count"] == 50
