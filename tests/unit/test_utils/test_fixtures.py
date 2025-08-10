"""Tests for sqlspec.utils.fixtures module.

Tests fixture loading utilities including synchronous and asynchronous
JSON fixture file loading.
"""

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.utils.fixtures import open_fixture, open_fixture_async


def test_open_fixture_valid_file() -> None:
    """Test open_fixture with valid JSON fixture file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        fixtures_path = Path(temp_dir)
        fixture_file = fixtures_path / "test_fixture.json"

        # Create a test fixture file
        test_data = {"name": "test", "value": 42, "items": [1, 2, 3]}
        with fixture_file.open("w") as f:
            import json

            json.dump(test_data, f)

        result = open_fixture(fixtures_path, "test_fixture")
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

        # Create invalid JSON file
        with fixture_file.open("w") as f:
            f.write("{ invalid json content")

        # Should raise an exception when trying to parse invalid JSON
        with pytest.raises(Exception):  # Could be JSONDecodeError or similar
            open_fixture(fixtures_path, "invalid")


@pytest.mark.asyncio
async def test_open_fixture_async_missing_anyio() -> None:
    """Test open_fixture_async raises error when anyio not available."""
    # Test by patching the import statement inside the function
    import builtins

    original_import = builtins.__import__

    def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "anyio":
            raise ImportError("No module named 'anyio'")
        return original_import(name, *args, **kwargs)

    with patch.object(builtins, "__import__", side_effect=mock_import):
        with pytest.raises(MissingDependencyError, match="anyio"):
            await open_fixture_async(Path("/tmp"), "test")
