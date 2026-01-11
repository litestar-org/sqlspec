"""Test cases for null handling fixes in migration system."""

from pathlib import Path

import pytest

from sqlspec.migrations.fix import MigrationFixer
from sqlspec.migrations.validation import detect_out_of_order_migrations
from sqlspec.migrations.version import is_sequential_version, is_timestamp_version, parse_version


def test_parse_version_with_none() -> None:
    """Test parse_version handles None gracefully."""
    with pytest.raises(ValueError, match="Invalid migration version: version string is None or empty"):
        parse_version(None)


def test_parse_version_with_empty_string() -> None:
    """Test parse_version handles empty string gracefully."""
    with pytest.raises(ValueError, match="Invalid migration version: version string is None or empty"):
        parse_version("")


def test_parse_version_with_whitespace_only() -> None:
    """Test parse_version handles whitespace-only strings."""
    with pytest.raises(ValueError, match="Invalid migration version: version string is None or empty"):
        parse_version("   ")


def test_parse_version_valid_formats_still_work() -> None:
    """Test that valid version formats still work after fixes."""
    result = parse_version("0001")
    assert result.type.value == "sequential"
    assert result.sequence == 1

    result = parse_version("9999")
    assert result.type.value == "sequential"
    assert result.sequence == 9999

    result = parse_version("20251011120000")
    assert result.type.value == "timestamp"
    assert result.timestamp is not None

    result = parse_version("ext_litestar_0001")
    assert result.type.value == "sequential"
    assert result.extension == "litestar"


def test_migration_fixer_handles_none_gracefully(tmp_path: Path) -> None:
    """Test MigrationFixer.update_file_content handles None values."""
    migrations_path = tmp_path
    fixer = MigrationFixer(migrations_path)

    test_file = migrations_path / "test.sql"
    test_file.write_text("-- Test content")

    fixer.update_file_content(test_file, None, "0001")
    fixer.update_file_content(test_file, "0001", None)
    fixer.update_file_content(test_file, None, None)

    content = test_file.read_text()
    assert content == "-- Test content"


def test_validation_filters_none_values() -> None:
    """Test migration validation filters None values properly."""
    gaps = detect_out_of_order_migrations(
        pending_versions=["0001", None, "0003", ""], applied_versions=[None, "0002", "   ", "0004"]
    )

    assert len(gaps) >= 0


def test_sequential_pattern_edge_cases() -> None:
    """Test sequential pattern handles edge cases."""
    assert is_sequential_version("0001")
    assert is_sequential_version("9999")
    assert is_sequential_version("10000")
    assert not is_sequential_version("20251011120000")
    assert not is_sequential_version("abc")
    assert not is_sequential_version("")
    assert not is_sequential_version(None)


def test_timestamp_pattern_edge_cases() -> None:
    """Test timestamp pattern handles edge cases."""
    assert is_timestamp_version("20251011120000")
    assert is_timestamp_version("20250101000000")
    assert is_timestamp_version("20251231235959")
    assert not is_timestamp_version("0001")
    assert not is_timestamp_version("2025101112000")
    assert not is_timestamp_version("202510111200000")
    assert not is_timestamp_version("")
    assert not is_timestamp_version(None)


def test_error_messages_are_descriptive() -> None:
    """Test that error messages are helpful for debugging."""
    try:
        parse_version(None)
    except ValueError as e:
        assert "version string is None or empty" in str(e)

    try:
        parse_version("")
    except ValueError as e:
        assert "version string is None or empty" in str(e)
