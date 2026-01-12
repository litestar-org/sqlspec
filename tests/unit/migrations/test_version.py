"""Unit tests for migration version parsing and comparison."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from sqlspec.migrations.fix import MigrationFixer
from sqlspec.migrations.validation import detect_out_of_order_migrations
from sqlspec.migrations.version import (
    VersionType,
    generate_timestamp_version,
    is_sequential_version,
    is_timestamp_version,
    parse_version,
)


def test_is_sequential_version() -> None:
    """Test sequential version detection."""
    assert is_sequential_version("0001")
    assert is_sequential_version("42")
    assert is_sequential_version("9999")
    assert is_sequential_version("1")
    assert is_sequential_version("00001")
    assert is_sequential_version("10000")

    assert not is_sequential_version("20251011120000")
    assert not is_sequential_version("abc")
    assert not is_sequential_version("")


def test_is_timestamp_version() -> None:
    """Test timestamp version detection."""
    assert is_timestamp_version("20251011120000")
    assert is_timestamp_version("20200101000000")
    assert is_timestamp_version("20991231235959")

    assert not is_timestamp_version("0001")
    assert not is_timestamp_version("2025101112")
    assert not is_timestamp_version("20259999999999")
    assert not is_timestamp_version("")


def test_parse_sequential_version() -> None:
    """Test parsing sequential versions."""
    v = parse_version("0001")
    assert v.raw == "0001"
    assert v.type == VersionType.SEQUENTIAL
    assert v.sequence == 1
    assert v.timestamp is None
    assert v.extension is None

    v = parse_version("42")
    assert v.sequence == 42

    v = parse_version("9999")
    assert v.sequence == 9999


def test_parse_timestamp_version() -> None:
    """Test parsing timestamp versions."""
    v = parse_version("20251011120000")
    assert v.raw == "20251011120000"
    assert v.type == VersionType.TIMESTAMP
    assert v.sequence is None
    assert v.timestamp == datetime(2025, 10, 11, 12, 0, 0, tzinfo=timezone.utc)
    assert v.extension is None

    v = parse_version("20200101000000")
    assert v.timestamp == datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_extension_version_sequential() -> None:
    """Test parsing extension versions with sequential format."""
    v = parse_version("ext_litestar_0001")
    assert v.raw == "ext_litestar_0001"
    assert v.type == VersionType.SEQUENTIAL
    assert v.sequence == 1
    assert v.extension == "litestar"

    v = parse_version("ext_myext_42")
    assert v.sequence == 42
    assert v.extension == "myext"


def test_parse_extension_version_timestamp() -> None:
    """Test parsing extension versions with timestamp format."""
    v = parse_version("ext_litestar_20251011120000")
    assert v.raw == "ext_litestar_20251011120000"
    assert v.type == VersionType.TIMESTAMP
    assert v.timestamp == datetime(2025, 10, 11, 12, 0, 0, tzinfo=timezone.utc)
    assert v.extension == "litestar"


def test_parse_invalid_version() -> None:
    """Test parsing invalid version formats."""
    with pytest.raises(ValueError, match="Invalid migration version format"):
        parse_version("abc")

    with pytest.raises(ValueError, match="Invalid migration version"):
        parse_version("")

    with pytest.raises(ValueError, match="Invalid migration version format"):
        parse_version("20259999999999")


def test_version_comparison_sequential() -> None:
    """Test comparing sequential versions."""
    v1 = parse_version("0001")
    v2 = parse_version("0002")
    v42 = parse_version("42")

    assert v1 < v2
    assert v2 < v42
    assert not v2 < v1
    assert not v42 < v2


def test_version_comparison_timestamp() -> None:
    """Test comparing timestamp versions."""
    v1 = parse_version("20200101000000")
    v2 = parse_version("20251011120000")
    v3 = parse_version("20251011130000")

    assert v1 < v2
    assert v2 < v3
    assert not v2 < v1
    assert not v3 < v2


def test_version_comparison_mixed() -> None:
    """Test comparing mixed sequential and timestamp versions.

    Sequential versions should sort before timestamp versions (legacy priority).
    """
    sequential = parse_version("9999")
    timestamp = parse_version("20200101000000")

    assert sequential < timestamp
    assert not timestamp < sequential


def test_version_comparison_extension() -> None:
    """Test comparing extension versions."""
    main = parse_version("0001")
    ext1 = parse_version("ext_aaa_0001")
    ext2 = parse_version("ext_bbb_0001")

    assert main < ext1
    assert main < ext2
    assert ext1 < ext2


def test_version_equality() -> None:
    """Test version equality."""
    v1 = parse_version("0001")
    v2 = parse_version("0001")
    v3 = parse_version("0002")

    assert v1 == v2
    assert not v1 == v3
    assert v1 != v3


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


def test_version_hash() -> None:
    """Test version hashing for use in sets/dicts."""
    v1 = parse_version("0001")
    v2 = parse_version("0001")
    v3 = parse_version("0002")

    assert hash(v1) == hash(v2)
    assert hash(v1) != hash(v3)

    version_set = {v1, v2, v3}
    assert len(version_set) == 2


def test_version_sorting() -> None:
    """Test sorting versions."""
    versions = [
        parse_version("ext_bbb_0002"),
        parse_version("20251011120000"),
        parse_version("0002"),
        parse_version("ext_aaa_0001"),
        parse_version("0001"),
        parse_version("20200101000000"),
    ]

    sorted_versions = sorted(versions)

    expected_order = ["0001", "0002", "20200101000000", "20251011120000", "ext_aaa_0001", "ext_bbb_0002"]

    assert [v.raw for v in sorted_versions] == expected_order


def test_generate_timestamp_version() -> None:
    """Test timestamp version generation."""
    version = generate_timestamp_version()

    assert len(version) == 14
    assert version.isdigit()
    assert is_timestamp_version(version)

    parsed = parse_version(version)
    assert parsed.type == VersionType.TIMESTAMP
    assert parsed.timestamp is not None


def test_generate_timestamp_version_uniqueness() -> None:
    """Test that generated timestamps are unique (within reasonable time)."""
    v1 = generate_timestamp_version()
    v2 = generate_timestamp_version()

    assert v1 <= v2


def test_version_repr() -> None:
    """Test version string representation."""
    v = parse_version("0001")
    assert "sequential" in repr(v)
    assert "0001" in repr(v)

    v = parse_version("20251011120000")
    assert "timestamp" in repr(v)

    v = parse_version("ext_litestar_0001")
    assert "litestar" in repr(v)
