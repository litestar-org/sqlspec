"""Unit tests for edge cases in migration version parsing."""

import pytest

from sqlspec.migrations.version import (
    VersionType,
    convert_to_sequential_version,
    generate_conversion_map,
    get_next_sequential_number,
    is_sequential_version,
    parse_version,
)


def test_is_sequential_version_no_digit_cap() -> None:
    """Test sequential version detection works without 4-digit limitation."""
    assert is_sequential_version("10000")
    assert is_sequential_version("99999")
    assert is_sequential_version("12345")
    assert is_sequential_version("00001")


def test_parse_sequential_version_large_numbers() -> None:
    """Test parsing sequential versions beyond 9999."""
    v = parse_version("10000")
    assert v.raw == "10000"
    assert v.type == VersionType.SEQUENTIAL
    assert v.sequence == 10000
    assert v.timestamp is None

    v = parse_version("99999")
    assert v.sequence == 99999


def test_version_comparison_large_sequential() -> None:
    """Test comparing large sequential versions."""
    v1 = parse_version("9999")
    v2 = parse_version("10000")
    v3 = parse_version("10001")

    assert v1 < v2
    assert v2 < v3
    assert not v2 < v1


def test_version_comparison_extension_with_large_numbers() -> None:
    """Test extension versions with large sequential numbers."""
    ext1 = parse_version("ext_litestar_9999")
    ext2 = parse_version("ext_litestar_10000")

    assert ext1 < ext2


def test_get_next_sequential_number_after_9999() -> None:
    """Test getting next sequential number after exceeding 9999."""
    v1 = parse_version("9999")
    v2 = parse_version("10000")

    next_num = get_next_sequential_number([v1, v2])
    assert next_num == 10001


def test_get_next_sequential_number_with_extension() -> None:
    """Test getting next sequential number for extension migrations."""
    core1 = parse_version("0001")
    core2 = parse_version("0002")
    ext1 = parse_version("ext_litestar_0001")
    ext2 = parse_version("ext_litestar_0002")

    next_core = get_next_sequential_number([core1, core2, ext1, ext2], extension=None)
    assert next_core == 3

    next_ext = get_next_sequential_number([core1, core2, ext1, ext2], extension="litestar")
    assert next_ext == 3


def test_get_next_sequential_number_only_timestamps() -> None:
    """Test getting next sequential number when only timestamp versions exist."""
    v1 = parse_version("20251011120000")
    v2 = parse_version("20251012130000")

    next_num = get_next_sequential_number([v1, v2])
    assert next_num == 1


def test_convert_to_sequential_version_preserves_extension() -> None:
    """Test converting timestamp to sequential preserves extension prefix."""
    timestamp_version = parse_version("ext_litestar_20251011120000")
    sequential = convert_to_sequential_version(timestamp_version, 5)

    assert sequential == "ext_litestar_0005"


def test_convert_to_sequential_version_large_sequence() -> None:
    """Test converting timestamp to large sequential number."""
    timestamp_version = parse_version("20251011120000")
    sequential = convert_to_sequential_version(timestamp_version, 10000)

    assert sequential == "10000"


def test_convert_to_sequential_version_rejects_sequential() -> None:
    """Test converting sequential version raises error."""
    sequential_version = parse_version("0001")

    with pytest.raises(ValueError, match="Can only convert timestamp versions"):
        convert_to_sequential_version(sequential_version, 2)


def test_generate_conversion_map_with_extensions() -> None:
    """Test conversion map generation with extension migrations."""
    from pathlib import Path

    migrations = [
        ("0001", Path("0001_init.sql")),
        ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
        ("20251011120000", Path("20251011120000_users.sql")),
        ("ext_litestar_20251012130000", Path("ext_litestar_20251012130000_users.sql")),
    ]

    conversion_map = generate_conversion_map(migrations)

    assert conversion_map["20251011120000"] == "0002"
    assert conversion_map["ext_litestar_20251012130000"] == "ext_litestar_0002"
    assert "0001" not in conversion_map
    assert "ext_litestar_0001" not in conversion_map


def test_generate_conversion_map_maintains_chronological_order() -> None:
    """Test conversion map assigns sequential numbers in chronological order."""
    from pathlib import Path

    migrations = [
        ("20251011120000", Path("20251011120000_third.sql")),
        ("20251010100000", Path("20251010100000_first.sql")),
        ("20251010120000", Path("20251010120000_second.sql")),
    ]

    conversion_map = generate_conversion_map(migrations)

    assert conversion_map["20251010100000"] == "0001"
    assert conversion_map["20251010120000"] == "0002"
    assert conversion_map["20251011120000"] == "0003"


def test_generate_conversion_map_separate_extension_namespaces() -> None:
    """Test extension migrations maintain separate sequential namespaces."""
    from pathlib import Path

    migrations = [
        ("0001", Path("0001_init.sql")),
        ("ext_aaa_0001", Path("ext_aaa_0001_init.sql")),
        ("ext_bbb_0001", Path("ext_bbb_0001_init.sql")),
        ("ext_aaa_20251011120000", Path("ext_aaa_20251011120000_users.sql")),
        ("ext_bbb_20251012130000", Path("ext_bbb_20251012130000_products.sql")),
    ]

    conversion_map = generate_conversion_map(migrations)

    assert conversion_map["ext_aaa_20251011120000"] == "ext_aaa_0002"
    assert conversion_map["ext_bbb_20251012130000"] == "ext_bbb_0002"


def test_version_sorting_with_large_numbers() -> None:
    """Test version sorting works correctly with large sequential numbers."""
    versions = [
        parse_version("10001"),
        parse_version("0001"),
        parse_version("9999"),
        parse_version("10000"),
        parse_version("20251011120000"),
    ]

    sorted_versions = sorted(versions)

    expected_order = ["0001", "9999", "10000", "10001", "20251011120000"]
    assert [v.raw for v in sorted_versions] == expected_order


def test_version_comparison_sequential_vs_timestamp_edge_case() -> None:
    """Test that even very large sequential numbers sort before timestamps."""
    large_sequential = parse_version("99999")
    early_timestamp = parse_version("20000101000000")

    assert large_sequential < early_timestamp
    assert not early_timestamp < large_sequential


def test_get_next_sequential_number_mixed_extensions() -> None:
    """Test getting next sequential with mixed core and extension migrations."""
    core1 = parse_version("0001")
    ext_litestar = parse_version("ext_litestar_0001")
    ext_adk = parse_version("ext_adk_0001")
    timestamp = parse_version("20251011120000")

    next_core = get_next_sequential_number([core1, ext_litestar, ext_adk, timestamp], extension=None)
    assert next_core == 2

    next_litestar = get_next_sequential_number([core1, ext_litestar, ext_adk, timestamp], extension="litestar")
    assert next_litestar == 2

    next_adk = get_next_sequential_number([core1, ext_litestar, ext_adk, timestamp], extension="adk")
    assert next_adk == 2


def test_generate_conversion_map_empty_list() -> None:
    """Test conversion map generation with empty migration list."""
    conversion_map = generate_conversion_map([])
    assert conversion_map == {}


def test_generate_conversion_map_only_sequential() -> None:
    """Test conversion map generation when only sequential migrations exist."""
    from pathlib import Path

    migrations = [("0001", Path("0001_init.sql")), ("0002", Path("0002_users.sql"))]

    conversion_map = generate_conversion_map(migrations)
    assert conversion_map == {}


def test_generate_conversion_map_invalid_versions_skipped() -> None:
    """Test conversion map skips invalid version strings."""
    from pathlib import Path

    migrations = [
        ("0001", Path("0001_init.sql")),
        ("invalid", Path("invalid_migration.sql")),
        ("20251011120000", Path("20251011120000_users.sql")),
    ]

    conversion_map = generate_conversion_map(migrations)

    assert "20251011120000" in conversion_map
    assert conversion_map["20251011120000"] == "0002"
    assert "invalid" not in conversion_map
