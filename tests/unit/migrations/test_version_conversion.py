"""Unit tests for migration version conversion utilities."""

from pathlib import Path

import pytest

from sqlspec.migrations.version import (
    convert_to_sequential_version,
    generate_conversion_map,
    get_next_sequential_number,
    parse_version,
)


def test_get_next_sequential_number_empty() -> None:
    """Test next sequential number with empty list."""
    assert get_next_sequential_number([]) == 1


def test_get_next_sequential_number_single() -> None:
    """Test next sequential number with single migration."""
    v1 = parse_version("0001")
    assert get_next_sequential_number([v1]) == 2


def test_get_next_sequential_number_multiple() -> None:
    """Test next sequential number with multiple sequential migrations."""
    v1 = parse_version("0001")
    v2 = parse_version("0002")
    v3 = parse_version("0003")
    assert get_next_sequential_number([v1, v2, v3]) == 4


def test_get_next_sequential_number_with_timestamps() -> None:
    """Test next sequential number ignores timestamp migrations."""
    v1 = parse_version("0001")
    v2 = parse_version("0002")
    t1 = parse_version("20251011120000")
    t2 = parse_version("20251012130000")

    result = get_next_sequential_number([v1, t1, v2, t2])
    assert result == 3


def test_get_next_sequential_number_ignores_extensions() -> None:
    """Test next sequential number ignores extension migrations."""
    core = parse_version("0001")
    ext1 = parse_version("ext_litestar_0001")
    ext2 = parse_version("ext_litestar_0002")

    result = get_next_sequential_number([core, ext1, ext2])
    assert result == 2


def test_get_next_sequential_number_only_timestamps() -> None:
    """Test next sequential number with only timestamp migrations."""
    t1 = parse_version("20251011120000")
    t2 = parse_version("20251012130000")

    result = get_next_sequential_number([t1, t2])
    assert result == 1


def test_get_next_sequential_number_only_extensions() -> None:
    """Test next sequential number with only extension migrations."""
    ext1 = parse_version("ext_litestar_0001")
    ext2 = parse_version("ext_adk_0001")

    result = get_next_sequential_number([ext1, ext2])
    assert result == 1


def test_get_next_sequential_number_high_numbers() -> None:
    """Test next sequential number with high sequence numbers."""
    v1 = parse_version("9998")
    v2 = parse_version("9999")

    result = get_next_sequential_number([v1, v2])
    assert result == 10000


def test_convert_to_sequential_version_basic() -> None:
    """Test basic timestamp to sequential conversion."""
    v = parse_version("20251011120000")
    result = convert_to_sequential_version(v, 3)
    assert result == "0003"


def test_convert_to_sequential_version_zero_padding() -> None:
    """Test zero padding in sequential version."""
    v = parse_version("20251011120000")

    assert convert_to_sequential_version(v, 1) == "0001"
    assert convert_to_sequential_version(v, 10) == "0010"
    assert convert_to_sequential_version(v, 100) == "0100"
    assert convert_to_sequential_version(v, 1000) == "1000"


def test_convert_to_sequential_version_with_extension() -> None:
    """Test conversion preserves extension prefix."""
    v = parse_version("ext_litestar_20251011120000")
    result = convert_to_sequential_version(v, 1)
    assert result == "ext_litestar_0001"


def test_convert_to_sequential_version_various_extensions() -> None:
    """Test conversion with various extension names."""
    v1 = parse_version("ext_adk_20251011120000")
    assert convert_to_sequential_version(v1, 2) == "ext_adk_0002"

    v2 = parse_version("ext_myext_20251011120000")
    assert convert_to_sequential_version(v2, 42) == "ext_myext_0042"


def test_convert_to_sequential_version_rejects_sequential() -> None:
    """Test conversion rejects sequential input."""
    v = parse_version("0001")

    with pytest.raises(ValueError, match="Can only convert timestamp versions"):
        convert_to_sequential_version(v, 2)


def test_generate_conversion_map_empty() -> None:
    """Test conversion map generation with empty list."""
    result = generate_conversion_map([])
    assert result == {}


def test_generate_conversion_map_no_timestamps() -> None:
    """Test conversion map with only sequential migrations."""
    migrations = [("0001", Path("0001_init.sql")), ("0002", Path("0002_users.sql"))]
    result = generate_conversion_map(migrations)
    assert result == {}


def test_generate_conversion_map_basic() -> None:
    """Test basic conversion map generation."""
    migrations = [
        ("0001", Path("0001_init.sql")),
        ("0002", Path("0002_users.sql")),
        ("20251011120000", Path("20251011120000_products.sql")),
        ("20251012130000", Path("20251012130000_orders.sql")),
    ]

    result = generate_conversion_map(migrations)

    assert result == {"20251011120000": "0003", "20251012130000": "0004"}


def test_generate_conversion_map_chronological_order() -> None:
    """Test conversion map respects chronological order."""
    migrations = [
        ("20251012130000", Path("20251012130000_later.sql")),
        ("20251011120000", Path("20251011120000_earlier.sql")),
        ("20251010090000", Path("20251010090000_earliest.sql")),
    ]

    result = generate_conversion_map(migrations)

    assert result == {"20251010090000": "0001", "20251011120000": "0002", "20251012130000": "0003"}


def test_generate_conversion_map_only_timestamps() -> None:
    """Test conversion map with only timestamp migrations."""
    migrations = [
        ("20251011120000", Path("20251011120000_first.sql")),
        ("20251012130000", Path("20251012130000_second.sql")),
    ]

    result = generate_conversion_map(migrations)

    assert result == {"20251011120000": "0001", "20251012130000": "0002"}


def test_generate_conversion_map_mixed_formats() -> None:
    """Test conversion map with mixed sequential and timestamp."""
    migrations = [
        ("0001", Path("0001_init.sql")),
        ("20251011120000", Path("20251011120000_products.sql")),
        ("0002", Path("0002_users.sql")),
        ("20251012130000", Path("20251012130000_orders.sql")),
        ("0003", Path("0003_settings.sql")),
    ]

    result = generate_conversion_map(migrations)

    assert result == {"20251011120000": "0004", "20251012130000": "0005"}


def test_generate_conversion_map_with_extensions() -> None:
    """Test conversion map handles extension migrations correctly."""
    migrations = [
        ("0001", Path("0001_init.sql")),
        ("ext_litestar_20251011120000", Path("ext_litestar_20251011120000_sessions.py")),
        ("20251012130000", Path("20251012130000_products.sql")),
        ("ext_adk_20251011120000", Path("ext_adk_20251011120000_tables.py")),
    ]

    result = generate_conversion_map(migrations)

    assert result == {
        "20251012130000": "0002",
        "ext_litestar_20251011120000": "ext_litestar_0001",
        "ext_adk_20251011120000": "ext_adk_0001",
    }


def test_generate_conversion_map_extension_namespaces() -> None:
    """Test extension migrations maintain separate numbering."""
    migrations = [
        ("ext_litestar_0001", Path("ext_litestar_0001_existing.py")),
        ("ext_litestar_20251011120000", Path("ext_litestar_20251011120000_new.py")),
        ("ext_adk_20251011120000", Path("ext_adk_20251011120000_new.py")),
    ]

    result = generate_conversion_map(migrations)

    assert result == {"ext_litestar_20251011120000": "ext_litestar_0002", "ext_adk_20251011120000": "ext_adk_0001"}


def test_generate_conversion_map_multiple_extension_timestamps() -> None:
    """Test multiple timestamp migrations for same extension."""
    migrations = [
        ("ext_litestar_20251011120000", Path("ext_litestar_20251011120000_first.py")),
        ("ext_litestar_20251012130000", Path("ext_litestar_20251012130000_second.py")),
        ("ext_litestar_20251013140000", Path("ext_litestar_20251013140000_third.py")),
    ]

    result = generate_conversion_map(migrations)

    assert result == {
        "ext_litestar_20251011120000": "ext_litestar_0001",
        "ext_litestar_20251012130000": "ext_litestar_0002",
        "ext_litestar_20251013140000": "ext_litestar_0003",
    }


def test_generate_conversion_map_ignores_invalid_versions() -> None:
    """Test conversion map skips invalid migration versions."""
    migrations = [
        ("0001", Path("0001_init.sql")),
        ("invalid", Path("invalid_migration.sql")),
        ("20251011120000", Path("20251011120000_products.sql")),
        ("", Path("empty.sql")),
    ]

    result = generate_conversion_map(migrations)

    assert result == {"20251011120000": "0002"}


def test_generate_conversion_map_complex_scenario() -> None:
    """Test conversion map with complex real-world scenario."""
    migrations = [
        ("0001", Path("0001_init.sql")),
        ("0002", Path("0002_users.sql")),
        ("20251011120000", Path("20251011120000_products.sql")),
        ("ext_litestar_0001", Path("ext_litestar_0001_sessions.py")),
        ("20251012130000", Path("20251012130000_orders.sql")),
        ("ext_litestar_20251011215440", Path("ext_litestar_20251011215440_new_session.py")),
        ("ext_adk_20251011215914", Path("ext_adk_20251011215914_tables.py")),
        ("0003", Path("0003_categories.sql")),
    ]

    result = generate_conversion_map(migrations)

    assert result == {
        "20251011120000": "0004",
        "20251012130000": "0005",
        "ext_litestar_20251011215440": "ext_litestar_0002",
        "ext_adk_20251011215914": "ext_adk_0001",
    }


def test_generate_conversion_map_preserves_path_info() -> None:
    """Test that conversion map generation doesn't fail with Path objects."""
    migrations = [
        ("20251011120000", Path("/migrations/20251011120000_products.sql")),
        ("20251012130000", Path("/migrations/20251012130000_orders.sql")),
    ]

    result = generate_conversion_map(migrations)

    assert "20251011120000" in result
    assert "20251012130000" in result


def test_get_next_sequential_number_unordered() -> None:
    """Test next sequential number with unordered input."""
    v3 = parse_version("0003")
    v1 = parse_version("0001")
    v2 = parse_version("0002")

    result = get_next_sequential_number([v3, v1, v2])
    assert result == 4


def test_convert_to_sequential_version_large_numbers() -> None:
    """Test conversion with large sequence numbers."""
    v = parse_version("20251011120000")
    result = convert_to_sequential_version(v, 99999)
    assert result == "99999"
    assert len(result) == 5
