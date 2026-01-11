"""Tests for migration validation and out-of-order detection."""

from typing import Any

import pytest

from sqlspec.exceptions import OutOfOrderMigrationError
from sqlspec.migrations.validation import (
    MigrationGap,
    detect_out_of_order_migrations,
    format_out_of_order_warning,
    validate_migration_order,
)
from sqlspec.migrations.version import parse_version


def test_detect_out_of_order_no_applied() -> None:
    """Test detection with no applied migrations."""
    pending: list[str] = ["20251011120000", "20251012120000"]
    applied: list[str] = []

    gaps = detect_out_of_order_migrations(pending, applied)

    assert gaps == []


def test_detect_out_of_order_no_pending() -> None:
    """Test detection with no pending migrations."""
    pending: list[str] = []
    applied: list[str] = ["20251011120000", "20251012120000"]

    gaps = detect_out_of_order_migrations(pending, applied)

    assert gaps == []


def test_detect_out_of_order_no_gaps() -> None:
    """Test detection with no out-of-order migrations."""
    pending: list[str] = ["20251013120000", "20251014120000"]
    applied: list[str] = ["20251011120000", "20251012120000"]

    gaps = detect_out_of_order_migrations(pending, applied)

    assert gaps == []


def test_detect_out_of_order_single_gap() -> None:
    """Test detection with single out-of-order migration."""
    pending: list[str] = ["20251011130000", "20251013120000"]
    applied: list[str] = ["20251011120000", "20251012120000"]

    gaps = detect_out_of_order_migrations(pending, applied)

    assert len(gaps) == 1
    assert gaps[0].missing_version == parse_version("20251011130000")
    assert gaps[0].applied_after == [parse_version("20251012120000")]


def test_detect_out_of_order_multiple_gaps() -> None:
    """Test detection with multiple out-of-order migrations."""
    pending: list[str] = ["20251011130000", "20251011140000", "20251013120000"]
    applied: list[str] = ["20251011120000", "20251012120000"]

    gaps = detect_out_of_order_migrations(pending, applied)

    assert len(gaps) == 2
    assert gaps[0].missing_version == parse_version("20251011130000")
    assert gaps[1].missing_version == parse_version("20251011140000")


def test_detect_out_of_order_with_sequential() -> None:
    """Test detection works with mixed sequential and timestamp versions."""
    pending: list[str] = ["20251011120000"]
    applied: list[str] = ["0001", "0002", "20251012120000"]

    gaps = detect_out_of_order_migrations(pending, applied)

    assert len(gaps) == 1
    assert gaps[0].missing_version == parse_version("20251011120000")


def test_detect_out_of_order_extension_versions_excluded() -> None:
    """Test that extension migrations are excluded from out-of-order detection.

    Extension migrations maintain independent sequences within their namespaces
    and should not be flagged as out-of-order relative to other migrations.
    """
    pending: list[str] = ["ext_adk_0001", "ext_litestar_0001"]
    applied: list[str] = ["0001", "0002", "ext_litestar_0002"]

    gaps = detect_out_of_order_migrations(pending, applied)

    assert gaps == []


def test_detect_out_of_order_mixed_core_and_extension() -> None:
    """Test detection with mixed core and extension migrations.

    Only core migrations should be checked for out-of-order status.
    """
    pending: list[str] = ["20251011130000", "ext_litestar_0001"]
    applied: list[str] = ["20251012120000", "ext_adk_0001"]

    gaps = detect_out_of_order_migrations(pending, applied)

    assert len(gaps) == 1
    assert gaps[0].missing_version == parse_version("20251011130000")
    assert gaps[0].missing_version.extension is None


def test_format_out_of_order_warning_empty() -> None:
    """Test formatting with no gaps."""
    warning = format_out_of_order_warning([])

    assert warning == ""


def test_format_out_of_order_warning_single() -> None:
    """Test formatting with single gap."""
    gap = MigrationGap(missing_version=parse_version("20251011130000"), applied_after=[parse_version("20251012120000")])

    warning = format_out_of_order_warning([gap])

    assert "Out-of-order migrations detected" in warning
    assert "20251011130000" in warning
    assert "20251012120000" in warning
    assert "created before" in warning


def test_format_out_of_order_warning_multiple() -> None:
    """Test formatting with multiple gaps."""
    gaps = [
        MigrationGap(
            missing_version=parse_version("20251011130000"),
            applied_after=[parse_version("20251012120000"), parse_version("20251013120000")],
        ),
        MigrationGap(missing_version=parse_version("20251011140000"), applied_after=[parse_version("20251012120000")]),
    ]

    warning = format_out_of_order_warning(gaps)

    assert "20251011130000" in warning
    assert "20251011140000" in warning
    assert "20251012120000" in warning
    assert "20251013120000" in warning


def test_validate_migration_order_no_gaps() -> None:
    """Test validation with no out-of-order migrations."""
    pending: list[str] = ["20251013120000"]
    applied: list[str] = ["20251011120000", "20251012120000"]

    validate_migration_order(pending, applied, strict_ordering=False)
    validate_migration_order(pending, applied, strict_ordering=True)


def test_validate_migration_order_warns_by_default(capsys: "Any") -> None:
    """Test validation warns but allows out-of-order migrations by default."""
    pending: list[str] = ["20251011130000"]
    applied: list[str] = ["20251012120000"]

    validate_migration_order(pending, applied, strict_ordering=False)

    captured = capsys.readouterr()
    assert "Out-of-order migrations detected" in captured.out


def test_validate_migration_order_strict_raises() -> None:
    """Test validation raises error in strict mode."""
    pending: list[str] = ["20251011130000"]
    applied: list[str] = ["20251012120000"]

    with pytest.raises(OutOfOrderMigrationError) as exc_info:
        validate_migration_order(pending, applied, strict_ordering=True)

    assert "Out-of-order migrations detected" in str(exc_info.value)
    assert "20251011130000" in str(exc_info.value)
    assert "Strict ordering is enabled" in str(exc_info.value)


def test_migration_gap_frozen() -> None:
    """Test MigrationGap is frozen (immutable)."""
    gap = MigrationGap(missing_version=parse_version("20251011130000"), applied_after=[parse_version("20251012120000")])

    with pytest.raises(AttributeError):
        gap.missing_version = parse_version("20251011140000")  # type: ignore[misc]


def test_detect_out_of_order_complex_scenario() -> None:
    """Test detection with complex real-world scenario."""
    pending: list[str] = ["20251011100000", "20251011150000", "20251012100000", "20251015120000"]
    applied: list[str] = ["20251011120000", "20251011140000", "20251013120000"]

    gaps = detect_out_of_order_migrations(pending, applied)

    assert len(gaps) == 3
    assert gaps[0].missing_version == parse_version("20251011100000")
    assert gaps[1].missing_version == parse_version("20251011150000")
    assert gaps[2].missing_version == parse_version("20251012100000")

    pending_versions = {g.missing_version.raw for g in gaps}
    assert "20251011100000" in pending_versions
    assert "20251011150000" in pending_versions
    assert "20251012100000" in pending_versions

    assert parse_version("20251015120000") not in [g.missing_version for g in gaps]
