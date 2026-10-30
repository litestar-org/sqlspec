"""Unit tests for Migration squash validation functions.

Tests for:
- validate_squash_range()
- validate_extension_consistency()
- validate_squash_idempotency()
"""

from pathlib import Path

import pytest

from sqlspec.exceptions import SquashValidationError
from sqlspec.migrations.squash import parse_version_range
from sqlspec.migrations.validation import (
    validate_extension_consistency,
    validate_squash_idempotency,
    validate_squash_range,
)


def test_validate_squash_range_validate_squash_range_valid_range() -> None:
    """Test validate_squash_range returns sorted migrations for valid range."""
    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("0002", Path("0002_users.sql")),
        ("0003", Path("0003_posts.sql")),
        ("0004", Path("0004_comments.sql")),
    ]
    result = validate_squash_range(migrations, "0001", "0003")
    assert len(result) == 3
    assert result[0][0] == "0001"
    assert result[1][0] == "0002"
    assert result[2][0] == "0003"


def test_validate_squash_range_retypes_nonnumeric_bounds() -> None:
    """Nonnumeric range bounds should use the migration exception contract."""
    with pytest.raises(SquashValidationError, match="must be numeric"):
        validate_squash_range([("0001", Path("0001.sql"))], "bad", "0001")


def test_validate_squash_range_validate_squash_range_gap_detected() -> None:
    """Test validate_squash_range raises error when gap detected."""
    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("0002", Path("0002_users.sql")),
        ("0004", Path("0004_comments.sql")),
    ]
    with pytest.raises(SquashValidationError, match=r"Gap detected.*0002.*0004"):
        validate_squash_range(migrations, "0001", "0004")


def test_validate_squash_range_validate_squash_range_allow_gaps_true() -> None:
    """Test validate_squash_range allows gaps when flag is set."""
    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("0002", Path("0002_users.sql")),
        ("0004", Path("0004_comments.sql")),
    ]
    result = validate_squash_range(migrations, "0001", "0004", allow_gaps=True)
    assert len(result) == 3
    assert [v for (v, _) in result] == ["0001", "0002", "0004"]


def test_validate_squash_range_validate_squash_range_invalid_start() -> None:
    """Test validate_squash_range raises error when start version not found."""
    migrations: list[tuple[str, Path]] = [("0002", Path("0002_users.sql")), ("0003", Path("0003_posts.sql"))]
    with pytest.raises(SquashValidationError, match="Start version 0001 not found"):
        validate_squash_range(migrations, "0001", "0003")


def test_validate_squash_range_validate_squash_range_invalid_end() -> None:
    """Test validate_squash_range raises error when end version not found."""
    migrations: list[tuple[str, Path]] = [("0001", Path("0001_initial.sql")), ("0002", Path("0002_users.sql"))]
    with pytest.raises(SquashValidationError, match="End version 0005 not found"):
        validate_squash_range(migrations, "0001", "0005")


def test_validate_squash_range_validate_squash_range_reversed() -> None:
    """Test validate_squash_range raises error when start > end."""
    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("0002", Path("0002_users.sql")),
        ("0003", Path("0003_posts.sql")),
    ]
    with pytest.raises(SquashValidationError, match="Invalid range"):
        validate_squash_range(migrations, "0003", "0001")


def test_validate_squash_range_validate_squash_range_version_not_found() -> None:
    """Test validate_squash_range raises error when version not in list."""
    migrations: list[tuple[str, Path]] = [("0001", Path("0001_initial.sql")), ("0010", Path("0010_final.sql"))]
    with pytest.raises(SquashValidationError, match="Start version 0005 not found"):
        validate_squash_range(migrations, "0005", "0007")


def test_validate_squash_range_validate_squash_range_single_migration() -> None:
    """Test validate_squash_range works with single migration in range."""
    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("0002", Path("0002_users.sql")),
        ("0003", Path("0003_posts.sql")),
    ]
    result = validate_squash_range(migrations, "0002", "0002")
    assert len(result) == 1
    assert result[0][0] == "0002"


def test_validate_extension_consistency_validate_extension_all_core() -> None:
    """Test validate_extension_consistency passes for all core migrations."""
    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("0002", Path("0002_users.sql")),
        ("0003", Path("0003_posts.sql")),
    ]
    validate_extension_consistency(migrations)


def test_validate_extension_consistency_validate_extension_all_same_ext() -> None:
    """Test validate_extension_consistency passes for all same extension."""
    migrations: list[tuple[str, Path]] = [
        ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
        ("ext_litestar_0002", Path("ext_litestar_0002_tables.sql")),
    ]
    validate_extension_consistency(migrations)


def test_validate_extension_consistency_validate_extension_mixed_core_and_ext() -> None:
    """Test validate_extension_consistency raises error for mixed core and ext."""
    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
    ]
    with pytest.raises(SquashValidationError, match=r"Cannot squash.*core.*extension"):
        validate_extension_consistency(migrations)


def test_validate_extension_consistency_validate_extension_mixed_different_exts() -> None:
    """Test validate_extension_consistency raises error for different extensions."""
    migrations: list[tuple[str, Path]] = [
        ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
        ("ext_adk_0001", Path("ext_adk_0001_init.sql")),
    ]
    with pytest.raises(SquashValidationError, match=r"Cannot squash.*different extensions"):
        validate_extension_consistency(migrations)


def test_validate_squash_idempotency_idempotency_ready(tmp_path: Path) -> None:
    """Test validate_squash_idempotency returns 'ready' when target doesn't exist."""
    (tmp_path / "0001_initial.sql").write_text("-- migration")
    (tmp_path / "0002_users.sql").write_text("-- migration")
    source_files = [tmp_path / "0001_initial.sql", tmp_path / "0002_users.sql"]
    target_file = tmp_path / "0001_squashed.sql"
    status = validate_squash_idempotency(source_files, target_file)
    assert status == "ready"


def test_validate_squash_idempotency_idempotency_already_squashed(tmp_path: Path) -> None:
    """Test validate_squash_idempotency returns 'already_squashed' when target exists and sources gone."""
    (tmp_path / "0001_squashed.sql").write_text("-- squashed migration")
    source_files = [tmp_path / "0001_initial.sql", tmp_path / "0002_users.sql"]
    target_file = tmp_path / "0001_squashed.sql"
    status = validate_squash_idempotency(source_files, target_file)
    assert status == "already_squashed"


def test_validate_squash_idempotency_idempotency_partial(tmp_path: Path) -> None:
    """Test validate_squash_idempotency returns 'partial' when target and some sources exist."""
    (tmp_path / "0001_initial.sql").write_text("-- migration")
    (tmp_path / "0001_squashed.sql").write_text("-- squashed migration")
    source_files = [tmp_path / "0001_initial.sql", tmp_path / "0002_users.sql"]
    target_file = tmp_path / "0001_squashed.sql"
    status = validate_squash_idempotency(source_files, target_file)
    assert status == "partial"


def test_parse_version_range_colon_separator() -> None:
    """Test START:END format."""
    assert parse_version_range("1:7") == ("0001", "0007")


def test_parse_version_range_double_dot_separator() -> None:
    """Test START..END format."""
    assert parse_version_range("1..7") == ("0001", "0007")


def test_parse_version_range_hyphen_separator() -> None:
    """Test START-END format."""
    assert parse_version_range("1-7") == ("0001", "0007")


def test_parse_version_range_zero_padded_input() -> None:
    """Test that already-padded input is preserved."""
    assert parse_version_range("0001:0007") == ("0001", "0007")


def test_parse_version_range_whitespace_stripped() -> None:
    """Test that whitespace around versions is stripped."""
    assert parse_version_range(" 1 : 7 ") == ("0001", "0007")


def test_parse_version_range_invalid_format_raises() -> None:
    """Test that unsupported format raises ValueError."""
    with pytest.raises(ValueError, match="Invalid VERSION_RANGE"):
        parse_version_range("1to7")


def test_parse_version_range_double_dot_preferred_over_hyphen() -> None:
    """Test that '..' separator is tried before '-' to avoid ambiguity."""
    assert parse_version_range("1..7") == ("0001", "0007")


def test_parse_version_range_preserves_timestamps_and_mixed_endpoints() -> None:
    """Timestamp endpoints must not be truncated or mis-padded across ':', '..', or '-'."""
    assert parse_version_range("20251011120000:20251012153000") == ("20251011120000", "20251012153000")
    assert parse_version_range("20251011120000..20251012153000") == ("20251011120000", "20251012153000")
    assert parse_version_range("20251011120000-20251012153000") == ("20251011120000", "20251012153000")
    assert parse_version_range("1..20251012153000") == ("0001", "20251012153000")


def test_validate_squash_range_supports_timestamp_and_mixed_ranges() -> None:
    """validate_squash_range should accept timestamped and mixed sequential+timestamp ranges."""
    ts_migrations: list[tuple[str, Path]] = [
        ("20251011120000", Path("20251011120000_init.sql")),
        ("20251012153000", Path("20251012153000_users.sql")),
        ("20251013090000", Path("20251013090000_posts.sql")),
    ]
    ts_result = validate_squash_range(ts_migrations, "20251011120000", "20251013090000")
    assert [v for v, _ in ts_result] == ["20251011120000", "20251012153000", "20251013090000"]

    mixed_migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_init.sql")),
        ("0002", Path("0002_users.sql")),
        ("20251011120000", Path("20251011120000_posts.sql")),
    ]
    mixed_result = validate_squash_range(mixed_migrations, "0001", "20251011120000")
    assert [v for v, _ in mixed_result] == ["0001", "0002", "20251011120000"]

    gapped_mixed: list[tuple[str, Path]] = [
        ("0001", Path("0001_init.sql")),
        ("0003", Path("0003_users.sql")),
        ("20251011120000", Path("20251011120000_posts.sql")),
    ]
    with pytest.raises(SquashValidationError, match="Gap detected"):
        validate_squash_range(gapped_mixed, "0001", "20251011120000")


@pytest.mark.parametrize("range_str", ["1-", ":7", "1..", " - 7", "1: "])
def test_parse_version_range_rejects_missing_endpoint(range_str: str) -> None:
    """A range with an empty START or END fails instead of normalizing to version 0000."""
    with pytest.raises(ValueError, match="both START and END are required"):
        parse_version_range(range_str)
