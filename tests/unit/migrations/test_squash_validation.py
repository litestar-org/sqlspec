"""Unit tests for Migration squash validation functions.

Tests for:
- validate_squash_range()
- validate_extension_consistency()
- validate_squash_idempotency()
"""

from pathlib import Path

import pytest


def test_validate_squash_range_validate_squash_range_valid_range() -> None:
    """Test validate_squash_range returns sorted migrations for valid range."""
    from sqlspec.migrations.validation import validate_squash_range

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


def test_validate_squash_range_validate_squash_range_gap_detected() -> None:
    """Test validate_squash_range raises error when gap detected."""
    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.validation import validate_squash_range

    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("0002", Path("0002_users.sql")),
        ("0004", Path("0004_comments.sql")),
    ]
    with pytest.raises(SquashValidationError, match=r"Gap detected.*0002.*0004"):
        validate_squash_range(migrations, "0001", "0004")


def test_validate_squash_range_validate_squash_range_allow_gaps_true() -> None:
    """Test validate_squash_range allows gaps when flag is set."""
    from sqlspec.migrations.validation import validate_squash_range

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
    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.validation import validate_squash_range

    migrations: list[tuple[str, Path]] = [("0002", Path("0002_users.sql")), ("0003", Path("0003_posts.sql"))]
    with pytest.raises(SquashValidationError, match="Start version 0001 not found"):
        validate_squash_range(migrations, "0001", "0003")


def test_validate_squash_range_validate_squash_range_invalid_end() -> None:
    """Test validate_squash_range raises error when end version not found."""
    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.validation import validate_squash_range

    migrations: list[tuple[str, Path]] = [("0001", Path("0001_initial.sql")), ("0002", Path("0002_users.sql"))]
    with pytest.raises(SquashValidationError, match="End version 0005 not found"):
        validate_squash_range(migrations, "0001", "0005")


def test_validate_squash_range_validate_squash_range_reversed() -> None:
    """Test validate_squash_range raises error when start > end."""
    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.validation import validate_squash_range

    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("0002", Path("0002_users.sql")),
        ("0003", Path("0003_posts.sql")),
    ]
    with pytest.raises(SquashValidationError, match="Invalid range"):
        validate_squash_range(migrations, "0003", "0001")


def test_validate_squash_range_validate_squash_range_version_not_found() -> None:
    """Test validate_squash_range raises error when version not in list."""
    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.validation import validate_squash_range

    migrations: list[tuple[str, Path]] = [("0001", Path("0001_initial.sql")), ("0010", Path("0010_final.sql"))]
    with pytest.raises(SquashValidationError, match="Start version 0005 not found"):
        validate_squash_range(migrations, "0005", "0007")


def test_validate_squash_range_validate_squash_range_single_migration() -> None:
    """Test validate_squash_range works with single migration in range."""
    from sqlspec.migrations.validation import validate_squash_range

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
    from sqlspec.migrations.validation import validate_extension_consistency

    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("0002", Path("0002_users.sql")),
        ("0003", Path("0003_posts.sql")),
    ]
    validate_extension_consistency(migrations)


def test_validate_extension_consistency_validate_extension_all_same_ext() -> None:
    """Test validate_extension_consistency passes for all same extension."""
    from sqlspec.migrations.validation import validate_extension_consistency

    migrations: list[tuple[str, Path]] = [
        ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
        ("ext_litestar_0002", Path("ext_litestar_0002_tables.sql")),
    ]
    validate_extension_consistency(migrations)


def test_validate_extension_consistency_validate_extension_mixed_core_and_ext() -> None:
    """Test validate_extension_consistency raises error for mixed core and ext."""
    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.validation import validate_extension_consistency

    migrations: list[tuple[str, Path]] = [
        ("0001", Path("0001_initial.sql")),
        ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
    ]
    with pytest.raises(SquashValidationError, match=r"Cannot squash.*core.*extension"):
        validate_extension_consistency(migrations)


def test_validate_extension_consistency_validate_extension_mixed_different_exts() -> None:
    """Test validate_extension_consistency raises error for different extensions."""
    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.validation import validate_extension_consistency

    migrations: list[tuple[str, Path]] = [
        ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
        ("ext_adk_0001", Path("ext_adk_0001_init.sql")),
    ]
    with pytest.raises(SquashValidationError, match=r"Cannot squash.*different extensions"):
        validate_extension_consistency(migrations)


def test_validate_squash_idempotency_idempotency_ready(tmp_path: Path) -> None:
    """Test validate_squash_idempotency returns 'ready' when target doesn't exist."""
    from sqlspec.migrations.validation import validate_squash_idempotency

    (tmp_path / "0001_initial.sql").write_text("-- migration")
    (tmp_path / "0002_users.sql").write_text("-- migration")
    source_files = [tmp_path / "0001_initial.sql", tmp_path / "0002_users.sql"]
    target_file = tmp_path / "0001_squashed.sql"
    status = validate_squash_idempotency(source_files, target_file)
    assert status == "ready"


def test_validate_squash_idempotency_idempotency_already_squashed(tmp_path: Path) -> None:
    """Test validate_squash_idempotency returns 'already_squashed' when target exists and sources gone."""
    from sqlspec.migrations.validation import validate_squash_idempotency

    (tmp_path / "0001_squashed.sql").write_text("-- squashed migration")
    source_files = [tmp_path / "0001_initial.sql", tmp_path / "0002_users.sql"]
    target_file = tmp_path / "0001_squashed.sql"
    status = validate_squash_idempotency(source_files, target_file)
    assert status == "already_squashed"


def test_validate_squash_idempotency_idempotency_partial(tmp_path: Path) -> None:
    """Test validate_squash_idempotency returns 'partial' when target and some sources exist."""
    from sqlspec.migrations.validation import validate_squash_idempotency

    (tmp_path / "0001_initial.sql").write_text("-- migration")
    (tmp_path / "0001_squashed.sql").write_text("-- squashed migration")
    source_files = [tmp_path / "0001_initial.sql", tmp_path / "0002_users.sql"]
    target_file = tmp_path / "0001_squashed.sql"
    status = validate_squash_idempotency(source_files, target_file)
    assert status == "partial"


def test_parse_version_range_colon_separator() -> None:
    """Test START:END format."""
    from sqlspec.migrations.squash import parse_version_range

    assert parse_version_range("1:7") == ("0001", "0007")


def test_parse_version_range_double_dot_separator() -> None:
    """Test START..END format."""
    from sqlspec.migrations.squash import parse_version_range

    assert parse_version_range("1..7") == ("0001", "0007")


def test_parse_version_range_hyphen_separator() -> None:
    """Test START-END format."""
    from sqlspec.migrations.squash import parse_version_range

    assert parse_version_range("1-7") == ("0001", "0007")


def test_parse_version_range_zero_padded_input() -> None:
    """Test that already-padded input is preserved."""
    from sqlspec.migrations.squash import parse_version_range

    assert parse_version_range("0001:0007") == ("0001", "0007")


def test_parse_version_range_whitespace_stripped() -> None:
    """Test that whitespace around versions is stripped."""
    from sqlspec.migrations.squash import parse_version_range

    assert parse_version_range(" 1 : 7 ") == ("0001", "0007")


def test_parse_version_range_invalid_format_raises() -> None:
    """Test that unsupported format raises ValueError."""
    from sqlspec.migrations.squash import parse_version_range

    with pytest.raises(ValueError, match="Invalid VERSION_RANGE"):
        parse_version_range("1to7")


def test_parse_version_range_double_dot_preferred_over_hyphen() -> None:
    """Test that '..' separator is tried before '-' to avoid ambiguity."""
    from sqlspec.migrations.squash import parse_version_range

    assert parse_version_range("1..7") == ("0001", "0007")
