# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for Migration squash validation functions.

Tests for:
- validate_squash_range()
- validate_extension_consistency()
- validate_squash_idempotency()
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.xdist_group("migrations")


class TestValidateSquashRange:
    """Tests for validate_squash_range() function."""

    def test_validate_squash_range_valid_range(self) -> None:
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

    def test_validate_squash_range_gap_detected(self) -> None:
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

    def test_validate_squash_range_allow_gaps_true(self) -> None:
        """Test validate_squash_range allows gaps when flag is set."""
        from sqlspec.migrations.validation import validate_squash_range

        migrations: list[tuple[str, Path]] = [
            ("0001", Path("0001_initial.sql")),
            ("0002", Path("0002_users.sql")),
            ("0004", Path("0004_comments.sql")),
        ]

        result = validate_squash_range(migrations, "0001", "0004", allow_gaps=True)

        assert len(result) == 3
        assert [v for v, _ in result] == ["0001", "0002", "0004"]

    def test_validate_squash_range_invalid_start(self) -> None:
        """Test validate_squash_range raises error when start version not found."""
        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.validation import validate_squash_range

        migrations: list[tuple[str, Path]] = [("0002", Path("0002_users.sql")), ("0003", Path("0003_posts.sql"))]

        with pytest.raises(SquashValidationError, match="Start version 0001 not found"):
            validate_squash_range(migrations, "0001", "0003")

    def test_validate_squash_range_invalid_end(self) -> None:
        """Test validate_squash_range raises error when end version not found."""
        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.validation import validate_squash_range

        migrations: list[tuple[str, Path]] = [("0001", Path("0001_initial.sql")), ("0002", Path("0002_users.sql"))]

        with pytest.raises(SquashValidationError, match="End version 0005 not found"):
            validate_squash_range(migrations, "0001", "0005")

    def test_validate_squash_range_reversed(self) -> None:
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

    def test_validate_squash_range_version_not_found(self) -> None:
        """Test validate_squash_range raises error when version not in list."""
        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.validation import validate_squash_range

        migrations: list[tuple[str, Path]] = [("0001", Path("0001_initial.sql")), ("0010", Path("0010_final.sql"))]

        with pytest.raises(SquashValidationError, match="Start version 0005 not found"):
            validate_squash_range(migrations, "0005", "0007")

    def test_validate_squash_range_single_migration(self) -> None:
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


class TestValidateExtensionConsistency:
    """Tests for validate_extension_consistency() function."""

    def test_validate_extension_all_core(self) -> None:
        """Test validate_extension_consistency passes for all core migrations."""
        from sqlspec.migrations.validation import validate_extension_consistency

        migrations: list[tuple[str, Path]] = [
            ("0001", Path("0001_initial.sql")),
            ("0002", Path("0002_users.sql")),
            ("0003", Path("0003_posts.sql")),
        ]

        validate_extension_consistency(migrations)

    def test_validate_extension_all_same_ext(self) -> None:
        """Test validate_extension_consistency passes for all same extension."""
        from sqlspec.migrations.validation import validate_extension_consistency

        migrations: list[tuple[str, Path]] = [
            ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
            ("ext_litestar_0002", Path("ext_litestar_0002_tables.sql")),
        ]

        validate_extension_consistency(migrations)

    def test_validate_extension_mixed_core_and_ext(self) -> None:
        """Test validate_extension_consistency raises error for mixed core and ext."""
        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.validation import validate_extension_consistency

        migrations: list[tuple[str, Path]] = [
            ("0001", Path("0001_initial.sql")),
            ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
        ]

        with pytest.raises(SquashValidationError, match=r"Cannot squash.*core.*extension"):
            validate_extension_consistency(migrations)

    def test_validate_extension_mixed_different_exts(self) -> None:
        """Test validate_extension_consistency raises error for different extensions."""
        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.validation import validate_extension_consistency

        migrations: list[tuple[str, Path]] = [
            ("ext_litestar_0001", Path("ext_litestar_0001_init.sql")),
            ("ext_adk_0001", Path("ext_adk_0001_init.sql")),
        ]

        with pytest.raises(SquashValidationError, match=r"Cannot squash.*different extensions"):
            validate_extension_consistency(migrations)


class TestValidateSquashIdempotency:
    """Tests for validate_squash_idempotency() function."""

    def test_idempotency_ready(self, tmp_path: Path) -> None:
        """Test validate_squash_idempotency returns 'ready' when target doesn't exist."""
        from sqlspec.migrations.validation import validate_squash_idempotency

        (tmp_path / "0001_initial.sql").write_text("-- migration")
        (tmp_path / "0002_users.sql").write_text("-- migration")

        source_files = [tmp_path / "0001_initial.sql", tmp_path / "0002_users.sql"]
        target_file = tmp_path / "0001_squashed.sql"

        status = validate_squash_idempotency(source_files, target_file)

        assert status == "ready"

    def test_idempotency_already_squashed(self, tmp_path: Path) -> None:
        """Test validate_squash_idempotency returns 'already_squashed' when target exists and sources gone."""
        from sqlspec.migrations.validation import validate_squash_idempotency

        (tmp_path / "0001_squashed.sql").write_text("-- squashed migration")

        source_files = [tmp_path / "0001_initial.sql", tmp_path / "0002_users.sql"]
        target_file = tmp_path / "0001_squashed.sql"

        status = validate_squash_idempotency(source_files, target_file)

        assert status == "already_squashed"

    def test_idempotency_partial(self, tmp_path: Path) -> None:
        """Test validate_squash_idempotency returns 'partial' when target and some sources exist."""
        from sqlspec.migrations.validation import validate_squash_idempotency

        (tmp_path / "0001_initial.sql").write_text("-- migration")
        (tmp_path / "0001_squashed.sql").write_text("-- squashed migration")

        source_files = [tmp_path / "0001_initial.sql", tmp_path / "0002_users.sql"]
        target_file = tmp_path / "0001_squashed.sql"

        status = validate_squash_idempotency(source_files, target_file)

        assert status == "partial"
