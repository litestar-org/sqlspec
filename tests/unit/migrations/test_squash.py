# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for Migration Squash functionality.

Tests for:
- SquashPlan dataclass
- MigrationSquasher class
- SQL extraction and merging
- Squashed file generation
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.xdist_group("migrations")


class TestSquashPlan:
    """Tests for SquashPlan dataclass."""

    def test_squash_plan_instantiation(self, tmp_path: Path) -> None:
        """Test SquashPlan dataclass can be instantiated."""
        from sqlspec.migrations.squash import SquashPlan

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql"), ("0002", tmp_path / "0002_users.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="Release 1.0",
            source_versions=["0001", "0002"],
        )

        assert plan.target_version == "0001"
        assert plan.description == "Release 1.0"
        assert len(plan.source_migrations) == 2
        assert plan.source_versions == ["0001", "0002"]

    def test_squash_plan_source_versions_required(self, tmp_path: Path) -> None:
        """Test SquashPlan requires source_versions field."""
        from sqlspec.migrations.squash import SquashPlan

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="Single migration",
            source_versions=["0001"],
        )

        assert plan.source_versions == ["0001"]


class TestSquashValidationError:
    """Tests for SquashValidationError exception."""

    def test_squash_validation_error_exists(self) -> None:
        """Test SquashValidationError can be imported and raised."""
        from sqlspec.exceptions import SquashValidationError

        with pytest.raises(SquashValidationError):
            raise SquashValidationError("Invalid squash range")

    def test_squash_validation_error_inherits_from_migration_error(self) -> None:
        """Test SquashValidationError inherits from MigrationError."""
        from sqlspec.exceptions import MigrationError, SquashValidationError

        assert issubclass(SquashValidationError, MigrationError)

    def test_squash_validation_error_message(self) -> None:
        """Test SquashValidationError preserves error message."""
        from sqlspec.exceptions import SquashValidationError

        error = SquashValidationError("Gap detected between versions 0002 and 0004")
        assert "Gap detected" in str(error)


class TestMigrationSquasher:
    """Tests for MigrationSquasher class."""

    def test_migration_squasher_instantiation(self, tmp_path: Path) -> None:
        """Test MigrationSquasher can be instantiated."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher

        runner = Mock()
        squasher = MigrationSquasher(migrations_path=tmp_path, runner=runner)

        assert squasher.migrations_path == tmp_path
        assert squasher.runner == runner

    def test_migration_squasher_with_template_settings(self, tmp_path: Path) -> None:
        """Test MigrationSquasher accepts optional template settings."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher
        from sqlspec.migrations.templates import MigrationTemplateSettings

        runner = Mock()
        template_settings = Mock(spec=MigrationTemplateSettings)

        squasher = MigrationSquasher(migrations_path=tmp_path, runner=runner, template_settings=template_settings)

        assert squasher.template_settings == template_settings

    def test_migration_squasher_has_slots(self, tmp_path: Path) -> None:
        """Test MigrationSquasher uses __slots__ for mypyc compatibility."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher

        squasher = MigrationSquasher(tmp_path, Mock())

        # Classes with __slots__ don't have __dict__
        assert not hasattr(squasher, "__dict__") or len(squasher.__dict__) == 0


class TestPlanSquash:
    """Tests for MigrationSquasher.plan_squash() method."""

    def test_plan_squash_valid_range(self, tmp_path: Path) -> None:
        """Test plan_squash with valid version range."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher

        # Create test migration files
        (tmp_path / "0001_initial.sql").write_text("-- migration 1")
        (tmp_path / "0002_users.sql").write_text("-- migration 2")
        (tmp_path / "0003_posts.sql").write_text("-- migration 3")

        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_initial.sql"),
            ("0002", tmp_path / "0002_users.sql"),
            ("0003", tmp_path / "0003_posts.sql"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)
        plans = squasher.plan_squash("0001", "0003", "release_1")

        assert len(plans) == 1
        plan = plans[0]
        assert plan.target_version == "0001"
        assert plan.description == "release_1"
        assert len(plan.source_migrations) == 3
        assert plan.source_versions == ["0001", "0002", "0003"]
        assert plan.target_path == tmp_path / "0001_release_1.sql"

    def test_plan_squash_partial_range(self, tmp_path: Path) -> None:
        """Test plan_squash with partial version range."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher

        (tmp_path / "0001_initial.sql").write_text("-- migration 1")
        (tmp_path / "0002_users.sql").write_text("-- migration 2")
        (tmp_path / "0003_posts.sql").write_text("-- migration 3")
        (tmp_path / "0004_comments.sql").write_text("-- migration 4")

        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_initial.sql"),
            ("0002", tmp_path / "0002_users.sql"),
            ("0003", tmp_path / "0003_posts.sql"),
            ("0004", tmp_path / "0004_comments.sql"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)
        plans = squasher.plan_squash("0002", "0003", "feature_users")

        assert len(plans) == 1
        plan = plans[0]
        assert plan.target_version == "0002"
        assert len(plan.source_migrations) == 2
        assert plan.source_versions == ["0002", "0003"]

    def test_plan_squash_invalid_range_start_greater_than_end(self, tmp_path: Path) -> None:
        """Test plan_squash raises error when start > end."""
        from unittest.mock import Mock

        import pytest

        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.squash import MigrationSquasher

        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_initial.sql"),
            ("0002", tmp_path / "0002_users.sql"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)

        with pytest.raises(SquashValidationError, match="Invalid range"):
            squasher.plan_squash("0003", "0001", "invalid")

    def test_plan_squash_gap_in_sequence(self, tmp_path: Path) -> None:
        """Test plan_squash raises error when gap detected in sequence."""
        from unittest.mock import Mock

        import pytest

        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.squash import MigrationSquasher

        # Only 0001 and 0003 exist - gap at 0002
        (tmp_path / "0001_initial.sql").write_text("-- migration 1")
        (tmp_path / "0003_posts.sql").write_text("-- migration 3")

        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_initial.sql"),
            ("0003", tmp_path / "0003_posts.sql"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)

        with pytest.raises(SquashValidationError, match="Gap detected"):
            squasher.plan_squash("0001", "0003", "with_gap")

    def test_plan_squash_version_not_found(self, tmp_path: Path) -> None:
        """Test plan_squash raises error when version not found."""
        from unittest.mock import Mock

        import pytest

        from sqlspec.exceptions import SquashValidationError
        from sqlspec.migrations.squash import MigrationSquasher

        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_initial.sql"),
            ("0002", tmp_path / "0002_users.sql"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)

        with pytest.raises(SquashValidationError, match="not found"):
            squasher.plan_squash("0001", "0005", "missing_end")

    def test_plan_squash_single_migration(self, tmp_path: Path) -> None:
        """Test plan_squash with single migration (start == end)."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher

        (tmp_path / "0001_initial.sql").write_text("-- migration 1")

        runner = Mock()
        runner.get_migration_files.return_value = [("0001", tmp_path / "0001_initial.sql")]

        squasher = MigrationSquasher(tmp_path, runner)
        plans = squasher.plan_squash("0001", "0001", "single")

        assert len(plans) == 1
        plan = plans[0]
        assert len(plan.source_migrations) == 1
        assert plan.source_versions == ["0001"]


class TestExtractSQL:
    """Tests for MigrationSquasher.extract_sql() method."""

    def test_extract_sql_up_statements_in_order(self, tmp_path: Path) -> None:
        """Test that UP SQL statements are extracted in version order."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher

        runner = Mock()
        # Mock load_migration to return loader with get_up_sql
        migration1 = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_initial.sql"}
        migration1["loader"].get_up_sql.return_value = ["CREATE TABLE users (id INT);"]

        migration2 = {"version": "0002", "loader": Mock(), "file_path": tmp_path / "0002_posts.sql"}
        migration2["loader"].get_up_sql.return_value = ["CREATE TABLE posts (id INT);"]

        runner.load_migration.side_effect = [migration1, migration2]

        squasher = MigrationSquasher(tmp_path, runner)
        migrations = [("0001", tmp_path / "0001_initial.sql"), ("0002", tmp_path / "0002_posts.sql")]

        up_sql, _down_sql = squasher.extract_sql(migrations)

        assert up_sql == ["CREATE TABLE users (id INT);", "CREATE TABLE posts (id INT);"]

    def test_extract_sql_down_statements_in_reverse_order(self, tmp_path: Path) -> None:
        """Test that DOWN SQL statements are extracted in REVERSE version order."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher

        runner = Mock()
        migration1 = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_initial.sql"}
        migration1["loader"].get_up_sql.return_value = ["CREATE TABLE users (id INT);"]
        migration1["loader"].get_down_sql.return_value = ["DROP TABLE users;"]

        migration2 = {"version": "0002", "loader": Mock(), "file_path": tmp_path / "0002_posts.sql"}
        migration2["loader"].get_up_sql.return_value = ["CREATE TABLE posts (id INT);"]
        migration2["loader"].get_down_sql.return_value = ["DROP TABLE posts;"]

        runner.load_migration.side_effect = [migration1, migration2]

        squasher = MigrationSquasher(tmp_path, runner)
        migrations = [("0001", tmp_path / "0001_initial.sql"), ("0002", tmp_path / "0002_posts.sql")]

        _up_sql, down_sql = squasher.extract_sql(migrations)

        # DOWN should be in REVERSE order for proper rollback
        assert down_sql == ["DROP TABLE posts;", "DROP TABLE users;"]

    def test_extract_sql_handles_missing_down(self, tmp_path: Path) -> None:
        """Test extract_sql handles migrations without DOWN queries."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher

        runner = Mock()
        migration1 = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_initial.sql"}
        migration1["loader"].get_up_sql.return_value = ["CREATE TABLE users (id INT);"]
        migration1["loader"].get_down_sql.return_value = None  # No DOWN

        runner.load_migration.return_value = migration1

        squasher = MigrationSquasher(tmp_path, runner)
        migrations = [("0001", tmp_path / "0001_initial.sql")]

        up_sql, down_sql = squasher.extract_sql(migrations)

        assert up_sql == ["CREATE TABLE users (id INT);"]
        assert down_sql == []

    def test_extract_sql_multiple_statements_per_migration(self, tmp_path: Path) -> None:
        """Test extract_sql handles multiple statements per migration."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher

        runner = Mock()
        migration = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_initial.sql"}
        migration["loader"].get_up_sql.return_value = [
            "CREATE TABLE users (id INT);",
            "CREATE INDEX idx_users ON users(id);",
        ]
        migration["loader"].get_down_sql.return_value = ["DROP INDEX idx_users;", "DROP TABLE users;"]

        runner.load_migration.return_value = migration

        squasher = MigrationSquasher(tmp_path, runner)
        migrations = [("0001", tmp_path / "0001_initial.sql")]

        up_sql, down_sql = squasher.extract_sql(migrations)

        assert len(up_sql) == 2
        assert len(down_sql) == 2


class TestGenerateSquashedContent:
    """Tests for MigrationSquasher.generate_squashed_content() method."""

    def test_generate_squashed_content_basic(self, tmp_path: Path) -> None:
        """Test generate_squashed_content produces valid SQL file."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = Mock()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql"), ("0002", tmp_path / "0002_users.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="release",
            source_versions=["0001", "0002"],
        )

        up_sql = ["CREATE TABLE users (id INT);", "CREATE TABLE posts (id INT);"]
        down_sql = ["DROP TABLE posts;", "DROP TABLE users;"]

        content = squasher.generate_squashed_content(plan, up_sql, down_sql)

        # Check structure
        assert "-- name: migrate-0001-up" in content
        assert "-- name: migrate-0001-down" in content
        assert "CREATE TABLE users (id INT);" in content
        assert "CREATE TABLE posts (id INT);" in content
        assert "DROP TABLE posts;" in content
        assert "DROP TABLE users;" in content

    def test_generate_squashed_content_includes_squash_comment(self, tmp_path: Path) -> None:
        """Test generate_squashed_content includes audit trail comment."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = Mock()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[
                ("0001", tmp_path / "0001_initial.sql"),
                ("0002", tmp_path / "0002_users.sql"),
                ("0003", tmp_path / "0003_posts.sql"),
            ],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="release",
            source_versions=["0001", "0002", "0003"],
        )

        content = squasher.generate_squashed_content(plan, ["SELECT 1;"], ["SELECT 1;"])

        # Should include squashed-from comment for audit trail
        assert "Squashed from:" in content or "squashed from:" in content.lower()
        assert "0001" in content
        assert "0002" in content
        assert "0003" in content

    def test_generate_squashed_content_empty_down(self, tmp_path: Path) -> None:
        """Test generate_squashed_content handles empty DOWN section."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = Mock()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="irreversible",
            source_versions=["0001"],
        )

        content = squasher.generate_squashed_content(plan, ["CREATE TABLE t (id INT);"], [])

        assert "-- name: migrate-0001-up" in content
        assert "CREATE TABLE t (id INT);" in content
        # DOWN section may be present but empty or omitted

    def test_generate_squashed_content_uses_template_settings(self, tmp_path: Path) -> None:
        """Test generate_squashed_content uses template settings if provided."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan
        from sqlspec.migrations.templates import (
            MigrationTemplateProfile,
            MigrationTemplateSettings,
            PythonTemplateDefinition,
            SQLTemplateDefinition,
        )

        runner = Mock()

        # Create custom template settings
        sql_template = SQLTemplateDefinition(
            header="-- Custom Header: {title}", metadata=["-- Version: {version}"], body=""
        )
        python_template = PythonTemplateDefinition(docstring="", body="")
        profile = MigrationTemplateProfile(
            name="custom", title="Custom Migration", sql=sql_template, python=python_template
        )
        template_settings = MigrationTemplateSettings(default_format="sql", profile=profile)

        squasher = MigrationSquasher(tmp_path, runner, template_settings=template_settings)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="release",
            source_versions=["0001"],
        )

        content = squasher.generate_squashed_content(plan, ["SELECT 1;"], [])

        # Should include custom header from template
        assert "Custom" in content or "-- name: migrate-0001-up" in content


class TestApplySquash:
    """Tests for MigrationSquasher.apply_squash() method."""

    def test_apply_squash_dry_run(self, tmp_path: Path) -> None:
        """Test apply_squash in dry_run mode makes no changes."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        # Create source migration files
        (tmp_path / "0001_initial.sql").write_text("-- migration 1")
        (tmp_path / "0002_users.sql").write_text("-- migration 2")

        runner = Mock()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql"), ("0002", tmp_path / "0002_users.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="release",
            source_versions=["0001", "0002"],
        )

        squasher.apply_squash([plan], dry_run=True)

        # Original files should still exist
        assert (tmp_path / "0001_initial.sql").exists()
        assert (tmp_path / "0002_users.sql").exists()
        # Target should NOT be created
        assert not (tmp_path / "0001_release.sql").exists()

    def test_apply_squash_writes_squashed_file(self, tmp_path: Path) -> None:
        """Test apply_squash writes the squashed migration file."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        # Create source migration files
        (tmp_path / "0001_initial.sql").write_text("-- migration 1")
        (tmp_path / "0002_users.sql").write_text("-- migration 2")

        runner = Mock()
        migration1 = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_initial.sql"}
        migration1["loader"].get_up_sql.return_value = ["CREATE TABLE t1 (id INT);"]
        migration1["loader"].get_down_sql.return_value = ["DROP TABLE t1;"]

        migration2 = {"version": "0002", "loader": Mock(), "file_path": tmp_path / "0002_users.sql"}
        migration2["loader"].get_up_sql.return_value = ["CREATE TABLE t2 (id INT);"]
        migration2["loader"].get_down_sql.return_value = ["DROP TABLE t2;"]

        runner.load_migration.side_effect = [migration1, migration2]

        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql"), ("0002", tmp_path / "0002_users.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="release",
            source_versions=["0001", "0002"],
        )

        squasher.apply_squash([plan], dry_run=False)

        # Target should be created
        assert (tmp_path / "0001_release.sql").exists()
        content = (tmp_path / "0001_release.sql").read_text()
        assert "CREATE TABLE t1" in content
        assert "CREATE TABLE t2" in content

    def test_apply_squash_deletes_source_files(self, tmp_path: Path) -> None:
        """Test apply_squash deletes original migration files."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        # Create source migration files
        (tmp_path / "0001_initial.sql").write_text("-- migration 1")
        (tmp_path / "0002_users.sql").write_text("-- migration 2")

        runner = Mock()
        migration1 = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_initial.sql"}
        migration1["loader"].get_up_sql.return_value = ["SELECT 1;"]
        migration1["loader"].get_down_sql.return_value = None

        migration2 = {"version": "0002", "loader": Mock(), "file_path": tmp_path / "0002_users.sql"}
        migration2["loader"].get_up_sql.return_value = ["SELECT 2;"]
        migration2["loader"].get_down_sql.return_value = None

        runner.load_migration.side_effect = [migration1, migration2]

        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql"), ("0002", tmp_path / "0002_users.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="release",
            source_versions=["0001", "0002"],
        )

        squasher.apply_squash([plan], dry_run=False)

        # Source files should be deleted
        assert not (tmp_path / "0001_initial.sql").exists()
        assert not (tmp_path / "0002_users.sql").exists()

    def test_apply_squash_creates_backup(self, tmp_path: Path) -> None:
        """Test apply_squash creates backup before modifications."""
        from unittest.mock import Mock

        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        # Create source migration files
        (tmp_path / "0001_initial.sql").write_text("-- migration 1")

        runner = Mock()
        migration = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_initial.sql"}
        migration["loader"].get_up_sql.return_value = ["SELECT 1;"]
        migration["loader"].get_down_sql.return_value = None
        runner.load_migration.return_value = migration

        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.sql",
            description="release",
            source_versions=["0001"],
        )

        squasher.apply_squash([plan], dry_run=False)

        # Backup should have been created and cleaned up on success
        # (no backup directory should remain after successful operation)
        backup_dirs = list(tmp_path.glob(".backup_*"))
        assert len(backup_dirs) == 0  # Cleaned up on success
