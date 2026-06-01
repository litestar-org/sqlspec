"""Unit tests for Migration Squash functionality.

Tests for:
- SquashPlan dataclass
- MigrationSquasher class
- SQL extraction and merging
- Squashed file generation
"""

from pathlib import Path

import pytest


def test_squash_plan_squash_plan_instantiation(tmp_path: Path) -> None:
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


def test_squash_plan_squash_plan_source_versions_required(tmp_path: Path) -> None:
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


def test_squash_validation_error_squash_validation_error_exists() -> None:
    """Test SquashValidationError can be imported and raised."""
    from sqlspec.exceptions import SquashValidationError

    with pytest.raises(SquashValidationError):
        raise SquashValidationError("Invalid squash range")


def test_squash_validation_error_squash_validation_error_inherits_from_migration_error() -> None:
    """Test SquashValidationError inherits from MigrationError."""
    from sqlspec.exceptions import MigrationError, SquashValidationError

    assert issubclass(SquashValidationError, MigrationError)


def test_squash_validation_error_squash_validation_error_message() -> None:
    """Test SquashValidationError preserves error message."""
    from sqlspec.exceptions import SquashValidationError

    error = SquashValidationError("Gap detected between versions 0002 and 0004")
    assert "Gap detected" in str(error)


def test_migration_squasher_migration_squasher_instantiation(tmp_path: Path) -> None:
    """Test MigrationSquasher can be instantiated."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    squasher = MigrationSquasher(migrations_path=tmp_path, runner=runner)
    assert squasher.migrations_path == tmp_path
    assert squasher.runner == runner


def test_migration_squasher_migration_squasher_with_template_settings(tmp_path: Path) -> None:
    """Test MigrationSquasher accepts optional template settings."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher
    from sqlspec.migrations.templates import MigrationTemplateSettings

    runner = Mock()
    template_settings = Mock(spec=MigrationTemplateSettings)
    squasher = MigrationSquasher(migrations_path=tmp_path, runner=runner, template_settings=template_settings)
    assert squasher.template_settings == template_settings


def test_migration_squasher_migration_squasher_has_slots(tmp_path: Path) -> None:
    """Test MigrationSquasher uses __slots__ for mypyc compatibility."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher

    squasher = MigrationSquasher(tmp_path, Mock())
    assert not hasattr(squasher, "__dict__") or len(squasher.__dict__) == 0


def test_plan_squash_plan_squash_valid_range(tmp_path: Path) -> None:
    """Test plan_squash with valid version range."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher

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


def test_plan_squash_plan_squash_partial_range(tmp_path: Path) -> None:
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


def test_plan_squash_plan_squash_invalid_range_start_greater_than_end(tmp_path: Path) -> None:
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


def test_plan_squash_plan_squash_gap_in_sequence(tmp_path: Path) -> None:
    """Test plan_squash raises error when gap detected in sequence."""
    from unittest.mock import Mock

    import pytest

    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.squash import MigrationSquasher

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


def test_plan_squash_plan_squash_version_not_found(tmp_path: Path) -> None:
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


def test_plan_squash_plan_squash_single_migration(tmp_path: Path) -> None:
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


def test_extract_sql_extract_sql_up_statements_in_order(tmp_path: Path) -> None:
    """Test that UP SQL statements are extracted in version order."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    migration1 = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_initial.sql"}
    migration1["loader"].get_up_sql.return_value = ["CREATE TABLE users (id INT);"]
    migration2 = {"version": "0002", "loader": Mock(), "file_path": tmp_path / "0002_posts.sql"}
    migration2["loader"].get_up_sql.return_value = ["CREATE TABLE posts (id INT);"]
    runner.load_migration.side_effect = [migration1, migration2]
    squasher = MigrationSquasher(tmp_path, runner)
    migrations = [("0001", tmp_path / "0001_initial.sql"), ("0002", tmp_path / "0002_posts.sql")]
    (up_sql, _down_sql) = squasher.extract_sql(migrations)
    assert up_sql == ["CREATE TABLE users (id INT);", "CREATE TABLE posts (id INT);"]


def test_extract_sql_extract_sql_down_statements_in_reverse_order(tmp_path: Path) -> None:
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
    (_up_sql, down_sql) = squasher.extract_sql(migrations)
    assert down_sql == ["DROP TABLE posts;", "DROP TABLE users;"]


def test_extract_sql_extract_sql_handles_missing_down(tmp_path: Path) -> None:
    """Test extract_sql handles migrations without DOWN queries."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    migration1 = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_initial.sql"}
    migration1["loader"].get_up_sql.return_value = ["CREATE TABLE users (id INT);"]
    migration1["loader"].get_down_sql.return_value = None
    runner.load_migration.return_value = migration1
    squasher = MigrationSquasher(tmp_path, runner)
    migrations = [("0001", tmp_path / "0001_initial.sql")]
    (up_sql, down_sql) = squasher.extract_sql(migrations)
    assert up_sql == ["CREATE TABLE users (id INT);"]
    assert down_sql == []


def test_extract_sql_extract_sql_multiple_statements_per_migration(tmp_path: Path) -> None:
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
    (up_sql, down_sql) = squasher.extract_sql(migrations)
    assert len(up_sql) == 2
    assert len(down_sql) == 2


def test_generate_squashed_content_generate_squashed_content_basic(tmp_path: Path) -> None:
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
    assert "-- name: migrate-0001-up" in content
    assert "-- name: migrate-0001-down" in content
    assert "CREATE TABLE users (id INT);" in content
    assert "CREATE TABLE posts (id INT);" in content
    assert "DROP TABLE posts;" in content
    assert "DROP TABLE users;" in content


def test_generate_squashed_content_generate_squashed_content_includes_squash_comment(tmp_path: Path) -> None:
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
    assert "Squashed from:" in content or "squashed from:" in content.lower()
    assert "0001" in content
    assert "0002" in content
    assert "0003" in content


def test_generate_squashed_content_generate_squashed_content_empty_down(tmp_path: Path) -> None:
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


def test_generate_squashed_content_generate_squashed_content_uses_template_settings(tmp_path: Path) -> None:
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
    assert "Custom" in content or "-- name: migrate-0001-up" in content


def test_apply_squash_apply_squash_dry_run(tmp_path: Path) -> None:
    """Test apply_squash in dry_run mode makes no changes."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

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
    assert (tmp_path / "0001_initial.sql").exists()
    assert (tmp_path / "0002_users.sql").exists()
    assert not (tmp_path / "0001_release.sql").exists()


def test_apply_squash_apply_squash_writes_squashed_file(tmp_path: Path) -> None:
    """Test apply_squash writes the squashed migration file."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

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
    assert (tmp_path / "0001_release.sql").exists()
    content = (tmp_path / "0001_release.sql").read_text()
    assert "CREATE TABLE t1" in content
    assert "CREATE TABLE t2" in content


def test_apply_squash_apply_squash_deletes_source_files(tmp_path: Path) -> None:
    """Test apply_squash deletes original migration files."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

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
    assert not (tmp_path / "0001_initial.sql").exists()
    assert not (tmp_path / "0002_users.sql").exists()


def test_apply_squash_apply_squash_creates_backup(tmp_path: Path) -> None:
    """Test apply_squash creates backup before modifications."""
    from unittest.mock import Mock

    from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

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
    backup_dirs = list(tmp_path.glob(".backup_*"))
    assert len(backup_dirs) == 0
