"""Unit tests for migration squash edge cases.

Tests for:
- Unicode content handling
- Large SQL migrations
- Empty migrations
- Missing DOWN sections
- Special characters in descriptions
"""

from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest


def test_squash_unicode_content_squash_unicode_sql_content_preserved(tmp_path: Path) -> None:
    """Test that Unicode characters in SQL are preserved after squash."""
    from sqlspec.migrations.squash import MigrationSquasher

    unicode_sql = "-- Émojis: 🎉 日本語 中文 العربية\nCREATE TABLE users (name VARCHAR(100) DEFAULT 'Ñoño');"
    (tmp_path / "0001_unicode.sql").write_text(unicode_sql, encoding="utf-8")
    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_unicode.sql")]
    migration = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_unicode.sql"}
    migration["loader"].get_up_sql.return_value = [unicode_sql]
    migration["loader"].get_down_sql.return_value = None
    runner.load_migration.return_value = migration
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "unicode_test")
    (up_sql, _) = squasher.extract_sql(plans[0].source_migrations)
    content = squasher.generate_squashed_content(plans[0], up_sql, [])
    assert "🎉" in content
    assert "日本語" in content
    assert "中文" in content
    assert "العربية" in content
    assert "Ñoño" in content


def test_squash_unicode_content_squash_unicode_description_in_header(tmp_path: Path) -> None:
    """Test that Unicode in description is preserved in file header."""
    from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

    runner = Mock()
    squasher = MigrationSquasher(tmp_path, runner)
    plan = SquashPlan(
        source_migrations=[("0001", tmp_path / "0001_test.sql")],
        target_version="0001",
        target_path=tmp_path / "0001_release.sql",
        description="données_utilisateur",
        source_versions=["0001"],
    )
    content = squasher.generate_squashed_content(plan, ["SELECT 1;"], [])
    assert "données_utilisateur" in content


def test_squash_large_migrations_squash_large_sql_content(tmp_path: Path) -> None:
    """Test that large SQL content is handled correctly."""
    from sqlspec.migrations.squash import MigrationSquasher

    large_insert = "INSERT INTO data (value) VALUES " + ", ".join(f"({i})" for i in range(50000))
    (tmp_path / "0001_large.sql").write_text(f"-- large migration\n{large_insert};")
    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_large.sql")]
    migration = {"version": "0001", "loader": Mock(), "file_path": tmp_path / "0001_large.sql"}
    migration["loader"].get_up_sql.return_value = [large_insert + ";"]
    migration["loader"].get_down_sql.return_value = None
    runner.load_migration.return_value = migration
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "large_test")
    (up_sql, _) = squasher.extract_sql(plans[0].source_migrations)
    content = squasher.generate_squashed_content(plans[0], up_sql, [])
    assert "(49999)" in content
    assert len(content) > 400000


def test_squash_large_migrations_squash_multiple_large_migrations(tmp_path: Path) -> None:
    """Test squashing multiple large migrations together."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_large1.sql").write_text("-- large 1")
    (tmp_path / "0002_large2.sql").write_text("-- large 2")
    large_sql_1 = "INSERT INTO t1 VALUES " + ", ".join(f"({i})" for i in range(10000)) + ";"
    large_sql_2 = "INSERT INTO t2 VALUES " + ", ".join(f"({i})" for i in range(10000)) + ";"
    runner = Mock()
    runner.get_migration_files.return_value = [
        ("0001", tmp_path / "0001_large1.sql"),
        ("0002", tmp_path / "0002_large2.sql"),
    ]
    migration1 = {"version": "0001", "loader": Mock()}
    migration1["loader"].get_up_sql.return_value = [large_sql_1]
    migration1["loader"].get_down_sql.return_value = None
    migration2 = {"version": "0002", "loader": Mock()}
    migration2["loader"].get_up_sql.return_value = [large_sql_2]
    migration2["loader"].get_down_sql.return_value = None
    runner.load_migration.side_effect = [migration1, migration2]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0002", "multi_large")
    (up_sql, _) = squasher.extract_sql(plans[0].source_migrations)
    content = squasher.generate_squashed_content(plans[0], up_sql, [])
    assert "INSERT INTO t1" in content
    assert "INSERT INTO t2" in content


def test_squash_empty_migrations_squash_empty_up_sql(tmp_path: Path) -> None:
    """Test handling migration with empty UP SQL."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_empty.sql").write_text("-- empty migration")
    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_empty.sql")]
    migration = {"version": "0001", "loader": Mock()}
    migration["loader"].get_up_sql.return_value = []
    migration["loader"].get_down_sql.return_value = None
    runner.load_migration.return_value = migration
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "empty_test")
    (up_sql, down_sql) = squasher.extract_sql(plans[0].source_migrations)
    content = squasher.generate_squashed_content(plans[0], up_sql, down_sql)
    assert "-- name: migrate-0001-up" in content
    assert "Squashed from:" in content


def test_squash_empty_migrations_squash_whitespace_only_sql(tmp_path: Path) -> None:
    """Test handling migration with whitespace-only SQL."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_whitespace.sql").write_text("   \n\n   ")
    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_whitespace.sql")]
    migration = {"version": "0001", "loader": Mock()}
    migration["loader"].get_up_sql.return_value = ["   \n\n   "]
    migration["loader"].get_down_sql.return_value = None
    runner.load_migration.return_value = migration
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "whitespace_test")
    (up_sql, down_sql) = squasher.extract_sql(plans[0].source_migrations)
    content = squasher.generate_squashed_content(plans[0], up_sql, down_sql)
    assert "-- name: migrate-0001-up" in content


def test_squash_missing_downs_squash_all_migrations_missing_down(tmp_path: Path) -> None:
    """Test squashing migrations that all lack DOWN sections."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_no_down.sql").write_text("CREATE TABLE t1;")
    (tmp_path / "0002_no_down.sql").write_text("CREATE TABLE t2;")
    runner = Mock()
    runner.get_migration_files.return_value = [
        ("0001", tmp_path / "0001_no_down.sql"),
        ("0002", tmp_path / "0002_no_down.sql"),
    ]
    migration1 = {"version": "0001", "loader": Mock()}
    migration1["loader"].get_up_sql.return_value = ["CREATE TABLE t1;"]
    migration1["loader"].get_down_sql.return_value = None
    migration2 = {"version": "0002", "loader": Mock()}
    migration2["loader"].get_up_sql.return_value = ["CREATE TABLE t2;"]
    migration2["loader"].get_down_sql.return_value = None
    runner.load_migration.side_effect = [migration1, migration2]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0002", "no_downs")
    (up_sql, down_sql) = squasher.extract_sql(plans[0].source_migrations)
    assert len(up_sql) == 2
    assert len(down_sql) == 0
    content = squasher.generate_squashed_content(plans[0], up_sql, down_sql)
    assert "-- name: migrate-0001-up" in content
    assert "-- name: migrate-0001-down" not in content


def test_squash_missing_downs_squash_partial_downs(tmp_path: Path) -> None:
    """Test squashing when some migrations have DOWN and some don't."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_with_down.sql").write_text("CREATE TABLE t1;")
    (tmp_path / "0002_no_down.sql").write_text("CREATE TABLE t2;")
    runner = Mock()
    runner.get_migration_files.return_value = [
        ("0001", tmp_path / "0001_with_down.sql"),
        ("0002", tmp_path / "0002_no_down.sql"),
    ]
    migration1 = {"version": "0001", "loader": Mock()}
    migration1["loader"].get_up_sql.return_value = ["CREATE TABLE t1;"]
    migration1["loader"].get_down_sql.return_value = ["DROP TABLE t1;"]
    migration2 = {"version": "0002", "loader": Mock()}
    migration2["loader"].get_up_sql.return_value = ["CREATE TABLE t2;"]
    migration2["loader"].get_down_sql.return_value = None
    runner.load_migration.side_effect = [migration1, migration2]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0002", "partial_downs")
    (up_sql, down_sql) = squasher.extract_sql(plans[0].source_migrations)
    assert len(up_sql) == 2
    assert len(down_sql) == 1
    content = squasher.generate_squashed_content(plans[0], up_sql, down_sql)
    assert "-- name: migrate-0001-down" in content
    assert "DROP TABLE t1" in content


def test_squash_special_characters_squash_description_with_spaces_is_slugified(tmp_path: Path) -> None:
    """Test description with spaces is slugified in filename."""
    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_test.sql")]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "release with spaces")
    assert " " not in plans[0].target_path.name
    assert plans[0].target_path.name == "0001_release_with_spaces.sql"


def test_squash_special_characters_squash_description_with_underscores(tmp_path: Path) -> None:
    """Test description with underscores is handled correctly."""
    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_test.sql")]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "release_v1_0_0")
    assert plans[0].target_path.name == "0001_release_v1_0_0.sql"
    assert plans[0].description == "release_v1_0_0"


def test_squash_special_characters_squash_description_with_hyphens(tmp_path: Path) -> None:
    """Test description with hyphens is slugified to underscores."""
    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_test.sql")]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "release-v1-0-0")
    assert plans[0].target_path.name == "0001_release_v1_0_0.sql"


def test_squash_special_characters_squash_description_in_content_header(tmp_path: Path) -> None:
    """Test that description appears correctly in file header."""
    from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

    runner = Mock()
    squasher = MigrationSquasher(tmp_path, runner)
    plan = SquashPlan(
        source_migrations=[("0001", tmp_path / "0001_test.sql")],
        target_version="0001",
        target_path=tmp_path / "0001_special.sql",
        description="my-special_description.v1",
        source_versions=["0001"],
    )
    content = squasher.generate_squashed_content(plan, ["SELECT 1;"], [])
    assert "-- Description: my-special_description.v1" in content


def test_squash_down_ordering_down_statements_reversed(tmp_path: Path) -> None:
    """Test that DOWN statements are in reverse order."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_first.sql").write_text("-- first")
    (tmp_path / "0002_second.sql").write_text("-- second")
    (tmp_path / "0003_third.sql").write_text("-- third")
    runner = Mock()
    runner.get_migration_files.return_value = [
        ("0001", tmp_path / "0001_first.sql"),
        ("0002", tmp_path / "0002_second.sql"),
        ("0003", tmp_path / "0003_third.sql"),
    ]
    migration1 = {"version": "0001", "loader": Mock()}
    migration1["loader"].get_up_sql.return_value = ["CREATE TABLE t1;"]
    migration1["loader"].get_down_sql.return_value = ["DROP TABLE t1;"]
    migration2 = {"version": "0002", "loader": Mock()}
    migration2["loader"].get_up_sql.return_value = ["CREATE TABLE t2;"]
    migration2["loader"].get_down_sql.return_value = ["DROP TABLE t2;"]
    migration3 = {"version": "0003", "loader": Mock()}
    migration3["loader"].get_up_sql.return_value = ["CREATE TABLE t3;"]
    migration3["loader"].get_down_sql.return_value = ["DROP TABLE t3;"]
    runner.load_migration.side_effect = [migration1, migration2, migration3]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0003", "test_order")
    (up_sql, down_sql) = squasher.extract_sql(plans[0].source_migrations)
    assert up_sql == ["CREATE TABLE t1;", "CREATE TABLE t2;", "CREATE TABLE t3;"]
    assert down_sql == ["DROP TABLE t3;", "DROP TABLE t2;", "DROP TABLE t1;"]


def test_extract_sql_async_bridge_extract_sql_with_async_loader(tmp_path: Path) -> None:
    """Test that extract_sql correctly handles async get_up_sql/get_down_sql."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_test.sql").write_text("CREATE TABLE t1;")
    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_test.sql")]
    loader = Mock()
    loader.get_up_sql = AsyncMock(return_value=["CREATE TABLE t1;"])
    loader.get_down_sql = AsyncMock(return_value=["DROP TABLE t1;"])
    migration = {"version": "0001", "loader": loader, "file_path": tmp_path / "0001_test.sql"}
    runner.load_migration.return_value = migration
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "async_test")
    (up_sql, down_sql) = squasher.extract_sql(plans[0].source_migrations)
    assert len(up_sql) == 1
    assert isinstance(up_sql[0], str)
    assert up_sql[0] == "CREATE TABLE t1;"
    assert len(down_sql) == 1
    assert isinstance(down_sql[0], str)
    assert down_sql[0] == "DROP TABLE t1;"


def test_extract_sql_async_bridge_extract_sql_with_multiple_async_migrations(tmp_path: Path) -> None:
    """Test async bridge works across multiple migrations."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_a.sql").write_text("-- a")
    (tmp_path / "0002_b.sql").write_text("-- b")
    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_a.sql"), ("0002", tmp_path / "0002_b.sql")]
    loader1 = Mock()
    loader1.get_up_sql = AsyncMock(return_value=["CREATE TABLE a;"])
    loader1.get_down_sql = AsyncMock(return_value=["DROP TABLE a;"])
    migration1 = {"version": "0001", "loader": loader1, "file_path": tmp_path / "0001_a.sql"}
    loader2 = Mock()
    loader2.get_up_sql = AsyncMock(return_value=["CREATE TABLE b;"])
    loader2.get_down_sql = AsyncMock(return_value=["DROP TABLE b;"])
    migration2 = {"version": "0002", "loader": loader2, "file_path": tmp_path / "0002_b.sql"}
    runner.load_migration.side_effect = [migration1, migration2]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0002", "multi_async")
    (up_sql, down_sql) = squasher.extract_sql(plans[0].source_migrations)
    assert up_sql == ["CREATE TABLE a;", "CREATE TABLE b;"]
    assert down_sql == ["DROP TABLE b;", "DROP TABLE a;"]


def test_squash_slugified_filenames_description_with_spaces_slugified(tmp_path: Path) -> None:
    """Test that spaces in description are converted to underscores."""
    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_test.sql")]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "Version 1.0 Data Model")
    assert " " not in plans[0].target_path.name
    assert plans[0].target_path.name == "0001_version_1_0_data_model.sql"


def test_squash_slugified_filenames_description_with_mixed_case_slugified(tmp_path: Path) -> None:
    """Test that mixed case is lowered."""
    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_test.sql")]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "MyRelease")
    assert plans[0].target_path.name == "0001_myrelease.sql"


def test_squash_slugified_filenames_empty_description_uses_fallback(tmp_path: Path) -> None:
    """Test that empty description uses 'migration' as fallback."""
    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_test.sql")]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "")
    assert plans[0].target_path.name == "0001_migration.sql"


def test_squash_slugified_filenames_python_format_description_slugified(tmp_path: Path) -> None:
    """Test that Python output format also slugifies description."""
    from sqlspec.migrations.squash import MigrationSquasher

    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_test.sql")]
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "Version 1.0", output_format="py")
    assert " " not in plans[0].target_path.name
    assert plans[0].target_path.name == "0001_version_1_0.py"


def test_backup_cleanup_after_rollback_backup_removed_after_rollback(tmp_path: Path) -> None:
    """Test that backup directory is removed after a failed squash operation."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_first.sql").write_text("CREATE TABLE t1;")
    runner = Mock()
    runner.get_migration_files.return_value = [("0001", tmp_path / "0001_first.sql")]
    loader = Mock()
    loader.get_up_sql.side_effect = RuntimeError("test failure")
    migration = {"version": "0001", "loader": loader, "file_path": tmp_path / "0001_first.sql"}
    runner.load_migration.return_value = migration
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0001", "fail_test")
    with pytest.raises(RuntimeError, match="test failure"):
        squasher.apply_squash(plans)
    backup_dirs = [p for p in tmp_path.iterdir() if p.is_dir() and p.name.startswith(".backup_")]
    assert len(backup_dirs) == 0, f"Backup directory not cleaned up: {backup_dirs}"
    assert squasher.backup_path is None


def test_backup_cleanup_after_rollback_original_files_restored_after_rollback(tmp_path: Path) -> None:
    """Test that original migration files are restored after rollback."""
    from sqlspec.migrations.squash import MigrationSquasher

    (tmp_path / "0001_first.sql").write_text("original content 1")
    (tmp_path / "0002_second.sql").write_text("original content 2")
    runner = Mock()
    runner.get_migration_files.return_value = [
        ("0001", tmp_path / "0001_first.sql"),
        ("0002", tmp_path / "0002_second.sql"),
    ]
    loader = Mock()
    loader.get_up_sql.side_effect = RuntimeError("test failure")
    migration = {"version": "0001", "loader": loader, "file_path": tmp_path / "0001_first.sql"}
    runner.load_migration.return_value = migration
    squasher = MigrationSquasher(tmp_path, runner)
    plans = squasher.plan_squash("0001", "0002", "restore_test")
    with pytest.raises(RuntimeError, match="test failure"):
        squasher.apply_squash(plans)
    assert (tmp_path / "0001_first.sql").read_text() == "original content 1"
    assert (tmp_path / "0002_second.sql").read_text() == "original content 2"
