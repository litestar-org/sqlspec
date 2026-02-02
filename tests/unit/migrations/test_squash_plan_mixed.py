# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for plan_squash() with mixed SQL/Python migrations.

Tests for:
- plan_squash() returning list[SquashPlan]
- Single plan for all-SQL migrations
- Single plan for all-Python migrations
- Multiple plans for mixed migrations
"""

from pathlib import Path
from unittest.mock import Mock

import pytest

pytestmark = pytest.mark.xdist_group("migrations")


class TestPlanSquashMixedTypes:
    """Tests for plan_squash() with mixed migration types."""

    def test_plan_squash_all_sql_returns_single_plan(self, tmp_path: Path) -> None:
        """Test plan_squash returns single plan for all SQL migrations."""
        from sqlspec.migrations.squash import MigrationSquasher

        # Create SQL migrations
        (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
        (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
        (tmp_path / "0003_posts.sql").write_text("-- name: migrate-0003-up\nCREATE TABLE t3 (id INT);")

        # Mock runner
        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_initial.sql"),
            ("0002", tmp_path / "0002_users.sql"),
            ("0003", tmp_path / "0003_posts.sql"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)
        plans = squasher.plan_squash("0001", "0003", "release_1")

        assert isinstance(plans, list)
        assert len(plans) == 1
        assert plans[0].target_version == "0001"
        assert len(plans[0].source_migrations) == 3

    def test_plan_squash_all_python_returns_single_plan(self, tmp_path: Path) -> None:
        """Test plan_squash returns single plan for all Python migrations."""
        from sqlspec.migrations.squash import MigrationSquasher

        # Create Python migrations
        (tmp_path / "0001_initial.py").write_text("def up(): return ['CREATE TABLE t1 (id INT);']")
        (tmp_path / "0002_users.py").write_text("def up(): return ['CREATE TABLE t2 (id INT);']")

        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_initial.py"),
            ("0002", tmp_path / "0002_users.py"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)
        plans = squasher.plan_squash("0001", "0002", "release_1")

        assert isinstance(plans, list)
        assert len(plans) == 1
        assert plans[0].target_version == "0001"
        assert plans[0].target_path.suffix == ".py"

    def test_plan_squash_mixed_returns_multiple_plans(self, tmp_path: Path) -> None:
        """Test plan_squash returns multiple plans for mixed SQL/Python migrations."""
        from sqlspec.migrations.squash import MigrationSquasher

        # Create mixed migrations: SQL, SQL, Python, Python, SQL
        (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
        (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
        (tmp_path / "0003_auth.py").write_text("def up(): return ['CREATE TABLE t3 (id INT);']")
        (tmp_path / "0004_posts.py").write_text("def up(): return ['CREATE TABLE t4 (id INT);']")
        (tmp_path / "0005_comments.sql").write_text("-- name: migrate-0005-up\nCREATE TABLE t5 (id INT);")

        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_initial.sql"),
            ("0002", tmp_path / "0002_users.sql"),
            ("0003", tmp_path / "0003_auth.py"),
            ("0004", tmp_path / "0004_posts.py"),
            ("0005", tmp_path / "0005_comments.sql"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)
        plans = squasher.plan_squash("0001", "0005", "release_1")

        assert isinstance(plans, list)
        assert len(plans) == 3

        # First group: SQL (0001, 0002)
        assert plans[0].target_version == "0001"
        assert plans[0].target_path.suffix == ".sql"
        assert len(plans[0].source_migrations) == 2

        # Second group: Python (0003, 0004)
        assert plans[1].target_version == "0002"  # Incremented version
        assert plans[1].target_path.suffix == ".py"
        assert len(plans[1].source_migrations) == 2

        # Third group: SQL (0005)
        assert plans[2].target_version == "0003"  # Incremented again
        assert plans[2].target_path.suffix == ".sql"
        assert len(plans[2].source_migrations) == 1

    def test_plan_squash_mixed_version_numbering(self, tmp_path: Path) -> None:
        """Test plan_squash assigns sequential versions to multiple plans."""
        from sqlspec.migrations.squash import MigrationSquasher

        # Create alternating types
        (tmp_path / "0001_a.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1;")
        (tmp_path / "0002_b.py").write_text("def up(): return ['CREATE TABLE t2;']")
        (tmp_path / "0003_c.sql").write_text("-- name: migrate-0003-up\nCREATE TABLE t3;")

        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_a.sql"),
            ("0002", tmp_path / "0002_b.py"),
            ("0003", tmp_path / "0003_c.sql"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)
        plans = squasher.plan_squash("0001", "0003", "release_1")

        # Versions should be sequential starting from start_version
        versions = [p.target_version for p in plans]
        assert versions == ["0001", "0002", "0003"]

    def test_plan_squash_mixed_preserves_source_versions(self, tmp_path: Path) -> None:
        """Test plan_squash tracks source versions correctly for each plan."""
        from sqlspec.migrations.squash import MigrationSquasher

        (tmp_path / "0001_a.sql").write_text("-- up\nCREATE TABLE t1;")
        (tmp_path / "0002_b.sql").write_text("-- up\nCREATE TABLE t2;")
        (tmp_path / "0003_c.py").write_text("def up(): return ['CREATE TABLE t3;']")

        runner = Mock()
        runner.get_migration_files.return_value = [
            ("0001", tmp_path / "0001_a.sql"),
            ("0002", tmp_path / "0002_b.sql"),
            ("0003", tmp_path / "0003_c.py"),
        ]

        squasher = MigrationSquasher(tmp_path, runner)
        plans = squasher.plan_squash("0001", "0003", "release_1")

        assert len(plans) == 2

        # First plan has source versions 0001, 0002
        assert plans[0].source_versions == ["0001", "0002"]

        # Second plan has source version 0003
        assert plans[1].source_versions == ["0003"]
