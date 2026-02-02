# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for migration grouping by type functionality.

Tests for:
- group_migrations_by_type()
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.xdist_group("migrations")


class TestGroupMigrationsByType:
    """Tests for group_migrations_by_type() function."""

    def test_group_all_sql(self) -> None:
        """Test grouping all SQL migrations returns single group."""
        from sqlspec.migrations.squash import group_migrations_by_type

        migrations: list[tuple[str, Path]] = [
            ("0001", Path("0001_initial.sql")),
            ("0002", Path("0002_users.sql")),
            ("0003", Path("0003_posts.sql")),
        ]

        groups = group_migrations_by_type(migrations)

        assert len(groups) == 1
        assert groups[0][0] == "sql"
        assert len(groups[0][1]) == 3

    def test_group_all_python(self) -> None:
        """Test grouping all Python migrations returns single group."""
        from sqlspec.migrations.squash import group_migrations_by_type

        migrations: list[tuple[str, Path]] = [("0001", Path("0001_initial.py")), ("0002", Path("0002_users.py"))]

        groups = group_migrations_by_type(migrations)

        assert len(groups) == 1
        assert groups[0][0] == "py"
        assert len(groups[0][1]) == 2

    def test_group_mixed_alternating(self) -> None:
        """Test alternating types creates multiple groups."""
        from sqlspec.migrations.squash import group_migrations_by_type

        migrations: list[tuple[str, Path]] = [
            ("0001", Path("0001_initial.sql")),
            ("0002", Path("0002_users.py")),
            ("0003", Path("0003_posts.sql")),
        ]

        groups = group_migrations_by_type(migrations)

        assert len(groups) == 3
        assert groups[0][0] == "sql"
        assert groups[1][0] == "py"
        assert groups[2][0] == "sql"

    def test_group_mixed_consecutive(self) -> None:
        """Test consecutive same types creates minimal groups."""
        from sqlspec.migrations.squash import group_migrations_by_type

        migrations: list[tuple[str, Path]] = [
            ("0001", Path("0001_initial.sql")),
            ("0002", Path("0002_users.sql")),
            ("0003", Path("0003_auth.py")),
            ("0004", Path("0004_posts.py")),
        ]

        groups = group_migrations_by_type(migrations)

        assert len(groups) == 2
        assert groups[0][0] == "sql"
        assert len(groups[0][1]) == 2
        assert groups[1][0] == "py"
        assert len(groups[1][1]) == 2

    def test_group_empty_list(self) -> None:
        """Test empty migrations list returns empty groups."""
        from sqlspec.migrations.squash import group_migrations_by_type

        groups = group_migrations_by_type([])

        assert groups == []
