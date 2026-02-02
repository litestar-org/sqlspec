# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false, reportArgumentType = false, reportIndexIssue = false
"""Unit tests for Python output format in migration squash.

Tests for:
- generate_python_squash() method
- Python file syntax and structure
- Correct up() and down() function generation
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.xdist_group("migrations")


class TestGeneratePythonSquash:
    """Tests for MigrationSquasher.generate_python_squash() method."""

    def test_generate_python_squash_valid_syntax(self, tmp_path: Path) -> None:
        """Test that generated Python file has valid syntax."""
        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = type("Mock", (), {"load_migration": lambda *a: None})()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_test.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.py",
            description="release_1",
            source_versions=["0001"],
        )

        up_sql = ["CREATE TABLE users (id INT);"]
        down_sql = ["DROP TABLE users;"]

        content = squasher.generate_python_squash(plan, up_sql, down_sql)

        # Should be valid Python - compile it
        compile(content, "<string>", "exec")

    def test_generate_python_squash_has_docstring(self, tmp_path: Path) -> None:
        """Test that generated Python file contains metadata docstring."""
        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = type("Mock", (), {"load_migration": lambda *a: None})()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_initial.sql"), ("0002", tmp_path / "0002_users.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.py",
            description="release_v1",
            source_versions=["0001", "0002"],
        )

        content = squasher.generate_python_squash(plan, ["SELECT 1;"], [])

        assert '"""' in content or "'''" in content  # Has docstring
        assert "Squashed from:" in content
        assert "0001" in content
        assert "0002" in content
        assert "release_v1" in content

    def test_generate_python_squash_up_function(self, tmp_path: Path) -> None:
        """Test that up() function returns SQL statements."""
        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = type("Mock", (), {"load_migration": lambda *a: None})()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_test.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.py",
            description="release",
            source_versions=["0001"],
        )

        up_sql = ["CREATE TABLE users (id INT);", "CREATE TABLE posts (id INT);"]
        down_sql: list[str] = []

        content = squasher.generate_python_squash(plan, up_sql, down_sql)

        # Execute and check the up() function
        namespace: dict[str, object] = {}
        exec(content, namespace)

        assert "up" in namespace
        up_func = namespace["up"]
        assert callable(up_func)
        up_result = up_func()
        assert isinstance(up_result, list)
        assert "CREATE TABLE users" in up_result[0]
        assert "CREATE TABLE posts" in up_result[1]

    def test_generate_python_squash_down_function(self, tmp_path: Path) -> None:
        """Test that down() function returns SQL statements in correct order."""
        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = type("Mock", (), {"load_migration": lambda *a: None})()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_test.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.py",
            description="release",
            source_versions=["0001"],
        )

        up_sql = ["CREATE TABLE users;"]
        down_sql = ["DROP TABLE posts;", "DROP TABLE users;"]

        content = squasher.generate_python_squash(plan, up_sql, down_sql)

        # Execute and check the down() function
        namespace: dict[str, object] = {}
        exec(content, namespace)

        assert "down" in namespace
        down_func = namespace["down"]
        assert callable(down_func)
        down_result = down_func()
        assert isinstance(down_result, list)
        assert len(down_result) == 2
        # Order should be preserved (already reversed by extract_sql)
        assert "DROP TABLE posts" in down_result[0]
        assert "DROP TABLE users" in down_result[1]

    def test_generate_python_squash_empty_down(self, tmp_path: Path) -> None:
        """Test that empty down SQL generates None-returning down() function."""
        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = type("Mock", (), {"load_migration": lambda *a: None})()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_test.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.py",
            description="release",
            source_versions=["0001"],
        )

        content = squasher.generate_python_squash(plan, ["SELECT 1;"], [])

        namespace: dict[str, object] = {}
        exec(content, namespace)

        assert "down" in namespace
        down_func = namespace["down"]
        assert callable(down_func)
        down_result = down_func()
        assert down_result is None or down_result == []

    def test_generate_python_squash_escapes_quotes(self, tmp_path: Path) -> None:
        """Test that SQL with quotes is properly escaped."""
        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = type("Mock", (), {"load_migration": lambda *a: None})()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_test.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.py",
            description="release",
            source_versions=["0001"],
        )

        # SQL with various quote types
        up_sql = ["INSERT INTO users (name) VALUES ('John''s');", 'UPDATE users SET name = "Test" WHERE id = 1;']

        content = squasher.generate_python_squash(plan, up_sql, [])

        # Should compile without syntax errors
        compile(content, "<string>", "exec")

        # Execute and verify the strings are correct
        namespace: dict[str, object] = {}
        exec(content, namespace)

        up_func = namespace["up"]
        assert callable(up_func)
        up_result = up_func()
        assert "John''s" in up_result[0]  # SQL escaping preserved

    def test_generate_python_squash_multiline_sql(self, tmp_path: Path) -> None:
        """Test that multiline SQL is handled correctly."""
        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = type("Mock", (), {"load_migration": lambda *a: None})()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_test.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.py",
            description="release",
            source_versions=["0001"],
        )

        multiline_sql = """CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP
);"""

        content = squasher.generate_python_squash(plan, [multiline_sql], [])

        # Should compile
        compile(content, "<string>", "exec")

        namespace: dict[str, object] = {}
        exec(content, namespace)

        up_func = namespace["up"]
        assert callable(up_func)
        up_result = up_func()
        assert "CREATE TABLE users" in up_result[0]
        assert "INT PRIMARY KEY" in up_result[0]

    def test_generate_python_squash_return_type_annotation(self, tmp_path: Path) -> None:
        """Test that functions have proper type annotations."""
        from sqlspec.migrations.squash import MigrationSquasher, SquashPlan

        runner = type("Mock", (), {"load_migration": lambda *a: None})()
        squasher = MigrationSquasher(tmp_path, runner)

        plan = SquashPlan(
            source_migrations=[("0001", tmp_path / "0001_test.sql")],
            target_version="0001",
            target_path=tmp_path / "0001_release.py",
            description="release",
            source_versions=["0001"],
        )

        content = squasher.generate_python_squash(plan, ["SELECT 1;"], ["SELECT 2;"])

        # Should have type annotations
        assert "def up() -> list[str]:" in content
        assert "def down() -> list[str]:" in content or "def down() -> list[str] | None:" in content
