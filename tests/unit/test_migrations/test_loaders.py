"""Unit tests for SQLSpec migration loaders."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from sqlspec.migrations.loaders import (
    BaseMigrationLoader,
    MigrationLoadError,
    PythonFileLoader,
    SQLFileLoader,
    get_migration_loader,
)


class TestBaseMigrationLoader:
    """Test the abstract BaseMigrationLoader class."""

    def test_cannot_instantiate_abstract_class(self) -> None:
        """Test that BaseMigrationLoader cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseMigrationLoader()  # type: ignore[abstract]


class TestSQLFileLoader:
    """Test the SQLFileLoader class."""

    def test_extract_version_valid_filename(self) -> None:
        """Test version extraction from valid filename."""
        loader = SQLFileLoader()
        assert loader._extract_version("0001_initial.sql") == "0001"
        assert loader._extract_version("123_create_table.sql") == "0123"
        assert loader._extract_version("1_test.sql") == "0001"

    def test_extract_version_invalid_filename(self) -> None:
        """Test version extraction from invalid filename."""
        loader = SQLFileLoader()
        assert loader._extract_version("invalid.sql") == ""
        assert loader._extract_version("abc_test.sql") == ""

    def test_validate_migration_file_invalid_filename(self) -> None:
        """Test validation fails for invalid filename."""
        loader = SQLFileLoader()

        with tempfile.NamedTemporaryFile(suffix=".sql", delete=False) as f:
            test_file = Path(f.name)

        try:
            with pytest.raises(MigrationLoadError, match="Invalid migration filename"):
                loader.validate_migration_file(test_file)
        finally:
            test_file.unlink()

    def test_validate_migration_file_missing_up_query(self) -> None:
        """Test validation fails when up query is missing."""
        loader = SQLFileLoader()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- name: some-other-query\nSELECT 1;")
            test_file = Path(f.name)
            # Change filename to valid format
            valid_file = test_file.parent / "0001_test.sql"
            test_file.rename(valid_file)

        try:
            with pytest.raises(MigrationLoadError, match="missing required 'up' query"):
                loader.validate_migration_file(valid_file)
        finally:
            valid_file.unlink()

    def test_validate_migration_file_success(self) -> None:
        """Test successful validation with valid migration file."""
        loader = SQLFileLoader()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- name: migrate-0001-up\nCREATE TABLE test (id INTEGER);")
            test_file = Path(f.name)
            # Change filename to valid format
            valid_file = test_file.parent / "0001_test.sql"
            test_file.rename(valid_file)

        try:
            loader.validate_migration_file(valid_file)  # Should not raise
        finally:
            valid_file.unlink()

    @pytest.mark.asyncio
    async def test_get_up_sql_success(self) -> None:
        """Test successful extraction of up SQL."""
        loader = SQLFileLoader()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- name: migrate-0001-up\nCREATE TABLE test (id INTEGER);")
            test_file = Path(f.name)
            # Change filename to valid format
            valid_file = test_file.parent / "0001_test.sql"
            test_file.rename(valid_file)

        try:
            result = await loader.get_up_sql(valid_file)
            assert len(result) == 1
            assert "CREATE TABLE test" in result[0]
        finally:
            valid_file.unlink()

    @pytest.mark.asyncio
    async def test_get_up_sql_missing_query(self) -> None:
        """Test error when up query is missing."""
        loader = SQLFileLoader()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- name: some-other-query\nSELECT 1;")
            test_file = Path(f.name)
            # Change filename to valid format
            valid_file = test_file.parent / "0001_test.sql"
            test_file.rename(valid_file)

        try:
            with pytest.raises(MigrationLoadError, match="missing 'up' query"):
                await loader.get_up_sql(valid_file)
        finally:
            valid_file.unlink()

    @pytest.mark.asyncio
    async def test_get_down_sql_success(self) -> None:
        """Test successful extraction of down SQL."""
        loader = SQLFileLoader()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("""-- name: migrate-0001-up
CREATE TABLE test (id INTEGER);

-- name: migrate-0001-down
DROP TABLE test;""")
            test_file = Path(f.name)
            # Change filename to valid format
            valid_file = test_file.parent / "0001_test.sql"
            test_file.rename(valid_file)

        try:
            result = await loader.get_down_sql(valid_file)
            assert len(result) == 1
            assert "DROP TABLE test" in result[0]
        finally:
            valid_file.unlink()

    @pytest.mark.asyncio
    async def test_get_down_sql_missing_query(self) -> None:
        """Test empty result when down query is missing."""
        loader = SQLFileLoader()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- name: migrate-0001-up\nCREATE TABLE test (id INTEGER);")
            test_file = Path(f.name)
            # Change filename to valid format
            valid_file = test_file.parent / "0001_test.sql"
            test_file.rename(valid_file)

        try:
            result = await loader.get_down_sql(valid_file)
            assert result == []
        finally:
            valid_file.unlink()


class TestPythonFileLoader:
    """Test the PythonFileLoader class."""

    def test_find_project_root_with_marker(self) -> None:
        """Test finding project root with marker file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root_dir = Path(tmpdir)
            migrations_dir = root_dir / "migrations"
            migrations_dir.mkdir()

            # Create marker file
            (root_dir / "pyproject.toml").touch()

            loader = PythonFileLoader(migrations_dir)
            assert loader.project_root == root_dir

    def test_project_root_explicit_config(self) -> None:
        """Test project root configuration via explicit parameter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir) / "migrations"
            migrations_dir.mkdir()
            project_root = Path(tmpdir) / "project"
            project_root.mkdir()

            loader = PythonFileLoader(migrations_dir, project_root)
            assert loader.project_root == project_root

    def test_project_root_fallback_detection(self) -> None:
        """Test fallback to filesystem detection when no explicit config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a nested structure to avoid system-level marker files
            base_dir = Path(tmpdir) / "deep" / "nested" / "structure"
            base_dir.mkdir(parents=True)
            migrations_dir = base_dir / "migrations"
            migrations_dir.mkdir()

            # Mock the marker detection to avoid system-level files

            def mock_find_project_root(start_path: Path) -> Path:
                """Mock that returns the parent directory as fallback."""
                return start_path.resolve().parent

            with patch.object(PythonFileLoader, "_find_project_root", mock_find_project_root):
                loader = PythonFileLoader(migrations_dir)
                assert loader.project_root == migrations_dir.parent

    def test_normalize_and_validate_sql_string(self) -> None:
        """Test SQL normalization with string input."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            result = loader._normalize_and_validate_sql("CREATE TABLE test;", Path("test.py"))
            assert result == ["CREATE TABLE test;"]

            # Test empty string
            result = loader._normalize_and_validate_sql("", Path("test.py"))
            assert result == []

    def test_normalize_and_validate_sql_list(self) -> None:
        """Test SQL normalization with list input."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            sql_list = ["CREATE TABLE test;", "INSERT INTO test VALUES (1);"]
            result = loader._normalize_and_validate_sql(sql_list, Path("test.py"))
            assert result == sql_list

    def test_normalize_and_validate_sql_invalid_type(self) -> None:
        """Test SQL normalization with invalid type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            with pytest.raises(MigrationLoadError, match="must return a 'str' or 'List\\[str\\]'"):
                loader._normalize_and_validate_sql(123, Path("test.py"))

    def test_normalize_and_validate_sql_invalid_list_element(self) -> None:
        """Test SQL normalization with invalid list element."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            with pytest.raises(MigrationLoadError, match="containing a non-string element"):
                loader._normalize_and_validate_sql(["CREATE TABLE test;", 123], Path("test.py"))

    def test_validate_migration_file_missing_function(self) -> None:
        """Test validation fails when migrate_up function is missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create Python file without migrate_up function
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("def some_other_function(): pass")

            with pytest.raises(MigrationLoadError, match="missing required 'migrate_up' function"):
                loader.validate_migration_file(test_file)

    def test_validate_migration_file_not_callable(self) -> None:
        """Test validation fails when migrate_up is not callable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create Python file with non-callable migrate_up
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("migrate_up = 'not a function'")

            with pytest.raises(MigrationLoadError, match="'migrate_up' is not callable"):
                loader.validate_migration_file(test_file)

    def test_validate_migration_file_success(self) -> None:
        """Test successful validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create valid Python migration file
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("""
def migrate_up():
    return "CREATE TABLE test (id INTEGER);"
""")

            loader.validate_migration_file(test_file)  # Should not raise

    @pytest.mark.asyncio
    async def test_get_up_sql_success(self) -> None:
        """Test successful extraction of up SQL from Python file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create valid Python migration file
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("""
def migrate_up():
    return "CREATE TABLE test (id INTEGER);"
""")

            result = await loader.get_up_sql(test_file)
            assert result == ["CREATE TABLE test (id INTEGER);"]

    @pytest.mark.asyncio
    async def test_get_up_sql_async_function(self) -> None:
        """Test extraction with async migrate_up function."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create Python migration file with async function
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("""
async def migrate_up():
    return "CREATE TABLE test (id INTEGER);"
""")

            result = await loader.get_up_sql(test_file)
            assert result == ["CREATE TABLE test (id INTEGER);"]

    @pytest.mark.asyncio
    async def test_get_up_sql_missing_function(self) -> None:
        """Test error when migrate_up function is missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create Python file without migrate_up
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("def some_other_function(): pass")

            with pytest.raises(MigrationLoadError, match="'migrate_up' function not found"):
                await loader.get_up_sql(test_file)

    @pytest.mark.asyncio
    async def test_get_down_sql_success(self) -> None:
        """Test successful extraction of down SQL from Python file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create valid Python migration file with both functions
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("""
def migrate_up():
    return "CREATE TABLE test (id INTEGER);"

def migrate_down():
    return "DROP TABLE test;"
""")

            result = await loader.get_down_sql(test_file)
            assert result == ["DROP TABLE test;"]

    @pytest.mark.asyncio
    async def test_get_down_sql_missing_function(self) -> None:
        """Test empty result when migrate_down function is missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create Python file without migrate_down
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("""
def migrate_up():
    return "CREATE TABLE test (id INTEGER);"
""")

            result = await loader.get_down_sql(test_file)
            assert result == []

    @pytest.mark.asyncio
    async def test_get_down_sql_not_callable(self) -> None:
        """Test empty result when migrate_down is not callable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create Python file with non-callable migrate_down
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("""
def migrate_up():
    return "CREATE TABLE test (id INTEGER);"

migrate_down = "not a function"
""")

            result = await loader.get_down_sql(test_file)
            assert result == []

    def test_load_module_from_path_success(self) -> None:
        """Test successful module loading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create valid Python file
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("""
def migrate_up():
    return "CREATE TABLE test (id INTEGER);"
""")

            module = loader._load_module_from_path(test_file)
            assert hasattr(module, "migrate_up")
            assert callable(module.migrate_up)

    def test_load_module_from_path_syntax_error(self) -> None:
        """Test error handling for Python syntax errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)

            # Create Python file with syntax error
            test_file = migrations_dir / "0001_test.py"
            test_file.write_text("def migrate_up(\n  # Syntax error - missing closing parenthesis")

            with pytest.raises(MigrationLoadError, match="Failed to execute migration module"):
                loader._load_module_from_path(test_file)

    def test_temporary_project_path_context_manager(self) -> None:
        """Test the temporary project path context manager."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)
            project_root_str = str(loader.project_root)

            # Project root should not be in sys.path initially
            import sys

            if project_root_str in sys.path:
                sys.path.remove(project_root_str)

            assert project_root_str not in sys.path

            with loader._temporary_project_path():
                assert project_root_str in sys.path

            assert project_root_str not in sys.path

    def test_temporary_project_path_already_in_path(self) -> None:
        """Test context manager when path is already in sys.path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            loader = PythonFileLoader(migrations_dir)
            project_root_str = str(loader.project_root)

            import sys

            # Add to sys.path manually
            if project_root_str not in sys.path:
                sys.path.insert(0, project_root_str)

            try:
                with loader._temporary_project_path():
                    assert project_root_str in sys.path

                # Should still be in path since it was there before
                assert project_root_str in sys.path
            finally:
                if project_root_str in sys.path:
                    sys.path.remove(project_root_str)


class TestGetMigrationLoader:
    """Test the get_migration_loader factory function."""

    def test_get_sql_loader(self) -> None:
        """Test getting SQL loader for .sql files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            test_file = migrations_dir / "0001_test.sql"
            test_file.touch()

            loader = get_migration_loader(test_file, migrations_dir)
            assert isinstance(loader, SQLFileLoader)

    def test_get_python_loader(self) -> None:
        """Test getting Python loader for .py files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            test_file = migrations_dir / "0001_test.py"
            test_file.touch()
            project_root = Path(tmpdir) / "project"
            project_root.mkdir()

            loader = get_migration_loader(test_file, migrations_dir, project_root)
            assert isinstance(loader, PythonFileLoader)
            assert loader.migrations_dir == migrations_dir
            assert loader.project_root == project_root

    def test_unsupported_file_type(self) -> None:
        """Test error for unsupported file types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            migrations_dir = Path(tmpdir)
            test_file = migrations_dir / "0001_test.txt"
            test_file.touch()

            with pytest.raises(MigrationLoadError, match="Unsupported migration file type: .txt"):
                get_migration_loader(test_file, migrations_dir)
