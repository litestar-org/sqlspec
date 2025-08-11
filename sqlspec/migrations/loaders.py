"""Migration loader abstractions for SQLSpec."""

import abc
import inspect
import sys
import types
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Final, Optional

from sqlspec.loader import SQLFileLoader as CoreSQLFileLoader

__all__ = ("BaseMigrationLoader", "MigrationLoadError", "PythonFileLoader", "SQLFileLoader", "get_migration_loader")

PROJECT_ROOT_MARKERS: Final[list[str]] = ["pyproject.toml", ".git", "setup.cfg", "setup.py"]


class MigrationLoadError(Exception):
    """Exception raised when migration loading fails."""


class BaseMigrationLoader(abc.ABC):
    """Abstract base class for migration loaders."""

    __slots__ = ()

    @abc.abstractmethod
    async def get_up_sql(self, path: Path) -> list[str]:
        """Load and return the 'up' SQL statements from a migration file.

        Args:
            path: Path to the migration file.

        Returns:
            List of SQL statements to execute for upgrade.

        Raises:
            MigrationLoadError: If loading fails.
        """
        ...

    @abc.abstractmethod
    async def get_down_sql(self, path: Path) -> list[str]:
        """Load and return the 'down' SQL statements from a migration file.

        Args:
            path: Path to the migration file.

        Returns:
            List of SQL statements to execute for downgrade.
            Empty list if no downgrade is available.

        Raises:
            MigrationLoadError: If loading fails.
        """
        ...

    @abc.abstractmethod
    def validate_migration_file(self, path: Path) -> None:
        """Validate that the migration file has required components.

        Args:
            path: Path to the migration file.

        Raises:
            MigrationLoadError: If validation fails.
        """
        ...


class SQLFileLoader(BaseMigrationLoader):
    """Loader for SQL migration files using SQLFileLoader."""

    __slots__ = ("sql_loader",)

    def __init__(self) -> None:
        """Initialize SQL file loader."""
        self.sql_loader: CoreSQLFileLoader = CoreSQLFileLoader()

    async def get_up_sql(self, path: Path) -> list[str]:
        """Extract the 'up' SQL from a SQL migration file.

        Args:
            path: Path to SQL migration file.

        Returns:
            List containing single SQL statement for upgrade.

        Raises:
            MigrationLoadError: If migration file is invalid or missing up query.
        """
        self.sql_loader.clear_cache()
        self.sql_loader.load_sql(path)

        version = self._extract_version(path.name)
        up_query = f"migrate-{version}-up"

        if not self.sql_loader.has_query(up_query):
            msg = f"Migration {path} missing 'up' query: {up_query}"
            raise MigrationLoadError(msg)

        sql_obj = self.sql_loader.get_sql(up_query)
        return [sql_obj.sql]

    async def get_down_sql(self, path: Path) -> list[str]:
        """Extract the 'down' SQL from a SQL migration file.

        Args:
            path: Path to SQL migration file.

        Returns:
            List containing single SQL statement for downgrade, or empty list.
        """
        self.sql_loader.clear_cache()
        self.sql_loader.load_sql(path)

        version = self._extract_version(path.name)
        down_query = f"migrate-{version}-down"

        if not self.sql_loader.has_query(down_query):
            return []

        sql_obj = self.sql_loader.get_sql(down_query)
        return [sql_obj.sql]

    def validate_migration_file(self, path: Path) -> None:
        """Validate SQL migration file has required up query.

        Args:
            path: Path to SQL migration file.

        Raises:
            MigrationLoadError: If file is invalid or missing required query.
        """
        version = self._extract_version(path.name)
        if not version:
            msg = f"Invalid migration filename: {path.name}"
            raise MigrationLoadError(msg)

        self.sql_loader.clear_cache()
        self.sql_loader.load_sql(path)
        up_query = f"migrate-{version}-up"
        if not self.sql_loader.has_query(up_query):
            msg = f"Migration {path} missing required 'up' query: {up_query}"
            raise MigrationLoadError(msg)

    def _extract_version(self, filename: str) -> str:
        """Extract version from filename.

        Args:
            filename: Migration filename to parse.

        Returns:
            Zero-padded version string or empty string if invalid.
        """
        parts = filename.split("_", 1)
        return parts[0].zfill(4) if parts and parts[0].isdigit() else ""


class PythonFileLoader(BaseMigrationLoader):
    """Loader for Python migration files."""

    __slots__ = ("migrations_dir", "project_root")

    def __init__(self, migrations_dir: Path, project_root: "Optional[Path]" = None) -> None:
        """Initialize Python file loader.

        Args:
            migrations_dir: Directory containing migration files.
            project_root: Optional project root directory for imports.
        """
        self.migrations_dir = migrations_dir
        self.project_root = project_root if project_root is not None else self._find_project_root(migrations_dir)

    async def get_up_sql(self, path: Path) -> list[str]:
        """Load Python migration and execute upgrade function.

        Args:
            path: Path to Python migration file.

        Returns:
            List of SQL statements for upgrade.

        Raises:
            MigrationLoadError: If function is missing or execution fails.
        """
        with self._temporary_project_path():
            module = self._load_module_from_path(path)

            upgrade_func = None
            func_name = None

            if hasattr(module, "up") and callable(module.up):
                upgrade_func = module.up
                func_name = "up"
            elif hasattr(module, "migrate_up") and callable(module.migrate_up):
                upgrade_func = module.migrate_up
                func_name = "migrate_up"
            else:
                msg = f"No upgrade function found in {path}. Expected 'up()' or 'migrate_up()'"
                raise MigrationLoadError(msg)

            if not callable(upgrade_func):
                msg = f"'{func_name}' is not callable in {path}"
                raise MigrationLoadError(msg)

            if inspect.iscoroutinefunction(upgrade_func):
                sql_result = await upgrade_func()
            else:
                sql_result = upgrade_func()

            return self._normalize_and_validate_sql(sql_result, path)

    async def get_down_sql(self, path: Path) -> list[str]:
        """Load Python migration and execute downgrade function.

        Args:
            path: Path to Python migration file.

        Returns:
            List of SQL statements for downgrade, or empty list if not available.
        """
        with self._temporary_project_path():
            module = self._load_module_from_path(path)

            downgrade_func = None

            if hasattr(module, "down") and callable(module.down):
                downgrade_func = module.down
            elif hasattr(module, "migrate_down") and callable(module.migrate_down):
                downgrade_func = module.migrate_down
            else:
                return []

            if not callable(downgrade_func):
                return []

            if inspect.iscoroutinefunction(downgrade_func):
                sql_result = await downgrade_func()
            else:
                sql_result = downgrade_func()

            return self._normalize_and_validate_sql(sql_result, path)

    def validate_migration_file(self, path: Path) -> None:
        """Validate Python migration file has required upgrade function.

        Args:
            path: Path to Python migration file.

        Raises:
            MigrationLoadError: If validation fails.
        """
        with self._temporary_project_path():
            module = self._load_module_from_path(path)

            upgrade_func = None
            func_name = None

            if hasattr(module, "up") and callable(module.up):
                upgrade_func = module.up
                func_name = "up"
            elif hasattr(module, "migrate_up") and callable(module.migrate_up):
                upgrade_func = module.migrate_up
                func_name = "migrate_up"
            else:
                msg = f"Migration {path} missing required upgrade function. Expected 'up()' or 'migrate_up()'"
                raise MigrationLoadError(msg)

            if not callable(upgrade_func):
                msg = f"Migration {path} '{func_name}' is not callable"
                raise MigrationLoadError(msg)

    def _find_project_root(self, start_path: Path) -> Path:
        """Find project root by searching upwards for marker files.

        Args:
            start_path: Directory to start searching from.

        Returns:
            Path to project root or parent directory.
        """
        current_path = start_path.resolve()

        while current_path != current_path.parent:
            for marker in PROJECT_ROOT_MARKERS:
                if (current_path / marker).exists():
                    return current_path
            current_path = current_path.parent

        return start_path.resolve().parent

    @contextmanager
    def _temporary_project_path(self) -> Iterator[None]:
        """Temporarily add project root to sys.path for imports."""
        path_to_add = str(self.project_root)
        if path_to_add in sys.path:
            yield
            return

        sys.path.insert(0, path_to_add)
        try:
            yield
        finally:
            sys.path.remove(path_to_add)

    def _load_module_from_path(self, path: Path) -> Any:
        """Load a Python module from file path.

        Args:
            path: Path to Python migration file.

        Returns:
            Loaded module object.

        Raises:
            MigrationLoadError: If module loading fails.
        """
        module_name = f"sqlspec_migration_{path.stem}"

        if module_name in sys.modules:
            sys.modules.pop(module_name, None)

        try:
            source_code = path.read_text(encoding="utf-8")
            compiled_code = compile(source_code, str(path), "exec")

            module = types.ModuleType(module_name)
            module.__file__ = str(path)

            sys.modules[module_name] = module

            exec(compiled_code, module.__dict__)  # noqa: S102

        except Exception as e:
            sys.modules.pop(module_name, None)
            msg = f"Failed to execute migration module {path}: {e}"
            raise MigrationLoadError(msg) from e

        return module

    def _normalize_and_validate_sql(self, sql: Any, migration_path: Path) -> list[str]:
        """Validate return type and normalize to list of strings.

        Args:
            sql: Return value from migration function.
            migration_path: Path to migration file for error messages.

        Returns:
            List of SQL statements.

        Raises:
            MigrationLoadError: If return type is invalid.
        """
        if isinstance(sql, str):
            stripped = sql.strip()
            return [stripped] if stripped else []
        if isinstance(sql, list):
            result = []
            for i, item in enumerate(sql):
                if not isinstance(item, str):
                    msg = (
                        f"Migration {migration_path} returned a list containing a non-string "
                        f"element at index {i} (type: {type(item).__name__})."
                    )
                    raise MigrationLoadError(msg)
                stripped_item = item.strip()
                if stripped_item:
                    result.append(stripped_item)
            return result

        msg = (
            f"Migration {migration_path} must return a 'str' or 'List[str]', but returned type '{type(sql).__name__}'."
        )
        raise MigrationLoadError(msg)


def get_migration_loader(
    file_path: Path, migrations_dir: Path, project_root: "Optional[Path]" = None
) -> BaseMigrationLoader:
    """Factory function to get appropriate loader for migration file.

    Args:
        file_path: Path to the migration file.
        migrations_dir: Directory containing migration files.
        project_root: Optional project root directory for Python imports.

    Returns:
        Appropriate loader instance for the file type.

    Raises:
        MigrationLoadError: If file type is not supported.
    """
    suffix = file_path.suffix

    if suffix == ".py":
        return PythonFileLoader(migrations_dir, project_root)
    if suffix == ".sql":
        return SQLFileLoader()
    msg = f"Unsupported migration file type: {suffix}"
    raise MigrationLoadError(msg)
