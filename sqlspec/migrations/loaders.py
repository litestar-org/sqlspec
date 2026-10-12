"""Migration loader implementations for SQLSpec.

This module provides loader classes for different migration file formats.
"""

import abc
import ast
import inspect
import sys
import types
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any, Final, cast

from sqlspec.loader import SQLFileLoader as CoreSQLFileLoader
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.version import extract_migration_version
from sqlspec.utils.sync_tools import await_

__all__ = ("BaseMigrationLoader", "MigrationLoadError", "PythonFileLoader", "SQLFileLoader", "get_migration_loader")

PROJECT_ROOT_MARKERS: Final[list[str]] = ["pyproject.toml", ".git", "setup.cfg", "setup.py"]


class MigrationLoadError(Exception):
    """Exception raised when migration loading fails."""


class BaseMigrationLoader:
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
    async def get_down_sql(self, path: Path) -> "list[str] | None":
        """Load and return the 'down' SQL statements from a migration file.

        Args:
            path: Path to the migration file.

        Returns:
            List of SQL statements to execute for downgrade, which may be empty.
            None if the migration has no downgrade direction.

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

    def has_downgrade(self, path: Path) -> bool:
        """Return whether the migration file defines a downgrade direction.

        Args:
            path: Path to the migration file.

        Returns:
            True when the migration defines a downgrade direction.
        """
        return False


class SQLFileLoader(BaseMigrationLoader):
    """Loader for SQL migration files."""

    __slots__ = ("sql_loader",)

    def __init__(self, sql_loader: "CoreSQLFileLoader | None" = None) -> None:
        """Initialize SQL file loader.

        Args:
            sql_loader: Optional shared SQLFileLoader instance to reuse.
                If not provided, creates a new instance.
        """
        self.sql_loader: CoreSQLFileLoader = sql_loader if sql_loader is not None else CoreSQLFileLoader()

    async def get_up_sql(self, path: Path) -> list[str]:
        """Extract the 'up' SQL from a SQL migration file.

        The SQL file must already be loaded via validate_migration_file()
        before calling this method. This design ensures the file is loaded
        exactly once during the migration process.

        Args:
            path: Path to SQL migration file.

        Returns:
            List containing single SQL statement for upgrade.

        Raises:
            MigrationLoadError: If migration file is invalid or missing up query.
        """
        version = self._extract_version(path.name)
        up_query = f"migrate-{version}-up"

        if not self.sql_loader.has_query(up_query):
            msg = f"Migration {path} missing 'up' query: {up_query}"
            raise MigrationLoadError(msg)

        sql_obj = self.sql_loader.get_sql(up_query)
        return [sql_obj.raw_sql]

    async def get_down_sql(self, path: Path) -> "list[str] | None":
        """Extract the 'down' SQL from a SQL migration file.

        The SQL file must already be loaded via validate_migration_file()
        before calling this method. This design ensures the file is loaded
        exactly once during the migration process.

        Args:
            path: Path to SQL migration file.

        Returns:
            List containing single SQL statement for downgrade, or None when the
            file has no down block.
        """
        version = self._extract_version(path.name)
        down_query = f"migrate-{version}-down"

        if not self.sql_loader.has_query(down_query):
            return None

        sql_obj = self.sql_loader.get_sql(down_query)
        return [sql_obj.raw_sql]

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

        self.sql_loader.load_sql(path)
        up_query = f"migrate-{version}-up"
        if not self.sql_loader.has_query(up_query):
            msg = f"Migration {path} missing required 'up' query: {up_query}"
            raise MigrationLoadError(msg)

    def has_downgrade(self, path: Path) -> bool:
        """Return whether the SQL migration file defines a downgrade query."""
        version = self._extract_version(path.name)
        return bool(version) and self.sql_loader.has_query(f"migrate-{version}-down")

    def _extract_version(self, filename: str) -> str:
        """Extract version from filename.

        Supports sequential (0001), timestamp (20251011120000), and extension-prefixed
        (ext_litestar_0001) version formats.

        Args:
            filename: Migration filename to parse.

        Returns:
            Version string or empty string if invalid.
        """
        return extract_migration_version(filename) or ""


class PythonFileLoader(BaseMigrationLoader):
    """Loader for Python migration files."""

    __slots__ = ("_module_cache", "context", "migrations_dir", "project_root")

    def __init__(self, migrations_dir: Path, project_root: "Path | None" = None, context: "Any | None" = None) -> None:
        """Initialize Python file loader.

        Args:
            migrations_dir: Directory containing migration files.
            project_root: Optional project root directory for imports.
            context: Optional migration context to pass to functions.
        """
        self.migrations_dir = migrations_dir
        self.project_root = project_root if project_root is not None else self._find_project_root(migrations_dir)
        self.context = context
        self._module_cache: dict[str, tuple[int, int, types.ModuleType]] = {}

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

            upgrade_func = _get_callable_attr(module, "up")
            if upgrade_func is None:
                upgrade_func = _get_callable_attr(module, "migrate_up")

            if upgrade_func is None:
                msg = f"No upgrade function found in {path}. Expected 'up()' or 'migrate_up()'"
                raise MigrationLoadError(msg)

            if isinstance(self.context, MigrationContext):
                self.context.validate_async_usage(upgrade_func)
            pass_context = _callable_accepts_context(upgrade_func, self.context)

            if inspect.iscoroutinefunction(upgrade_func):
                sql_result = await upgrade_func(self.context) if pass_context else await upgrade_func()
            else:
                sql_result = upgrade_func(self.context) if pass_context else upgrade_func()

            return self._normalize_and_validate_sql(sql_result, path)

    async def get_down_sql(self, path: Path) -> "list[str] | None":
        """Load Python migration and execute downgrade function.

        Args:
            path: Path to Python migration file.

        Returns:
            List of SQL statements for downgrade, which may be empty. None when the
            module defines no downgrade function or returns None.
        """
        with self._temporary_project_path():
            module = self._load_module_from_path(path)

            downgrade_func = _get_callable_attr(module, "down")
            if downgrade_func is None:
                downgrade_func = _get_callable_attr(module, "migrate_down")

            if downgrade_func is None:
                return None

            if isinstance(self.context, MigrationContext):
                self.context.validate_async_usage(downgrade_func)
            pass_context = _callable_accepts_context(downgrade_func, self.context)

            if inspect.iscoroutinefunction(downgrade_func):
                sql_result = await downgrade_func(self.context) if pass_context else await downgrade_func()
            else:
                sql_result = downgrade_func(self.context) if pass_context else downgrade_func()

            if sql_result is None:
                return None

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

            upgrade_func = _get_callable_attr(module, "up")
            if upgrade_func is None:
                upgrade_func = _get_callable_attr(module, "migrate_up")

            if upgrade_func is None:
                msg = f"Migration {path} missing required upgrade function. Expected 'up()' or 'migrate_up()'"
                raise MigrationLoadError(msg)

    def has_downgrade(self, path: Path) -> bool:
        """Return whether the Python migration defines a downgrade function without executing it."""
        with self._temporary_project_path():
            module = self._load_module_from_path(path)
            func_name = "down" if _get_callable_attr(module, "down") is not None else None
            if func_name is None and _get_callable_attr(module, "migrate_down") is not None:
                func_name = "migrate_down"
            if func_name is None:
                return False
            return not _is_literal_none_return_function(path, func_name)

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
            with suppress(ValueError):
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
        cache_key = str(path)
        module_name = f"sqlspec_migration_{path.stem}"
        try:
            stat_result = path.stat()
        except OSError as e:
            msg = f"Failed to execute migration module {path}: {e}"
            raise MigrationLoadError(msg) from e

        cached = self._module_cache.get(cache_key)
        if cached is not None and cached[0] == stat_result.st_mtime_ns and cached[1] == stat_result.st_size:
            sys.modules[module_name] = cached[2]
            return cached[2]

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

        self._module_cache[cache_key] = (stat_result.st_mtime_ns, stat_result.st_size, module)
        return module

    def _normalize_and_validate_sql(self, sql: Any, migration_path: Path) -> list[str]:
        """Validate and normalize SQL return value to list of strings.

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
        if isinstance(sql, Iterable) and not isinstance(sql, (str, bytes)):
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
    file_path: Path,
    migrations_dir: Path,
    project_root: "Path | None" = None,
    context: "Any | None" = None,
    sql_loader: "CoreSQLFileLoader | None" = None,
) -> BaseMigrationLoader:
    """Factory function to get appropriate loader for migration file.

    Args:
        file_path: Path to the migration file.
        migrations_dir: Directory containing migration files.
        project_root: Optional project root directory for Python imports.
        context: Optional migration context to pass to Python migrations.
        sql_loader: Optional shared SQLFileLoader instance for SQL migrations.
            When provided, SQL files are loaded using this shared instance,
            avoiding redundant file parsing.

    Returns:
        Appropriate loader instance for the file type.

    Raises:
        MigrationLoadError: If file type is not supported.
    """
    suffix = file_path.suffix

    if suffix == ".py":
        return PythonFileLoader(migrations_dir, project_root, context)
    if suffix == ".sql":
        return SQLFileLoader(sql_loader)
    msg = f"Unsupported migration file type: {suffix}"
    raise MigrationLoadError(msg)


def _get_callable_attr(module: types.ModuleType, name: str) -> "Callable[..., Any] | None":
    """Get a callable attribute from a module if it exists.

    Args:
        module: The module to check.
        name: The attribute name to look for.

    Returns:
        The callable if it exists and is callable, None otherwise.
    """
    attr = module.__dict__.get(name)
    if attr is None:
        return None
    if callable(attr):
        return cast("Callable[..., Any]", attr)
    return None


def _callable_accepts_context(func: "Callable[..., Any]", context: Any) -> bool:
    """Return whether the migration function should be invoked with ``context``."""
    sig = inspect.signature(func)
    positional_params = [
        p
        for p in sig.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    has_var_positional = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
    if not positional_params:
        return bool(has_var_positional and context is not None)
    first_param = positional_params[0]
    if first_param.default is inspect.Parameter.empty:
        return True
    return context is not None


def _is_literal_none_return_function(path: Path, func_name: str) -> bool:
    """Return True when a top-level function body only returns None."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return False
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name:
            body = list(node.body)
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                body = body[1:]
            if len(body) == 1 and isinstance(body[0], ast.Return):
                ret_val = body[0].value
                return ret_val is None or (isinstance(ret_val, ast.Constant) and ret_val.value is None)
            return False
    return False


def _load_migration_sql(loader: BaseMigrationLoader, path: Path, direction: str) -> "list[str] | None":
    """Bridge the async loader contract for synchronous migration consumers."""
    method = loader.get_up_sql if direction == "up" else loader.get_down_sql
    if inspect.iscoroutinefunction(method):
        return await_(method, raise_sync_error=False)(path)
    sync_method = cast("Callable[[Path], list[str] | None]", method)
    return sync_method(path)
