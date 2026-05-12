from importlib import resources
from importlib.resources import as_file
from typing import TYPE_CHECKING

from sqlspec.data_dictionary._registry import get_dialect_config
from sqlspec.exceptions import SQLFileNotFoundError
from sqlspec.loader import SQLFileLoader

if TYPE_CHECKING:
    from importlib.abc import Traversable

    from sqlspec.core.statement import SQL
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = ("DataDictionaryLoader", "get_data_dictionary_loader")


SQL_RESOURCE_PACKAGE = "sqlspec.data_dictionary"
SQL_RESOURCE_NAME = "sql"


def _sql_resource_root() -> "Traversable":
    return resources.files(SQL_RESOURCE_PACKAGE).joinpath(SQL_RESOURCE_NAME)


class DataDictionaryLoader:
    """Loads and manages data dictionary SQL for all dialects."""

    __slots__ = ("_loaded_dialects", "_sql_loaders")

    def __init__(self) -> None:
        """Initialize the data dictionary loader."""
        self._sql_loaders: dict[str, SQLFileLoader] = {}
        self._loaded_dialects: set[str] = set()

    def _get_loader(self, dialect: str) -> "SQLFileLoader":
        """Return or create a SQL loader for a dialect.

        Args:
            dialect: Dialect name.

        Returns:
            SQLFileLoader instance.
        """
        loader = self._sql_loaders.get(dialect)
        if loader is None:
            loader = SQLFileLoader()
            self._sql_loaders[dialect] = loader
        return loader

    def _ensure_dialect_loaded(self, dialect: str) -> None:
        """Lazy load SQL files for a dialect.

        Args:
            dialect: Dialect name.

        Raises:
            SQLFileNotFoundError: When the dialect SQL directory is missing.
        """
        if dialect in self._loaded_dialects:
            return
        dialect_resource = _sql_resource_root().joinpath(dialect)
        if not dialect_resource.is_dir():
            raise SQLFileNotFoundError(str(dialect_resource))
        loader = self._get_loader(dialect)
        with as_file(dialect_resource) as dialect_path:
            loader.load_sql(dialect_path)
        self._loaded_dialects.add(dialect)

    def get_query(self, dialect: str, query_name: str) -> "SQL":
        """Get SQL query for a specific dialect and operation.

        Args:
            dialect: Dialect name.
            query_name: Query name to fetch.

        Returns:
            SQL object for the named query.
        """
        self._ensure_dialect_loaded(dialect)
        loader = self._get_loader(dialect)
        return loader.get_sql(query_name)

    def get_query_text(self, dialect: str, query_name: str) -> str:
        """Get raw SQL text for a specific dialect and operation.

        Args:
            dialect: Dialect name.
            query_name: Query name to fetch.

        Returns:
            Raw SQL text for the named query.
        """
        self._ensure_dialect_loaded(dialect)
        loader = self._get_loader(dialect)
        return loader.get_query_text(query_name)

    def get_dialect_config(self, dialect: str) -> "DialectConfig":
        """Get static configuration for a dialect.

        Args:
            dialect: Dialect name.

        Returns:
            DialectConfig for the dialect.
        """
        return get_dialect_config(dialect)

    def list_dialects(self) -> "list[str]":
        """List available SQL dialects.

        Returns:
            List of dialect names with SQL directories.
        """
        sql_root = _sql_resource_root()
        if not sql_root.is_dir():
            return []
        return sorted([path.name for path in sql_root.iterdir() if path.is_dir()])


_loader_instance: DataDictionaryLoader | None = None


def get_data_dictionary_loader() -> DataDictionaryLoader:
    """Get singleton data dictionary loader instance.

    Returns:
        DataDictionaryLoader singleton.
    """
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = DataDictionaryLoader()
    return _loader_instance
