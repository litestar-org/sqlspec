"""Base store classes for ADK memory backend (sync and async)."""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar

from sqlspec.extensions.adk._config_utils import _adk_memory_store_config, _ADKMemoryStoreConfig
from sqlspec.extensions.adk._table_utils import ensure_table_name, owner_id_column_name, reset_drop_sql
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.extensions.adk.memory._types import MemoryRecord

__all__ = ("BaseAsyncADKMemoryStore", "BaseSyncADKMemoryStore")

ConfigT = TypeVar("ConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")

logger = get_logger("sqlspec.extensions.adk.memory.store")


ADK_RESET_MEMORY_TABLES: Final = ("adk_memory", "adk_memory_entries")


class _ADKMemoryStoreCommon(Generic[ConfigT]):
    """Shared non-async ADK store state and helpers."""

    if TYPE_CHECKING:
        _drop_memory_table_sql: "Callable[[], list[str]]"

    __slots__ = (
        "_config",
        "_enabled",
        "_max_results",
        "_memory_table",
        "_owner_id_column_ddl",
        "_owner_id_column_name",
        "_use_fts",
    )

    def __init__(self, config: ConfigT) -> None:
        """Initialize the ADK memory store.

        Args:
            config: SQLSpec database configuration.
        """
        self._config = config
        store_config = self._store_config_from_extension()
        self._enabled: bool = store_config.get("enable_memory", True)
        self._memory_table: str = str(store_config["memory_table"])
        self._use_fts: bool = bool(store_config.get("use_fts", False))
        self._max_results: int = store_config.get("max_results", 20)
        self._owner_id_column_ddl: str | None = store_config.get("owner_id_column")
        self._owner_id_column_name: str | None = (
            owner_id_column_name(self._owner_id_column_ddl) if self._owner_id_column_ddl else None
        )
        ensure_table_name(self._memory_table)

    @property
    def config(self) -> ConfigT:
        """Return the database configuration."""
        return self._config

    @property
    def memory_table(self) -> str:
        """Return the memory table name."""
        return self._memory_table

    @property
    def enabled(self) -> bool:
        """Return whether memory store is enabled."""
        return self._enabled

    @property
    def use_fts(self) -> bool:
        """Return whether full-text search is enabled."""
        return self._use_fts

    @property
    def max_results(self) -> int:
        """Return the max search results limit."""
        return self._max_results

    @property
    def owner_id_column_ddl(self) -> "str | None":
        """Return the full owner ID column DDL (or None if not configured)."""
        return self._owner_id_column_ddl

    @property
    def owner_id_column_name(self) -> "str | None":
        """Return the owner ID column name only (or None if not configured)."""
        return self._owner_id_column_name

    def _store_config_from_extension(self) -> "_ADKMemoryStoreConfig":
        """Extract ADK memory configuration from config.extension_config.

        Returns:
            Dict with memory_table, use_fts, max_results, and optionally owner_id_column.
        """
        return _adk_memory_store_config(self._config)

    def _reset_drop_memory_table_sql(self) -> "list[str]":
        """Return memory drops needed before recreating the clean-break schema."""
        return reset_drop_sql(
            list(self._drop_memory_table_sql()), ADK_RESET_MEMORY_TABLES, self._drop_memory_sql_for_table
        )

    def _drop_memory_sql_for_table(self, table_name: str) -> "list[str]":
        current_table = self._memory_table
        self._memory_table = table_name
        try:
            return list(self._drop_memory_table_sql())
        finally:
            self._memory_table = current_table

    def _log_memory_table_created(self) -> None:
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.memory.table.ready",
            db_system=resolve_db_system(type(self).__name__),
            memory_table=self._memory_table,
        )

    def _log_memory_table_skipped(self) -> None:
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.memory.table.skipped",
            db_system=resolve_db_system(type(self).__name__),
            memory_table=self._memory_table,
            reason="disabled",
        )


class BaseAsyncADKMemoryStore(_ADKMemoryStoreCommon[ConfigT], ABC):
    """Base class for async SQLSpec-backed ADK memory stores.

    Implements storage operations for Google ADK memory entries using
    SQLSpec database adapters with async/await.

    This abstract base class provides common functionality for all database-specific
    memory store implementations including:
    - Connection management via SQLSpec configs
    - Table name validation
    - Memory entry CRUD operations
    - Text search with optional full-text search support

    Subclasses must implement dialect-specific SQL queries and will be created
    in each adapter directory.

    Args:
        config: SQLSpec database configuration with extension_config["adk"] settings.
    """

    __slots__ = ()

    @abstractmethod
    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist.

        Should check self._enabled and skip table creation if False.
        """
        raise NotImplementedError

    async def ensure_tables(self) -> None:
        """Create tables when enabled and emit a standardized log entry."""

        if not self._enabled:
            self._log_memory_table_skipped()
            return
        await self.create_tables()
        self._log_memory_table_created()

    @abstractmethod
    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication.

        Uses UPSERT pattern to skip duplicates based on event_id.

        Args:
            entries: List of memory records to insert.
            owner_id: Optional owner ID value for owner_id_column (if configured).

        Returns:
            Number of entries actually inserted (excludes duplicates).

        Raises:
            RuntimeError: If memory store is disabled.
        """
        raise NotImplementedError

    @abstractmethod
    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query.

        Uses the configured search strategy (simple ILIKE or FTS).

        Args:
            query: Text query to search for.
            app_name: Application name to filter by.
            user_id: User ID to filter by.
            limit: Maximum number of results (defaults to max_results config).

        Returns:
            List of matching memory records ordered by relevance/timestamp.

        Raises:
            RuntimeError: If memory store is disabled.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session.

        Args:
            session_id: Session ID to delete entries for.

        Returns:
            Number of entries deleted.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days.

        Used for TTL cleanup operations.

        Args:
            days: Number of days to retain entries.

        Returns:
            Number of entries deleted.
        """
        raise NotImplementedError

    @abstractmethod
    async def _memory_table_ddl(self) -> "str | list[str]":
        """Get the CREATE TABLE SQL for the memory table.

        Returns:
            SQL statement(s) to create the memory table with indexes.
        """
        raise NotImplementedError

    @abstractmethod
    def _drop_memory_table_sql(self) -> "list[str]":
        """Get the DROP TABLE SQL statements for this database dialect.

        Returns:
            List of SQL statements to drop the memory table and indexes.
        """
        raise NotImplementedError


class BaseSyncADKMemoryStore(_ADKMemoryStoreCommon[ConfigT], ABC):
    """Base class for sync SQLSpec-backed ADK memory stores.

    Implements storage operations for Google ADK memory entries using
    SQLSpec database adapters with synchronous execution.

    This abstract base class provides common functionality for sync database-specific
    memory store implementations including:
    - Connection management via SQLSpec configs
    - Table name validation
    - Memory entry CRUD operations
    - Text search with optional full-text search support

    Subclasses must implement dialect-specific SQL queries and will be created
    in each adapter directory.

    Args:
        config: SQLSpec database configuration with extension_config["adk"] settings.
    """

    __slots__ = ()

    @abstractmethod
    def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist.

        Should check self._enabled and skip table creation if False.
        """
        raise NotImplementedError

    def ensure_tables(self) -> None:
        """Create tables when enabled and emit a standardized log entry."""

        if not self._enabled:
            self._log_memory_table_skipped()
            return
        self.create_tables()
        self._log_memory_table_created()

    @abstractmethod
    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication.

        Uses UPSERT pattern to skip duplicates based on event_id.

        Args:
            entries: List of memory records to insert.
            owner_id: Optional owner ID value for owner_id_column (if configured).

        Returns:
            Number of entries actually inserted (excludes duplicates).

        Raises:
            RuntimeError: If memory store is disabled.
        """
        raise NotImplementedError

    @abstractmethod
    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        """Search memory entries by text query.

        Uses the configured search strategy (simple ILIKE or FTS).

        Args:
            query: Text query to search for.
            app_name: Application name to filter by.
            user_id: User ID to filter by.
            limit: Maximum number of results (defaults to max_results config).

        Returns:
            List of matching memory records ordered by relevance/timestamp.

        Raises:
            RuntimeError: If memory store is disabled.
        """
        raise NotImplementedError

    @abstractmethod
    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session.

        Args:
            session_id: Session ID to delete entries for.

        Returns:
            Number of entries deleted.
        """
        raise NotImplementedError

    @abstractmethod
    def delete_entries_older_than(self, days: int) -> int:
        """Delete memory entries older than specified days.

        Used for TTL cleanup operations.

        Args:
            days: Number of days to retain entries.

        Returns:
            Number of entries deleted.
        """
        raise NotImplementedError

    @abstractmethod
    def _memory_table_ddl(self) -> "str | list[str]":
        """Get the CREATE TABLE SQL for the memory table.

        Returns:
            SQL statement(s) to create the memory table with indexes.
        """
        raise NotImplementedError

    @abstractmethod
    def _drop_memory_table_sql(self) -> "list[str]":
        """Get the DROP TABLE SQL statements for this database dialect.

        Returns:
            List of SQL statements to drop the memory table and indexes.
        """
        raise NotImplementedError
