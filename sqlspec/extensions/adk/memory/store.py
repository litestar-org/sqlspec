"""Base store classes for ADK memory backend (sync and async)."""

import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar, cast

from typing_extensions import NotRequired, TypedDict

from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.config import ADKConfig, DatabaseConfigProtocol
    from sqlspec.extensions.adk.memory._types import MemoryRecord

ConfigT = TypeVar("ConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")

logger = get_logger("sqlspec.extensions.adk.memory.store")

__all__ = ("BaseAsyncADKMemoryStore", "BaseSyncADKMemoryStore")

VALID_TABLE_NAME_PATTERN: Final = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
COLUMN_NAME_PATTERN: Final = re.compile(r"^(\w+)")
MAX_TABLE_NAME_LENGTH: Final = 63


class _ADKMemoryStoreConfig(TypedDict):
    """Normalized ADK memory store configuration."""

    enable_memory: bool
    memory_table: str
    use_fts: bool
    max_results: int
    owner_id_column: NotRequired[str]


def _parse_owner_id_column(owner_id_column_ddl: str) -> str:
    """Extract column name from owner ID column DDL definition.

    Args:
        owner_id_column_ddl: Full column DDL string.

    Returns:
        Column name only (first word).

    Raises:
        ValueError: If DDL format is invalid.
    """
    match = COLUMN_NAME_PATTERN.match(owner_id_column_ddl.strip())
    if not match:
        msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Must start with column name."
        raise ValueError(msg)

    return match.group(1)


def _validate_table_name(table_name: str) -> None:
    """Validate table name for SQL safety.

    Args:
        table_name: Table name to validate.

    Raises:
        ValueError: If table name is invalid.
    """
    if not table_name:
        msg = "Table name cannot be empty"
        raise ValueError(msg)

    if len(table_name) > MAX_TABLE_NAME_LENGTH:
        msg = f"Table name too long: {len(table_name)} chars (max {MAX_TABLE_NAME_LENGTH})"
        raise ValueError(msg)

    if not VALID_TABLE_NAME_PATTERN.match(table_name):
        msg = (
            f"Invalid table name: {table_name!r}. "
            "Must start with letter/underscore and contain only alphanumeric characters and underscores"
        )
        raise ValueError(msg)


class BaseAsyncADKMemoryStore(ABC, Generic[ConfigT]):
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
    in each adapter directory (e.g., sqlspec/adapters/asyncpg/adk/store.py).

    Args:
        config: SQLSpec database configuration with extension_config["adk"] settings.

    Notes:
        Configuration is read from config.extension_config["adk"]:
        - memory_table: Memory table name (default: "adk_memory_entries")
        - memory_use_fts: Enable full-text search when supported (default: False)
        - memory_max_results: Max search results (default: 20)
        - owner_id_column: Optional owner FK column DDL (default: None)
        - enable_memory: Whether memory is enabled (default: True)
    """

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

        Notes:
            Reads configuration from config.extension_config["adk"]:
            - memory_table: Memory table name (default: "adk_memory_entries")
            - memory_use_fts: Enable full-text search when supported (default: False)
            - memory_max_results: Max search results (default: 20)
            - owner_id_column: Optional owner FK column DDL (default: None)
            - enable_memory: Whether memory is enabled (default: True)
        """
        self._config = config
        store_config = self._get_store_config_from_extension()
        self._enabled: bool = store_config.get("enable_memory", True)
        self._memory_table: str = str(store_config["memory_table"])
        self._use_fts: bool = bool(store_config.get("use_fts", False))
        self._max_results: int = store_config.get("max_results", 20)
        self._owner_id_column_ddl: str | None = store_config.get("owner_id_column")
        self._owner_id_column_name: str | None = (
            _parse_owner_id_column(self._owner_id_column_ddl) if self._owner_id_column_ddl else None
        )
        _validate_table_name(self._memory_table)

    def _get_store_config_from_extension(self) -> "_ADKMemoryStoreConfig":
        """Extract ADK memory configuration from config.extension_config.

        Returns:
            Dict with memory_table, use_fts, max_results, and optionally owner_id_column.
        """
        extension_config = self._config.extension_config
        adk_config = cast("ADKConfig", extension_config.get("adk", {}))
        enable_memory = adk_config.get("enable_memory")
        memory_table = adk_config.get("memory_table")
        use_fts = adk_config.get("memory_use_fts")
        max_results = adk_config.get("memory_max_results")

        result: _ADKMemoryStoreConfig = {
            "enable_memory": bool(enable_memory) if enable_memory is not None else True,
            "memory_table": str(memory_table) if memory_table is not None else "adk_memory_entries",
            "use_fts": bool(use_fts) if use_fts is not None else False,
            "max_results": int(max_results) if isinstance(max_results, int) else 20,
        }

        owner_id = adk_config.get("owner_id_column")
        if owner_id is not None:
            result["owner_id_column"] = owner_id

        return result

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
    async def _get_create_memory_table_sql(self) -> "str | list[str]":
        """Get the CREATE TABLE SQL for the memory table.

        Returns:
            SQL statement(s) to create the memory table with indexes.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get the DROP TABLE SQL statements for this database dialect.

        Returns:
            List of SQL statements to drop the memory table and indexes.
        """
        raise NotImplementedError


class BaseSyncADKMemoryStore(ABC, Generic[ConfigT]):
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
    in each adapter directory (e.g., sqlspec/adapters/sqlite/adk/store.py).

    Args:
        config: SQLSpec database configuration with extension_config["adk"] settings.

    Notes:
        Configuration is read from config.extension_config["adk"]:
        - memory_table: Memory table name (default: "adk_memory_entries")
        - memory_use_fts: Enable full-text search when supported (default: False)
        - memory_max_results: Max search results (default: 20)
        - owner_id_column: Optional owner FK column DDL (default: None)
        - enable_memory: Whether memory is enabled (default: True)
    """

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
        """Initialize the sync ADK memory store.

        Args:
            config: SQLSpec database configuration.

        Notes:
            Reads configuration from config.extension_config["adk"]:
            - memory_table: Memory table name (default: "adk_memory_entries")
            - memory_use_fts: Enable full-text search when supported (default: False)
            - memory_max_results: Max search results (default: 20)
            - owner_id_column: Optional owner FK column DDL (default: None)
            - enable_memory: Whether memory is enabled (default: True)
        """
        self._config = config
        store_config = self._get_store_config_from_extension()
        self._enabled: bool = store_config.get("enable_memory", True)
        self._memory_table: str = str(store_config["memory_table"])
        self._use_fts: bool = bool(store_config.get("use_fts", False))
        self._max_results: int = store_config.get("max_results", 20)
        self._owner_id_column_ddl: str | None = store_config.get("owner_id_column")
        self._owner_id_column_name: str | None = (
            _parse_owner_id_column(self._owner_id_column_ddl) if self._owner_id_column_ddl else None
        )
        _validate_table_name(self._memory_table)

    def _get_store_config_from_extension(self) -> "_ADKMemoryStoreConfig":
        """Extract ADK memory configuration from config.extension_config.

        Returns:
            Dict with memory_table, use_fts, max_results, and optionally owner_id_column.
        """
        extension_config = self._config.extension_config
        adk_config = cast("ADKConfig", extension_config.get("adk", {}))
        enable_memory = adk_config.get("enable_memory")
        memory_table = adk_config.get("memory_table")
        use_fts = adk_config.get("memory_use_fts")
        max_results = adk_config.get("memory_max_results")

        result: _ADKMemoryStoreConfig = {
            "enable_memory": bool(enable_memory) if enable_memory is not None else True,
            "memory_table": str(memory_table) if memory_table is not None else "adk_memory_entries",
            "use_fts": bool(use_fts) if use_fts is not None else False,
            "max_results": int(max_results) if isinstance(max_results, int) else 20,
        }

        owner_id = adk_config.get("owner_id_column")
        if owner_id is not None:
            result["owner_id_column"] = owner_id

        return result

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

    def _log_memory_table_created(self) -> None:
        logger.debug(
            "ADK memory table ready",
            extra={"db.system": resolve_db_system(type(self).__name__), "memory_table": self._memory_table},
        )

    def _log_memory_table_skipped(self) -> None:
        logger.debug(
            "ADK memory table creation skipped",
            extra={
                "db.system": resolve_db_system(type(self).__name__),
                "memory_table": self._memory_table,
                "reason": "disabled",
            },
        )

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
    def _get_create_memory_table_sql(self) -> "str | list[str]":
        """Get the CREATE TABLE SQL for the memory table.

        Returns:
            SQL statement(s) to create the memory table with indexes.
        """
        raise NotImplementedError

    @abstractmethod
    def _get_drop_memory_table_sql(self) -> "list[str]":
        """Get the DROP TABLE SQL statements for this database dialect.

        Returns:
            List of SQL statements to drop the memory table and indexes.
        """
        raise NotImplementedError
