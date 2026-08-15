"""Base store classes for ADK memory backend (sync and async)."""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Final, Generic, Literal, TypeVar, cast

from sqlspec.extensions.adk._config_utils import _adk_memory_store_config, _ADKMemoryStoreConfig
from sqlspec.extensions.adk._table_utils import owner_id_column_name, unique_statements
from sqlspec.extensions.adk.store import _reconcile_adk_schema_sync
from sqlspec.migrations.schema import SchemaTarget, ensure_schema_async
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.extensions.adk.memory._types import StoredMemory

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

    def _store_config_from_extension(self) -> _ADKMemoryStoreConfig:
        return _adk_memory_store_config(self._config)

    @property
    def config(self) -> ConfigT:
        """Return the database configuration."""
        return self._config

    @property
    def is_enabled(self) -> bool:
        """Return whether memory storage is enabled."""
        return self._enabled

    @property
    def memory_table(self) -> str:
        """Return the configured memory table name."""
        return self._memory_table

    @property
    def use_fts(self) -> bool:
        """Return whether full-text search is enabled."""
        return self._use_fts

    @property
    def max_results(self) -> int:
        """Return the default maximum results for search."""
        return self._max_results

    @property
    def owner_id_column_ddl(self) -> str | None:
        """Return the configured owner column DDL snippet, if any."""
        return self._owner_id_column_ddl

    @property
    def owner_id_column_name(self) -> str | None:
        """Return the extracted owner column name, if configured."""
        return self._owner_id_column_name

    def _schema_management_flags(self) -> tuple[bool, bool]:
        extension_config = getattr(self._config, "extension_config", {})
        adk_config = extension_config.get("adk", {}) if isinstance(extension_config, dict) else {}
        manage_schema = adk_config.get("manage_schema", True) if isinstance(adk_config, dict) else True
        create_schema = adk_config.get("create_schema", True) if isinstance(adk_config, dict) else True
        return bool(manage_schema), bool(create_schema)

    @property
    def create_schema_enabled(self) -> bool:
        """Return whether adapter-level table creation should run."""
        manage_schema, create_schema = self._schema_management_flags()
        return manage_schema and create_schema

    def _drop_sql_for_table(self, table_name: str) -> list[str]:
        current_table = self._memory_table
        self._memory_table = table_name
        try:
            return list(self._drop_memory_table_sql())
        finally:
            self._memory_table = current_table

    def _reset_drop_memory_table_sql(self) -> list[str]:
        configured = self._memory_table
        candidates = (configured, *[name for name in ADK_RESET_MEMORY_TABLES if name != configured])
        statements: list[str] = []
        for cand in candidates:
            statements.extend(self._drop_sql_for_table(cand))
        return unique_statements(statements)

    def _require_enabled(self) -> None:
        if not self._enabled:
            msg = "ADK memory store is disabled for this database configuration"
            raise RuntimeError(msg)

    def _effective_limit(self, limit: int | None) -> int:
        return limit if limit is not None else self._max_results

    def _log_operation(self, event: str, **kwargs: Any) -> None:
        log_with_context(
            logger,
            logging.DEBUG,
            event,
            table_name=self._memory_table,
            db_system=resolve_db_system(type(self).__name__),
            **kwargs,
        )


class BaseAsyncADKMemoryStore(_ADKMemoryStoreCommon[ConfigT], ABC):
    """Base class for async SQLSpec-backed ADK memory stores.

    Implements storage operations for Google ADK memory entries using
    SQLSpec database adapters with async/await.
    """

    __slots__ = ()

    @abstractmethod
    async def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist.

        Should check self._enabled and skip table creation if False.
        """
        raise NotImplementedError

    async def drop_tables(self) -> None:
        """Drop the memory table and indexes if they exist.

        Should drop all dialect-specific objects (tables, indexes, FTS virtual tables, triggers).
        """
        statements = self._drop_memory_table_sql()
        session_context = self._config.provide_session()
        async with cast("Any", session_context) as driver:
            for statement in statements:
                await driver.execute(statement)

    async def ensure_tables(self) -> None:
        """Create tables and emit a standardized log entry."""
        if not self._enabled:
            log_with_context(
                logger,
                logging.DEBUG,
                "adk.memory.table.skipped",
                memory_table=self._memory_table,
                reason="disabled",
                db_system=resolve_db_system(type(self).__name__),
            )
            return

        manage_schema, _create_schema = self._schema_management_flags()
        if self.create_schema_enabled:
            await self.create_tables()
        if manage_schema:
            await self.reconcile_schema(assume_existing=self.create_schema_enabled)
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.memory.table.ready",
            memory_table=self._memory_table,
            db_system=resolve_db_system(type(self).__name__),
        )

    async def reconcile_schema(self, *, assume_existing: bool = False) -> None:
        """Apply additive ADK memory table changes from canonical adapter DDL."""
        manage_schema, create_schema = self._schema_management_flags()
        if not manage_schema or not self._enabled:
            return
        statement_config = getattr(self._config, "statement_config", None)
        dialect = getattr(statement_config, "dialect", None)
        ddl = await self._memory_table_ddl()
        ddl_str = ddl if isinstance(ddl, str) else ";\n".join(ddl)
        target = SchemaTarget.from_ddl(self._memory_table, ddl_str, dialect=dialect)
        session_context = self._config.provide_session()
        if hasattr(session_context, "__aenter__"):
            async with cast("Any", session_context) as driver:
                await ensure_schema_async(
                    driver, [target], manage_schema=True, create_schema=create_schema, assume_existing=assume_existing
                )
            return
        await async_(_reconcile_adk_schema_sync)(
            self._config, [target], create_schema=create_schema, assume_existing=assume_existing
        )

    @abstractmethod
    async def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication.

        Args:
            entries: List of stored memory records to insert.
            owner_id: Optional owner ID value for owner_id_column (if configured).

        Returns:
            Number of entries actually inserted (excludes duplicates).
        """
        raise NotImplementedError

    @abstractmethod
    async def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: "int | None" = None,
        scope_filter: Literal["all", "user", "app"] = "all",
    ) -> "list[StoredMemory]":
        """Search memory entries by text query.

        Args:
            query: Text query to search for.
            app_name: Application name to filter by.
            user_id: User ID to filter by.
            limit: Maximum number of results (defaults to max_results config).
            scope_filter: Scope filter ('all', 'user', 'app').

        Returns:
            List of matching memory records ordered by relevance/timestamp.
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
    async def delete_entries_older_than(
        self, days: int, app_name: "str | None" = None, scope: "str | None" = None
    ) -> int:
        """Delete memory entries older than specified days.

        Args:
            days: Number of days to retain entries.
            app_name: Optional application name to scope deletion.
            scope: Optional scope ('user' or 'app') to scope deletion.

        Returns:
            Number of entries deleted.
        """
        raise NotImplementedError

    @abstractmethod
    async def _memory_table_ddl(self) -> "str | list[str]":
        """Get the CREATE TABLE SQL for the memory table."""
        raise NotImplementedError

    @abstractmethod
    def _drop_memory_table_sql(self) -> "list[str]":
        """Get the DROP TABLE SQL statements for this database dialect."""
        raise NotImplementedError


class BaseSyncADKMemoryStore(_ADKMemoryStoreCommon[ConfigT], ABC):
    """Base class for sync SQLSpec-backed ADK memory stores."""

    __slots__ = ()

    @abstractmethod
    def create_tables(self) -> None:
        """Create the memory table and indexes if they don't exist."""
        raise NotImplementedError

    def drop_tables(self) -> None:
        """Drop the memory table and indexes if they exist."""
        statements = self._drop_memory_table_sql()
        with cast("Any", self._config.provide_session()) as driver:
            for statement in statements:
                driver.execute(statement)

    def ensure_tables(self) -> None:
        """Create tables and emit a standardized log entry."""
        if not self._enabled:
            log_with_context(
                logger,
                logging.DEBUG,
                "adk.memory.table.skipped",
                memory_table=self._memory_table,
                reason="disabled",
                db_system=resolve_db_system(type(self).__name__),
            )
            return

        manage_schema, _create_schema = self._schema_management_flags()
        if self.create_schema_enabled:
            self.create_tables()
        if manage_schema:
            self.reconcile_schema(assume_existing=self.create_schema_enabled)
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.memory.table.ready",
            memory_table=self._memory_table,
            db_system=resolve_db_system(type(self).__name__),
        )

    def reconcile_schema(self, *, assume_existing: bool = False) -> None:
        """Apply additive ADK memory table changes from canonical adapter DDL."""
        manage_schema, create_schema = self._schema_management_flags()
        if not manage_schema or not self._enabled:
            return
        statement_config = getattr(self._config, "statement_config", None)
        dialect = getattr(statement_config, "dialect", None)
        ddl = self._memory_table_ddl()
        ddl_str = ddl if isinstance(ddl, str) else ";\n".join(ddl)
        target = SchemaTarget.from_ddl(self._memory_table, ddl_str, dialect=dialect)
        _reconcile_adk_schema_sync(self._config, [target], create_schema=create_schema, assume_existing=assume_existing)

    @abstractmethod
    def insert_memory_entries(self, entries: "list[StoredMemory]", owner_id: "object | None" = None) -> int:
        """Bulk insert memory entries with deduplication."""
        raise NotImplementedError

    @abstractmethod
    def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: "int | None" = None,
        scope_filter: Literal["all", "user", "app"] = "all",
    ) -> "list[StoredMemory]":
        """Search memory entries by text query."""
        raise NotImplementedError

    @abstractmethod
    def delete_entries_by_session(self, session_id: str) -> int:
        """Delete all memory entries for a specific session."""
        raise NotImplementedError

    @abstractmethod
    def delete_entries_older_than(self, days: int, app_name: "str | None" = None, scope: "str | None" = None) -> int:
        """Delete memory entries older than specified days."""
        raise NotImplementedError

    @abstractmethod
    def _memory_table_ddl(self) -> "str | list[str]":
        """Get the CREATE TABLE SQL for the memory table."""
        raise NotImplementedError

    @abstractmethod
    def _drop_memory_table_sql(self) -> "list[str]":
        """Get the DROP TABLE SQL statements for this database dialect."""
        raise NotImplementedError
