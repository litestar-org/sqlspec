"""Spanner event queue store with GoogleSQL-optimized DDL.

Spanner requires:
    - STRING instead of VARCHAR
    - INT64 instead of INTEGER
    - No DEFAULT clause for non-computed columns
    - Separate index creation statements (no IF NOT EXISTS)
    - PRIMARY KEY declared inline in CREATE TABLE
"""

import logging
from typing import TYPE_CHECKING

from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.core import execute_ddl_async, execute_ddl_sync
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.utils.logging import get_logger, log_with_context

__all__ = ("SpannerAsyncEventQueueStore", "SpannerSyncEventQueueStore")

logger = get_logger("sqlspec.adapters.spanner.events.store")


class _SpannerEventStoreMixin:
    """GoogleSQL DDL hooks for sync and async Spanner event queue stores."""

    __slots__ = ()

    if TYPE_CHECKING:

        @property
        def table_name(self) -> str: ...

        def _index_name(self) -> str: ...

    def _column_types(self) -> "tuple[str, str, str]":
        """Return Spanner-specific column types."""
        return "JSON", "JSON", "TIMESTAMP"

    def _string_type(self, length: int) -> str:
        """Return Spanner STRING(N) type syntax."""
        return f"STRING({length})"

    def _integer_type(self) -> str:
        """Return Spanner INT64 type."""
        return "INT64"

    def _primary_key_syntax(self) -> str:
        """Return Spanner inline PRIMARY KEY clause."""
        return " PRIMARY KEY (event_id)"

    def _table_ddl(self) -> str:
        """Build Spanner CREATE TABLE with PRIMARY KEY inline.

        Spanner does not support DEFAULT clauses on non-computed columns, so
        the DDL has none; values are provided at insert time.
        """
        payload_type, metadata_type, timestamp_type = self._column_types()
        string_64 = self._string_type(64)
        string_128 = self._string_type(128)
        string_32 = self._string_type(32)
        integer_type = self._integer_type()
        pk_inline = self._primary_key_syntax()

        return f"CREATE TABLE {self.table_name} (event_id {string_64} NOT NULL, channel {string_128} NOT NULL, payload_json {payload_type} NOT NULL, metadata_json {metadata_type}, status {string_32} NOT NULL, available_at {timestamp_type} NOT NULL, lease_expires_at {timestamp_type}, attempts {integer_type} NOT NULL, created_at {timestamp_type} NOT NULL, acknowledged_at {timestamp_type}){pk_inline}"

    def _index_ddl(self) -> "str | None":
        """Build Spanner secondary index for queue operations."""
        return f"CREATE INDEX {self._index_name()} ON {self.table_name}(channel, status, available_at)"

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Return statement unchanged because Spanner does not support IF NOT EXISTS."""
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        """Return statement unchanged because Spanner does not support IF EXISTS."""
        return statement

    def create_statements(self) -> "list[str]":
        """Return separate statements for table and index creation.

        Spanner requires DDL statements to be executed individually.
        The caller should handle errors for already-existing objects.
        """
        statements = [self._table_ddl()]
        index_sql = self._index_ddl()
        if index_sql:
            statements.append(index_sql)
        return statements

    def drop_statements(self) -> "list[str]":
        """Return drop statements in reverse dependency order.

        Spanner requires index to be dropped before the table.
        The caller should handle errors for non-existent objects.
        """
        return [f"DROP INDEX {self._index_name()}", f"DROP TABLE {self.table_name}"]

    def _log_ddl(self, event: str, statement_count: int) -> None:
        log_with_context(
            logger,
            logging.DEBUG,
            event,
            adapter_name="spanner",
            table_name=self.table_name,
            statement_count=statement_count,
        )


class SpannerSyncEventQueueStore(_SpannerEventStoreMixin, BaseEventQueueStore[SpannerSyncConfig]):
    """Spanner event queue store for synchronous configs.

    Spanner does not support IF NOT EXISTS, so statements must be executed
    with proper error handling for existing objects.

    Args:
        config: SpannerSyncConfig with extension_config["events"] settings.
    """

    __slots__ = ()

    def create_table(self) -> None:
        """Create the event queue table and index through ``Database.update_ddl``.

        Raises:
            TypeError: If the store's config is not a SpannerSyncConfig.
        """
        config = self._config
        if not isinstance(config, SpannerSyncConfig):
            msg = "create_table requires SpannerSyncConfig"
            raise TypeError(msg)
        statements = self.create_statements()
        self._log_ddl("events.queue.create", len(statements))
        execute_ddl_sync(config.get_database(), statements)

    def drop_table(self) -> None:
        """Drop the event queue table and index through ``Database.update_ddl``.

        Raises:
            TypeError: If the store's config is not a SpannerSyncConfig.
        """
        config = self._config
        if not isinstance(config, SpannerSyncConfig):
            msg = "drop_table requires SpannerSyncConfig"
            raise TypeError(msg)
        statements = self.drop_statements()
        self._log_ddl("events.queue.drop", len(statements))
        execute_ddl_sync(config.get_database(), statements)


class SpannerAsyncEventQueueStore(_SpannerEventStoreMixin, BaseEventQueueStore[SpannerAsyncConfig]):
    """Spanner event queue store for asynchronous configs.

    Spanner does not support IF NOT EXISTS, so statements must be executed
    with proper error handling for existing objects.

    Args:
        config: SpannerAsyncConfig with extension_config["events"] settings.
    """

    __slots__ = ()

    async def create_table(self) -> None:
        """Create the event queue table and index through ``Database.update_ddl``.

        Raises:
            TypeError: If the store's config is not a SpannerAsyncConfig.
        """
        config = self._config
        if not isinstance(config, SpannerAsyncConfig):
            msg = "create_table requires SpannerAsyncConfig"
            raise TypeError(msg)
        statements = self.create_statements()
        self._log_ddl("events.queue.create", len(statements))
        await execute_ddl_async(await config.get_database(), statements)

    async def drop_table(self) -> None:
        """Drop the event queue table and index through ``Database.update_ddl``.

        Raises:
            TypeError: If the store's config is not a SpannerAsyncConfig.
        """
        config = self._config
        if not isinstance(config, SpannerAsyncConfig):
            msg = "drop_table requires SpannerAsyncConfig"
            raise TypeError(msg)
        statements = self.drop_statements()
        self._log_ddl("events.queue.drop", len(statements))
        await execute_ddl_async(await config.get_database(), statements)
