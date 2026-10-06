"""Spanner event queue store with GoogleSQL-optimized DDL.

Spanner requires:
    - STRING instead of VARCHAR
    - INT64 instead of INTEGER
    - No DEFAULT clause for non-computed columns
    - Separate index creation statements (no IF NOT EXISTS)
    - PRIMARY KEY declared inline in CREATE TABLE
"""

import inspect
import logging
from typing import Any, TypeVar

from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.utils.logging import get_logger, log_with_context

__all__ = ("SpannerAsyncEventQueueStore", "SpannerSyncEventQueueStore")

logger = get_logger("sqlspec.adapters.spanner.events.store")

ConfigT = TypeVar("ConfigT", SpannerSyncConfig, SpannerAsyncConfig)


class _SpannerEventQueueStoreMixin(BaseEventQueueStore[ConfigT]):
    """Shared GoogleSQL DDL generation for Spanner sync and async event queue stores."""

    __slots__ = ()

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

        Spanner does not support DEFAULT clauses on non-computed columns,
        so we omit them entirely. Values must be provided at insert time.
        """
        payload_type, metadata_type, timestamp_type = self._column_types()
        string_64 = self._string_type(64)
        string_128 = self._string_type(128)
        string_32 = self._string_type(32)
        integer_type = self._integer_type()
        pk_inline = self._primary_key_syntax()

        return f"CREATE TABLE {self.table_name} (event_id {string_64} NOT NULL, channel {string_128} NOT NULL, payload_json {payload_type} NOT NULL, metadata_json {metadata_type}, status {string_32} NOT NULL, available_at {timestamp_type} NOT NULL, lease_expires_at {timestamp_type}, attempts {integer_type} NOT NULL, created_at {timestamp_type} NOT NULL, acknowledged_at {timestamp_type}){pk_inline}"

    def _index_ddl(self) -> str | None:
        """Build Spanner secondary index for queue operations."""
        index_name = self._index_name()
        return f"CREATE INDEX {index_name} ON {self.table_name}(channel, status, available_at)"

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Return statement unchanged because Spanner does not support IF NOT EXISTS.

        Args:
            statement: The DDL statement.
            object_type: Type of object (table, index).

        Returns:
            The statement unchanged.
        """
        del object_type
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
        index_name = self._index_name()
        return [f"DROP INDEX {index_name}", f"DROP TABLE {self.table_name}"]


class SpannerSyncEventQueueStore(_SpannerEventQueueStoreMixin[SpannerSyncConfig]):
    """Spanner-specific synchronous event queue store with GoogleSQL DDL.

    Generates optimized DDL for Google Cloud Spanner using GoogleSQL dialect.
    Spanner does not support IF NOT EXISTS, so statements must be executed
    with proper error handling for existing objects.

    Args:
        config: SpannerSyncConfig with extension_config["events"] settings.
    """

    __slots__ = ()

    def create_table(self) -> None:
        """Create the event queue table and index.

        Executes DDL statements via database.update_ddl() which is the
        recommended approach for Spanner schema changes.
        """
        config = self._config
        if not isinstance(config, SpannerSyncConfig):
            msg = "create_table requires SpannerSyncConfig"
            raise TypeError(msg)

        database: Any = config.get_database()
        statements = self.create_statements()
        log_with_context(
            logger,
            logging.DEBUG,
            "events.queue.create",
            adapter_name="spanner",
            table_name=self.table_name,
            statement_count=len(statements),
        )
        operation: Any = database.update_ddl(statements)
        operation.result()

    def drop_table(self) -> None:
        """Drop the event queue table and index.

        Executes DDL statements via database.update_ddl() which is the
        recommended approach for Spanner schema changes.
        """
        config = self._config
        if not isinstance(config, SpannerSyncConfig):
            msg = "drop_table requires SpannerSyncConfig"
            raise TypeError(msg)

        database: Any = config.get_database()
        statements = self.drop_statements()
        log_with_context(
            logger,
            logging.DEBUG,
            "events.queue.drop",
            adapter_name="spanner",
            table_name=self.table_name,
            statement_count=len(statements),
        )
        operation: Any = database.update_ddl(statements)
        operation.result()


class SpannerAsyncEventQueueStore(_SpannerEventQueueStoreMixin[SpannerAsyncConfig]):
    """Async Spanner event queue store with native async DDL execution.

    Generates optimized DDL for Google Cloud Spanner using GoogleSQL dialect
    and executes schema updates via the async database client.

    Args:
        config: SpannerAsyncConfig with extension_config["events"] settings.
    """

    __slots__ = ()

    async def create_table(self) -> None:
        """Create the event queue table and index using Spanner async update_ddl."""
        config = self._config
        if not isinstance(config, SpannerAsyncConfig):
            msg = "create_table requires SpannerAsyncConfig"
            raise TypeError(msg)

        statements = self.create_statements()
        database: Any = await config.get_database()
        log_with_context(
            logger,
            logging.DEBUG,
            "events.queue.create",
            adapter_name="spanner",
            table_name=self.table_name,
            statement_count=len(statements),
        )
        operation: Any = database.update_ddl(statements)
        if inspect.isawaitable(operation):
            operation = await operation
        result = operation.result()
        if inspect.isawaitable(result):
            await result

    async def drop_table(self) -> None:
        """Drop the event queue table and index using Spanner async update_ddl."""
        config = self._config
        if not isinstance(config, SpannerAsyncConfig):
            msg = "drop_table requires SpannerAsyncConfig"
            raise TypeError(msg)

        statements = self.drop_statements()
        database: Any = await config.get_database()
        log_with_context(
            logger,
            logging.DEBUG,
            "events.queue.drop",
            adapter_name="spanner",
            table_name=self.table_name,
            statement_count=len(statements),
        )
        operation: Any = database.update_ddl(statements)
        if inspect.isawaitable(operation):
            operation = await operation
        result = operation.result()
        if inspect.isawaitable(result):
            await result
