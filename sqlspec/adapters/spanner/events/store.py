"""Spanner event queue store with GoogleSQL-optimized DDL.

Spanner requires:
    - STRING instead of VARCHAR
    - INT64 instead of INTEGER
    - No DEFAULT clause for non-computed columns
    - Separate index creation statements (no IF NOT EXISTS)
    - PRIMARY KEY declared inline in CREATE TABLE
"""

import logging
import math
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.core import (
    execute_ddl_async,
    execute_ddl_sync,
    list_existing_table_names_async,
    list_existing_table_names_sync,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.utils.logging import get_logger, log_with_context

__all__ = ("SpannerAsyncEventQueueStore", "SpannerSyncEventQueueStore")

logger = get_logger("sqlspec.adapters.spanner.events.store")


class _SpannerEventStoreMixin:
    """GoogleSQL and Spangres DDL hooks for sync and async Spanner event queue stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _config: Any
        _extension_settings: dict[str, Any]

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

    def _is_spangres(self) -> bool:
        """Return True when the statement dialect is Spangres (PostgreSQL on Spanner)."""
        statement_config = getattr(self._config, "statement_config", None)
        dialect = str(getattr(statement_config, "dialect", "spanner")).lower()
        return dialect == "spangres"

    def _resolve_shard_count(self) -> int:
        """Validate and return the configured shard count."""
        shard_count = self._extension_settings.get("shard_count", 1)
        if isinstance(shard_count, bool) or not isinstance(shard_count, int) or shard_count < 1:
            msg = "shard_count must be an integer >= 1"
            raise ImproperConfigurationError(msg)
        return shard_count

    def _resolve_ttl_days(self) -> "int | None":
        """Validate retention_seconds and return positive whole days or None when disabled."""
        retention_seconds = self._extension_settings.get("retention_seconds", 86_400)
        if isinstance(retention_seconds, bool) or not isinstance(retention_seconds, int):
            msg = "retention_seconds must be an integer"
            raise ImproperConfigurationError(msg)
        if retention_seconds <= 0:
            return None
        return max(1, math.ceil(retention_seconds / 86_400))

    def _table_ddl(self) -> str:
        """Build Spanner CREATE TABLE with optional sharding and row deletion policy.

        Spanner does not support DEFAULT clauses on non-computed columns, so
        the DDL has none; values are provided at insert time.
        """
        shard_count = self._resolve_shard_count()
        ttl_days = self._resolve_ttl_days()

        if self._is_spangres():
            shard_col = (
                f"shard_id bigint NOT NULL GENERATED ALWAYS AS (spanner.farm_fingerprint(event_id) % {shard_count}) STORED, "
                if shard_count > 1
                else ""
            )
            pk_cols = "shard_id, event_id" if shard_count > 1 else "event_id"
            ttl_clause = f" TTL INTERVAL '{ttl_days} days' ON acknowledged_at" if ttl_days is not None else ""
            return (
                f"CREATE TABLE {self.table_name} ("
                f"event_id varchar(64) NOT NULL, "
                f"{shard_col}"
                f"channel varchar(128) NOT NULL, "
                f"payload_json jsonb NOT NULL, "
                f"metadata_json jsonb, "
                f"status varchar(32) NOT NULL, "
                f"available_at timestamptz NOT NULL, "
                f"lease_expires_at timestamptz, "
                f"attempts bigint NOT NULL, "
                f"created_at timestamptz NOT NULL, "
                f"acknowledged_at timestamptz, "
                f"PRIMARY KEY ({pk_cols})"
                f"){ttl_clause}"
            )

        payload_type, metadata_type, timestamp_type = self._column_types()
        string_64 = self._string_type(64)
        string_128 = self._string_type(128)
        string_32 = self._string_type(32)
        integer_type = self._integer_type()
        shard_col = (
            f"shard_id INT64 NOT NULL AS (MOD(FARM_FINGERPRINT(event_id), {shard_count})) STORED, "
            if shard_count > 1
            else ""
        )
        pk_inline = " PRIMARY KEY (shard_id, event_id)" if shard_count > 1 else self._primary_key_syntax()
        ttl_clause = (
            f", ROW DELETION POLICY (OLDER_THAN(acknowledged_at, INTERVAL {ttl_days} DAY))"
            if ttl_days is not None
            else ""
        )

        return (
            f"CREATE TABLE {self.table_name} ("
            f"event_id {string_64} NOT NULL, "
            f"{shard_col}"
            f"channel {string_128} NOT NULL, "
            f"payload_json {payload_type} NOT NULL, "
            f"metadata_json {metadata_type}, "
            f"status {string_32} NOT NULL, "
            f"available_at {timestamp_type} NOT NULL, "
            f"lease_expires_at {timestamp_type}, "
            f"attempts {integer_type} NOT NULL, "
            f"created_at {timestamp_type} NOT NULL, "
            f"acknowledged_at {timestamp_type}"
            f"){pk_inline}{ttl_clause}"
        )

    def _index_ddl(self) -> "str | None":
        """Build Spanner covering secondary index for queue operations."""
        shard_count = self._resolve_shard_count()
        shard_prefix = "shard_id, " if shard_count > 1 else ""
        covering_keyword = "INCLUDE" if self._is_spangres() else "STORING"
        return (
            f"CREATE INDEX {self._index_name()} ON {self.table_name}"
            f"({shard_prefix}channel, status, created_at, available_at) "
            f"{covering_keyword} (payload_json, metadata_json, attempts, lease_expires_at)"
        )

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
        database = config.get_database()
        table_id = self.table_name.split(".")[-1]
        if table_id in list_existing_table_names_sync(database):
            return
        statements = self.create_statements()
        self._log_ddl("events.queue.create", len(statements))
        execute_ddl_sync(database, statements)

    def drop_table(self) -> None:
        """Drop the event queue table and index through ``Database.update_ddl``.

        Raises:
            TypeError: If the store's config is not a SpannerSyncConfig.
        """
        config = self._config
        if not isinstance(config, SpannerSyncConfig):
            msg = "drop_table requires SpannerSyncConfig"
            raise TypeError(msg)
        database = config.get_database()
        table_id = self.table_name.split(".")[-1]
        if table_id not in list_existing_table_names_sync(database):
            return
        statements = self.drop_statements()
        self._log_ddl("events.queue.drop", len(statements))
        execute_ddl_sync(database, statements)


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
        database = await config.get_database()
        table_id = self.table_name.split(".")[-1]
        if table_id in await list_existing_table_names_async(database):
            return
        statements = self.create_statements()
        self._log_ddl("events.queue.create", len(statements))
        await execute_ddl_async(database, statements)

    async def drop_table(self) -> None:
        """Drop the event queue table and index through ``Database.update_ddl``.

        Raises:
            TypeError: If the store's config is not a SpannerAsyncConfig.
        """
        config = self._config
        if not isinstance(config, SpannerAsyncConfig):
            msg = "drop_table requires SpannerAsyncConfig"
            raise TypeError(msg)
        database = await config.get_database()
        table_id = self.table_name.split(".")[-1]
        if table_id not in await list_existing_table_names_async(database):
            return
        statements = self.drop_statements()
        self._log_ddl("events.queue.drop", len(statements))
        await execute_ddl_async(database, statements)
