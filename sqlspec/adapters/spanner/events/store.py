"""Spanner event queue store with GoogleSQL-optimized DDL.

Spanner requires:
- STRING instead of VARCHAR
- INT64 instead of INTEGER
- No DEFAULT clause for non-computed columns
- Separate index creation statements (no IF NOT EXISTS)
- PRIMARY KEY declared inline in CREATE TABLE
"""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.spanner.config import SpannerSyncConfig  # noqa: F401

__all__ = ("SpannerSyncEventQueueStore",)

logger = get_logger("adapters.spanner.events.store")


class SpannerSyncEventQueueStore(BaseEventQueueStore["SpannerSyncConfig"]):
    """Spanner-specific event queue store with GoogleSQL DDL.

    Generates optimized DDL for Google Cloud Spanner using GoogleSQL dialect.
    Spanner does not support IF NOT EXISTS, so statements must be executed
    with proper error handling for existing objects.

    Args:
        config: SpannerSyncConfig with extension_config["events"] settings.

    Notes:
        Configuration is read from config.extension_config["events"]:
        - queue_table: Table name (default: "sqlspec_event_queue")

    Example:
        from sqlspec.adapters.spanner import SpannerSyncConfig
        from sqlspec.adapters.spanner.events import SpannerSyncEventQueueStore

        config = SpannerSyncConfig(
            connection_config={"project": "my-project", "instance": "my-instance", "database": "my-db"},
            extension_config={"events": {"queue_table": "my_events"}}
        )
        store = SpannerSyncEventQueueStore(config)
        store.create_table()
    """

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        """Return Spanner-specific column types."""
        return "JSON", "JSON", "TIMESTAMP"

    def _build_create_table_sql(self) -> str:
        """Build Spanner CREATE TABLE with PRIMARY KEY inline."""
        return (
            f"CREATE TABLE {self.table_name} ("
            "event_id STRING(64) NOT NULL,"
            " channel STRING(128) NOT NULL,"
            " payload_json JSON NOT NULL,"
            " metadata_json JSON,"
            " status STRING(32) NOT NULL,"
            " available_at TIMESTAMP NOT NULL,"
            " lease_expires_at TIMESTAMP,"
            " attempts INT64 NOT NULL,"
            " created_at TIMESTAMP NOT NULL,"
            " acknowledged_at TIMESTAMP"
            ") PRIMARY KEY (event_id)"
        )

    def _build_index_sql(self) -> str | None:
        """Build Spanner secondary index for queue operations."""
        index_name = self._index_name()
        return f"CREATE INDEX {index_name} ON {self.table_name}(channel, status, available_at)"

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Return statement unchanged - Spanner does not support IF NOT EXISTS.

        Args:
            statement: The DDL statement.
            object_type: Type of object (table, index).

        Returns:
            The statement unchanged.
        """
        del object_type
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        """Return statement unchanged - Spanner does not support IF EXISTS."""
        return statement

    def create_statements(self) -> "list[str]":
        """Return separate statements for table and index creation.

        Spanner requires DDL statements to be executed individually.
        The caller should handle errors for already-existing objects.
        """
        statements = [self._build_create_table_sql()]
        index_sql = self._build_index_sql()
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

    def create_table(self) -> None:
        """Create the event queue table and index.

        Executes DDL statements via database.update_ddl() which is the
        recommended approach for Spanner schema changes.

        Raises:
            google.api_core.exceptions.AlreadyExists: If table or index exists.
        """
        from sqlspec.adapters.spanner.config import SpannerSyncConfig

        config = self._config
        if not isinstance(config, SpannerSyncConfig):
            msg = "create_table requires SpannerSyncConfig"
            raise TypeError(msg)

        database = config.get_database()
        statements = self.create_statements()
        logger.debug("Creating event queue table with %d DDL statements", len(statements))
        database.update_ddl(statements).result()  # type: ignore[no-untyped-call]

    def drop_table(self) -> None:
        """Drop the event queue table and index.

        Executes DDL statements via database.update_ddl() which is the
        recommended approach for Spanner schema changes.

        Raises:
            google.api_core.exceptions.NotFound: If table or index does not exist.
        """
        from sqlspec.adapters.spanner.config import SpannerSyncConfig

        config = self._config
        if not isinstance(config, SpannerSyncConfig):
            msg = "drop_table requires SpannerSyncConfig"
            raise TypeError(msg)

        database = config.get_database()
        statements = self.drop_statements()
        logger.debug("Dropping event queue table with %d DDL statements", len(statements))
        database.update_ddl(statements).result()  # type: ignore[no-untyped-call]
