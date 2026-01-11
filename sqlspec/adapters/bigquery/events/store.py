"""BigQuery event queue store with clustering optimization.

BigQuery uses clustered tables instead of indexes for query optimization.
The event queue table is clustered by (channel, status, available_at) to
optimize polling queries that filter by channel and status.

Configuration:
    extension_config={
        "events": {
            "queue_table": "my_events"  # Default: "sqlspec_event_queue"
        }
    }
"""

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("BigQueryEventQueueStore",)


class BigQueryEventQueueStore(BaseEventQueueStore[BigQueryConfig]):
    """BigQuery-specific event queue store with clustering optimization.

    Generates DDL optimized for BigQuery. BigQuery does not support traditional
    indexes, so the table uses CLUSTER BY for query optimization instead.

    Args:
        config: BigQueryConfig with extension_config["events"] settings.

    Notes:
        Configuration is read from config.extension_config["events"]:
        - queue_table: Table name (default: "sqlspec_event_queue")

        BigQuery-specific optimizations:
        - Uses STRING instead of VARCHAR (BigQuery's native string type)
        - Uses INT64 instead of INTEGER
        - Uses CLUSTER BY instead of CREATE INDEX
        - Supports IF NOT EXISTS / IF EXISTS in DDL

    Example:
        from sqlspec.adapters.bigquery import BigQueryConfig
        from sqlspec.adapters.bigquery.events import BigQueryEventQueueStore

        config = BigQueryConfig(
            connection_config={"project": "my-project"},
            extension_config={"events": {"queue_table": "my_events"}}
        )
        store = BigQueryEventQueueStore(config)
        for stmt in store.create_statements():
            driver.execute_script(stmt)
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return BigQuery-specific column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSON", "JSON", "TIMESTAMP"

    def _string_type(self, length: int) -> str:
        """Return BigQuery STRING type (length is ignored)."""
        del length
        return "STRING"

    def _integer_type(self) -> str:
        """Return BigQuery INT64 type."""
        return "INT64"

    def _timestamp_default(self) -> str:
        """Return BigQuery timestamp default expression."""
        return "CURRENT_TIMESTAMP()"

    def _table_clause(self) -> str:
        """Return BigQuery CLUSTER BY clause for query optimization."""
        return " CLUSTER BY channel, status, available_at"

    def _build_create_table_sql(self) -> str:
        """Build BigQuery CREATE TABLE with CLUSTER BY optimization.

        BigQuery uses CLUSTER BY for query optimization instead of indexes.
        The clustering columns match the typical polling query pattern.

        Note: BigQuery does not support column-level PRIMARY KEY, so we
        omit it entirely. event_id uniqueness must be enforced at insert time.
        """
        payload_type, metadata_type, timestamp_type = self._column_types()
        string_type = self._string_type(0)
        integer_type = self._integer_type()
        ts_default = self._timestamp_default()
        table_clause = self._table_clause()

        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
            f"event_id {string_type} NOT NULL,"
            f" channel {string_type} NOT NULL,"
            f" payload_json {payload_type} NOT NULL,"
            f" metadata_json {metadata_type},"
            f" status {string_type} NOT NULL DEFAULT 'pending',"
            f" available_at {timestamp_type} NOT NULL DEFAULT {ts_default},"
            f" lease_expires_at {timestamp_type},"
            f" attempts {integer_type} NOT NULL DEFAULT 0,"
            f" created_at {timestamp_type} NOT NULL DEFAULT {ts_default},"
            f" acknowledged_at {timestamp_type}"
            f"){table_clause}"
        )

    def _build_index_sql(self) -> str | None:
        """Return None since BigQuery uses CLUSTER BY instead of indexes.

        Returns:
            None, as BigQuery does not support traditional indexes.
        """
        return None

    def create_statements(self) -> "list[str]":
        """Return DDL statement for table creation.

        Returns:
            List containing single CREATE TABLE statement.

        Notes:
            BigQuery uses CLUSTER BY instead of separate index creation,
            so only one statement is returned.
        """
        return [self._build_create_table_sql()]

    def drop_statements(self) -> "list[str]":
        """Return DDL statement for table deletion.

        Returns:
            List containing single DROP TABLE statement.

        Notes:
            BigQuery has no index to drop, only the table.
        """
        return [f"DROP TABLE IF EXISTS {self.table_name}"]
