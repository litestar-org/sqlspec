"""BigQuery event queue store with clustering optimization.

BigQuery uses clustered tables instead of indexes for query optimization.
The event queue table is clustered by (channel, status, available_at) to
optimize polling queries that filter by channel and status.

Configuration:
    extension_config={
    "events": {
    "queue_table": "my_events" # Default: "sqlspec_event_queue"
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
    """

    __slots__ = ("_partition_expiration_days", "_partitioning", "_require_partition_filter")
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({
        "partition_expiration_days",
        "partitioning",
        "require_partition_filter",
    })

    def __init__(self, config: BigQueryConfig) -> None:
        super().__init__(config)
        self._partitioning = bool(self.settings.get("partitioning", False))
        self._partition_expiration_days = _positive_int_or_none(
            self.settings.get("partition_expiration_days"), "partition_expiration_days"
        )
        self._require_partition_filter = bool(self.settings.get("require_partition_filter", False))

    def create_statements(self) -> "list[str]":
        """Return DDL statement for table creation.

        Returns:
            List containing single CREATE TABLE statement.
        """
        return [self._table_ddl()]

    def drop_statements(self) -> "list[str]":
        """Return DDL statement for table deletion.

        Returns:
            List containing single DROP TABLE statement.
        """
        return [f"DROP TABLE IF EXISTS {self.table_name}"]

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
        partition_clause, options_clause = _bigquery_partition_clauses(
            "available_at",
            enabled=self._partitioning or self._partition_expiration_days is not None or self._require_partition_filter,
            expiration_days=self._partition_expiration_days,
            require_filter=self._require_partition_filter,
        )
        return f"{partition_clause} CLUSTER BY channel, status, available_at{options_clause}"

    def _table_ddl(self) -> str:
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

        return f"CREATE TABLE IF NOT EXISTS {self.table_name} (event_id {string_type} NOT NULL, channel {string_type} NOT NULL, payload_json {payload_type} NOT NULL, metadata_json {metadata_type}, status {string_type} NOT NULL DEFAULT 'pending', available_at {timestamp_type} NOT NULL DEFAULT {ts_default}, lease_expires_at {timestamp_type}, attempts {integer_type} NOT NULL DEFAULT 0, created_at {timestamp_type} NOT NULL DEFAULT {ts_default}, acknowledged_at {timestamp_type}){table_clause}"

    def _index_ddl(self) -> str | None:
        """Return None since BigQuery uses CLUSTER BY instead of indexes.

        Returns:
            None, as BigQuery does not support traditional indexes.
        """
        return None


def _positive_int_or_none(value: object, key: str) -> "int | None":
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        msg = f"extension_config['events']['{key}'] must be a positive integer"
        raise ValueError(msg)
    return value


def _bigquery_partition_clauses(
    column: str, *, enabled: bool, expiration_days: "int | None", require_filter: bool
) -> "tuple[str, str]":
    if not enabled:
        return "", ""
    options: list[str] = []
    if require_filter:
        options.append("require_partition_filter = TRUE")
    if expiration_days is not None:
        options.append(f"partition_expiration_days = {expiration_days}")
    options_clause = f" OPTIONS({', '.join(options)})" if options else ""
    return f" PARTITION BY DATE({column})", options_clause
