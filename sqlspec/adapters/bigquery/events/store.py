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
from sqlspec.extensions.events._store import BaseEventQueueStore

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

    def _column_types(self) -> tuple[str, str, str]:
        """Return BigQuery-specific column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSON", "JSON", "TIMESTAMP"

    def _build_create_table_sql(self) -> str:
        """Build BigQuery CREATE TABLE with CLUSTER BY optimization.

        Returns:
            DDL statement for creating the event queue table.

        Notes:
            BigQuery uses CLUSTER BY for query optimization instead of indexes.
            The clustering columns match the typical polling query pattern.
        """
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
            "event_id STRING NOT NULL,"
            " channel STRING NOT NULL,"
            " payload_json JSON NOT NULL,"
            " metadata_json JSON,"
            " status STRING NOT NULL DEFAULT 'pending',"
            " available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),"
            " lease_expires_at TIMESTAMP,"
            " attempts INT64 NOT NULL DEFAULT 0,"
            " created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),"
            " acknowledged_at TIMESTAMP"
            ") CLUSTER BY channel, status, available_at"
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
