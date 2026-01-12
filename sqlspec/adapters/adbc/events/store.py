"""ADBC event queue store with multi-dialect support.

ADBC supports the following database backends:
- PostgreSQL (adbc_driver_postgresql)
- SQLite (adbc_driver_sqlite)
- DuckDB (adbc_driver_duckdb)
- Snowflake (adbc_driver_snowflake)
- BigQuery (adbc_driver_bigquery)
- FlightSQL (adbc_driver_flightsql)

For unsupported databases (Oracle, Spanner, MySQL), use their native adapters instead.
"""

import logging
from typing import TYPE_CHECKING, Final

from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.config import AdbcConfig

__all__ = ("AdbcEventQueueStore",)

logger = get_logger("sqlspec.adapters.adbc.events.store")

DIALECT_POSTGRESQL: Final = "postgres"
DIALECT_SQLITE: Final = "sqlite"
DIALECT_DUCKDB: Final = "duckdb"
DIALECT_BIGQUERY: Final = "bigquery"
DIALECT_SNOWFLAKE: Final = "snowflake"
DIALECT_FLIGHTSQL: Final = "flightsql"


class AdbcEventQueueStore(BaseEventQueueStore["AdbcConfig"]):
    """Event queue store for ADBC with multi-dialect support.

    ADBC supports multiple database backends, so this store dynamically
    generates DDL appropriate for the detected dialect. Each dialect
    produces different column types and table structures.

    Dialect detection is performed once on first access and cached for the
    lifetime of the store instance (database type never changes).

    Supported dialects:
        - PostgreSQL: JSONB columns, TIMESTAMPTZ, partial indexes
        - SQLite: TEXT columns, TIMESTAMP (default fallback)
        - DuckDB: JSON columns, TIMESTAMP
        - BigQuery: JSON/STRING columns, CLUSTER BY (no indexes)
        - Snowflake: VARIANT columns, no indexes supported
        - FlightSQL: Uses SQLite-compatible types (TEXT, TIMESTAMP)

    Args:
        config: AdbcConfig with extension_config["events"] settings.

    Notes:
        Configuration is read from config.extension_config["events"]:
        - queue_table: Table name (default: "sqlspec_event_queue")

    Example:
        from sqlspec.adapters.adbc import AdbcConfig
        from sqlspec.adapters.adbc.events import AdbcEventQueueStore

        config = AdbcConfig(
            connection_config={"driver_name": "postgres", "uri": "postgresql://..."},
            extension_config={"events": {"queue_table": "my_events"}}
        )
        store = AdbcEventQueueStore(config)
        for stmt in store.create_statements():
            driver.execute_script(stmt)
    """

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        """Initialize ADBC event queue store.

        Args:
            config: AdbcConfig instance.
        """
        super().__init__(config)
        self._dialect: str | None = None

    @property
    def dialect(self) -> str:
        """Return the detected database dialect (cached after first access)."""
        if self._dialect is None:
            self._dialect = self._detect_dialect_from_config()
        return self._dialect

    def _detect_dialect_from_config(self) -> str:
        """Detect ADBC driver dialect from connection config.

        Returns:
            Dialect identifier for DDL generation.

        Notes:
            Called once on first dialect property access. Inspects driver_name
            and uri from connection_config, then falls back to statement_config
            dialect if available.
        """
        connection_config = self._config.connection_config
        driver_name = connection_config.get("driver_name", "")
        uri = connection_config.get("uri", "")

        driver_lower = str(driver_name).lower() if driver_name else ""
        uri_lower = str(uri).lower() if uri else ""

        if "postgres" in driver_lower or uri_lower.startswith(("postgres://", "postgresql://")):
            return DIALECT_POSTGRESQL
        if "duckdb" in driver_lower or uri_lower.startswith("duckdb://"):
            return DIALECT_DUCKDB
        if (
            "gizmosql" in driver_lower
            or "gizmo" in driver_lower
            or uri_lower.startswith(("gizmosql://", "gizmo://", "grpc+tls://"))
        ):
            return DIALECT_DUCKDB
        if "bigquery" in driver_lower or uri_lower.startswith("bigquery://"):
            return DIALECT_BIGQUERY
        if "snowflake" in driver_lower or uri_lower.startswith("snowflake://"):
            return DIALECT_SNOWFLAKE
        if "flightsql" in driver_lower or "grpc" in driver_lower or uri_lower.startswith("grpc://"):
            return DIALECT_FLIGHTSQL
        if "sqlite" in driver_lower or uri_lower.startswith("sqlite://"):
            return DIALECT_SQLITE

        statement_config = self._config.statement_config
        if statement_config and statement_config.dialect is not None:
            dialect_str = str(statement_config.dialect).lower()
            if dialect_str in {DIALECT_POSTGRESQL, DIALECT_SQLITE, DIALECT_DUCKDB, DIALECT_BIGQUERY, DIALECT_SNOWFLAKE}:
                return dialect_str

        log_with_context(
            logger,
            logging.DEBUG,
            "events.queue.dialect.fallback",
            adapter_name="adbc",
            driver_name=driver_lower,
            uri=uri_lower,
            dialect=DIALECT_SQLITE,
        )
        return DIALECT_SQLITE

    def _column_types(self) -> "tuple[str, str, str]":
        """Return payload, metadata, and timestamp column types for the dialect."""
        dialect = self.dialect

        if dialect == DIALECT_POSTGRESQL:
            return "JSONB", "JSONB", "TIMESTAMPTZ"
        if dialect == DIALECT_DUCKDB:
            return "JSON", "JSON", "TIMESTAMP"
        if dialect == DIALECT_BIGQUERY:
            return "JSON", "JSON", "TIMESTAMP"
        if dialect == DIALECT_SNOWFLAKE:
            return "VARIANT", "VARIANT", "TIMESTAMP_TZ"

        return "TEXT", "TEXT", "TIMESTAMP"

    def _build_create_table_sql(self) -> str:
        """Build dialect-specific CREATE TABLE SQL."""
        dialect = self.dialect

        if dialect == DIALECT_BIGQUERY:
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

        if dialect == DIALECT_SNOWFLAKE:
            return (
                f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
                "event_id VARCHAR(64) NOT NULL PRIMARY KEY,"
                " channel VARCHAR(128) NOT NULL,"
                " payload_json VARIANT NOT NULL,"
                " metadata_json VARIANT,"
                " status VARCHAR(32) NOT NULL DEFAULT 'pending',"
                " available_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),"
                " lease_expires_at TIMESTAMP_TZ,"
                " attempts INTEGER NOT NULL DEFAULT 0,"
                " created_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),"
                " acknowledged_at TIMESTAMP_TZ"
                ")"
            )

        if dialect == DIALECT_DUCKDB:
            return (
                f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
                "event_id VARCHAR(64) PRIMARY KEY,"
                " channel VARCHAR(128) NOT NULL,"
                " payload_json JSON NOT NULL,"
                " metadata_json JSON,"
                " status VARCHAR(32) NOT NULL DEFAULT 'pending',"
                " available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                " lease_expires_at TIMESTAMP,"
                " attempts INTEGER NOT NULL DEFAULT 0,"
                " created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                " acknowledged_at TIMESTAMP"
                ")"
            )

        if dialect == DIALECT_POSTGRESQL:
            return (
                f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
                "event_id VARCHAR(64) PRIMARY KEY,"
                " channel VARCHAR(128) NOT NULL,"
                " payload_json JSONB NOT NULL,"
                " metadata_json JSONB,"
                " status VARCHAR(32) NOT NULL DEFAULT 'pending',"
                " available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                " lease_expires_at TIMESTAMPTZ,"
                " attempts INTEGER NOT NULL DEFAULT 0,"
                " created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                " acknowledged_at TIMESTAMPTZ"
                ")"
            )

        return super()._build_create_table_sql()

    def _build_index_sql(self) -> str | None:
        """Build dialect-specific index SQL."""
        dialect = self.dialect

        if dialect == DIALECT_BIGQUERY:
            return None
        if dialect == DIALECT_SNOWFLAKE:
            return None

        if dialect == DIALECT_POSTGRESQL:
            return (
                f"CREATE INDEX IF NOT EXISTS {self._index_name()} "
                f"ON {self.table_name}(channel, status, available_at) "
                "WHERE status = 'pending'"
            )

        return super()._build_index_sql()

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Return statement unchanged since ADBC dialects support IF NOT EXISTS.

        Args:
            statement: The DDL statement.
            object_type: Unused - ADBC dialects handle existence checks natively.

        Returns:
            The statement unchanged.
        """
        del object_type
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        """Return statement unchanged since ADBC dialects support IF EXISTS."""
        return statement

    def create_statements(self) -> "list[str]":
        """Return DDL statements for table creation.

        Returns separate statements for table and index to support
        databases that require separate execution.
        """
        statements = [self._build_create_table_sql()]
        index_sql = self._build_index_sql()
        if index_sql:
            statements.append(index_sql)
        return statements

    def drop_statements(self) -> "list[str]":
        """Return drop statements in reverse dependency order."""
        dialect = self.dialect

        if dialect in {DIALECT_BIGQUERY, DIALECT_SNOWFLAKE}:
            return [f"DROP TABLE IF EXISTS {self.table_name}"]

        index_name = self._index_name()
        return [f"DROP INDEX IF EXISTS {index_name}", f"DROP TABLE IF EXISTS {self.table_name}"]
