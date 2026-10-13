"""ADBC event queue store with multi-dialect support.

ADBC supports the following database backends:
    - PostgreSQL (adbc_driver_postgresql)
    - SQLite (adbc_driver_sqlite)
    - DuckDB (adbc_driver_duckdb / adbc_driver_quack)
    - Oracle (adbc_driver_oracle)
    - IBM Db2 (adbc_driver_db2)
    - SQL Server (adbc_driver_mssql / adbc_driver_manager)
    - Snowflake (adbc_driver_snowflake)
    - BigQuery (adbc_driver_bigquery)
    - FlightSQL / GizmoSQL (adbc_driver_flightsql / adbc_driver_gizmosql)
"""

import logging
from typing import TYPE_CHECKING, Final

from sqlspec.adapters.adbc.core import is_postgres_dialect, resolve_dialect_from_config
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
DIALECT_ORACLE: Final = "oracle"
DIALECT_DB2: Final = "db2"
DIALECT_MSSQL: Final = "mssql"


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
        - Oracle: CLOB columns, TIMESTAMP WITH TIME ZONE, PL/SQL existence guards
        - IBM Db2: CLOB(1M) columns, TIMESTAMP, SQLSTATE existence handlers
        - SQL Server: NVARCHAR(MAX) columns, DATETIME2, OBJECT_ID / sys.indexes guards
        - BigQuery: JSON/STRING columns, CLUSTER BY (no indexes)
        - Snowflake: VARIANT columns, no indexes supported
        - FlightSQL: Uses SQLite-compatible types (TEXT, TIMESTAMP)

    Args:
        config: AdbcConfig with extension_config["events"] settings.
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

    def create_statements(self) -> "list[str]":
        """Return DDL statements for table creation.

        Returns separate statements for table and index to support
        databases that require separate execution.
        """
        statements = [self._wrap_create_statement(self._table_ddl(), "table")]
        index_sql = self._index_ddl()
        if index_sql:
            statements.append(self._wrap_create_statement(index_sql, "index"))
        return statements

    def drop_statements(self) -> "list[str]":
        """Return drop statements in reverse dependency order."""
        dialect = self.dialect

        if dialect in {DIALECT_BIGQUERY, DIALECT_SNOWFLAKE}:
            return [f"DROP TABLE IF EXISTS {self.table_name}"]

        if dialect == DIALECT_ORACLE:
            escaped_table = self.table_name.replace("'", "''")
            return [
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {escaped_table}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
            ]

        if dialect == DIALECT_DB2:
            escaped_table = self.table_name.replace("'", "''")
            return [
                f"BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN END; EXECUTE IMMEDIATE 'DROP TABLE {escaped_table}'; END"
            ]

        if dialect == DIALECT_MSSQL:
            return [f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NOT NULL DROP TABLE {self.table_name}"]

        index_name = self._index_name()
        return [f"DROP INDEX IF EXISTS {index_name}", f"DROP TABLE IF EXISTS {self.table_name}"]

    def _detect_dialect_from_config(self) -> str:
        """Detect ADBC driver dialect from connection config.

        Returns:
            Dialect identifier for DDL generation.
        """
        connection_config = self._config.connection_config
        driver_name = connection_config.get("driver_name") or connection_config.get("driver") or ""
        uri = connection_config.get("uri", "")

        driver_lower = str(driver_name).lower() if driver_name else ""
        uri_lower = str(uri).lower() if uri else ""

        if "flightsql" in driver_lower or "grpc" in driver_lower or uri_lower.startswith("grpc://"):
            return DIALECT_FLIGHTSQL

        resolved = resolve_dialect_from_config(connection_config)
        if is_postgres_dialect(resolved):
            return DIALECT_POSTGRESQL
        if resolved in {"mssql", "tsql"}:
            return DIALECT_MSSQL
        if resolved in {DIALECT_DUCKDB, DIALECT_BIGQUERY, DIALECT_SNOWFLAKE, DIALECT_ORACLE, DIALECT_DB2}:
            return resolved
        if resolved == DIALECT_SQLITE and (
            "sqlite" in driver_lower
            or uri_lower.startswith("sqlite://")
            or str(connection_config.get("gizmosql_backend", "")).lower() == DIALECT_SQLITE
        ):
            return DIALECT_SQLITE

        statement_config = self._config.statement_config
        if statement_config and statement_config.dialect is not None:
            dialect_str = str(statement_config.dialect).lower()
            if is_postgres_dialect(dialect_str):
                return DIALECT_POSTGRESQL
            if dialect_str in {"mssql", "tsql"}:
                return DIALECT_MSSQL
            if dialect_str in {
                DIALECT_SQLITE,
                DIALECT_DUCKDB,
                DIALECT_BIGQUERY,
                DIALECT_SNOWFLAKE,
                DIALECT_FLIGHTSQL,
                DIALECT_ORACLE,
                DIALECT_DB2,
            }:
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
        if dialect == DIALECT_ORACLE:
            return "CLOB", "CLOB", "TIMESTAMP WITH TIME ZONE"
        if dialect == DIALECT_DB2:
            return "CLOB(1M)", "CLOB(1M)", "TIMESTAMP"
        if dialect == DIALECT_MSSQL:
            return "NVARCHAR(MAX)", "NVARCHAR(MAX)", "DATETIME2"

        return "TEXT", "TEXT", "TIMESTAMP"

    def _table_ddl(self) -> str:
        """Build dialect-specific CREATE TABLE SQL."""
        dialect = self.dialect

        if dialect == DIALECT_BIGQUERY:
            return f"CREATE TABLE IF NOT EXISTS {self.table_name} (event_id STRING NOT NULL, channel STRING NOT NULL, payload_json JSON NOT NULL, metadata_json JSON, status STRING NOT NULL DEFAULT 'pending', available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), lease_expires_at TIMESTAMP, attempts INT64 NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), acknowledged_at TIMESTAMP) CLUSTER BY channel, status, available_at"

        if dialect == DIALECT_SNOWFLAKE:
            return f"CREATE TABLE IF NOT EXISTS {self.table_name} (event_id VARCHAR(64) NOT NULL PRIMARY KEY, channel VARCHAR(128) NOT NULL, payload_json VARIANT NOT NULL, metadata_json VARIANT, status VARCHAR(32) NOT NULL DEFAULT 'pending', available_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), lease_expires_at TIMESTAMP_TZ, attempts INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), acknowledged_at TIMESTAMP_TZ)"

        if dialect == DIALECT_DUCKDB:
            return f"CREATE TABLE IF NOT EXISTS {self.table_name} (event_id VARCHAR(64) PRIMARY KEY, channel VARCHAR(128) NOT NULL, payload_json JSON NOT NULL, metadata_json JSON, status VARCHAR(32) NOT NULL DEFAULT 'pending', available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, lease_expires_at TIMESTAMP, attempts INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, acknowledged_at TIMESTAMP)"

        if dialect == DIALECT_POSTGRESQL:
            return f"CREATE TABLE IF NOT EXISTS {self.table_name} (event_id VARCHAR(64) PRIMARY KEY, channel VARCHAR(128) NOT NULL, payload_json JSONB NOT NULL, metadata_json JSONB, status VARCHAR(32) NOT NULL DEFAULT 'pending', available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, lease_expires_at TIMESTAMPTZ, attempts INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, acknowledged_at TIMESTAMPTZ)"

        if dialect == DIALECT_ORACLE:
            return f"CREATE TABLE {self.table_name} (event_id VARCHAR2(64) PRIMARY KEY, channel VARCHAR2(128) NOT NULL, payload_json CLOB NOT NULL, metadata_json CLOB, status VARCHAR2(32) DEFAULT 'pending' NOT NULL, available_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, lease_expires_at TIMESTAMP WITH TIME ZONE, attempts NUMBER(10) DEFAULT 0 NOT NULL, created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL, acknowledged_at TIMESTAMP WITH TIME ZONE)"

        if dialect == DIALECT_DB2:
            return f"CREATE TABLE {self.table_name} (event_id VARCHAR(64) NOT NULL PRIMARY KEY, channel VARCHAR(128) NOT NULL, payload_json CLOB(1M) NOT NULL, metadata_json CLOB(1M), status VARCHAR(32) NOT NULL DEFAULT 'pending', available_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, lease_expires_at TIMESTAMP, attempts INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, acknowledged_at TIMESTAMP)"

        if dialect == DIALECT_MSSQL:
            return f"CREATE TABLE {self.table_name} (event_id NVARCHAR(64) NOT NULL PRIMARY KEY, channel NVARCHAR(128) NOT NULL, payload_json NVARCHAR(MAX) NOT NULL, metadata_json NVARCHAR(MAX), status NVARCHAR(32) NOT NULL DEFAULT 'pending', available_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(), lease_expires_at DATETIME2, attempts INT NOT NULL DEFAULT 0, created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(), acknowledged_at DATETIME2)"

        return super()._table_ddl()

    def _index_ddl(self) -> str | None:
        """Build dialect-specific index SQL."""
        dialect = self.dialect

        if dialect in {DIALECT_BIGQUERY, DIALECT_SNOWFLAKE}:
            return None

        if dialect == DIALECT_POSTGRESQL:
            return f"CREATE INDEX IF NOT EXISTS {self._index_name()} ON {self.table_name}(channel, status, available_at) WHERE status = 'pending'"

        if dialect in {DIALECT_ORACLE, DIALECT_DB2, DIALECT_MSSQL}:
            return f"CREATE INDEX {self._index_name()} ON {self.table_name}(channel, status, available_at)"

        return super()._index_ddl()

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Wrap DDL statements in dialect-specific existence guards when needed.

        Args:
            statement: The DDL statement.
            object_type: Either ``"table"`` or ``"index"``.

        Returns:
            Idempotent DDL statement.
        """
        dialect = self.dialect
        if dialect == DIALECT_ORACLE:
            escaped = statement.replace("'", "''")
            if object_type == "table":
                return f"BEGIN EXECUTE IMMEDIATE '{escaped}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;"
            if object_type == "index":
                return f"BEGIN EXECUTE IMMEDIATE '{escaped}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE NOT IN (-955, -1408) THEN RAISE; END IF; END;"
        if dialect == DIALECT_DB2:
            escaped = statement.replace("'", "''")
            if object_type == "table":
                return (
                    f"BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END; EXECUTE IMMEDIATE '{escaped}'; END"
                )
            if object_type == "index":
                return f"BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '01550' BEGIN END; DECLARE CONTINUE HANDLER FOR SQLSTATE '42710' BEGIN END; EXECUTE IMMEDIATE '{escaped}'; END"
        if dialect == DIALECT_MSSQL:
            if object_type == "table":
                return f"IF OBJECT_ID(N'{self.table_name}', N'U') IS NULL BEGIN {statement}; END"
            if object_type == "index":
                return f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{self._index_name()}' AND object_id = OBJECT_ID(N'{self.table_name}')) BEGIN {statement}; END"
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        """Return statement unchanged since drop_statements handles dialect guards."""
        return statement
