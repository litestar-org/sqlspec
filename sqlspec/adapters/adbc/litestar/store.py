"""ADBC session store for Litestar integration with multi-dialect support.

ADBC (Arrow Database Connectivity) supports multiple database backends including
PostgreSQL, SQLite, DuckDB, BigQuery, MySQL, and Snowflake. This store automatically
detects the dialect and adapts SQL syntax accordingly.

Supports:
    - PostgreSQL: BYTEA data type, TIMESTAMPTZ, $1 parameters, ON CONFLICT
    - SQLite: BLOB data type, DATETIME, ? parameters, INSERT OR REPLACE
    - DuckDB: BLOB data type, TIMESTAMP, ? parameters, ON CONFLICT
    - MySQL/MariaDB: BLOB data type, DATETIME, %s parameters, ON DUPLICATE KEY UPDATE
    - BigQuery: BYTES data type, TIMESTAMP, @param parameters, MERGE
    - Snowflake: BINARY data type, TIMESTAMP WITH TIME ZONE, ? parameters, MERGE
"""

import contextlib
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.adbc.core import (
    _DB2_INDEX_EXISTS_SQL,
    _DB2_TABLE_EXISTS_SQL,
    _TERADATA_TABLE_EXISTS_SQL,
    is_postgres_dialect,
    resolve_reconcile_adbc_dialect,
    split_teradata_table_name,
)
from sqlspec.adapters.adbc.core import split_db2_table_name as _split_db2_table_name
from sqlspec.exceptions import IntegrityError
from sqlspec.extensions.litestar.store import BaseSQLSpecStore, CompanionIndex
from sqlspec.migrations.schema import SchemaTarget
from sqlspec.utils.sync_tools import async_
from sqlspec.utils.text import normalize_identifier

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.config import AdbcConfig


__all__ = ("AdbcStore",)


class AdbcStore(BaseSQLSpecStore["AdbcConfig"]):
    """ADBC session store using synchronous ADBC driver.

    Implements server-side session storage for Litestar using ADBC
    (Arrow Database Connectivity) via the synchronous driver. Uses
    Litestar's sync_to_thread utility to provide an async interface
    compatible with the Store protocol.

    ADBC supports multiple database backends (PostgreSQL, SQLite, DuckDB, etc.).
    The SQL schema is optimized for PostgreSQL by default, but can work with
    other backends that support TIMESTAMPTZ and BYTEA equivalents.

    Provides efficient session management with:
        - Sync operations wrapped for async compatibility
        - INSERT ON CONFLICT (UPSERT) for PostgreSQL
        - Automatic expiration handling with TIMESTAMPTZ
        - Efficient cleanup of expired sessions

    Args:
        config: AdbcConfig instance.
    """

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        """Initialize ADBC session store.

        Args:
            config: AdbcConfig instance.
        """
        super().__init__(config)
        self._dialect: str | None = None

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        """Validate bare or schema-qualified session table names for SQL safety."""
        if "." in table_name:
            schema_part, _, table_part = table_name.partition(".")
            if schema_part and table_part and "." not in table_part:
                BaseSQLSpecStore._ensure_table_name(schema_part)
                BaseSQLSpecStore._ensure_table_name(table_part)
                return
        BaseSQLSpecStore._ensure_table_name(table_name)

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
        """
        return await async_(self._get)(key, renew_for)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
        await async_(self._set)(key, value, expires_in)

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        await async_(self._delete)(key)

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await async_(self._delete_all)()

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        return await async_(self._exists)(key)

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        return await async_(self._expires_in)(key)

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        return await async_(self._delete_expired)()

    def prepare_schema_sync(self, driver: Any) -> None:
        """Cache dialect from explicit FlightSQL/GizmoSQL config hints or the active driver."""
        if self._dialect is not None:
            return
        connection_config = getattr(self._config, "connection_config", None)
        explicit_dialect = getattr(self._config, "explicit_statement_dialect", None)
        if isinstance(connection_config, Mapping) and (
            connection_config.get("gizmosql_backend") or connection_config.get("adbc_dialect") or explicit_dialect
        ):
            resolved = resolve_reconcile_adbc_dialect(connection_config, explicit_dialect)
            if resolved is not None:
                self._dialect = resolved
                return
        dialect_value = getattr(driver, "dialect", None)
        raw_dialect = str(dialect_value).lower() if dialect_value else "postgres"
        if raw_dialect in {"tsql", "sqlserver"}:
            self._dialect = "mssql"
        elif raw_dialect == "postgresql":
            self._dialect = "postgres"
        else:
            self._dialect = raw_dialect

    def _resolve_schema_dialect(self) -> str:
        """Return cached or config-classified dialect before falling back to a live session."""
        if self._dialect is not None:
            return self._dialect
        connection_config = getattr(self._config, "connection_config", None)
        if isinstance(connection_config, Mapping):
            explicit_dialect = getattr(self._config, "explicit_statement_dialect", None)
            resolved = resolve_reconcile_adbc_dialect(connection_config, explicit_dialect)
            if resolved is not None:
                self._dialect = resolved
                return resolved
        return self._get_dialect()

    def _schema_target(self) -> SchemaTarget:
        """Build the primary session table SchemaTarget using the resolved ADBC backend dialect."""
        dialect = self._resolve_schema_dialect()
        parse_dialect = "postgres" if is_postgres_dialect(dialect) else ("tsql" if dialect == "mssql" else dialect)
        if dialect == "teradata":
            schema_name, catalog_table = split_teradata_table_name(self._table_name)
            return SchemaTarget.from_ddl(catalog_table, self._table_ddl(), schema=schema_name, dialect=parse_dialect)
        if dialect == "db2":
            schema_name, catalog_table = _split_db2_table_name(self._table_name)
            return SchemaTarget.from_ddl(catalog_table, self._table_ddl(), schema=schema_name, dialect=parse_dialect)
        raw_schema, dot, raw_table = self._table_name.partition(".")
        if dot:
            schema_name = raw_schema.strip('"`[]') or None
            bare_table = raw_table.strip('"`[]')
        else:
            schema_name = None
            bare_table = self._table_name.strip('"`[]')
        return SchemaTarget.from_ddl(bare_table, self._table_ddl(), schema=schema_name, dialect=parse_dialect)

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return dialect-specific companion index definitions for ADBC session storage."""
        dialect = self._resolve_schema_dialect()
        if dialect in {"bigquery", "snowflake"}:
            return []

        if dialect == "oracle":
            raw_schema, dot, raw_table = self._table_name.partition(".")
            schema_name = raw_schema.strip('"') or None if dot else None
            bare_table = raw_table.strip('"') if dot else self._table_name.strip('"')
            index_name = f"idx_{self._table_name.replace('.', '_')}_expires_at"
            ddl = f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX {index_name} ON {self._table_name}(expires_at)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE NOT IN (-955, -1408) THEN
                        RAISE;
                    END IF;
            END;
            """.strip()
            return [CompanionIndex(name=index_name, table=bare_table, schema=schema_name, ddl=ddl)]

        if dialect == "teradata":
            schema, table = split_teradata_table_name(self._table_name)
            slug = self._table_name.replace(".", "_").replace('"', "")
            name = f"idx_{slug}_expires_at"
            return [
                CompanionIndex(
                    name=name, table=table, schema=schema, ddl=f"CREATE INDEX {name} (expires_at) ON {self._table_name}"
                )
            ]

        if dialect == "db2":
            schema_name, catalog_table = _split_db2_table_name(self._table_name)
            raw_index_name = f"idx_{self._table_name.replace('.', '_')}_expires_at"
            index_name = normalize_identifier(raw_index_name, "db2")
            ddl = f"CREATE INDEX {raw_index_name} ON {self._table_name}(expires_at)"
            return [CompanionIndex(name=index_name, table=catalog_table, schema=schema_name, ddl=ddl)]

        if dialect == "mssql":
            raw_schema, dot, raw_table = self._table_name.partition(".")
            schema_name = raw_schema.strip('[]"') or None if dot else None
            bare_table = raw_table.strip('[]"') if dot else self._table_name.strip('[]"')
            index_name = f"idx_{self._table_name.replace('.', '_')}_expires_at"
            ddl = f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = N'{index_name}'
                AND object_id = OBJECT_ID(N'{self._table_name}')
            )
            CREATE INDEX {index_name}
            ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;
            """.strip()
            return [CompanionIndex(name=index_name, table=bare_table, schema=schema_name, ddl=ddl)]

        return super().get_companion_indexes()

    def _get_dialect(self) -> str:
        """Get the database dialect, caching it after first access.

        Returns:
            Dialect name (postgres, sqlite, duckdb, mysql, bigquery, snowflake).
        """
        if self._dialect is not None:
            return self._dialect

        with self._config.provide_session() as driver:
            dialect_value = driver.dialect
            raw_dialect = str(dialect_value).lower() if dialect_value else "postgres"
            self._dialect = "mssql" if raw_dialect in {"tsql", "sqlserver"} else raw_dialect

        assert self._dialect is not None
        return self._dialect

    def _table_ddl(self) -> str:
        """Get dialect-specific CREATE TABLE SQL for ADBC.

        Returns:
            SQL statement to create the sessions table with proper indexes.
        """
        dialect = self._get_dialect()

        if dialect == "teradata":
            return (
                f"CREATE MULTISET TABLE {self._table_name} ("
                "session_id VARCHAR(255) NOT NULL PRIMARY KEY, data BLOB NOT NULL, "
                "expires_at TIMESTAMP(6) WITH TIME ZONE)"
            )

        if is_postgres_dialect(dialect):
            return f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                session_id TEXT PRIMARY KEY,
                data BYTEA NOT NULL,
                expires_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
            ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;
            """

        if dialect == "sqlite":
            return f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                session_id TEXT PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at DATETIME
            );
            CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
            ON {self._table_name}(expires_at);
            """

        if dialect == "duckdb":
            return f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                session_id VARCHAR PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
            ON {self._table_name}(expires_at);
            """

        if dialect in {"mysql", "mariadb"}:
            return f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                session_id VARCHAR(255) PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at DATETIME
            );
            CREATE INDEX idx_{self._table_name}_expires_at
            ON {self._table_name}(expires_at);
            """

        if dialect == "bigquery":
            return f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                session_id STRING NOT NULL,
                data BYTES NOT NULL,
                expires_at TIMESTAMP
            )
            """

        if dialect == "snowflake":
            return f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                session_id VARCHAR(255) PRIMARY KEY,
                data BINARY NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE
            )
            """

        if dialect == "oracle":
            return f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE TABLE {self._table_name} (
                    session_id VARCHAR2(255) PRIMARY KEY,
                    data BLOB NOT NULL,
                    expires_at TIMESTAMP WITH TIME ZONE
                )';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """

        if dialect == "db2":
            return f"""
            CREATE TABLE {self._table_name} (
                session_id VARCHAR(255) NOT NULL PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at TIMESTAMP
            )
            """

        if dialect == "mssql":
            return f"""
            IF OBJECT_ID(N'{self._table_name}', N'U') IS NULL
            CREATE TABLE {self._table_name} (
                session_id NVARCHAR(255) PRIMARY KEY,
                data VARBINARY(MAX) NOT NULL,
                expires_at DATETIME2
            );
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = N'idx_{self._table_name}_expires_at'
                AND object_id = OBJECT_ID(N'{self._table_name}')
            )
            CREATE INDEX idx_{self._table_name}_expires_at
            ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;
            """

        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BYTEA NOT NULL,
            expires_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at);
        """

    def _get_param_placeholder(self, position: int) -> str:
        """Get the parameter placeholder syntax for the current dialect.

        Args:
            position: 1-based parameter position.

        Returns:
            Parameter placeholder string.
        """
        dialect = self._get_dialect()

        if is_postgres_dialect(dialect):
            return f"${position}"
        if dialect == "oracle":
            return f":{position}"
        if dialect in {"mysql", "mariadb"}:
            return "%s"
        if dialect == "bigquery":
            return f"@param{position}"
        return "?"

    def _get_current_timestamp_expr(self) -> str:
        """Get the current timestamp expression for the current dialect.

        Returns:
            SQL expression for getting current timestamp with timezone.
        """
        dialect = self._get_dialect()

        if is_postgres_dialect(dialect):
            return "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'"
        if dialect == "oracle":
            return "SYSTIMESTAMP AT TIME ZONE 'UTC'"
        if dialect == "db2":
            return "CURRENT TIMESTAMP"
        if dialect == "mssql":
            return "SYSUTCDATETIME()"
        if dialect in {"mysql", "mariadb"}:
            return "UTC_TIMESTAMP()"
        if dialect == "bigquery":
            return "CURRENT_TIMESTAMP()"
        if dialect == "snowflake":
            return "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())"
        return "CURRENT_TIMESTAMP"

    def _create_table(self) -> None:
        """Synchronous implementation of create_table using ADBC driver."""
        dialect = self._get_dialect()
        if dialect == "teradata":
            schema, table = split_teradata_table_name(self._table_name)
            with self._config.provide_session() as driver:
                try:
                    if driver.select_one_or_none(_TERADATA_TABLE_EXISTS_SQL, (schema, table)) is None:
                        driver.execute(self._table_ddl())
                        driver.commit()
                    self.reconcile_companion_objects_sync(driver)
                    driver.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        driver.rollback()
                    raise
            self._log_table_created()
            return
        if dialect == "oracle":
            table_block = self._table_ddl().strip()
            index_block = f"""
            BEGIN
                EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._table_name}_expires_at ON {self._table_name}(expires_at)';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE NOT IN (-955, -1408) THEN
                        RAISE;
                    END IF;
            END;
            """.strip()
            with self._config.provide_session() as driver:
                try:
                    driver.execute(table_block)
                    driver.execute(index_block)
                    driver.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        driver.rollback()
                    raise
            self._log_table_created()
            return

        if dialect == "db2":
            schema_name, catalog_table = _split_db2_table_name(self._table_name)
            index_name = normalize_identifier(f"idx_{self._table_name}_expires_at", "db2")
            with self._config.provide_session() as driver:
                try:
                    if driver.select_one_or_none(_DB2_TABLE_EXISTS_SQL, (schema_name, catalog_table)) is None:
                        driver.execute(self._table_ddl().strip())
                    if driver.select_one_or_none(_DB2_INDEX_EXISTS_SQL, (schema_name, index_name)) is None:
                        driver.execute(
                            f"CREATE INDEX idx_{self._table_name}_expires_at ON {self._table_name}(expires_at)"
                        )
                    driver.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        driver.rollback()
                    raise
            self._log_table_created()
            return

        sql_text = self._table_ddl()
        with self._config.provide_session() as driver:
            try:
                driver.execute_script(sql_text)
                driver.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise
        self._log_table_created()

    def _drop_table_sql(self) -> "list[str]":
        """Get dialect-specific DROP TABLE SQL statements for ADBC.

        Returns:
            List of SQL statements to drop indexes and table.
        """
        dialect = self._get_dialect()

        if dialect == "teradata":
            return [f"DROP TABLE {self._table_name}"]

        if dialect in {"bigquery", "snowflake"}:
            return [f"DROP TABLE IF EXISTS {self._table_name}"]

        if dialect == "oracle":
            return [
                f"BEGIN EXECUTE IMMEDIATE 'DROP INDEX idx_{self._table_name}_expires_at'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1418 THEN RAISE; END IF; END;",
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {self._table_name}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;",
            ]

        if dialect == "db2":
            schema_name, catalog_table = _split_db2_table_name(self._table_name)
            schema_filter = f"TABSCHEMA = '{schema_name}'" if schema_name is not None else "TABSCHEMA = CURRENT SCHEMA"
            return [
                f"BEGIN IF EXISTS (SELECT 1 FROM SYSCAT.TABLES WHERE {schema_filter} AND TABNAME = '{catalog_table}') THEN EXECUTE IMMEDIATE 'DROP TABLE {self._table_name}'; END IF; END"
            ]

        if dialect == "mssql":
            return [
                f"IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_{self._table_name}_expires_at' AND object_id = OBJECT_ID(N'{self._table_name}')) DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
                f"IF OBJECT_ID(N'{self._table_name}', N'U') IS NOT NULL DROP TABLE {self._table_name}",
            ]

        if dialect in {"mysql", "mariadb"}:
            return [
                f"DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
                f"DROP TABLE IF EXISTS {self._table_name}",
            ]

        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get using ADBC driver."""
        dialect = self._get_dialect()
        p1 = self._get_param_placeholder(1)
        current_ts = self._get_current_timestamp_expr()
        if dialect == "db2":
            select_cols = (
                "HEX(CAST(data AS VARCHAR(16336) FOR BIT DATA)) AS data, "
                "CASE WHEN expires_at IS NULL THEN 0 ELSE 1 END AS expires_at"
            )
        else:
            select_cols = "data, expires_at"

        sql = f"""
        SELECT {select_cols} FROM {self._table_name}
        WHERE session_id = {p1}
        AND (expires_at IS NULL OR expires_at > {current_ts})
        """

        with self._config.provide_session() as driver:
            result = driver.select_one_or_none(sql, key)

            if result is None:
                return None

            data = result["data"] if "data" in result else result["DATA"]
            expires_at = result.get("expires_at") if "expires_at" in result else result.get("EXPIRES_AT")

            if renew_for is not None and expires_at:
                new_expires_at = self._calculate_expires_at(renew_for)
                p1_update = self._get_param_placeholder(1)
                p2_update = self._get_param_placeholder(2)
                update_sql = f"""
                UPDATE {self._table_name}
                SET expires_at = {p1_update}
                WHERE session_id = {p2_update}
                """
                driver.execute(update_sql, new_expires_at, key)
                driver.commit()

            if isinstance(data, str):
                try:
                    return bytes.fromhex(data)
                except ValueError:
                    return data.encode("utf-8")
            return bytes(data)

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Synchronous implementation of set using ADBC driver with dialect-specific UPSERT.

        Uses separate SQL statements for ``NULL`` vs non-``NULL`` ``expires_at``
        on PostgreSQL, DuckDB, BigQuery, and Snowflake so ADBC Arrow parameter
        binding does not infer an untyped ``na`` Arrow array from ``None``.
        """
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        dialect = self._get_dialect()

        p1 = self._get_param_placeholder(1)
        p2 = self._get_param_placeholder(2)
        p3 = self._get_param_placeholder(3)

        if is_postgres_dialect(dialect) or dialect in {"sqlite", "duckdb"}:
            if dialect == "sqlite":
                sql = f"""
                INSERT OR REPLACE INTO {self._table_name} (session_id, data, expires_at)
                VALUES ({p1}, {p2}, {p3})
                """
            else:
                if expires_at is None:
                    sql = f"""
                    INSERT INTO {self._table_name} (session_id, data, expires_at)
                    VALUES ({p1}, {p2}, NULL)
                    ON CONFLICT (session_id) DO UPDATE
                    SET data = EXCLUDED.data, expires_at = NULL
                    """
                    with self._config.provide_session() as driver:
                        try:
                            driver.execute(sql, key, data)
                            driver.commit()
                        except Exception:
                            with contextlib.suppress(Exception):
                                driver.rollback()
                            raise
                    return
                sql = f"""
                INSERT INTO {self._table_name} (session_id, data, expires_at)
                VALUES ({p1}, {p2}, {p3})
                ON CONFLICT (session_id) DO UPDATE
                SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at
                """
        elif dialect in {"mysql", "mariadb"}:
            sql = f"""
            INSERT INTO {self._table_name} (session_id, data, expires_at)
            VALUES ({p1}, {p2}, {p3})
            ON DUPLICATE KEY UPDATE data = VALUES(data), expires_at = VALUES(expires_at)
            """
        elif dialect in {"bigquery", "snowflake", "oracle", "db2", "mssql", "teradata"}:
            bind_data: bytes | str = data.hex() if dialect == "snowflake" else data
            insert_data_expr = f"TO_BINARY({p2}, 'HEX')" if dialect == "snowflake" else p2
            update_data_expr = f"TO_BINARY({p1}, 'HEX')" if dialect == "snowflake" else p1
            with self._config.provide_session() as driver:
                try:
                    check_sql = f"SELECT COUNT(*) as count FROM {self._table_name} WHERE session_id = {p1}"
                    result = driver.select_one(check_sql, key)
                    raw_count = (result.get("count") if "count" in result else result.get("COUNT", 0)) if result else 0
                    exists = bool(raw_count and int(raw_count) > 0)

                    if exists:
                        if expires_at is None:
                            sql = f"""
                            UPDATE {self._table_name}
                            SET data = {update_data_expr}, expires_at = NULL
                            WHERE session_id = {p2}
                            """
                            driver.execute(sql, bind_data, key)
                        else:
                            sql = f"""
                            UPDATE {self._table_name}
                            SET data = {update_data_expr}, expires_at = {p2}
                            WHERE session_id = {p3}
                            """
                            driver.execute(sql, bind_data, expires_at, key)
                    elif expires_at is None:
                        sql = f"""
                        INSERT INTO {self._table_name} (session_id, data, expires_at)
                        VALUES ({p1}, {insert_data_expr}, NULL)
                        """
                        try:
                            driver.execute(sql, key, bind_data)
                        except IntegrityError:
                            sql = f"""
                            UPDATE {self._table_name}
                            SET data = {update_data_expr}, expires_at = NULL
                            WHERE session_id = {p2}
                            """
                            driver.execute(sql, bind_data, key)
                    else:
                        sql = f"""
                        INSERT INTO {self._table_name} (session_id, data, expires_at)
                        VALUES ({p1}, {insert_data_expr}, {p3})
                        """
                        try:
                            driver.execute(sql, key, bind_data, expires_at)
                        except IntegrityError:
                            sql = f"""
                            UPDATE {self._table_name}
                            SET data = {update_data_expr}, expires_at = {p2}
                            WHERE session_id = {p3}
                            """
                            driver.execute(sql, bind_data, expires_at, key)
                    driver.commit()
                except Exception:
                    with contextlib.suppress(Exception):
                        driver.rollback()
                    raise
                return
        else:
            sql = f"""
            INSERT INTO {self._table_name} (session_id, data, expires_at)
            VALUES ({p1}, {p2}, {p3})
            ON CONFLICT (session_id) DO UPDATE
            SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at
            """

        with self._config.provide_session() as driver:
            try:
                driver.execute(sql, key, data, expires_at)
                driver.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise

    def _delete(self, key: str) -> None:
        """Synchronous implementation of delete using ADBC driver."""
        p1 = self._get_param_placeholder(1)
        sql = f"DELETE FROM {self._table_name} WHERE session_id = {p1}"

        with self._config.provide_session() as driver:
            try:
                driver.execute(sql, key)
                driver.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise

    def _delete_all(self) -> None:
        """Synchronous implementation of delete_all using ADBC driver."""

        sql = f"DELETE FROM {self._table_name}"

        with self._config.provide_session() as driver:
            try:
                driver.execute(sql)
                driver.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise

    def _exists(self, key: str) -> bool:
        """Synchronous implementation of exists using ADBC driver."""

        p1 = self._get_param_placeholder(1)
        current_ts = self._get_current_timestamp_expr()

        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = {p1}
        AND (expires_at IS NULL OR expires_at > {current_ts})
        """

        with self._config.provide_session() as driver:
            return bool(driver.select_one_or_none(sql, key) is not None)

    def _expires_in(self, key: str) -> "int | None":
        """Synchronous implementation of expires_in using ADBC driver."""
        p1 = self._get_param_placeholder(1)
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = {p1}
        AND expires_at IS NOT NULL
        """

        with self._config.provide_session() as driver:
            result = driver.select_one_or_none(sql, key)

            if result is None:
                return None

            expires_at = result.get("expires_at") if "expires_at" in result else result.get("EXPIRES_AT")
            if expires_at is None:
                return None

            if not isinstance(expires_at, datetime):
                return None

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=UTC)

            now = datetime.now(UTC)

            if expires_at <= now:
                return 0

            delta = expires_at - now
            return int(delta.total_seconds())

    def _delete_expired(self) -> int:
        """Synchronous implementation of delete_expired using ADBC driver."""
        current_ts = self._get_current_timestamp_expr()
        dialect = self._get_dialect()

        if is_postgres_dialect(dialect):
            sql = f"DELETE FROM {self._table_name} WHERE expires_at <= {current_ts} RETURNING session_id"
        else:
            count_sql = f"SELECT COUNT(*) as count FROM {self._table_name} WHERE expires_at <= {current_ts}"
            delete_sql = f"DELETE FROM {self._table_name} WHERE expires_at <= {current_ts}"

            with self._config.provide_session() as driver:
                try:
                    result = driver.select_one(count_sql)
                    raw_count = (result.get("count") if "count" in result else result.get("COUNT", 0)) if result else 0
                    count = int(raw_count) if raw_count else 0

                    if count > 0:
                        driver.execute(delete_sql)
                        driver.commit()
                        self._log_delete_expired(count)
                except Exception:
                    with contextlib.suppress(Exception):
                        driver.rollback()
                    raise
                else:
                    return count

        with self._config.provide_session() as driver:
            try:
                exec_result = driver.execute(sql)
                driver.commit()
                count = exec_result.rows_affected
                if count > 0:
                    self._log_delete_expired(count)
            except Exception:
                with contextlib.suppress(Exception):
                    driver.rollback()
                raise
            else:
                return count
