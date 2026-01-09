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

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.config import AdbcConfig


__all__ = ("ADBCStore",)


class ADBCStore(BaseSQLSpecStore["AdbcConfig"]):
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

    Example:
        from sqlspec.adapters.adbc import AdbcConfig
        from sqlspec.adapters.adbc.litestar.store import ADBCStore

        config = AdbcConfig(
            connection_config={
                "uri": "postgresql://user:pass@localhost/db"
            }
        )
        store = ADBCStore(config)
        await store.create_table()
    """

    __slots__ = ("_dialect",)

    def __init__(self, config: "AdbcConfig") -> None:
        """Initialize ADBC session store.

        Args:
            config: AdbcConfig instance.

        Notes:
            Table name is read from config.extension_config["litestar"]["session_table"].
        """
        super().__init__(config)
        self._dialect: str | None = None

    def _get_dialect(self) -> str:
        """Get the database dialect, caching it after first access.

        Returns:
            Dialect name (postgres, sqlite, duckdb, mysql, bigquery, snowflake).
        """
        if self._dialect is not None:
            return self._dialect

        with self._config.provide_session() as driver:
            dialect_value = driver.dialect
            self._dialect = str(dialect_value) if dialect_value else "postgres"

        assert self._dialect is not None
        return self._dialect

    def _get_create_table_sql(self) -> str:
        """Get dialect-specific CREATE TABLE SQL for ADBC.

        Returns:
            SQL statement to create the sessions table with proper indexes.

        Notes:
            Automatically adapts to the detected database dialect:
            - PostgreSQL: BYTEA, TIMESTAMPTZ with partial index
            - SQLite: BLOB, DATETIME
            - DuckDB: BLOB, TIMESTAMP
            - MySQL/MariaDB: BLOB, DATETIME
            - BigQuery: BYTES, TIMESTAMP
            - Snowflake: BINARY, TIMESTAMP WITH TIME ZONE
        """
        dialect = self._get_dialect()

        if dialect in {"postgres", "postgresql"}:
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
            );
            CREATE INDEX idx_{self._table_name}_expires_at
            ON {self._table_name}(expires_at);
            """

        if dialect == "snowflake":
            return f"""
            CREATE TABLE IF NOT EXISTS {self._table_name} (
                session_id VARCHAR(255) PRIMARY KEY,
                data BINARY NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE
            );
            CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
            ON {self._table_name}(expires_at);
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
            Parameter placeholder string (e.g., '$1', '?', '%s', '@param1').
        """
        dialect = self._get_dialect()

        if dialect in {"postgres", "postgresql"}:
            return f"${position}"
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

        if dialect in {"postgres", "postgresql"}:
            return "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'"
        if dialect in {"mysql", "mariadb"}:
            return "UTC_TIMESTAMP()"
        if dialect == "bigquery":
            return "CURRENT_TIMESTAMP()"
        if dialect == "snowflake":
            return "CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())"
        return "CURRENT_TIMESTAMP"

    def _create_table(self) -> None:
        """Synchronous implementation of create_table using ADBC driver."""
        sql_text = self._get_create_table_sql()
        with self._config.provide_session() as driver:
            driver.execute_script(sql_text)
            driver.commit()
        self._log_table_created()

    def _get_drop_table_sql(self) -> "list[str]":
        """Get dialect-specific DROP TABLE SQL statements for ADBC.

        Returns:
            List of SQL statements to drop indexes and table.
        """
        dialect = self._get_dialect()

        if dialect in {"mysql", "mariadb"}:
            return [
                f"DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
                f"DROP TABLE IF EXISTS {self._table_name}",
            ]

        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        await async_(self._create_table)()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get using ADBC driver."""
        p1 = self._get_param_placeholder(1)
        current_ts = self._get_current_timestamp_expr()

        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = {p1}
        AND (expires_at IS NULL OR expires_at > {current_ts})
        """

        with self._config.provide_session() as driver:
            result = driver.select_one_or_none(sql, key)

            if result is None:
                return None

            data = result["data"]
            expires_at = result["expires_at"]

            if renew_for is not None and expires_at is not None:
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

            return bytes(data)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
        """
        return await async_(self._get)(key, renew_for)

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Synchronous implementation of set using ADBC driver with dialect-specific UPSERT."""
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        dialect = self._get_dialect()

        p1 = self._get_param_placeholder(1)
        p2 = self._get_param_placeholder(2)
        p3 = self._get_param_placeholder(3)

        if dialect in {"postgres", "postgresql", "sqlite", "duckdb"}:
            if dialect == "sqlite":
                sql = f"""
                INSERT OR REPLACE INTO {self._table_name} (session_id, data, expires_at)
                VALUES ({p1}, {p2}, {p3})
                """
            else:
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
        elif dialect in {"bigquery", "snowflake"}:
            with self._config.provide_session() as driver:
                check_sql = f"SELECT COUNT(*) as count FROM {self._table_name} WHERE session_id = {p1}"
                result = driver.select_one(check_sql, key)
                exists = result and result.get("count", 0) > 0

                if exists:
                    sql = f"""
                    UPDATE {self._table_name}
                    SET data = {p1}, expires_at = {p2}
                    WHERE session_id = {p3}
                    """
                    driver.execute(sql, data, expires_at, key)
                else:
                    sql = f"""
                    INSERT INTO {self._table_name} (session_id, data, expires_at)
                    VALUES ({p1}, {p2}, {p3})
                    """
                    driver.execute(sql, key, data, expires_at)
                driver.commit()
                return
        else:
            sql = f"""
            INSERT INTO {self._table_name} (session_id, data, expires_at)
            VALUES ({p1}, {p2}, {p3})
            ON CONFLICT (session_id) DO UPDATE
            SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at
            """

        with self._config.provide_session() as driver:
            driver.execute(sql, key, data, expires_at)
            driver.commit()

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
        await async_(self._set)(key, value, expires_in)

    def _delete(self, key: str) -> None:
        """Synchronous implementation of delete using ADBC driver."""
        p1 = self._get_param_placeholder(1)
        sql = f"DELETE FROM {self._table_name} WHERE session_id = {p1}"

        with self._config.provide_session() as driver:
            driver.execute(sql, key)
            driver.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        await async_(self._delete)(key)

    def _delete_all(self) -> None:
        """Synchronous implementation of delete_all using ADBC driver."""

        sql = f"DELETE FROM {self._table_name}"

        with self._config.provide_session() as driver:
            driver.execute(sql)
            driver.commit()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await async_(self._delete_all)()

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

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        return await async_(self._exists)(key)

    def _expires_in(self, key: str) -> "int | None":
        """Synchronous implementation of expires_in using ADBC driver."""
        p1 = self._get_param_placeholder(1)
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = {p1}
        """

        with self._config.provide_session() as driver:
            result = driver.select_one(sql, key)

            if result is None or result.get("expires_at") is None:
                return None

            expires_at = result["expires_at"]

            if not isinstance(expires_at, datetime):
                return None

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)

            if expires_at <= now:
                return 0

            delta = expires_at - now
            return int(delta.total_seconds())

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        return await async_(self._expires_in)(key)

    def _delete_expired(self) -> int:
        """Synchronous implementation of delete_expired using ADBC driver."""
        current_ts = self._get_current_timestamp_expr()
        dialect = self._get_dialect()

        if dialect in {"postgres", "postgresql"}:
            sql = f"DELETE FROM {self._table_name} WHERE expires_at <= {current_ts} RETURNING session_id"
        else:
            count_sql = f"SELECT COUNT(*) as count FROM {self._table_name} WHERE expires_at <= {current_ts}"
            delete_sql = f"DELETE FROM {self._table_name} WHERE expires_at <= {current_ts}"

            with self._config.provide_session() as driver:
                result = driver.select_one(count_sql)
                count = result.get("count", 0) if result else 0

                if count > 0:
                    driver.execute(delete_sql)
                    driver.commit()
                    self._log_delete_expired(count)

                return count

        with self._config.provide_session() as driver:
            exec_result = driver.execute(sql)
            driver.commit()
            count = exec_result.rows_affected
            if count > 0:
                self._log_delete_expired(count)
            return count

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        return await async_(self._delete_expired)()
