"""IBM Db2 Litestar Store implementation."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.db2.core import INDEX_EXISTS_SQL, TABLE_EXISTS_SQL, split_db2_table_name, to_db_timestamp, utc_now
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.adapters.db2.config import Db2SyncConfig

__all__ = ("Db2SyncStore",)


def session_catalog_names(table: str) -> "tuple[str | None, str]":
    """Return the catalog schema and table name for an unquoted session table name.

    Session DDL writes the table name unquoted, so Db2 folds it to uppercase.

    Args:
        table: Session table name.

    Returns:
        The schema, or ``None`` for ``CURRENT SCHEMA``, and the upper-folded table name.
    """
    return split_db2_table_name(table.upper())


def session_index_name(table: str) -> str:
    """Return the catalog name of the session expiry index.

    Args:
        table: Session table name.

    Returns:
        The upper-folded index name.
    """
    return f"IX_{session_catalog_names(table)[1]}_EXP"


def session_table_ddl(table: str) -> str:
    """Return the CREATE TABLE statement for the session table.

    Args:
        table: Session table name.

    Returns:
        Db2 DDL for the session table.
    """
    return f"""
    CREATE TABLE {table} (
        session_id VARCHAR(255) NOT NULL PRIMARY KEY,
        data BLOB(10M) NOT NULL,
        expires_at TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP
    )
    """


def session_index_ddl(table: str) -> str:
    """Return the CREATE INDEX statement for the session expiry index.

    Args:
        table: Session table name.

    Returns:
        Db2 DDL for the expiry index.
    """
    return f"CREATE INDEX {session_index_name(table)} ON {table}(expires_at)"


def select_session_sql(table: str) -> str:
    """Return the query reading an unexpired session.

    Parameters: session id, current naive-UTC time.

    Args:
        table: Session table name.

    Returns:
        The SELECT statement.
    """
    return f"SELECT data, expires_at FROM {table} WHERE session_id = ? AND (expires_at IS NULL OR expires_at > ?)"


def renew_session_sql(table: str) -> str:
    """Return the statement extending a session's expiry.

    Parameters: new naive-UTC expiry, current naive-UTC time, session id.

    Args:
        table: Session table name.

    Returns:
        The UPDATE statement.
    """
    return f"UPDATE {table} SET expires_at = ?, updated_at = ? WHERE session_id = ?"


def upsert_session_sql(table: str) -> str:
    """Return the MERGE statement inserting or replacing a session.

    Parameters: session id, data, naive-UTC expiry or ``None``, current naive-UTC time.

    Args:
        table: Session table name.

    Returns:
        The MERGE statement.
    """
    return f"""
    MERGE INTO {table} AS target
    USING (
        SELECT
            CAST(? AS VARCHAR(255)) AS session_id,
            CAST(? AS BLOB(10M)) AS data,
            CAST(? AS TIMESTAMP) AS expires_at,
            CAST(? AS TIMESTAMP) AS now_utc
        FROM SYSIBM.SYSDUMMY1
    ) AS src
       ON target.session_id = src.session_id
    WHEN MATCHED THEN
        UPDATE SET
            data = src.data,
            expires_at = src.expires_at,
            updated_at = src.now_utc
    WHEN NOT MATCHED THEN
        INSERT (session_id, data, expires_at, created_at, updated_at)
        VALUES (src.session_id, src.data, src.expires_at, src.now_utc, src.now_utc)
    """


def delete_session_sql(table: str) -> str:
    """Return the statement deleting one session.

    Parameters: session id.

    Args:
        table: Session table name.

    Returns:
        The DELETE statement.
    """
    return f"DELETE FROM {table} WHERE session_id = ?"


def delete_all_sql(table: str) -> str:
    """Return the statement deleting every session.

    Args:
        table: Session table name.

    Returns:
        The DELETE statement.
    """
    return f"DELETE FROM {table}"


def exists_session_sql(table: str) -> str:
    """Return the query checking that an unexpired session exists.

    Parameters: session id, current naive-UTC time.

    Args:
        table: Session table name.

    Returns:
        The SELECT statement.
    """
    return f"SELECT 1 AS present FROM {table} WHERE session_id = ? AND (expires_at IS NULL OR expires_at > ?)"


def expires_at_sql(table: str) -> str:
    """Return the query reading a session's expiry.

    Parameters: session id.

    Args:
        table: Session table name.

    Returns:
        The SELECT statement.
    """
    return f"SELECT expires_at FROM {table} WHERE session_id = ?"


def delete_expired_sql(table: str) -> str:
    """Return the statement deleting expired sessions.

    Parameters: current naive-UTC time.

    Args:
        table: Session table name.

    Returns:
        The DELETE statement.
    """
    return f"DELETE FROM {table} WHERE expires_at IS NOT NULL AND expires_at <= ?"


def _coerce_bytes(value: Any) -> "bytes | None":
    """Coerce row data into bytes if not None."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, memoryview):
        return value.tobytes()
    return bytes(value)


def _normalize_utc(dt: Any) -> "datetime | None":
    """Return a stored naive-UTC timestamp as an aware UTC datetime."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


class Db2SyncStore(BaseSQLSpecStore["Db2SyncConfig"]):
    """IBM Db2-backed session store using synchronous Db2 sessions.

    Every timestamp written or compared is a naive-UTC value bound from Python, so expiry does
    not depend on the server's time zone.
    """

    __slots__ = ()

    def __init__(self, config: "Db2SyncConfig") -> None:
        """Initialize Db2 session store."""
        super().__init__(config)

    async def create_table(self) -> None:
        """Create the session table if it does not exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key."""
        return await async_(self._get)(key, renew_for)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value."""
        await async_(self._set)(key, value, expires_in)

    async def delete(self, key: str) -> None:
        """Delete a session by key."""
        await async_(self._delete)(key)

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await async_(self._delete_all)()

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired."""
        return await async_(self._exists)(key)

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires."""
        return await async_(self._expires_in)(key)

    async def delete_expired(self) -> int:
        """Delete all expired sessions."""
        return await async_(self._delete_expired)()

    def _table_ddl(self) -> str:
        """Get Db2 CREATE TABLE SQL."""
        return session_table_ddl(self._table_name)

    def _drop_table_sql(self) -> "list[str]":
        """Get Db2 DROP TABLE statements."""
        return [f"DROP TABLE {self._table_name}"]

    def _create_table(self) -> None:
        """Create the table and its expiry index when the catalog does not list them."""
        schema, table = session_catalog_names(self._table_name)
        with self._config.provide_session() as driver:
            if driver.select_one_or_none(TABLE_EXISTS_SQL, (schema, table)) is None:
                driver.execute(session_table_ddl(self._table_name))
            if driver.select_one_or_none(INDEX_EXISTS_SQL, (schema, session_index_name(self._table_name))) is None:
                driver.execute(session_index_ddl(self._table_name))
            driver.commit()
        self._log_table_created()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Retrieve session data by key, extending its expiry when requested."""
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(select_session_sql(self._table_name), (key, utc_now()))
            if row is None:
                return None
            if renew_for is not None and row["expires_at"] is not None:
                new_expires_at = to_db_timestamp(self._calculate_expires_at(renew_for))
                if new_expires_at is not None:
                    driver.execute(renew_session_sql(self._table_name), (new_expires_at, utc_now(), key))
                    driver.commit()
            return _coerce_bytes(row["data"])

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Upsert session data using an atomic MERGE."""
        data = self._value_to_bytes(value)
        expires_at = to_db_timestamp(self._calculate_expires_at(expires_in))
        with self._config.provide_session() as driver:
            driver.execute(upsert_session_sql(self._table_name), (key, data, expires_at, utc_now()))
            driver.commit()

    def _delete(self, key: str) -> None:
        """Delete session by key."""
        with self._config.provide_session() as driver:
            driver.execute(delete_session_sql(self._table_name), (key,))
            driver.commit()

    def _delete_all(self) -> None:
        """Delete every session row."""
        with self._config.provide_session() as driver:
            driver.execute(delete_all_sql(self._table_name))
            driver.commit()
        self._log_delete_all()

    def _exists(self, key: str) -> bool:
        """Check that an unexpired session exists."""
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(exists_session_sql(self._table_name), (key, utc_now()))
        return row is not None

    def _expires_in(self, key: str) -> "int | None":
        """Calculate seconds until session expiration."""
        with self._config.provide_session() as driver:
            expires_at = _normalize_utc(driver.select_value_or_none(expires_at_sql(self._table_name), (key,)))
        if expires_at is None:
            return None
        remaining = int((expires_at - datetime.now(timezone.utc)).total_seconds())
        return max(remaining, 0)

    def _delete_expired(self) -> int:
        """Remove expired sessions and return the deleted row count."""
        with self._config.provide_session() as driver:
            result = driver.execute(delete_expired_sql(self._table_name), (utc_now(),))
            driver.commit()
        count = max(result.rows_affected, 0)
        if count > 0:
            self._log_delete_expired(count)
        return count
