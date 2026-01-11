"""Oracle session store for Litestar integration."""

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_
from sqlspec.utils.type_guards import is_async_readable, is_readable

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig


ORACLE_SMALL_BLOB_LIMIT = 32000

__all__ = ("OracleAsyncStore", "OracleSyncStore")


def _coerce_bytes_payload(value: object) -> bytes:
    """Coerce a payload into bytes for session storage."""
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return str(value).encode("utf-8")


async def _read_blob_async(value: object) -> bytes:
    """Read LOB values from async connections into bytes."""
    if is_async_readable(value):
        return _coerce_bytes_payload(await value.read())
    if is_readable(value):
        return _coerce_bytes_payload(value.read())
    return _coerce_bytes_payload(value)


def _read_blob_sync(value: object) -> bytes:
    """Read LOB values from sync connections into bytes."""
    if is_readable(value):
        return _coerce_bytes_payload(value.read())
    return _coerce_bytes_payload(value)


class OracleAsyncStore(BaseSQLSpecStore["OracleAsyncConfig"]):
    """Oracle session store using async OracleDB driver.

    Implements server-side session storage for Litestar using Oracle Database
    via the async python-oracledb driver. Provides efficient session management with:
    - Native async Oracle operations
    - MERGE statement for atomic UPSERT
    - Automatic expiration handling
    - Efficient cleanup of expired sessions
    - Optional In-Memory Column Store support (requires Oracle Database In-Memory license)

    Args:
        config: OracleAsyncConfig with extension_config["litestar"] settings.

    Example:
        from sqlspec.adapters.oracledb import OracleAsyncConfig
        from sqlspec.adapters.oracledb.litestar.store import OracleAsyncStore

        config = OracleAsyncConfig(
            connection_config={"dsn": "oracle://..."},
            extension_config={
                "litestar": {
                    "session_table": "my_sessions",
                    "in_memory": True
                }
            }
        )
        store = OracleAsyncStore(config)
        await store.create_table()

    Notes:
        Configuration is read from config.extension_config["litestar"]:
        - session_table: Session table name (default: "litestar_session")
        - in_memory: Enable INMEMORY PRIORITY HIGH clause (default: False, Oracle-specific)

        When in_memory=True, the table is created with INMEMORY PRIORITY HIGH clause for
        faster read operations. PRIORITY HIGH ensures the table is populated into the
        In-Memory column store at database startup for immediate performance benefits.
        This requires Oracle Database 12.1.0.2+ with the Database In-Memory option licensed.
        If In-Memory is not available, the table creation will fail with ORA-00439 or ORA-62142.
    """

    __slots__ = ("_in_memory",)

    def __init__(self, config: "OracleAsyncConfig") -> None:
        """Initialize Oracle session store.

        Args:
            config: OracleAsyncConfig instance.

        Notes:
            Configuration is read from config.extension_config["litestar"]:
            - session_table: Session table name (default: "litestar_session")
            - in_memory: Enable INMEMORY clause (default: False)
        """
        super().__init__(config)

        litestar_config = config.extension_config.get("litestar", {})
        self._in_memory = bool(litestar_config.get("in_memory", False))

    def _get_create_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.

        Notes:
            - Uses TIMESTAMP WITH TIME ZONE for timezone-aware expiration timestamps
            - Index on expires_at for efficient cleanup queries
            - BLOB type for data storage (Oracle native binary type)
            - Audit columns (created_at, updated_at) help with debugging
            - Table name is internally controlled, not user input (S608 suppressed)
            - INMEMORY PRIORITY HIGH clause added when in_memory=True for faster reads
            - HIGH priority ensures table population at database startup
        """
        inmemory_clause = "INMEMORY PRIORITY HIGH" if self._in_memory else ""
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._table_name} (
                session_id VARCHAR2(255) PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            ) {inmemory_clause}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._table_name}_expires_at
                ON {self._table_name}(expires_at)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_table_sql(self) -> "list[str]":
        """Get Oracle DROP TABLE SQL with PL/SQL error handling.

        Returns:
            List of SQL statements with exception handling for non-existent objects.
        """
        return [
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._table_name}_expires_at';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._table_name}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
        ]

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        sql = self._get_create_table_sql()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)

        self._log_table_created()

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.

        Notes:
            Uses SYSTIMESTAMP for Oracle current timestamp.
            The query uses the index for expires_at > SYSTIMESTAMP.
        """
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = :session_id
        AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": key})
            row = await cursor.fetchone()

            if row is None:
                return None

            data_blob, expires_at = row

            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = :expires_at, updated_at = SYSTIMESTAMP
                    WHERE session_id = :session_id
                    """
                    await cursor.execute(update_sql, {"expires_at": new_expires_at, "session_id": key})
                    await conn.commit()

            return await _read_blob_async(data_blob)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.

        Notes:
            Uses MERGE for atomic UPSERT operation in Oracle.
            Updates updated_at timestamp on every write for audit trail.
            For large BLOBs, uses empty_blob() and then writes data separately.
        """
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            cursor = conn.cursor()

            if len(data) > ORACLE_SMALL_BLOB_LIMIT:
                merge_sql = f"""
                MERGE INTO {self._table_name} t
                USING (SELECT :session_id AS session_id FROM DUAL) s
                ON (t.session_id = s.session_id)
                WHEN MATCHED THEN
                    UPDATE SET
                        data = EMPTY_BLOB(),
                        expires_at = :expires_at,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (:session_id, EMPTY_BLOB(), :expires_at, SYSTIMESTAMP, SYSTIMESTAMP)
                """
                await cursor.execute(merge_sql, {"session_id": key, "expires_at": expires_at})

                select_sql = f"""
                SELECT data FROM {self._table_name}
                WHERE session_id = :session_id FOR UPDATE
                """
                await cursor.execute(select_sql, {"session_id": key})
                row = await cursor.fetchone()
                if row:
                    blob = row[0]
                    await blob.write(data)

                await conn.commit()
            else:
                sql = f"""
                MERGE INTO {self._table_name} t
                USING (SELECT :session_id AS session_id FROM DUAL) s
                ON (t.session_id = s.session_id)
                WHEN MATCHED THEN
                    UPDATE SET
                        data = :data,
                        expires_at = :expires_at,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (:session_id, :data, :expires_at, SYSTIMESTAMP, SYSTIMESTAMP)
                """
                await cursor.execute(sql, {"session_id": key, "data": data, "expires_at": expires_at})
                await conn.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        sql = f"DELETE FROM {self._table_name} WHERE session_id = :session_id"

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": key})
            await conn.commit()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        sql = f"DELETE FROM {self._table_name}"

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            cursor = conn.cursor()
            await cursor.execute(sql)
            await conn.commit()
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.

        Notes:
            Uses SYSTIMESTAMP for consistency with get() method.
        """
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = :session_id
        AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": key})
            result = await cursor.fetchone()
            return result is not None

    async def expires_in(self, key: str) -> "int | None":
        """Get the time in seconds until the session expires.

        Args:
            key: Session ID to check.

        Returns:
            Seconds until expiration, or None if no expiry or key doesn't exist.
        """
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = :session_id
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": key})
            row = await cursor.fetchone()

            if row is None or row[0] is None:
                return None

            expires_at = row[0]

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)

            if expires_at <= now:
                return 0

            delta = expires_at - now
            return int(delta.total_seconds())

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.

        Notes:
            Uses SYSTIMESTAMP for consistency.
            Oracle automatically commits DDL, so we explicitly commit for DML.
        """
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= SYSTIMESTAMP"

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            cursor = conn.cursor()
            await cursor.execute(sql)
            count = cursor.rowcount if cursor.rowcount is not None else 0
            await conn.commit()
            if count > 0:
                self._log_delete_expired(count)
            return count


class OracleSyncStore(BaseSQLSpecStore["OracleSyncConfig"]):
    """Oracle session store using sync OracleDB driver.

    Implements server-side session storage for Litestar using Oracle Database
    via the synchronous python-oracledb driver. Uses async_() wrapper to provide
    an async interface compatible with the Store protocol.

    Provides efficient session management with:
    - Sync operations wrapped for async compatibility
    - MERGE statement for atomic UPSERT
    - Automatic expiration handling
    - Efficient cleanup of expired sessions
    - Optional In-Memory Column Store support (requires Oracle Database In-Memory license)

    Note:
        For high-concurrency applications, consider using OracleAsyncStore instead,
        as it provides native async operations without threading overhead.

    Args:
        config: OracleSyncConfig with extension_config["litestar"] settings.

    Example:
        from sqlspec.adapters.oracledb import OracleSyncConfig
        from sqlspec.adapters.oracledb.litestar.store import OracleSyncStore

        config = OracleSyncConfig(
            connection_config={"dsn": "oracle://..."},
            extension_config={
                "litestar": {
                    "session_table": "my_sessions",
                    "in_memory": True
                }
            }
        )
        store = OracleSyncStore(config)
        await store.create_table()

    Notes:
        Configuration is read from config.extension_config["litestar"]:
        - session_table: Session table name (default: "litestar_session")
        - in_memory: Enable INMEMORY PRIORITY HIGH clause (default: False, Oracle-specific)

        When in_memory=True, the table is created with INMEMORY PRIORITY HIGH clause for
        faster read operations. PRIORITY HIGH ensures the table is populated into the
        In-Memory column store at database startup for immediate performance benefits.
        This requires Oracle Database 12.1.0.2+ with the Database In-Memory option licensed.
        If In-Memory is not available, the table creation will fail with ORA-00439 or ORA-62142.
    """

    __slots__ = ("_in_memory",)

    def __init__(self, config: "OracleSyncConfig") -> None:
        """Initialize Oracle sync session store.

        Args:
            config: OracleSyncConfig instance.

        Notes:
            Configuration is read from config.extension_config["litestar"]:
            - session_table: Session table name (default: "litestar_session")
            - in_memory: Enable INMEMORY clause (default: False)
        """
        super().__init__(config)

        litestar_config = config.extension_config.get("litestar", {})
        self._in_memory = bool(litestar_config.get("in_memory", False))

    def _get_create_table_sql(self) -> str:
        """Get Oracle CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.

        Notes:
            - Uses TIMESTAMP WITH TIME ZONE for timezone-aware expiration timestamps
            - Index on expires_at for efficient cleanup queries
            - BLOB type for data storage (Oracle native binary type)
            - Audit columns (created_at, updated_at) help with debugging
            - Table name is internally controlled, not user input (S608 suppressed)
            - INMEMORY PRIORITY HIGH clause added when in_memory=True for faster reads
            - HIGH priority ensures table population at database startup
        """
        inmemory_clause = "INMEMORY PRIORITY HIGH" if self._in_memory else ""
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._table_name} (
                session_id VARCHAR2(255) PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            ) {inmemory_clause}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._table_name}_expires_at
                ON {self._table_name}(expires_at)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_table_sql(self) -> "list[str]":
        """Get Oracle DROP TABLE SQL with PL/SQL error handling.

        Returns:
            List of SQL statements with exception handling for non-existent objects.
        """
        return [
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._table_name}_expires_at';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._table_name}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
        ]

    def _create_table(self) -> None:
        """Synchronous implementation of create_table."""
        sql = self._get_create_table_sql()
        with self._config.provide_session() as driver:
            driver.execute_script(sql)

        self._log_table_created()

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        await async_(self._create_table)()

    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get.

        Notes:
            Uses SYSTIMESTAMP for Oracle current timestamp.
        """
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = :session_id
        AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"session_id": key})
            row = cursor.fetchone()

            if row is None:
                return None

            data_blob, expires_at = row

            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = :expires_at, updated_at = SYSTIMESTAMP
                    WHERE session_id = :session_id
                    """
                    cursor.execute(update_sql, {"expires_at": new_expires_at, "session_id": key})
                    conn.commit()

            return _read_blob_sync(data_blob)

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
        """Synchronous implementation of set.

        Notes:
            Uses MERGE for atomic UPSERT operation in Oracle.
        """
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()

            if len(data) > ORACLE_SMALL_BLOB_LIMIT:
                merge_sql = f"""
                MERGE INTO {self._table_name} t
                USING (SELECT :session_id AS session_id FROM DUAL) s
                ON (t.session_id = s.session_id)
                WHEN MATCHED THEN
                    UPDATE SET
                        data = EMPTY_BLOB(),
                        expires_at = :expires_at,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (:session_id, EMPTY_BLOB(), :expires_at, SYSTIMESTAMP, SYSTIMESTAMP)
                """
                cursor.execute(merge_sql, {"session_id": key, "expires_at": expires_at})

                select_sql = f"""
                SELECT data FROM {self._table_name}
                WHERE session_id = :session_id FOR UPDATE
                """
                cursor.execute(select_sql, {"session_id": key})
                row = cursor.fetchone()
                if row:
                    blob = row[0]
                    blob.write(data)

                conn.commit()
            else:
                sql = f"""
                MERGE INTO {self._table_name} t
                USING (SELECT :session_id AS session_id FROM DUAL) s
                ON (t.session_id = s.session_id)
                WHEN MATCHED THEN
                    UPDATE SET
                        data = :data,
                        expires_at = :expires_at,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (:session_id, :data, :expires_at, SYSTIMESTAMP, SYSTIMESTAMP)
                """
                cursor.execute(sql, {"session_id": key, "data": data, "expires_at": expires_at})
                conn.commit()

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
        await async_(self._set)(key, value, expires_in)

    def _delete(self, key: str) -> None:
        """Synchronous implementation of delete."""
        sql = f"DELETE FROM {self._table_name} WHERE session_id = :session_id"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"session_id": key})
            conn.commit()

    async def delete(self, key: str) -> None:
        """Delete a session by key.

        Args:
            key: Session ID to delete.
        """
        await async_(self._delete)(key)

    def _delete_all(self) -> None:
        """Synchronous implementation of delete_all."""
        sql = f"DELETE FROM {self._table_name}"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
        self._log_delete_all()

    async def delete_all(self) -> None:
        """Delete all sessions from the store."""
        await async_(self._delete_all)()

    def _exists(self, key: str) -> bool:
        """Synchronous implementation of exists."""
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = :session_id
        AND (expires_at IS NULL OR expires_at > SYSTIMESTAMP)
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"session_id": key})
            result = cursor.fetchone()
            return result is not None

    async def exists(self, key: str) -> bool:
        """Check if a session key exists and is not expired.

        Args:
            key: Session ID to check.

        Returns:
            True if the session exists and is not expired.
        """
        return await async_(self._exists)(key)

    def _expires_in(self, key: str) -> "int | None":
        """Synchronous implementation of expires_in."""
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = :session_id
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"session_id": key})
            row = cursor.fetchone()

            if row is None or row[0] is None:
                return None

            expires_at = row[0]

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
        """Synchronous implementation of delete_expired."""
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= SYSTIMESTAMP"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            count = cursor.rowcount if cursor.rowcount is not None else 0
            conn.commit()
            if count > 0:
                self._log_delete_expired(count)
            return count

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        return await async_(self._delete_expired)()
