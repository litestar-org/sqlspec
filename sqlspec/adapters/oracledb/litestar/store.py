"""Oracle session store for Litestar integration."""

from datetime import timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.oracledb._storage import (
    _oracle_table_feature_report,
    _resolve_oracle_storage_capabilities_async,
    _resolve_oracle_storage_capabilities_sync,
)
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_
from sqlspec.utils.type_guards import is_async_readable, is_readable

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig

__all__ = ("OracleAsyncStore", "OracleSyncStore")


ORACLE_SMALL_BLOB_LIMIT = 32000










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
    """

    __slots__ = ("_in_memory",)

    def __init__(self, config: "OracleAsyncConfig") -> None:
        """Initialize Oracle session store.

        Args:
            config: OracleAsyncConfig instance.
        """
        super().__init__(config)

        litestar_config = config.extension_config.get("litestar", {})
        self._in_memory = bool(litestar_config.get("in_memory", False))

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        async with self._config.provide_session() as driver:
            await _resolve_oracle_storage_capabilities_async(driver)
            sql = self._table_ddl()
            await driver.execute_script(sql)

        self._log_table_created()
        await self.reconcile_schema(assume_existing=True)

    async def prepare_schema_async(self, driver: Any) -> None:
        """Resolve pool-scoped Oracle storage capabilities before DDL generation."""
        await _resolve_oracle_storage_capabilities_async(driver)

    async def get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Get a session value by key.

        Args:
            key: Session ID to retrieve.
            renew_for: If given, renew the expiry time for this duration.

        Returns:
            Session data as bytes if found and not expired, None otherwise.
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
                expires_in_seconds = _oracle_expiry_seconds(renew_for)
                if expires_in_seconds is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        updated_at = SYSTIMESTAMP
                    WHERE session_id = :session_id
                    """
                    await cursor.execute(update_sql, {"expires_in_seconds": expires_in_seconds, "session_id": key})
                    await conn.commit()

            return await _read_blob_async(data_blob)

    async def set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Store a session value.

        Args:
            key: Session ID.
            value: Session data.
            expires_in: Time until expiration.
        """
        data = self._value_to_bytes(value)
        expires_in_seconds = _oracle_expiry_seconds(expires_in)

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
                        expires_at = CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (
                        :session_id,
                        EMPTY_BLOB(),
                        CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        SYSTIMESTAMP,
                        SYSTIMESTAMP
                    )
                """
                await cursor.execute(merge_sql, {"session_id": key, "expires_in_seconds": expires_in_seconds})

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
                        expires_at = CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (
                        :session_id,
                        :data,
                        CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        SYSTIMESTAMP,
                        SYSTIMESTAMP
                    )
                """
                await cursor.execute(sql, {"session_id": key, "data": data, "expires_in_seconds": expires_in_seconds})
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
        SELECT expires_at, SYSTIMESTAMP FROM {self._table_name}
        WHERE session_id = :session_id
        """

        conn_context = self._config.provide_connection()
        async with conn_context as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": key})
            row = await cursor.fetchone()

            if row is None or row[0] is None:
                return None

            expires_at, db_now = row

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if db_now.tzinfo is None:
                db_now = db_now.replace(tzinfo=timezone.utc)

            if expires_at <= db_now:
                return 0

            delta = expires_at - db_now
            return int(delta.total_seconds())

    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
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

    def _table_ddl(self) -> str:
        """Get Oracle CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.
        """
        table_clause = _litestar_table_feature_clause(self._config, self._in_memory)
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._table_name} (
                session_id VARCHAR2(255) PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            ){table_clause}';
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

    def _drop_table_sql(self) -> "list[str]":
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

    Args:
        config: OracleSyncConfig with extension_config["litestar"] settings.
    """

    __slots__ = ("_in_memory",)

    def __init__(self, config: "OracleSyncConfig") -> None:
        """Initialize Oracle sync session store.

        Args:
            config: OracleSyncConfig instance.
        """
        super().__init__(config)

        litestar_config = config.extension_config.get("litestar", {})
        self._in_memory = bool(litestar_config.get("in_memory", False))

    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        await async_(self._create_table)()
        await self.reconcile_schema(assume_existing=True)

    def prepare_schema_sync(self, driver: Any) -> None:
        """Resolve pool-scoped Oracle storage capabilities before DDL generation."""
        _resolve_oracle_storage_capabilities_sync(driver)

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

    def _table_ddl(self) -> str:
        """Get Oracle CREATE TABLE SQL with optimized schema.

        Returns:
            SQL statement to create the sessions table with proper indexes.
        """
        table_clause = _litestar_table_feature_clause(self._config, self._in_memory)
        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._table_name} (
                session_id VARCHAR2(255) PRIMARY KEY,
                data BLOB NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            ){table_clause}';
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

    def _drop_table_sql(self) -> "list[str]":
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
        with self._config.provide_session() as driver:
            _resolve_oracle_storage_capabilities_sync(driver)
            sql = self._table_ddl()
            driver.execute_script(sql)

        self._log_table_created()
    def _get(self, key: str, renew_for: "int | timedelta | None" = None) -> "bytes | None":
        """Synchronous implementation of get."""
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
                expires_in_seconds = _oracle_expiry_seconds(renew_for)
                if expires_in_seconds is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        updated_at = SYSTIMESTAMP
                    WHERE session_id = :session_id
                    """
                    cursor.execute(update_sql, {"expires_in_seconds": expires_in_seconds, "session_id": key})
                    conn.commit()

            return _read_blob_sync(data_blob)

    def _set(self, key: str, value: "str | bytes", expires_in: "int | timedelta | None" = None) -> None:
        """Synchronous implementation of set."""
        data = self._value_to_bytes(value)
        expires_in_seconds = _oracle_expiry_seconds(expires_in)

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
                        expires_at = CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (
                        :session_id,
                        EMPTY_BLOB(),
                        CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        SYSTIMESTAMP,
                        SYSTIMESTAMP
                    )
                """
                cursor.execute(merge_sql, {"session_id": key, "expires_in_seconds": expires_in_seconds})

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
                        expires_at = CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (session_id, data, expires_at, created_at, updated_at)
                    VALUES (
                        :session_id,
                        :data,
                        CASE
                            WHEN :expires_in_seconds IS NULL THEN NULL
                            ELSE SYSTIMESTAMP + NUMTODSINTERVAL(:expires_in_seconds, 'SECOND')
                        END,
                        SYSTIMESTAMP,
                        SYSTIMESTAMP
                    )
                """
                cursor.execute(sql, {"session_id": key, "data": data, "expires_in_seconds": expires_in_seconds})
                conn.commit()

    def _delete(self, key: str) -> None:
        """Synchronous implementation of delete."""
        sql = f"DELETE FROM {self._table_name} WHERE session_id = :session_id"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"session_id": key})
            conn.commit()

    def _delete_all(self) -> None:
        """Synchronous implementation of delete_all."""
        sql = f"DELETE FROM {self._table_name}"

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
        self._log_delete_all()

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

    def _expires_in(self, key: str) -> "int | None":
        """Synchronous implementation of expires_in."""
        sql = f"""
        SELECT expires_at, SYSTIMESTAMP FROM {self._table_name}
        WHERE session_id = :session_id
        """

        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"session_id": key})
            row = cursor.fetchone()

            if row is None or row[0] is None:
                return None

            expires_at, db_now = row

            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if db_now.tzinfo is None:
                db_now = db_now.replace(tzinfo=timezone.utc)

            if expires_at <= db_now:
                return 0

            delta = expires_at - db_now
            return int(delta.total_seconds())

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


def _oracle_expiry_seconds(expires_in: "int | timedelta | None") -> "int | None":
    """Convert a session TTL value to whole seconds for Oracle interval binds."""
    if expires_in is None:
        return None

    expires_in_seconds = int(expires_in.total_seconds()) if isinstance(expires_in, timedelta) else int(expires_in)
    if expires_in_seconds <= 0:
        return None
    return expires_in_seconds


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


def _litestar_table_feature_clause(config: Any, in_memory: bool) -> str:
    extension_config = cast("dict[str, Any]", config.extension_config)
    settings = cast("dict[str, Any]", extension_config.get("litestar", {}))
    report = _oracle_table_feature_report(
        config,
        "litestar",
        settings,
        "session",
        in_memory=in_memory,
        hash_partition_key="session_id",
        range_partition_key="expires_at",
        table_options_key="table_options",
    )
    return report["clause"]
