"""Oracle ADK memory store for Google Agent Development Kit memory storage."""

from typing import TYPE_CHECKING, Any, Final, cast

import oracledb

from sqlspec.adapters.oracledb.adk.store import (
    ORACLE_TABLE_NOT_FOUND_ERROR,
    JSONStorageType,
    coerce_decimal_values,
    storage_type_from_version,
)
from sqlspec.adapters.oracledb.data_dictionary import OracleAsyncDataDictionary, OracleSyncDataDictionary
from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_guards import is_async_readable, is_readable

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.adapters.oracledb.data_dictionary import OracleVersionInfo
    from sqlspec.extensions.adk.memory._types import MemoryRecord

logger = get_logger("adapters.oracledb.adk.memory_store")

__all__ = ("OracleAsyncADKMemoryStore", "OracleSyncADKMemoryStore")

ORACLE_DUPLICATE_KEY_ERROR: Final = 1


def _extract_json_value(data: Any) -> "dict[str, Any]":
    if isinstance(data, dict):
        return cast("dict[str, Any]", coerce_decimal_values(data))
    if isinstance(data, bytes):
        return from_json(data)  # type: ignore[no-any-return]
    if isinstance(data, str):
        return from_json(data)  # type: ignore[no-any-return]
    return from_json(str(data))  # type: ignore[no-any-return]


async def _read_lob_async(data: Any) -> Any:
    if is_async_readable(data):
        return await data.read()
    if is_readable(data):
        return data.read()
    return data


def _read_lob_sync(data: Any) -> Any:
    if is_readable(data):
        return data.read()
    return data


class OracleAsyncADKMemoryStore(BaseAsyncADKMemoryStore["OracleAsyncConfig"]):
    """Oracle ADK memory store using async oracledb driver."""

    __slots__ = ("_in_memory", "_json_storage_type", "_oracle_version_info")

    def __init__(self, config: "OracleAsyncConfig") -> None:
        super().__init__(config)
        self._json_storage_type: JSONStorageType | None = None
        self._oracle_version_info: OracleVersionInfo | None = None
        adk_config = config.extension_config.get("adk", {})
        self._in_memory: bool = bool(adk_config.get("in_memory", False))

    async def _detect_json_storage_type(self) -> "JSONStorageType":
        if self._json_storage_type is not None:
            return self._json_storage_type

        version_info = await self._get_version_info()
        self._json_storage_type = storage_type_from_version(version_info)
        return self._json_storage_type

    async def _get_version_info(self) -> "OracleVersionInfo | None":
        if self._oracle_version_info is not None:
            return self._oracle_version_info

        async with self._config.provide_session() as driver:
            dictionary = OracleAsyncDataDictionary()
            self._oracle_version_info = await dictionary.get_version(driver)

        if self._oracle_version_info is None:
            logger.warning("Could not detect Oracle version, defaulting to BLOB_JSON storage")

        return self._oracle_version_info

    async def _serialize_json_field(self, value: Any) -> "str | bytes | None":
        if value is None:
            return None

        storage_type = await self._detect_json_storage_type()
        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(value)
        return to_json(value, as_bytes=True)

    async def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        if data is None:
            return None

        if is_async_readable(data) or is_readable(data):
            data = await _read_lob_async(data)

        return _extract_json_value(data)

    async def _get_create_memory_table_sql(self) -> str:
        storage_type = await self._detect_json_storage_type()
        return self._get_create_memory_table_sql_for_type(storage_type)

    def _get_create_memory_table_sql_for_type(self, storage_type: "JSONStorageType") -> str:
        if storage_type == JSONStorageType.JSON_NATIVE:
            json_columns = """
                content_json JSON,
                metadata_json JSON
            """
        elif storage_type == JSONStorageType.BLOB_JSON:
            json_columns = """
                content_json BLOB CHECK (content_json IS JSON),
                metadata_json BLOB CHECK (metadata_json IS JSON)
            """
        else:
            json_columns = """
                content_json BLOB,
                metadata_json BLOB
            """

        owner_id_line = f",\n                {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_fts
                ON {self._memory_table}(content_text) INDEXTYPE IS CTXSYS.CONTEXT';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
            """

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._memory_table} (
                id VARCHAR2(128) PRIMARY KEY,
                session_id VARCHAR2(128) NOT NULL,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                event_id VARCHAR2(128) NOT NULL UNIQUE,
                author VARCHAR2(256){owner_id_line},
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                {json_columns},
                content_text CLOB NOT NULL,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            ){inmemory_clause}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_app_user_time
                ON {self._memory_table}(app_name, user_id, timestamp DESC)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_session
                ON {self._memory_table}(session_id)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        return [
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._memory_table}_session';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._memory_table}_app_user_time';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._memory_table}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
        ]

    async def create_tables(self) -> None:
        if not self._enabled:
            logger.debug("Memory store disabled, skipping table creation")
            return

        async with self._config.provide_session() as driver:
            await driver.execute_script(await self._get_create_memory_table_sql())
        logger.debug("Created ADK memory table: %s", self._memory_table)

    async def _execute_insert_entry(self, cursor: Any, sql: str, params: "dict[str, Any]") -> bool:
        """Execute an insert and skip duplicate key errors."""
        try:
            await cursor.execute(sql, params)
        except oracledb.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_DUPLICATE_KEY_ERROR:
                return False
            raise
        return True

    async def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        owner_column = f", {self._owner_id_column_name}" if self._owner_id_column_name else ""
        owner_param = ", :owner_id" if self._owner_id_column_name else ""
        sql = f"""
        INSERT INTO {self._memory_table} (
            id, session_id, app_name, user_id, event_id, author{owner_column},
            timestamp, content_json, content_text, metadata_json, inserted_at
        ) VALUES (
            :id, :session_id, :app_name, :user_id, :event_id, :author{owner_param},
            :timestamp, :content_json, :content_text, :metadata_json, :inserted_at
        )
        """

        inserted_count = 0
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            for entry in entries:
                content_json = await self._serialize_json_field(entry["content_json"])
                metadata_json = await self._serialize_json_field(entry["metadata_json"])
                params = {
                    "id": entry["id"],
                    "session_id": entry["session_id"],
                    "app_name": entry["app_name"],
                    "user_id": entry["user_id"],
                    "event_id": entry["event_id"],
                    "author": entry["author"],
                    "timestamp": entry["timestamp"],
                    "content_json": content_json,
                    "content_text": entry["content_text"],
                    "metadata_json": metadata_json,
                    "inserted_at": entry["inserted_at"],
                }
                if self._owner_id_column_name:
                    params["owner_id"] = str(owner_id) if owner_id is not None else None
                if await self._execute_insert_entry(cursor, sql, params):
                    inserted_count += 1
            await conn.commit()

        return inserted_count

    async def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        try:
            if self._use_fts:
                return await self._search_entries_fts(query, app_name, user_id, effective_limit)
            return await self._search_entries_simple(query, app_name, user_id, effective_limit)
        except oracledb.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    async def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM (
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at,
                   SCORE(1) AS score
            FROM {self._memory_table}
            WHERE app_name = :app_name
              AND user_id = :user_id
              AND CONTAINS(content_text, :query, 1) > 0
            ORDER BY score DESC, timestamp DESC
        )
        WHERE ROWNUM <= :limit
        """
        params = {"app_name": app_name, "user_id": user_id, "query": query, "limit": limit}
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()
        return await self._rows_to_records(rows)

    async def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM (
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {self._memory_table}
            WHERE app_name = :app_name
              AND user_id = :user_id
              AND LOWER(content_text) LIKE :pattern
            ORDER BY timestamp DESC
        )
        WHERE ROWNUM <= :limit
        """
        pattern = f"%{query.lower()}%"
        params = {"app_name": app_name, "user_id": user_id, "pattern": pattern, "limit": limit}
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()
        return await self._rows_to_records(rows)

    async def delete_entries_by_session(self, session_id: str) -> int:
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = :session_id"
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"session_id": session_id})
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    async def delete_entries_older_than(self, days: int) -> int:
        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < SYSTIMESTAMP - NUMTODSINTERVAL(:days, 'DAY')
        """
        async with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            await cursor.execute(sql, {"days": days})
            await conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    async def _rows_to_records(self, rows: "list[Any]") -> "list[MemoryRecord]":
        records: list[MemoryRecord] = []
        for row in rows:
            content_json = await self._deserialize_json_field(row[7]) if row[7] is not None else {}
            metadata_json = await self._deserialize_json_field(row[9])
            content_text = row[8]
            if is_async_readable(content_text) or is_readable(content_text):
                content_text = await _read_lob_async(content_text)
            records.append({
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "event_id": row[4],
                "author": row[5],
                "timestamp": row[6],
                "content_json": cast("dict[str, Any]", content_json),
                "content_text": str(content_text),
                "metadata_json": metadata_json,
                "inserted_at": row[10],
            })
        return records


class OracleSyncADKMemoryStore(BaseSyncADKMemoryStore["OracleSyncConfig"]):
    """Oracle ADK memory store using sync oracledb driver."""

    __slots__ = ("_in_memory", "_json_storage_type", "_oracle_version_info")

    def __init__(self, config: "OracleSyncConfig") -> None:
        super().__init__(config)
        self._json_storage_type: JSONStorageType | None = None
        self._oracle_version_info: OracleVersionInfo | None = None
        adk_config = config.extension_config.get("adk", {})
        self._in_memory = bool(adk_config.get("in_memory", False))

    def _detect_json_storage_type(self) -> "JSONStorageType":
        if self._json_storage_type is not None:
            return self._json_storage_type

        version_info = self._get_version_info()
        self._json_storage_type = storage_type_from_version(version_info)
        return self._json_storage_type

    def _get_version_info(self) -> "OracleVersionInfo | None":
        if self._oracle_version_info is not None:
            return self._oracle_version_info

        with self._config.provide_session() as driver:
            dictionary = OracleSyncDataDictionary()
            self._oracle_version_info = dictionary.get_version(driver)

        if self._oracle_version_info is None:
            logger.warning("Could not detect Oracle version, defaulting to BLOB_JSON storage")

        return self._oracle_version_info

    def _serialize_json_field(self, value: Any) -> "str | bytes | None":
        if value is None:
            return None

        storage_type = self._detect_json_storage_type()
        if storage_type == JSONStorageType.JSON_NATIVE:
            return to_json(value)
        return to_json(value, as_bytes=True)

    def _deserialize_json_field(self, data: Any) -> "dict[str, Any] | None":
        if data is None:
            return None

        if is_readable(data):
            data = _read_lob_sync(data)

        return _extract_json_value(data)

    def _get_create_memory_table_sql(self) -> str:
        storage_type = self._detect_json_storage_type()
        return self._get_create_memory_table_sql_for_type(storage_type)

    def _get_create_memory_table_sql_for_type(self, storage_type: "JSONStorageType") -> str:
        if storage_type == JSONStorageType.JSON_NATIVE:
            json_columns = """
                content_json JSON,
                metadata_json JSON
            """
        elif storage_type == JSONStorageType.BLOB_JSON:
            json_columns = """
                content_json BLOB CHECK (content_json IS JSON),
                metadata_json BLOB CHECK (metadata_json IS JSON)
            """
        else:
            json_columns = """
                content_json BLOB,
                metadata_json BLOB
            """

        owner_id_line = f",\n                {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        inmemory_clause = " INMEMORY PRIORITY HIGH" if self._in_memory else ""

        fts_index = ""
        if self._use_fts:
            fts_index = f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_fts
                ON {self._memory_table}(content_text) INDEXTYPE IS CTXSYS.CONTEXT';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
            """

        return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {self._memory_table} (
                id VARCHAR2(128) PRIMARY KEY,
                session_id VARCHAR2(128) NOT NULL,
                app_name VARCHAR2(128) NOT NULL,
                user_id VARCHAR2(128) NOT NULL,
                event_id VARCHAR2(128) NOT NULL UNIQUE,
                author VARCHAR2(256){owner_id_line},
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                {json_columns},
                content_text CLOB NOT NULL,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
            ){inmemory_clause}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_app_user_time
                ON {self._memory_table}(app_name, user_id, timestamp DESC)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX idx_{self._memory_table}_session
                ON {self._memory_table}(session_id)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        {fts_index}
        """

    def _get_drop_memory_table_sql(self) -> "list[str]":
        return [
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._memory_table}_session';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX idx_{self._memory_table}_app_user_time';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
            f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {self._memory_table}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
        ]

    def create_tables(self) -> None:
        if not self._enabled:
            logger.debug("Memory store disabled, skipping table creation")
            return

        with self._config.provide_session() as driver:
            driver.execute_script(self._get_create_memory_table_sql())
        logger.debug("Created ADK memory table: %s", self._memory_table)

    def _execute_insert_entry(self, cursor: Any, sql: str, params: "dict[str, Any]") -> bool:
        """Execute an insert and skip duplicate key errors."""
        try:
            cursor.execute(sql, params)
        except oracledb.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_DUPLICATE_KEY_ERROR:
                return False
            raise
        return True

    def insert_memory_entries(self, entries: "list[MemoryRecord]", owner_id: "object | None" = None) -> int:
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        if not entries:
            return 0

        owner_column = f", {self._owner_id_column_name}" if self._owner_id_column_name else ""
        owner_param = ", :owner_id" if self._owner_id_column_name else ""
        sql = f"""
        INSERT INTO {self._memory_table} (
            id, session_id, app_name, user_id, event_id, author{owner_column},
            timestamp, content_json, content_text, metadata_json, inserted_at
        ) VALUES (
            :id, :session_id, :app_name, :user_id, :event_id, :author{owner_param},
            :timestamp, :content_json, :content_text, :metadata_json, :inserted_at
        )
        """

        inserted_count = 0
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            for entry in entries:
                content_json = self._serialize_json_field(entry["content_json"])
                metadata_json = self._serialize_json_field(entry["metadata_json"])
                params = {
                    "id": entry["id"],
                    "session_id": entry["session_id"],
                    "app_name": entry["app_name"],
                    "user_id": entry["user_id"],
                    "event_id": entry["event_id"],
                    "author": entry["author"],
                    "timestamp": entry["timestamp"],
                    "content_json": content_json,
                    "content_text": entry["content_text"],
                    "metadata_json": metadata_json,
                    "inserted_at": entry["inserted_at"],
                }
                if self._owner_id_column_name:
                    params["owner_id"] = str(owner_id) if owner_id is not None else None
                if self._execute_insert_entry(cursor, sql, params):
                    inserted_count += 1
            conn.commit()

        return inserted_count

    def search_entries(
        self, query: str, app_name: str, user_id: str, limit: "int | None" = None
    ) -> "list[MemoryRecord]":
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)

        effective_limit = limit if limit is not None else self._max_results

        try:
            if self._use_fts:
                return self._search_entries_fts(query, app_name, user_id, effective_limit)
            return self._search_entries_simple(query, app_name, user_id, effective_limit)
        except oracledb.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if error_obj and error_obj.code == ORACLE_TABLE_NOT_FOUND_ERROR:
                return []
            raise

    def _search_entries_fts(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM (
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at,
                   SCORE(1) AS score
            FROM {self._memory_table}
            WHERE app_name = :app_name
              AND user_id = :user_id
              AND CONTAINS(content_text, :query, 1) > 0
            ORDER BY score DESC, timestamp DESC
        )
        WHERE ROWNUM <= :limit
        """
        params = {"app_name": app_name, "user_id": user_id, "query": query, "limit": limit}
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        return self._rows_to_records(rows)

    def _search_entries_simple(self, query: str, app_name: str, user_id: str, limit: int) -> "list[MemoryRecord]":
        sql = f"""
        SELECT id, session_id, app_name, user_id, event_id, author,
               timestamp, content_json, content_text, metadata_json, inserted_at
        FROM (
            SELECT id, session_id, app_name, user_id, event_id, author,
                   timestamp, content_json, content_text, metadata_json, inserted_at
            FROM {self._memory_table}
            WHERE app_name = :app_name
              AND user_id = :user_id
              AND LOWER(content_text) LIKE :pattern
            ORDER BY timestamp DESC
        )
        WHERE ROWNUM <= :limit
        """
        pattern = f"%{query.lower()}%"
        params = {"app_name": app_name, "user_id": user_id, "pattern": pattern, "limit": limit}
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        return self._rows_to_records(rows)

    def delete_entries_by_session(self, session_id: str) -> int:
        sql = f"DELETE FROM {self._memory_table} WHERE session_id = :session_id"
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"session_id": session_id})
            conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    def delete_entries_older_than(self, days: int) -> int:
        sql = f"""
        DELETE FROM {self._memory_table}
        WHERE inserted_at < SYSTIMESTAMP - NUMTODSINTERVAL(:days, 'DAY')
        """
        with self._config.provide_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"days": days})
            conn.commit()
            return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    def _rows_to_records(self, rows: "list[Any]") -> "list[MemoryRecord]":
        records: list[MemoryRecord] = []
        for row in rows:
            content_json = self._deserialize_json_field(row[7]) if row[7] is not None else {}
            metadata_json = self._deserialize_json_field(row[9])
            content_text = row[8]
            if is_readable(content_text):
                content_text = _read_lob_sync(content_text)
            records.append({
                "id": row[0],
                "session_id": row[1],
                "app_name": row[2],
                "user_id": row[3],
                "event_id": row[4],
                "author": row[5],
                "timestamp": row[6],
                "content_json": cast("dict[str, Any]", content_json),
                "content_text": str(content_text),
                "metadata_json": metadata_json,
                "inserted_at": row[10],
            })
        return records
