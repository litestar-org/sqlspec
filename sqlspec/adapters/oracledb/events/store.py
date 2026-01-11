"""Oracle event queue stores with auto-detected JSON storage.

JSON storage is automatically detected based on Oracle version:
- json: Native JSON type (Oracle 21c+ with COMPATIBLE >= 20)
- blob_json: BLOB with IS JSON constraint (Oracle 12c+, recommended)
- blob: Plain BLOB without constraint (Oracle 11g and earlier)

Note: CLOB should be avoided for JSON storage in Oracle. Oracle recommends
BLOB over CLOB for JSON data as BLOB performs significantly better.

Configuration (optional override):
    extension_config={
        "events": {
            "json_storage": "blob_json",  # Override auto-detection
            "in_memory": False  # Enable INMEMORY PRIORITY HIGH
        }
    }
"""

import logging
from enum import Enum
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.oracledb.data_dictionary import (
    OracledbAsyncDataDictionary,
    OracledbSyncDataDictionary,
    OracleVersionInfo,
)
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig

__all__ = ("OracleAsyncEventQueueStore", "OracleSyncEventQueueStore")

logger = get_logger("sqlspec.adapters.oracledb.events.store")


class JSONStorageType(Enum):
    """Oracle JSON storage types based on database version."""

    JSON_NATIVE = "json"
    BLOB_JSON = "blob_json"
    BLOB_PLAIN = "blob"


def _storage_type_from_version(version_info: "OracleVersionInfo | None") -> JSONStorageType:
    """Determine JSON storage type based on Oracle version metadata."""
    if version_info and version_info.supports_native_json():
        log_with_context(
            logger,
            logging.DEBUG,
            "events.queue.storage.detected",
            storage_type=JSONStorageType.JSON_NATIVE.value,
            version=str(version_info),
        )
        return JSONStorageType.JSON_NATIVE

    if version_info and version_info.supports_json_blob():
        log_with_context(
            logger,
            logging.DEBUG,
            "events.queue.storage.detected",
            storage_type=JSONStorageType.BLOB_JSON.value,
            version=str(version_info),
        )
        return JSONStorageType.BLOB_JSON

    log_with_context(
        logger,
        logging.DEBUG,
        "events.queue.storage.detected",
        storage_type=JSONStorageType.BLOB_PLAIN.value,
        version=str(version_info),
    )
    return JSONStorageType.BLOB_PLAIN


def _init_oracle_settings(extension_settings: "dict[str, Any]") -> "tuple[bool, JSONStorageType | None]":
    """Initialize Oracle-specific settings from extension config.

    Args:
        extension_settings: The events extension settings dict.

    Returns:
        Tuple of (in_memory, json_storage) settings.
    """
    in_memory = bool(extension_settings.get("in_memory", False))

    json_storage_override = extension_settings.get("json_storage")
    if json_storage_override == "json":
        json_storage: JSONStorageType | None = JSONStorageType.JSON_NATIVE
    elif json_storage_override == "blob_json":
        json_storage = JSONStorageType.BLOB_JSON
    elif json_storage_override == "blob":
        json_storage = JSONStorageType.BLOB_PLAIN
    else:
        json_storage = None

    return in_memory, json_storage


def _build_oracle_create_table_sql(
    table_name: str, storage_type: "JSONStorageType", in_memory: bool, index_name: str
) -> str:
    """Build Oracle CREATE TABLE and INDEX SQL as a single PL/SQL script.

    Args:
        table_name: The queue table name.
        storage_type: JSON storage type (native, blob_json, or blob).
        in_memory: Whether to add INMEMORY clause.
        index_name: The index name to create.

    Returns:
        PL/SQL script for creating table and index.
    """
    inmemory_clause = "INMEMORY PRIORITY HIGH" if in_memory else ""

    if storage_type == JSONStorageType.JSON_NATIVE:
        payload_col = "payload_json JSON NOT NULL"
        metadata_col = "metadata_json JSON"
    elif storage_type == JSONStorageType.BLOB_JSON:
        payload_col = "payload_json BLOB CHECK (payload_json IS JSON) NOT NULL"
        metadata_col = "metadata_json BLOB CHECK (metadata_json IS JSON)"
    else:
        payload_col = "payload_json BLOB NOT NULL"
        metadata_col = "metadata_json BLOB"

    return f"""
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE {table_name} (
                event_id VARCHAR2(64) PRIMARY KEY,
                channel VARCHAR2(128) NOT NULL,
                {payload_col},
                {metadata_col},
                status VARCHAR2(32) DEFAULT ''pending'' NOT NULL,
                available_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                lease_expires_at TIMESTAMP,
                attempts NUMBER(10) DEFAULT 0 NOT NULL,
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                acknowledged_at TIMESTAMP
            ) {inmemory_clause}';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;

        BEGIN
            EXECUTE IMMEDIATE 'CREATE INDEX {index_name}
                ON {table_name}(channel, status, available_at)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """


def _build_oracle_drop_sql(table_name: str, index_name: str) -> "list[str]":
    """Build Oracle DROP TABLE SQL with PL/SQL error handling.

    Args:
        table_name: The queue table name.
        index_name: The index name to drop.

    Returns:
        List of PL/SQL scripts for dropping index and table.
    """
    return [
        f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP INDEX {index_name}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -1418 THEN
                        RAISE;
                    END IF;
            END;
            """,
        f"""
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE {table_name}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
    ]


class OracleSyncEventQueueStore(BaseEventQueueStore["OracleSyncConfig"]):
    """Oracle sync event queue store with auto-detected JSON storage.

    Automatically detects the Oracle version and uses the optimal JSON storage:
    - Oracle 21c+: Native JSON type
    - Oracle 12c-19c: BLOB with IS JSON constraint
    - Oracle 11g: Plain BLOB

    Args:
        config: OracleSyncConfig with extension_config["events"] settings.

    Notes:
        Configuration is read from config.extension_config["events"]:
        - queue_table: Table name (default: "sqlspec_event_queue")
        - json_storage: Override auto-detection ("json", "blob_json", "blob")
        - in_memory: Enable INMEMORY PRIORITY HIGH clause (default: False)

    Example:
        from sqlspec.adapters.oracledb import OracleSyncConfig
        from sqlspec.adapters.oracledb.events import OracleSyncEventQueueStore

        config = OracleSyncConfig(connection_config={"dsn": "oracle://..."})
        store = OracleSyncEventQueueStore(config)
        store.create_table()  # Auto-detects version and creates table
    """

    __slots__ = ("_in_memory", "_json_storage", "_oracle_version_info")

    def __init__(self, config: "OracleSyncConfig") -> None:
        """Initialize Oracle sync event queue store."""
        super().__init__(config)
        self._in_memory, self._json_storage = _init_oracle_settings(self._extension_settings)
        self._oracle_version_info: OracleVersionInfo | None = None

    def _column_types(self) -> "tuple[str, str, str]":
        """Return Oracle column types based on storage mode."""
        storage = self._json_storage or JSONStorageType.BLOB_JSON
        if storage == JSONStorageType.JSON_NATIVE:
            return "JSON", "JSON", "TIMESTAMP"
        return "BLOB", "BLOB", "TIMESTAMP"

    def _string_type(self, length: int) -> str:
        """Return Oracle VARCHAR2 type syntax."""
        return f"VARCHAR2({length})"

    def _index_name(self) -> str:
        """Return index name truncated to Oracle's 30-character limit."""
        base_name = f"idx_{self.table_name.replace('.', '_')}_channel_status"
        return base_name[:30]

    def create_statements(self) -> "list[str]":
        """Return single PL/SQL script for table and index creation.

        Uses cached storage type if available, otherwise defaults to BLOB_JSON.
        For auto-detection, use create_table() instead.
        """
        storage_type = self._json_storage or JSONStorageType.BLOB_JSON
        return [_build_oracle_create_table_sql(self.table_name, storage_type, self._in_memory, self._index_name())]

    def drop_statements(self) -> "list[str]":
        """Return drop statements in reverse dependency order."""
        return _build_oracle_drop_sql(self.table_name, self._index_name())

    def _detect_json_storage_type(self) -> JSONStorageType:
        """Detect the appropriate JSON storage type based on Oracle version.

        Returns cached storage type if already detected, otherwise queries the database.
        """
        if self._json_storage is not None:
            return self._json_storage

        version_info = self._get_version_info()
        self._json_storage = _storage_type_from_version(version_info)
        return self._json_storage

    def _get_version_info(self) -> "OracleVersionInfo | None":
        """Return cached Oracle version info using data dictionary."""
        if self._oracle_version_info is not None:
            return self._oracle_version_info

        with self._config.provide_session() as driver:
            dictionary = OracledbSyncDataDictionary()
            self._oracle_version_info = dictionary.get_version(driver)

        if self._oracle_version_info is None:
            log_with_context(
                logger,
                logging.WARNING,
                "events.queue.storage.fallback",
                storage_type=JSONStorageType.BLOB_JSON.value,
                reason="version_detection_failed",
            )

        return self._oracle_version_info

    def create_table(self) -> None:
        """Create the event queue table with auto-detected storage type."""
        storage_type = self._detect_json_storage_type()
        log_with_context(
            logger, logging.DEBUG, "events.queue.create", storage_type=storage_type.value, table_name=self.table_name
        )

        with self._config.provide_session() as driver:
            sql = _build_oracle_create_table_sql(self.table_name, storage_type, self._in_memory, self._index_name())
            driver.execute_script(sql)

    def drop_table(self) -> None:
        """Drop the event queue table and index."""
        with self._config.provide_session() as driver:
            for stmt in _build_oracle_drop_sql(self.table_name, self._index_name()):
                driver.execute_script(stmt)


class OracleAsyncEventQueueStore(BaseEventQueueStore["OracleAsyncConfig"]):
    """Oracle async event queue store with auto-detected JSON storage.

    Automatically detects the Oracle version and uses the optimal JSON storage:
    - Oracle 21c+: Native JSON type
    - Oracle 12c-19c: BLOB with IS JSON constraint
    - Oracle 11g: Plain BLOB

    Args:
        config: OracleAsyncConfig with extension_config["events"] settings.

    Notes:
        Configuration is read from config.extension_config["events"]:
        - queue_table: Table name (default: "sqlspec_event_queue")
        - json_storage: Override auto-detection ("json", "blob_json", "blob")
        - in_memory: Enable INMEMORY PRIORITY HIGH clause (default: False)

    Example:
        from sqlspec.adapters.oracledb import OracleAsyncConfig
        from sqlspec.adapters.oracledb.events import OracleAsyncEventQueueStore

        config = OracleAsyncConfig(connection_config={"dsn": "oracle://..."})
        store = OracleAsyncEventQueueStore(config)
        await store.create_table()  # Auto-detects version and creates table
    """

    __slots__ = ("_in_memory", "_json_storage", "_oracle_version_info")

    def __init__(self, config: "OracleAsyncConfig") -> None:
        """Initialize Oracle async event queue store."""
        super().__init__(config)
        self._in_memory, self._json_storage = _init_oracle_settings(self._extension_settings)
        self._oracle_version_info: OracleVersionInfo | None = None

    def _column_types(self) -> "tuple[str, str, str]":
        """Return Oracle column types based on storage mode."""
        storage = self._json_storage or JSONStorageType.BLOB_JSON
        if storage == JSONStorageType.JSON_NATIVE:
            return "JSON", "JSON", "TIMESTAMP"
        return "BLOB", "BLOB", "TIMESTAMP"

    def _string_type(self, length: int) -> str:
        """Return Oracle VARCHAR2 type syntax."""
        return f"VARCHAR2({length})"

    def _index_name(self) -> str:
        """Return index name truncated to Oracle's 30-character limit."""
        base_name = f"idx_{self.table_name.replace('.', '_')}_channel_status"
        return base_name[:30]

    def create_statements(self) -> "list[str]":
        """Return single PL/SQL script for table and index creation.

        Uses cached storage type if available, otherwise defaults to BLOB_JSON.
        For auto-detection, use create_table() instead.
        """
        storage_type = self._json_storage or JSONStorageType.BLOB_JSON
        return [_build_oracle_create_table_sql(self.table_name, storage_type, self._in_memory, self._index_name())]

    def drop_statements(self) -> "list[str]":
        """Return drop statements in reverse dependency order."""
        return _build_oracle_drop_sql(self.table_name, self._index_name())

    async def _detect_json_storage_type(self) -> JSONStorageType:
        """Detect the appropriate JSON storage type based on Oracle version.

        Returns cached storage type if already detected, otherwise queries the database.
        """
        if self._json_storage is not None:
            return self._json_storage

        version_info = await self._get_version_info()
        self._json_storage = _storage_type_from_version(version_info)
        return self._json_storage

    async def _get_version_info(self) -> "OracleVersionInfo | None":
        """Return cached Oracle version info using data dictionary."""
        if self._oracle_version_info is not None:
            return self._oracle_version_info

        async with self._config.provide_session() as driver:
            dictionary = OracledbAsyncDataDictionary()
            self._oracle_version_info = await dictionary.get_version(driver)

        if self._oracle_version_info is None:
            log_with_context(
                logger,
                logging.WARNING,
                "events.queue.storage.fallback",
                storage_type=JSONStorageType.BLOB_JSON.value,
                reason="version_detection_failed",
            )

        return self._oracle_version_info

    async def create_table(self) -> None:
        """Create the event queue table with auto-detected storage type."""
        storage_type = await self._detect_json_storage_type()
        log_with_context(
            logger, logging.DEBUG, "events.queue.create", storage_type=storage_type.value, table_name=self.table_name
        )

        async with self._config.provide_session() as driver:
            sql = _build_oracle_create_table_sql(self.table_name, storage_type, self._in_memory, self._index_name())
            await driver.execute_script(sql)

    async def drop_table(self) -> None:
        """Drop the event queue table and index."""
        async with self._config.provide_session() as driver:
            for stmt in _build_oracle_drop_sql(self.table_name, self._index_name()):
                await driver.execute_script(stmt)
