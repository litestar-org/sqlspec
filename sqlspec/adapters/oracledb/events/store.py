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

from enum import Enum
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.oracledb.data_dictionary import (
    OracleAsyncDataDictionary,
    OracleSyncDataDictionary,
    OracleVersionInfo,
)
from sqlspec.extensions.events._store import BaseEventQueueStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig

__all__ = ("OracleAsyncEventQueueStore", "OracleSyncEventQueueStore")

logger = get_logger("adapters.oracledb.events.store")


class JSONStorageType(Enum):
    """Oracle JSON storage types based on database version."""

    JSON_NATIVE = "json"
    BLOB_JSON = "blob_json"
    BLOB_PLAIN = "blob"


def _storage_type_from_version(version_info: "OracleVersionInfo | None") -> JSONStorageType:
    """Determine JSON storage type based on Oracle version metadata."""
    if version_info and version_info.supports_native_json():
        logger.debug("Detected Oracle %s with compatible >= 20, using JSON_NATIVE", version_info)
        return JSONStorageType.JSON_NATIVE

    if version_info and version_info.supports_json_blob():
        logger.debug("Detected Oracle %s with IS JSON support, using BLOB_JSON", version_info)
        return JSONStorageType.BLOB_JSON

    logger.debug("Oracle version %s, using BLOB_PLAIN", version_info)
    return JSONStorageType.BLOB_PLAIN


class _OracleEventQueueStoreMixin:
    """Mixin providing shared Oracle event queue store logic.

    This mixin contains all Oracle-specific DDL generation logic shared between
    sync and async store implementations. Subclasses must provide the following
    attributes (typically set in __init__):
    - _in_memory: bool
    - _json_storage: JSONStorageType | None
    - _oracle_version_info: OracleVersionInfo | None
    - table_name: str (from BaseEventQueueStore property)

    Slots are defined in the concrete subclasses to allow proper attribute assignment.
    """

    __slots__ = ()

    if TYPE_CHECKING:
        _in_memory: bool
        _json_storage: "JSONStorageType | None"
        _oracle_version_info: "OracleVersionInfo | None"
        _extension_settings: "dict[str, Any]"

        @property
        def table_name(self) -> str: ...

    def _init_oracle_settings(self) -> None:
        """Initialize Oracle-specific settings from extension config.

        Must be called from subclass __init__ after super().__init__().
        Note: Attributes assigned here are defined in subclass __slots__.
        """
        events_config = self._extension_settings
        self._in_memory = bool(events_config.get("in_memory", False))  # type: ignore[misc]
        self._oracle_version_info = None  # type: ignore[misc]

        json_storage_override = events_config.get("json_storage")
        if json_storage_override == "json":
            self._json_storage = JSONStorageType.JSON_NATIVE  # type: ignore[misc]
        elif json_storage_override == "blob_json":
            self._json_storage = JSONStorageType.BLOB_JSON  # type: ignore[misc]
        elif json_storage_override == "blob":
            self._json_storage = JSONStorageType.BLOB_PLAIN  # type: ignore[misc]
        else:
            self._json_storage = None  # type: ignore[misc]

    def _column_types(self) -> "tuple[str, str, str]":
        """Return Oracle column types based on storage mode."""
        storage = self._json_storage or JSONStorageType.BLOB_JSON
        if storage == JSONStorageType.JSON_NATIVE:
            return "JSON", "JSON", "TIMESTAMP"
        return "BLOB", "BLOB", "TIMESTAMP"

    def _index_name(self) -> str:
        """Return index name truncated to Oracle's 30-character limit."""
        base_name = f"idx_{self.table_name.replace('.', '_')}_channel_status"
        return base_name[:30]

    def _get_create_table_sql(self, storage_type: "JSONStorageType") -> str:
        """Get Oracle CREATE TABLE and INDEX SQL as a single PL/SQL script."""
        inmemory_clause = "INMEMORY PRIORITY HIGH" if self._in_memory else ""
        index_name = self._index_name()

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
            EXECUTE IMMEDIATE 'CREATE TABLE {self.table_name} (
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
                ON {self.table_name}(channel, status, available_at)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN
                    RAISE;
                END IF;
        END;
        """

    def _get_drop_table_sql(self) -> "list[str]":
        """Get Oracle DROP TABLE SQL with PL/SQL error handling."""
        index_name = self._index_name()
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
                EXECUTE IMMEDIATE 'DROP TABLE {self.table_name}';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """,
        ]

    def create_statements(self) -> "list[str]":
        """Return single PL/SQL script for table and index creation.

        Uses cached storage type if available, otherwise defaults to BLOB_JSON.
        For auto-detection, use create_table() instead.
        """
        storage_type = self._json_storage or JSONStorageType.BLOB_JSON
        return [self._get_create_table_sql(storage_type)]

    def drop_statements(self) -> "list[str]":
        """Return drop statements in reverse dependency order."""
        return self._get_drop_table_sql()


class OracleSyncEventQueueStore(_OracleEventQueueStoreMixin, BaseEventQueueStore["OracleSyncConfig"]):
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
        self._init_oracle_settings()

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
            dictionary = OracleSyncDataDictionary()
            self._oracle_version_info = dictionary.get_version(driver)

        if self._oracle_version_info is None:
            logger.warning("Could not detect Oracle version, defaulting to BLOB_JSON storage")

        return self._oracle_version_info

    def create_table(self) -> None:
        """Create the event queue table with auto-detected storage type."""
        storage_type = self._detect_json_storage_type()
        logger.debug("Creating event queue table with storage type: %s", storage_type.value)

        with self._config.provide_session() as driver:
            driver.execute_script(self._get_create_table_sql(storage_type))

    def drop_table(self) -> None:
        """Drop the event queue table and index."""
        with self._config.provide_session() as driver:
            for stmt in self._get_drop_table_sql():
                driver.execute_script(stmt)


class OracleAsyncEventQueueStore(_OracleEventQueueStoreMixin, BaseEventQueueStore["OracleAsyncConfig"]):
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
        self._init_oracle_settings()

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
            dictionary = OracleAsyncDataDictionary()
            self._oracle_version_info = await dictionary.get_version(driver)

        if self._oracle_version_info is None:
            logger.warning("Could not detect Oracle version, defaulting to BLOB_JSON storage")

        return self._oracle_version_info

    async def create_table(self) -> None:
        """Create the event queue table with auto-detected storage type."""
        storage_type = await self._detect_json_storage_type()
        logger.debug("Creating event queue table with storage type: %s", storage_type.value)

        async with self._config.provide_session() as driver:
            await driver.execute_script(self._get_create_table_sql(storage_type))

    async def drop_table(self) -> None:
        """Drop the event queue table and index."""
        async with self._config.provide_session() as driver:
            for stmt in self._get_drop_table_sql():
                await driver.execute_script(stmt)
