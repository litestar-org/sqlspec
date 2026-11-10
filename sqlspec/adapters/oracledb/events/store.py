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
            "json_storage": "blob_json",
            "in_memory": False,
        }
    }
"""

import contextlib
import logging
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal, NotRequired

from typing_extensions import TypedDict

from sqlspec.adapters.oracledb._storage import oracle_table_feature_report, validate_oracle_identifier
from sqlspec.adapters.oracledb.data_dictionary import JSONStorageType, storage_type_from_version
from sqlspec.adapters.oracledb.events._hub import _channel_queue_name
from sqlspec.config import EventsConfig
from sqlspec.exceptions import EventChannelError, MigrationError
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.extensions.events._names import normalize_event_channel_name
from sqlspec.migrations.schema import SchemaTarget
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
    from sqlspec.adapters.oracledb.data_dictionary import OracleVersionInfo

__all__ = (
    "OracleAsyncEventQueueStore",
    "OracleEventsCompressionConfig",
    "OracleEventsConfig",
    "OracleEventsPartitionConfig",
    "OracleSyncEventQueueStore",
)

logger = get_logger("sqlspec.adapters.oracledb.events.store")

_ORACLE_NATIVE_BACKENDS: Final[frozenset[str]] = frozenset({"aq", "txeventq"})
_ORACLE_JSON_QUEUE_MIN_MAJOR: Final[int] = 21
_DEFAULT_AQ_QUEUE: Final[str] = "SQLSPEC_EVENTS_QUEUE"
_PROBE_USER_QUEUE_SQL: Final[str] = (
    "SELECT name, TRIM(enqueue_enabled), TRIM(dequeue_enabled) FROM user_queues WHERE name = :name"
)
_PROBE_USER_QUEUE_TABLE_SQL: Final[str] = "SELECT queue_table FROM user_queue_tables WHERE queue_table = :name"
_CREATE_QUEUE_TABLE_PLSQL: Final[str] = (
    "BEGIN DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table => :queue_table, queue_payload_type => 'JSON'); END;"
)
_CREATE_QUEUE_PLSQL: Final[str] = (
    "BEGIN DBMS_AQADM.CREATE_QUEUE(queue_name => :queue_name, queue_table => :queue_table); END;"
)
_CREATE_TXEVENTQ_PLSQL: Final[str] = (
    "BEGIN DBMS_AQADM.CREATE_TRANSACTIONAL_EVENT_QUEUE(queue_name => :queue_name, queue_payload_type => 'JSON'); END;"
)
_START_QUEUE_PLSQL: Final[str] = "BEGIN DBMS_AQADM.START_QUEUE(queue_name => :queue_name); END;"


class OracleEventsCompressionConfig(TypedDict):
    """Oracle queue-table compression settings."""

    enabled: NotRequired[bool]
    """Enable compression. Default: False."""

    algorithm: NotRequired[str]
    """Oracle compression algorithm key. Default: advanced."""


class OracleEventsPartitionConfig(TypedDict):
    """Oracle queue-table hash or range partition settings."""

    strategy: NotRequired[str]
    """Partitioning strategy: hash or range; omitted to disable partitioning."""

    partition_count: NotRequired[int]
    """Hash partition count, at least two. Default: 16."""

    partitions: NotRequired[int]
    """Alternative name for partition_count."""

    interval: NotRequired[str]
    """Range interval: day, week, month or year. Default: month."""

    initial_less_than: NotRequired[str]
    """Initial range upper bound. Default: TIMESTAMP '2000-01-01 00:00:00'."""

    partition_key: NotRequired[str]
    """Partition key; defaults to event_id for hash and available_at for range."""

    queue_partition_key: NotRequired[str]
    """Queue-specific override for partition_key."""


class OracleEventsConfig(EventsConfig):
    """Oracle events settings for queue storage and supported native transports."""

    in_memory: NotRequired[bool]
    """Emit Oracle INMEMORY PRIORITY HIGH. Default: False."""

    partitioning: NotRequired[OracleEventsPartitionConfig]
    """Oracle hash/range queue partition settings; omitted for an unpartitioned table."""

    compression: NotRequired[OracleEventsCompressionConfig]
    """Oracle queue-table compression; disabled unless explicitly enabled."""

    table_options: NotRequired[str]
    """Additional Oracle queue-table DDL options; omitted by default."""

    json_storage: NotRequired[Literal["json", "blob_json", "blob_plain"]]
    """Override Oracle JSON storage detection with json, blob_json or blob_plain."""

    aq_queue: NotRequired[str]
    """Oracle AQ queue name or channel template. Default: SQLSPEC_EVENTS_QUEUE."""

    aq_channels: NotRequired[list[str]]
    """Channels reconcile provisions for a {channel} aq_queue template; never restricts runtime publish or dequeue."""

    aq_visibility: NotRequired[int | Literal["DEQ_IMMEDIATE", "DEQ_ON_COMMIT"]]
    """Oracle AQ visibility integer or DEQ_IMMEDIATE/DEQ_ON_COMMIT name."""

    aq_wait_seconds: NotRequired[int]
    """Oracle AQ dequeue wait in seconds. Default: 5."""


class OracleSyncEventQueueStore(BaseEventQueueStore["OracleSyncConfig"]):
    """Oracle sync event queue store with auto-detected JSON storage.

    Automatically detects the Oracle version and uses the optimal JSON storage:
    - Oracle 21c+: Native JSON type
    - Oracle 12c-19c: BLOB with IS JSON constraint
    - Oracle 11g: Plain BLOB

    Args:
        config: OracleSyncConfig with extension_config["events"] settings.
    """

    __slots__ = ("_in_memory", "_json_storage_override")
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({
        "aq_channels",
        "aq_queue",
        "aq_visibility",
        "aq_wait_seconds",
        "compression",
        "in_memory",
        "json_storage",
        "partitioning",
        "table_options",
    })

    def __init__(self, config: "OracleSyncConfig") -> None:
        """Initialize Oracle sync event queue store."""
        super().__init__(config)
        for part in self.table_name.split("."):
            validate_oracle_identifier(part, "table name")
        self._in_memory, self._json_storage_override = _init_oracle_settings(self._extension_settings)

    def create_statements(self) -> "list[str]":
        """Return single PL/SQL script for table and index creation.

        Uses the configured storage override when set, otherwise defaults to
        BLOB_JSON. For auto-detection, use create_table() instead.
        """
        storage_type = self._json_storage_override or JSONStorageType.BLOB_JSON
        return [_oracle_table_ddl(self.table_name, storage_type, self._table_feature_clause(), self._index_name())]

    def drop_statements(self) -> "list[str]":
        """Return drop statements in reverse dependency order."""
        return _oracle_drop_sql(self.table_name, self._index_name())

    def create_table(self) -> None:
        """Create the event queue table with auto-detected storage type."""
        storage_type = self._detect_json_storage_type()
        log_with_context(
            logger, logging.DEBUG, "events.queue.create", storage_type=storage_type.value, table_name=self.table_name
        )

        with self._config.provide_session() as driver:
            sql = _oracle_table_ddl(self.table_name, storage_type, self._table_feature_clause(), self._index_name())
            driver.execute_script(sql)

    def drop_table(self) -> None:
        """Drop the event queue table and index."""
        with self._config.provide_session() as driver:
            for stmt in _oracle_drop_sql(self.table_name, self._index_name()):
                driver.execute_script(stmt)

    def prepare_schema_sync(self, driver: Any) -> None:
        """Populate the pool-scoped Oracle version cache on an active synchronous driver."""
        if (
            self._json_storage_override is not None
            and self._extension_settings.get("backend") not in _ORACLE_NATIVE_BACKENDS
        ):
            return
        _prepare_oracle_events_schema_sync(self._config, driver)

    def native_backend_targets(self, backend_name: str) -> tuple[str, ...]:
        """Return validated Oracle AQ or TxEventQ queue names for reconcile provisioning."""
        return _oracle_native_backend_targets(self._config, self._extension_settings, backend_name)

    def reconcile_native_backend_sync(
        self, driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
    ) -> None:
        """Provision missing or stopped Oracle AQ or TxEventQ queues on a synchronous driver."""
        _reconcile_oracle_native_backend_sync(driver, backend_name, targets, create_schema=create_schema)

    async def reconcile_native_backend_async(
        self, driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
    ) -> None:
        """Provision missing or stopped Oracle AQ or TxEventQ queues on an asynchronous driver."""
        await _reconcile_oracle_native_backend_async(driver, backend_name, targets, create_schema=create_schema)

    def _schema_target(self) -> SchemaTarget:
        """Build the Oracle queue table SchemaTarget using cached version storage when resolved."""
        storage_type = _resolve_cached_oracle_json_storage(self._config, self._json_storage_override)
        return SchemaTarget.from_ddl(
            self.table_name,
            _oracle_table_ddl(self.table_name, storage_type, self._table_feature_clause(), self._index_name()),
            dialect="oracle",
        )

    def _index_existence_target(self) -> "tuple[str | None, str]":
        """Return optional schema and table name for catalog index lookup."""
        return _oracle_index_existence_target(self.table_name)

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Wrap Oracle companion index DDL in PL/SQL duplicate-index error handling."""
        return _oracle_wrap_create_statement(statement, object_type)

    def _column_types(self) -> "tuple[str, str, str]":
        """Return Oracle column types based on storage mode."""
        storage = self._json_storage_override or JSONStorageType.BLOB_JSON
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

    def _detect_json_storage_type(self) -> JSONStorageType:
        """Resolve the JSON storage type from the configured override or server version."""
        if self._json_storage_override is not None:
            return self._json_storage_override
        return storage_type_from_version(self._get_version_info())

    def _get_version_info(self) -> "OracleVersionInfo | None":
        """Return the pool-scoped Oracle version through the data dictionary."""
        cache = self._config._oracle_version_cache
        if cache.resolved:
            return cache.version

        with self._config.provide_session() as driver:
            version_info = driver.data_dictionary.get_version(driver)

        if version_info is None:
            log_with_context(
                logger,
                logging.WARNING,
                "events.queue.storage.fallback",
                storage_type=JSONStorageType.BLOB_JSON.value,
                reason="version_detection_failed",
            )

        return version_info

    def _table_feature_clause(self) -> str:
        report = oracle_table_feature_report(
            self._config,
            "events",
            self._extension_settings,
            "queue",
            in_memory=self._in_memory,
            hash_partition_key="event_id",
            range_partition_key="available_at",
            table_options_key="table_options",
        )
        return report["plsql_clause"]


class OracleAsyncEventQueueStore(BaseEventQueueStore["OracleAsyncConfig"]):
    """Oracle async event queue store with auto-detected JSON storage.

    Automatically detects the Oracle version and uses the optimal JSON storage:
    - Oracle 21c+: Native JSON type
    - Oracle 12c-19c: BLOB with IS JSON constraint
    - Oracle 11g: Plain BLOB

    Args:
        config: OracleAsyncConfig with extension_config["events"] settings.
    """

    __slots__ = ("_in_memory", "_json_storage_override")
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({
        "aq_channels",
        "aq_queue",
        "aq_visibility",
        "aq_wait_seconds",
        "compression",
        "in_memory",
        "json_storage",
        "partitioning",
        "table_options",
    })

    def __init__(self, config: "OracleAsyncConfig") -> None:
        """Initialize Oracle async event queue store."""
        super().__init__(config)
        for part in self.table_name.split("."):
            validate_oracle_identifier(part, "table name")
        self._in_memory, self._json_storage_override = _init_oracle_settings(self._extension_settings)

    def create_statements(self) -> "list[str]":
        """Return single PL/SQL script for table and index creation.

        Uses the configured storage override when set, otherwise defaults to
        BLOB_JSON. For auto-detection, use create_table() instead.
        """
        storage_type = self._json_storage_override or JSONStorageType.BLOB_JSON
        return [_oracle_table_ddl(self.table_name, storage_type, self._table_feature_clause(), self._index_name())]

    def drop_statements(self) -> "list[str]":
        """Return drop statements in reverse dependency order."""
        return _oracle_drop_sql(self.table_name, self._index_name())

    async def create_table(self) -> None:
        """Create the event queue table with auto-detected storage type."""
        storage_type = await self._detect_json_storage_type()
        log_with_context(
            logger, logging.DEBUG, "events.queue.create", storage_type=storage_type.value, table_name=self.table_name
        )

        async with self._config.provide_session() as driver:
            sql = _oracle_table_ddl(self.table_name, storage_type, self._table_feature_clause(), self._index_name())
            await driver.execute_script(sql)

    async def drop_table(self) -> None:
        """Drop the event queue table and index."""
        async with self._config.provide_session() as driver:
            for stmt in _oracle_drop_sql(self.table_name, self._index_name()):
                await driver.execute_script(stmt)

    async def prepare_schema_async(self, driver: Any) -> None:
        """Populate the pool-scoped Oracle version cache on an active asynchronous driver."""
        if (
            self._json_storage_override is not None
            and self._extension_settings.get("backend") not in _ORACLE_NATIVE_BACKENDS
        ):
            return
        await _prepare_oracle_events_schema_async(self._config, driver)

    def native_backend_targets(self, backend_name: str) -> tuple[str, ...]:
        """Return validated Oracle AQ or TxEventQ queue names for reconcile provisioning."""
        return _oracle_native_backend_targets(self._config, self._extension_settings, backend_name)

    def reconcile_native_backend_sync(
        self, driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
    ) -> None:
        """Provision missing or stopped Oracle AQ or TxEventQ queues on a synchronous driver."""
        _reconcile_oracle_native_backend_sync(driver, backend_name, targets, create_schema=create_schema)

    async def reconcile_native_backend_async(
        self, driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
    ) -> None:
        """Provision missing or stopped Oracle AQ or TxEventQ queues on an asynchronous driver."""
        await _reconcile_oracle_native_backend_async(driver, backend_name, targets, create_schema=create_schema)

    def _schema_target(self) -> SchemaTarget:
        """Build the Oracle queue table SchemaTarget using cached version storage when resolved."""
        storage_type = _resolve_cached_oracle_json_storage(self._config, self._json_storage_override)
        return SchemaTarget.from_ddl(
            self.table_name,
            _oracle_table_ddl(self.table_name, storage_type, self._table_feature_clause(), self._index_name()),
            dialect="oracle",
        )

    def _index_existence_target(self) -> "tuple[str | None, str]":
        """Return optional schema and table name for catalog index lookup."""
        return _oracle_index_existence_target(self.table_name)

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Wrap Oracle companion index DDL in PL/SQL duplicate-index error handling."""
        return _oracle_wrap_create_statement(statement, object_type)

    def _column_types(self) -> "tuple[str, str, str]":
        """Return Oracle column types based on storage mode."""
        storage = self._json_storage_override or JSONStorageType.BLOB_JSON
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

    async def _detect_json_storage_type(self) -> JSONStorageType:
        """Resolve the JSON storage type from the configured override or server version."""
        if self._json_storage_override is not None:
            return self._json_storage_override
        return storage_type_from_version(await self._get_version_info())

    async def _get_version_info(self) -> "OracleVersionInfo | None":
        """Return the pool-scoped Oracle version through the data dictionary."""
        cache = self._config._oracle_version_cache
        if cache.resolved:
            return cache.version

        async with self._config.provide_session() as driver:
            version_info = await driver.data_dictionary.get_version(driver)

        if version_info is None:
            log_with_context(
                logger,
                logging.WARNING,
                "events.queue.storage.fallback",
                storage_type=JSONStorageType.BLOB_JSON.value,
                reason="version_detection_failed",
            )

        return version_info

    def _table_feature_clause(self) -> str:
        report = oracle_table_feature_report(
            self._config,
            "events",
            self._extension_settings,
            "queue",
            in_memory=self._in_memory,
            hash_partition_key="event_id",
            range_partition_key="available_at",
            table_options_key="table_options",
        )
        return report["plsql_clause"]


def _prepare_oracle_events_schema_sync(config: "OracleSyncConfig", driver: Any) -> None:
    """Resolve and cache the pool-scoped Oracle version on a synchronous driver."""
    cache = getattr(config, "_oracle_version_cache", None)
    if cache is None or cache.resolved:
        return
    if getattr(driver, "_oracle_version_cache", None) is None:
        with contextlib.suppress(AttributeError):
            driver._oracle_version_cache = cache
    version_info = driver.data_dictionary.get_version(driver)
    cache.version = version_info
    cache.resolved = True


async def _prepare_oracle_events_schema_async(config: "OracleAsyncConfig", driver: Any) -> None:
    """Resolve and cache the pool-scoped Oracle version on an asynchronous driver."""
    cache = getattr(config, "_oracle_version_cache", None)
    if cache is None or cache.resolved:
        return
    if getattr(driver, "_oracle_version_cache", None) is None:
        with contextlib.suppress(AttributeError):
            driver._oracle_version_cache = cache
    version_info = await driver.data_dictionary.get_version(driver)
    cache.version = version_info
    cache.resolved = True


def _resolve_cached_oracle_json_storage(config: Any, override: "JSONStorageType | None") -> JSONStorageType:
    """Resolve Oracle JSON storage type from explicit override or warm version cache."""
    if override is not None:
        return override
    cache = getattr(config, "_oracle_version_cache", None)
    if cache is not None and cache.resolved:
        return storage_type_from_version(cache.version)
    return JSONStorageType.BLOB_JSON


def _oracle_index_existence_target(table_name: str) -> "tuple[str | None, str]":
    """Split a table name into optional schema and bare table name."""
    if "." in table_name:
        schema_name, bare_table = table_name.split(".", 1)
        return schema_name, bare_table
    return None, table_name


def _oracle_wrap_create_statement(statement: str, object_type: str) -> str:
    """Wrap Oracle CREATE INDEX statements in PL/SQL duplicate-index handling."""
    if object_type == "index":
        escaped = statement.replace("'", "''")
        return (
            f"BEGIN EXECUTE IMMEDIATE '{escaped}'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE NOT IN (-955, -1408) THEN RAISE; END IF; END;"
        )
    return statement


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
    elif json_storage_override in {"blob", "blob_plain"}:
        json_storage = JSONStorageType.BLOB_PLAIN
    else:
        json_storage = None

    return in_memory, json_storage


def _oracle_table_ddl(table_name: str, storage_type: "JSONStorageType", table_clause: str, index_name: str) -> str:
    """Build Oracle CREATE TABLE and INDEX SQL as a single PL/SQL script.

    Args:
        table_name: The queue table name.
        storage_type: JSON storage type (native, blob_json, or blob).
        table_clause: Capability-gated table storage clauses.
        index_name: The index name to create.

    Returns:
        PL/SQL script for creating table and index.
    """
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
            ){table_clause}';
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


def _oracle_drop_sql(table_name: str, index_name: str) -> "list[str]":
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


def _oracle_native_backend_targets(config: Any, settings: Mapping[str, Any], backend_name: str) -> tuple[str, ...]:
    """Validate and expand Oracle AQ or TxEventQ target queue names for reconcile."""
    if backend_name not in _ORACLE_NATIVE_BACKENDS:
        return ()

    cache = getattr(config, "_oracle_version_cache", None)
    if (
        cache is not None
        and cache.resolved
        and cache.version is not None
        and cache.version.major < _ORACLE_JSON_QUEUE_MIN_MAJOR
    ):
        msg = (
            f"Oracle events backend '{backend_name}' requires Oracle 21c or newer for JSON payload queues "
            f"(detected major version {cache.version.major}); upgrade to Oracle 21c+ or use "
            'migration_mode="versioned".'
        )
        raise MigrationError(msg)

    raw_template = settings.get("aq_queue", _DEFAULT_AQ_QUEUE)
    if raw_template is None:
        template = _DEFAULT_AQ_QUEUE
    elif isinstance(raw_template, str):
        template = raw_template
    else:
        msg = f"Invalid Oracle aq_queue {raw_template!r}: configure a bare unquoted queue name."
        raise MigrationError(msg)

    raw_channels = settings.get("aq_channels")
    if raw_channels is None:
        channels: list[Any] = []
    elif isinstance(raw_channels, str) or not isinstance(raw_channels, Sequence):
        msg = "Oracle events setting 'aq_channels' must be a sequence of channel names."
        raise MigrationError(msg)
    else:
        channels = list(raw_channels)

    has_channel_placeholder = "{channel}" in template
    if has_channel_placeholder and not channels:
        msg = (
            "Oracle events aq_queue template contains '{channel}', which requires non-empty 'aq_channels' "
            'during reconcile; configure aq_channels or use migration_mode="versioned".'
        )
        raise MigrationError(msg)
    if channels and not has_channel_placeholder:
        msg = (
            "Oracle events 'aq_channels' requires '{channel}' in 'aq_queue'; "
            "omit aq_channels for a fixed queue name or include '{channel}' in aq_queue."
        )
        raise MigrationError(msg)

    if has_channel_placeholder:
        resolved_names: list[str] = []
        for raw_channel in channels:
            if not isinstance(raw_channel, str):
                msg = f"Invalid events channel name in 'aq_channels': {raw_channel!r}"
                raise MigrationError(msg)
            try:
                normalized_channel = normalize_event_channel_name(raw_channel)
            except EventChannelError as exc:
                raise MigrationError(str(exc)) from exc
            expanded = _channel_queue_name(template, normalized_channel).upper()
            _validate_bare_oracle_queue_name(expanded)
            resolved_names.append(expanded)
        return tuple(dict.fromkeys(resolved_names))

    expanded = _channel_queue_name(template, "").upper()
    _validate_bare_oracle_queue_name(expanded)
    return (expanded,)


def _validate_bare_oracle_queue_name(queue_name: str) -> str:
    """Validate that an expanded Oracle queue name is a bare unquoted SQL identifier."""
    try:
        return validate_oracle_identifier(queue_name, "aq_queue")
    except ValueError as exc:
        msg = (
            f"Invalid Oracle aq_queue '{queue_name}': configure a bare unquoted queue name "
            "without schema qualification."
        )
        raise MigrationError(msg) from exc


def _extract_queue_flags(row: Any) -> tuple[str, str]:
    """Extract upper-cased enqueue and dequeue enabled flags from a user_queues row."""
    if isinstance(row, Mapping):
        lower_map = {str(key).lower(): val for key, val in row.items()}
        enq = lower_map.get("enqueue_enabled", lower_map.get("trim(enqueue_enabled)"))
        deq = lower_map.get("dequeue_enabled", lower_map.get("trim(dequeue_enabled)"))
        if enq is not None and deq is not None:
            return str(enq).strip().upper(), str(deq).strip().upper()
        values = tuple(row.values())
        return str(values[1]).strip().upper(), str(values[2]).strip().upper()
    return str(row[1]).strip().upper(), str(row[2]).strip().upper()


def _is_dbms_aqadm_privilege_error(exc: Exception) -> bool:
    """Return whether a driver error indicates missing DBMS_AQADM privileges."""
    text = str(exc).upper()
    if "ORA-01031" in text:
        return True
    return "ORA-06550" in text and "PLS-00201" in text and "DBMS_AQADM" in text


def _wrap_aqadm_privilege_error(queue_name: str, exc: Exception) -> MigrationError:
    """Build a MigrationError naming required Oracle queue-administration grants."""
    msg = (
        f"Oracle events queue provisioning for '{queue_name}' failed due to missing privileges; "
        "grant AQ_ADMINISTRATOR_ROLE and GRANT EXECUTE ON DBMS_AQADM to the database user."
    )
    return MigrationError(msg)


def _exec_aqadm_sync(driver: Any, queue_name: str, statement: str, parameters: dict[str, Any]) -> None:
    """Execute one DBMS_AQADM PL/SQL block synchronously, translating privilege refusals."""
    try:
        driver.execute(statement, parameters)
    except Exception as exc:
        if _is_dbms_aqadm_privilege_error(exc):
            raise _wrap_aqadm_privilege_error(queue_name, exc) from exc
        raise


async def _exec_aqadm_async(driver: Any, queue_name: str, statement: str, parameters: dict[str, Any]) -> None:
    """Execute one DBMS_AQADM PL/SQL block asynchronously, translating privilege refusals."""
    try:
        await driver.execute(statement, parameters)
    except Exception as exc:
        if _is_dbms_aqadm_privilege_error(exc):
            raise _wrap_aqadm_privilege_error(queue_name, exc) from exc
        raise


def _reconcile_oracle_native_backend_sync(
    driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
) -> None:
    """Provision missing or stopped Oracle AQ or TxEventQ queues on a synchronous driver."""
    if not targets or backend_name not in _ORACLE_NATIVE_BACKENDS:
        return

    for raw_name in targets:
        queue_name = raw_name.upper()
        rows = driver.select(_PROBE_USER_QUEUE_SQL, {"name": queue_name})
        if rows:
            enq_enabled, deq_enabled = _extract_queue_flags(rows[0])
            if enq_enabled == "YES" and deq_enabled == "YES":
                continue
            if not create_schema:
                msg = (
                    f"Oracle events queue '{queue_name}' is stopped while create_schema=False; "
                    "start the queue manually or set create_schema=True."
                )
                raise MigrationError(msg)
            _exec_aqadm_sync(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})
            continue

        if not create_schema:
            msg = (
                f"Oracle events queue '{queue_name}' does not exist while create_schema=False; "
                "provision the queue manually or set create_schema=True."
            )
            raise MigrationError(msg)

        if backend_name == "aq":
            queue_table = f"{queue_name[:125]}_QT"
            qt_rows = driver.select(_PROBE_USER_QUEUE_TABLE_SQL, {"name": queue_table})
            if not qt_rows:
                _exec_aqadm_sync(driver, queue_name, _CREATE_QUEUE_TABLE_PLSQL, {"queue_table": queue_table})
            _exec_aqadm_sync(
                driver, queue_name, _CREATE_QUEUE_PLSQL, {"queue_name": queue_name, "queue_table": queue_table}
            )
            _exec_aqadm_sync(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})
        else:
            _exec_aqadm_sync(driver, queue_name, _CREATE_TXEVENTQ_PLSQL, {"queue_name": queue_name})
            _exec_aqadm_sync(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})


async def _reconcile_oracle_native_backend_async(
    driver: Any, backend_name: str, targets: tuple[str, ...], *, create_schema: bool
) -> None:
    """Provision missing or stopped Oracle AQ or TxEventQ queues on an asynchronous driver."""
    if not targets or backend_name not in _ORACLE_NATIVE_BACKENDS:
        return

    for raw_name in targets:
        queue_name = raw_name.upper()
        rows = await driver.select(_PROBE_USER_QUEUE_SQL, {"name": queue_name})
        if rows:
            enq_enabled, deq_enabled = _extract_queue_flags(rows[0])
            if enq_enabled == "YES" and deq_enabled == "YES":
                continue
            if not create_schema:
                msg = (
                    f"Oracle events queue '{queue_name}' is stopped while create_schema=False; "
                    "start the queue manually or set create_schema=True."
                )
                raise MigrationError(msg)
            await _exec_aqadm_async(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})
            continue

        if not create_schema:
            msg = (
                f"Oracle events queue '{queue_name}' does not exist while create_schema=False; "
                "provision the queue manually or set create_schema=True."
            )
            raise MigrationError(msg)

        if backend_name == "aq":
            queue_table = f"{queue_name[:125]}_QT"
            qt_rows = await driver.select(_PROBE_USER_QUEUE_TABLE_SQL, {"name": queue_table})
            if not qt_rows:
                await _exec_aqadm_async(driver, queue_name, _CREATE_QUEUE_TABLE_PLSQL, {"queue_table": queue_table})
            await _exec_aqadm_async(
                driver, queue_name, _CREATE_QUEUE_PLSQL, {"queue_name": queue_name, "queue_table": queue_table}
            )
            await _exec_aqadm_async(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})
        else:
            await _exec_aqadm_async(driver, queue_name, _CREATE_TXEVENTQ_PLSQL, {"queue_name": queue_name})
            await _exec_aqadm_async(driver, queue_name, _START_QUEUE_PLSQL, {"queue_name": queue_name})
