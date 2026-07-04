"""mssql-python database configuration."""

import asyncio
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.mssql_python._typing import (
    MssqlPythonAsyncSessionContext,
    MssqlPythonConnection,
    MssqlPythonSessionContext,
)
from sqlspec.adapters.mssql_python.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.mssql_python.driver import MssqlPythonAsyncDriver, MssqlPythonDriver
from sqlspec.adapters.mssql_python.migrations import MssqlPythonAsyncMigrationTracker, MssqlPythonSyncMigrationTracker
from sqlspec.adapters.mssql_python.pool import MssqlPythonConnectionPool
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.driver._async import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.driver._sync import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "MssqlPythonAsyncConfig",
    "MssqlPythonConfig",
    "MssqlPythonConnectionParams",
    "MssqlPythonConnectionPool",
    "MssqlPythonDriverFeatures",
    "MssqlPythonPoolParams",
)


class MssqlPythonConnectionParams(TypedDict):
    """mssql-python connection parameters."""

    connection_string: NotRequired[str]
    server: NotRequired[str]
    address: NotRequired[str]
    addr: NotRequired[str]
    port: NotRequired[int]
    database: NotRequired[str]
    db: NotRequired[str]
    uid: NotRequired[str]
    user: NotRequired[str]
    username: NotRequired[str]
    pwd: NotRequired[str]
    password: NotRequired[str]
    authentication: NotRequired[str]
    trusted_connection: NotRequired[bool | str]
    trust_server_certificate: NotRequired[bool | str]
    trust: NotRequired[bool | str]
    encrypt: NotRequired[bool | str]
    hostname_in_certificate: NotRequired[str]
    hostnameincertificate: NotRequired[str]
    server_certificate: NotRequired[str]
    servercertificate: NotRequired[str]
    server_spn: NotRequired[str]
    serverspn: NotRequired[str]
    multi_subnet_failover: NotRequired[bool | str]
    multisubnetfailover: NotRequired[bool | str]
    application_intent: NotRequired[str]
    applicationintent: NotRequired[str]
    connect_retry_count: NotRequired[int]
    connectretrycount: NotRequired[int]
    connect_retry_interval: NotRequired[int]
    connectretryinterval: NotRequired[int]
    keep_alive: NotRequired[int]
    keepalive: NotRequired[int]
    keep_alive_interval: NotRequired[int]
    keepaliveinterval: NotRequired[int]
    ip_address_preference: NotRequired[str]
    ipaddresspreference: NotRequired[str]
    packet_size: NotRequired[int]
    packetsize: NotRequired[int]
    connection_timeout: NotRequired[int]
    login_timeout: NotRequired[int]
    command_timeout: NotRequired[int]
    autocommit: NotRequired[bool]
    attrs_before: NotRequired[dict[int, int | str | bytes]]
    timeout: NotRequired[int]
    native_uuid: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class MssqlPythonPoolParams(MssqlPythonConnectionParams):
    """mssql-python driver-level pooling parameters."""

    pool_size: NotRequired[int]
    pool_idle_timeout: NotRequired[int]
    pool_enabled: NotRequired[bool]


class MssqlPythonDriverFeatures(TypedDict):
    """mssql-python driver feature flags."""

    use_pool: NotRequired[bool]
    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    on_connection_create: "NotRequired[Callable[[MssqlPythonConnection], None]]"
    enable_events: NotRequired[bool]


class MssqlPythonConnectionContext(SyncPoolConnectionContext):
    """Context manager for mssql-python sync connections."""

    __slots__ = ("_conn",)

    def __init__(self, config: "MssqlPythonConfig") -> None:
        super().__init__(config)
        self._conn: MssqlPythonConnection | None = None

    def __enter__(self) -> "MssqlPythonConnection":
        pool = self._config.provide_pool()
        conn = pool.acquire()
        self._conn = conn
        return cast("MssqlPythonConnection", conn)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._conn is not None:
            self._config.provide_pool().release(self._conn)
            self._conn = None
        return None


class MssqlPythonAsyncConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for mssql-python connections via to_thread."""

    __slots__ = ("_conn",)

    def __init__(self, config: "MssqlPythonAsyncConfig") -> None:
        super().__init__(config)
        self._conn: MssqlPythonConnection | None = None

    async def __aenter__(self) -> "MssqlPythonConnection":
        pool = await self._config.provide_pool()
        conn = await asyncio.to_thread(pool.acquire)
        self._conn = conn
        return cast("MssqlPythonConnection", conn)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        if self._conn is not None:
            pool = await self._config.provide_pool()
            await asyncio.to_thread(pool.release, self._conn)
            self._conn = None
        return None


class _MssqlPythonSyncSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ("_conn",)

    def __init__(self, config: "MssqlPythonConfig") -> None:
        super().__init__(config)
        self._conn: MssqlPythonConnection | None = None

    def acquire_connection(self) -> "MssqlPythonConnection":
        pool = self._config.provide_pool()
        conn = pool.acquire()
        self._conn = conn
        return cast("MssqlPythonConnection", conn)

    def release_connection(self, _conn: "MssqlPythonConnection", **kwargs: Any) -> None:
        if self._conn is None:
            return
        self._config.provide_pool().release(self._conn)
        self._conn = None


class _MssqlPythonAsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    __slots__ = ("_conn",)

    def __init__(self, config: "MssqlPythonAsyncConfig") -> None:
        super().__init__(config)
        self._conn: MssqlPythonConnection | None = None

    async def acquire_connection(self) -> "MssqlPythonConnection":
        pool = await self._config.provide_pool()
        conn = await asyncio.to_thread(pool.acquire)
        self._conn = conn
        return cast("MssqlPythonConnection", conn)

    async def release_connection(self, _conn: "MssqlPythonConnection", **kwargs: Any) -> None:
        if self._conn is None:
            return
        pool = await self._config.provide_pool()
        await asyncio.to_thread(pool.release, self._conn)
        self._conn = None


class MssqlPythonConfig(SyncDatabaseConfig[MssqlPythonConnection, MssqlPythonConnectionPool, MssqlPythonDriver]):
    """Configuration for mssql-python synchronous database connections."""

    __slots__ = ("_user_connection_hook",)

    driver_type: "ClassVar[type[MssqlPythonDriver]]" = MssqlPythonDriver
    connection_type: "ClassVar[type[MssqlPythonConnection]]" = MssqlPythonConnection
    migration_tracker_type: "ClassVar[type[MssqlPythonSyncMigrationTracker]]" = MssqlPythonSyncMigrationTracker
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_arrow_streaming: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    _connection_context_class: "ClassVar[type[MssqlPythonConnectionContext]]" = MssqlPythonConnectionContext
    _session_factory_class: "ClassVar[type[_MssqlPythonSyncSessionConnectionHandler]]" = (
        _MssqlPythonSyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[MssqlPythonSessionContext]]" = MssqlPythonSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "MssqlPythonPoolParams | dict[str, Any] | None" = None,
        connection_instance: "MssqlPythonConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "MssqlPythonDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        normalized, features_dict, user_connection_hook = _normalize_mssql_python_init(
            connection_config, driver_features
        )
        self._user_connection_hook = user_connection_hook
        super().__init__(
            bind_key=bind_key,
            connection_config=normalized,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config or default_statement_config,
            driver_features=features_dict,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def create_connection(self) -> "MssqlPythonConnection":
        pool = self.provide_pool()
        return pool.acquire()

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "MssqlPythonConfig": MssqlPythonConfig,
            "MssqlPythonConnection": MssqlPythonConnection,
            "MssqlPythonConnectionContext": MssqlPythonConnectionContext,
            "MssqlPythonConnectionParams": MssqlPythonConnectionParams,
            "MssqlPythonConnectionPool": MssqlPythonConnectionPool,
            "MssqlPythonDriver": MssqlPythonDriver,
            "MssqlPythonDriverFeatures": MssqlPythonDriverFeatures,
            "MssqlPythonPoolParams": MssqlPythonPoolParams,
            "MssqlPythonSessionContext": MssqlPythonSessionContext,
        })
        return namespace

    def _create_pool(self) -> "MssqlPythonConnectionPool":
        return _create_mssql_python_pool(dict(self.connection_config), self.driver_features, self._user_connection_hook)

    def _close_pool(self) -> None:
        if self.connection_instance is not None:
            self.connection_instance.close()


class MssqlPythonAsyncConfig(
    AsyncDatabaseConfig[MssqlPythonConnection, MssqlPythonConnectionPool, MssqlPythonAsyncDriver]
):
    """Configuration for mssql-python async database connections."""

    __slots__ = ("_user_connection_hook",)

    driver_type: "ClassVar[type[MssqlPythonAsyncDriver]]" = MssqlPythonAsyncDriver
    connection_type: "ClassVar[type[MssqlPythonConnection]]" = MssqlPythonConnection
    migration_tracker_type: "ClassVar[type[MssqlPythonAsyncMigrationTracker]]" = MssqlPythonAsyncMigrationTracker
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_arrow_streaming: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    _connection_context_class: "ClassVar[type[MssqlPythonAsyncConnectionContext]]" = MssqlPythonAsyncConnectionContext
    _session_factory_class: "ClassVar[type[_MssqlPythonAsyncSessionConnectionHandler]]" = (
        _MssqlPythonAsyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[MssqlPythonAsyncSessionContext]]" = MssqlPythonAsyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "MssqlPythonPoolParams | dict[str, Any] | None" = None,
        connection_instance: "MssqlPythonConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "MssqlPythonDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        normalized, features_dict, user_connection_hook = _normalize_mssql_python_init(
            connection_config, driver_features
        )
        self._user_connection_hook = user_connection_hook
        super().__init__(
            bind_key=bind_key,
            connection_config=normalized,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config or default_statement_config,
            driver_features=features_dict,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    async def create_connection(self) -> "MssqlPythonConnection":
        pool = await self.provide_pool()
        return await asyncio.to_thread(pool.acquire)

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "MssqlPythonAsyncConfig": MssqlPythonAsyncConfig,
            "MssqlPythonAsyncConnectionContext": MssqlPythonAsyncConnectionContext,
            "MssqlPythonAsyncDriver": MssqlPythonAsyncDriver,
            "MssqlPythonAsyncSessionContext": MssqlPythonAsyncSessionContext,
            "MssqlPythonConfig": MssqlPythonConfig,
            "MssqlPythonConnection": MssqlPythonConnection,
            "MssqlPythonConnectionParams": MssqlPythonConnectionParams,
            "MssqlPythonConnectionPool": MssqlPythonConnectionPool,
            "MssqlPythonDriver": MssqlPythonDriver,
            "MssqlPythonDriverFeatures": MssqlPythonDriverFeatures,
            "MssqlPythonPoolParams": MssqlPythonPoolParams,
            "MssqlPythonSessionContext": MssqlPythonSessionContext,
        })
        return namespace

    async def _create_pool(self) -> "MssqlPythonConnectionPool":
        return await asyncio.to_thread(
            _create_mssql_python_pool, dict(self.connection_config), self.driver_features, self._user_connection_hook
        )

    async def _close_pool(self) -> None:
        if self.connection_instance is not None:
            await asyncio.to_thread(self.connection_instance.close)


def _create_mssql_python_pool(
    connection_config: "dict[str, Any]",
    driver_features: "dict[str, Any]",
    on_connection_create: "Callable[[MssqlPythonConnection], None] | None" = None,
) -> "MssqlPythonConnectionPool":
    pool_size = int(connection_config.get("pool_size", 100))
    pool_idle_timeout = int(connection_config.get("pool_idle_timeout", 600))
    pool_enabled = bool(connection_config.get("pool_enabled", driver_features.get("use_pool", True)))
    connection_string, connect_kwargs = build_connection_config(connection_config)
    return MssqlPythonConnectionPool(
        connection_string=connection_string,
        connect_kwargs=connect_kwargs,
        max_size=pool_size,
        idle_timeout=pool_idle_timeout,
        enabled=pool_enabled,
        on_connection_create=on_connection_create,
    )


def _normalize_mssql_python_init(
    connection_config: "MssqlPythonPoolParams | dict[str, Any] | None",
    driver_features: "MssqlPythonDriverFeatures | dict[str, Any] | None",
) -> "tuple[dict[str, Any], dict[str, Any], Callable[[MssqlPythonConnection], None] | None]":
    normalized = normalize_connection_config(connection_config)
    _, features_dict = apply_driver_features(default_statement_config, driver_features)
    hook = cast("Callable[[MssqlPythonConnection], None] | None", features_dict.pop("on_connection_create", None))
    return normalized, features_dict, hook
