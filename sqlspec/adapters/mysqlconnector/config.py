"""MysqlConnector database configuration."""

import contextlib
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

import mysql.connector
from mysql.connector import pooling
from typing_extensions import NotRequired

from sqlspec.adapters.mysqlconnector._typing import (
    MysqlConnectorAsyncConnection,
    MysqlConnectorAsyncSessionContext,
    MysqlConnectorSyncConnection,
    MysqlConnectorSyncSessionContext,
)
from sqlspec.adapters.mysqlconnector.core import apply_driver_features, default_statement_config
from sqlspec.adapters.mysqlconnector.driver import (
    MysqlConnectorAsyncCursor,
    MysqlConnectorAsyncDriver,
    MysqlConnectorAsyncExceptionHandler,
    MysqlConnectorSyncCursor,
    MysqlConnectorSyncDriver,
    MysqlConnectorSyncExceptionHandler,
)
from sqlspec.config import ExtensionConfigs, NoPoolAsyncConfig, SyncDatabaseConfig
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config, reject_pool_aliases

if TYPE_CHECKING:
    from collections.abc import Callable

    from mysql.connector.pooling import MySQLConnectionPool

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = (
    "MysqlConnectorAsyncConfig",
    "MysqlConnectorAsyncConnectionParams",
    "MysqlConnectorDriverFeatures",
    "MysqlConnectorPoolParams",
    "MysqlConnectorSyncConfig",
    "MysqlConnectorSyncConnectionParams",
)


class MysqlConnectorSyncConnectionParams(TypedDict):
    """MysqlConnector sync connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    charset: NotRequired[str]
    connection_timeout: NotRequired[int]
    autocommit: NotRequired[bool]
    use_pure: NotRequired[bool]
    ssl_ca: NotRequired[str]
    ssl_cert: NotRequired[str]
    ssl_key: NotRequired[str]
    ssl_verify_cert: NotRequired[bool]
    ssl_verify_identity: NotRequired[bool]
    client_flags: NotRequired[int]
    pool_name: NotRequired[str]
    pool_size: NotRequired[int]
    pool_reset_session: NotRequired[bool]
    extra: NotRequired["dict[str, Any]"]


class MysqlConnectorPoolParams(MysqlConnectorSyncConnectionParams):
    """MysqlConnector pooling parameters.

    Note: pool_name, pool_size, and pool_reset_session are inherited
    from MysqlConnectorSyncConnectionParams.
    """


class MysqlConnectorAsyncConnectionParams(TypedDict):
    """MysqlConnector async connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    charset: NotRequired[str]
    connection_timeout: NotRequired[int]
    autocommit: NotRequired[bool]
    use_pure: NotRequired[bool]
    ssl_ca: NotRequired[str]
    ssl_cert: NotRequired[str]
    ssl_key: NotRequired[str]
    ssl_verify_cert: NotRequired[bool]
    ssl_verify_identity: NotRequired[bool]
    client_flags: NotRequired[int]
    extra: NotRequired["dict[str, Any]"]


class MysqlConnectorDriverFeatures(TypedDict):
    """MysqlConnector driver feature flags.

    json_serializer: Custom JSON serializer function.
        Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
        Defaults to sqlspec.utils.serializers.from_json.
    enable_events: Enable database event channel support.
        Defaults to True when extension_config["events"] is configured.
    events_backend: Event channel backend selection.
        Only option: "table_queue".
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class MysqlConnectorSyncConnectionContext:
    """Context manager for mysql-connector sync connections."""

    __slots__ = ("_config", "_connection")

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        self._config = config
        self._connection: MysqlConnectorSyncConnection | None = None

    def __enter__(self) -> MysqlConnectorSyncConnection:
        if self._config.connection_instance is not None:
            self._connection = cast("MysqlConnectorSyncConnection", self._config.connection_instance.get_connection())
            return self._connection
        self._connection = self._config.create_connection()
        return self._connection

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        return None


class _MysqlConnectorSyncSessionConnectionHandler:
    __slots__ = ("_config", "_connection")

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        self._config = config
        self._connection: MysqlConnectorSyncConnection | None = None

    def acquire_connection(self) -> MysqlConnectorSyncConnection:
        if self._config.connection_instance is not None:
            self._connection = cast("MysqlConnectorSyncConnection", self._config.connection_instance.get_connection())
            return self._connection
        self._connection = self._config.create_connection()
        return self._connection

    def release_connection(self, _conn: MysqlConnectorSyncConnection) -> None:
        if self._connection is None:
            return
        self._connection.close()
        self._connection = None


class MysqlConnectorAsyncConnectionContext:
    """Async context manager for mysql-connector async connections."""

    __slots__ = ("_config", "_connection")

    def __init__(self, config: "MysqlConnectorAsyncConfig") -> None:
        self._config = config
        self._connection: MysqlConnectorAsyncConnection | None = None

    async def __aenter__(self) -> MysqlConnectorAsyncConnection:
        self._connection = await self._config.create_connection()
        return self._connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        return None


class _MysqlConnectorAsyncSessionConnectionHandler:
    __slots__ = ("_config", "_connection")

    def __init__(self, config: "MysqlConnectorAsyncConfig") -> None:
        self._config = config
        self._connection: MysqlConnectorAsyncConnection | None = None

    async def acquire_connection(self) -> MysqlConnectorAsyncConnection:
        self._connection = await self._config.create_connection()
        return self._connection

    async def release_connection(self, _conn: MysqlConnectorAsyncConnection) -> None:
        if self._connection is None:
            return
        await self._connection.close()
        self._connection = None


class MysqlConnectorSyncConfig(
    SyncDatabaseConfig[MysqlConnectorSyncConnection, "MySQLConnectionPool", MysqlConnectorSyncDriver]
):
    """Configuration for mysql-connector synchronous MySQL connections."""

    driver_type: ClassVar[type[MysqlConnectorSyncDriver]] = MysqlConnectorSyncDriver
    connection_type: ClassVar[type[MysqlConnectorSyncConnection]] = MysqlConnectorSyncConnection
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True

    def __init__(
        self,
        *,
        connection_config: "MysqlConnectorPoolParams | dict[str, Any] | None" = None,
        connection_instance: "MySQLConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "MysqlConnectorDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        reject_pool_aliases(kwargs)

        connection_config = normalize_connection_config(connection_config)
        connection_config.setdefault("host", "localhost")
        connection_config.setdefault("port", 3306)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def _create_pool(self) -> "MySQLConnectionPool":
        config = dict(self.connection_config)
        pool_name = config.pop("pool_name", None)
        pool_size = config.pop("pool_size", None)
        pool_reset = config.pop("pool_reset_session", True)
        return pooling.MySQLConnectionPool(
            pool_name=pool_name, pool_size=pool_size or 5, pool_reset_session=pool_reset, **config
        )

    def _close_pool(self) -> None:
        if self.connection_instance is not None:
            self.connection_instance = None

    def create_connection(self) -> MysqlConnectorSyncConnection:
        connection = cast("MysqlConnectorSyncConnection", mysql.connector.connect(**self.connection_config))
        autocommit = self.connection_config.get("autocommit")
        if autocommit is not None and hasattr(connection, "autocommit"):
            with contextlib.suppress(Exception):
                setattr(connection, "autocommit", bool(autocommit))
        return connection

    def provide_connection(self, *args: Any, **kwargs: Any) -> "MysqlConnectorSyncConnectionContext":
        return MysqlConnectorSyncConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "MysqlConnectorSyncSessionContext":
        statement_config = statement_config or self.statement_config or default_statement_config
        handler = _MysqlConnectorSyncSessionConnectionHandler(self)

        return MysqlConnectorSyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "MysqlConnectorSyncConfig": MysqlConnectorSyncConfig,
            "MysqlConnectorSyncConnection": MysqlConnectorSyncConnection,
            "MysqlConnectorSyncConnectionParams": MysqlConnectorSyncConnectionParams,
            "MysqlConnectorSyncCursor": MysqlConnectorSyncCursor,
            "MysqlConnectorSyncDriver": MysqlConnectorSyncDriver,
            "MysqlConnectorSyncExceptionHandler": MysqlConnectorSyncExceptionHandler,
            "MysqlConnectorSyncSessionContext": MysqlConnectorSyncSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5, select_for_update=True, skip_locked=True)


class MysqlConnectorAsyncConfig(NoPoolAsyncConfig[MysqlConnectorAsyncConnection, MysqlConnectorAsyncDriver]):
    """Configuration for mysql-connector async MySQL connections."""

    driver_type: ClassVar[type[MysqlConnectorAsyncDriver]] = MysqlConnectorAsyncDriver
    connection_type: "ClassVar[type[Any]]" = cast("type[Any]", MysqlConnectorAsyncConnection)
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True

    def __init__(
        self,
        *,
        connection_config: "MysqlConnectorAsyncConnectionParams | dict[str, Any] | None" = None,
        connection_instance: Any = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "MysqlConnectorDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        self.connection_config = normalize_connection_config(connection_config)
        self.connection_config.setdefault("host", "localhost")
        self.connection_config.setdefault("port", 3306)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        super().__init__(
            connection_config=self.connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    async def create_connection(self) -> MysqlConnectorAsyncConnection:
        from mysql.connector import aio

        connection = await aio.connect(**self.connection_config)
        autocommit = self.connection_config.get("autocommit")
        if autocommit is not None and hasattr(connection, "set_autocommit"):
            with contextlib.suppress(Exception):
                await connection.set_autocommit(bool(autocommit))
        return cast("MysqlConnectorAsyncConnection", connection)

    def provide_connection(self, *args: Any, **kwargs: Any) -> "MysqlConnectorAsyncConnectionContext":
        return MysqlConnectorAsyncConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "MysqlConnectorAsyncSessionContext":
        statement_config = statement_config or self.statement_config or default_statement_config
        handler = _MysqlConnectorAsyncSessionConnectionHandler(self)

        return MysqlConnectorAsyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "MysqlConnectorAsyncConfig": MysqlConnectorAsyncConfig,
            "MysqlConnectorAsyncConnection": MysqlConnectorAsyncConnection,
            "MysqlConnectorAsyncConnectionParams": MysqlConnectorAsyncConnectionParams,
            "MysqlConnectorAsyncCursor": MysqlConnectorAsyncCursor,
            "MysqlConnectorAsyncDriver": MysqlConnectorAsyncDriver,
            "MysqlConnectorAsyncExceptionHandler": MysqlConnectorAsyncExceptionHandler,
            "MysqlConnectorAsyncSessionContext": MysqlConnectorAsyncSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5, select_for_update=True, skip_locked=True)
