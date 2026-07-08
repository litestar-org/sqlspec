"""MysqlConnector database configuration."""

import contextlib
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast
from weakref import WeakSet

from typing_extensions import NotRequired

from sqlspec.adapters.mysqlconnector._typing import (
    MysqlConnectorAio,
    MysqlConnectorAsyncConnection,
    MysqlConnectorAsyncCursor,
    MysqlConnectorAsyncSessionContext,
    MysqlConnectorConnectionPool,
    MysqlConnectorMysqlModule,
    MysqlConnectorSyncConnection,
    MysqlConnectorSyncCursor,
    MysqlConnectorSyncSessionContext,
)
from sqlspec.adapters.mysqlconnector.core import apply_driver_features, default_statement_config
from sqlspec.adapters.mysqlconnector.driver import (
    MysqlConnectorAsyncDriver,
    MysqlConnectorAsyncExceptionHandler,
    MysqlConnectorSyncDriver,
    MysqlConnectorSyncExceptionHandler,
)
from sqlspec.config import ExtensionConfigs, NoPoolAsyncConfig, SyncDatabaseConfig
from sqlspec.driver._async import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.driver._sync import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = (
    "MysqlConnectorAsyncConfig",
    "MysqlConnectorAsyncConnectionParams",
    "MysqlConnectorCursorParams",
    "MysqlConnectorDriverFeatures",
    "MysqlConnectorFailoverTarget",
    "MysqlConnectorPoolParams",
    "MysqlConnectorSyncConfig",
    "MysqlConnectorSyncConnectionParams",
)
mysql: "MysqlConnectorMysqlModule" = cast("MysqlConnectorMysqlModule", MysqlConnectorMysqlModule)
mysqlconnector_aio: "MysqlConnectorAio" = cast("MysqlConnectorAio", MysqlConnectorAio)


MysqlConnectorClientFlags = int | list[int] | tuple[int, ...]
MysqlConnectorPathSequence = str | list[str] | tuple[str, ...]
MysqlConnectorStringSequence = list[str] | tuple[str, ...]


class MysqlConnectorFailoverTarget(TypedDict):
    """Connector/Python failover target parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    pool_name: NotRequired[str]
    pool_size: NotRequired[int]


class MysqlConnectorCursorParams(TypedDict):
    """Connector/Python cursor parameters routed by SQLSpec."""

    buffered: NotRequired[bool]
    raw: NotRequired[bool]
    dictionary: NotRequired[bool]
    prepared: NotRequired[bool]
    cursor_class: NotRequired[type[Any]]


class _MysqlConnectorBaseConnectionParams(TypedDict):
    """Common Connector/Python connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    username: NotRequired[str]
    password: NotRequired[str]
    passwd: NotRequired[str]
    password1: NotRequired[str]
    password2: NotRequired[str]
    password3: NotRequired[str]
    database: NotRequired[str]
    db: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    conn_attrs: NotRequired[dict[str, str]]
    init_command: NotRequired[str]
    auth_plugin: NotRequired[str]
    webauthn_callback: NotRequired[Callable[[], None] | str]
    openid_token_file: NotRequired[str]
    use_unicode: NotRequired[bool]
    charset: NotRequired[str]
    collation: NotRequired[str]
    autocommit: NotRequired[bool]
    time_zone: NotRequired[str]
    sql_mode: NotRequired[str]
    get_warnings: NotRequired[bool]
    raise_on_warnings: NotRequired[bool]
    connection_timeout: NotRequired[int | float]
    connect_timeout: NotRequired[int | float]
    read_timeout: NotRequired[int | float]
    write_timeout: NotRequired[int | float]
    client_flags: NotRequired[MysqlConnectorClientFlags]
    buffered: NotRequired[bool]
    raw: NotRequired[bool]
    consume_results: NotRequired[bool]
    tls_versions: NotRequired[MysqlConnectorStringSequence]
    tls_ciphersuites: NotRequired[MysqlConnectorStringSequence]
    ssl_ca: NotRequired[str]
    ssl_cert: NotRequired[str]
    ssl_key: NotRequired[str]
    ssl_cipher: NotRequired[str]
    ssl_disabled: NotRequired[bool]
    ssl_verify_cert: NotRequired[bool]
    ssl_verify_identity: NotRequired[bool]
    force_ipv6: NotRequired[bool]
    dns_srv: NotRequired[bool]
    kerberos_auth_mode: NotRequired[Literal["SSPI", "GSSAPI"]]
    krb_service_principal: NotRequired[str]
    oci_config_file: NotRequired[str]
    oci_config_profile: NotRequired[str]
    compress: NotRequired[bool]
    converter_class: NotRequired[type[Any]]
    converter_str_fallback: NotRequired[bool]
    failover: NotRequired[list[MysqlConnectorFailoverTarget] | tuple[MysqlConnectorFailoverTarget, ...]]
    option_files: NotRequired[MysqlConnectorPathSequence]
    option_groups: NotRequired[MysqlConnectorStringSequence]
    allow_local_infile: NotRequired[bool]
    allow_local_infile_in_path: NotRequired[str]
    use_pure: NotRequired[bool]
    dsn: NotRequired[str]
    extra: NotRequired["dict[str, Any]"]


class MysqlConnectorSyncConnectionParams(_MysqlConnectorBaseConnectionParams):
    """MysqlConnector sync connection parameters."""

    pool_name: NotRequired[str]
    pool_size: NotRequired[int]
    pool_reset_session: NotRequired[bool]


class MysqlConnectorPoolParams(MysqlConnectorSyncConnectionParams):
    """MysqlConnector pooling parameters.

    Note: pool_name, pool_size, and pool_reset_session are inherited from
    MysqlConnectorSyncConnectionParams.
    """


class MysqlConnectorAsyncConnectionParams(_MysqlConnectorBaseConnectionParams):
    """MysqlConnector async connection parameters."""


class MysqlConnectorDriverFeatures(TypedDict):
    """MysqlConnector driver feature flags.

    json_serializer: Custom JSON serializer function.
     Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
     Defaults to sqlspec.utils.serializers.from_json.
    on_connection_create: Callback executed when a connection is acquired.
     For sync: Callable[[MysqlConnectorSyncConnection], None]
     For async: Callable[[MysqlConnectorAsyncConnection], Awaitable[None]]
     Called exactly once per physical connection using WeakSet tracking.
    enable_events: Enable database event channel support.
     Defaults to True when extension_config["events"] is configured.
    events_backend: Event channel backend selection.
     Only option: "table_queue".
    cursor_options: Cursor keyword arguments SQLSpec forwards to
     ``connection.cursor()`` for statement execution.
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[..., Any]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]
    cursor_options: NotRequired[MysqlConnectorCursorParams]
    enable_local_infile_bulk_load: NotRequired[bool]


def _normalize_local_infile(connection_config: "Mapping[str, Any] | None") -> "dict[str, Any]":
    """Normalize mysql-connector local-infile consent."""
    config = normalize_connection_config(connection_config)
    config.pop("local_infile", None)
    config["allow_local_infile"] = bool(config.get("allow_local_infile", False))
    return config


class MysqlConnectorSyncConnectionContext(SyncPoolConnectionContext):
    """Context manager for mysql-connector sync connections."""

    __slots__ = ("_connection",)

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        super().__init__(config)
        self._connection: MysqlConnectorSyncConnection | None = None

    def __enter__(self) -> MysqlConnectorSyncConnection:
        self._connection = self._config._acquire_sync_connection()  # pyright: ignore[reportPrivateUsage]
        return cast("MysqlConnectorSyncConnection", self._connection)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        return None


class _MysqlConnectorSyncSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ("_connection",)

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        super().__init__(config)
        self._connection: MysqlConnectorSyncConnection | None = None

    def acquire_connection(self) -> MysqlConnectorSyncConnection:
        self._connection = self._config._acquire_sync_connection()  # pyright: ignore[reportPrivateUsage]
        return cast("MysqlConnectorSyncConnection", self._connection)

    def release_connection(self, _conn: MysqlConnectorSyncConnection, **kwargs: Any) -> None:
        if self._connection is None:
            return
        self._connection.close()
        self._connection = None


class MysqlConnectorAsyncConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for mysql-connector async connections."""

    __slots__ = ()

    async def __aenter__(self) -> MysqlConnectorAsyncConnection:
        self._connection = await self._config.create_connection()
        return cast("MysqlConnectorAsyncConnection", self._connection)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        return None


class _MysqlConnectorAsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    __slots__ = ()

    async def acquire_connection(self) -> MysqlConnectorAsyncConnection:
        self._connection = await self._config.create_connection()
        return cast("MysqlConnectorAsyncConnection", self._connection)

    async def release_connection(self, _conn: MysqlConnectorAsyncConnection, **kwargs: Any) -> None:
        if self._connection is None:
            return
        await self._connection.close()
        self._connection = None


class MysqlConnectorSyncConfig(
    SyncDatabaseConfig[MysqlConnectorSyncConnection, "MysqlConnectorConnectionPool", MysqlConnectorSyncDriver]
):
    """Configuration for mysql-connector synchronous MySQL connections."""

    driver_type: ClassVar[type[MysqlConnectorSyncDriver]] = MysqlConnectorSyncDriver
    connection_type: ClassVar[type[MysqlConnectorSyncConnection]] = MysqlConnectorSyncConnection
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    _connection_context_class: "ClassVar[type[MysqlConnectorSyncConnectionContext]]" = (
        MysqlConnectorSyncConnectionContext
    )
    _session_factory_class: "ClassVar[type[_MysqlConnectorSyncSessionConnectionHandler]]" = (
        _MysqlConnectorSyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[MysqlConnectorSyncSessionContext]]" = MysqlConnectorSyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "MysqlConnectorPoolParams | dict[str, Any] | None" = None,
        connection_instance: "MysqlConnectorConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "MysqlConnectorDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        connection_config = _normalize_local_infile(connection_config)
        connection_config.setdefault("host", "localhost")
        connection_config.setdefault("port", 3306)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        # Extract user connection hook before storing driver_features
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[MysqlConnectorSyncConnection], None] | None = features_dict.pop(
            "on_connection_create", None
        )
        # Track initialized connections to ensure callback runs exactly once per physical connection
        self._initialized_connections: WeakSet[Any] = WeakSet()

        if features_dict.get("enable_local_infile_bulk_load") and not connection_config.get("allow_local_infile"):
            msg = "enable_local_infile_bulk_load requires allow_local_infile=True in connection_config."
            raise ImproperConfigurationError(msg)

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def _ensure_connection(self, connection: "MysqlConnectorSyncConnection") -> None:
        """Ensure connection callback has been called exactly once for this connection."""
        if self._user_connection_hook is None:
            return
        if connection not in self._initialized_connections:
            self._user_connection_hook(connection)
            self._initialized_connections.add(connection)

    def _acquire_sync_connection(self) -> MysqlConnectorSyncConnection:
        """Acquire and initialize a sync mysql-connector connection."""
        pool = self.provide_pool()
        connection = cast("MysqlConnectorSyncConnection", pool.get_connection())
        self._ensure_connection(connection)
        return connection

    def _create_pool(self) -> "MysqlConnectorConnectionPool":
        config = dict(self.connection_config)
        pool_name = config.pop("pool_name", None)
        pool_size = config.pop("pool_size", None)
        pool_reset = config.pop("pool_reset_session", True)
        return MysqlConnectorConnectionPool(
            pool_name=pool_name, pool_size=pool_size or 5, pool_reset_session=pool_reset, **config
        )

    def _close_pool(self) -> None:
        if self.connection_instance is not None:
            # MySQLConnectionPool has no explicit close API; dropping the reference releases the pool.
            self.connection_instance = None

    def create_connection(self) -> MysqlConnectorSyncConnection:
        connection = mysql.connector.connect(**self.connection_config)
        autocommit = self.connection_config.get("autocommit")
        if autocommit is not None and hasattr(connection, "autocommit"):
            with contextlib.suppress(Exception):
                setattr(connection, "autocommit", bool(autocommit))
        return connection

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
    supports_native_row_streaming: ClassVar[bool] = True
    _connection_context_class: "ClassVar[type[MysqlConnectorAsyncConnectionContext]]" = (
        MysqlConnectorAsyncConnectionContext
    )
    _session_factory_class: "ClassVar[type[_MysqlConnectorAsyncSessionConnectionHandler]]" = (
        _MysqlConnectorAsyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[MysqlConnectorAsyncSessionContext]]" = MysqlConnectorAsyncSessionContext
    _default_statement_config = default_statement_config

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
        self.connection_config = _normalize_local_infile(connection_config)
        self.connection_config.setdefault("host", "localhost")
        self.connection_config.setdefault("port", 3306)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        # Extract user connection hook before storing driver_features
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[MysqlConnectorAsyncConnection], Awaitable[None]] | None = (
            features_dict.pop("on_connection_create", None)
        )

        if features_dict.get("enable_local_infile_bulk_load") and not self.connection_config.get("allow_local_infile"):
            msg = "enable_local_infile_bulk_load requires allow_local_infile=True in connection_config."
            raise ImproperConfigurationError(msg)

        super().__init__(
            connection_config=self.connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features_dict,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    async def create_connection(self) -> MysqlConnectorAsyncConnection:
        connection = await mysqlconnector_aio.connect(**self.connection_config)
        autocommit = self.connection_config.get("autocommit")
        if autocommit is not None and hasattr(connection, "set_autocommit"):
            with contextlib.suppress(Exception):
                await connection.set_autocommit(bool(autocommit))

        # Call user-provided callback after connection setup
        if self._user_connection_hook is not None:
            await self._user_connection_hook(connection)

        return connection

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
