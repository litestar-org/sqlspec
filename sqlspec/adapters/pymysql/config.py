"""PyMySQL database configuration."""

import ssl
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.pymysql._typing import PyMysqlConnection, PyMysqlCursor, PyMysqlRawCursor, PyMysqlSessionContext
from sqlspec.adapters.pymysql.core import apply_driver_features, default_statement_config
from sqlspec.adapters.pymysql.driver import PyMysqlDriver, PyMysqlExceptionHandler
from sqlspec.adapters.pymysql.pool import PyMysqlConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.driver._sync import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "PyMysqlConfig",
    "PyMysqlConnectionParams",
    "PyMysqlConverter",
    "PyMysqlDriverFeatures",
    "PyMysqlPoolParams",
    "PyMysqlSslConfig",
    "PyMysqlSslParams",
    "PyMysqlTimeout",
)


PyMysqlConverter = Mapping[int | type[Any], Callable[..., Any]]
PyMysqlTimeout = int | float


class PyMysqlSslParams(TypedDict):
    """Mapping-style PyMySQL SSL parameters.

    Passing an ``ssl`` mapping is deprecated by PyMySQL but remains supported
    for compatibility with existing SQLSpec configs.
    """

    ca: NotRequired[str]
    capath: NotRequired[str]
    cert: NotRequired[str]
    key: NotRequired[str]
    password: NotRequired[str]
    cipher: NotRequired[str]
    check_hostname: NotRequired[bool]
    verify_mode: NotRequired[bool | int | str]


PyMysqlSslConfig = ssl.SSLContext | PyMysqlSslParams | Mapping[str, Any]


class PyMysqlConnectionParams(TypedDict):
    """PyMySQL connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    charset: NotRequired[str]
    collation: NotRequired[str]
    sql_mode: NotRequired[str]
    read_default_file: NotRequired[str]
    read_default_group: NotRequired[str]
    conv: NotRequired[PyMysqlConverter]
    use_unicode: NotRequired[bool]
    client_flag: NotRequired[int]
    cursorclass: NotRequired[type[PyMysqlRawCursor]]
    init_command: NotRequired[str]
    connect_timeout: NotRequired[PyMysqlTimeout]
    read_timeout: NotRequired[PyMysqlTimeout]
    write_timeout: NotRequired[PyMysqlTimeout]
    autocommit: NotRequired[bool]
    local_infile: NotRequired[bool]
    max_allowed_packet: NotRequired[int]
    defer_connect: NotRequired[bool]
    auth_plugin_map: NotRequired[Mapping[str, type[Any]]]
    bind_address: NotRequired[str]
    binary_prefix: NotRequired[bool]
    program_name: NotRequired[str]
    server_public_key: NotRequired[str | bytes]
    ssl: NotRequired[PyMysqlSslConfig]
    ssl_ca: NotRequired[str]
    ssl_cert: NotRequired[str]
    ssl_disabled: NotRequired[bool]
    ssl_key: NotRequired[str]
    ssl_key_password: NotRequired[str]
    ssl_verify_cert: NotRequired[bool]
    ssl_verify_identity: NotRequired[bool]
    extra: NotRequired["dict[str, Any]"]


class PyMysqlPoolParams(PyMysqlConnectionParams):
    """PyMySQL pool parameters."""

    pool_recycle_seconds: NotRequired[int]
    health_check_interval: NotRequired[float]


class PyMysqlDriverFeatures(TypedDict):
    """PyMySQL driver feature flags.

    json_serializer: Custom JSON serializer function.
     Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
     Defaults to sqlspec.utils.serializers.from_json.
    on_connection_create: Callback executed when a connection is created.
     Receives the raw pymysql connection for low-level driver configuration.
     Runs after connection creation.
    enable_events: Enable database event channel support.
    events_backend: Event channel backend selection.
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[PyMysqlConnection], None]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class PyMysqlConnectionContext(SyncPoolConnectionContext):
    """Context manager for PyMySQL connections."""

    __slots__ = ()


class _PyMysqlSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()


class PyMysqlConfig(SyncDatabaseConfig[PyMysqlConnection, PyMysqlConnectionPool, PyMysqlDriver]):
    """Configuration for PyMySQL synchronous connections."""

    driver_type: "ClassVar[type[PyMysqlDriver]]" = PyMysqlDriver
    connection_type: "ClassVar[type[PyMysqlConnection]]" = cast("type[PyMysqlConnection]", PyMysqlConnection)
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    _connection_context_class: "ClassVar[type[PyMysqlConnectionContext]]" = PyMysqlConnectionContext
    _session_factory_class: "ClassVar[type[_PyMysqlSessionConnectionHandler]]" = _PyMysqlSessionConnectionHandler
    _session_context_class: "ClassVar[type[PyMysqlSessionContext]]" = PyMysqlSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "PyMysqlPoolParams | dict[str, Any] | None" = None,
        connection_instance: "PyMysqlConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "PyMysqlDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        connection_config = normalize_connection_config(connection_config)
        connection_config.setdefault("host", "localhost")
        connection_config.setdefault("port", 3306)
        connection_config.setdefault("local_infile", False)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        # Extract user connection hook before storing driver_features
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[PyMysqlConnection], None] | None = features_dict.pop(
            "on_connection_create", None
        )

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

    def _create_pool(self) -> "PyMysqlConnectionPool":
        config = dict(self.connection_config)
        pool_recycle = config.pop("pool_recycle_seconds", 86400)
        health_check = config.pop("health_check_interval", 30.0)
        return PyMysqlConnectionPool(
            config,
            recycle_seconds=pool_recycle,
            health_check_interval=health_check,
            on_connection_create=self._user_connection_hook,
        )

    def _close_pool(self) -> None:
        if self.connection_instance:
            self.connection_instance.close()

    def create_connection(self) -> PyMysqlConnection:
        pool = self.provide_pool()
        return pool.acquire()

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "PyMysqlConnectionContext": PyMysqlConnectionContext,
            "PyMysqlConnection": PyMysqlConnection,
            "PyMysqlConnectionParams": PyMysqlConnectionParams,
            "PyMysqlConnectionPool": PyMysqlConnectionPool,
            "PyMysqlCursor": PyMysqlCursor,
            "PyMysqlDriver": PyMysqlDriver,
            "PyMysqlDriverFeatures": PyMysqlDriverFeatures,
            "PyMysqlExceptionHandler": PyMysqlExceptionHandler,
            "PyMysqlPoolParams": PyMysqlPoolParams,
            "PyMysqlSessionContext": PyMysqlSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5, select_for_update=True, skip_locked=True)
