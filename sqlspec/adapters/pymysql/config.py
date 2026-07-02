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
from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.typing import CLOUD_SQL_CONNECTOR_INSTALLED
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
    enable_local_infile_bulk_load: Route load_from_arrow through LOAD DATA LOCAL INFILE.
     Requires local_infile=True in connection_config.
    enable_cloud_sql: Enable Google Cloud SQL connector integration.
     Requires cloud-sql-python-connector package.
     Defaults to False (explicit opt-in required).
    cloud_sql_instance: Cloud SQL instance connection name.
     Format: "project:region:instance"
     Required when enable_cloud_sql is True.
    cloud_sql_enable_iam_auth: Enable IAM database authentication.
     Defaults to False for passwordless authentication.
    cloud_sql_ip_type: IP address type for connection.
     Options: "PUBLIC", "PRIVATE", "PSC"
     Defaults to "PRIVATE".
    """

    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[PyMysqlConnection], None]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]
    enable_local_infile_bulk_load: NotRequired[bool]
    enable_cloud_sql: NotRequired[bool]
    cloud_sql_instance: NotRequired[str]
    cloud_sql_enable_iam_auth: NotRequired[bool]
    cloud_sql_ip_type: NotRequired[str]


_CLOUD_SQL_DIRECT_CONNECTION_KEYS = frozenset((
    "bind_address",
    "database",
    "host",
    "password",
    "port",
    "ssl",
    "unix_socket",
    "user",
))


class _PyMysqlCloudSqlConnector:
    __slots__ = ("_config", "_database", "_driver_kwargs", "_password", "_user")

    def __init__(
        self,
        config: "PyMysqlConfig",
        user: str | None,
        password: str | None,
        database: str | None,
        driver_kwargs: "dict[str, Any]",
    ) -> None:
        self._config = config
        self._user = user
        self._password = password
        self._database = database
        self._driver_kwargs = driver_kwargs

    def __call__(self) -> "PyMysqlConnection":
        connector = self._config.get_cloud_sql_connector()
        if connector is None:
            msg = "Cloud SQL connector is not initialized"
            raise ImproperConfigurationError(msg)

        conn_kwargs: dict[str, Any] = {
            **self._driver_kwargs,
            "instance_connection_string": self._config.driver_features["cloud_sql_instance"],
            "driver": "pymysql",
            "enable_iam_auth": self._config.driver_features.get("cloud_sql_enable_iam_auth", False),
            "ip_type": self._config.driver_features.get("cloud_sql_ip_type", "PRIVATE"),
        }
        if self._user:
            conn_kwargs["user"] = self._user
        if self._password:
            conn_kwargs["password"] = self._password
        if self._database:
            conn_kwargs["db"] = self._database

        return cast("PyMysqlConnection", connector.connect(**conn_kwargs))


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

        if features_dict.get("enable_local_infile_bulk_load") and not connection_config.get("local_infile"):
            msg = "enable_local_infile_bulk_load requires local_infile=True in connection_config."
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

        self._cloud_sql_connector: Any | None = None
        self._validate_connector_config()

    def get_cloud_sql_connector(self) -> Any | None:
        """Return the configured Cloud SQL connector instance."""
        return self._cloud_sql_connector

    def _validate_connector_config(self) -> None:
        """Validate Google Cloud SQL connector configuration."""
        if not self.driver_features.get("enable_cloud_sql", False):
            return

        if not CLOUD_SQL_CONNECTOR_INSTALLED:
            raise MissingDependencyError(package="cloud-sql-python-connector", install_package="cloud-sql")

        instance = self.driver_features.get("cloud_sql_instance")
        if not instance:
            msg = "cloud_sql_instance required when enable_cloud_sql is True. Format: 'project:region:instance'"
            raise ImproperConfigurationError(msg)

        cloud_sql_instance_parts_expected = 2
        if instance.count(":") != cloud_sql_instance_parts_expected:
            msg = f"Invalid Cloud SQL instance format: {instance}. Expected format: 'project:region:instance'"
            raise ImproperConfigurationError(msg)

    def _setup_cloud_sql_connector(self, config: "dict[str, Any]") -> "_PyMysqlCloudSqlConnector":
        """Setup Cloud SQL connector and return a pool connection factory."""
        from google.cloud.sql.connector import Connector  # type: ignore[import-untyped,unused-ignore]

        self._cloud_sql_connector = Connector()

        user = config.get("user")
        password = config.get("password")
        database = config.get("database")

        for key in _CLOUD_SQL_DIRECT_CONNECTION_KEYS:
            config.pop(key, None)

        return _PyMysqlCloudSqlConnector(self, user, password, database, dict(config))

    def _create_pool(self) -> "PyMysqlConnectionPool":
        config = dict(self.connection_config)
        pool_recycle = config.pop("pool_recycle_seconds", 86400)
        health_check = config.pop("health_check_interval", 30.0)
        connection_factory = None
        if self.driver_features.get("enable_cloud_sql", False):
            connection_factory = self._setup_cloud_sql_connector(config)
        return PyMysqlConnectionPool(
            config,
            recycle_seconds=pool_recycle,
            health_check_interval=health_check,
            on_connection_create=self._user_connection_hook,
            connection_factory=connection_factory,
        )

    def _close_pool(self) -> None:
        if self.connection_instance:
            self.connection_instance.close()
            self.connection_instance = None

        if self._cloud_sql_connector is not None:
            self._cloud_sql_connector.close()
            self._cloud_sql_connector = None

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
