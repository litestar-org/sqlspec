"""MysqlConnector database configuration."""

import contextlib
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, TypedDict, cast
from weakref import WeakSet

from sqlspec.adapters.mysqlconnector._typing import (
    MysqlConnectorAio,
    MysqlConnectorAsyncConnection,
    MysqlConnectorAsyncCursor,
    MysqlConnectorAsyncPool,
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
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import (
    AsyncPoolConnectionContext,
    AsyncPoolSessionFactory,
    SyncPoolConnectionContext,
    SyncPoolSessionFactory,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config, parse_mysql_dsn

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
    "build_connection_config",
)
mysql: "MysqlConnectorMysqlModule" = cast("MysqlConnectorMysqlModule", MysqlConnectorMysqlModule)
mysqlconnector_aio: "MysqlConnectorAio" = cast("MysqlConnectorAio", MysqlConnectorAio)


MysqlConnectorClientFlags = int | list[int] | tuple[int, ...]
MysqlConnectorPathSequence = str | list[str] | tuple[str, ...]
MysqlConnectorStringSequence = list[str] | tuple[str, ...]


class MysqlConnectorFailoverTarget(TypedDict):
    """Connector/Python failover target parameters."""

    host: NotRequired[str]
    """Failover MySQL server hostname or IP address."""
    user: NotRequired[str]
    """Account username for the failover host."""
    password: NotRequired[str]
    """Account password for the failover host."""
    database: NotRequired[str]
    """Target database name on the failover host."""
    port: NotRequired[int]
    """MySQL TCP port on the failover host."""
    unix_socket: NotRequired[str]
    """Unix socket path on the failover target."""
    pool_name: NotRequired[str]
    """Pool identifier used if the failover entry initializes a Connector/Python pool."""
    pool_size: NotRequired[int]
    """Pool size used if the failover entry initializes a Connector/Python pool."""


class MysqlConnectorCursorParams(TypedDict):
    """Connector/Python cursor parameters routed by SQLSpec."""

    buffered: NotRequired[bool]
    """Fetch the complete result set client-side immediately after query execution."""
    raw: NotRequired[bool]
    """Return raw MySQL wire bytes without Python type conversion."""
    dictionary: NotRequired[bool]
    """Return rows from raw cursors as column-keyed dictionaries."""
    prepared: NotRequired[bool]
    """Execute statements through the MySQL binary prepared-statement protocol."""
    cursor_class: NotRequired[type[Any]]
    """Custom ``MySQLCursor`` subclass passed to ``connection.cursor()``."""


class _MysqlConnectorBaseConnectionParams(TypedDict):
    """Common Connector/Python connection parameters."""

    host: NotRequired[str]
    """MySQL server hostname or IP address (defaults to ``'localhost'``)."""
    user: NotRequired[str]
    """Database account username."""
    username: NotRequired[str]
    """Alias normalized to ``user`` when ``user`` is omitted."""
    password: NotRequired[str]
    """Primary password for ``user``."""
    passwd: NotRequired[str]
    """Legacy Connector/Python alias for ``password``."""
    password1: NotRequired[str]
    """First authentication factor password for multi-factor MySQL accounts."""
    password2: NotRequired[str]
    """Second authentication factor password or token for MFA accounts."""
    password3: NotRequired[str]
    """Third authentication factor password or token for MFA accounts."""
    database: NotRequired[str]
    """Default schema/database selected upon connecting."""
    db: NotRequired[str]
    """Alias normalized to ``database`` when ``database`` is omitted."""
    port: NotRequired[int]
    """MySQL server TCP port (defaults to ``3306``)."""
    unix_socket: NotRequired[str]
    """Filesystem path to a local MySQL Unix domain socket."""
    conn_attrs: NotRequired[dict[str, str]]
    """Custom key/value attributes exposed in ``performance_schema.session_connect_attrs``."""
    init_command: NotRequired[str]
    """SQL statement executed immediately after establishing a connection."""
    auth_plugin: NotRequired[str]
    """Default client authentication plugin name (such as ``caching_sha2_password``)."""
    webauthn_callback: NotRequired[Callable[[], None] | str]
    """Callback invoked during FIDO/WebAuthn challenge-response authentication."""
    openid_token_file: NotRequired[str]
    """Path to a file containing an OpenID Connect identity token."""
    use_unicode: NotRequired[bool]
    """Decode character columns into Python ``str`` values (defaults to ``True``)."""
    charset: NotRequired[str]
    """Connection character set (defaults to ``'utf8mb4'``)."""
    collation: NotRequired[str]
    """Connection collation paired with ``charset``."""
    autocommit: NotRequired[bool]
    """Enable MySQL autocommit mode outside explicit transactions."""
    time_zone: NotRequired[str]
    """Session ``time_zone`` offset or named zone set on connect."""
    sql_mode: NotRequired[str]
    """Session ``sql_mode`` flags applied at connection startup."""
    get_warnings: NotRequired[bool]
    """Automatically fetch MySQL warnings after each statement."""
    raise_on_warnings: NotRequired[bool]
    """Raise a Python exception whenever the server reports a SQL warning."""
    connection_timeout: NotRequired[int | float]
    """Connection establishment timeout in seconds."""
    connect_timeout: NotRequired[int | float]
    """Alias for ``connection_timeout`` in seconds."""
    read_timeout: NotRequired[int | float]
    """Socket read timeout in seconds for server responses."""
    write_timeout: NotRequired[int | float]
    """Socket write timeout in seconds for outgoing packets."""
    client_flags: NotRequired[MysqlConnectorClientFlags]
    """MySQL ``ClientFlag`` bitmask or sequence of capability flags."""
    buffered: NotRequired[bool]
    """Default ``buffered`` cursor setting across statements on the connection."""
    raw: NotRequired[bool]
    """Default ``raw`` cursor setting returning unconverted wire bytes."""
    consume_results: NotRequired[bool]
    """Automatically drain unread cursor result sets before closing or reusing cursors."""
    tls_versions: NotRequired[MysqlConnectorStringSequence]
    """Permitted TLS protocol versions (such as ``['TLSv1.2', 'TLSv1.3']``)."""
    tls_ciphersuites: NotRequired[MysqlConnectorStringSequence]
    """Allowed TLS cipher suite names."""
    ssl_ca: NotRequired[str]
    """Path to the PEM CA bundle file used for server certificate verification."""
    ssl_cert: NotRequired[str]
    """Path to the client TLS certificate file."""
    ssl_key: NotRequired[str]
    """Path to the client TLS private key file."""
    ssl_cipher: NotRequired[str]
    """Deprecated TLS cipher specification string."""
    ssl_disabled: NotRequired[bool]
    """Disable TLS encryption completely when ``True``."""
    ssl_verify_cert: NotRequired[bool]
    """Validate the server TLS certificate chain against ``ssl_ca``."""
    ssl_verify_identity: NotRequired[bool]
    """Verify that the server TLS certificate subject/SAN matches ``host``."""
    force_ipv6: NotRequired[bool]
    """Prefer IPv6 addresses when a hostname resolves to both IPv4 and IPv6."""
    dns_srv: NotRequired[bool]
    """Resolve ``host`` as a DNS SRV record for multi-endpoint discovery."""
    kerberos_auth_mode: NotRequired[Literal["SSPI", "GSSAPI"]]
    """Kerberos client library mode on Windows (``SSPI`` or ``GSSAPI``)."""
    krb_service_principal: NotRequired[str]
    """Kerberos service principal name for ``authentication_kerberos_client``."""
    oci_config_file: NotRequired[str]
    """Path to an Oracle Cloud Infrastructure SDK configuration file for IAM auth."""
    oci_config_profile: NotRequired[str]
    """Profile section within ``oci_config_file``."""
    compress: NotRequired[bool]
    """Enable zlib packet compression on the MySQL wire protocol."""
    converter_class: NotRequired[type[Any]]
    """Custom ``MySQLConverterBase`` subclass converting Python and MySQL values."""
    converter_str_fallback: NotRequired[bool]
    """Serialize unsupported Python types via ``str(value)`` inside the converter."""
    failover: NotRequired[list[MysqlConnectorFailoverTarget] | tuple[MysqlConnectorFailoverTarget, ...]]
    """Ordered sequence of ``MysqlConnectorFailoverTarget`` server definitions."""
    option_files: NotRequired[MysqlConnectorPathSequence]
    """MySQL ``my.cnf`` / ``my.ini`` option file path or paths to load."""
    option_groups: NotRequired[MysqlConnectorStringSequence]
    """Option group names read from ``option_files``."""
    allow_local_infile: NotRequired[bool]
    """Permit ``LOAD DATA LOCAL INFILE`` client file uploads."""
    local_infile: NotRequired[bool]
    """Alias enabling ``LOAD DATA LOCAL INFILE`` transfers."""
    allow_local_infile_in_path: NotRequired[str]
    """Restrict ``LOAD DATA LOCAL INFILE`` uploads to files beneath this directory."""
    use_pure: NotRequired[bool]
    """Force the pure-Python Connector/Python implementation instead of the C extension."""
    dsn: NotRequired[str]
    """MySQL connection URI parsed by SQLSpec into Connector/Python keyword arguments."""
    url: NotRequired[str]
    """Connection URL alias parsed identically to ``dsn``."""
    connection_string: NotRequired[str]
    """Connection string alias parsed identically to ``dsn``."""
    extra: NotRequired["dict[str, Any]"]
    """Additional keyword arguments forwarded to ``mysql.connector.connect``."""


class MysqlConnectorSyncConnectionParams(_MysqlConnectorBaseConnectionParams):
    """MysqlConnector sync connection parameters."""

    pool_name: NotRequired[str]
    """Identifier assigned to the native ``MySQLConnectionPool``."""
    pool_size: NotRequired[int]
    """Number of connections maintained by ``MySQLConnectionPool`` (max ``32`` by default)."""
    pool_reset_session: NotRequired[bool]
    """Reset session variables when returning a connection to ``MySQLConnectionPool``."""


class MysqlConnectorPoolParams(MysqlConnectorSyncConnectionParams):
    """MysqlConnector pooling parameters.

    Note: pool_name, pool_size, and pool_reset_session are inherited from
    MysqlConnectorSyncConnectionParams.
    """


class MysqlConnectorAsyncConnectionParams(_MysqlConnectorBaseConnectionParams):
    """MysqlConnector async connection parameters."""

    pool_name: NotRequired[str]
    """Identifier assigned to the async connection pool."""
    pool_size: NotRequired[int]
    """Maximum number of connections managed by the async pool."""
    pool_reset_session: NotRequired[bool]
    """Reset session state when releasing a connection back to the async pool."""


class MysqlConnectorDriverFeatures(TypedDict):
    """Optional Connector/Python driver feature flags, cursor options, and bulk-load settings."""

    json_serializer: NotRequired["Callable[[Any], str]"]
    """Custom JSON serializer applied to bound JSON parameters (defaults to ``to_json``)."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom JSON deserializer applied to JSON result columns (defaults to ``from_json``)."""
    on_connection_create: "NotRequired[Callable[..., Any]]"
    """Sync or async callback invoked when a physical or session-reset connection is acquired."""
    enable_events: NotRequired[bool]
    """Enable durable table-backed SQLSpec event queue support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend (``poll_queue`` is the sole MySQL backend)."""
    cursor_options: NotRequired[MysqlConnectorCursorParams]
    """Default ``MysqlConnectorCursorParams`` forwarded to ``connection.cursor()``."""
    enable_local_infile_bulk_load: NotRequired[bool]
    """Route ``load_from_arrow`` through ``LOAD DATA LOCAL INFILE`` when local infile is enabled."""


def build_connection_config(
    connection_config: "MysqlConnectorPoolParams | MysqlConnectorAsyncConnectionParams | Mapping[str, Any] | None",
) -> dict[str, Any]:
    """Normalize mysqlconnector connection configuration mapping, parsing any DSN and applying overrides."""
    config = normalize_connection_config(connection_config)
    dsn = config.pop("dsn", None) or config.pop("url", None) or config.pop("connection_string", None)
    user_alias = config.pop("username", None)
    if user_alias is not None and "user" not in config:
        config["user"] = user_alias
    db_alias = config.pop("db", None)
    if db_alias is not None and "database" not in config:
        config["database"] = db_alias
    if dsn is not None and isinstance(dsn, str):
        dsn_params = parse_mysql_dsn(dsn)
        for key, value in dsn_params.items():
            config.setdefault(key, value)
    config.setdefault("host", "localhost")
    config.setdefault("port", 3306)
    config.setdefault("charset", "utf8mb4")
    local_infile = bool(config.pop("local_infile", False))
    config["allow_local_infile"] = bool(config.get("allow_local_infile", False) or local_infile)
    return config


class MysqlConnectorSyncConnectionContext(SyncPoolConnectionContext):
    """Context manager for mysql-connector sync connections."""

    __slots__ = ("_connection",)

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        super().__init__(config)
        self._connection: MysqlConnectorSyncConnection | None = None

    def __enter__(self) -> MysqlConnectorSyncConnection:
        self._connection = self._config._acquire_sync_connection()
        return cast("MysqlConnectorSyncConnection", self._connection)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._connection is not None:
            conn = self._connection
            self._connection = None
            if not self._config.connection_config.get("pool_reset_session", True) and getattr(
                conn, "in_transaction", False
            ):
                with contextlib.suppress(Exception):
                    conn.rollback()
            conn.close()
        return None


class _MysqlConnectorSyncSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ("_connection",)

    def __init__(self, config: "MysqlConnectorSyncConfig") -> None:
        super().__init__(config)
        self._connection: MysqlConnectorSyncConnection | None = None

    def acquire_connection(self) -> MysqlConnectorSyncConnection:
        self._connection = self._config._acquire_sync_connection()
        return cast("MysqlConnectorSyncConnection", self._connection)

    def release_connection(self, _conn: MysqlConnectorSyncConnection, **kwargs: Any) -> None:
        if self._connection is None:
            return
        conn = self._connection
        self._connection = None
        if not self._config.connection_config.get("pool_reset_session", True) and getattr(
            conn, "in_transaction", False
        ):
            with contextlib.suppress(Exception):
                conn.rollback()
        conn.close()


class MysqlConnectorAsyncConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for mysql-connector async connections."""

    __slots__ = ()

    async def __aenter__(self) -> MysqlConnectorAsyncConnection:
        self._connection = await self._config._acquire_async_connection()
        return cast("MysqlConnectorAsyncConnection", self._connection)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._connection is not None:
            conn = self._connection
            self._connection = None
            if not self._config.connection_config.get("pool_reset_session", True) and getattr(
                conn, "in_transaction", False
            ):
                with contextlib.suppress(Exception):
                    await conn.rollback()
            await conn.close()
        return None


class _MysqlConnectorAsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    __slots__ = ()

    async def acquire_connection(self) -> MysqlConnectorAsyncConnection:
        self._connection = await self._config._acquire_async_connection()
        return cast("MysqlConnectorAsyncConnection", self._connection)

    async def release_connection(self, _conn: MysqlConnectorAsyncConnection, **kwargs: Any) -> None:
        if self._connection is None:
            return
        conn = self._connection
        self._connection = None
        if not self._config.connection_config.get("pool_reset_session", True) and getattr(
            conn, "in_transaction", False
        ):
            with contextlib.suppress(Exception):
                await conn.rollback()
        await conn.close()


_POOL_ONLY_CONFIG_KEYS: Final[frozenset[str]] = frozenset({"pool_name", "pool_size", "pool_reset_session"})


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
    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
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
        connection_config = build_connection_config(connection_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[MysqlConnectorSyncConnection], None] | None = features_dict.pop(
            "on_connection_create", None
        )
        self._initialized_connections: WeakSet[Any] = WeakSet()

        features_dict.setdefault("enable_local_infile_bulk_load", connection_config["allow_local_infile"])
        if features_dict.get("enable_local_infile_bulk_load") and not connection_config.get("allow_local_infile"):
            msg = "enable_local_infile_bulk_load requires local_infile=True or allow_local_infile=True in connection_config."
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

    def create_connection(self) -> MysqlConnectorSyncConnection:
        """Open a standalone connection owned by the caller.

        Pool settings are dropped, because mysql-connector routes ``connect`` to
        its own module-global pool as soon as it sees one, which would return a
        wrapper that neither applies autocommit nor closes on request.

        Returns:
            A newly opened mysql-connector connection.
        """
        config = {key: value for key, value in self.connection_config.items() if key not in _POOL_ONLY_CONFIG_KEYS}
        connection = mysql.connector.connect(**config)
        try:
            autocommit = config.get("autocommit")
            if autocommit is not None:
                connection.autocommit = bool(autocommit)
            self._ensure_connection(connection)
        except BaseException:
            with contextlib.suppress(Exception):
                connection.close()
            raise
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

    def _ensure_connection(self, connection: "MysqlConnectorSyncConnection") -> None:
        """Initialize connection state after creation or a native session reset."""
        if self._user_connection_hook is None:
            return
        underlying = getattr(connection, "_cnx", None)
        if underlying is not None and self.connection_config.get("pool_reset_session", True):
            self._user_connection_hook(connection)
            return
        underlying = underlying or connection
        if underlying not in self._initialized_connections:
            self._user_connection_hook(connection)
            self._initialized_connections.add(underlying)

    def _acquire_sync_connection(self) -> MysqlConnectorSyncConnection:
        """Acquire and initialize a sync mysql-connector connection."""
        pool = self.provide_pool()
        connection = cast("MysqlConnectorSyncConnection", pool.get_connection())
        try:
            self._ensure_connection(connection)
        except BaseException:
            with contextlib.suppress(Exception):
                connection.close()
            raise
        return connection

    def _create_pool(self) -> "MysqlConnectorConnectionPool":
        config = dict(self.connection_config)
        pool_name = config.pop("pool_name", None)
        pool_size = config.pop("pool_size", None)
        pool_reset = config.pop("pool_reset_session", True)
        return MysqlConnectorConnectionPool(
            pool_name=pool_name,
            pool_size=5 if pool_size is None else pool_size,
            pool_reset_session=pool_reset,
            **config,
        )

    def _close_pool(self) -> None:
        pool = self.connection_instance
        if pool is not None:
            cnx_queue = getattr(pool, "_cnx_queue", None)
            if cnx_queue is not None:
                while not cnx_queue.empty():
                    with contextlib.suppress(Exception):
                        conn = cnx_queue.get_nowait()
                        conn.close()
            self.connection_instance = None


class MysqlConnectorAsyncConfig(
    AsyncDatabaseConfig[MysqlConnectorAsyncConnection, "MysqlConnectorAsyncPool", MysqlConnectorAsyncDriver]
):
    """Configuration for mysql-connector async MySQL connections."""

    driver_type: ClassVar[type[MysqlConnectorAsyncDriver]] = MysqlConnectorAsyncDriver
    connection_type: "ClassVar[type[Any]]" = cast("type[Any]", MysqlConnectorAsyncConnection)
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    type_coercion_capabilities: ClassVar[TypeCoercionCapabilities] = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="text"
    )
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
        connection_config = build_connection_config(connection_config)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[MysqlConnectorAsyncConnection], Awaitable[None]] | None = (
            features_dict.pop("on_connection_create", None)
        )

        features_dict.setdefault("enable_local_infile_bulk_load", connection_config["allow_local_infile"])
        if features_dict.get("enable_local_infile_bulk_load") and not connection_config.get("allow_local_infile"):
            msg = "enable_local_infile_bulk_load requires local_infile=True or allow_local_infile=True in connection_config."
            raise ImproperConfigurationError(msg)

        self._initialized_connections: WeakSet[Any] = WeakSet()

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

    async def create_connection(self) -> MysqlConnectorAsyncConnection:
        """Open and initialize a standalone connection owned by the caller."""
        config = {key: value for key, value in self.connection_config.items() if key not in _POOL_ONLY_CONFIG_KEYS}
        connection = await mysqlconnector_aio.connect(**config)
        try:
            autocommit = self.connection_config.get("autocommit")
            if autocommit is not None:
                await connection.set_autocommit(bool(autocommit))
            await self._ensure_connection(connection)
        except BaseException:
            with contextlib.suppress(Exception):
                await connection.close()
            raise
        return connection

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

    async def _create_pool(self) -> "MysqlConnectorAsyncPool":
        if MysqlConnectorAsyncPool is None:
            msg = "Async connection pooling requires mysql-connector-python 9.4 or later"
            raise ImproperConfigurationError(msg)
        config = dict(self.connection_config)
        pool_name = config.pop("pool_name", None)
        pool_size = config.pop("pool_size", None)
        pool_reset = config.pop("pool_reset_session", True)
        pool = MysqlConnectorAsyncPool(
            pool_name=pool_name,
            pool_size=5 if pool_size is None else pool_size,
            pool_reset_session=pool_reset,
            **config,
        )
        try:
            await pool.initialize_pool()
        except BaseException:
            with contextlib.suppress(Exception):
                await pool.close_pool()
            raise
        return pool

    async def _close_pool(self) -> None:
        if self.connection_instance is not None:
            await self.connection_instance.close_pool()
            self.connection_instance = None

    async def _ensure_connection(self, connection: "MysqlConnectorAsyncConnection") -> None:
        """Initialize connection state after creation or a native session reset."""
        if self._user_connection_hook is None:
            return
        underlying = getattr(connection, "_cnx", None)
        if underlying is not None and self.connection_config.get("pool_reset_session", True):
            await self._user_connection_hook(connection)
            return
        underlying = underlying or connection
        if underlying not in self._initialized_connections:
            await self._user_connection_hook(connection)
            self._initialized_connections.add(underlying)

    async def _acquire_async_connection(self) -> MysqlConnectorAsyncConnection:
        """Acquire and initialize an async mysql-connector connection from pool."""
        if (
            MysqlConnectorAsyncPool is None
            and self.connection_instance is None
            and not any(key in self.connection_config for key in _POOL_ONLY_CONFIG_KEYS)
        ):
            return await self.create_connection()
        pool = await self.provide_pool()
        connection = cast("MysqlConnectorAsyncConnection", await pool.get_connection())
        try:
            await self._ensure_connection(connection)
        except BaseException:
            with contextlib.suppress(Exception):
                await connection.close()
            raise
        return connection
