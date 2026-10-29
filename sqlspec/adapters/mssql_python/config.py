"""mssql-python database configuration."""

import contextlib
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from sqlspec.adapters.mssql_python._typing import MssqlPythonConnection, MssqlPythonSessionContext, TokenProvider
from sqlspec.adapters.mssql_python.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.mssql_python.driver import MssqlPythonDriver
from sqlspec.adapters.mssql_python.migrations import MssqlPythonSyncMigrationTracker
from sqlspec.adapters.mssql_python.pool import MssqlPythonConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import TypeCoercionCapabilities
from sqlspec.driver import SyncPoolConnectionContext, SyncPoolSessionFactory
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("MssqlPythonConfig", "MssqlPythonConnectionParams", "MssqlPythonDriverFeatures", "MssqlPythonPoolParams")


class MssqlPythonConnectionParams(TypedDict):
    """mssql-python connection parameters."""

    connection_string: NotRequired[str]
    """Full ODBC/TDS semicolon-delimited connection string passed to ``mssql_python.connect``."""
    server: NotRequired[str]
    """SQL Server hostname, ``host,port``, or ``host\\instance`` target."""
    address: NotRequired[str]
    """ODBC ``Address`` alias for ``server``."""
    addr: NotRequired[str]
    """ODBC ``Addr`` alias for ``server``."""
    port: NotRequired[int]
    """SQL Server TCP port (defaults to ``1433`` when omitted)."""
    database: NotRequired[str]
    """Initial SQL Server or Azure SQL database name."""
    db: NotRequired[str]
    """Alias normalized to ``Database`` in the connection string."""
    uid: NotRequired[str]
    """SQL authentication login ID."""
    user: NotRequired[str]
    """Alias normalized to ``UID`` in the connection string."""
    username: NotRequired[str]
    """Alias normalized to ``UID`` in the connection string."""
    pwd: NotRequired[str]
    """SQL authentication password."""
    password: NotRequired[str]
    """Alias normalized to ``PWD`` in the connection string."""
    authentication: NotRequired[str]
    """ODBC ``Authentication`` mode (such as ``ActiveDirectoryDefault`` or ``SqlPassword``)."""
    trusted_connection: NotRequired[bool | str]
    """Use integrated Windows / Kerberos authentication (``yes``/``no``)."""
    trust_server_certificate: NotRequired[bool | str]
    """Bypass TLS server certificate chain validation (``yes``/``no``)."""
    trust: NotRequired[bool | str]
    """Short alias for ``trust_server_certificate``."""
    encrypt: NotRequired[bool | str]
    """TLS encryption policy (``yes``, ``no``, ``strict``, ``mandatory``, or ``optional``)."""
    hostname_in_certificate: NotRequired[str]
    """Expected DNS subject name validated in the server TLS certificate."""
    hostnameincertificate: NotRequired[str]
    """ODBC keyword alias for ``hostname_in_certificate``."""
    server_certificate: NotRequired[str]
    """Filesystem path to an expected server certificate file for pinning."""
    servercertificate: NotRequired[str]
    """ODBC keyword alias for ``server_certificate``."""
    server_spn: NotRequired[str]
    """Kerberos Service Principal Name for the SQL Server target."""
    serverspn: NotRequired[str]
    """ODBC keyword alias for ``server_spn``."""
    multi_subnet_failover: NotRequired[bool | str]
    """Enable parallel IP connection attempts for Always On availability groups."""
    multisubnetfailover: NotRequired[bool | str]
    """ODBC keyword alias for ``multi_subnet_failover``."""
    application_intent: NotRequired[str]
    """Availability group workload routing intent (``ReadWrite`` or ``ReadOnly``)."""
    applicationintent: NotRequired[str]
    """ODBC keyword alias for ``application_intent``."""
    connect_retry_count: NotRequired[int]
    """Number of automatic connection retry attempts on broken idle connections."""
    connectretrycount: NotRequired[int]
    """ODBC keyword alias for ``connect_retry_count``."""
    connect_retry_interval: NotRequired[int]
    """Delay in seconds between automatic connection recovery attempts."""
    connectretryinterval: NotRequired[int]
    """ODBC keyword alias for ``connect_retry_interval``."""
    keep_alive: NotRequired[int]
    """TCP keepalive idle interval in milliseconds."""
    keepalive: NotRequired[int]
    """ODBC keyword alias for ``keep_alive``."""
    keep_alive_interval: NotRequired[int]
    """Interval in milliseconds between TCP keepalive probes."""
    keepaliveinterval: NotRequired[int]
    """ODBC keyword alias for ``keep_alive_interval``."""
    ip_address_preference: NotRequired[str]
    """Address family preference during DNS resolution (``IPv4First``, ``IPv6First``, or ``UsePlatformDefault``)."""
    ipaddresspreference: NotRequired[str]
    """ODBC keyword alias for ``ip_address_preference``."""
    packet_size: NotRequired[int]
    """TDS network packet size in bytes."""
    packetsize: NotRequired[int]
    """ODBC keyword alias for ``packet_size``."""
    connection_timeout: NotRequired[int]
    """Connection establishment timeout in seconds."""
    login_timeout: NotRequired[int]
    """Login handshake timeout in seconds."""
    command_timeout: NotRequired[int]
    """Default SQL statement execution timeout in seconds."""
    autocommit: NotRequired[bool]
    """Enable autocommit mode on new ``mssql-python`` connections."""
    attrs_before: NotRequired[dict[int, int | str | bytes]]
    """Pre-connect SQL_ATTR_* ODBC connection attributes dictionary (such as access tokens)."""
    timeout: NotRequired[int]
    """Query execution timeout in seconds assigned on the connection."""
    native_uuid: NotRequired[bool]
    """Return ``UNIQUEIDENTIFIER`` columns as ``uuid.UUID`` objects instead of strings."""
    token_provider: NotRequired[TokenProvider]
    """Callable or Entra ID credential provider supplying OAuth access tokens."""
    extra: NotRequired[dict[str, Any]]
    """Additional connection attributes forwarded to ``mssql_python.connect``."""


class MssqlPythonPoolParams(MssqlPythonConnectionParams):
    """mssql-python driver-level pooling parameters."""

    pool_size: NotRequired[int]
    """Maximum number of connections retained in ``MssqlPythonConnectionPool``."""
    pool_idle_timeout: NotRequired[int]
    """Idle duration in seconds before pooled SQL Server connections are closed."""
    pool_enabled: NotRequired[bool]
    """Enable ``mssql-python`` driver-level connection pooling."""


class MssqlPythonDriverFeatures(TypedDict):
    """mssql-python driver feature flags."""

    use_pool: NotRequired[bool]
    """Enable SQLSpec connection pooling around ``mssql-python`` connections (defaults to ``True``)."""
    json_serializer: "NotRequired[Callable[[Any], str]]"
    """Custom JSON serializer applied to bound JSON parameters (defaults to ``to_json``)."""
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    """Custom JSON deserializer applied to JSON result columns (defaults to ``from_json``)."""
    on_connection_create: "NotRequired[Callable[[MssqlPythonConnection], None]]"
    """Synchronous hook invoked with each raw ``mssql-python`` connection upon creation."""
    enable_events: NotRequired[bool]
    """Enable durable table-backed SQLSpec event queue support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend (``poll_queue`` is the sole SQL Server backend)."""


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
            if exc_type is not None:
                with contextlib.suppress(Exception):
                    self._conn.rollback()
            self._config.provide_pool().release(self._conn)
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


class MssqlPythonConfig(SyncDatabaseConfig[MssqlPythonConnection, MssqlPythonConnectionPool, MssqlPythonDriver]):
    """Configuration for mssql-python synchronous database connections."""

    __slots__ = ("_user_connection_hook",)

    driver_type: "ClassVar[type[MssqlPythonDriver]]" = MssqlPythonDriver
    connection_type: "ClassVar[type[MssqlPythonConnection]]" = MssqlPythonConnection
    migration_tracker_type: "ClassVar[type[MssqlPythonSyncMigrationTracker]]" = MssqlPythonSyncMigrationTracker
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_migration_schemas: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_arrow_streaming: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=False, uuid_binding="native"
    )
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
        statement_config, _ = apply_driver_features(statement_config or default_statement_config, features_dict)
        super().__init__(
            bind_key=bind_key,
            connection_config=normalized,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
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

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for SQL Server queue fallback."""
        return EventRuntimeHints(poll_interval=0.25, lease_seconds=5)

    def _create_pool(self) -> "MssqlPythonConnectionPool":
        return _create_mssql_python_pool(dict(self.connection_config), self.driver_features, self._user_connection_hook)

    def _close_pool(self) -> None:
        if self.connection_instance is not None:
            self.connection_instance.close()
            self.connection_instance = None


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
