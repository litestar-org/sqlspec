"""OracleDB database configuration with direct field-based configuration."""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from inspect import isawaitable
from pathlib import Path
from ssl import TLSVersion
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, Self, TypeAlias, TypedDict, cast

from sqlspec.adapters.oracledb._json_handlers import register_json_handlers
from sqlspec.adapters.oracledb._plugins import (
    OraclePluginName,
    ensure_oracle_config_plugins,
    refresh_oracle_config_cache,
)
from sqlspec.adapters.oracledb._typing import (
    OracleAsyncConnection,
    OracleAsyncConnectionPool,
    OracleAsyncCursor,
    OracleAsyncSessionContext,
    OracleAsyncSessionlessTransactionContext,
    OracleSyncConnection,
    OracleSyncConnectionPool,
    OracleSyncCursor,
    OracleSyncSessionContext,
    OracleSyncSessionlessTransactionContext,
)
from sqlspec.adapters.oracledb._typing import OracleAuthMode as AuthMode
from sqlspec.adapters.oracledb._typing import OraclePoolGetMode as PoolGetMode
from sqlspec.adapters.oracledb._typing import OraclePurity as Purity
from sqlspec.adapters.oracledb._typing import oracledb_module as oracledb
from sqlspec.adapters.oracledb._uuid_handlers import register_uuid_handlers
from sqlspec.adapters.oracledb._vector_handlers import register_numpy_handlers
from sqlspec.adapters.oracledb.core import (
    VALID_ORACLE_DRIVER_FEATURE_KEYS,
    OracleAcquireOptions,
    OracleEndpoint,
    OracleEndUserSecurityConfig,
    OracleSessionContextConfig,
    OracleTopologyConfig,
    apply_driver_features,
    apply_oracle_transaction_priority,
    apply_wallet_ssl_context,
    build_connection_config,
    capture_oracle_transaction_priority_baseline,
    clear_oracle_otel_trace_context,
    create_mapped_exception,
    default_statement_config,
    execute_session_checkout_async,
    execute_session_checkout_sync,
    execute_session_release_async,
    execute_session_release_sync,
    extract_oracle_wallet_zip,
    extract_static_alter_session_config,
    forget_oracle_session_state,
    has_callable_alter_session_fields,
    is_oracle_standby_failover_error,
    oracle_transaction_priority_matches,
    release_extracted_wallet_dir,
    retain_extracted_wallet_dir,
    rewrite_oracle_dsn_service_name,
    synthesize_session_state_tag,
    validate_oracle_drcp_and_mode_constraints,
    validate_oracle_session_callback,
)
from sqlspec.adapters.oracledb.data_dictionary import (
    OracleVersionCache,
    resolve_oracle_connection_compatible_major,
    resolve_oracle_connection_major,
)
from sqlspec.adapters.oracledb.driver import (
    OracleAsyncDriver,
    OracleAsyncExceptionHandler,
    OracleSyncDriver,
    OracleSyncExceptionHandler,
)
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.adapters.oracledb.tenant import (
    OracleAsyncClientContextStrategy,
    OracleAsyncEndUserSecurityStrategy,
    OracleAsyncIdentifierStrategy,
    OracleAsyncTenantLease,
    OracleShardResolver,
    OracleSyncClientContextStrategy,
    OracleSyncEndUserSecurityStrategy,
    OracleSyncIdentifierStrategy,
    OracleSyncTenantLease,
    bind_oracle_tenant_hook,
    validate_oracle_tenant_shard_features,
)
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, MigrationConfig, SyncDatabaseConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.core.tenant import TenantStrategy, TenantStrategyShorthand
from sqlspec.driver import (
    AsyncPoolConnectionContext,
    AsyncPoolSessionFactory,
    SyncPoolConnectionContext,
    SyncPoolSessionFactory,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


__all__ = (
    "OracleAsyncConfig",
    "OracleConnectionParams",
    "OracleDriverFeatures",
    "OracleEndUserSecurityConfig",
    "OracleEndpoint",
    "OraclePoolBoundary",
    "OraclePoolParams",
    "OraclePurityInput",
    "OracleSessionContextConfig",
    "OracleSyncConfig",
    "OracleTopologyConfig",
    "OracleTransactionPriority",
)


OracleAccessToken = str | tuple[str, str] | Callable[..., str | tuple[str, str]]
OracleAppContext = tuple[str, str, str]
OracleProtocol = Literal["tcp", "tcps"]
OracleServerType = Literal["dedicated", "shared", "pooled"]
OraclePoolBoundary = Literal["statement", "transaction"]
OracleVectorReturnFormat = Literal["array", "list", "numpy"]
OracleEventsBackend = Literal["aq", "poll_queue", "txeventq"]
OracleTenantStrategyShorthand: TypeAlias = TenantStrategyShorthand | Literal["client_context"]
OraclePurityInput: TypeAlias = Purity | Literal["DEFAULT", "NEW", "SELF"] | int
OracleTransactionPriority: TypeAlias = Literal["DEFAULT", "LOW", "MEDIUM", "HIGH"]
_ORACLE_VECTOR_MIN_MAJOR = 23
logger = logging.getLogger(__name__)


class OracleConnectionParams(TypedDict):
    """OracleDB connection parameters forwarded to ``python-oracledb``."""

    dsn: NotRequired[str]
    """Easy Connect string, TNS alias, or full connect descriptor."""
    pool_alias: NotRequired[str]
    """Named Thick-mode pool alias registered in the driver cache."""
    user: NotRequired[str]
    """Database username for authentication."""
    proxy_user: NotRequired[str]
    """Proxy target username appended inside ``[proxy_user]`` credentials."""
    password: NotRequired[str]
    """Database password for the authenticating user."""
    newpassword: NotRequired[str]
    """Replacement password applied atomically during connection establishment."""
    wallet_password: NotRequired[str]
    """Passphrase decrypting an encrypted PEM ``ewallet.pem`` or PKCS#12 ``ewallet.p12`` wallet."""
    access_token: NotRequired[OracleAccessToken]
    """OAuth 2.0 or IAM bearer token string, ``(token, private_key)`` tuple, or refresh callable."""
    host: NotRequired[str]
    """Oracle listener hostname or IP address when building a connect descriptor."""
    port: NotRequired[int]
    """Oracle listener TCP/TCPS port (defaults to ``1521``)."""
    protocol: NotRequired[OracleProtocol]
    """Transport protocol (``'tcp'`` or ``'tcps'`` for TLS/mTLS)."""
    https_proxy: NotRequired[str]
    """HTTPS proxy hostname used when tunneling TCPS connections."""
    https_proxy_port: NotRequired[int]
    """HTTPS proxy TCP port used with ``https_proxy``."""
    service_name: NotRequired[str]
    """Oracle database service name registered with the listener."""
    instance_name: NotRequired[str]
    """Specific RAC instance name targeted within the service."""
    sid: NotRequired[str]
    """Legacy Oracle System Identifier used when ``service_name`` is unavailable."""
    server_type: NotRequired[OracleServerType]
    """Server handler mode (``'dedicated'``, ``'shared'``, or ``'pooled'`` for DRCP)."""
    cclass: NotRequired[str]
    """Database Resident Connection Pooling (DRCP) connection class name."""
    purity: NotRequired[Purity]
    """DRCP session purity (``PURITY_NEW`` or ``PURITY_SELF``)."""
    expire_time: NotRequired[int]
    """Dead-connection detection probe interval in minutes."""
    externalauth: NotRequired[bool]
    """Authenticate via OS credentials or an Oracle external password store wallet."""
    mode: NotRequired[AuthMode]
    """Administrative privilege mode such as ``AUTH_MODE_SYSDBA`` or ``AUTH_MODE_SYSOPER``."""
    wallet_location: NotRequired[str]
    """Directory path containing ``ewallet.pem`` or ``ewallet.p12`` for mTLS."""
    config_dir: NotRequired[str]
    """Directory path containing ``tnsnames.ora`` and ``sqlnet.ora`` configuration files."""
    tcp_connect_timeout: NotRequired[float]
    """Maximum seconds to wait when establishing the initial TCP socket."""
    retry_count: NotRequired[int]
    """Number of connection-level retry attempts across listener endpoints."""
    retry_delay: NotRequired[int]
    """Delay in seconds between connection retry attempts."""
    ssl_server_dn_match: NotRequired[bool]
    """Verify that the server certificate distinguished name matches the target service."""
    ssl_server_cert_dn: NotRequired[str]
    """Expected distinguished name on the Oracle server TLS certificate."""
    events: NotRequired[bool]
    """Enable Thick-mode Continuous Query Notification and FAN HA event delivery."""
    disable_oob: NotRequired[bool]
    """Disable TCP out-of-band break messages on networks that drop urgent packets."""
    stmtcachesize: NotRequired[int]
    """Per-connection cursor statement cache size."""
    edition: NotRequired[str]
    """Edition-Based Redefinition (EBR) edition name activated on connect."""
    tag: NotRequired[str]
    """Pool session state tag requested when acquiring a connection."""
    matchanytag: NotRequired[bool]
    """Allow acquiring a pooled connection with a different tag when an exact match is unavailable."""
    appcontext: NotRequired[Sequence[OracleAppContext] | Mapping[str, Mapping[str, str]]]
    """Initial ``(namespace, name, value)`` application context entries set on connect."""
    sharding_key: NotRequired[Sequence[Any]]
    """Primary shard key forwarded as native ``shardingkey``."""
    super_sharding_key: NotRequired[Sequence[Any]]
    """Super-shard key forwarded as native ``supershardingkey``."""
    transaction_priority: NotRequired[OracleTransactionPriority]
    """Priority applied during connection checkout on supported drivers."""
    shardingkey: NotRequired[list[Any]]
    """Primary sharding key values routing the connection to an Oracle shard."""
    supershardingkey: NotRequired[list[Any]]
    """Composite super-sharding key values for multi-tier sharded topologies."""
    debug_jdwp: NotRequired[str]
    """JDWP ``host:port`` endpoint attached for PL/SQL and Java stored-procedure debugging."""
    connection_id_prefix: NotRequired[str]
    """Prefix prepended to driver-generated end-to-end diagnostic connection identifiers."""
    ssl_context: NotRequired[Any]
    """Custom ``ssl.SSLContext`` overriding default Thin-mode TLS verification."""
    sdu: NotRequired[int]
    """Oracle Net Session Data Unit buffer size in bytes."""
    pool_boundary: NotRequired[OraclePoolBoundary]
    """Implicit DRCP/PRCP release boundary (``'statement'`` or ``'transaction'``)."""
    use_tcp_fast_open: NotRequired[bool]
    """Enable TCP Fast Open (TFO) for Oracle Autonomous Database connections."""
    ssl_version: NotRequired[TLSVersion]
    """Minimum or pinned ``ssl.TLSVersion`` negotiated for TCPS connections."""
    program: NotRequired[str]
    """Client program name reported in ``V$SESSION.PROGRAM``."""
    machine: NotRequired[str]
    """Client host name reported in ``V$SESSION.MACHINE``."""
    terminal: NotRequired[str]
    """Client terminal identifier reported in ``V$SESSION.TERMINAL``."""
    osuser: NotRequired[str]
    """Operating system username reported in ``V$SESSION.OSUSER``."""
    driver_name: NotRequired[str]
    """Client driver label reported in ``V$SESSION_CONNECT_INFO.CLIENT_DRIVER``."""
    use_sni: NotRequired[bool]
    """Send TLS Server Name Indication (SNI) to bypass double-handshake listener negotiation."""
    thick_mode_dsn_passthrough: NotRequired[bool]
    """Forward the raw DSN string unchanged to Oracle Client libraries in Thick mode."""
    extra_auth_params: NotRequired[dict[str, Any]]
    """Additional provider parameters passed to cloud token authentication plugins."""
    pool_name: NotRequired[str]
    """Explicit server-side DRCP/PRCP pool name when multiple pools exist."""
    on_connect_callback: NotRequired[Callable[..., Any]]
    """Driver-level callback invoked by ``python-oracledb`` immediately on physical connect."""
    handle: NotRequired[int]
    """Raw OCI service context pointer wrapped in Thick mode."""
    extra: NotRequired[dict[str, Any]]
    """Additional keyword arguments merged into ``oracledb.connect`` or ``create_pool``."""
    topology: NotRequired[OracleTopologyConfig | dict[str, Any]]
    """Multi-endpoint RAC/Data Guard topology descriptor compiled into a TNS ``DESCRIPTION``."""
    wallet_zip: NotRequired[str | Path | bytes]
    """Autonomous Database wallet ZIP path, directory, or raw bytes extracted into a secure temporary directory."""
    tns_alias: NotRequired[str]
    """Alias resolved from ``tnsnames.ora`` inside ``config_dir`` or an extracted ``wallet_zip``."""


class OraclePoolParams(OracleConnectionParams):
    """OracleDB session pool parameters extending ``OracleConnectionParams``."""

    thick_mode: NotRequired[bool]
    """Initialize Oracle Client Thick mode via ``oracledb.init_oracle_client()`` before creating the pool."""
    lib_dir: NotRequired[str]
    """Filesystem path to Oracle Instant Client libraries implying Thick mode."""
    pool_class: NotRequired[type[Any]]
    """Custom ``ConnectionPool`` subclass passed to ``python-oracledb``."""
    params: NotRequired[oracledb.PoolParams]
    """Pre-constructed ``oracledb.PoolParams`` instance merged with keyword settings."""
    min: NotRequired[int]
    """Minimum number of sessions maintained in the pool."""
    max: NotRequired[int]
    """Maximum number of concurrent sessions permitted in the pool."""
    increment: NotRequired[int]
    """Number of new sessions opened at once when all pooled sessions are busy."""
    connectiontype: NotRequired[type[Any]]
    """Custom ``oracledb.Connection`` subclass instantiated for pooled sessions."""
    getmode: NotRequired[PoolGetMode]
    """Behavior when the pool is at capacity (``POOL_GETMODE_WAIT``, ``NOWAIT``, ``FORCEGET``, or ``TIMEDWAIT``)."""
    homogeneous: NotRequired[bool]
    """Use identical database credentials across all pooled sessions (defaults to ``True``)."""
    timeout: NotRequired[int]
    """Idle seconds after which surplus sessions above ``min`` are closed."""
    wait_timeout: NotRequired[int]
    """Maximum milliseconds to wait for an available session under ``POOL_GETMODE_TIMEDWAIT``."""
    max_lifetime_session: NotRequired[int]
    """Maximum lifetime in seconds before a pooled session is retired."""
    session_callback: NotRequired[Callable[..., Any] | str]
    """Callback ``(connection, requested_tag)`` invoked when a pooled session is acquired."""
    max_sessions_per_shard: NotRequired[int]
    """Maximum pooled sessions allocated to any single Oracle shard."""
    soda_metadata_cache: NotRequired[bool]
    """Enable Thick-mode SODA collection metadata caching across pooled sessions."""
    ping_interval: NotRequired[int]
    """Idle seconds elapsed before a pooled session is health-checked on checkout."""
    ping_timeout: NotRequired[int]
    """Maximum milliseconds allowed for a checkout health-check ping before replacing the session."""


class OracleDriverFeatures(TypedDict):
    """Oracle driver feature flags controlling VECTOR/UUID codecs, LOB coercion, and enterprise hooks.

    Native async pipeline execution is runtime-gated on Oracle Database 23ai/26ai+ in Thin mode and opted out via ``stack_native_disabled``.
    """

    retry_on_standby_lag: NotRequired[bool]
    """Allow primary fallback on standby checkout lag or switchover."""
    plugins: NotRequired[Sequence[OraclePluginName | str]]
    """Optional cloud configuration or token plugins imported lazily."""
    params_hooks: NotRequired[Sequence[Callable[[Any], None]]]
    """Native parameter hooks registered idempotently before connecting."""
    enable_auto_state_tagging: NotRequired[bool]
    """Synthesize and reuse Thick pool session state tags (defaults to True)."""
    enable_numpy_vectors: NotRequired[bool]
    """Register bidirectional NumPy ``ndarray`` handlers for Oracle 23ai+ ``VECTOR`` columns (defaults to ``True`` when NumPy is installed)."""
    enable_lowercase_column_names: NotRequired[bool]
    """Normalize unquoted uppercase Oracle column names to lowercase while preserving quoted aliases (defaults to ``True``)."""
    enable_uuid_binary: NotRequired[bool]
    """Convert Python ``UUID`` values to and from ``RAW(16)`` columns."""
    vector_return_format: NotRequired[OracleVectorReturnFormat]
    """Decoded type for dense ``VECTOR`` columns (``'numpy'``, ``'list'``, or ``'array'``)."""
    oracle_varchar2_byte_limit: NotRequired[int]
    """UTF-8 byte threshold above which ``str`` parameters bind as ``DB_TYPE_CLOB`` (defaults to ``4000``)."""
    oracle_raw_byte_limit: NotRequired[int]
    """Byte threshold above which ``bytes`` parameters bind as ``DB_TYPE_BLOB`` (defaults to ``2000``)."""
    arraysize: NotRequired[int]
    """Per-cursor ``arraysize`` row fetch buffer override."""
    prefetchrows: NotRequired[int]
    """Per-cursor ``prefetchrows`` initial round-trip prefetch count override."""
    fetch_lobs: NotRequired[bool]
    """Return LOB columns as locator objects when ``True`` or inline ``str``/``bytes`` when ``False``."""
    fetch_decimals: NotRequired[bool]
    """Return ``NUMBER`` columns as ``decimal.Decimal`` values when ``True``."""
    on_connection_create: NotRequired[Callable[..., Any]]
    """Hook ``(connection, tag)`` invoked after built-in JSON, VECTOR, and UUID handlers are registered."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec event channels (separate from ``connection_config['events']`` FAN/CQN notifications)."""
    events_backend: NotRequired[OracleEventsBackend]
    """Event queue backend (``'poll_queue'`` default, ``'aq'``, or ``'txeventq'``)."""
    enable_direct_path_load: NotRequired[bool]
    """Route ``load_from_arrow`` through Thin-mode ``Connection.direct_path_load`` with ``executemany`` fallback (defaults to ``True``)."""
    tenant_strategy: NotRequired[TenantStrategy | OracleTenantStrategyShorthand | None]
    """Opt-in tenant strategy, or the ``"identifier"`` shorthand, applied to every session and connection lease."""
    session_context_hook: NotRequired[OracleSessionContextConfig | dict[str, Any]]
    """Declarative ``DBMS_SESSION`` / ``CLIENT_IDENTIFIER`` context applied on checkout and cleared on release."""
    sharding_key_variable: NotRequired[str]
    """Tenant variable carrying the primary shard key."""
    super_sharding_key_variable: NotRequired[str]
    """Tenant variable carrying the super-shard key."""
    shard_resolver: NotRequired[OracleShardResolver]
    """Resolve primary and super-shard keys from the captured tenant payload."""
    use_tenant_id_as_sharding_key: NotRequired[bool]
    """Route directly by captured tenant identity in Thick mode."""
    enable_otel_trace_context: NotRequired[bool]
    """Piggyback W3C trace headers on supported Oracle connections."""
    enable_session_tracing: NotRequired[bool]
    """Piggyback active correlation and trace identifiers onto ``module``, ``action``, ``client_identifier``, and ``econtext_id`` in zero extra RTTs."""
    dbms_cloud_credential: NotRequired[str | None]
    """Default ``DBMS_CLOUD`` credential name used for server-side ``DBMS_CLOUD.COPY_DATA`` object-store loads."""


class OracleSyncConnectionContext(SyncPoolConnectionContext):
    """Own one sync checkout and physically discard unverified sessions."""

    __slots__ = ("_acquire_options", "_conn", "_desired_tag", "_priority_baseline")

    def __init__(
        self, config: "OracleSyncConfig", tenant_lease: Any = None, acquire_options: OracleAcquireOptions | None = None
    ) -> None:
        super().__init__(config, tenant_lease)
        self._conn: Any = None
        self._desired_tag: str | None = None
        self._priority_baseline: Any = None
        self._acquire_options = acquire_options or OracleAcquireOptions()
        if self._tenant_lease is not None:
            self._tenant_lease._acquire_options = self._acquire_options

    def acquire_connection(self) -> OracleSyncConnection:
        if self._tenant_lease is not None:
            try:
                return cast("OracleSyncConnection", self._tenant_lease.acquire())
            except oracledb.Error as exc:
                raise create_mapped_exception(exc) from exc
        pool = self._config.provide_pool()
        hook = self._config.driver_features.get("session_context_hook")
        try:
            conn = pool.acquire(
                **self._config._build_pool_acquire_kwargs(self._acquire_options, pool=pool, session_hook=hook)
            )
        except oracledb.Error as exc:
            raise create_mapped_exception(exc) from exc
        self._conn = conn
        try:
            self._desired_tag, skip, self._priority_baseline = _oracle_checkout_state(
                self._config, pool, conn, self._acquire_options
            )
            if hook is not None:
                execute_session_checkout_sync(conn, hook, skip_alter_session=skip)
        except BaseException as exc:
            self._conn = None
            try:
                forget_oracle_session_state(conn)
                pool.drop(conn)
            except BaseException as drop_error:
                exc.add_note(f"Oracle connection disposal failed: {drop_error}")
            raise
        return cast("OracleSyncConnection", conn)

    def release_connection(self, _conn: Any = None, **kwargs: Any) -> None:
        if self._tenant_lease is not None:
            self._tenant_lease.release(_conn, **kwargs)
            return
        conn, self._conn = self._conn, None
        if conn is None:
            return
        pool = self._config.connection_instance
        body_error = kwargs.get("exc_val")
        try:
            if kwargs.get("exc_type") is not None or getattr(conn, "autocommit", None) is False:
                conn.rollback()
            clear_oracle_otel_trace_context(conn)
            hook = self._config.driver_features.get("session_context_hook")
            if hook is not None:
                execute_session_release_sync(conn, hook)
            if self._priority_baseline is not None and not oracle_transaction_priority_matches(
                conn, self._priority_baseline
            ):
                conn.rollback()
                apply_oracle_transaction_priority(conn, self._priority_baseline)
        except BaseException as exc:
            if kwargs.get("exc_type") is None:
                try:
                    conn.rollback()
                except BaseException as rollback_error:
                    exc.add_note(f"Oracle rollback failed: {rollback_error}")
            try:
                forget_oracle_session_state(conn)
                pool.drop(conn)
            except BaseException as drop_error:
                exc.add_note(f"Oracle connection disposal failed: {drop_error}")
            _oracle_cleanup_error(body_error, exc)
            return
        release_kwargs = (
            {"tag": self._desired_tag} if self._desired_tag is not None and getattr(conn, "thin", None) is False else {}
        )
        try:
            pool.release(conn, **release_kwargs)
        except BaseException as exc:
            try:
                forget_oracle_session_state(conn)
                pool.drop(conn)
            except BaseException as drop_error:
                exc.add_note(f"Oracle connection disposal failed: {drop_error}")
            _oracle_cleanup_error(body_error, exc)

    def __enter__(self) -> OracleSyncConnection:
        return self.acquire_connection()

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: "TracebackType | None"
    ) -> None:
        self.release_connection(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)


class _OracleSyncSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ("_driver",)

    def __init__(self, config: "OracleSyncConfig", acquire_options: OracleAcquireOptions | None = None) -> None:
        super().__init__(config)
        self._ctx = OracleSyncConnectionContext(config, self._tenant_lease, acquire_options)
        self._driver: OracleSyncDriver | None = None

    def prepare_driver(self, driver: OracleSyncDriver) -> None:
        """Capture session ownership for forgotten sessionless transaction cleanup."""
        self._driver = driver

    def acquire_connection(self) -> OracleSyncConnection:
        return cast("OracleSyncConnection", self._ctx.acquire_connection())

    def release_connection(self, _conn: OracleSyncConnection, **kwargs: Any) -> None:
        driver, self._driver = self._driver, None
        cleanup_error = driver._sessionless_cleanup_error if driver is not None else None
        if driver is not None:
            driver._sessionless_cleanup_error = None
        if cleanup_error is None and driver is not None and driver.sessionless_transaction_id is not None:
            try:
                driver.rollback()
            except BaseException as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            self._ctx._conn = None
            pool = self._config.connection_instance
            if self._tenant_lease is not None:
                if self._tenant_lease._pool is not None:
                    pool = self._tenant_lease._pool
                self._tenant_lease._connection = None
                self._tenant_lease._state = None
                self._tenant_lease._pool = None
                self._tenant_lease._bypass_hook = None
            try:
                forget_oracle_session_state(_conn)
                if pool is not None:
                    pool.drop(_conn)
            except BaseException as drop_error:
                cleanup_error.add_note(f"Oracle connection disposal failed: {drop_error}")
            _oracle_cleanup_error(kwargs.get("exc_val"), cleanup_error)
            return
        self._ctx.release_connection(_conn, **kwargs)


class OracleSyncConfig(SyncDatabaseConfig[OracleSyncConnection, "OracleSyncConnectionPool", OracleSyncDriver]):
    """Configuration for Oracle synchronous database connections."""

    __slots__ = (
        "_extracted_wallet_dir",
        "_oracle_version_cache",
        "_owns_pool_session_callback",
        "_pool_session_callback",
        "_raw_tenant_strategy",
        "_source_topology",
        "_tenant_source_hook",
        "_thin_sharding_warned",
        "_user_connection_hook",
    )

    driver_type: ClassVar[type[OracleSyncDriver]] = OracleSyncDriver
    connection_type: "ClassVar[type[OracleSyncConnection]]" = OracleSyncConnection
    migration_tracker_type: "ClassVar[type[OracleSyncMigrationTracker]]" = OracleSyncMigrationTracker
    supports_tenant_strategy: ClassVar[bool] = True
    tenant_strategy_shorthands: "ClassVar[frozenset[str]]" = frozenset({"client_context", "default", "identifier"})
    tenant_strategy_type: "ClassVar[type[Any] | None]" = OracleSyncIdentifierStrategy
    tenant_lease_type: "ClassVar[type[Any] | None]" = OracleSyncTenantLease
    supports_transactional_ddl: ClassVar[bool] = False
    supports_migration_schemas: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_arrow_streaming: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="native"
    )
    _connection_context_class: "ClassVar[type[OracleSyncConnectionContext]]" = OracleSyncConnectionContext
    _session_factory_class: "ClassVar[type[_OracleSyncSessionConnectionHandler]]" = _OracleSyncSessionConnectionHandler
    _session_context_class: "ClassVar[type[OracleSyncSessionContext]]" = OracleSyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "OraclePoolParams | dict[str, Any] | None" = None,
        connection_instance: "OracleSyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "OracleDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Oracle synchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters.
            connection_instance: Existing pool instance to use.
            migration_config: Migration configuration.
            statement_config: Default SQL statement configuration.
            driver_features: Optional driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration.
            **kwargs: Additional keyword arguments.
        """
        source_topology = connection_config.get("topology") if connection_config is not None else None
        self._source_topology = (
            OracleTopologyConfig(**source_topology) if isinstance(source_topology, Mapping) else source_topology
        )
        connection_config, pending_wallet_dir = _process_oracle_wallet_and_config(connection_config)
        try:
            self._extracted_wallet_dir: Path | None = pending_wallet_dir
            self._oracle_version_cache = OracleVersionCache()
            self._owns_pool_session_callback = False
            self._thin_sharding_warned = False
            self._pool_session_callback = validate_oracle_session_callback(
                connection_config.pop("session_callback", None), is_thin_mode=False
            )
            statement_config = statement_config or default_statement_config
            statement_config, driver_features = apply_driver_features(statement_config, driver_features)
            features_dict = dict(driver_features) if driver_features else {}
            self._raw_tenant_strategy: Any = features_dict.get("tenant_strategy")
            self._tenant_source_hook: OracleSessionContextConfig | None = features_dict.get("session_context_hook")
            self._user_connection_hook: Callable[[OracleSyncConnection, str], None] | None = features_dict.pop(
                "on_connection_create", None
            )

            ensure_oracle_config_plugins(
                dsn=connection_config.get("dsn"),
                plugins=features_dict.get("plugins"),
                params_hooks=features_dict.get("params_hooks"),
            )
            super().__init__(
                connection_config=connection_config,
                connection_instance=connection_instance,
                migration_config=migration_config,
                statement_config=statement_config,
                driver_features=features_dict,
                bind_key=bind_key,
                extension_config=extension_config,
                **kwargs,
            )
            bound_strategy = bind_oracle_tenant_hook(
                self.driver_features.get("tenant_strategy"), self.driver_features.get("session_context_hook")
            )
            if bound_strategy is not None:
                self.driver_features["tenant_strategy"] = bound_strategy
            self.connection_config = validate_oracle_drcp_and_mode_constraints(
                self.connection_config, self.driver_features
            )
            validate_oracle_tenant_shard_features(self.driver_features)
            if isinstance(
                self.driver_features.get("tenant_strategy"),
                (OracleSyncEndUserSecurityStrategy, OracleAsyncEndUserSecurityStrategy),
            ):
                strategy = self.driver_features["tenant_strategy"]
                validate_oracle_drcp_and_mode_constraints(
                    self.connection_config,
                    {
                        **self.driver_features,
                        "session_context_hook": OracleSessionContextConfig(
                            end_user_security=OracleEndUserSecurityConfig(strategy._database_access_token)
                        ),
                    },
                )
        except BaseException:
            if pending_wallet_dir is not None:
                release_extracted_wallet_dir(pending_wallet_dir)
            raise

    def refresh_config_cache(self, *, dsn: str | None = None, remove_permanently: bool = False) -> None:
        """Expire centralized provider configuration for this pool's DSN."""
        resolved = dsn if dsn is not None else self.connection_config.get("dsn")
        if not isinstance(resolved, str) or not resolved:
            msg = "refresh_config_cache requires a configured DSN or an explicit dsn"
            raise ImproperConfigurationError(msg)
        refresh_oracle_config_cache(resolved, remove_permanently=remove_permanently)

    def create_connection(self) -> "OracleSyncConnection":
        """Create a single connection (not from pool).

        Returns:
            An Oracle Connection instance.
        """
        self._require_leased_tenant_connection()
        _initialize_oracle_client(self.connection_config)
        validate_oracle_session_callback(self._pool_session_callback, is_thin_mode=bool(oracledb.is_thin_mode()))
        config = _build_standalone_connection_params(self.connection_config)
        conn = oracledb.connect(**config)
        try:
            self._init_connection(conn, "")
        except BaseException:
            with contextlib.suppress(BaseException):
                conn.close()
            raise
        return conn

    def provide_pool(self) -> "OracleSyncConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = self.create_pool()
        return self.connection_instance

    def derive_pool(
        self,
        *,
        connection_config_overrides: "Mapping[str, Any] | None" = None,
        driver_features_overrides: "Mapping[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        bind_key: "str | None" = None,
        **overrides: Any,
    ) -> Self:
        """Clone this OracleSyncConfig with overridden connection/pool or feature settings and ``connection_instance=None``."""
        derived_kwargs = _derive_oracle_pool_kwargs(
            self,
            connection_config_overrides,
            driver_features_overrides,
            statement_config,
            migration_config,
            extension_config,
            observability_config,
            bind_key,
            overrides,
        )
        derived = type(self)(**derived_kwargs)
        if (
            self._extracted_wallet_dir is not None
            and derived._extracted_wallet_dir is None
            and str(self._extracted_wallet_dir)
            in {derived.connection_config.get("config_dir"), derived.connection_config.get("wallet_location")}
        ):
            derived._extracted_wallet_dir = self._extracted_wallet_dir
            retain_extracted_wallet_dir(self._extracted_wallet_dir)
        if bind_key is None or bind_key == self.bind_key:
            derived._tenant_key = self._tenant_key
        return derived

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for OracleDB types.

        Provides OracleDB-specific types for Litestar framework recognition.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({
            "OracleSyncSessionlessTransactionContext": OracleSyncSessionlessTransactionContext,
            "OracleAsyncSessionlessTransactionContext": OracleAsyncSessionlessTransactionContext,
            "OracleAsyncConnection": OracleAsyncConnection,
            "OracleAsyncConnectionPool": OracleAsyncConnectionPool,
            "OracleAsyncCursor": OracleAsyncCursor,
            "OracleAsyncDriver": OracleAsyncDriver,
            "OracleAsyncExceptionHandler": OracleAsyncExceptionHandler,
            "OracleConnectionParams": OracleConnectionParams,
            "OracleDriverFeatures": OracleDriverFeatures,
            "OracleEndpoint": OracleEndpoint,
            "OracleEndUserSecurityConfig": OracleEndUserSecurityConfig,
            "OracleAsyncClientContextStrategy": OracleAsyncClientContextStrategy,
            "OracleAsyncEndUserSecurityStrategy": OracleAsyncEndUserSecurityStrategy,
            "OracleSyncClientContextStrategy": OracleSyncClientContextStrategy,
            "OracleSyncEndUserSecurityStrategy": OracleSyncEndUserSecurityStrategy,
            "OracleShardResolver": OracleShardResolver,
            "OracleAcquireOptions": OracleAcquireOptions,
            "OraclePluginName": OraclePluginName,
            "OraclePoolBoundary": OraclePoolBoundary,
            "OraclePoolParams": OraclePoolParams,
            "OraclePurityInput": OraclePurityInput,
            "OracleTransactionPriority": OracleTransactionPriority,
            "OracleSessionContextConfig": OracleSessionContextConfig,
            "OracleSyncConnectionContext": OracleSyncConnectionContext,
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
            "OracleSyncSessionContext": OracleSyncSessionContext,
            "OracleTopologyConfig": OracleTopologyConfig,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for Oracle table-backed event queues."""

        return EventRuntimeHints(select_for_update=True, skip_locked=True)

    def _warn_if_thin_sharding(self, pool: Any, options: OracleAcquireOptions) -> None:
        if (
            not self._thin_sharding_warned
            and getattr(pool, "thin", None) is not False
            and (
                options.sharding_key is not None
                or self.connection_config.get("shardingkey")
                or self.connection_config.get("max_sessions_per_shard")
            )
        ):
            logger.warning(
                "Oracle Thin mode ignores sharding keys and max_sessions_per_shard; use Thick mode for shard routing"
            )
            self._thin_sharding_warned = True

    def _build_pool_acquire_kwargs(
        self, options: OracleAcquireOptions | None = None, *, pool: Any = None, session_hook: Any = None
    ) -> dict[str, Any]:
        options = options or OracleAcquireOptions()
        self._warn_if_thin_sharding(pool, options)
        result: dict[str, Any] = {}
        for attr, key in (
            ("sharding_key", "shardingkey"),
            ("super_sharding_key", "supershardingkey"),
            ("cclass", "cclass"),
            ("purity", "purity"),
        ):
            value = getattr(options, attr)
            if value is not None and (
                attr not in {"sharding_key", "super_sharding_key"} or getattr(pool, "thin", None) is False
            ):
                result[key] = value
        if getattr(pool, "thin", None) is False:
            if options.tag is not None:
                result["tag"] = options.tag
                result["matchanytag"] = options.match_any_tag
            elif self.driver_features.get("enable_auto_state_tagging", True):
                desired = synthesize_session_state_tag(session_hook)
                if desired is not None:
                    result.update(tag=desired, matchanytag=True)
            elif options.match_any_tag:
                result["matchanytag"] = True
        return result

    def _provide_connection_impl(
        self,
        *args: Any,
        acquire_options: OracleAcquireOptions | None = None,
        sharding_key: Sequence[Any] | None = None,
        super_sharding_key: Sequence[Any] | None = None,
        tag: str | None = None,
        match_any_tag: bool = False,
        cclass: str | None = None,
        purity: OraclePurityInput | None = None,
        transaction_priority: Any = None,
        **kwargs: Any,
    ) -> Any:
        options = acquire_options or OracleAcquireOptions(
            sharding_key=sharding_key,
            super_sharding_key=super_sharding_key,
            tag=tag,
            match_any_tag=match_any_tag,
            cclass=cclass,
            purity=purity,
            transaction_priority=transaction_priority,
        )
        return OracleSyncConnectionContext(self, acquire_options=options)

    def _provide_session_impl(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        acquire_options: OracleAcquireOptions | None = None,
        sharding_key: Sequence[Any] | None = None,
        super_sharding_key: Sequence[Any] | None = None,
        tag: str | None = None,
        match_any_tag: bool = False,
        cclass: str | None = None,
        purity: OraclePurityInput | None = None,
        transaction_priority: Any = None,
        **kwargs: Any,
    ) -> Any:
        options = acquire_options or OracleAcquireOptions(
            sharding_key=sharding_key,
            super_sharding_key=super_sharding_key,
            tag=tag,
            match_any_tag=match_any_tag,
            cclass=cclass,
            purity=purity,
            transaction_priority=transaction_priority,
        )
        handler = _OracleSyncSessionConnectionHandler(self, acquire_options=options)
        return self._session_context_class(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def _resolve_tenant_strategy(self, strategy: Any) -> Any:
        if strategy == "client_context":
            return OracleSyncClientContextStrategy(hook=self._tenant_source_hook)
        return super()._resolve_tenant_strategy(strategy)

    def _default_tenant_strategy(self) -> TenantStrategy:
        return OracleSyncIdentifierStrategy(self._tenant_source_hook)

    def _allow_replica_fallback(self, _error: Exception) -> bool:
        if is_oracle_standby_failover_error(_error):
            return bool(self.driver_features.get("retry_on_standby_lag", False))
        return super()._allow_replica_fallback(_error)

    def _create_pool(self) -> "OracleSyncConnectionPool":
        """Create the actual connection pool."""
        _initialize_oracle_client(self.connection_config)
        config = dict(self.connection_config)
        config.pop("thick_mode", None)
        config.pop("lib_dir", None)

        apply_wallet_ssl_context(config)

        config.pop("threaded", None)
        validate_oracle_session_callback(self._pool_session_callback, is_thin_mode=bool(oracledb.is_thin_mode()))
        config.pop("transaction_priority", None)
        config["session_callback"] = self._init_connection
        self._owns_pool_session_callback = True

        try:
            return oracledb.create_pool(**config)
        except oracledb.Error as exc:
            raise create_mapped_exception(exc) from exc

    def _init_connection(self, connection: "OracleSyncConnection", tag: str) -> None:
        """Initialize connection with type handlers and cached server metadata.

        Args:
            connection: Oracle connection to initialize.
            tag: Connection tag for session state.
        """
        server_major = resolve_oracle_connection_major(connection, self._oracle_version_cache)
        compatible_major = resolve_oracle_connection_compatible_major(connection, self._oracle_version_cache)
        enable_vectors = bool(self.driver_features.get("enable_numpy_vectors", True))

        conn_any = cast("Any", connection)
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_oracle_major = server_major
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_oracle_compatible_major = compatible_major
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_vector_return_format = self.driver_features.get("vector_return_format")
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_enable_numpy_vectors = enable_vectors

        if enable_vectors and (server_major is None or server_major >= _ORACLE_VECTOR_MIN_MAJOR):
            register_numpy_handlers(connection)

        register_json_handlers(connection)

        if self.driver_features.get("enable_uuid_binary", False):
            register_uuid_handlers(connection)

        if (
            self._owns_pool_session_callback
            and getattr(connection, "thin", None) is not False
            and not self.driver_features.get("tenant_strategy")
        ):
            static_hook = extract_static_alter_session_config(self.driver_features.get("session_context_hook"))
            if static_hook is not None:
                execute_session_checkout_sync(connection, static_hook)
        if isinstance(self._pool_session_callback, str):
            with connection.cursor() as cursor:
                cursor.execute(
                    f"BEGIN {self._pool_session_callback}(:requested_tag, :actual_tag); END;",
                    {"requested_tag": tag, "actual_tag": connection.tag},
                )
        elif self._pool_session_callback is not None:
            self._pool_session_callback(connection, tag)

        if self._user_connection_hook is not None:
            self._user_connection_hook(connection, tag)

    def _prepare_driver(self, driver: "OracleSyncDriver") -> "OracleSyncDriver":
        """Attach the pool-scoped version cache alongside the observability runtime."""
        driver = super()._prepare_driver(driver)
        driver._oracle_version_cache = self._oracle_version_cache
        return driver

    def _close_pool(self) -> None:
        """Close the actual connection pool and release any extracted temporary wallet directory."""
        try:
            if self.connection_instance:
                self.connection_instance.close()
                self.connection_instance = None
            self._oracle_version_cache.reset()
            self._owns_pool_session_callback = False
            self._thin_sharding_warned = False
        finally:
            if self._extracted_wallet_dir is not None:
                wallet_dir = self._extracted_wallet_dir
                self._extracted_wallet_dir = None
                release_extracted_wallet_dir(wallet_dir)


class OracleAsyncConnectionContext(AsyncPoolConnectionContext):
    """Own one async checkout and physically discard unverified sessions."""

    __slots__ = ("_acquire_options", "_desired_tag", "_priority_baseline")

    def __init__(
        self, config: "OracleAsyncConfig", acquire_options: OracleAcquireOptions | None = None, tenant_lease: Any = None
    ) -> None:
        super().__init__(config)
        if tenant_lease is not None:
            self._tenant_lease = tenant_lease
        self._acquire_options = acquire_options or OracleAcquireOptions()
        if self._tenant_lease is not None:
            self._tenant_lease._acquire_options = self._acquire_options
        self._desired_tag: str | None = None
        self._priority_baseline: Any = None

    async def __aenter__(self) -> Any:
        if self._tenant_lease is not None:
            try:
                return await self._tenant_lease.acquire()
            except oracledb.Error as exc:
                raise create_mapped_exception(exc) from exc
        pool = await self._config.provide_pool()
        self._pool = pool
        hook = self._config.driver_features.get("session_context_hook")
        try:
            conn = await pool.acquire(
                **self._config._build_pool_acquire_kwargs(self._acquire_options, pool=pool, session_hook=hook)
            )
        except oracledb.Error as exc:
            raise create_mapped_exception(exc) from exc
        self._connection = conn
        try:
            self._desired_tag, skip, self._priority_baseline = _oracle_checkout_state(
                self._config, pool, conn, self._acquire_options
            )
            if hook is not None:
                await execute_session_checkout_async(conn, hook, skip_alter_session=skip)
        except BaseException as exc:
            self._connection = None
            self._pool = None
            try:
                forget_oracle_session_state(conn)
                await pool.drop(conn)
            except BaseException as drop_error:
                exc.add_note(f"Oracle connection disposal failed: {drop_error}")
            raise
        return conn

    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        if self._tenant_lease is not None:
            await self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            return
        conn, self._connection = self._connection, None
        pool, self._pool = self._pool, None
        if conn is None:
            return
        try:
            if exc_type is not None or getattr(conn, "autocommit", None) is False:
                await conn.rollback()
            clear_oracle_otel_trace_context(conn)
            hook = self._config.driver_features.get("session_context_hook")
            if hook is not None:
                await execute_session_release_async(conn, hook)
            if self._priority_baseline is not None and not oracle_transaction_priority_matches(
                conn, self._priority_baseline
            ):
                await conn.rollback()
                apply_oracle_transaction_priority(conn, self._priority_baseline)
        except BaseException as exc:
            if exc_type is None:
                try:
                    await conn.rollback()
                except BaseException as rollback_error:
                    exc.add_note(f"Oracle rollback failed: {rollback_error}")
            try:
                forget_oracle_session_state(conn)
                await pool.drop(conn)
            except BaseException as drop_error:
                exc.add_note(f"Oracle connection disposal failed: {drop_error}")
            _oracle_cleanup_error(exc_val, exc)
            return
        release_kwargs = (
            {"tag": self._desired_tag} if self._desired_tag is not None and getattr(conn, "thin", None) is False else {}
        )
        try:
            await pool.release(conn, **release_kwargs)
        except BaseException as exc:
            try:
                forget_oracle_session_state(conn)
                await pool.drop(conn)
            except BaseException as drop_error:
                exc.add_note(f"Oracle connection disposal failed: {drop_error}")
            _oracle_cleanup_error(exc_val, exc)


class _OracleAsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    __slots__ = ("_ctx", "_driver")

    def __init__(self, config: "OracleAsyncConfig", acquire_options: OracleAcquireOptions | None = None) -> None:
        super().__init__(config)
        self._ctx = OracleAsyncConnectionContext(config, acquire_options, self._tenant_lease)
        self._driver: OracleAsyncDriver | None = None

    def prepare_driver(self, driver: OracleAsyncDriver) -> None:
        """Capture session ownership for forgotten sessionless transaction cleanup."""
        self._driver = driver

    async def acquire_connection(self) -> Any:
        return await self._ctx.__aenter__()

    async def release_connection(self, _conn: Any, **kwargs: Any) -> None:
        driver, self._driver = self._driver, None
        cleanup_error = driver._sessionless_cleanup_error if driver is not None else None
        if driver is not None:
            driver._sessionless_cleanup_error = None
        if cleanup_error is None and driver is not None and driver.sessionless_transaction_id is not None:
            try:
                await driver.rollback()
            except BaseException as exc:
                cleanup_error = exc
        if cleanup_error is not None:
            pool = self._ctx._pool or self._config.connection_instance
            self._ctx._connection = None
            self._ctx._pool = None
            if self._tenant_lease is not None:
                if self._tenant_lease._pool is not None:
                    pool = self._tenant_lease._pool
                self._tenant_lease._connection = None
                self._tenant_lease._state = None
                self._tenant_lease._pool = None
                self._tenant_lease._bypass_hook = None
            try:
                forget_oracle_session_state(_conn)
                if pool is not None:
                    await pool.drop(_conn)
            except BaseException as drop_error:
                cleanup_error.add_note(f"Oracle connection disposal failed: {drop_error}")
            _oracle_cleanup_error(kwargs.get("exc_val"), cleanup_error)
            return
        await self._ctx.__aexit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))


class OracleAsyncConfig(AsyncDatabaseConfig[OracleAsyncConnection, "OracleAsyncConnectionPool", OracleAsyncDriver]):
    """Configuration for Oracle asynchronous database connections."""

    __slots__ = (
        "_extracted_wallet_dir",
        "_oracle_version_cache",
        "_owns_pool_session_callback",
        "_pool_session_callback",
        "_raw_tenant_strategy",
        "_source_topology",
        "_tenant_source_hook",
        "_thin_sharding_warned",
        "_user_connection_hook",
    )

    connection_type: "ClassVar[type[OracleAsyncConnection]]" = OracleAsyncConnection
    driver_type: ClassVar[type[OracleAsyncDriver]] = OracleAsyncDriver
    migration_tracker_type: "ClassVar[type[OracleAsyncMigrationTracker]]" = OracleAsyncMigrationTracker
    supports_tenant_strategy: ClassVar[bool] = True
    tenant_strategy_shorthands: "ClassVar[frozenset[str]]" = frozenset({"client_context", "default", "identifier"})
    tenant_strategy_type: "ClassVar[type[Any] | None]" = OracleAsyncIdentifierStrategy
    tenant_lease_type: "ClassVar[type[Any] | None]" = OracleAsyncTenantLease
    supports_transactional_ddl: ClassVar[bool] = False
    supports_migration_schemas: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_arrow_streaming: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="native"
    )
    _connection_context_class: "ClassVar[type[OracleAsyncConnectionContext]]" = OracleAsyncConnectionContext
    _session_factory_class: "ClassVar[type[_OracleAsyncSessionConnectionHandler]]" = (
        _OracleAsyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[OracleAsyncSessionContext]]" = OracleAsyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "OraclePoolParams | dict[str, Any] | None" = None,
        connection_instance: "OracleAsyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "OracleDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Oracle asynchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters.
            connection_instance: Existing pool instance to use.
            migration_config: Migration configuration.
            statement_config: Default SQL statement configuration.
            driver_features: Optional driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration.
            **kwargs: Additional keyword arguments.
        """
        source_topology = connection_config.get("topology") if connection_config is not None else None
        self._source_topology = (
            OracleTopologyConfig(**source_topology) if isinstance(source_topology, Mapping) else source_topology
        )
        connection_config, pending_wallet_dir = _process_oracle_wallet_and_config(connection_config)
        try:
            self._extracted_wallet_dir: Path | None = pending_wallet_dir
            self._oracle_version_cache = OracleVersionCache()
            self._owns_pool_session_callback = False
            self._thin_sharding_warned = False
            self._pool_session_callback = validate_oracle_session_callback(
                connection_config.pop("session_callback", None), is_thin_mode=True
            )

            statement_config = statement_config or default_statement_config
            statement_config, driver_features = apply_driver_features(statement_config, driver_features)
            features_dict = dict(driver_features) if driver_features else {}
            self._raw_tenant_strategy: Any = features_dict.get("tenant_strategy")
            self._tenant_source_hook: OracleSessionContextConfig | None = features_dict.get("session_context_hook")
            self._user_connection_hook: Callable[[OracleAsyncConnection, str], Awaitable[None]] | None = (
                features_dict.pop("on_connection_create", None)
            )

            ensure_oracle_config_plugins(
                dsn=connection_config.get("dsn"),
                plugins=features_dict.get("plugins"),
                params_hooks=features_dict.get("params_hooks"),
            )
            super().__init__(
                connection_config=connection_config,
                connection_instance=connection_instance,
                migration_config=migration_config,
                statement_config=statement_config,
                driver_features=features_dict,
                bind_key=bind_key,
                extension_config=extension_config,
                **kwargs,
            )
            bound_strategy = bind_oracle_tenant_hook(
                self.driver_features.get("tenant_strategy"), self.driver_features.get("session_context_hook")
            )
            if bound_strategy is not None:
                self.driver_features["tenant_strategy"] = bound_strategy
            self.connection_config = validate_oracle_drcp_and_mode_constraints(
                self.connection_config, self.driver_features
            )
            validate_oracle_tenant_shard_features(self.driver_features)
            if isinstance(
                self.driver_features.get("tenant_strategy"),
                (OracleSyncEndUserSecurityStrategy, OracleAsyncEndUserSecurityStrategy),
            ):
                strategy = self.driver_features["tenant_strategy"]
                validate_oracle_drcp_and_mode_constraints(
                    self.connection_config,
                    {
                        **self.driver_features,
                        "session_context_hook": OracleSessionContextConfig(
                            end_user_security=OracleEndUserSecurityConfig(strategy._database_access_token)
                        ),
                    },
                )
        except BaseException:
            if pending_wallet_dir is not None:
                release_extracted_wallet_dir(pending_wallet_dir)
            raise

    def refresh_config_cache(self, *, dsn: str | None = None, remove_permanently: bool = False) -> None:
        """Expire centralized provider configuration for this pool's DSN."""
        resolved = dsn if dsn is not None else self.connection_config.get("dsn")
        if not isinstance(resolved, str) or not resolved:
            msg = "refresh_config_cache requires a configured DSN or an explicit dsn"
            raise ImproperConfigurationError(msg)
        refresh_oracle_config_cache(resolved, remove_permanently=remove_permanently)

    async def create_connection(self) -> OracleAsyncConnection:
        """Create a single async connection (not from pool).

        Returns:
            An Oracle AsyncConnection instance.
        """
        self._require_leased_tenant_connection()
        _validate_async_oracle_mode(self.connection_config)
        config = _build_standalone_connection_params(self.connection_config)
        conn = await oracledb.connect_async(**config)
        try:
            await self._init_connection(conn, "")
        except BaseException:
            with contextlib.suppress(BaseException):
                await conn.close()
            raise
        return cast("OracleAsyncConnection", conn)

    async def provide_pool(self) -> "OracleAsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def derive_pool(
        self,
        *,
        connection_config_overrides: "Mapping[str, Any] | None" = None,
        driver_features_overrides: "Mapping[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        bind_key: "str | None" = None,
        **overrides: Any,
    ) -> Self:
        """Clone this OracleAsyncConfig with overridden connection/pool or feature settings and ``connection_instance=None``."""
        derived_kwargs = _derive_oracle_pool_kwargs(
            self,
            connection_config_overrides,
            driver_features_overrides,
            statement_config,
            migration_config,
            extension_config,
            observability_config,
            bind_key,
            overrides,
        )
        derived = type(self)(**derived_kwargs)
        if (
            self._extracted_wallet_dir is not None
            and derived._extracted_wallet_dir is None
            and str(self._extracted_wallet_dir)
            in {derived.connection_config.get("config_dir"), derived.connection_config.get("wallet_location")}
        ):
            derived._extracted_wallet_dir = self._extracted_wallet_dir
            retain_extracted_wallet_dir(self._extracted_wallet_dir)
        if bind_key is None or bind_key == self.bind_key:
            derived._tenant_key = self._tenant_key
        return derived

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for OracleAsyncConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "OracleSyncSessionlessTransactionContext": OracleSyncSessionlessTransactionContext,
            "OracleAsyncSessionlessTransactionContext": OracleAsyncSessionlessTransactionContext,
            "OracleAsyncConnectionContext": OracleAsyncConnectionContext,
            "OracleAsyncConnection": OracleAsyncConnection,
            "OracleAsyncConnectionPool": OracleAsyncConnectionPool,
            "OracleAsyncCursor": OracleAsyncCursor,
            "OracleAsyncDriver": OracleAsyncDriver,
            "OracleAsyncExceptionHandler": OracleAsyncExceptionHandler,
            "OracleAsyncSessionContext": OracleAsyncSessionContext,
            "OracleConnectionParams": OracleConnectionParams,
            "OracleDriverFeatures": OracleDriverFeatures,
            "OracleEndpoint": OracleEndpoint,
            "OracleEndUserSecurityConfig": OracleEndUserSecurityConfig,
            "OracleAsyncClientContextStrategy": OracleAsyncClientContextStrategy,
            "OracleAsyncEndUserSecurityStrategy": OracleAsyncEndUserSecurityStrategy,
            "OracleSyncClientContextStrategy": OracleSyncClientContextStrategy,
            "OracleSyncEndUserSecurityStrategy": OracleSyncEndUserSecurityStrategy,
            "OracleShardResolver": OracleShardResolver,
            "OracleAcquireOptions": OracleAcquireOptions,
            "OraclePluginName": OraclePluginName,
            "OraclePoolBoundary": OraclePoolBoundary,
            "OraclePoolParams": OraclePoolParams,
            "OraclePurityInput": OraclePurityInput,
            "OracleTransactionPriority": OracleTransactionPriority,
            "OracleSessionContextConfig": OracleSessionContextConfig,
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
            "OracleTopologyConfig": OracleTopologyConfig,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return polling defaults for Oracle table-backed event queues."""

        return EventRuntimeHints(select_for_update=True, skip_locked=True)

    def _warn_if_thin_sharding(self, pool: Any, options: OracleAcquireOptions) -> None:
        if (
            not self._thin_sharding_warned
            and getattr(pool, "thin", None) is not False
            and (
                options.sharding_key is not None
                or self.connection_config.get("shardingkey")
                or self.connection_config.get("max_sessions_per_shard")
            )
        ):
            logger.warning(
                "Oracle Thin mode ignores sharding keys and max_sessions_per_shard; use Thick mode for shard routing"
            )
            self._thin_sharding_warned = True

    def _build_pool_acquire_kwargs(
        self, options: OracleAcquireOptions | None = None, *, pool: Any = None, session_hook: Any = None
    ) -> dict[str, Any]:
        options = options or OracleAcquireOptions()
        self._warn_if_thin_sharding(pool, options)
        result: dict[str, Any] = {}
        for attr, key in (
            ("sharding_key", "shardingkey"),
            ("super_sharding_key", "supershardingkey"),
            ("cclass", "cclass"),
            ("purity", "purity"),
        ):
            value = getattr(options, attr)
            if value is not None and (
                attr not in {"sharding_key", "super_sharding_key"} or getattr(pool, "thin", None) is False
            ):
                result[key] = value
        if getattr(pool, "thin", None) is False:
            if options.tag is not None:
                result["tag"] = options.tag
                result["matchanytag"] = options.match_any_tag
            elif self.driver_features.get("enable_auto_state_tagging", True):
                desired = synthesize_session_state_tag(session_hook)
                if desired is not None:
                    result.update(tag=desired, matchanytag=True)
            elif options.match_any_tag:
                result["matchanytag"] = True
        return result

    def _provide_connection_impl(
        self,
        *args: Any,
        acquire_options: OracleAcquireOptions | None = None,
        sharding_key: Sequence[Any] | None = None,
        super_sharding_key: Sequence[Any] | None = None,
        tag: str | None = None,
        match_any_tag: bool = False,
        cclass: str | None = None,
        purity: OraclePurityInput | None = None,
        transaction_priority: Any = None,
        **kwargs: Any,
    ) -> Any:
        options = acquire_options or OracleAcquireOptions(
            sharding_key=sharding_key,
            super_sharding_key=super_sharding_key,
            tag=tag,
            match_any_tag=match_any_tag,
            cclass=cclass,
            purity=purity,
            transaction_priority=transaction_priority,
        )
        return OracleAsyncConnectionContext(self, acquire_options=options)

    def _provide_session_impl(
        self,
        *args: Any,
        statement_config: "StatementConfig | None" = None,
        acquire_options: OracleAcquireOptions | None = None,
        sharding_key: Sequence[Any] | None = None,
        super_sharding_key: Sequence[Any] | None = None,
        tag: str | None = None,
        match_any_tag: bool = False,
        cclass: str | None = None,
        purity: OraclePurityInput | None = None,
        transaction_priority: Any = None,
        **kwargs: Any,
    ) -> Any:
        options = acquire_options or OracleAcquireOptions(
            sharding_key=sharding_key,
            super_sharding_key=super_sharding_key,
            tag=tag,
            match_any_tag=match_any_tag,
            cclass=cclass,
            purity=purity,
            transaction_priority=transaction_priority,
        )
        handler = _OracleAsyncSessionConnectionHandler(self, acquire_options=options)
        return self._session_context_class(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def _resolve_tenant_strategy(self, strategy: Any) -> Any:
        if strategy == "client_context":
            return OracleAsyncClientContextStrategy(hook=self._tenant_source_hook)
        return super()._resolve_tenant_strategy(strategy)

    def _default_tenant_strategy(self) -> TenantStrategy:
        return OracleAsyncIdentifierStrategy(self._tenant_source_hook)

    def _allow_replica_fallback(self, _error: Exception) -> bool:
        if is_oracle_standby_failover_error(_error):
            return bool(self.driver_features.get("retry_on_standby_lag", False))
        return super()._allow_replica_fallback(_error)

    async def _create_pool(self) -> "OracleAsyncConnectionPool":
        """Create the actual async connection pool."""
        _validate_async_oracle_mode(self.connection_config)
        config = dict(self.connection_config)
        config.pop("thick_mode", None)
        config.pop("lib_dir", None)

        apply_wallet_ssl_context(config)

        config.pop("threaded", None)
        config.pop("transaction_priority", None)
        config["session_callback"] = self._init_connection
        self._owns_pool_session_callback = True

        try:
            return oracledb.create_pool_async(**config)
        except oracledb.Error as exc:
            raise create_mapped_exception(exc) from exc

    async def _init_connection(self, connection: "OracleAsyncConnection", tag: str) -> None:
        """Initialize async connection with type handlers and cached server metadata.

        Args:
            connection: Oracle async connection to initialize.
            tag: Connection tag for session state.
        """
        server_major = resolve_oracle_connection_major(connection, self._oracle_version_cache)
        compatible_major = resolve_oracle_connection_compatible_major(connection, self._oracle_version_cache)
        enable_vectors = bool(self.driver_features.get("enable_numpy_vectors", True))

        conn_any = cast("Any", connection)
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_oracle_major = server_major
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_oracle_compatible_major = compatible_major
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_vector_return_format = self.driver_features.get("vector_return_format")
        with contextlib.suppress(AttributeError):
            conn_any._sqlspec_enable_numpy_vectors = enable_vectors

        if enable_vectors and (server_major is None or server_major >= _ORACLE_VECTOR_MIN_MAJOR):
            register_numpy_handlers(connection)

        register_json_handlers(connection)

        if self.driver_features.get("enable_uuid_binary", False):
            register_uuid_handlers(connection)

        if self._owns_pool_session_callback and not self.driver_features.get("tenant_strategy"):
            static_hook = extract_static_alter_session_config(self.driver_features.get("session_context_hook"))
            if static_hook is not None:
                await execute_session_checkout_async(connection, static_hook)
        if callable(self._pool_session_callback):
            session_callback_result = self._pool_session_callback(connection, tag)
            if isawaitable(session_callback_result):
                await session_callback_result

        if self._user_connection_hook is not None:
            hook_result = self._user_connection_hook(connection, tag)
            if isawaitable(hook_result):
                await hook_result

    def _prepare_driver(self, driver: "OracleAsyncDriver") -> "OracleAsyncDriver":
        """Attach the pool-scoped version cache alongside the observability runtime."""
        driver = super()._prepare_driver(driver)
        driver._oracle_version_cache = self._oracle_version_cache
        return driver

    async def _close_pool(self) -> None:
        """Close the actual async connection pool and release any extracted temporary wallet directory."""
        try:
            if self.connection_instance:
                await self.connection_instance.close()
                self.connection_instance = None
            self._oracle_version_cache.reset()
            self._owns_pool_session_callback = False
            self._thin_sharding_warned = False
        finally:
            if self._extracted_wallet_dir is not None:
                wallet_dir = self._extracted_wallet_dir
                self._extracted_wallet_dir = None
                release_extracted_wallet_dir(wallet_dir)


def _process_oracle_wallet_and_config(
    connection_config: "Mapping[str, Any] | None",
) -> "tuple[dict[str, Any], Path | None]":
    """Extract wallet zip, retain any created directory, and normalize connection configuration."""
    normalized_conn = dict(normalize_connection_config(connection_config or {}))
    pending_wallet_dir: Path | None = None
    if normalized_conn.get("wallet_zip") is not None:
        raw_wallet_source = normalized_conn.pop("wallet_zip")
        source_is_existing_dir = isinstance(raw_wallet_source, (str, Path)) and Path(raw_wallet_source).is_dir()
        wallet_dir = extract_oracle_wallet_zip(
            raw_wallet_source, wallet_password=normalized_conn.get("wallet_password")
        )
        if not source_is_existing_dir or wallet_dir.resolve() != Path(raw_wallet_source).resolve():
            pending_wallet_dir = wallet_dir
            retain_extracted_wallet_dir(pending_wallet_dir)
        normalized_conn.setdefault("config_dir", str(wallet_dir))
        normalized_conn.setdefault("wallet_location", str(wallet_dir))
    try:
        built_config = build_connection_config(normalized_conn)
    except BaseException:
        if pending_wallet_dir is not None:
            release_extracted_wallet_dir(pending_wallet_dir)
        raise
    return built_config, pending_wallet_dir


def _build_standalone_connection_params(connection_config: "Mapping[str, Any]") -> dict[str, Any]:
    """Build connection parameters for standalone (unpooled) connect calls."""
    config = dict(connection_config)
    config.pop("thick_mode", None)
    config.pop("lib_dir", None)
    config.pop("soda_metadata_cache", None)
    config.pop("transaction_priority", None)
    config.pop("min", None)
    config.pop("max", None)
    config.pop("increment", None)
    config.pop("homogeneous", None)
    config.pop("timeout", None)
    config.pop("wait_timeout", None)
    config.pop("max_lifetime_session", None)
    config.pop("session_callback", None)
    config.pop("threaded", None)
    config.pop("getmode", None)
    config.pop("ping_interval", None)
    config.pop("ping_timeout", None)
    config.pop("max_sessions_per_shard", None)
    config.pop("pool_class", None)
    connection_type = config.pop("connectiontype", None)
    if connection_type is not None:
        config["conn_class"] = connection_type
    apply_wallet_ssl_context(config)
    return config


def _initialize_oracle_client(connection_config: "Mapping[str, Any]") -> None:
    """Initialize requested Thick mode before creating a pool or connection."""
    lib_dir = connection_config.get("lib_dir")
    if (
        not (connection_config.get("thick_mode") or lib_dir is not None or connection_config.get("soda_metadata_cache"))
        or not oracledb.is_thin_mode()
    ):
        return
    client_config = {}
    if lib_dir is not None:
        client_config["lib_dir"] = lib_dir
    if connection_config.get("config_dir") is not None:
        client_config["config_dir"] = connection_config["config_dir"]
    try:
        oracledb.init_oracle_client(**client_config)
    except oracledb.Error as exc:
        msg = f"Failed to initialize Oracle client in thick mode: {exc}"
        raise ImproperConfigurationError(msg) from exc


def _validate_async_oracle_mode(connection_config: "Mapping[str, Any]") -> None:
    """Reject Thick mode settings unsupported by async connections."""
    if (
        connection_config.get("thick_mode")
        or connection_config.get("lib_dir") is not None
        or connection_config.get("soda_metadata_cache")
    ):
        msg = "OracleAsyncConfig only supports Thin mode; Thick mode options (thick_mode, lib_dir, soda_metadata_cache) require OracleSyncConfig."
        raise ImproperConfigurationError(msg)


def _derive_oracle_pool_kwargs(
    source_config: Any,
    connection_config_overrides: "Mapping[str, Any] | None",
    driver_features_overrides: "Mapping[str, Any] | None",
    statement_config: "StatementConfig | None",
    migration_config: "dict[str, Any] | MigrationConfig | None",
    extension_config: "ExtensionConfigs | None",
    observability_config: "ObservabilityConfig | None",
    bind_key: "str | None",
    overrides: dict[str, Any],
) -> dict[str, Any]:
    """Derive common configuration keyword arguments for cloned Oracle pools."""
    overrides = dict(overrides)
    is_replica = bool(overrides.pop("is_replica", False))
    overrides.pop("replicas", None)
    connection_overrides = dict(connection_config_overrides or {})
    new_conn_cfg: dict[str, Any] = dict(source_config.connection_config)
    new_driver_features: dict[str, Any] = {
        key: value
        for key, value in source_config.driver_features.items()
        if key not in {"_statement_cache_holder", "storage_capabilities"}
    }
    if source_config._pool_session_callback is not None and "session_callback" not in new_conn_cfg:
        new_conn_cfg["session_callback"] = source_config._pool_session_callback
    if source_config._user_connection_hook is not None and "on_connection_create" not in new_driver_features:
        new_driver_features["on_connection_create"] = source_config._user_connection_hook
    if connection_config_overrides:
        new_conn_cfg.update(connection_config_overrides)
    if driver_features_overrides:
        new_driver_features.update(driver_features_overrides)
    for key, val in overrides.items():
        if key in VALID_ORACLE_DRIVER_FEATURE_KEYS:
            new_driver_features[key] = val
        else:
            new_conn_cfg[key] = val
            connection_overrides[key] = val
    endpoint_keys = {"topology", "service_name", "host", "port", "sid", "tns_alias", "url", "connection_string"}
    if (
        source_config._source_topology is not None
        and not endpoint_keys.intersection(connection_overrides)
        and "dsn" not in connection_overrides
    ):
        new_conn_cfg["topology"] = source_config._source_topology
    if endpoint_keys.intersection(connection_overrides):
        source_dsn = source_config.connection_config.get("dsn")
        topology = source_config._source_topology
        if "topology" in connection_overrides:
            for key in ("dsn", "host", "port", "service_name", "sid", "tns_alias", "url", "connection_string"):
                if key not in connection_overrides:
                    new_conn_cfg.pop(key, None)
        elif set(connection_overrides).intersection(endpoint_keys) == {"service_name"} and isinstance(source_dsn, str):
            service_name = str(connection_overrides["service_name"])
            if isinstance(topology, OracleTopologyConfig):
                new_conn_cfg.pop("dsn", None)
                new_conn_cfg.pop("service_name", None)
                new_conn_cfg["topology"] = topology.with_service_name(service_name)
            else:
                new_conn_cfg["dsn"] = rewrite_oracle_dsn_service_name(source_dsn, service_name)
                new_conn_cfg.pop("service_name", None)
        elif "dsn" not in connection_overrides:
            new_conn_cfg.pop("dsn", None)
            if (
                isinstance(source_dsn, str)
                and ("/" in source_dsn or source_dsn.startswith("("))
                and not set(connection_overrides).intersection({"tns_alias", "url", "connection_string"})
            ):
                params = oracledb.ConnectParams()
                params.parse_connect_string(source_dsn)
                for key in ("host", "port", "service_name", "sid"):
                    value = getattr(params, key, None)
                    if value is not None:
                        new_conn_cfg.setdefault(key, value)
    if (
        "tenant_strategy" in new_driver_features
        and "tenant_strategy" not in (driver_features_overrides or {})
        and "tenant_strategy" not in overrides
    ):
        new_driver_features["tenant_strategy"] = source_config._raw_tenant_strategy
    return {
        "is_replica": is_replica,
        "replicas": None,
        "connection_config": new_conn_cfg,
        "connection_instance": None,
        "migration_config": cast("dict[str, Any] | None", dict(source_config.migration_config))
        if migration_config is None
        else cast("dict[str, Any] | None", migration_config),
        "statement_config": source_config.statement_config if statement_config is None else statement_config,
        "driver_features": new_driver_features,
        "bind_key": source_config.bind_key if bind_key is None else bind_key,
        "extension_config": cast("ExtensionConfigs", dict(source_config.extension_config))
        if extension_config is None
        else extension_config,
        "observability_config": source_config.observability_config
        if observability_config is None
        else observability_config,
    }


def _oracle_checkout_state(
    config: Any, pool: Any, connection: Any, options: OracleAcquireOptions
) -> tuple[str | None, bool, Any]:
    """Resolve checkout state without touching Thin connection tags."""
    hook = config.driver_features.get("session_context_hook")
    desired = None
    skip = False
    if getattr(connection, "thin", None) is False and config.driver_features.get("enable_auto_state_tagging", True):
        desired = synthesize_session_state_tag(hook, explicit_tag=options.tag)
        skip = desired is not None and not has_callable_alter_session_fields(hook) and connection.tag == desired
    elif config._owns_pool_session_callback and not has_callable_alter_session_fields(hook):
        skip = True
    baseline = capture_oracle_transaction_priority_baseline(
        connection, configured_priority=config.connection_config.get("transaction_priority")
    )
    requested = options.transaction_priority
    if requested is None:
        requested = config.connection_config.get("transaction_priority")
    if requested is not None:
        apply_oracle_transaction_priority(connection, requested)
    return desired, skip, baseline


def _oracle_cleanup_error(body_error: BaseException | None, cleanup_error: BaseException) -> None:
    """Preserve an active body exception while recording failed cleanup."""
    if isinstance(cleanup_error, asyncio.CancelledError):
        raise cleanup_error
    if body_error is not None:
        body_error.add_note(f"Oracle connection cleanup failed: {cleanup_error}")
        return
    raise cleanup_error
