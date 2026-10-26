"""Psqlpy database configuration."""

import sys
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, Self, TypedDict, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.psqlpy._typing import PsqlpyConnection, PsqlpyCursor, PsqlpySessionContext
from sqlspec.adapters.psqlpy._typing import PsqlpyConnectionPool as ConnectionPool
from sqlspec.adapters.psqlpy.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver, PsqlpyExceptionHandler
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.core.config_runtime import (
    build_postgres_extension_probe_names,
    is_postgres_extension_active,
    resolve_postgres_extension_state,
    resolve_runtime_statement_config,
)
from sqlspec.driver import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.exceptions import MissingDependencyError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from sqlspec.core import StatementConfig

__all__ = ("PsqlpyConfig", "PsqlpyConnectionParams", "PsqlpyCursor", "PsqlpyDriverFeatures", "PsqlpyPoolParams")

_MAX_INITIALIZED_CONNECTION_IDS: Final[int] = 4096


class PsqlpyConnectionParams(TypedDict):
    """Psqlpy connection parameters."""

    dsn: NotRequired[str]
    """PostgreSQL connection URI (``postgresql://user:pass@host:port/db``)."""
    username: NotRequired[str]
    """PostgreSQL role name used for authentication."""
    password: NotRequired[str]
    """Password for ``username``."""
    db_name: NotRequired[str]
    """Target PostgreSQL database name."""
    host: NotRequired[str]
    """Database server hostname or IP address."""
    port: NotRequired[int]
    """Database server TCP port (defaults to ``5432``)."""
    connect_timeout_sec: NotRequired[int]
    """Whole-second component of the TCP connection establishment timeout."""
    connect_timeout_nanosec: NotRequired[int]
    """Sub-second nanosecond component of the connection establishment timeout."""
    tcp_user_timeout_sec: NotRequired[int]
    """Whole-second TCP unacknowledged transmission timeout."""
    tcp_user_timeout_nanosec: NotRequired[int]
    """Sub-second nanosecond component of the TCP user timeout."""
    keepalives: NotRequired[bool]
    """Enable TCP keepalive probes on client sockets."""
    keepalives_idle_sec: NotRequired[int]
    """Idle duration in seconds before sending the first TCP keepalive probe."""
    keepalives_idle_nanosec: NotRequired[int]
    """Nanosecond component of the TCP keepalive idle interval."""
    keepalives_interval_sec: NotRequired[int]
    """Interval in seconds between consecutive unacknowledged TCP keepalive probes."""
    keepalives_interval_nanosec: NotRequired[int]
    """Nanosecond component of the TCP keepalive probe interval."""
    keepalives_retries: NotRequired[int]
    """Maximum unacknowledged TCP keepalive probes before dropping the connection."""
    ssl_mode: NotRequired[str]
    """TLS negotiation policy (``disable``, ``prefer``, ``require``, ``verify-ca``, or ``verify-full``)."""
    ca_file: NotRequired[str]
    """Path to a PEM root CA bundle used to verify server certificates."""
    target_session_attrs: NotRequired[str]
    """Required server session state for multi-host routing (such as ``read-write``)."""
    options: NotRequired[str]
    """Command-line startup GUC options sent to the PostgreSQL backend."""
    application_name: NotRequired[str]
    """Application identifier reported in ``pg_stat_activity``."""
    client_encoding: NotRequired[str]
    """Client character set encoding (defaults to ``UTF8``)."""
    gssencmode: NotRequired[str]
    """GSSAPI transport encryption mode (``disable``, ``prefer``, or ``require``)."""
    sslnegotiation: NotRequired[str]
    """PostgreSQL 17+ TLS handshake mode (``postgres`` or ``direct``)."""
    sslcompression: NotRequired[str]
    """TLS compression setting forwarded to ``libpq``/Rust TLS configuration."""
    sslcert: NotRequired[str]
    """Path to the client TLS certificate file."""
    sslkey: NotRequired[str]
    """Path to the client TLS private key file."""
    sslpassword: NotRequired[str]
    """Passphrase unlocking an encrypted ``sslkey``."""
    sslrootcert: NotRequired[str]
    """Path to the trusted root certificate authority file."""
    sslcrl: NotRequired[str]
    """Path to the certificate revocation list file."""
    require_auth: NotRequired[str]
    """Comma-separated list of acceptable server authentication methods."""
    channel_binding: NotRequired[str]
    """SCRAM channel binding requirement (``disable``, ``prefer``, or ``require``)."""
    krbsrvname: NotRequired[str]
    """Kerberos service principal name for GSSAPI authentication."""
    gsslib: NotRequired[str]
    """GSSAPI library implementation selector."""
    gssdelegation: NotRequired[str]
    """Whether to delegate GSSAPI credentials to the PostgreSQL server."""
    service: NotRequired[str]
    """Service profile name defined in ``pg_service.conf``."""
    load_balance_hosts: NotRequired[str]
    """Host ordering policy across multi-host configurations (``disable`` or ``random``)."""


class PsqlpyPoolParams(PsqlpyConnectionParams):
    """Psqlpy pool parameters."""

    hosts: NotRequired[list[str]]
    """Ordered list of PostgreSQL hostnames for multi-host failover or load balancing."""
    ports: NotRequired[list[int]]
    """Port numbers corresponding to ``hosts``."""
    conn_recycling_method: NotRequired[str]
    """Pool connection reset strategy (such as ``fast`` or ``verified``)."""
    max_db_pool_size: NotRequired[int]
    """Maximum number of concurrent connections held by ``psqlpy.ConnectionPool``."""
    max_cached_statement_lifetime: NotRequired[int]
    """Maximum lifetime in seconds for cached prepared statements."""
    configure: NotRequired["Callable[..., Any]"]
    """Optional pool connection configuration callback."""
    extra: NotRequired["dict[str, Any]"]
    """Additional keyword options passed directly to ``psqlpy.ConnectionPool``."""


class PsqlpyDriverFeatures(TypedDict):
    """Optional driver feature flags, extension detection, and pooler settings for Psqlpy."""

    enable_cast_detection: NotRequired[bool]
    """Use SQL type casts in statements to coerce JSON, UUID, and temporal bind values (defaults to ``True``)."""
    enable_pgvector: NotRequired[bool]
    """Detect the PostgreSQL ``vector`` extension and promote the dialect to ``pgvector``."""
    enable_paradedb: NotRequired[bool]
    """Detect ``pg_search`` and promote the dialect to ``paradedb`` (defaults to ``True``)."""
    enable_pg_textsearch: NotRequired[bool]
    """Detect ``pg_textsearch`` and promote the dialect for BM25 ``<@>`` ranking (defaults to ``True``)."""
    enable_alloydb_columnar: NotRequired[bool]
    """Enable ``google_columnar_engine.enabled=on`` at session startup or per transaction."""
    pooler_mode: NotRequired[Literal["session", "transaction", "statement", "auto"] | None]
    """External pooler compatibility mode; executes queries with ``prepared=False`` when not ``session``."""
    pgbouncer: NotRequired[bool]
    """Alias for ``pooler_mode='transaction'`` disabling prepared execution."""
    pgbouncer_mode: NotRequired[bool]
    """Alias for ``pooler_mode='transaction'`` disabling prepared execution."""
    direct_dsn: NotRequired["str | Mapping[str, Any] | None"]
    """Direct PostgreSQL DSN or connection mapping bypassing the pooler for migrations and ``LISTEN/NOTIFY``."""
    json_serializer: NotRequired["Callable[[Any], str]"]
    """Custom JSON serializer applied to statement parameter conversion (defaults to ``to_json``)."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom JSON deserializer retained alongside ``json_serializer`` (defaults to ``from_json``)."""
    on_connection_create: "NotRequired[Callable[[PsqlpyConnection], Awaitable[None]]]"
    """Async callback invoked once per physical ``psqlpy`` connection acquired from the pool."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec event channel pub/sub support (defaults to ``True`` when ``events`` is configured)."""
    events_backend: NotRequired[Literal["notify", "notify_queue", "poll_queue"]]
    """Event transport backend (``notify`` for LISTEN/NOTIFY, ``notify_queue``, or ``poll_queue``)."""


class _PsqlpySessionFactory(AsyncPoolSessionFactory):
    __slots__ = ("_ctx",)

    def __init__(self, config: "PsqlpyConfig") -> None:
        super().__init__(config)
        self._ctx: Any | None = None

    async def acquire_connection(self) -> "PsqlpyConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        self._ctx = ctx
        connection = cast("PsqlpyConnection", await ctx.__aenter__())
        try:
            await self._config._ensure_connection(connection)  # pyright: ignore[reportPrivateUsage]
        except BaseException:
            await ctx.__aexit__(*sys.exc_info())
            self._ctx = None
            raise
        return connection

    async def release_connection(self, _conn: "PsqlpyConnection", **kwargs: Any) -> None:
        if self._ctx is not None:
            await self._ctx.__aexit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))
            self._ctx = None


class PsqlpyConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for Psqlpy connections."""

    __slots__ = ("_ctx",)

    def __init__(self, config: "PsqlpyConfig") -> None:
        super().__init__(config)
        self._ctx: Any = None

    async def __aenter__(self) -> PsqlpyConnection:
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool

        ctx = pool.acquire()
        self._ctx = ctx
        connection = await ctx.__aenter__()
        try:
            await self._config._ensure_connection(connection)  # pyright: ignore[reportPrivateUsage]
        except BaseException:
            await ctx.__aexit__(*sys.exc_info())
            self._ctx = None
            raise
        return connection  # type: ignore[no-any-return]

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._ctx:
            try:
                return await self._ctx.__aexit__(exc_type, exc_val, exc_tb)  # type: ignore[no-any-return]
            finally:
                self._ctx = None
        return None


@mypyc_attr(native_class=False)
class PsqlpyConfig(AsyncDatabaseConfig[PsqlpyConnection, "ConnectionPool", PsqlpyDriver]):
    """Configuration for Psqlpy asynchronous database connections."""

    driver_type: ClassVar[type[PsqlpyDriver]] = PsqlpyDriver
    connection_type: "ClassVar[type[PsqlpyConnection]]" = PsqlpyConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_migration_schemas: "ClassVar[bool]" = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    supports_native_row_streaming: ClassVar[bool] = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="native"
    )
    _connection_context_class: "ClassVar[type[PsqlpyConnectionContext]]" = PsqlpyConnectionContext
    _session_factory_class: "ClassVar[type[_PsqlpySessionFactory]]" = _PsqlpySessionFactory
    _session_context_class: "ClassVar[type[PsqlpySessionContext]]" = PsqlpySessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "PsqlpyPoolParams | dict[str, Any] | None" = None,
        connection_instance: "ConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "PsqlpyDriverFeatures | dict[str, Any] | None" = None,
        bind_key: str | None = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Psqlpy configuration.

        Args:
            connection_config: Connection and pool configuration parameters.
            connection_instance: Existing connection pool instance to use.
            migration_config: Migration configuration.
            statement_config: SQL statement configuration.
            driver_features: Driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration.
            **kwargs: Additional keyword arguments.
        """
        raw_conn_cfg = normalize_connection_config(connection_config)
        raw_options = raw_conn_cfg.get("options")
        self._explicit_options: str | None = raw_options if isinstance(raw_options, str) else None
        connection_config = build_connection_config(raw_conn_cfg)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        if bool(features_dict.get("enable_alloydb_columnar", False)) and features_dict.get("pooler_mode") == "session":
            columnar_opt = "-c google_columnar_engine.enable_columnar_scan=on"
            existing_options = connection_config.get("options")
            if isinstance(existing_options, str) and existing_options.strip():
                if "google_columnar_engine.enable_columnar_scan" not in existing_options:
                    connection_config["options"] = f"{existing_options} {columnar_opt}"
            else:
                connection_config["options"] = columnar_opt
        self._user_connection_hook: Callable[[PsqlpyConnection], Awaitable[None]] | None = features_dict.get(
            "on_connection_create"
        )
        self._initialized_connection_ids: set[int] = set()
        self._pgvector_available: bool | None = None
        self._paradedb_available: bool | None = None
        self._pg_textsearch_available: bool | None = None
        self._pg_textsearch_probe_error: Exception | None = None

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

    @property
    def pg_textsearch_available(self) -> bool:
        """Return True if the pg_textsearch extension is available."""
        return bool(self._pg_textsearch_available)

    async def create_connection(self) -> "PsqlpyConnection":
        """Create a single async connection (not from pool).

        Returns:
            A psqlpy Connection instance.
        """
        pool = self.connection_instance
        if pool is None:
            pool = await self.create_pool()
            self.connection_instance = pool

        connection = await pool.connection()
        await self._ensure_connection(connection)
        return connection

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "PsqlpySessionContext":
        """Provide an async driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            A PsqlpyDriver session context manager.
        """
        factory = _PsqlpySessionFactory(self)
        return PsqlpySessionContext(
            acquire_connection=factory.acquire_connection,
            release_connection=factory.release_connection,
            statement_config=statement_config
            or (lambda: resolve_runtime_statement_config(None, self.statement_config, default_statement_config)),
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "ConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for Psqlpy types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "PsqlpyConnectionContext": PsqlpyConnectionContext,
            "PsqlpyConnection": PsqlpyConnection,
            "PsqlpyConnectionParams": PsqlpyConnectionParams,
            "PsqlpyCursor": PsqlpyCursor,
            "PsqlpyDriver": PsqlpyDriver,
            "PsqlpyDriverFeatures": PsqlpyDriverFeatures,
            "PsqlpyExceptionHandler": PsqlpyExceptionHandler,
            "PsqlpyPoolParams": PsqlpyPoolParams,
            "PsqlpySessionContext": PsqlpySessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        """Return LISTEN/NOTIFY defaults for Psqlpy adapters."""

        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)

    def derive_direct_config(self) -> Self:
        """Derive a session-mode configuration using direct_dsn for DDL, migrations, or LISTEN/NOTIFY."""
        direct_dsn = self.driver_features.get("direct_dsn")
        if not direct_dsn:
            return self
        conn_cfg: dict[str, Any] = dict(self.connection_config)
        if self._explicit_options is not None:
            conn_cfg["options"] = self._explicit_options
        else:
            conn_cfg.pop("options", None)
        if isinstance(direct_dsn, str):
            conn_cfg["dsn"] = direct_dsn
        elif isinstance(direct_dsn, Mapping):
            conn_cfg.update(build_connection_config(direct_dsn))
        features: dict[str, Any] = dict(self.driver_features)
        features["pooler_mode"] = "session"
        features["pgbouncer"] = False
        features["pgbouncer_mode"] = False
        features["direct_dsn"] = None
        if self._user_connection_hook is not None:
            features["on_connection_create"] = self._user_connection_hook
        return type(self)(
            connection_config=cast("PsqlpyPoolParams", conn_cfg),
            connection_instance=None,
            migration_config=dict(self.migration_config),
            statement_config=self.statement_config,
            driver_features=cast("PsqlpyDriverFeatures", features),
            bind_key=self.bind_key,
            extension_config=cast("ExtensionConfigs", dict(self.extension_config)),
            observability_config=self.observability_config,
        )

    async def _ensure_connection(self, connection: "PsqlpyConnection") -> None:
        """Ensure connection callback has been called once for this connection.

        Detects PostgreSQL extensions on first connection and updates dialect accordingly.
        """
        if self._pgvector_available is None:
            detected_extensions: set[str] = set()
            extensions = build_postgres_extension_probe_names(self.driver_features)
            probe_succeeded = True
            if extensions:
                try:
                    result = await connection.fetch(
                        "SELECT extname FROM pg_extension WHERE extname = ANY($1::text[])", [extensions]
                    )
                    rows = result.result() if result else []
                    detected_extensions = {r["extname"] for r in rows}
                except Exception as exc:
                    probe_succeeded = False
                    self._pg_textsearch_probe_error = exc
            if probe_succeeded:
                self.statement_config, self._pgvector_available, self._paradedb_available = (
                    resolve_postgres_extension_state(self.statement_config, self.driver_features, detected_extensions)
                )
                self._pg_textsearch_available = is_postgres_extension_active(self.driver_features, "pg_textsearch")
                self._pg_textsearch_probe_error = None

        conn_id = id(connection)
        if conn_id not in self._initialized_connection_ids:
            if self._user_connection_hook is not None:
                await self._user_connection_hook(connection)
            if len(self._initialized_connection_ids) >= _MAX_INITIALIZED_CONNECTION_IDS:
                self._initialized_connection_ids.clear()
            self._initialized_connection_ids.add(conn_id)

    def _ensure_pg_textsearch_available(self) -> None:
        """Raise MissingDependencyError if pg_textsearch is required but unavailable."""
        if not self.pg_textsearch_available:
            msg = "PostgreSQL extension 'pg_textsearch' is required for BM25 hybrid search but is not installed or enabled."
            raise MissingDependencyError(msg) from self._pg_textsearch_probe_error

    async def _create_pool(self) -> "ConnectionPool":
        """Create the actual async connection pool."""
        return ConnectionPool(**build_connection_config(self.connection_config))

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if not self.connection_instance:
            return

        self.connection_instance.close()
        self.connection_instance = None
        self._initialized_connection_ids.clear()
