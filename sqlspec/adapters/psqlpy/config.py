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
    username: NotRequired[str]
    password: NotRequired[str]
    db_name: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    connect_timeout_sec: NotRequired[int]
    connect_timeout_nanosec: NotRequired[int]
    tcp_user_timeout_sec: NotRequired[int]
    tcp_user_timeout_nanosec: NotRequired[int]
    keepalives: NotRequired[bool]
    keepalives_idle_sec: NotRequired[int]
    keepalives_idle_nanosec: NotRequired[int]
    keepalives_interval_sec: NotRequired[int]
    keepalives_interval_nanosec: NotRequired[int]
    keepalives_retries: NotRequired[int]
    ssl_mode: NotRequired[str]
    ca_file: NotRequired[str]
    target_session_attrs: NotRequired[str]
    options: NotRequired[str]
    application_name: NotRequired[str]
    client_encoding: NotRequired[str]
    gssencmode: NotRequired[str]
    sslnegotiation: NotRequired[str]
    sslcompression: NotRequired[str]
    sslcert: NotRequired[str]
    sslkey: NotRequired[str]
    sslpassword: NotRequired[str]
    sslrootcert: NotRequired[str]
    sslcrl: NotRequired[str]
    require_auth: NotRequired[str]
    channel_binding: NotRequired[str]
    krbsrvname: NotRequired[str]
    gsslib: NotRequired[str]
    gssdelegation: NotRequired[str]
    service: NotRequired[str]
    load_balance_hosts: NotRequired[str]


class PsqlpyPoolParams(PsqlpyConnectionParams):
    """Psqlpy pool parameters."""

    hosts: NotRequired[list[str]]
    ports: NotRequired[list[int]]
    conn_recycling_method: NotRequired[str]
    max_db_pool_size: NotRequired[int]
    max_cached_statement_lifetime: NotRequired[int]
    configure: NotRequired["Callable[..., Any]"]
    extra: NotRequired["dict[str, Any]"]


class PsqlpyDriverFeatures(TypedDict):
    """Psqlpy driver feature flags.

    enable_cast_detection: Enable cast-aware parameter processing.
     Defaults to True. When enabled, SQL casts in prepared statements guide
     psqlpy parameter coercion for JSON, UUID, and timestamp-like values.
    enable_pgvector: Enable pgvector extension detection for vector similarity search.
     Defaults to True when the pgvector Python package is installed.
     Detects the PostgreSQL vector extension and promotes the dialect to "pgvector".
     It does not register psqlpy type handlers; use psqlpy.extra_types.PgVector
     or explicit SQL casts for vector values.
    enable_paradedb: Enable ParadeDB (pg_search) extension detection.
     When enabled and the pg_search extension is detected, the SQL dialect
     switches to "paradedb" which supports search operators (@@@, &&&, etc.)
     and inherits all pgvector distance operators.
     Defaults to True. Independent of enable_pgvector.
    enable_pg_textsearch: Enable pg_textsearch extension detection for BM25 search.
     When enabled and the pg_textsearch extension is detected, the SQL dialect
     switches to "pg_textsearch" which supports the BM25 <@> relevance ranking operator.
     Defaults to True.
    json_serializer: Custom JSON serializer applied to the statement configuration.
    json_deserializer: Custom JSON deserializer retained alongside the serializer for parity with asyncpg.
    on_connection_create: Async callback executed when a connection is acquired from pool.
     Receives the raw psqlpy connection for low-level driver configuration.
     Called once per tracked connection instance.
    enable_events: Enable database event channel support.
     Defaults to True when extension_config["events"] is configured.
     Provides pub/sub capabilities via LISTEN/NOTIFY or table-backed fallback.
     Requires extension_config["events"] for migration setup when using poll_queue or notify_queue.
    events_backend: Event channel backend selection.
     Options: "notify", "notify_queue", "poll_queue"
     - "notify": Transient PostgreSQL LISTEN/NOTIFY with no replay or retry
     - "notify_queue": Durable queue plus a PostgreSQL notification wakeup hint
     - "poll_queue": Durable queue discovered by polling
     Defaults to "notify".
    """

    enable_cast_detection: NotRequired[bool]
    enable_pgvector: NotRequired[bool]
    enable_paradedb: NotRequired[bool]
    enable_pg_textsearch: NotRequired[bool]
    enable_alloydb_columnar: NotRequired[bool]
    pooler_mode: NotRequired[Literal["session", "transaction", "statement", "auto"] | None]
    pgbouncer: NotRequired[bool]
    pgbouncer_mode: NotRequired[bool]
    direct_dsn: NotRequired["str | Mapping[str, Any] | None"]
    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    on_connection_create: "NotRequired[Callable[[PsqlpyConnection], Awaitable[None]]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[Literal["notify", "notify_queue", "poll_queue"]]


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
        self._explicit_max_cached_statement_lifetime = "max_cached_statement_lifetime" in raw_conn_cfg
        raw_options = raw_conn_cfg.get("options")
        self._explicit_options: str | None = raw_options if isinstance(raw_options, str) else None
        connection_config = build_connection_config(raw_conn_cfg)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        features_dict = dict(driver_features) if driver_features else {}
        if features_dict.get("pooler_mode") in {"transaction", "statement"} or bool(
            features_dict.get("pgbouncer") or features_dict.get("pgbouncer_mode")
        ):
            connection_config.setdefault("max_cached_statement_lifetime", 0)
        if bool(features_dict.get("enable_alloydb_columnar", False)) and features_dict.get("pooler_mode") == "session":
            columnar_opt = "-c google_columnar_engine.enabled=on"
            existing_options = connection_config.get("options")
            if isinstance(existing_options, str) and existing_options.strip():
                if "google_columnar_engine.enabled" not in existing_options:
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
        if not self._explicit_max_cached_statement_lifetime and conn_cfg.get("max_cached_statement_lifetime") == 0:
            conn_cfg.pop("max_cached_statement_lifetime", None)
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
