"""CockroachDB AsyncPG configuration."""

from contextlib import suppress
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, TypedDict, cast

from sqlspec.adapters.asyncpg.core import default_statement_config, register_json_codecs, register_pgvector_support
from sqlspec.adapters.cockroach_asyncpg._typing import (
    CockroachAsyncpgConnection,
    CockroachAsyncpgPool,
    CockroachAsyncpgSessionContext,
)
from sqlspec.adapters.cockroach_asyncpg._typing import CockroachAsyncpgRecord as Record
from sqlspec.adapters.cockroach_asyncpg._typing import cockroach_asyncpg_connect as asyncpg_connect
from sqlspec.adapters.cockroach_asyncpg._typing import cockroach_asyncpg_create_pool as asyncpg_create_pool
from sqlspec.adapters.cockroach_asyncpg.core import (
    CockroachAsyncpgRetryConfig,
    apply_driver_features,
    build_connection_config,
    validate_follower_read_staleness,
)
from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver, CockroachAsyncpgExceptionHandler
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.core.config_runtime import resolve_runtime_statement_config
from sqlspec.driver import AsyncPoolConnectionContext, AsyncPoolSessionFactory
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop
    from collections.abc import Awaitable, Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "CockroachAsyncpgConfig",
    "CockroachAsyncpgConnectionConfig",
    "CockroachAsyncpgConnectionContext",
    "CockroachAsyncpgDriverFeatures",
    "CockroachAsyncpgGSSLib",
    "CockroachAsyncpgPoolConfig",
    "CockroachAsyncpgTargetSessionAttrs",
    "build_connection_config",
)

_POOL_ONLY_CONFIG_KEYS: Final[frozenset[str]] = frozenset({
    "init",
    "max_inactive_connection_lifetime",
    "max_queries",
    "max_size",
    "min_size",
    "reset",
    "setup",
})


CockroachAsyncpgTargetSessionAttrs = Literal["any", "primary", "standby", "read-write", "read-only", "prefer-standby"]
CockroachAsyncpgGSSLib = Literal["gssapi", "sspi"]


class CockroachAsyncpgConnectionConfig(TypedDict):
    """AsyncPG connection parameters for CockroachDB."""

    dsn: NotRequired[str]
    """CockroachDB PostgreSQL-compatible connection URI."""
    host: NotRequired[str]
    """CockroachDB node or load-balancer hostname."""
    port: NotRequired[int]
    """CockroachDB SQL TCP port (defaults to ``26257``)."""
    user: NotRequired[str]
    """CockroachDB SQL username."""
    password: NotRequired[str]
    """Password or JWT token for ``user``."""
    service: NotRequired[str]
    """Service profile name in ``pg_service.conf``."""
    servicefile: NotRequired[str]
    """Path to an alternate ``pg_service.conf`` file."""
    database: NotRequired[str]
    """Target CockroachDB database name."""
    ssl: NotRequired[Any]
    """TLS mode string, boolean flag, or ``ssl.SSLContext`` instance."""
    passfile: NotRequired[str]
    """Path to a ``pgpass`` password file."""
    direct_tls: NotRequired[bool]
    """Initiate TLS handshake directly without ``SSLRequest`` negotiation."""
    timeout: NotRequired[float]
    """Connection establishment timeout in seconds."""
    connect_timeout: NotRequired[float]
    """Pool connection establishment timeout in seconds."""
    command_timeout: NotRequired[float]
    """Default per-command timeout in seconds."""
    application_name: NotRequired[str]
    """Application identifier forwarded via ``server_settings``."""
    cluster: NotRequired[str]
    """CockroachDB Serverless / Cloud routing cluster identifier."""
    pgbouncer: NotRequired[bool]
    """Disable prepared statement caching when connecting through a transaction pooler."""
    default_transaction_use_follower_reads: NotRequired[bool]
    """Session setting enabling bounded-staleness follower reads on read-only transactions."""
    results_buffer_size: NotRequired[int]
    """CockroachDB server result streaming buffer size in bytes."""
    statement_cache_size: NotRequired[int]
    """Maximum prepared statements cached per connection (``0`` disables caching)."""
    max_cached_statement_lifetime: NotRequired[int]
    """Maximum age in seconds before cached prepared statements are evicted."""
    max_cacheable_statement_size: NotRequired[int]
    """Maximum SQL byte length eligible for prepared statement caching."""
    server_settings: NotRequired["dict[str, str]"]
    """Startup CockroachDB session variables passed at connection time."""
    target_session_attrs: NotRequired[CockroachAsyncpgTargetSessionAttrs]
    """Required node session attribute when connecting across multiple hosts."""
    statement_timeout: NotRequired[float | int | None]
    """Statement execution timeout in seconds forwarded via server_settings."""
    idle_in_transaction_session_timeout: NotRequired[float | int | None]
    """Idle in transaction timeout in seconds forwarded via server_settings."""
    krbsrvname: NotRequired[str]
    """Kerberos service principal name for GSSAPI authentication."""
    gsslib: NotRequired[CockroachAsyncpgGSSLib]
    """GSSAPI library implementation (``gssapi`` or ``sspi``)."""


class CockroachAsyncpgPoolConfig(CockroachAsyncpgConnectionConfig):
    """AsyncPG pool parameters for CockroachDB."""

    min_size: NotRequired[int]
    """Minimum number of open connections maintained in the pool."""
    max_size: NotRequired[int]
    """Maximum number of concurrent connections allowed in the pool."""
    max_queries: NotRequired[int]
    """Number of queries after which a pooled connection is recycled."""
    max_inactive_connection_lifetime: NotRequired[float]
    """Idle timeout in seconds before closing unused connections above ``min_size``."""
    connect: NotRequired["Callable[..., Awaitable[CockroachAsyncpgConnection]]"]
    """Custom async connection factory passed to ``asyncpg.create_pool``."""
    setup: NotRequired["Callable[[CockroachAsyncpgConnection], Awaitable[None]]"]
    """Async hook run before each connection checkout from the pool."""
    init: NotRequired["Callable[[CockroachAsyncpgConnection], Awaitable[None]]"]
    """Async hook run once when a new physical connection is opened."""
    reset: NotRequired["Callable[[CockroachAsyncpgConnection], Awaitable[None]]"]
    """Async hook run when returning a connection to the pool."""
    loop: NotRequired["AbstractEventLoop"]
    """Explicit ``asyncio`` event loop for pool tasks."""
    connection_class: NotRequired[type["CockroachAsyncpgConnection"]]
    """Custom ``asyncpg.Connection`` subclass used by the pool."""
    record_class: NotRequired[type[Record]]
    """Custom ``asyncpg.Record`` subclass returned by raw queries."""
    extra: NotRequired["dict[str, Any]"]
    """Additional keyword arguments forwarded to ``asyncpg.create_pool``."""


class _NativeStorageCSVOptions(TypedDict):
    """Explicit CSV conventions; no header or NULL marker is inferred."""

    nullas: NotRequired[str]
    nullif: NotRequired[str]
    skip: NotRequired[int]


class CockroachAsyncpgDriverFeatures(TypedDict):
    """Driver feature flags for CockroachDB retries, follower reads, codecs, and native storage."""

    enable_native_storage: NotRequired[bool]
    """Opt in to server-side CockroachDB ``EXPORT`` and ``IMPORT INTO`` storage operations."""
    native_storage_csv_options: NotRequired[_NativeStorageCSVOptions]
    """Explicit CSV markers mapping supporting ``nullas`` (export NULL string), ``nullif`` (import NULL string), and ``skip`` (header line count to skip on import)."""
    enable_auto_retry: NotRequired[bool]
    """Automatically retry CockroachDB ``40001`` serialization failure transactions with backoff."""
    max_retries: NotRequired[int]
    """Maximum retry attempts on ``40001`` transaction serialization conflicts."""
    retry_delay_base_ms: NotRequired[float]
    """Initial exponential backoff delay in milliseconds between serialization retries."""
    retry_delay_max_ms: NotRequired[float]
    """Upper bound in milliseconds on exponential backoff delay between retries."""
    enable_retry_logging: NotRequired[bool]
    """Emit structured debug/warning logs when serialization retries occur."""
    enable_follower_reads: NotRequired[bool]
    """Allow ``AS OF SYSTEM TIME`` follower-read clauses on read queries."""
    default_staleness: NotRequired[str]
    """Default ``AS OF SYSTEM TIME`` staleness expression (such as ``'-5s'`` or ``follower_read_timestamp()``)."""
    json_serializer: NotRequired["Callable[[Any], str]"]
    """Custom JSON serializer registered on ``json``/``jsonb`` codecs (defaults to ``to_json``)."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom JSON deserializer registered on ``json``/``jsonb`` codecs (defaults to ``from_json``)."""
    enable_json_codecs: NotRequired[bool]
    """Register automatic ``json`` and ``jsonb`` codecs on new connections (defaults to ``True``)."""
    enable_pgvector: NotRequired[bool]
    """Register ``pgvector`` codecs for CockroachDB ``VECTOR`` columns when ``pgvector`` is installed."""
    on_connection_create: "NotRequired[Callable[[CockroachAsyncpgConnection], Awaitable[None]]]"
    """Async callback invoked with each raw ``asyncpg`` connection after codec initialization."""
    enable_events: NotRequired[bool]
    """Enable durable queue-backed SQLSpec event channel support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend (``poll_queue`` for CockroachDB table-backed polling)."""


class _CockroachAsyncpgSessionFactory(AsyncPoolSessionFactory):
    """Factory creating AsyncPG database connections for CockroachDB sessions."""

    __slots__ = ()


class CockroachAsyncpgConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for CockroachDB AsyncPG connections."""

    __slots__ = ()


class CockroachAsyncpgConfig(
    AsyncDatabaseConfig[CockroachAsyncpgConnection, CockroachAsyncpgPool, CockroachAsyncpgDriver]
):
    """Configuration for CockroachDB using AsyncPG."""

    driver_type: "ClassVar[type[CockroachAsyncpgDriver]]" = CockroachAsyncpgDriver
    connection_type: "ClassVar[type[Any]]" = cast("type[Any]", CockroachAsyncpgConnection)
    supports_transactional_ddl: "ClassVar[bool]" = False
    supports_migration_schemas: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_row_streaming: "ClassVar[bool]" = True
    type_coercion_capabilities: "ClassVar[TypeCoercionCapabilities]" = TypeCoercionCapabilities(
        datetime_binding="native", timestamp_precision="microsecond", json_columns_decoded=True, uuid_binding="native"
    )
    _connection_context_class: "ClassVar[type[CockroachAsyncpgConnectionContext]]" = CockroachAsyncpgConnectionContext
    _session_factory_class: "ClassVar[type[_CockroachAsyncpgSessionFactory]]" = _CockroachAsyncpgSessionFactory
    _session_context_class: "ClassVar[type[CockroachAsyncpgSessionContext]]" = CockroachAsyncpgSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "CockroachAsyncpgPoolConfig | dict[str, Any] | None" = None,
        connection_instance: "CockroachAsyncpgPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "CockroachAsyncpgDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        connection_config = build_connection_config(connection_config)
        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        driver_features.setdefault("enable_native_storage", False)
        _validate_native_storage_options(driver_features)
        _validate_follower_read_features(driver_features)
        driver_features.setdefault("enable_auto_retry", True)
        features_dict = dict(driver_features)
        self._user_connection_hook: Callable[[CockroachAsyncpgConnection], Awaitable[None]] | None = features_dict.pop(
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
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

    async def create_connection(self) -> "CockroachAsyncpgConnection":
        """Open a standalone connection owned by the caller.

        The connection carries the same connection settings and init hook the
        pool applies, consumes no pool slot, and must be closed by the caller.

        Returns:
            A CockroachDB asyncpg connection.
        """
        config = build_connection_config(self.connection_config)
        for key in _POOL_ONLY_CONFIG_KEYS:
            config.pop(key, None)
        connect = config.pop("connect", None)
        connection = await connect() if connect is not None else await asyncpg_connect(**config)
        init = self.connection_config.get("init", self._init_connection)
        try:
            await init(connection)
        except BaseException:
            with suppress(Exception):
                await connection.close()
            raise
        return cast("CockroachAsyncpgConnection", connection)

    def provide_session(
        self,
        *_args: Any,
        statement_config: "StatementConfig | None" = None,
        follower_reads: bool | None = None,
        staleness: str | None = None,
        **_kwargs: Any,
    ) -> "CockroachAsyncpgSessionContext":
        factory = _CockroachAsyncpgSessionFactory(self)
        driver_features = dict(self.driver_features)
        if follower_reads is not None:
            driver_features["enable_follower_reads"] = follower_reads
        if staleness is not None:
            driver_features["default_staleness"] = validate_follower_read_staleness(staleness)

        return CockroachAsyncpgSessionContext(
            acquire_connection=factory.acquire_connection,
            release_connection=factory.release_connection,
            statement_config=statement_config
            or (lambda: resolve_runtime_statement_config(None, self.statement_config, default_statement_config)),
            driver_features=driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "CockroachAsyncpgPool":
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "CockroachAsyncpgConfig": CockroachAsyncpgConfig,
            "CockroachAsyncpgConnection": CockroachAsyncpgConnection,
            "CockroachAsyncpgConnectionConfig": CockroachAsyncpgConnectionConfig,
            "CockroachAsyncpgConnectionContext": CockroachAsyncpgConnectionContext,
            "CockroachAsyncpgDriver": CockroachAsyncpgDriver,
            "CockroachAsyncpgDriverFeatures": CockroachAsyncpgDriverFeatures,
            "CockroachAsyncpgExceptionHandler": CockroachAsyncpgExceptionHandler,
            "CockroachAsyncpgPool": CockroachAsyncpgPool,
            "CockroachAsyncpgPoolConfig": CockroachAsyncpgPoolConfig,
            "CockroachAsyncpgRetryConfig": CockroachAsyncpgRetryConfig,
            "CockroachAsyncpgSessionContext": CockroachAsyncpgSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)

    async def _create_pool(self) -> "CockroachAsyncpgPool":
        config = build_connection_config(self.connection_config)
        config.setdefault("init", self._init_connection)
        return await asyncpg_create_pool(**config)

    async def _init_connection(self, connection: "CockroachAsyncpgConnection") -> None:
        """Initialize connection with JSON codecs and user callback."""
        if self.driver_features.get("enable_json_codecs", True):
            await register_json_codecs(
                connection,
                encoder=self.driver_features.get("json_serializer", to_json),
                decoder=self.driver_features.get("json_deserializer", from_json),
            )

        if self.driver_features.get("enable_pgvector") is True:
            await register_pgvector_support(connection)

        if self._user_connection_hook is not None:
            await self._user_connection_hook(connection)

    async def _close_pool(self) -> None:
        if not self.connection_instance:
            return
        await self.connection_instance.close()
        self.connection_instance = None


def _validate_follower_read_features(driver_features: "dict[str, Any]") -> None:
    staleness = driver_features.get("default_staleness")
    if staleness is None:
        return
    if not isinstance(staleness, str):
        msg = "default_staleness must be a string."
        raise ImproperConfigurationError(msg)
    driver_features["default_staleness"] = validate_follower_read_staleness(staleness)


def _validate_native_storage_options(driver_features: "dict[str, Any]") -> None:
    if "native_storage_csv_options" not in driver_features:
        return
    options = driver_features["native_storage_csv_options"]
    if not isinstance(options, dict) or options.keys() - {"nullas", "nullif", "skip"}:
        msg = "native_storage_csv_options must contain only nullas, nullif, and skip."
        raise ImproperConfigurationError(msg)
    for key in ("nullas", "nullif"):
        if key in options and not isinstance(options[key], str):
            msg = "native_storage_csv_options nullas and nullif must be strings."
            raise ImproperConfigurationError(msg)
    if "skip" in options and (type(options["skip"]) is not int or options["skip"] < 0):
        msg = "native_storage_csv_options skip must be a nonnegative integer, excluding bool."
        raise ImproperConfigurationError(msg)
    driver_features["native_storage_csv_options"] = dict(options)
