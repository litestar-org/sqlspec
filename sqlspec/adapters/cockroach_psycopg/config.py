"""CockroachDB configuration using psycopg."""

import inspect
import re
from contextlib import suppress
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, NotRequired, TypedDict, cast

from sqlspec.adapters.cockroach_psycopg._typing import (
    CockroachAsyncConnection,
    CockroachPsycopgAsyncSessionContext,
    CockroachPsycopgSyncSessionContext,
    CockroachSyncConnection,
)
from sqlspec.adapters.cockroach_psycopg._typing import CockroachPsycopgAdaptersMap as AdaptersMap
from sqlspec.adapters.cockroach_psycopg._typing import CockroachPsycopgAsyncConnectionPool as AsyncConnectionPool
from sqlspec.adapters.cockroach_psycopg._typing import CockroachPsycopgConnectionPool as ConnectionPool
from sqlspec.adapters.cockroach_psycopg._typing import cockroach_psycopg_crdb as psycopg_crdb
from sqlspec.adapters.cockroach_psycopg._typing import cockroach_psycopg_set_json_dumps as set_json_dumps
from sqlspec.adapters.cockroach_psycopg._typing import cockroach_psycopg_set_json_loads as set_json_loads
from sqlspec.adapters.cockroach_psycopg.core import (
    CockroachPsycopgRetryConfig,
    apply_driver_features,
    build_connection_config,
    build_statement_config,
    validate_follower_read_staleness,
)
from sqlspec.adapters.cockroach_psycopg.driver import (
    CockroachPsycopgAsyncDriver,
    CockroachPsycopgAsyncExceptionHandler,
    CockroachPsycopgSyncDriver,
    CockroachPsycopgSyncExceptionHandler,
)
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core.capabilities import TypeCoercionCapabilities
from sqlspec.core.config_runtime import resolve_runtime_statement_config
from sqlspec.driver import (
    AsyncPoolConnectionContext,
    AsyncPoolSessionFactory,
    SyncPoolConnectionContext,
    SyncPoolSessionFactory,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "CockroachPsycopgAsyncConfig",
    "CockroachPsycopgAsyncConnectionContext",
    "CockroachPsycopgConnectionConfig",
    "CockroachPsycopgDriverFeatures",
    "CockroachPsycopgPoolConfig",
    "CockroachPsycopgSyncConfig",
    "CockroachPsycopgSyncConnectionContext",
    "build_connection_config",
)

default_statement_config = build_statement_config()


class CockroachPsycopgConnectionConfig(TypedDict):
    """CockroachDB connection parameters."""

    conninfo: NotRequired[str]
    """CockroachDB libpq connection URI or keyword/value conninfo string."""
    host: NotRequired[str]
    """CockroachDB node or load-balancer hostname."""
    port: NotRequired[int]
    """CockroachDB SQL TCP port (defaults to ``26257``)."""
    user: NotRequired[str]
    """CockroachDB SQL username."""
    password: NotRequired[str]
    """Password or JWT token for ``user``."""
    dbname: NotRequired[str]
    """Target CockroachDB database name."""
    connect_timeout: NotRequired[int]
    """Connection establishment timeout in seconds."""
    options: NotRequired[str]
    """Command-line startup session options sent to CockroachDB."""
    default_transaction_use_follower_reads: NotRequired[bool]
    """Enable bounded-staleness follower reads for read-only transactions."""
    results_buffer_size: NotRequired[int]
    """CockroachDB server result streaming buffer size in bytes."""
    statement_timeout: NotRequired[int]
    """Per-statement execution timeout in milliseconds."""
    idle_in_transaction_session_timeout: NotRequired[int]
    """Idle-in-transaction timeout in milliseconds before aborting open transactions."""
    application_name: NotRequired[str]
    """Application identifier visible in CockroachDB session diagnostics."""
    sslmode: NotRequired[str]
    """libpq TLS verification mode (such as ``require`` or ``verify-full``)."""
    sslcert: NotRequired[str]
    """Path to the client TLS certificate file."""
    sslkey: NotRequired[str]
    """Path to the client TLS private key file."""
    sslrootcert: NotRequired[str]
    """Path to the root CA certificate bundle."""
    autocommit: NotRequired[bool]
    """Enable Psycopg autocommit mode outside explicit transactions."""
    prepare_threshold: NotRequired[int | None]
    """Executions before Psycopg prepares a statement server-side, or ``None`` to disable."""
    row_factory: NotRequired["Callable[..., Any]"]
    """Custom Psycopg row factory applied to raw connections."""
    cursor_factory: NotRequired["type[Any]"]
    """Custom Psycopg cursor class used by raw connections."""
    context: NotRequired[Any]
    """Psycopg ``AdaptContext`` providing custom type adapters."""
    cluster: NotRequired[str]
    """CockroachDB Serverless / Cloud routing cluster identifier."""
    extra: NotRequired["dict[str, Any]"]
    """Additional libpq keyword parameters merged into ``connect``."""


class CockroachPsycopgPoolConfig(CockroachPsycopgConnectionConfig):
    """CockroachDB pool parameters."""

    min_size: NotRequired[int]
    """Minimum number of connections maintained in ``ConnectionPool`` / ``AsyncConnectionPool``."""
    max_size: NotRequired[int]
    """Maximum number of connections allowed in the pool."""
    name: NotRequired[str]
    """Diagnostic pool name used in logs and worker threads."""
    timeout: NotRequired[float]
    """Maximum wait time in seconds when acquiring a connection from the pool."""
    max_waiting: NotRequired[int]
    """Maximum queued callers waiting for an available connection (``0`` means unlimited)."""
    max_lifetime: NotRequired[float]
    """Maximum lifetime in seconds before a pooled connection is recycled."""
    max_idle: NotRequired[float]
    """Maximum idle time in seconds before shrinking connections above ``min_size``."""
    reconnect_timeout: NotRequired[float]
    """Duration in seconds the pool retries failed connection attempts before invoking ``reconnect_failed``."""
    reconnect_failed: NotRequired["Callable[..., Any]"]
    """Callback invoked when pool reconnection attempts exceed ``reconnect_timeout``."""
    num_workers: NotRequired[int]
    """Number of background maintenance worker threads/tasks managed by the pool."""
    configure: NotRequired["Callable[..., Any]"]
    """Callback invoked once on each newly created connection before entering the pool."""
    check: NotRequired["Callable[..., Any]"]
    """Health-check callback invoked before handing out a pooled connection."""
    reset: NotRequired["Callable[..., Any]"]
    """Callback invoked to clean connection state when returned to the pool."""
    open: NotRequired[bool | None]
    """Whether to open the pool immediately during construction."""
    close_returns: NotRequired[bool]
    """Make ``conn.close()`` return the connection to the pool instead of closing the socket."""
    kwargs: NotRequired["dict[str, Any]"]
    """Explicit connection keyword dictionary passed to ``psycopg.connect``."""


class _NativeStorageCSVOptions(TypedDict):
    """Explicit CSV conventions; no header or NULL marker is inferred."""

    nullas: NotRequired[str]
    nullif: NotRequired[str]
    skip: NotRequired[int]


class CockroachPsycopgDriverFeatures(TypedDict):
    """Driver feature flags for CockroachDB Psycopg retries, follower reads, and native storage."""

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
    """Custom JSON serializer applied to statement parameters (defaults to ``to_json``)."""
    json_deserializer: NotRequired["Callable[[str], Any]"]
    """Custom JSON deserializer retained alongside ``json_serializer`` (defaults to ``from_json``)."""
    on_connection_create: "NotRequired[Callable[..., Any]]"
    """Sync or async callback invoked with each raw Psycopg connection after internal setup."""
    enable_events: NotRequired[bool]
    """Enable durable queue-backed SQLSpec event channel support."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend (``poll_queue`` for CockroachDB table-backed polling)."""


class CockroachPsycopgSyncConnectionContext(SyncPoolConnectionContext):
    """Context manager for CockroachDB psycopg connections."""

    __slots__ = ()

    def __init__(self, config: "CockroachPsycopgSyncConfig") -> None:
        super().__init__(config)

    def __enter__(self) -> "CockroachSyncConnection":
        self._ctx = self._config.provide_pool().connection()
        return cast("CockroachSyncConnection", self._ctx.__enter__())

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._ctx is None:
            return None
        return cast("bool | None", self._ctx.__exit__(exc_type, exc_val, exc_tb))


class _CockroachPsycopgSyncSessionConnectionHandler(SyncPoolSessionFactory):
    __slots__ = ()

    def acquire_connection(self) -> "CockroachSyncConnection":
        self._ctx = self._config.provide_pool().connection()
        return cast("CockroachSyncConnection", self._ctx.__enter__())

    def release_connection(self, _conn: "CockroachSyncConnection", **kwargs: Any) -> None:
        if self._ctx is not None:
            self._ctx.__exit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))
            self._ctx = None


class CockroachPsycopgSyncConfig(
    SyncDatabaseConfig[CockroachSyncConnection, ConnectionPool, CockroachPsycopgSyncDriver]
):
    """Configuration for CockroachDB synchronous connections using psycopg."""

    driver_type: "ClassVar[type[CockroachPsycopgSyncDriver]]" = CockroachPsycopgSyncDriver
    connection_type: "ClassVar[type[CockroachSyncConnection]]" = CockroachSyncConnection
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
    _connection_context_class: "ClassVar[type[CockroachPsycopgSyncConnectionContext]]" = (
        CockroachPsycopgSyncConnectionContext
    )
    _session_factory_class: "ClassVar[type[_CockroachPsycopgSyncSessionConnectionHandler]]" = (
        _CockroachPsycopgSyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[CockroachPsycopgSyncSessionContext]]" = CockroachPsycopgSyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "CockroachPsycopgPoolConfig | dict[str, Any] | None" = None,
        connection_instance: "ConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "CockroachPsycopgDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        connection_config = build_connection_config(connection_config)
        statement_config = statement_config or default_statement_config
        _validate_driver_features(driver_features)
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        driver_features.setdefault("enable_native_storage", False)
        _validate_native_storage_options(driver_features)
        _validate_follower_read_features(driver_features)
        _validate_native_storage_autocommit(connection_config, driver_features)
        driver_features.setdefault("enable_auto_retry", True)
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[CockroachSyncConnection], None] | None = features_dict.pop(
            "on_connection_create", None
        )
        self._user_configure: Callable[[CockroachSyncConnection], Any] | None = connection_config.pop("configure", None)

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

    def create_connection(self) -> "CockroachSyncConnection":
        """Open a standalone connection owned by the caller.

        Returns:
            A connection that is not bound to the pool and can be closed directly.
        """
        conninfo, connection_kwargs = _standalone_connection_kwargs(dict(self.connection_config))
        connection = psycopg_crdb.CrdbConnection.connect(conninfo, **connection_kwargs)
        try:
            self._configure_connection(cast("CockroachSyncConnection", connection))
        except BaseException:
            with suppress(Exception):
                connection.close()
            raise
        return cast("CockroachSyncConnection", connection)

    def provide_session(
        self,
        *_args: Any,
        statement_config: "StatementConfig | None" = None,
        follower_reads: bool | None = None,
        staleness: str | None = None,
        **_kwargs: Any,
    ) -> "CockroachPsycopgSyncSessionContext":
        handler = _CockroachPsycopgSyncSessionConnectionHandler(self)
        driver_features = dict(self.driver_features)
        if follower_reads is not None:
            driver_features["enable_follower_reads"] = follower_reads
        if staleness is not None:
            driver_features["default_staleness"] = validate_follower_read_staleness(staleness)

        return CockroachPsycopgSyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config
            or (lambda: resolve_runtime_statement_config(None, self.statement_config, default_statement_config)),
            driver_features=driver_features,
            prepare_driver=self._prepare_driver,
        )

    def provide_pool(self, *args: Any, **kwargs: Any) -> "ConnectionPool":
        if not self.connection_instance:
            self.connection_instance = self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "CockroachPsycopgConnectionConfig": CockroachPsycopgConnectionConfig,
            "CockroachPsycopgConnectionPool": ConnectionPool,
            "CockroachPsycopgDriverFeatures": CockroachPsycopgDriverFeatures,
            "CockroachPsycopgPoolConfig": CockroachPsycopgPoolConfig,
            "CockroachPsycopgRetryConfig": CockroachPsycopgRetryConfig,
            "CockroachPsycopgSyncConfig": CockroachPsycopgSyncConfig,
            "CockroachPsycopgSyncConnectionContext": CockroachPsycopgSyncConnectionContext,
            "CockroachPsycopgSyncDriver": CockroachPsycopgSyncDriver,
            "CockroachPsycopgSyncExceptionHandler": CockroachPsycopgSyncExceptionHandler,
            "CockroachPsycopgSyncSessionContext": CockroachPsycopgSyncSessionContext,
            "CockroachSyncConnection": CockroachSyncConnection,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)

    def _create_pool(self) -> "ConnectionPool":
        all_config = dict(self.connection_config)

        pool_parameters = {
            "min_size": all_config.pop("min_size", 4),
            "max_size": all_config.pop("max_size", None),
            "name": all_config.pop("name", None),
            "timeout": all_config.pop("timeout", 30.0),
            "max_waiting": all_config.pop("max_waiting", 0),
            "max_lifetime": all_config.pop("max_lifetime", 3600.0),
            "max_idle": all_config.pop("max_idle", 600.0),
            "reconnect_timeout": all_config.pop("reconnect_timeout", 300.0),
            "reconnect_failed": all_config.pop("reconnect_failed", None),
            "num_workers": all_config.pop("num_workers", 3),
            "check": all_config.pop("check", None),
            "reset": all_config.pop("reset", None),
            "open": all_config.pop("open", True),
            "close_returns": all_config.pop("close_returns", False),
        }

        pool_parameters["configure"] = self._configure_connection
        pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

        conninfo = all_config.pop("conninfo", None)
        kwargs = all_config.pop("kwargs", {})
        extra = all_config.pop("extra", {})
        all_config.update(kwargs)
        all_config.update(extra)
        _apply_cluster_option(all_config)
        if conninfo:
            return ConnectionPool(
                conninfo, kwargs=all_config, connection_class=psycopg_crdb.CrdbConnection, **pool_parameters
            )

        return ConnectionPool("", kwargs=all_config, connection_class=psycopg_crdb.CrdbConnection, **pool_parameters)

    def _configure_connection(self, conn: "CockroachSyncConnection") -> None:
        autocommit_setting = self.connection_config.get("autocommit")
        if autocommit_setting is not None:
            conn.autocommit = autocommit_setting

        if isinstance(getattr(conn, "adapters", None), AdaptersMap):
            serializer = self.driver_features.get("json_serializer")
            deserializer = self.driver_features.get("json_deserializer")
            if serializer is not None:
                set_json_dumps(serializer, conn)
            if deserializer is not None:
                set_json_loads(deserializer, conn)

        if self._user_connection_hook is not None:
            self._user_connection_hook(conn)

        if self._user_configure is not None:
            result = self._user_configure(conn)
            if inspect.isawaitable(result):
                msg = "Async configure callback cannot be used with CockroachPsycopgSyncConfig"
                raise ImproperConfigurationError(msg)

    def _close_pool(self) -> None:
        if not self.connection_instance:
            return
        try:
            self.connection_instance.close()
        finally:
            self.connection_instance = None


class CockroachPsycopgAsyncConnectionContext(AsyncPoolConnectionContext):
    """Async context manager for CockroachDB psycopg connections."""

    __slots__ = ("_ctx",)

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
        super().__init__(config)
        self._ctx: Any = None

    async def __aenter__(self) -> "CockroachAsyncConnection":
        pool = await self._config.provide_pool()
        self._ctx = pool.connection()
        return cast("CockroachAsyncConnection", await self._ctx.__aenter__())

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> bool | None:
        if self._ctx:
            return cast("bool | None", await self._ctx.__aexit__(exc_type, exc_val, exc_tb))
        return None


class _CockroachPsycopgAsyncSessionConnectionHandler(AsyncPoolSessionFactory):
    __slots__ = ("_ctx",)

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
        super().__init__(config)
        self._ctx: Any = None

    async def acquire_connection(self) -> "CockroachAsyncConnection":
        pool = await self._config.provide_pool()
        ctx = pool.connection()
        self._ctx = ctx
        return cast("CockroachAsyncConnection", await ctx.__aenter__())

    async def release_connection(self, _conn: "CockroachAsyncConnection", **kwargs: Any) -> None:
        if self._ctx is not None:
            await self._ctx.__aexit__(kwargs.get("exc_type"), kwargs.get("exc_val"), kwargs.get("exc_tb"))
            self._ctx = None


class CockroachPsycopgAsyncConfig(
    AsyncDatabaseConfig[CockroachAsyncConnection, AsyncConnectionPool, CockroachPsycopgAsyncDriver]
):
    """Configuration for CockroachDB async connections using psycopg."""

    driver_type: "ClassVar[type[CockroachPsycopgAsyncDriver]]" = CockroachPsycopgAsyncDriver
    connection_type: "ClassVar[type[CockroachAsyncConnection]]" = CockroachAsyncConnection
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
    _connection_context_class: "ClassVar[type[CockroachPsycopgAsyncConnectionContext]]" = (
        CockroachPsycopgAsyncConnectionContext
    )
    _session_factory_class: "ClassVar[type[_CockroachPsycopgAsyncSessionConnectionHandler]]" = (
        _CockroachPsycopgAsyncSessionConnectionHandler
    )
    _session_context_class: "ClassVar[type[CockroachPsycopgAsyncSessionContext]]" = CockroachPsycopgAsyncSessionContext
    _default_statement_config = default_statement_config

    def __init__(
        self,
        *,
        connection_config: "CockroachPsycopgPoolConfig | dict[str, Any] | None" = None,
        connection_instance: "AsyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "CockroachPsycopgDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        connection_config = build_connection_config(connection_config)
        statement_config = statement_config or default_statement_config
        _validate_driver_features(driver_features)
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        driver_features.setdefault("enable_native_storage", False)
        _validate_native_storage_options(driver_features)
        _validate_follower_read_features(driver_features)
        _validate_native_storage_autocommit(connection_config, driver_features)
        driver_features.setdefault("enable_auto_retry", True)
        features_dict = dict(driver_features) if driver_features else {}
        self._user_connection_hook: Callable[[CockroachAsyncConnection], Awaitable[None]] | None = features_dict.pop(
            "on_connection_create", None
        )
        self._user_configure: Callable[[CockroachAsyncConnection], Any] | None = connection_config.pop(
            "configure", None
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

    async def create_connection(self) -> "CockroachAsyncConnection":
        """Open a standalone connection owned by the caller.

        Returns:
            A connection that is not bound to the pool and can be closed directly.
        """
        conninfo, connection_kwargs = _standalone_connection_kwargs(dict(self.connection_config))
        connection = await psycopg_crdb.AsyncCrdbConnection.connect(conninfo, **connection_kwargs)
        try:
            await self._configure_async_connection(cast("CockroachAsyncConnection", connection))
        except BaseException:
            with suppress(Exception):
                await connection.close()
            raise
        return cast("CockroachAsyncConnection", connection)

    def provide_session(
        self,
        *_args: Any,
        statement_config: "StatementConfig | None" = None,
        follower_reads: bool | None = None,
        staleness: str | None = None,
        **_kwargs: Any,
    ) -> "CockroachPsycopgAsyncSessionContext":
        handler = _CockroachPsycopgAsyncSessionConnectionHandler(self)
        driver_features = dict(self.driver_features)
        if follower_reads is not None:
            driver_features["enable_follower_reads"] = follower_reads
        if staleness is not None:
            driver_features["default_staleness"] = validate_follower_read_staleness(staleness)

        return CockroachPsycopgAsyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config
            or (lambda: resolve_runtime_statement_config(None, self.statement_config, default_statement_config)),
            driver_features=driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "AsyncConnectionPool":
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        namespace = super().get_signature_namespace()
        namespace.update({
            "CockroachAsyncConnection": CockroachAsyncConnection,
            "CockroachPsycopgAsyncConfig": CockroachPsycopgAsyncConfig,
            "CockroachPsycopgAsyncConnectionContext": CockroachPsycopgAsyncConnectionContext,
            "CockroachPsycopgAsyncConnectionPool": AsyncConnectionPool,
            "CockroachPsycopgAsyncDriver": CockroachPsycopgAsyncDriver,
            "CockroachPsycopgAsyncExceptionHandler": CockroachPsycopgAsyncExceptionHandler,
            "CockroachPsycopgAsyncSessionContext": CockroachPsycopgAsyncSessionContext,
            "CockroachPsycopgConnectionConfig": CockroachPsycopgConnectionConfig,
            "CockroachPsycopgDriverFeatures": CockroachPsycopgDriverFeatures,
            "CockroachPsycopgPoolConfig": CockroachPsycopgPoolConfig,
            "CockroachPsycopgRetryConfig": CockroachPsycopgRetryConfig,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)

    async def _create_pool(self) -> "AsyncConnectionPool":
        all_config = dict(self.connection_config)

        pool_parameters = {
            "min_size": all_config.pop("min_size", 4),
            "max_size": all_config.pop("max_size", None),
            "name": all_config.pop("name", None),
            "timeout": all_config.pop("timeout", 30.0),
            "max_waiting": all_config.pop("max_waiting", 0),
            "max_lifetime": all_config.pop("max_lifetime", 3600.0),
            "max_idle": all_config.pop("max_idle", 600.0),
            "reconnect_timeout": all_config.pop("reconnect_timeout", 300.0),
            "reconnect_failed": all_config.pop("reconnect_failed", None),
            "num_workers": all_config.pop("num_workers", 3),
            "check": all_config.pop("check", None),
            "reset": all_config.pop("reset", None),
            "close_returns": all_config.pop("close_returns", False),
        }
        open_pool = all_config.pop("open", True)
        pool_parameters["open"] = False if open_pool is True else open_pool

        pool_parameters["configure"] = self._configure_async_connection
        pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

        conninfo = all_config.pop("conninfo", None)
        kwargs = all_config.pop("kwargs", {})
        extra = all_config.pop("extra", {})
        all_config.update(kwargs)
        all_config.update(extra)
        _apply_cluster_option(all_config)
        if conninfo:
            pool = AsyncConnectionPool(
                conninfo, kwargs=all_config, connection_class=psycopg_crdb.AsyncCrdbConnection, **pool_parameters
            )
        else:
            pool = AsyncConnectionPool(
                "", kwargs=all_config, connection_class=psycopg_crdb.AsyncCrdbConnection, **pool_parameters
            )

        if open_pool is True:
            await pool.open()
        return cast("AsyncConnectionPool", pool)

    async def _configure_async_connection(self, conn: "CockroachAsyncConnection") -> None:
        autocommit_setting = self.connection_config.get("autocommit")
        if autocommit_setting is not None:
            await conn.set_autocommit(autocommit_setting)

        if isinstance(getattr(conn, "adapters", None), AdaptersMap):
            serializer = self.driver_features.get("json_serializer")
            deserializer = self.driver_features.get("json_deserializer")
            if serializer is not None:
                set_json_dumps(serializer, conn)
            if deserializer is not None:
                set_json_loads(deserializer, conn)

        if self._user_connection_hook is not None:
            await self._user_connection_hook(conn)

        if self._user_configure is not None:
            result = self._user_configure(conn)
            if inspect.isawaitable(result):
                await result

    async def _close_pool(self) -> None:
        if not self.connection_instance:
            return
        try:
            await self.connection_instance.close()
        finally:
            self.connection_instance = None


_POOL_ONLY_CONFIG_KEYS: Final[frozenset[str]] = frozenset({
    "check",
    "close_returns",
    "configure",
    "max_idle",
    "max_lifetime",
    "max_size",
    "max_waiting",
    "min_size",
    "name",
    "num_workers",
    "open",
    "reconnect_failed",
    "reconnect_timeout",
    "reset",
    "timeout",
})


def _standalone_connection_kwargs(connection_config: "dict[str, Any]") -> "tuple[str, dict[str, Any]]":
    """Split a pooled connection config into conninfo plus connect keyword arguments.

    Args:
        connection_config: The adapter's normalized connection configuration.

    Returns:
        The conninfo string and the keyword arguments accepted by ``connect``.
    """
    all_config = {k: v for k, v in connection_config.items() if k not in _POOL_ONLY_CONFIG_KEYS}
    conninfo = all_config.pop("conninfo", "")
    all_config.update(all_config.pop("kwargs", {}))
    all_config.update(all_config.pop("extra", {}))
    _apply_cluster_option(all_config)
    return str(conninfo or ""), all_config


_CLUSTER_NAME_PATTERN: "re.Pattern[str]" = re.compile(r"[A-Za-z0-9._-]+")


def _apply_cluster_option(connection_kwargs: "dict[str, Any]") -> None:
    """Fold a CockroachDB Cloud cluster name into the libpq options string.

    ``cluster`` is not a libpq connection parameter; multi-tenant routing is
    expressed through ``options=--cluster=<name>``.

    Raises:
        ImproperConfigurationError: If the name carries anything but the
            characters a cluster name may contain. The value is interpolated
            into an options string, where whitespace would start a new option.
    """
    cluster = connection_kwargs.pop("cluster", None)
    if not cluster:
        return
    name = str(cluster)
    if _CLUSTER_NAME_PATTERN.fullmatch(name) is None:
        msg = f"CockroachDB cluster name {name!r} may contain only letters, digits, hyphens, underscores, and periods."
        raise ImproperConfigurationError(msg)
    routing = f"--cluster={name}"
    existing = connection_kwargs.get("options")
    connection_kwargs["options"] = f"{existing} {routing}" if existing else routing


def _validate_driver_features(driver_features: "CockroachPsycopgDriverFeatures | dict[str, Any] | None") -> None:
    if driver_features and "prefer_uuid_keys" in driver_features:
        msg = "CockroachDB psycopg driver_features no longer supports unused 'prefer_uuid_keys'."
        raise ImproperConfigurationError(msg)


def _validate_native_storage_autocommit(connection_config: "dict[str, Any]", driver_features: "dict[str, Any]") -> None:
    if not driver_features.get("enable_native_storage"):
        return
    autocommit = connection_config.get("autocommit")
    if autocommit is None:
        autocommit = connection_config.get("kwargs", {}).get("autocommit")
    if autocommit is not True:
        msg = "enable_native_storage requires connection_config autocommit=True."
        raise ImproperConfigurationError(msg)


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
