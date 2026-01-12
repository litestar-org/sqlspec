"""CockroachDB AsyncPG configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from asyncpg import create_pool as asyncpg_create_pool
from typing_extensions import NotRequired

from sqlspec.adapters.asyncpg.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.cockroach_asyncpg._typing import (
    CockroachAsyncpgConnection,
    CockroachAsyncpgPool,
    CockroachAsyncpgSessionContext,
)
from sqlspec.adapters.cockroach_asyncpg.core import CockroachAsyncpgRetryConfig
from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver, CockroachAsyncpgExceptionHandler
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config, reject_pool_aliases

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "CockroachAsyncpgConfig",
    "CockroachAsyncpgConnectionConfig",
    "CockroachAsyncpgDriverFeatures",
    "CockroachAsyncpgPoolConfig",
)


class CockroachAsyncpgConnectionConfig(TypedDict):
    """AsyncPG connection parameters for CockroachDB."""

    dsn: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    ssl: NotRequired[Any]
    passfile: NotRequired[str]
    direct_tls: NotRequired[bool]
    connect_timeout: NotRequired[float]
    command_timeout: NotRequired[float]
    statement_cache_size: NotRequired[int]
    max_cached_statement_lifetime: NotRequired[int]
    max_cacheable_statement_size: NotRequired[int]
    server_settings: NotRequired["dict[str, str]"]


class CockroachAsyncpgPoolConfig(CockroachAsyncpgConnectionConfig):
    """AsyncPG pool parameters for CockroachDB."""

    min_size: NotRequired[int]
    max_size: NotRequired[int]
    max_queries: NotRequired[int]
    max_inactive_connection_lifetime: NotRequired[float]
    setup: NotRequired["Callable[[CockroachAsyncpgConnection], Awaitable[None]]"]
    init: NotRequired["Callable[[CockroachAsyncpgConnection], Awaitable[None]]"]
    extra: NotRequired["dict[str, Any]"]


class CockroachAsyncpgDriverFeatures(TypedDict):
    """Driver feature flags for CockroachDB AsyncPG adapter."""

    enable_auto_retry: NotRequired[bool]
    max_retries: NotRequired[int]
    retry_delay_base_ms: NotRequired[float]
    retry_delay_max_ms: NotRequired[float]
    enable_retry_logging: NotRequired[bool]
    enable_follower_reads: NotRequired[bool]
    default_staleness: NotRequired[str]
    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    enable_json_codecs: NotRequired[bool]
    enable_pgvector: NotRequired[bool]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class _CockroachAsyncpgSessionFactory:
    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "CockroachAsyncpgConfig") -> None:
        self._config = config
        self._ctx: Any | None = None

    async def acquire_connection(self) -> "CockroachAsyncpgConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.acquire()
        self._ctx = ctx
        return cast("CockroachAsyncpgConnection", await ctx.__aenter__())

    async def release_connection(self, _conn: "CockroachAsyncpgConnection") -> None:
        if self._ctx is not None:
            await self._ctx.__aexit__(None, None, None)
            self._ctx = None


class CockroachAsyncpgConnectionContext:
    """Async context manager for CockroachDB AsyncPG connections."""

    __slots__ = ("_config", "_connection")

    def __init__(self, config: "CockroachAsyncpgConfig") -> None:
        self._config = config
        self._connection: CockroachAsyncpgConnection | None = None

    async def __aenter__(self) -> "CockroachAsyncpgConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        self._connection = await pool.acquire()
        return self._connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._connection is not None:
            if self._config.connection_instance:
                await self._config.connection_instance.release(self._connection)  # type: ignore[arg-type]
            self._connection = None
        return None


class CockroachAsyncpgConfig(
    AsyncDatabaseConfig[CockroachAsyncpgConnection, CockroachAsyncpgPool, CockroachAsyncpgDriver]
):
    """Configuration for CockroachDB using AsyncPG."""

    driver_type: "ClassVar[type[CockroachAsyncpgDriver]]" = CockroachAsyncpgDriver
    connection_type: "ClassVar[type[CockroachAsyncpgConnection]]" = CockroachAsyncpgConnection  # type: ignore[assignment]
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

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
        reject_pool_aliases(kwargs)

        connection_config = normalize_connection_config(connection_config)
        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        driver_features.setdefault("enable_auto_retry", True)
        _ = CockroachAsyncpgRetryConfig.from_features(driver_features)

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    async def _create_pool(self) -> "CockroachAsyncpgPool":
        config = build_connection_config(self.connection_config)
        return await asyncpg_create_pool(**config)

    async def _close_pool(self) -> None:
        if not self.connection_instance:
            return
        await self.connection_instance.close()
        self.connection_instance = None

    async def create_connection(self) -> "CockroachAsyncpgConnection":
        if self.connection_instance is None:
            self.connection_instance = await self.create_pool()
        return cast("CockroachAsyncpgConnection", await self.connection_instance.acquire())

    def provide_connection(self, *args: Any, **kwargs: Any) -> "CockroachAsyncpgConnectionContext":
        return CockroachAsyncpgConnectionContext(self)

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
            driver_features["default_staleness"] = staleness

        return CockroachAsyncpgSessionContext(
            acquire_connection=factory.acquire_connection,
            release_connection=factory.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
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
            "CockroachAsyncpgConnectionConfig": CockroachAsyncpgConnectionConfig,
            "CockroachAsyncpgPoolConfig": CockroachAsyncpgPoolConfig,
            "CockroachAsyncpgDriver": CockroachAsyncpgDriver,
            "CockroachAsyncpgExceptionHandler": CockroachAsyncpgExceptionHandler,
            "CockroachAsyncpgSessionContext": CockroachAsyncpgSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)
