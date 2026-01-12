"""CockroachDB configuration using psycopg."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from psycopg import crdb as psycopg_crdb
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, ConnectionPool
from typing_extensions import NotRequired

from sqlspec.adapters.cockroach_psycopg._typing import (
    CockroachAsyncConnection,
    CockroachPsycopgAsyncSessionContext,
    CockroachPsycopgSyncSessionContext,
    CockroachSyncConnection,
)
from sqlspec.adapters.cockroach_psycopg.core import (
    CockroachPsycopgRetryConfig,
    apply_driver_features,
    build_statement_config,
)
from sqlspec.adapters.cockroach_psycopg.driver import (
    CockroachPsycopgAsyncDriver,
    CockroachPsycopgAsyncExceptionHandler,
    CockroachPsycopgSyncDriver,
    CockroachPsycopgSyncExceptionHandler,
)
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import EventRuntimeHints
from sqlspec.utils.config_tools import normalize_connection_config, reject_pool_aliases

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = (
    "CockroachPsycopgAsyncConfig",
    "CockroachPsycopgConnectionConfig",
    "CockroachPsycopgDriverFeatures",
    "CockroachPsycopgPoolConfig",
    "CockroachPsycopgSyncConfig",
)


class CockroachPsycopgConnectionConfig(TypedDict):
    """CockroachDB connection parameters."""

    conninfo: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    user: NotRequired[str]
    password: NotRequired[str]
    dbname: NotRequired[str]
    connect_timeout: NotRequired[int]
    options: NotRequired[str]
    application_name: NotRequired[str]
    sslmode: NotRequired[str]
    sslcert: NotRequired[str]
    sslkey: NotRequired[str]
    sslrootcert: NotRequired[str]
    autocommit: NotRequired[bool]
    cluster: NotRequired[str]
    extra: NotRequired["dict[str, Any]"]


class CockroachPsycopgPoolConfig(CockroachPsycopgConnectionConfig):
    """CockroachDB pool parameters."""

    min_size: NotRequired[int]
    max_size: NotRequired[int]
    name: NotRequired[str]
    timeout: NotRequired[float]
    max_waiting: NotRequired[int]
    max_lifetime: NotRequired[float]
    max_idle: NotRequired[float]
    reconnect_timeout: NotRequired[float]
    num_workers: NotRequired[int]
    configure: NotRequired["Callable[..., Any]"]
    kwargs: NotRequired["dict[str, Any]"]


class CockroachPsycopgDriverFeatures(TypedDict):
    """CockroachDB driver feature configuration."""

    enable_auto_retry: NotRequired[bool]
    max_retries: NotRequired[int]
    retry_delay_base_ms: NotRequired[float]
    retry_delay_max_ms: NotRequired[float]
    enable_retry_logging: NotRequired[bool]
    enable_follower_reads: NotRequired[bool]
    default_staleness: NotRequired[str]
    prefer_uuid_keys: NotRequired[bool]
    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class CockroachPsycopgSyncConnectionContext:
    """Context manager for CockroachDB psycopg connections."""

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "CockroachPsycopgSyncConfig") -> None:
        self._config = config
        self._ctx: Any = None

    def __enter__(self) -> "CockroachSyncConnection":
        if self._config.connection_instance:
            self._ctx = self._config.connection_instance.connection()
            return cast("CockroachSyncConnection", self._ctx.__enter__())
        self._ctx = self._config.create_connection()
        return cast("CockroachSyncConnection", self._ctx)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._config.connection_instance and self._ctx:
            return cast("bool | None", self._ctx.__exit__(exc_type, exc_val, exc_tb))
        if self._ctx:
            self._ctx.close()
        return None


class _CockroachPsycopgSyncSessionConnectionHandler:
    __slots__ = ("_config", "_conn", "_ctx")

    def __init__(self, config: "CockroachPsycopgSyncConfig") -> None:
        self._config = config
        self._ctx: Any = None
        self._conn: CockroachSyncConnection | None = None

    def acquire_connection(self) -> "CockroachSyncConnection":
        if self._config.connection_instance:
            self._ctx = self._config.connection_instance.connection()
            return cast("CockroachSyncConnection", self._ctx.__enter__())
        self._conn = self._config.create_connection()
        return self._conn

    def release_connection(self, _conn: "CockroachSyncConnection") -> None:
        if self._ctx is not None:
            self._ctx.__exit__(None, None, None)
            self._ctx = None
            return
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class CockroachPsycopgSyncConfig(
    SyncDatabaseConfig[CockroachSyncConnection, ConnectionPool, CockroachPsycopgSyncDriver]
):
    """Configuration for CockroachDB synchronous connections using psycopg."""

    driver_type: "ClassVar[type[CockroachPsycopgSyncDriver]]" = CockroachPsycopgSyncDriver
    connection_type: "ClassVar[type[CockroachSyncConnection]]" = CockroachSyncConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

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
        reject_pool_aliases(kwargs)

        connection_config = normalize_connection_config(connection_config)
        statement_config = statement_config or build_statement_config()
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        driver_features.setdefault("enable_auto_retry", True)
        _ = CockroachPsycopgRetryConfig.from_features(driver_features)

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
            "num_workers": all_config.pop("num_workers", 3),
        }

        pool_parameters["configure"] = all_config.pop("configure", self._configure_connection)
        pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

        conninfo = all_config.pop("conninfo", None)
        if conninfo:
            return ConnectionPool(conninfo, open=True, connection_class=psycopg_crdb.CrdbConnection, **pool_parameters)

        kwargs = all_config.pop("kwargs", {})
        all_config.update(kwargs)
        return ConnectionPool(
            "", kwargs=all_config, open=True, connection_class=psycopg_crdb.CrdbConnection, **pool_parameters
        )

    def _configure_connection(self, conn: "CockroachSyncConnection") -> None:
        conn.row_factory = dict_row
        autocommit_setting = self.connection_config.get("autocommit")
        if autocommit_setting is not None:
            conn.autocommit = autocommit_setting

    def _close_pool(self) -> None:
        if not self.connection_instance:
            return
        try:
            self.connection_instance.close()
        finally:
            self.connection_instance = None

    def create_connection(self) -> "CockroachSyncConnection":
        if self.connection_instance is None:
            self.connection_instance = self.create_pool()
        return cast("CockroachSyncConnection", self.connection_instance.getconn())

    def provide_connection(self, *args: Any, **kwargs: Any) -> "CockroachPsycopgSyncConnectionContext":
        return CockroachPsycopgSyncConnectionContext(self)

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
            driver_features["default_staleness"] = staleness

        return CockroachPsycopgSyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or build_statement_config(),
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
            "CockroachPsycopgPoolConfig": CockroachPsycopgPoolConfig,
            "CockroachSyncConnection": CockroachSyncConnection,
            "CockroachPsycopgSyncDriver": CockroachPsycopgSyncDriver,
            "CockroachPsycopgSyncExceptionHandler": CockroachPsycopgSyncExceptionHandler,
            "CockroachPsycopgSyncSessionContext": CockroachPsycopgSyncSessionContext,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)


class CockroachPsycopgAsyncConnectionContext:
    """Async context manager for CockroachDB psycopg connections."""

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
        self._config = config
        self._ctx: Any = None

    async def __aenter__(self) -> "CockroachAsyncConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = await self._config.create_pool()
        if self._config.connection_instance:
            self._ctx = self._config.connection_instance.connection()
            return cast("CockroachAsyncConnection", await self._ctx.__aenter__())
        msg = "Connection pool is not initialized"
        raise ImproperConfigurationError(msg)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._ctx:
            return cast("bool | None", await self._ctx.__aexit__(exc_type, exc_val, exc_tb))
        return None


class _CockroachPsycopgAsyncSessionConnectionHandler:
    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "CockroachPsycopgAsyncConfig") -> None:
        self._config = config
        self._ctx: Any = None

    async def acquire_connection(self) -> "CockroachAsyncConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        ctx = pool.connection()
        self._ctx = ctx
        return cast("CockroachAsyncConnection", await ctx.__aenter__())

    async def release_connection(self, _conn: "CockroachAsyncConnection") -> None:
        if self._ctx is not None:
            await self._ctx.__aexit__(None, None, None)
            self._ctx = None


class CockroachPsycopgAsyncConfig(
    AsyncDatabaseConfig[CockroachAsyncConnection, AsyncConnectionPool, CockroachPsycopgAsyncDriver]
):
    """Configuration for CockroachDB async connections using psycopg."""

    driver_type: "ClassVar[type[CockroachPsycopgAsyncDriver]]" = CockroachPsycopgAsyncDriver
    connection_type: "ClassVar[type[CockroachAsyncConnection]]" = CockroachAsyncConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

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
        reject_pool_aliases(kwargs)

        connection_config = normalize_connection_config(connection_config)
        statement_config = statement_config or build_statement_config()
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        driver_features.setdefault("enable_auto_retry", True)
        _ = CockroachPsycopgRetryConfig.from_features(driver_features)

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
            "num_workers": all_config.pop("num_workers", 3),
        }

        pool_parameters["configure"] = all_config.pop("configure", self._configure_async_connection)
        pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

        conninfo = all_config.pop("conninfo", None)
        if conninfo:
            pool = AsyncConnectionPool(
                conninfo, open=False, connection_class=psycopg_crdb.AsyncCrdbConnection, **pool_parameters
            )
        else:
            kwargs = all_config.pop("kwargs", {})
            all_config.update(kwargs)
            pool = AsyncConnectionPool(
                "", kwargs=all_config, open=False, connection_class=psycopg_crdb.AsyncCrdbConnection, **pool_parameters
            )

        await pool.open()
        return cast("AsyncConnectionPool", pool)

    async def _configure_async_connection(self, conn: "CockroachAsyncConnection") -> None:
        conn.row_factory = dict_row
        autocommit_setting = self.connection_config.get("autocommit")
        if autocommit_setting is not None:
            await conn.set_autocommit(autocommit_setting)

    async def _close_pool(self) -> None:
        if not self.connection_instance:
            return
        try:
            await self.connection_instance.close()
        finally:
            self.connection_instance = None

    async def create_connection(self) -> "CockroachAsyncConnection":
        if self.connection_instance is None:
            self.connection_instance = await self.create_pool()
        return cast("CockroachAsyncConnection", await self.connection_instance.getconn())

    def provide_connection(self, *args: Any, **kwargs: Any) -> "CockroachPsycopgAsyncConnectionContext":
        return CockroachPsycopgAsyncConnectionContext(self)

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
            driver_features["default_staleness"] = staleness

        return CockroachPsycopgAsyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or build_statement_config(),
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
            "CockroachPsycopgAsyncDriver": CockroachPsycopgAsyncDriver,
            "CockroachPsycopgAsyncExceptionHandler": CockroachPsycopgAsyncExceptionHandler,
            "CockroachPsycopgAsyncSessionContext": CockroachPsycopgAsyncSessionContext,
            "CockroachPsycopgConnectionConfig": CockroachPsycopgConnectionConfig,
            "CockroachPsycopgPoolConfig": CockroachPsycopgPoolConfig,
        })
        return namespace

    def get_event_runtime_hints(self) -> "EventRuntimeHints":
        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True)
