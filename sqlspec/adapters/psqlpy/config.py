"""Psqlpy database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from mypy_extensions import mypyc_attr
from psqlpy import ConnectionPool
from typing_extensions import NotRequired

from sqlspec.adapters.psqlpy._typing import PsqlpyConnection
from sqlspec.adapters.psqlpy.driver import (
    PsqlpyCursor,
    PsqlpyDriver,
    PsqlpyExceptionHandler,
    PsqlpySessionContext,
    build_psqlpy_statement_config,
    psqlpy_statement_config,
)
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.core import StatementConfig
from sqlspec.extensions.events._hints import EventRuntimeHints
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.config_normalization import apply_pool_deprecations, normalize_connection_config
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable


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
    configure: NotRequired["Callable[..., Any]"]
    extra: NotRequired[dict[str, Any]]


class PsqlpyDriverFeatures(TypedDict):
    """Psqlpy driver feature flags.

    enable_pgvector: Enable automatic pgvector extension support for vector similarity search.
        Requires pgvector-python package installed.
        Defaults to True when pgvector is installed.
        Provides automatic conversion between NumPy arrays and PostgreSQL vector types.
    json_serializer: Custom JSON serializer applied to the statement configuration.
    json_deserializer: Custom JSON deserializer retained alongside the serializer for parity with asyncpg.
    enable_events: Enable database event channel support.
        Defaults to True when extension_config["events"] is configured.
        Provides pub/sub capabilities via LISTEN/NOTIFY or table-backed fallback.
        Requires extension_config["events"] for migration setup when using table_queue backend.
    events_backend: Event channel backend selection.
        Options: "listen_notify", "table_queue", "listen_notify_durable"
        - "listen_notify": Zero-copy PostgreSQL LISTEN/NOTIFY (ephemeral, real-time) - coming soon
        - "table_queue": Durable table-backed queue with retries and exactly-once delivery (current default)
        - "listen_notify_durable": Hybrid - real-time + durable (available when native support lands)
        Defaults to "table_queue" until native LISTEN/NOTIFY support is implemented.
    """

    enable_pgvector: NotRequired[bool]
    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]


__all__ = ("PsqlpyConfig", "PsqlpyConnectionParams", "PsqlpyCursor", "PsqlpyDriverFeatures", "PsqlpyPoolParams")


class PsqlpyConnectionContext:
    """Async context manager for Psqlpy connections."""

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "PsqlpyConfig") -> None:
        self._config = config
        self._ctx: Any = None

    async def __aenter__(self) -> PsqlpyConnection:
        if self._config.connection_instance is None:
            self._config.connection_instance = await self._config._create_pool()  # pyright: ignore[reportPrivateUsage]

        self._ctx = self._config.connection_instance.acquire()
        return await self._ctx.__aenter__()  # type: ignore[no-any-return]

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._ctx:
            return await self._ctx.__aexit__(exc_type, exc_val, exc_tb)  # type: ignore[no-any-return]
        return None


@mypyc_attr(native_class=False)
class PsqlpyConfig(AsyncDatabaseConfig[PsqlpyConnection, ConnectionPool, PsqlpyDriver]):
    """Configuration for Psqlpy asynchronous database connections."""

    driver_type: ClassVar[type[PsqlpyDriver]] = PsqlpyDriver
    connection_type: "ClassVar[type[PsqlpyConnection]]" = PsqlpyConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True

    def __init__(
        self,
        *,
        connection_config: PsqlpyPoolParams | dict[str, Any] | None = None,
        connection_instance: ConnectionPool | None = None,
        migration_config: dict[str, Any] | None = None,
        statement_config: StatementConfig | None = None,
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
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings).
            **kwargs: Additional keyword arguments (handles deprecated pool_config/pool_instance).
        """
        connection_config, connection_instance = apply_pool_deprecations(
            kwargs=kwargs, connection_config=connection_config, connection_instance=connection_instance
        )

        processed_connection_config = normalize_connection_config(connection_config)

        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        serializer = processed_driver_features.get("json_serializer")
        serializer_callable = to_json if serializer is None else cast("Callable[[Any], str]", serializer)
        processed_driver_features.setdefault("json_serializer", serializer_callable)
        processed_driver_features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)

        super().__init__(
            connection_config=processed_connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config or build_psqlpy_statement_config(json_serializer=serializer_callable),
            driver_features=processed_driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            **kwargs,
        )

    def _get_pool_config_dict(self) -> dict[str, Any]:
        """Get pool configuration as plain dict for external library.

        Returns:
            Dictionary with pool parameters, filtering out None values.
        """
        return {k: v for k, v in self.connection_config.items() if v is not None}

    async def _create_pool(self) -> "ConnectionPool":
        """Create the actual async connection pool."""
        config = self._get_pool_config_dict()
        return ConnectionPool(**config)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if not self.connection_instance:
            return

        self.connection_instance.close()

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> "PsqlpyConnection":
        """Create a single async connection (not from pool).

        Returns:
            A psqlpy Connection instance.
        """
        if not self.connection_instance:
            self.connection_instance = await self._create_pool()

        return await self.connection_instance.connection()

    def provide_connection(self, *args: Any, **kwargs: Any) -> "PsqlpyConnectionContext":
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            A psqlpy Connection context manager.
        """
        return PsqlpyConnectionContext(self)

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
        acquire_ctx_holder: dict[str, Any] = {}

        async def acquire_connection() -> PsqlpyConnection:
            if self.connection_instance is None:
                self.connection_instance = await self._create_pool()
            ctx = self.connection_instance.acquire()
            acquire_ctx_holder["ctx"] = ctx
            return await ctx.__aenter__()

        async def release_connection(_conn: PsqlpyConnection) -> None:
            if "ctx" in acquire_ctx_holder:
                await acquire_ctx_holder["ctx"].__aexit__(None, None, None)
                acquire_ctx_holder.clear()

        return PsqlpySessionContext(
            acquire_connection=acquire_connection,
            release_connection=release_connection,
            statement_config=statement_config or self.statement_config or psqlpy_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> ConnectionPool:
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

        return EventRuntimeHints(poll_interval=0.5, select_for_update=True, skip_locked=True, json_passthrough=True)
