"""Psqlpy database configuration."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from psqlpy import ConnectionPool
from typing_extensions import NotRequired

from sqlspec.adapters.psqlpy._types import PsqlpyConnection
from sqlspec.adapters.psqlpy.driver import (
    PsqlpyCursor,
    PsqlpyDriver,
    PsqlpyExceptionHandler,
    build_psqlpy_statement_config,
)
from sqlspec.config import (
    ADKConfig,
    AsyncDatabaseConfig,
    FastAPIConfig,
    FlaskConfig,
    LitestarConfig,
    OpenTelemetryConfig,
    PrometheusConfig,
    StarletteConfig,
)
from sqlspec.core import StatementConfig
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable


logger = logging.getLogger("sqlspec.adapters.psqlpy")


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
    """

    enable_pgvector: NotRequired[bool]
    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]


__all__ = ("PsqlpyConfig", "PsqlpyConnectionParams", "PsqlpyCursor", "PsqlpyDriverFeatures", "PsqlpyPoolParams")


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
        pool_config: PsqlpyPoolParams | dict[str, Any] | None = None,
        pool_instance: ConnectionPool | None = None,
        migration_config: dict[str, Any] | None = None,
        statement_config: StatementConfig | None = None,
        driver_features: "PsqlpyDriverFeatures | dict[str, Any] | None" = None,
        bind_key: str | None = None,
        extension_config: "dict[str, dict[str, Any] | LitestarConfig | FastAPIConfig | StarletteConfig | FlaskConfig | ADKConfig | OpenTelemetryConfig | PrometheusConfig] | None" = None,
    ) -> None:
        """Initialize Psqlpy configuration.

        Args:
            pool_config: Pool configuration parameters.
            pool_instance: Existing connection pool instance to use.
            migration_config: Migration configuration.
            statement_config: SQL statement configuration.
            driver_features: Driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings).
        """
        processed_pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        if "extra" in processed_pool_config:
            extras = processed_pool_config.pop("extra")
            processed_pool_config.update(extras)

        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        serializer = processed_driver_features.get("json_serializer")
        serializer_callable = to_json if serializer is None else cast("Callable[[Any], str]", serializer)
        processed_driver_features.setdefault("json_serializer", serializer_callable)
        processed_driver_features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)

        super().__init__(
            pool_config=processed_pool_config,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or build_psqlpy_statement_config(json_serializer=serializer_callable),
            driver_features=processed_driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
        )

    def _get_pool_config_dict(self) -> dict[str, Any]:
        """Get pool configuration as plain dict for external library.

        Returns:
            Dictionary with pool parameters, filtering out None values.
        """
        return {k: v for k, v in self.pool_config.items() if v is not None}

    async def _create_pool(self) -> "ConnectionPool":
        """Create the actual async connection pool."""
        logger.info("Creating psqlpy connection pool", extra={"adapter": "psqlpy"})

        try:
            config = self._get_pool_config_dict()

            pool = ConnectionPool(**config)
            logger.info("Psqlpy connection pool created successfully", extra={"adapter": "psqlpy"})
        except Exception as e:
            logger.exception("Failed to create psqlpy connection pool", extra={"adapter": "psqlpy", "error": str(e)})
            raise
        return pool

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if not self.pool_instance:
            return

        logger.info("Closing psqlpy connection pool", extra={"adapter": "psqlpy"})

        try:
            self.pool_instance.close()
            logger.info("Psqlpy connection pool closed successfully", extra={"adapter": "psqlpy"})
        except Exception as e:
            logger.exception("Failed to close psqlpy connection pool", extra={"adapter": "psqlpy", "error": str(e)})
            raise

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> "PsqlpyConnection":
        """Create a single async connection (not from pool).

        Returns:
            A psqlpy Connection instance.
        """
        if not self.pool_instance:
            self.pool_instance = await self._create_pool()

        return await self.pool_instance.connection()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncGenerator[PsqlpyConnection, None]:
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A psqlpy Connection instance.
        """
        if not self.pool_instance:
            self.pool_instance = await self._create_pool()

        async with self.pool_instance.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> AsyncGenerator[PsqlpyDriver, None]:
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsqlpyDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as conn:
            driver = self.driver_type(
                connection=conn,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
            )
            yield self._prepare_driver(driver)

    async def provide_pool(self, *args: Any, **kwargs: Any) -> ConnectionPool:
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for Psqlpy types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "PsqlpyConnection": PsqlpyConnection,
            "PsqlpyConnectionParams": PsqlpyConnectionParams,
            "PsqlpyCursor": PsqlpyCursor,
            "PsqlpyDriver": PsqlpyDriver,
            "PsqlpyExceptionHandler": PsqlpyExceptionHandler,
            "PsqlpyPoolParams": PsqlpyPoolParams,
        })
        return namespace
