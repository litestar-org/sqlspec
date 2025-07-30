"""Psqlpy database configuration with direct field-based configuration."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union

from psqlpy import Connection, ConnectionPool
from typing_extensions import NotRequired

from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.config import AsyncDatabaseConfig
from sqlspec.statement.sql import StatementConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    from typing_extensions import TypeAlias

    from sqlspec.adapters.psqlpy.driver import PsqlpyCursor, PsqlpyDriver

    PsqlpyConnection: TypeAlias = Connection
else:
    from psqlpy import Connection as PsqlpyConnection

    from sqlspec.adapters.psqlpy.driver import PsqlpyCursor, PsqlpyDriver


logger = logging.getLogger("sqlspec.adapters.psqlpy")


class PsqlpyConnectionParams(TypedDict, total=False):
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


class PsqlpyPoolParams(PsqlpyConnectionParams, total=False):
    """Psqlpy pool parameters."""

    hosts: NotRequired[list[str]]
    ports: NotRequired[list[int]]
    conn_recycling_method: NotRequired[str]
    max_db_pool_size: NotRequired[int]
    configure: NotRequired["Callable[..., Any]"]
    extra: NotRequired[dict[str, Any]]


__all__ = ("PsqlpyConfig", "PsqlpyConnectionParams", "PsqlpyCursor", "PsqlpyPoolParams")


class PsqlpyConfig(AsyncDatabaseConfig[PsqlpyConnection, ConnectionPool, PsqlpyDriver]):
    """Configuration for Psqlpy asynchronous database connections with direct field-based configuration."""

    driver_type: ClassVar[type[PsqlpyDriver]] = PsqlpyDriver
    connection_type: "ClassVar[type[PsqlpyConnection]]" = PsqlpyConnection
    # Parameter style support information
    supported_parameter_styles: ClassVar[tuple[str, ...]] = ("numeric",)
    """Psqlpy only supports $1, $2, ... (numeric) parameter style."""

    default_parameter_style: ClassVar[str] = "numeric"

    def __init__(
        self,
        *,
        pool_config: Optional[Union[PsqlpyPoolParams, dict[str, Any]]] = None,
        statement_config: Optional[StatementConfig] = None,
        pool_instance: Optional[ConnectionPool] = None,
        migration_config: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize Psqlpy asynchronous configuration.

        Args:
            pool_config: Pool configuration parameters (TypedDict or dict)
            pool_instance: Existing connection pool instance to use
            statement_config: Default SQL statement configuration
            migration_config: Migration configuration
        """
        # Store pool config as dict and extract/merge extras
        self.pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        if "extra" in self.pool_config:
            extras = self.pool_config.pop("extra")
            self.pool_config.update(extras)

        super().__init__(pool_instance=pool_instance, migration_config=migration_config)

        # Override parent's empty StatementConfig with Psqlpy-specific configuration
        if statement_config is None:
            from sqlspec.parameters import ParameterStyle

            self.statement_config = StatementConfig(
                supported_parameter_styles=(ParameterStyle.NUMERIC.value,),
                default_parameter_style=ParameterStyle.NUMERIC.value,
                type_coercion_map={
                    # Psqlpy handles most PostgreSQL types natively
                    dict: lambda v: v,  # Psqlpy handles JSON natively
                    list: lambda v: v,  # Psqlpy handles arrays natively
                },
                has_native_list_expansion=True,
            )
        else:
            self.statement_config = statement_config

    def _get_pool_config_dict(self) -> dict[str, Any]:
        """Get pool configuration as plain dict for external library.

        Returns:
            Dictionary with pool parameters, filtering out None values.
        """
        # Filter out None values since external libraries may not handle them
        return {k: v for k, v in self.pool_config.items() if v is not None}

    async def _create_pool(self) -> "ConnectionPool":
        """Create the actual async connection pool."""
        logger.info("Creating psqlpy connection pool", extra={"adapter": "psqlpy"})

        try:
            # Get properly typed configuration dictionary
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
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
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
            yield self.driver_type(connection=conn, statement_config=statement_config)

    async def provide_pool(self, *args: Any, **kwargs: Any) -> ConnectionPool:
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for Psqlpy types.

        This provides all Psqlpy-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({"PsqlpyConnection": PsqlpyConnection, "PsqlpyCursor": PsqlpyCursor})
        return namespace
