"""SQLSpec extension for Starlette applications."""

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Union, overload

from sqlspec.base import SQLSpec as SQLSpecBase
from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, DriverT, SyncConfigT
from sqlspec.extensions.starlette.config import DatabaseConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from starlette.applications import Starlette

    from sqlspec.typing import ConnectionT, PoolT

logger = get_logger("extensions.starlette")

__all__ = ("SQLSpec",)


class SQLSpec(SQLSpecBase):
    """SQLSpec integration for Starlette applications."""

    __slots__ = ("_app", "_configs")

    def __init__(self, config: Union["SyncConfigT", "AsyncConfigT", "DatabaseConfig", list["DatabaseConfig"]]) -> None:
        """Initialize SQLSpec for Starlette.

        Args:
            config: Database configuration(s) for SQLSpec.
        """
        super().__init__()
        self._app: Union[Starlette, None] = None

        if isinstance(config, DatabaseConfigProtocol):
            self._configs: list[DatabaseConfig] = [DatabaseConfig(config=config)]
        elif isinstance(config, DatabaseConfig):
            self._configs = [config]
        else:
            self._configs = config

    @property
    def config(self) -> list[DatabaseConfig]:
        """Return the database configurations.

        Returns:
            List of database configurations.
        """
        return self._configs

    def init_app(self, app: "Starlette") -> None:
        """Initialize SQLSpec with Starlette application.

        Args:
            app: The Starlette application instance.
        """
        self._app = app

        # Initialize each database configuration
        for db_config in self._configs:
            # Add the configuration to SQLSpec base
            annotation = self.add_config(db_config.config)
            db_config.annotation = annotation  # type: ignore[attr-defined]

            # Initialize with the app
            db_config.init_app(app)

    @overload
    def provide_session(self, config: "SyncConfigT") -> "AsyncGenerator[DriverT, None]": ...

    @overload
    def provide_session(self, config: "AsyncConfigT") -> "AsyncGenerator[DriverT, None]": ...

    @overload
    def provide_session(self, config: "type[Union[SyncConfigT, AsyncConfigT]]") -> "AsyncGenerator[DriverT, None]": ...

    @asynccontextmanager
    async def provide_session(
        self, config: Union["SyncConfigT", "AsyncConfigT", "type[Union[SyncConfigT, AsyncConfigT]]"]
    ) -> "AsyncGenerator[DriverT, None]":
        """Provide a database session for the given configuration.

        Args:
            config: Configuration instance or type to get session for.

        Yields:
            Database driver/session instance.

        Raises:
            KeyError: If configuration is not found.
        """
        # Find the database config for this configuration
        db_config = None
        for cfg in self._configs:
            if config in (cfg.config, cfg.annotation):  # type: ignore[attr-defined]
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        # Get connection and create session
        if db_config.connection_provider:
            async with db_config.connection_provider() as connection:
                if db_config.session_provider:
                    async with db_config.session_provider(connection) as session:
                        yield session
                else:
                    # Fallback: create driver directly
                    yield db_config.config.driver_type(connection=connection)  # type: ignore[attr-defined]
        else:
            # Fallback: use base class session management
            async with super().provide_session(config) as session:
                yield session

    @overload
    def provide_connection(self, config: "SyncConfigT") -> "AsyncGenerator[ConnectionT, None]": ...

    @overload
    def provide_connection(self, config: "AsyncConfigT") -> "AsyncGenerator[ConnectionT, None]": ...

    @overload
    def provide_connection(
        self, config: "type[Union[SyncConfigT, AsyncConfigT]]"
    ) -> "AsyncGenerator[ConnectionT, None]": ...

    @asynccontextmanager
    async def provide_connection(
        self, config: Union["SyncConfigT", "AsyncConfigT", "type[Union[SyncConfigT, AsyncConfigT]]"]
    ) -> "AsyncGenerator[ConnectionT, None]":
        """Provide a database connection for the given configuration.

        Args:
            config: Configuration instance or type to get connection for.

        Yields:
            Database connection instance.

        Raises:
            KeyError: If configuration is not found.
        """
        # Find the database config for this configuration
        db_config = None
        for cfg in self._configs:
            if config in (cfg.config, cfg.annotation):  # type: ignore[attr-defined]
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        # Get connection
        if db_config.connection_provider:
            async with db_config.connection_provider() as connection:
                yield connection
        else:
            # Fallback: use base class connection management
            async with super().provide_connection(config) as connection:
                yield connection

    @overload
    def provide_pool(self, config: "SyncConfigT") -> "PoolT": ...

    @overload
    def provide_pool(self, config: "AsyncConfigT") -> "PoolT": ...

    @overload
    def provide_pool(self, config: "type[Union[SyncConfigT, AsyncConfigT]]") -> "PoolT": ...

    async def provide_pool(
        self, config: Union["SyncConfigT", "AsyncConfigT", "type[Union[SyncConfigT, AsyncConfigT]]"]
    ) -> "PoolT":
        """Provide a database pool for the given configuration.

        Args:
            config: Configuration instance or type to get pool for.

        Returns:
            Database connection pool.

        Raises:
            KeyError: If configuration is not found.
        """
        # Find the database config for this configuration
        db_config = None
        for cfg in self._configs:
            if config in (cfg.config, cfg.annotation):  # type: ignore[attr-defined]
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {config}"
            raise KeyError(msg)

        # Get pool
        if db_config.pool_provider:
            return await db_config.pool_provider()
        # Fallback: create pool directly
        from sqlspec.utils.sync_tools import ensure_async_

        return await ensure_async_(db_config.config.create_pool)()

    def get_annotation(
        self, key: Union[str, "SyncConfigT", "AsyncConfigT", "type[Union[SyncConfigT, AsyncConfigT]]"]
    ) -> "type[Union[SyncConfigT, AsyncConfigT]]":
        """Return the annotation for the given configuration.

        Args:
            key: The configuration instance, type, or key to lookup.

        Returns:
            The annotation for the configuration.

        Raises:
            KeyError: If no configuration is found for the given key.
        """
        for cfg in self._configs:
            if key in (cfg.config, cfg.annotation, cfg.connection_key, cfg.pool_key):
                return cfg.annotation  # type: ignore[attr-defined]
        msg = f"No configuration found for {key}"
        raise KeyError(msg)
