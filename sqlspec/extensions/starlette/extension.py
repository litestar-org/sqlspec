"""SQLSpec extension for Starlette applications."""

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from sqlspec.base import SQLSpec as SQLSpecBase
from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, DriverT, SyncConfigT
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.starlette.config import DatabaseConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from starlette.applications import Starlette
    from starlette.requests import Request

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.loader import SQLFileLoader
    from sqlspec.typing import ConnectionT, PoolT

logger = get_logger("extensions.starlette")

__all__ = ("SQLSpec",)


class SQLSpec(SQLSpecBase):
    """SQLSpec integration for Starlette applications."""

    __slots__ = ("_app", "_configs")

    def __init__(
        self,
        config: Union["SyncConfigT", "AsyncConfigT", "DatabaseConfig", list["DatabaseConfig"]],
        *,
        loader: "Optional[SQLFileLoader]" = None,
    ) -> None:
        """Initialize SQLSpec for Starlette.

        Args:
            config: Database configuration(s) for SQLSpec.
            loader: Optional SQL file loader instance.
        """
        super().__init__(loader=loader)
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

        self._validate_dependency_keys()

        # Store SQLSpec instance in app state for providers
        app.state.sqlspec = self

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
        for cfg in self.config:
            if key in (cfg.config, cfg.annotation, cfg.connection_key, cfg.pool_key):
                return cfg.annotation  # type: ignore[attr-defined]
        msg = f"No configuration found for {key}. Available keys: {self._get_available_keys()}"
        raise KeyError(msg)

    def provide_request_session(
        self,
        key: Union[str, "SyncConfigT", "AsyncConfigT", "type[Union[SyncConfigT, AsyncConfigT]]"],
        request: "Request",
    ) -> "Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]":
        """Provide a database session for the specified configuration key from request scope.

        This is a convenience method that combines get_config and get_request_session
        into a single call, similar to Advanced Alchemy's provide_session pattern.

        Args:
            key: The configuration identifier (same as get_config)
            request: The Starlette Request object

        Returns:
            A driver session instance for the specified database configuration

        Example:
            >>> sqlspec_plugin = request.app.state.sqlspec
            >>> # Direct session access by key
            >>> auth_session = sqlspec_plugin.provide_request_session(
            ...     "auth_db", request
            ... )
            >>> analytics_session = sqlspec_plugin.provide_request_session(
            ...     "analytics_db", request
            ... )
        """
        # Get DatabaseConfig wrapper for Starlette methods
        db_config = self._get_database_config(key)
        return db_config.get_request_session(request)

    def provide_sync_request_session(
        self, key: Union[str, "SyncConfigT", "type[SyncConfigT]"], request: "Request"
    ) -> "SyncDriverAdapterBase":
        """Provide a sync database session for the specified configuration key from request scope.

        This method provides better type hints for sync database sessions, ensuring the returned
        session is properly typed as SyncDriverAdapterBase for better IDE support and type safety.

        Args:
            key: The sync configuration identifier
            request: The Starlette Request object

        Returns:
            A sync driver session instance for the specified database configuration
        """
        # Get DatabaseConfig wrapper for Starlette methods
        db_config = self._get_database_config(key)
        session = db_config.get_request_session(request)
        return cast("SyncDriverAdapterBase", session)

    def provide_async_request_session(
        self, key: Union[str, "AsyncConfigT", "type[AsyncConfigT]"], request: "Request"
    ) -> "AsyncDriverAdapterBase":
        """Provide an async database session for the specified configuration key from request scope.

        This method provides better type hints for async database sessions, ensuring the returned
        session is properly typed as AsyncDriverAdapterBase for better IDE support and type safety.

        Args:
            key: The async configuration identifier
            request: The Starlette Request object

        Returns:
            An async driver session instance for the specified database configuration
        """
        # Get DatabaseConfig wrapper for Starlette methods
        db_config = self._get_database_config(key)
        session = db_config.get_request_session(request)
        return cast("AsyncDriverAdapterBase", session)

    def provide_request_connection(
        self,
        key: Union[str, "SyncConfigT", "AsyncConfigT", "type[Union[SyncConfigT, AsyncConfigT]]"],
        request: "Request",
    ) -> Any:
        """Provide a database connection for the specified configuration key from request scope.

        This is a convenience method that combines get_config and get_request_connection
        into a single call.

        Args:
            key: The configuration identifier (same as get_config)
            request: The Starlette Request object

        Returns:
            A database connection instance for the specified database configuration
        """
        # Get DatabaseConfig wrapper for Starlette methods
        db_config = self._get_database_config(key)
        return db_config.get_request_connection(request)

    def get_config(
        self, name: Union["type[DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]]", str, Any]
    ) -> Union["DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]", DatabaseConfig]:
        """Get a configuration instance by name, supporting both base behavior and Starlette extensions.

        This method extends the base get_config to support Starlette-specific lookup patterns
        while maintaining compatibility with the base class signature. It supports lookup by
        connection key, pool key, session key, config instance, or annotation type.

        Args:
            name: The configuration identifier - can be:
                - Type annotation (base class behavior)
                - connection_key (e.g., "auth_db_connection")
                - pool_key (e.g., "analytics_db_pool")
                - session_key (e.g., "reporting_db_session")
                - config instance
                - annotation type

        Raises:
            KeyError: If no configuration is found for the given name.

        Returns:
            The configuration instance for the specified name.
        """
        # First try base class behavior for type-based lookup
        # Only call super() if name matches the expected base class types
        if not isinstance(name, str):
            try:
                return super().get_config(name)  # type: ignore[no-any-return]
            except (KeyError, AttributeError):
                # Fall back to Starlette-specific lookup patterns
                pass

        # Starlette-specific lookups by string keys
        if isinstance(name, str):
            for c in self.config:
                if name in {c.connection_key, c.pool_key, c.session_key}:
                    return c  # Return the DatabaseConfig wrapper for string lookups

        # Lookup by config instance or annotation
        for c in self.config:
            annotation_match = hasattr(c, "annotation") and name == c.annotation
            if name == c.config or annotation_match:
                return c.config  # Return the underlying config for type-based lookups

        msg = f"No database configuration found for name '{name}'. Available keys: {self._get_available_keys()}"
        raise KeyError(msg)

    def _get_database_config(
        self, key: Union[str, "SyncConfigT", "AsyncConfigT", "type[Union[SyncConfigT, AsyncConfigT]]"]
    ) -> DatabaseConfig:
        """Get a DatabaseConfig wrapper instance by name.

        This is used internally by provide_request_session and provide_request_connection
        to get the DatabaseConfig wrapper that has the request session methods.

        Args:
            key: The configuration identifier

        Returns:
            The DatabaseConfig wrapper instance

        Raises:
            KeyError: If no configuration is found for the given key
        """
        # For string keys, lookup by connection/pool/session keys
        if isinstance(key, str):
            for c in self.config:
                if key in {c.connection_key, c.pool_key, c.session_key}:
                    return c

        # For other keys, lookup by config instance or annotation
        for c in self.config:
            annotation_match = hasattr(c, "annotation") and key == c.annotation
            if key == c.config or annotation_match:
                return c

        msg = f"No database configuration found for name '{key}'. Available keys: {self._get_available_keys()}"
        raise KeyError(msg)

    def _get_available_keys(self) -> list[str]:
        """Get a list of all available configuration keys for error messages."""
        keys = []
        for c in self.config:
            keys.extend([c.connection_key, c.pool_key, c.session_key])
        return keys

    def _validate_dependency_keys(self) -> None:
        """Validate that connection and pool keys are unique across configurations.

        Raises:
            ImproperConfigurationError: If connection keys or pool keys are not unique.
        """
        connection_keys = [c.connection_key for c in self.config]
        pool_keys = [c.pool_key for c in self.config]
        if len(set(connection_keys)) != len(connection_keys):
            msg = "When using multiple database configuration, each configuration must have a unique `connection_key`."
            raise ImproperConfigurationError(detail=msg)
        if len(set(pool_keys)) != len(pool_keys):
            msg = "When using multiple database configuration, each configuration must have a unique `pool_key`."
            raise ImproperConfigurationError(detail=msg)
