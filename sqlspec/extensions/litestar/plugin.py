from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from litestar.di import Provide
from litestar.plugins import CLIPlugin, InitPluginProtocol

from sqlspec.base import SQLSpec as SQLSpecBase
from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, DriverT, SyncConfigT
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar.config import AsyncDatabaseConfig, DatabaseConfig, SyncDatabaseConfig
from sqlspec.typing import ConnectionT, PoolT
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from click import Group
    from litestar.config.app import AppConfig
    from litestar.datastructures.state import State
    from litestar.types import Scope

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.loader import SQLFileLoader

logger = get_logger("extensions.litestar")


class SQLSpec(SQLSpecBase, InitPluginProtocol, CLIPlugin):
    """Litestar plugin for SQLSpec database integration."""

    __slots__ = ("_plugin_configs",)

    def __init__(
        self,
        config: Union["SyncConfigT", "AsyncConfigT", "DatabaseConfig", list["DatabaseConfig"]],
        *,
        loader: "Optional[SQLFileLoader]" = None,
    ) -> None:
        """Initialize SQLSpec plugin.

        Args:
            config: Database configuration for SQLSpec plugin.
            loader: Optional SQL file loader instance.
        """
        super().__init__(loader=loader)
        if isinstance(config, DatabaseConfigProtocol):
            self._plugin_configs: list[DatabaseConfig] = [DatabaseConfig(config=config)]  # pyright: ignore
        elif isinstance(config, DatabaseConfig):
            self._plugin_configs = [config]
        else:
            self._plugin_configs = config

    @property
    def config(self) -> "list[DatabaseConfig]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Return the plugin configuration.

        Returns:
            List of database configurations.
        """
        return self._plugin_configs

    def on_cli_init(self, cli: "Group") -> None:
        """Configure CLI commands for SQLSpec database operations.

        Args:
            cli: The Click command group to add commands to.
        """
        from sqlspec.extensions.litestar.cli import database_group

        cli.add_command(database_group)

    def on_app_init(self, app_config: "AppConfig") -> "AppConfig":
        """Configure Litestar application with SQLSpec database integration.

        Args:
            app_config: The Litestar application configuration instance.

        Returns:
            The updated application configuration instance.
        """

        self._validate_dependency_keys()

        def store_sqlspec_in_state() -> None:
            app_config.state.sqlspec = self

        app_config.on_startup.append(store_sqlspec_in_state)
        app_config.signature_types.extend(
            [SQLSpec, ConnectionT, PoolT, DriverT, DatabaseConfig, DatabaseConfigProtocol, SyncConfigT, AsyncConfigT]
        )

        signature_namespace = {}

        for c in self._plugin_configs:
            c.annotation = self.add_config(c.config)
            app_config.signature_types.append(c.annotation)
            app_config.signature_types.append(c.config.connection_type)  # type: ignore[union-attr]
            app_config.signature_types.append(c.config.driver_type)  # type: ignore[union-attr]

            signature_namespace.update(c.config.get_signature_namespace())  # type: ignore[union-attr]

            app_config.before_send.append(c.before_send_handler)
            app_config.lifespan.append(c.lifespan_handler)  # pyright: ignore[reportUnknownMemberType]
            app_config.dependencies.update(
                {
                    c.connection_key: Provide(c.connection_provider),
                    c.pool_key: Provide(c.pool_provider),
                    c.session_key: Provide(c.session_provider),
                }
            )

        if signature_namespace:
            app_config.signature_namespace.update(signature_namespace)

        return app_config

    def get_annotations(self) -> "list[type[Union[SyncConfigT, AsyncConfigT]]]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Return the list of annotations.

        Returns:
            List of annotations.
        """
        return [c.annotation for c in self.config]

    def get_annotation(
        self, key: "Union[str, SyncConfigT, AsyncConfigT, type[Union[SyncConfigT, AsyncConfigT]]]"
    ) -> "type[Union[SyncConfigT, AsyncConfigT]]":
        """Return the annotation for the given configuration.

        Args:
            key: The configuration instance or key to lookup

        Raises:
            KeyError: If no configuration is found for the given key.

        Returns:
            The annotation for the configuration.
        """
        for c in self.config:
            # Check annotation only if it's been set (during on_app_init)
            annotation_match = hasattr(c, "annotation") and key == c.annotation
            if key == c.config or annotation_match or key in {c.connection_key, c.pool_key}:
                if not hasattr(c, "annotation"):
                    msg = (
                        "Annotation not set for configuration. Ensure the plugin has been initialized with on_app_init."
                    )
                    raise AttributeError(msg)
                return c.annotation
        msg = f"No configuration found for {key}"
        raise KeyError(msg)

    @overload
    def get_config(self, name: "type[SyncConfigT]") -> "SyncConfigT": ...

    @overload
    def get_config(self, name: "type[AsyncConfigT]") -> "AsyncConfigT": ...

    @overload
    def get_config(self, name: str) -> "DatabaseConfig": ...

    @overload
    def get_config(self, name: "type[SyncDatabaseConfig]") -> "SyncDatabaseConfig": ...

    @overload
    def get_config(self, name: "type[AsyncDatabaseConfig]") -> "AsyncDatabaseConfig": ...

    def get_config(
        self, name: "Union[type[DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]], str, Any]"
    ) -> "Union[DatabaseConfigProtocol[ConnectionT, PoolT, DriverT], DatabaseConfig, SyncDatabaseConfig, AsyncDatabaseConfig]":
        """Get a configuration instance by name, supporting both base behavior and Litestar extensions.

        This method extends the base get_config to support Litestar-specific lookup patterns
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
                # Fall back to Litestar-specific lookup patterns
                pass

        # Litestar-specific lookups by string keys
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

    def provide_request_session(
        self,
        key: "Union[str, SyncConfigT, AsyncConfigT, type[Union[SyncConfigT, AsyncConfigT]]]",
        state: "State",
        scope: "Scope",
    ) -> "Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]":
        """Provide a database session for the specified configuration key from request scope.

        This is a convenience method that combines get_config and get_request_session
        into a single call, similar to Advanced Alchemy's provide_session pattern.

        Args:
            key: The configuration identifier (same as get_config)
            state: The Litestar application State object
            scope: The ASGI scope containing the request context

        Returns:
            A driver session instance for the specified database configuration

        Example:
            >>> sqlspec_plugin = connection.app.state.sqlspec
            >>> # Direct session access by key
            >>> auth_session = sqlspec_plugin.provide_request_session(
            ...     "auth_db", state, scope
            ... )
            >>> analytics_session = sqlspec_plugin.provide_request_session(
            ...     "analytics_db", state, scope
            ... )
        """
        # Get DatabaseConfig wrapper for Litestar methods
        db_config = self._get_database_config(key)
        return db_config.get_request_session(state, scope)

    def provide_sync_request_session(
        self, key: "Union[str, SyncConfigT, type[SyncConfigT]]", state: "State", scope: "Scope"
    ) -> "SyncDriverAdapterBase":
        """Provide a sync database session for the specified configuration key from request scope.

        This method provides better type hints for sync database sessions, ensuring the returned
        session is properly typed as SyncDriverAdapterBase for better IDE support and type safety.

        Args:
            key: The sync configuration identifier
            state: The Litestar application State object
            scope: The ASGI scope containing the request context

        Returns:
            A sync driver session instance for the specified database configuration

        Example:
            >>> sqlspec_plugin = connection.app.state.sqlspec
            >>> auth_session = sqlspec_plugin.provide_sync_request_session(
            ...     "auth_db", state, scope
            ... )
            >>> # auth_session is now correctly typed as SyncDriverAdapterBase
        """
        # Get DatabaseConfig wrapper for Litestar methods
        db_config = self._get_database_config(key)
        session = db_config.get_request_session(state, scope)
        return cast("SyncDriverAdapterBase", session)

    def provide_async_request_session(
        self, key: "Union[str, AsyncConfigT, type[AsyncConfigT]]", state: "State", scope: "Scope"
    ) -> "AsyncDriverAdapterBase":
        """Provide an async database session for the specified configuration key from request scope.

        This method provides better type hints for async database sessions, ensuring the returned
        session is properly typed as AsyncDriverAdapterBase for better IDE support and type safety.

        Args:
            key: The async configuration identifier
            state: The Litestar application State object
            scope: The ASGI scope containing the request context

        Returns:
            An async driver session instance for the specified database configuration

        Example:
            >>> sqlspec_plugin = connection.app.state.sqlspec
            >>> auth_session = sqlspec_plugin.provide_async_request_session(
            ...     "auth_db", state, scope
            ... )
            >>> # auth_session is now correctly typed as AsyncDriverAdapterBase
        """
        # Get DatabaseConfig wrapper for Litestar methods
        db_config = self._get_database_config(key)
        session = db_config.get_request_session(state, scope)
        return cast("AsyncDriverAdapterBase", session)

    def provide_request_connection(
        self,
        key: "Union[str, SyncConfigT, AsyncConfigT, type[Union[SyncConfigT, AsyncConfigT]]]",
        state: "State",
        scope: "Scope",
    ) -> Any:
        """Provide a database connection for the specified configuration key from request scope.

        This is a convenience method that combines get_config and get_request_connection
        into a single call.

        Args:
            key: The configuration identifier (same as get_config)
            state: The Litestar application State object
            scope: The ASGI scope containing the request context

        Returns:
            A database connection instance for the specified database configuration

        Example:
            >>> sqlspec_plugin = connection.app.state.sqlspec
            >>> # Direct connection access by key
            >>> auth_conn = sqlspec_plugin.provide_request_connection(
            ...     "auth_db", state, scope
            ... )
            >>> analytics_conn = sqlspec_plugin.provide_request_connection(
            ...     "analytics_db", state, scope
            ... )
        """
        # Get DatabaseConfig wrapper for Litestar methods
        db_config = self._get_database_config(key)
        return db_config.get_request_connection(state, scope)

    def _get_database_config(
        self, key: "Union[str, SyncConfigT, AsyncConfigT, type[Union[SyncConfigT, AsyncConfigT]]]"
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

    def _get_available_keys(self) -> "list[str]":
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
