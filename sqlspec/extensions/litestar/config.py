from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Union, cast

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._utils import get_sqlspec_scope_state, set_sqlspec_scope_state
from sqlspec.extensions.litestar.handlers import (
    autocommit_handler_maker,
    connection_provider_maker,
    lifespan_handler_maker,
    manual_handler_maker,
    pool_provider_maker,
    session_provider_maker,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from litestar import Litestar
    from litestar.datastructures.state import State
    from litestar.types import BeforeMessageSendHookHandler, Scope

    from sqlspec.config import AsyncConfigT, DriverT, SyncConfigT
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.typing import ConnectionT, PoolT


CommitMode = Literal["manual", "autocommit", "autocommit_include_redirect"]
DEFAULT_COMMIT_MODE: CommitMode = "manual"
DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"

__all__ = (
    "DEFAULT_COMMIT_MODE",
    "DEFAULT_CONNECTION_KEY",
    "DEFAULT_POOL_KEY",
    "DEFAULT_SESSION_KEY",
    "AsyncDatabaseConfig",
    "CommitMode",
    "DatabaseConfig",
    "SyncDatabaseConfig",
)


@dataclass
class DatabaseConfig:
    config: "Union[SyncConfigT, AsyncConfigT]" = field()  # type: ignore[valid-type]   # pyright: ignore[reportGeneralTypeIssues]
    connection_key: str = field(default=DEFAULT_CONNECTION_KEY)
    pool_key: str = field(default=DEFAULT_POOL_KEY)
    session_key: str = field(default=DEFAULT_SESSION_KEY)
    commit_mode: "CommitMode" = field(default=DEFAULT_COMMIT_MODE)
    extra_commit_statuses: "Optional[set[int]]" = field(default=None)
    extra_rollback_statuses: "Optional[set[int]]" = field(default=None)
    enable_correlation_middleware: bool = field(default=True)
    connection_provider: "Callable[[State, Scope], AsyncGenerator[ConnectionT, None]]" = field(  # pyright: ignore[reportGeneralTypeIssues]
        init=False, repr=False, hash=False
    )
    pool_provider: "Callable[[State,Scope], Awaitable[PoolT]]" = field(init=False, repr=False, hash=False)  # pyright: ignore[reportGeneralTypeIssues]
    session_provider: "Callable[[Any], AsyncGenerator[DriverT, None]]" = field(init=False, repr=False, hash=False)  # pyright: ignore[reportGeneralTypeIssues]
    before_send_handler: "BeforeMessageSendHookHandler" = field(init=False, repr=False, hash=False)
    lifespan_handler: "Callable[[Litestar], AbstractAsyncContextManager[None]]" = field(
        init=False, repr=False, hash=False
    )
    annotation: "type[Union[SyncConfigT, AsyncConfigT]]" = field(init=False, repr=False, hash=False)  # type: ignore[valid-type]   # pyright: ignore[reportGeneralTypeIssues]

    def __post_init__(self) -> None:
        if not self.config.supports_connection_pooling and self.pool_key == DEFAULT_POOL_KEY:  # type: ignore[union-attr,unused-ignore]
            self.pool_key = f"_{self.pool_key}_{id(self.config)}"
        if self.commit_mode == "manual":
            self.before_send_handler = manual_handler_maker(connection_scope_key=self.connection_key)
        elif self.commit_mode == "autocommit":
            self.before_send_handler = autocommit_handler_maker(
                commit_on_redirect=False,
                extra_commit_statuses=self.extra_commit_statuses,
                extra_rollback_statuses=self.extra_rollback_statuses,
                connection_scope_key=self.connection_key,
            )
        elif self.commit_mode == "autocommit_include_redirect":
            self.before_send_handler = autocommit_handler_maker(
                commit_on_redirect=True,
                extra_commit_statuses=self.extra_commit_statuses,
                extra_rollback_statuses=self.extra_rollback_statuses,
                connection_scope_key=self.connection_key,
            )
        else:
            msg = f"Invalid commit mode: {self.commit_mode}"
            raise ImproperConfigurationError(detail=msg)
        self.lifespan_handler = lifespan_handler_maker(config=self.config, pool_key=self.pool_key)
        self.connection_provider = connection_provider_maker(
            connection_key=self.connection_key, pool_key=self.pool_key, config=self.config
        )
        self.pool_provider = pool_provider_maker(config=self.config, pool_key=self.pool_key)
        self.session_provider = session_provider_maker(
            config=self.config, connection_dependency_key=self.connection_key
        )

    def get_request_session(
        self, state: "State", scope: "Scope"
    ) -> "Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]":
        """Get a session instance from the current request.

        This method provides access to the database session that has been added to the request
        scope, similar to Advanced Alchemy's provide_session method. It first looks for an
        existing session in the request scope state, and if not found, creates a new one using
        the connection from the scope.

        Args:
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A driver session instance.

        Raises:
            ImproperConfigurationError: If no connection is available in the scope.
        """
        # Create a unique scope key for sessions to avoid conflicts
        session_scope_key = f"{self.session_key}_instance"

        # Try to get existing session from scope
        session = get_sqlspec_scope_state(scope, session_scope_key)
        if session is not None:
            return cast("Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]", session)

        # Get connection from scope state
        connection = get_sqlspec_scope_state(scope, self.connection_key)
        if connection is None:
            msg = f"No database connection found in scope for key '{self.connection_key}'. "
            msg += "Ensure the connection dependency is properly configured and available."
            raise ImproperConfigurationError(detail=msg)

        # Create new session using the connection
        # Access driver_type which is available on all config types
        session = self.config.driver_type(connection=connection)  # type: ignore[union-attr]

        # Store session in scope for future use
        set_sqlspec_scope_state(scope, session_scope_key, session)

        return cast("Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]", session)

    def get_request_connection(self, state: "State", scope: "Scope") -> "Any":
        """Get a connection instance from the current request.

        This method provides access to the database connection that has been added to the request
        scope. This is useful in guards, middleware, or other contexts where you need direct
        access to the connection that's been established for the current request.

        Args:
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A database connection instance.

        Raises:
            ImproperConfigurationError: If no connection is available in the scope.
        """
        connection = get_sqlspec_scope_state(scope, self.connection_key)
        if connection is None:
            msg = f"No database connection found in scope for key '{self.connection_key}'. "
            msg += "Ensure the connection dependency is properly configured and available."
            raise ImproperConfigurationError(detail=msg)

        return cast("Any", connection)


# Add passthrough methods to both specialized classes for convenience
class SyncDatabaseConfig(DatabaseConfig):
    """Sync-specific DatabaseConfig with better typing for get_request_session."""

    def get_request_session(self, state: "State", scope: "Scope") -> "SyncDriverAdapterBase":
        """Get a sync session instance from the current request.

        This method provides access to the database session that has been added to the request
        scope, similar to Advanced Alchemy's provide_session method. It first looks for an
        existing session in the request scope state, and if not found, creates a new one using
        the connection from the scope.

        Args:
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            A sync driver session instance.
        """
        session = super().get_request_session(state, scope)
        return cast("SyncDriverAdapterBase", session)

    def provide_session(self) -> "AbstractContextManager[SyncDriverAdapterBase]":
        """Provide a database session context manager.

        This is a passthrough to the underlying config's provide_session method
        for convenient access to database sessions.

        Returns:
            Context manager that yields a sync driver session.
        """
        return self.config.provide_session()  # type: ignore[union-attr,no-any-return]

    def provide_connection(self) -> "AbstractContextManager[Any]":
        """Provide a database connection context manager.

        This is a passthrough to the underlying config's provide_connection method
        for convenient access to database connections.

        Returns:
            Context manager that yields a sync database connection.
        """
        return self.config.provide_connection()  # type: ignore[union-attr,no-any-return]

    def create_connection(self) -> "Any":
        """Create and return a new database connection.

        This is a passthrough to the underlying config's create_connection method
        for direct connection creation without context management.

        Returns:
            A new sync database connection.
        """
        return self.config.create_connection()  # type: ignore[union-attr]


class AsyncDatabaseConfig(DatabaseConfig):
    """Async-specific DatabaseConfig with better typing for get_request_session."""

    def get_request_session(self, state: "State", scope: "Scope") -> "AsyncDriverAdapterBase":
        """Get an async session instance from the current request.

        This method provides access to the database session that has been added to the request
        scope, similar to Advanced Alchemy's provide_session method. It first looks for an
        existing session in the request scope state, and if not found, creates a new one using
        the connection from the scope.

        Args:
            state: The Litestar application State object.
            scope: The ASGI scope containing the request context.

        Returns:
            An async driver session instance.
        """
        session = super().get_request_session(state, scope)
        return cast("AsyncDriverAdapterBase", session)

    def provide_session(self) -> "AbstractAsyncContextManager[AsyncDriverAdapterBase]":
        """Provide a database session context manager.

        This is a passthrough to the underlying config's provide_session method
        for convenient access to database sessions.

        Returns:
            Context manager that yields an async driver session.
        """
        return self.config.provide_session()  # type: ignore[union-attr,no-any-return]

    def provide_connection(self) -> "AbstractAsyncContextManager[Any]":
        """Provide a database connection context manager.

        This is a passthrough to the underlying config's provide_connection method
        for convenient access to database connections.

        Returns:
            Context manager that yields an async database connection.
        """
        return self.config.provide_connection()  # type: ignore[union-attr,no-any-return]

    async def create_connection(self) -> "Any":
        """Create and return a new database connection.

        This is a passthrough to the underlying config's create_connection method
        for direct connection creation without context management.

        Returns:
            A new async database connection.
        """
        return await self.config.create_connection()  # type: ignore[union-attr]
