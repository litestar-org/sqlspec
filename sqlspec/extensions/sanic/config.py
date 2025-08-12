"""Configuration classes for SQLSpec Sanic integration."""

import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Union

from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from sanic import Sanic

    from sqlspec.config import AsyncConfigT, DriverT, SyncConfigT
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
    "CommitMode",
    "DatabaseConfig",
)


@dataclass
class DatabaseConfig:
    """Configuration for SQLSpec database integration with Sanic applications."""

    config: "Union[SyncConfigT, AsyncConfigT]" = field()  # type: ignore[valid-type]   # pyright: ignore[reportGeneralTypeIssues]
    connection_key: str = field(default=DEFAULT_CONNECTION_KEY)
    pool_key: str = field(default=DEFAULT_POOL_KEY)
    session_key: str = field(default=DEFAULT_SESSION_KEY)
    commit_mode: "CommitMode" = field(default=DEFAULT_COMMIT_MODE)
    extra_commit_statuses: "Optional[set[int]]" = field(default=None)
    extra_rollback_statuses: "Optional[set[int]]" = field(default=None)
    enable_middleware: bool = field(default=True)

    # Generated providers
    connection_provider: "Optional[Callable[[], AsyncGenerator[ConnectionT, None]]]" = field(
        init=False, repr=False, hash=False, default=None
    )
    pool_provider: "Optional[Callable[[], Awaitable[PoolT]]]" = field(init=False, repr=False, hash=False, default=None)
    session_provider: "Optional[Callable[[ConnectionT], AsyncGenerator[DriverT, None]]]" = field(
        init=False, repr=False, hash=False, default=None
    )

    # Generated unique context keys for Sanic
    engine_key: str = field(init=False, repr=False, hash=False)
    session_maker_key: str = field(init=False, repr=False, hash=False)

    def __post_init__(self) -> None:
        """Initialize providers and keys after object creation."""
        if not self.config.supports_connection_pooling and self.pool_key == DEFAULT_POOL_KEY:  # type: ignore[union-attr,unused-ignore]
            self.pool_key = f"_{self.pool_key}_{id(self.config)}"

        # Generate unique context keys
        self.engine_key = f"engine_{id(self.config)}"
        self.session_maker_key = f"session_maker_{id(self.config)}"

        # Validate commit mode
        if self.commit_mode not in {"manual", "autocommit", "autocommit_include_redirect"}:
            msg = f"Invalid commit mode: {self.commit_mode}"
            raise ImproperConfigurationError(detail=msg)

        # Validate status code sets
        if (
            self.extra_commit_statuses
            and self.extra_rollback_statuses
            and self.extra_commit_statuses & self.extra_rollback_statuses
        ):
            msg = "Extra rollback statuses and commit statuses must not share any status codes"
            raise ImproperConfigurationError(msg)

    def init_app(self, app: "Sanic") -> None:
        """Initialize SQLSpec configuration for Sanic application.

        Args:
            app: The Sanic application instance.
        """
        from sqlspec.extensions.sanic._providers import (
            create_connection_provider,
            create_pool_provider,
            create_session_provider,
        )

        # Create providers
        self.pool_provider = create_pool_provider(self.config, self.pool_key)
        self.connection_provider = create_connection_provider(self.config, self.pool_key, self.connection_key)
        self.session_provider = create_session_provider(self.config, self.connection_key)

        # Add middleware if enabled
        if self.enable_middleware:
            app.middleware("request")(self._before_request_middleware)
            app.middleware("response")(self._after_response_middleware)

        # Add startup and shutdown listeners
        app.add_listener(self._startup_handler(app), "before_server_start")
        app.add_listener(self._shutdown_handler(app), "after_server_stop")

    def _startup_handler(self, app: "Sanic") -> "Callable[[Sanic, Any], Awaitable[None]]":
        """Create startup handler for database pool initialization.

        Args:
            app: The Sanic application instance.

        Returns:
            Startup handler function.
        """

        async def startup(app: "Sanic", loop: Any) -> None:
            """Initialize database pool on startup.

            Args:
                app: The Sanic application instance.
                loop: The event loop.
            """
            from sqlspec.utils.sync_tools import ensure_async_

            db_pool = await ensure_async_(self.config.create_pool)()
            app.ctx.__dict__[self.pool_key] = db_pool

        return startup

    def _shutdown_handler(self, app: "Sanic") -> "Callable[[Sanic, Any], Awaitable[None]]":
        """Create shutdown handler for database pool cleanup.

        Args:
            app: The Sanic application instance.

        Returns:
            Shutdown handler function.
        """

        async def shutdown(app: "Sanic", loop: Any) -> None:
            """Clean up database pool on shutdown.

            Args:
                app: The Sanic application instance.
                loop: The event loop.
            """
            from sqlspec.utils.sync_tools import ensure_async_

            app.ctx.__dict__.pop(self.pool_key, None)
            with contextlib.suppress(Exception):
                await ensure_async_(self.config.close_pool)()

        return shutdown

    async def _before_request_middleware(self, request: Any) -> None:
        """Set up database connection before request processing.

        Args:
            request: The Sanic request object.
        """
        # Sanic handles this automatically through dependency injection
        # Connection will be created when first accessed

    async def _after_response_middleware(self, request: Any, response: Any) -> None:
        """Handle transaction commit/rollback after response processing.

        Args:
            request: The Sanic request object.
            response: The Sanic response object.
        """
        if self.commit_mode == "manual":
            return

        # Get connection from request context if it exists
        connection = getattr(request.ctx, self.connection_key, None)
        if connection is None:
            return

        try:
            should_commit = self._should_commit_transaction(response.status)

            if should_commit and hasattr(connection, "commit") and callable(connection.commit):
                await connection.commit()
            elif hasattr(connection, "rollback") and callable(connection.rollback):
                await connection.rollback()
        except Exception:
            # Always try to rollback on exception
            if hasattr(connection, "rollback") and callable(connection.rollback):
                with contextlib.suppress(Exception):
                    await connection.rollback()
        finally:
            # Clean up connection
            if hasattr(connection, "close") and callable(connection.close):
                with contextlib.suppress(Exception):
                    await connection.close()
            if hasattr(request.ctx, self.connection_key):
                delattr(request.ctx, self.connection_key)

    def _should_commit_transaction(self, status_code: int) -> bool:
        """Determine if transaction should be committed based on status code.

        Args:
            status_code: HTTP response status code.

        Returns:
            True if transaction should be committed, False otherwise.
        """
        http_ok = 200
        http_multiple_choices = 300
        http_bad_request = 400

        should_commit = False

        if self.commit_mode == "autocommit":
            should_commit = http_ok <= status_code < http_multiple_choices
        elif self.commit_mode == "autocommit_include_redirect":
            should_commit = http_ok <= status_code < http_bad_request

        # Apply extra status overrides
        if self.extra_commit_statuses and status_code in self.extra_commit_statuses:
            should_commit = True
        elif self.extra_rollback_statuses and status_code in self.extra_rollback_statuses:
            should_commit = False

        return should_commit
