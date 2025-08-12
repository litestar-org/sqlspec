"""Middleware for SQLSpec Starlette integration."""

import contextlib
from typing import TYPE_CHECKING, Any, Optional, cast

from starlette.middleware.base import BaseHTTPMiddleware

from sqlspec.utils.sync_tools import ensure_async_

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from starlette.requests import Request
    from starlette.responses import Response

    from sqlspec.extensions.starlette.config import CommitMode, DatabaseConfig


__all__ = ("SessionMiddleware", "create_session_middleware")


class SessionMiddleware(BaseHTTPMiddleware):
    """Middleware for managing database sessions and transactions."""

    def __init__(
        self,
        app: Any,
        config: "DatabaseConfig",
        commit_mode: "CommitMode" = "manual",
        extra_commit_statuses: "Optional[set[int]]" = None,
        extra_rollback_statuses: "Optional[set[int]]" = None,
    ) -> None:
        """Initialize session middleware.

        Args:
            app: The ASGI application.
            config: Database configuration instance.
            commit_mode: Transaction commit behavior.
            extra_commit_statuses: Additional status codes that trigger commits.
            extra_rollback_statuses: Additional status codes that trigger rollbacks.
        """
        super().__init__(app)
        self.config = config
        self.commit_mode = commit_mode
        self.extra_commit_statuses = extra_commit_statuses or set()
        self.extra_rollback_statuses = extra_rollback_statuses or set()

    async def dispatch(self, request: "Request", call_next: "Callable[[Request], Awaitable[Response]]") -> "Response":
        """Handle request with session management.

        Args:
            request: The incoming request.
            call_next: The next middleware or endpoint.

        Returns:
            The response from the application.
        """
        if not self.config.connection_provider:
            # If no connection provider, just pass through
            return await call_next(request)

        # Get connection from provider
        connection_gen = self.config.connection_provider()
        connection = await connection_gen.__anext__()

        # Store connection in request state
        request.state.__dict__[self.config.connection_key] = connection

        try:
            response = await call_next(request)

            # Handle transaction based on commit mode and response status
            if self.commit_mode != "manual":
                await self._handle_transaction(connection, response.status_code)

        except Exception:
            # Rollback on exception
            if hasattr(connection, "rollback") and callable(connection.rollback):
                await ensure_async_(connection.rollback)()
            raise
        else:
            return response
        finally:
            # Clean up connection
            with contextlib.suppress(StopAsyncIteration):
                await connection_gen.__anext__()
            if hasattr(connection, "close") and callable(connection.close):
                await ensure_async_(connection.close)()

    async def _handle_transaction(self, connection: Any, status_code: int) -> None:
        """Handle transaction commit/rollback based on status code.

        Args:
            connection: The database connection.
            status_code: HTTP response status code.
        """
        http_ok = 200
        http_multiple_choices = 300
        http_bad_request = 400

        should_commit = False

        if self.commit_mode == "autocommit":
            # Commit on 2xx status codes
            should_commit = http_ok <= status_code < http_multiple_choices
        elif self.commit_mode == "autocommit_include_redirect":
            # Commit on 2xx and 3xx status codes
            should_commit = http_ok <= status_code < http_bad_request

        # Apply extra status overrides
        if status_code in self.extra_commit_statuses:
            should_commit = True
        elif status_code in self.extra_rollback_statuses:
            should_commit = False

        # Execute transaction action
        if should_commit and hasattr(connection, "commit") and callable(connection.commit):
            await ensure_async_(connection.commit)()
        elif not should_commit and hasattr(connection, "rollback") and callable(connection.rollback):
            await ensure_async_(connection.rollback)()


def create_session_middleware(
    config: "DatabaseConfig",
    commit_mode: "CommitMode" = "manual",
    extra_commit_statuses: "Optional[set[int]]" = None,
    extra_rollback_statuses: "Optional[set[int]]" = None,
) -> SessionMiddleware:
    """Create a session middleware instance.

    Args:
        config: Database configuration instance.
        commit_mode: Transaction commit behavior.
        extra_commit_statuses: Additional status codes that trigger commits.
        extra_rollback_statuses: Additional status codes that trigger rollbacks.

    Returns:
        Configured session middleware instance.
    """

    def middleware_factory(app: Any) -> SessionMiddleware:
        return SessionMiddleware(
            app=app,
            config=config,
            commit_mode=commit_mode,
            extra_commit_statuses=extra_commit_statuses,
            extra_rollback_statuses=extra_rollback_statuses,
        )

    # Return a pre-configured middleware instance
    return cast(
        "SessionMiddleware",
        type(
            "SessionMiddleware",
            (SessionMiddleware,),
            {
                "__init__": lambda self, app: SessionMiddleware.__init__(
                    self, app, config, commit_mode, extra_commit_statuses, extra_rollback_statuses
                )
            },
        ),
    )
