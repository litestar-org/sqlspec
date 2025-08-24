"""Middleware for SQLSpec Starlette integration."""

import contextlib
import uuid
from typing import TYPE_CHECKING, Any, Optional

from starlette.middleware.base import BaseHTTPMiddleware

from sqlspec.utils.correlation import set_correlation_id
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import ensure_async_

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from starlette.requests import Request
    from starlette.responses import Response

    from sqlspec.extensions.starlette.config import CommitMode, DatabaseConfig

logger = get_logger("extensions.starlette.middleware")


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
        # Set up correlation ID for request tracking
        correlation_id = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        set_correlation_id(correlation_id)

        if not self.config.connection_provider or not self.config.session_provider:
            # If no providers, just pass through
            logger.debug("No connection or session provider found, skipping middleware")
            return await call_next(request)

        # Get pool from app state
        pool = getattr(request.app.state, self.config.pool_key, None)
        if pool is None:
            logger.warning("Database pool '%s' not found in app state", self.config.pool_key)
            return await call_next(request)

        # Get connection from provider
        connection_gen = self.config.connection_provider()
        try:
            connection = await connection_gen.__anext__()
        except StopAsyncIteration:
            logger.exception("Connection provider exhausted")
            return await call_next(request)

        # Store connection in request state
        request.state.__dict__[self.config.connection_key] = connection

        # Get session from provider
        session_gen = self.config.session_provider(connection)
        session = None

        try:
            session = await session_gen.__anext__()

            # Store session in request state
            request.state.__dict__[self.config.session_key] = session

            logger.debug("Database session established for connection key: %s", self.config.connection_key)

            response = await call_next(request)

            # Handle transaction based on commit mode and response status
            if self.commit_mode != "manual":
                await self._handle_transaction(session or connection, response.status_code)

        except Exception:
            logger.exception("Exception in request processing")
            # Rollback on exception
            await self._rollback_transaction(session or connection)
            raise
        else:
            return response
        finally:
            # Clean up session
            if session is not None:
                with contextlib.suppress(StopAsyncIteration):
                    await session_gen.__anext__()

            # Clean up connection
            with contextlib.suppress(StopAsyncIteration):
                await connection_gen.__anext__()

            # Clean up request state
            request.state.__dict__.pop(self.config.connection_key, None)
            request.state.__dict__.pop(self.config.session_key, None)

            logger.debug("Database session and connection cleaned up")

    async def _handle_transaction(self, session_or_connection: Any, status_code: int) -> None:
        """Handle transaction commit/rollback based on status code.

        Args:
            session_or_connection: The database session or connection.
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
        if should_commit:
            await self._commit_transaction(session_or_connection)
            logger.debug("Transaction committed for status code: %s", status_code)
        else:
            await self._rollback_transaction(session_or_connection)
            logger.debug("Transaction rolled back for status code: %s", status_code)

    async def _commit_transaction(self, session_or_connection: Any) -> None:
        """Commit transaction on session or connection.

        Args:
            session_or_connection: The database session or connection.
        """
        try:
            if hasattr(session_or_connection, "commit") and callable(session_or_connection.commit):
                await ensure_async_(session_or_connection.commit)()
            elif hasattr(session_or_connection, "connection"):
                # Try to commit on underlying connection if session doesn't have commit
                conn = session_or_connection.connection
                if hasattr(conn, "commit") and callable(conn.commit):
                    await ensure_async_(conn.commit)()
        except Exception:
            logger.exception("Error committing transaction")
            # Try to rollback after failed commit
            await self._rollback_transaction(session_or_connection)

    async def _rollback_transaction(self, session_or_connection: Any) -> None:
        """Rollback transaction on session or connection.

        Args:
            session_or_connection: The database session or connection.
        """
        try:
            if hasattr(session_or_connection, "rollback") and callable(session_or_connection.rollback):
                await ensure_async_(session_or_connection.rollback)()
            elif hasattr(session_or_connection, "connection"):
                # Try to rollback on underlying connection if session doesn't have rollback
                conn = session_or_connection.connection
                if hasattr(conn, "rollback") and callable(conn.rollback):
                    await ensure_async_(conn.rollback)()
        except Exception:
            logger.exception("Error rolling back transaction")


def create_session_middleware(
    config: "DatabaseConfig",
    commit_mode: "CommitMode" = "manual",
    extra_commit_statuses: "Optional[set[int]]" = None,
    extra_rollback_statuses: "Optional[set[int]]" = None,
) -> type[SessionMiddleware]:
    """Create a session middleware class.

    Args:
        config: Database configuration instance.
        commit_mode: Transaction commit behavior.
        extra_commit_statuses: Additional status codes that trigger commits.
        extra_rollback_statuses: Additional status codes that trigger rollbacks.

    Returns:
        Configured session middleware class.
    """

    class ConfiguredSessionMiddleware(SessionMiddleware):
        def __init__(self, app: Any) -> None:
            super().__init__(
                app=app,
                config=config,
                commit_mode=commit_mode,
                extra_commit_statuses=extra_commit_statuses,
                extra_rollback_statuses=extra_rollback_statuses,
            )

    return ConfiguredSessionMiddleware
