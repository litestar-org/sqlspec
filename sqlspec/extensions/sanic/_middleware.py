"""Middleware system for SQLSpec Sanic integration."""

import contextlib
from typing import TYPE_CHECKING, Any

from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import ensure_async_

if TYPE_CHECKING:
    from sqlspec.extensions.sanic.config import DatabaseConfig

logger = get_logger("extensions.sanic.middleware")

__all__ = ("SessionMiddleware",)


class SessionMiddleware:
    """Session middleware for managing database sessions in Sanic requests."""

    __slots__ = ("_connection_key", "_session_key", "database_config")

    def __init__(self, database_config: "DatabaseConfig") -> None:
        """Initialize session middleware.

        Args:
            database_config: The database configuration instance.
        """
        self.database_config = database_config
        self._connection_key = database_config.connection_key
        self._session_key = database_config.session_key

    async def before_request(self, request: Any) -> None:
        """Set up database connection and session before request processing.

        Args:
            request: The Sanic request object.
        """
        logger.debug("Setting up database connection for request %s", request.id)

        try:
            # Only create connection if it doesn't exist
            if not hasattr(request.ctx, self._connection_key):
                # Get connection from provider if available
                if self.database_config.connection_provider:
                    connection_gen = self.database_config.connection_provider()
                    connection = await connection_gen.__anext__()
                    setattr(request.ctx, self._connection_key, connection)
                    # Store the generator for cleanup
                    setattr(request.ctx, f"_{self._connection_key}_gen", connection_gen)
                else:
                    # Fallback: create connection directly
                    pool = await ensure_async_(self.database_config.config.create_pool)()
                    connection_cm = self.database_config.config.provide_connection(pool)
                    if hasattr(connection_cm, "__aenter__"):
                        connection = await connection_cm.__aenter__()
                        setattr(request.ctx, self._connection_key, connection)
                        setattr(request.ctx, f"_{self._connection_key}_cm", connection_cm)
                    else:
                        connection = await connection_cm if hasattr(connection_cm, "__await__") else connection_cm
                        setattr(request.ctx, self._connection_key, connection)

                logger.debug("Database connection established for request %s", request.id)

            # Create session if provider is available and session doesn't exist
            if self.database_config.session_provider and not hasattr(request.ctx, self._session_key):
                connection = getattr(request.ctx, self._connection_key)
                session_gen = self.database_config.session_provider(connection)
                session = await session_gen.__anext__()
                setattr(request.ctx, self._session_key, session)
                setattr(request.ctx, f"_{self._session_key}_gen", session_gen)
                logger.debug("Database session created for request %s", request.id)

        except Exception:
            logger.exception("Failed to set up database connection/session for request %s", request.id)
            raise

    async def after_response(self, request: Any, response: Any) -> None:
        """Handle transaction commit/rollback and cleanup after response processing.

        Args:
            request: The Sanic request object.
            response: The Sanic response object.
        """
        logger.debug("Cleaning up database resources for request %s", request.id)

        # Handle session cleanup
        if hasattr(request.ctx, f"_{self._session_key}_gen"):
            session_gen = getattr(request.ctx, f"_{self._session_key}_gen")
            with contextlib.suppress(Exception):
                await session_gen.__anext__()  # This should raise StopAsyncIteration
            with contextlib.suppress(Exception):
                delattr(request.ctx, f"_{self._session_key}_gen")

        if hasattr(request.ctx, self._session_key):
            with contextlib.suppress(Exception):
                delattr(request.ctx, self._session_key)

        # Handle transaction management
        connection = getattr(request.ctx, self._connection_key, None)
        if connection is not None:
            try:
                should_commit = self._should_commit_transaction(response.status)

                if should_commit and hasattr(connection, "commit") and callable(connection.commit):
                    await ensure_async_(connection.commit)()
                    logger.debug("Transaction committed for request %s", request.id)
                elif hasattr(connection, "rollback") and callable(connection.rollback):
                    await ensure_async_(connection.rollback)()
                    logger.debug("Transaction rolled back for request %s", request.id)
            except Exception:
                logger.exception("Error during transaction handling for request %s", request.id)
                # Always try to rollback on exception
                if hasattr(connection, "rollback") and callable(connection.rollback):
                    with contextlib.suppress(Exception):
                        await ensure_async_(connection.rollback)()

        # Handle connection cleanup
        if hasattr(request.ctx, f"_{self._connection_key}_gen"):
            connection_gen = getattr(request.ctx, f"_{self._connection_key}_gen")
            with contextlib.suppress(Exception):
                await connection_gen.__anext__()  # This should raise StopAsyncIteration
            with contextlib.suppress(Exception):
                delattr(request.ctx, f"_{self._connection_key}_gen")

        if hasattr(request.ctx, f"_{self._connection_key}_cm"):
            connection_cm = getattr(request.ctx, f"_{self._connection_key}_cm")
            with contextlib.suppress(Exception):
                await connection_cm.__aexit__(None, None, None)
            with contextlib.suppress(Exception):
                delattr(request.ctx, f"_{self._connection_key}_cm")

        if hasattr(request.ctx, self._connection_key):
            connection = getattr(request.ctx, self._connection_key)
            if hasattr(connection, "close") and callable(connection.close):
                with contextlib.suppress(Exception):
                    await ensure_async_(connection.close)()
            with contextlib.suppress(Exception):
                delattr(request.ctx, self._connection_key)

        logger.debug("Database resources cleaned up for request %s", request.id)

    def _should_commit_transaction(self, status_code: int) -> bool:
        """Determine if transaction should be committed based on status code.

        Args:
            status_code: HTTP response status code.

        Returns:
            True if transaction should be committed, False otherwise.
        """
        if self.database_config.commit_mode == "manual":
            return False

        http_ok = 200
        http_multiple_choices = 300
        http_bad_request = 400

        should_commit = False

        if self.database_config.commit_mode == "autocommit":
            should_commit = http_ok <= status_code < http_multiple_choices
        elif self.database_config.commit_mode == "autocommit_include_redirect":
            should_commit = http_ok <= status_code < http_bad_request

        # Apply extra status overrides
        if self.database_config.extra_commit_statuses and status_code in self.database_config.extra_commit_statuses:
            should_commit = True
        elif (
            self.database_config.extra_rollback_statuses
            and status_code in self.database_config.extra_rollback_statuses
        ):
            should_commit = False

        return should_commit
