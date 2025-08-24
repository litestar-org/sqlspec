"""Provider functions for SQLSpec Sanic integration."""

import contextlib
from typing import TYPE_CHECKING, Any, cast

from sqlspec.utils.sync_tools import ensure_async_

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from sqlspec.config import DatabaseConfigProtocol, DriverT
    from sqlspec.typing import ConnectionT, PoolT


__all__ = ("create_connection_provider", "create_pool_provider", "create_session_provider")


def create_pool_provider(
    config: "DatabaseConfigProtocol[Any, Any, Any]", pool_key: str
) -> "Callable[[], Awaitable[PoolT]]":
    """Create provider for database pool access.

    Args:
        config: The database configuration object.
        pool_key: The key used to store the connection pool.

    Returns:
        The pool provider function.
    """

    async def provide_pool() -> "PoolT":
        """Provide the database pool.

        Returns:
            The database connection pool.
        """
        db_pool = await ensure_async_(config.create_pool)()
        return cast("PoolT", db_pool)

    return provide_pool


def create_connection_provider(
    config: "DatabaseConfigProtocol[Any, Any, Any]", pool_key: str, connection_key: str
) -> "Callable[[], AsyncGenerator[ConnectionT, None]]":
    """Create provider for database connections.

    Args:
        config: The database configuration object.
        pool_key: The key used to store the connection pool.
        connection_key: The key used to store the connection.

    Returns:
        The connection provider function.
    """

    async def provide_connection() -> "AsyncGenerator[ConnectionT, None]":
        """Provide a database connection.

        Yields:
            Database connection instance.
        """
        db_pool = await ensure_async_(config.create_pool)()

        try:
            connection_cm = config.provide_connection(db_pool)

            # Handle both context managers and direct connections
            if hasattr(connection_cm, "__aenter__"):
                async with connection_cm as conn:
                    yield cast("ConnectionT", conn)
            else:
                conn = await connection_cm if hasattr(connection_cm, "__await__") else connection_cm
                yield cast("ConnectionT", conn)
        finally:
            with contextlib.suppress(Exception):
                await ensure_async_(config.close_pool)()

    return provide_connection


def create_session_provider(
    config: "DatabaseConfigProtocol[Any, Any, Any]", connection_key: str
) -> "Callable[[ConnectionT], AsyncGenerator[DriverT, None]]":
    """Create provider for database sessions/drivers.

    Args:
        config: The database configuration object.
        connection_key: The key used to access the connection.

    Returns:
        The session provider function.
    """

    async def provide_session(connection: "ConnectionT") -> "AsyncGenerator[DriverT, None]":
        """Provide a database session/driver.

        Args:
            connection: The database connection.

        Yields:
            Database driver/session instance.
        """
        yield cast("DriverT", config.driver_type(connection=connection))

    return provide_session
