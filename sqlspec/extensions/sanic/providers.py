"""Provider system for SQLSpec Sanic integration.

Provides connection, pool, and session providers for Sanic applications.
"""

from typing import TYPE_CHECKING, Callable, Optional, Union, cast

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sanic import Request

    from sqlspec.config import AsyncConfigT, DriverT, SyncConfigT
    from sqlspec.extensions.sanic.config import DatabaseConfig
    from sqlspec.typing import ConnectionT, PoolT

logger = get_logger("extensions.sanic.providers")

__all__ = (
    "provide_connection",
    "provide_pool",
    "provide_session",
)



def provide_connection(
    config: "Optional[Union[SyncConfigT, AsyncConfigT, DatabaseConfig]]" = None,
) -> "Callable[[Request], ConnectionT]":
    """Create a connection provider for direct database connection access.

    Args:
        config: Optional database configuration.

    Returns:
        A provider function that returns database connections.
    """
    def provider(request: "Request") -> "ConnectionT":
        """Provide database connection from request context.

        Args:
            request: The Sanic request object.

        Returns:
            Database connection instance.
        """
        sqlspec = getattr(request.app.ctx, "sqlspec", None)
        if sqlspec is None:
            msg = "SQLSpec not initialized in application context"
            raise RuntimeError(msg)

        # Determine which config to use
        active_config = config
        if active_config is None:
            if not sqlspec._configs:
                msg = "No database configurations available"
                raise RuntimeError(msg)
            active_config = sqlspec._configs[0]

        # Find the database config for this configuration
        db_config = None
        for cfg in sqlspec._configs:
            if active_config in (cfg.config, getattr(cfg, "annotation", None)):
                db_config = cfg
                break

        if db_config is None:
            msg = f"No configuration found for {active_config}"
            raise KeyError(msg)

        # Get connection from request context
        connection = getattr(request.ctx, db_config.connection_key, None)
        if connection is None:
            msg = f"No connection available for {active_config}. Ensure middleware is enabled."
            raise RuntimeError(msg)

        return cast("ConnectionT", connection)

    return provider


def provide_pool(
    config: "Optional[Union[SyncConfigT, AsyncConfigT, DatabaseConfig]]" = None,
) -> "Callable[[Request], PoolT]":
    """Create a pool provider for database connection pool access.

    Args:
        config: Optional database configuration.

    Returns:
        A provider function that returns database connection pools.
    """
    def provider(request: "Request") -> "PoolT":
        """Provide database connection pool from app context.

        Args:
            request: The Sanic request object.

        Returns:
            Database connection pool instance.
        """
        sqlspec = getattr(request.app.ctx, "sqlspec", None)
        if sqlspec is None:
            msg = "SQLSpec not initialized in application context"
            raise RuntimeError(msg)

        # Determine which config to use
        active_config = config
        if active_config is None:
            if not sqlspec._configs:
                msg = "No database configurations available"
                raise RuntimeError(msg)
            active_config = sqlspec._configs[0]

        # Get pool from app context
        pool = sqlspec.get_engine(request, active_config.config if hasattr(active_config, "config") else active_config)
        if pool is None:
            msg = f"No pool available for {active_config}"
            raise RuntimeError(msg)

        return cast("PoolT", pool)

    return provider


def provide_session(
    config: "Optional[Union[SyncConfigT, AsyncConfigT, DatabaseConfig]]" = None,
) -> "Callable[[Request], DriverT]":
    """Create a session provider for database session/driver access.

    Args:
        config: Optional database configuration.

    Returns:
        A provider function that returns database sessions/drivers.
    """
    def provider(request: "Request") -> "DriverT":
        """Provide database session/driver from request context.

        Args:
            request: The Sanic request object.

        Returns:
            Database session/driver instance.
        """
        sqlspec = getattr(request.app.ctx, "sqlspec", None)
        if sqlspec is None:
            msg = "SQLSpec not initialized in application context"
            raise RuntimeError(msg)

        # Get session using SQLSpec's method
        session = sqlspec.get_session(request, config.config if hasattr(config, "config") else config)
        return cast("DriverT", session)

    return provider


