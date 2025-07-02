"""Consolidated connection management utilities for database drivers.

This module provides centralized connection handling to avoid duplication
across database adapter implementations.
"""

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, TypeVar, cast

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

from sqlspec.utils.type_guards import (
    is_async_closeable_connection,
    is_async_transaction_capable,
    is_sync_closeable_connection,
    is_sync_transaction_capable,
)

__all__ = (
    "AsyncConnectionContextBase",
    "SyncConnectionContextBase",
    "get_connection_info",
    "managed_connection_async",
    "managed_connection_sync",
    "managed_transaction_async",
    "managed_transaction_sync",
    "validate_pool_config",
)


ConnectionT = TypeVar("ConnectionT")
PoolT = TypeVar("PoolT")


@contextmanager
def managed_connection_sync(config: Any, provided_connection: Optional[ConnectionT] = None) -> "Iterator[ConnectionT]":
    """Context manager for database connections.

    Args:
        config: Database configuration with provide_connection method
        provided_connection: Optional existing connection to use

    Yields:
        Database connection
    """
    if provided_connection is not None:
        yield provided_connection
        return

    # Get connection from config
    with config.provide_connection() as connection:
        yield connection


@contextmanager
def managed_transaction_sync(connection: ConnectionT, auto_commit: bool = True) -> "Iterator[ConnectionT]":
    """Context manager for database transactions.

    Args:
        connection: Database connection
        auto_commit: Whether to auto-commit on success

    Yields:
        Database connection
    """
    # Check if connection already has autocommit enabled
    has_autocommit = getattr(connection, "autocommit", False)

    if not auto_commit or not is_sync_transaction_capable(connection) or has_autocommit:
        yield connection
        return

    try:
        yield cast("ConnectionT", connection)
        cast("Any", connection).commit()
    except Exception:
        # Some databases (like DuckDB) throw an error if rollback is called
        # when no transaction is active. Catch and ignore these specific errors.
        try:
            cast("Any", connection).rollback()
        except Exception as rollback_error:
            # Check if this is a "no transaction active" type error
            error_msg = str(rollback_error).lower()
            if "no transaction" in error_msg or "transaction context error" in error_msg:
                # Ignore rollback errors when no transaction is active
                pass
            else:
                # Re-raise other rollback errors
                raise
        raise


@asynccontextmanager
async def managed_connection_async(
    config: Any, provided_connection: Optional[ConnectionT] = None
) -> "AsyncIterator[ConnectionT]":
    """Async context manager for database connections.

    Args:
        config: Database configuration with provide_connection method
        provided_connection: Optional existing connection to use

    Yields:
        Database connection
    """
    if provided_connection is not None:
        yield provided_connection
        return

    # Get connection from config
    async with config.provide_connection() as connection:
        yield connection


@asynccontextmanager
async def managed_transaction_async(connection: ConnectionT, auto_commit: bool = True) -> "AsyncIterator[ConnectionT]":
    """Async context manager for database transactions.

    Args:
        connection: Database connection
        auto_commit: Whether to auto-commit on success

    Yields:
        Database connection
    """
    # Check if connection already has autocommit enabled
    has_autocommit = getattr(connection, "autocommit", False)

    if not auto_commit or not is_async_transaction_capable(connection) or has_autocommit:
        yield connection
        return

    try:
        yield cast("ConnectionT", connection)
        await cast("Any", connection).commit()
    except Exception:
        # Some databases (like DuckDB) throw an error if rollback is called
        # when no transaction is active. Catch and ignore these specific errors.
        try:
            await cast("Any", connection).rollback()
        except Exception as rollback_error:
            # Check if this is a "no transaction active" type error
            error_msg = str(rollback_error).lower()
            if "no transaction" in error_msg or "transaction context error" in error_msg:
                # Ignore rollback errors when no transaction is active
                pass
            else:
                # Re-raise other rollback errors
                raise
        raise


def get_connection_info(connection: Any) -> dict[str, Any]:
    """Extract connection information for logging/debugging.

    Args:
        connection: Database connection object

    Returns:
        Dictionary of connection information
    """
    info = {"type": type(connection).__name__, "module": type(connection).__module__}

    # Try to get database name
    for attr in ("database", "dbname", "db", "catalog"):
        if hasattr(connection, attr):
            info["database"] = getattr(connection, attr)
            break

    # Try to get host information
    for attr in ("host", "hostname", "server"):
        if hasattr(connection, attr):
            info["host"] = getattr(connection, attr)
            break

    return info


def validate_pool_config(
    min_size: int, max_size: int, max_idle_time: Optional[int] = None, max_lifetime: Optional[int] = None
) -> None:
    """Validate connection pool configuration.

    Args:
        min_size: Minimum pool size
        max_size: Maximum pool size
        max_idle_time: Maximum idle time in seconds
        max_lifetime: Maximum connection lifetime in seconds

    Raises:
        ValueError: If configuration is invalid
    """
    if min_size < 0:
        msg = f"min_size must be >= 0, got {min_size}"
        raise ValueError(msg)

    if max_size < 1:
        msg = f"max_size must be >= 1, got {max_size}"
        raise ValueError(msg)

    if min_size > max_size:
        msg = f"min_size ({min_size}) cannot be greater than max_size ({max_size})"
        raise ValueError(msg)

    if max_idle_time is not None and max_idle_time < 0:
        msg = f"max_idle_time must be >= 0, got {max_idle_time}"
        raise ValueError(msg)

    if max_lifetime is not None and max_lifetime < 0:
        msg = f"max_lifetime must be >= 0, got {max_lifetime}"
        raise ValueError(msg)


class SyncConnectionContextBase:
    """Base class for connection context managers."""

    def __init__(
        self,
        connection: "ConnectionT",  # pyright: ignore
        *,
        close_on_exit: bool = True,
        commit_on_exit: bool = False,
    ) -> None:
        """Initialize connection context.

        Args:
            connection: Database connection
            close_on_exit: Whether to close connection on exit
            commit_on_exit: Whether to commit on successful exit
        """
        self.connection = connection
        self.close_on_exit = close_on_exit
        self.commit_on_exit = commit_on_exit

    def _handle_exit(self, exc_type: Any) -> None:
        """Handle context exit.

        Args:
            exc_type: Exception type if an error occurred
        """
        if exc_type is None and self.commit_on_exit:
            if is_sync_transaction_capable(self.connection):
                self.connection.commit()
        elif exc_type is not None and is_sync_transaction_capable(self.connection):
            self.connection.rollback()

        if self.close_on_exit and is_sync_closeable_connection(self.connection):
            self.connection.close()


class AsyncConnectionContextBase:
    """Base class for async connection context managers."""

    def __init__(
        self,
        connection: "ConnectionT",  # pyright: ignore
        *,
        close_on_exit: bool = True,
        commit_on_exit: bool = False,
    ) -> None:
        """Initialize async connection context.

        Args:
            connection: Database connection
            close_on_exit: Whether to close connection on exit
            commit_on_exit: Whether to commit on successful exit
        """
        self.connection = connection
        self.close_on_exit = close_on_exit
        self.commit_on_exit = commit_on_exit

    async def _handle_exit(self, exc_type: Any) -> None:
        """Handle async context exit.

        Args:
            exc_type: Exception type if an error occurred
        """
        if exc_type is None and self.commit_on_exit:
            if is_async_transaction_capable(self.connection):
                await self.connection.commit()
        elif exc_type is not None and is_async_transaction_capable(self.connection):
            await self.connection.rollback()

        if self.close_on_exit and is_async_closeable_connection(self.connection):
            await self.connection.close()
