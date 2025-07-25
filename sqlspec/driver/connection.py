"""Consolidated connection management utilities for database drivers.

This module provides centralized connection handling to avoid duplication
across database adapter implementations.
"""

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

from sqlspec.typing import ConnectionT
from sqlspec.utils.type_guards import is_async_transaction_capable, is_sync_transaction_capable

__all__ = (
    "managed_connection_async",
    "managed_connection_sync",
    "managed_transaction_async",
    "managed_transaction_sync",
)


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
        yield cast("Any", connection)
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
async def managed_connection_async(config: Any, provided_connection: Optional[Any] = None) -> "AsyncIterator[Any]":
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
async def managed_transaction_async(connection: Any, auto_commit: bool = True) -> "AsyncIterator[Any]":
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
        yield cast("Any", connection)
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
