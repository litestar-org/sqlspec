"""Consolidated connection management utilities for database drivers.

This module provides centralized connection handling to avoid duplication
across database adapter implementations.
"""

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

from sqlspec.typing import ConnectionT

__all__ = (
    "managed_connection_async",
    "managed_connection_sync",
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


