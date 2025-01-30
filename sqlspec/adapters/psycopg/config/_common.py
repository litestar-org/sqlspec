from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

from sqlspec.base import GenericPoolConfig
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from psycopg import AsyncConnection, Connection
    from psycopg_pool import AsyncConnectionPool, ConnectionPool

    from sqlspec.typing import EmptyType


__all__ = ("PsycoPgGenericPoolConfig",)


ConnectionT = TypeVar("ConnectionT", bound="Connection | AsyncConnection")
PoolT = TypeVar("PoolT", bound="ConnectionPool | AsyncConnectionPool")


@dataclass
class PsycoPgGenericPoolConfig(Generic[ConnectionT, PoolT], GenericPoolConfig):
    """Configuration for Psycopg connection pools.

    This class provides configuration options for both synchronous and asynchronous Psycopg
    database connection pools. It supports all standard Psycopg connection parameters and pool-specific
    settings.([1](https://www.psycopg.org/psycopg3/docs/api/pool.html))
    """

    conninfo: str | EmptyType = Empty
    """Connection string in libpq format"""
    kwargs: dict[str, Any] | EmptyType = Empty
    """Additional connection parameters"""
    min_size: int | EmptyType = Empty
    """Minimum number of connections in the pool"""
    max_size: int | EmptyType = Empty
    """Maximum number of connections in the pool"""
    name: str | EmptyType = Empty
    """Name of the connection pool"""
    timeout: float | EmptyType = Empty
    """Timeout for acquiring connections"""
    max_waiting: int | EmptyType = Empty
    """Maximum number of waiting clients"""
    max_lifetime: float | EmptyType = Empty
    """Maximum connection lifetime"""
    max_idle: float | EmptyType = Empty
    """Maximum idle time for connections"""
    reconnect_timeout: float | EmptyType = Empty
    """Time between reconnection attempts"""
    num_workers: int | EmptyType = Empty
    """Number of background workers"""
    configure: Callable[[ConnectionT], None] | EmptyType = Empty
    """Callback to configure new connections"""
