from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar, Union

from sqlspec.base import GenericPoolConfig
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from psycopg import AsyncConnection, Connection
    from psycopg_pool import AsyncConnectionPool, ConnectionPool

    from sqlspec.typing import EmptyType


__all__ = ("PsycoPgGenericPoolConfig",)


ConnectionT = TypeVar("ConnectionT", bound="Union[Connection, AsyncConnection]")
PoolT = TypeVar("PoolT", bound="Union[ConnectionPool, AsyncConnectionPool]")


@dataclass
class PsycoPgGenericPoolConfig(Generic[ConnectionT, PoolT], GenericPoolConfig):
    """Configuration for Psycopg connection pools.

    This class provides configuration options for both synchronous and asynchronous Psycopg
    database connection pools. It supports all standard Psycopg connection parameters and pool-specific
    settings.([1](https://www.psycopg.org/psycopg3/docs/api/pool.html))
    """

    conninfo: "Union[str, EmptyType]" = Empty
    """Connection string in libpq format"""
    kwargs: "Union[dict[str, Any], EmptyType]" = Empty
    """Additional connection parameters"""
    min_size: "Union[int, EmptyType]" = Empty
    """Minimum number of connections in the pool"""
    max_size: "Union[int, EmptyType]" = Empty
    """Maximum number of connections in the pool"""
    name: "Union[str, EmptyType]" = Empty
    """Name of the connection pool"""
    timeout: "Union[float, EmptyType]" = Empty
    """Timeout for acquiring connections"""
    max_waiting: "Union[int, EmptyType]" = Empty
    """Maximum number of waiting clients"""
    max_lifetime: "Union[float, EmptyType]" = Empty
    """Maximum connection lifetime"""
    max_idle: "Union[float, EmptyType]" = Empty
    """Maximum idle time for connections"""
    reconnect_timeout: "Union[float, EmptyType]" = Empty
    """Time between reconnection attempts"""
    num_workers: "Union[int, EmptyType]" = Empty
    """Number of background workers"""
    configure: "Union[Callable[[ConnectionT], None], EmptyType]" = Empty
    """Callback to configure new connections"""
