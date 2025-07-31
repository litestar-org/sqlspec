from typing import Union

from asyncpg import Connection as AsyncpgNativeConnection
from asyncpg.pool import PoolConnectionProxy
from typing_extensions import TypeAlias

AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection, PoolConnectionProxy]


__all__ = ("AsyncpgConnection",)
