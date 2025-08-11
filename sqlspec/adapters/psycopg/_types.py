from typing import TYPE_CHECKING

from psycopg.rows import DictRow as PsycopgDictRow

if TYPE_CHECKING:
    from psycopg import AsyncConnection, Connection
    from typing_extensions import TypeAlias

    PsycopgSyncConnection: TypeAlias = Connection[PsycopgDictRow]
    PsycopgAsyncConnection: TypeAlias = AsyncConnection[PsycopgDictRow]
else:
    from psycopg import AsyncConnection, Connection

    PsycopgSyncConnection = Connection
    PsycopgAsyncConnection = AsyncConnection

__all__ = ("PsycopgAsyncConnection", "PsycopgDictRow", "PsycopgSyncConnection")
