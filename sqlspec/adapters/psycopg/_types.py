from typing import TYPE_CHECKING

from psycopg.rows import DictRow as PsycopgDictRow

if TYPE_CHECKING:
    from typing import TypeAlias

    from psycopg import AsyncConnection, Connection

    PsycopgSyncConnection: TypeAlias = Connection[PsycopgDictRow]
    PsycopgAsyncConnection: TypeAlias = AsyncConnection[PsycopgDictRow]
else:
    from psycopg import AsyncConnection, Connection

    PsycopgSyncConnection = Connection
    PsycopgAsyncConnection = AsyncConnection

__all__ = ("PsycopgAsyncConnection", "PsycopgDictRow", "PsycopgSyncConnection")
