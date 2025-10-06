"""Psycopg ADK store for Google Agent Development Kit session/event storage."""

from typing import TYPE_CHECKING

from sqlspec.adapters.asyncpg.adk.store import AsyncpgADKStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig

logger = get_logger("adapters.psycopg.adk.store")

__all__ = ("PsycopgADKStore",)


class PsycopgADKStore(AsyncpgADKStore):
    """PostgreSQL ADK store using Psycopg driver.

    Inherits from AsyncpgADKStore as both drivers use PostgreSQL and share
    the same SQL dialect. The only difference is the underlying connection
    management, which is handled by the config's provide_connection method.

    Args:
        config: PsycopgAsyncConfig instance.
        session_table: Name of the sessions table. Defaults to "adk_sessions".
        events_table: Name of the events table. Defaults to "adk_events".

    Example:
        from sqlspec.adapters.psycopg import PsycopgAsyncConfig
        from sqlspec.adapters.psycopg.adk import PsycopgADKStore

        config = PsycopgAsyncConfig(pool_config={"conninfo": "postgresql://..."})
        store = PsycopgADKStore(config)
        await store.create_tables()

    Notes:
        - Uses same PostgreSQL SQL dialect as AsyncPG
        - All SQL operations inherited from AsyncpgADKStore
        - Connection management delegated to PsycopgAsyncConfig
        - Parameter placeholders ($1, $2) work identically
    """

    __slots__ = ()

    def __init__(
        self, config: "PsycopgAsyncConfig", session_table: str = "adk_sessions", events_table: str = "adk_events"
    ) -> None:
        """Initialize Psycopg ADK store.

        Args:
            config: PsycopgAsyncConfig instance.
            session_table: Name of the sessions table.
            events_table: Name of the events table.
        """
        super().__init__(config, session_table, events_table)
