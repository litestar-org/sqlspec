"""Psqlpy ADK store for Google Agent Development Kit session/event storage."""

from typing import TYPE_CHECKING

from sqlspec.adapters.asyncpg.adk.store import AsyncpgADKStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig

logger = get_logger("adapters.psqlpy.adk.store")

__all__ = ("PsqlpyADKStore",)


class PsqlpyADKStore(AsyncpgADKStore):
    """PostgreSQL ADK store using Psqlpy driver.

    Inherits from AsyncpgADKStore as both drivers use PostgreSQL and share
    the same SQL dialect. The only difference is the underlying connection
    management, which is handled by the config's provide_connection method.

    Args:
        config: PsqlpyConfig instance.
        session_table: Name of the sessions table. Defaults to "adk_sessions".
        events_table: Name of the events table. Defaults to "adk_events".

    Example:
        from sqlspec.adapters.psqlpy import PsqlpyConfig
        from sqlspec.adapters.psqlpy.adk import PsqlpyADKStore

        config = PsqlpyConfig(pool_config={"dsn": "postgresql://..."})
        store = PsqlpyADKStore(config)
        await store.create_tables()

    Notes:
        - Uses same PostgreSQL SQL dialect as AsyncPG
        - All SQL operations inherited from AsyncpgADKStore
        - Connection management delegated to PsqlpyConfig
        - Parameter placeholders ($1, $2) work identically
    """

    __slots__ = ()

    def __init__(
        self,
        config: "PsqlpyConfig",
        session_table: str = "adk_sessions",
        events_table: str = "adk_events",
    ) -> None:
        """Initialize Psqlpy ADK store.

        Args:
            config: PsqlpyConfig instance.
            session_table: Name of the sessions table.
            events_table: Name of the events table.
        """
        super().__init__(config, session_table, events_table)
