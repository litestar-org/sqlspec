"""Google ADK session backend extension for SQLSpec.

Provides session and event storage for Google Agent Development Kit using
SQLSpec database adapters.

Example:
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.adapters.asyncpg.adk.store import AsyncpgADKStore
    from sqlspec.extensions.adk import SQLSpecSessionService

    config = AsyncpgConfig(pool_config={"dsn": "postgresql://..."})
    store = AsyncpgADKStore(config)
    await store.create_tables()

    service = SQLSpecSessionService(store)
    session = await service.create_session(
        app_name="my_app",
        user_id="user123",
        state={"key": "value"}
    )
"""

from sqlspec.extensions.adk._types import EventRecord, SessionRecord
from sqlspec.extensions.adk.service import SQLSpecSessionService
from sqlspec.extensions.adk.store import BaseADKStore

__all__ = ("BaseADKStore", "EventRecord", "SQLSpecSessionService", "SessionRecord")
