"""Psycopg event queue stores for sync and async drivers."""

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.extensions.events._store import BaseEventQueueStore

__all__ = ("PsycopgAsyncEventQueueStore", "PsycopgSyncEventQueueStore")


class _BasePsycopgEventQueueStore(BaseEventQueueStore[PsycopgSyncConfig]):
    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "JSONB", "JSONB", "TIMESTAMPTZ"


class PsycopgSyncEventQueueStore(_BasePsycopgEventQueueStore):
    """Queue DDL for psycopg synchronous configs."""

    __slots__ = ()


class PsycopgAsyncEventQueueStore(BaseEventQueueStore[PsycopgAsyncConfig]):
    """Queue DDL for psycopg async configs."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "JSONB", "JSONB", "TIMESTAMPTZ"
