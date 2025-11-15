"""Psycopg event queue stores for sync and async drivers."""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig

__all__ = (
    "PsycopgAsyncEventQueueStore",
    "PsycopgSyncEventQueueStore",
)


class _BasePsycopgEventQueueStore(BaseEventQueueStore["PsycopgSyncConfig"]):
    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "JSONB", "JSONB", "TIMESTAMPTZ"


class PsycopgSyncEventQueueStore(_BasePsycopgEventQueueStore):
    """Queue DDL for psycopg synchronous configs."""

    __slots__ = ()


class PsycopgAsyncEventQueueStore(BaseEventQueueStore["PsycopgAsyncConfig"]):
    """Queue DDL for psycopg async configs."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "JSONB", "JSONB", "TIMESTAMPTZ"

