"""DuckDB event queue store."""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb.config import DuckDBConfig

__all__ = ("DuckDBEventQueueStore",)


class DuckDBEventQueueStore(BaseEventQueueStore["DuckDBConfig"]):
    """Provide DuckDB-specific column definitions for the queue table."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "TEXT", "TEXT", "TIMESTAMP"

