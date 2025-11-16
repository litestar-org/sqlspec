"""DuckDB event queue store."""

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.events._store import BaseEventQueueStore

__all__ = ("DuckDBEventQueueStore",)


class DuckDBEventQueueStore(BaseEventQueueStore[DuckDBConfig]):
    """Provide DuckDB-specific column definitions for the queue table."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "TEXT", "TEXT", "TIMESTAMP"
