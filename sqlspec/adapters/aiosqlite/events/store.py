"""AioSQLite event queue store."""

from typing import Any

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig, _apply_extension_pragmas, _extension_pragma_statements
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("AiosqliteEventQueueStore",)


class AiosqliteEventQueueStore(BaseEventQueueStore[AiosqliteConfig]):
    """Provide column definitions for the async SQLite adapter.

    SQLite stores JSON as TEXT since it lacks a native JSON column type.
    JSON functions can still operate on TEXT columns containing valid JSON.
    """

    __slots__ = ("_pragma_statements",)
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({
        "pragma_overrides",
        "pragma_profile",
    })

    def __init__(self, config: AiosqliteConfig) -> None:
        super().__init__(config)
        self._pragma_statements = _extension_pragma_statements(config, "events")

    async def prepare_schema_async(self, driver: Any) -> None:
        """Apply configured SQLite PRAGMAs before queue DDL."""
        await _apply_extension_pragmas(driver.connection, self._pragma_statements)

    def _column_types(self) -> "tuple[str, str, str]":
        """Return SQLite-compatible column types for the event queue."""
        return "TEXT", "TEXT", "TIMESTAMP"
