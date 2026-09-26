"""AioSQLite event queue store."""

from typing import Any

from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.aiosqlite.core import apply_extension_pragmas, extension_pragma_statements
from sqlspec.config import EventsConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("AiosqliteEventQueueStore", "AiosqliteEventsConfig")


class AiosqliteEventsConfig(EventsConfig):
    """Aiosqlite events settings for queue storage and supported native transports."""

    pragma_profile: NotRequired[bool]
    """Apply the SQLite extension-store PRAGMA profile. Default: False."""

    pragma_overrides: NotRequired[dict[str, str | int | bool]]
    """Validated SQLite PRAGMA overrides applied after the optional profile."""


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
        self._pragma_statements = extension_pragma_statements(config, "events")

    async def prepare_schema_async(self, driver: Any) -> None:
        """Apply configured SQLite PRAGMAs before queue DDL."""
        await apply_extension_pragmas(driver.connection, self._pragma_statements)

    def _column_types(self) -> "tuple[str, str, str]":
        """Return SQLite-compatible column types for the event queue."""
        return "TEXT", "TEXT", "TIMESTAMP"
