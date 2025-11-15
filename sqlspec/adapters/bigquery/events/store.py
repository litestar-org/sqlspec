"""BigQuery event queue store."""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery.config import BigQueryConfig

__all__ = ("BigQueryEventQueueStore",)


class BigQueryEventQueueStore(BaseEventQueueStore["BigQueryConfig"]):
    """Provide BigQuery column mappings for the events queue."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "JSON", "JSON", "TIMESTAMP"

