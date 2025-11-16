"""BigQuery event queue store."""

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.extensions.events._store import BaseEventQueueStore

__all__ = ("BigQueryEventQueueStore",)


class BigQueryEventQueueStore(BaseEventQueueStore[BigQueryConfig]):
    """Provide BigQuery column mappings for the events queue."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "JSON", "JSON", "TIMESTAMP"
