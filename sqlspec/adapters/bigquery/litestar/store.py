"""BigQuery-specific session store handler."""

from typing import Any

from sqlspec.extensions.litestar.store import BaseStoreHandler

__all__ = ("StoreHandler",)


class StoreHandler(BaseStoreHandler):
    """BigQuery-specific session store handler.

    BigQuery has native JSON support but uses standard SQL features.
    Uses check-update-insert pattern since BigQuery doesn't support UPSERT syntax.
    """

    def serialize_data(self, data: Any) -> Any:
        """Serialize session data for BigQuery JSON storage.

        Args:
            data: Session data to serialize

        Returns:
            JSON string for BigQuery JSON columns
        """
        # BigQuery handles JSON as strings internally
        return super().serialize_data(data)
