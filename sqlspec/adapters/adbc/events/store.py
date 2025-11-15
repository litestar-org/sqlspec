"""ADBC event queue store with multi-dialect support."""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.adbc.config import AdbcConfig

__all__ = ("AdbcEventQueueStore",)


class AdbcEventQueueStore(BaseEventQueueStore["AdbcConfig"]):
    """Map queue column types based on the configured dialect."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        dialect = self._dialect()
        if "postgres" in dialect or "pg" in dialect:
            return "JSONB", "JSONB", "TIMESTAMPTZ"
        if "oracle" in dialect:
            return "CLOB", "CLOB", "TIMESTAMP"
        if "mysql" in dialect or "maria" in dialect:
            return "JSON", "JSON", "DATETIME(6)"
        if "bigquery" in dialect or "bq" in dialect:
            return "JSON", "JSON", "TIMESTAMP"
        if "duckdb" in dialect:
            return "JSON", "JSON", "TIMESTAMP"
        return "TEXT", "TEXT", "TIMESTAMP"

    def _dialect(self) -> str:
        statement_config = self._config.statement_config
        dialect_value = getattr(statement_config, "dialect", None)
        if dialect_value:
            return str(dialect_value).lower()
        return "sqlite"

