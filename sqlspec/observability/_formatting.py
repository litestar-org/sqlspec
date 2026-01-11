"""Formatters for standardized SQLSpec observability logs."""

import logging
from logging import LogRecord
from typing import Any

from sqlspec.utils.logging import StructuredFormatter

__all__ = ("OTelConsoleFormatter", "OTelJSONFormatter")


class OTelConsoleFormatter(logging.Formatter):
    """Console formatter that orders OTel-aligned fields consistently."""

    def __init__(self, datefmt: str | None = None) -> None:
        super().__init__(datefmt=datefmt)
        self._field_order = (
            "db.system",
            "db.operation",
            "trace_id",
            "span_id",
            "correlation_id",
            "duration_ms",
            "rows_affected",
            "execution_mode",
            "is_many",
            "is_script",
            "sqlspec.driver",
            "sqlspec.bind_key",
            "sqlspec.transaction_state",
            "sqlspec.prepared_statement",
            "db.statement",
            "db.statement.truncated",
            "db.statement.length",
            "db.statement.preview_length",
            "db.statement.hash",
        )

    def format(self, record: LogRecord) -> str:
        timestamp = self.formatTime(record, self.datefmt)
        parts = [timestamp, f"[{record.levelname}]", record.getMessage()]
        record_dict = record.__dict__
        parts.extend(
            self._format_field(key, record_dict[key])
            for key in self._field_order
            if key in record_dict and record_dict[key] is not None
        )
        return " ".join(parts)

    @staticmethod
    def _format_field(key: str, value: Any) -> str:
        if isinstance(value, bool):
            return f"{key}={str(value).lower()}"
        return f"{key}={value}"


class OTelJSONFormatter(StructuredFormatter):
    """Structured JSON formatter for OTel-aligned log fields."""
