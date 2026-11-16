"""Formatters for standardized SQLSpec observability logs."""

import logging
import re
from logging import LogRecord
from typing import Any, Final

from sqlspec.utils.logging import StructuredFormatter

__all__ = ("OTelConsoleFormatter", "OTelJSONFormatter")

_CONTROL_CHAR_PATTERN: Final[re.Pattern[str]] = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_CONTROL_CHAR_ESCAPES: Final[dict[str, str]] = {"\n": r"\n", "\r": r"\r", "\t": r"\t"}


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
        timestamp = _escape_control_chars(self.formatTime(record, self.datefmt))
        parts = [timestamp, f"[{_escape_control_chars(record.levelname)}]", _escape_control_chars(record.getMessage())]
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
        return f"{key}={_escape_control_chars(str(value))}"


class OTelJSONFormatter(StructuredFormatter):
    """Structured JSON formatter for OTel-aligned log fields."""


def _replace_control_char(match: re.Match[str]) -> str:
    """Replace one C0, DEL, or C1 control character with an escaped printable token."""
    char = match.group(0)
    mapped = _CONTROL_CHAR_ESCAPES.get(char)
    if mapped is not None:
        return mapped
    return f"\\x{ord(char):02x}"


def _escape_control_chars(text: str) -> str:
    """Escape C0, DEL, and C1 control characters in text log outputs."""
    return _CONTROL_CHAR_PATTERN.sub(_replace_control_char, text)
