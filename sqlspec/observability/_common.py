"""Shared utilities for observability instrumentation."""

import functools
import hashlib
import re
from typing import Any

__all__ = (
    "compute_sql_hash",
    "get_trace_context",
    "get_w3c_trace_context",
    "redact_sql_literals",
    "resolve_db_system",
    "resolve_effective_db_system",
)

_LITERAL_PATTERN = re.compile(r"\$([A-Za-z0-9_]*)\$.*?\$\1\$|'(?:''|\\.|[^'\\])*'", re.DOTALL)

_DB_SYSTEM_MAP: "tuple[tuple[str, str], ...]" = (
    ("sqlanywhere", "sqlanywhere"),
    ("sybase", "sybase"),
    ("cockroach", "cockroachdb"),
    ("asyncpg", "postgresql"),
    ("psycopg", "postgresql"),
    ("psqlpy", "postgresql"),
    ("postgres", "postgresql"),
    ("asyncmy", "mysql"),
    ("mariadb", "mariadb"),
    ("mysql", "mysql"),
    ("mssql", "mssql"),
    ("sqlserver", "mssql"),
    ("pyodbc", "mssql"),
    ("aiosqlite", "sqlite"),
    ("sqlite", "sqlite"),
    ("duckdb", "duckdb"),
    ("bigquery", "bigquery"),
    ("spanner", "spanner"),
    ("oracle", "oracle"),
    ("oracledb", "oracle"),
    ("db2", "db2"),
    ("snowflake", "snowflake"),
    ("teradata", "teradata"),
    ("vantage", "teradata"),
    ("adbc", "adbc"),
)


@functools.lru_cache(maxsize=32)
def resolve_db_system(adapter_or_driver: str) -> str:
    """Resolve adapter/driver name to OTel db.system value.

    Args:
        adapter_or_driver: Adapter or driver name.

    Returns:
        OTel db.system value.
    """
    normalized = adapter_or_driver.lower()
    for needle, system in _DB_SYSTEM_MAP:
        if needle in normalized:
            return system
    return "other_sql"


def resolve_effective_db_system(adapter: str, driver: str | None = None, db_system: str | None = None) -> str:
    """Resolve effective OTel db.system from explicit override, adapter, or driver.

    Args:
        adapter: Adapter or configuration class name.
        driver: Optional driver class name used when adapter resolves to ``other_sql``.
        db_system: Optional explicit backend system override from a session driver.

    Returns:
        Resolved OTel db.system value.
    """
    if db_system is not None:
        return db_system
    resolved = resolve_db_system(adapter)
    if resolved == "other_sql" and driver:
        return resolve_db_system(driver)
    return resolved


def get_trace_context() -> "tuple[str | None, str | None]":
    """Extract trace_id and span_id from the current OTel context.

    Returns:
        Tuple of trace_id and span_id if available, otherwise (None, None).
    """
    try:
        from opentelemetry import trace
    except ImportError:
        return None, None

    span: Any | None = trace.get_current_span()
    if span is None or not span.is_recording():
        return None, None

    ctx = span.get_span_context()
    if not ctx or not ctx.is_valid:
        return None, None

    if not ctx.trace_id or not ctx.span_id:
        return None, None

    return format(ctx.trace_id, "032x"), format(ctx.span_id, "016x")


def compute_sql_hash(sql: str) -> str:
    """Return the 16-character SHA256 hash for SQL text.

    Args:
        sql: SQL statement text.

    Returns:
        SHA256 hash prefix.
    """
    return hashlib.sha256(sql.encode("utf-8"), usedforsecurity=False).hexdigest()[:16]


def redact_sql_literals(sql: str) -> str:
    """Mask single-quoted (including backslash escapes) and dollar-quoted SQL literals."""
    return _LITERAL_PATTERN.sub("'***'", sql)


_OTEL_TRACE_MODULE: Any = None
_OTEL_TRACE_RESOLVED = False
_SQLCOMMENTER_CONTEXT: Any = None


def _reset_otel_trace_cache() -> None:
    global _OTEL_TRACE_MODULE, _OTEL_TRACE_RESOLVED
    _OTEL_TRACE_MODULE = None
    _OTEL_TRACE_RESOLVED = False


def _get_sqlcommenter_context() -> Any:
    global _SQLCOMMENTER_CONTEXT
    if _SQLCOMMENTER_CONTEXT is None:
        from sqlspec.core.sqlcommenter import SQLCommenterContext

        _SQLCOMMENTER_CONTEXT = SQLCommenterContext
    return _SQLCOMMENTER_CONTEXT.get()


def get_w3c_trace_context() -> tuple[str | None, str]:
    """Extract W3C headers from recording or propagated OpenTelemetry spans."""
    global _OTEL_TRACE_MODULE, _OTEL_TRACE_RESOLVED
    if not _OTEL_TRACE_RESOLVED:
        try:
            from opentelemetry import trace

            _OTEL_TRACE_MODULE = trace
        except ImportError:
            _OTEL_TRACE_MODULE = None
        _OTEL_TRACE_RESOLVED = True
    if _OTEL_TRACE_MODULE is not None:
        span = _OTEL_TRACE_MODULE.get_current_span()
        context = span.get_span_context() if span is not None else None
        if context is not None and context.is_valid and context.trace_id and context.span_id:
            raw_flags = getattr(context, "trace_flags", None)
            flags = int(raw_flags) & 0xFF if raw_flags is not None else 1
            traceparent = f"00-{context.trace_id:032x}-{context.span_id:016x}-{flags:02x}"
            state = getattr(context, "trace_state", None)
            to_header = getattr(state, "to_header", None)
            return traceparent, str(to_header() or "") if callable(to_header) else ""
    fallback = _get_sqlcommenter_context() or {}
    parent = (fallback.get("traceparent") or "").strip()
    return (parent, (fallback.get("tracestate") or "").strip()) if parent else (None, "")
