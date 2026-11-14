"""Shared utilities for observability instrumentation."""

import functools
import hashlib
import re
from typing import Any, Final

__all__ = (
    "compute_sql_hash",
    "get_trace_context",
    "redact_sql_literals",
    "resolve_db_system",
    "resolve_effective_db_system",
)

_SQL_REDACT_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?P<comment>--[^\n]*(?:\n|\Z)|/\*[\s\S]*?(?:\*/|\Z))"
    r"|(?P<dquote>\"(?>[^\"]+|\"\")*(?:\"|\Z))"
    r"|(?P<dollar>\$(?P<tag>(?:[A-Za-z_][A-Za-z0-9_]*)?)\$[\s\S]*?(?:\$(?P=tag)\$|\Z))"
    r"|(?P<squote>(?:(?<![A-Za-z0-9_])[eEnNbBxX])?'(?>[^'\\]+|''|\\[\s\S])*(?:'|\\?\Z))"
)

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
    """Mask single-quoted and dollar-quoted SQL literals while keeping comments and quoted identifiers."""
    return _SQL_REDACT_TOKEN_PATTERN.sub(_replace_sql_token, sql)


def _replace_sql_token(match: re.Match[str]) -> str:
    """Preserve comments and double-quoted identifiers while masking SQL literals."""
    if match.lastgroup in {"comment", "dquote"}:
        return match.group(0)
    return "'***'"
