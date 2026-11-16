"""Shared utilities for observability instrumentation."""

import functools
import hashlib
import re
import sys
from typing import Any, Final

__all__ = (
    "compute_sql_hash",
    "get_trace_context",
    "get_w3c_trace_context",
    "redact_sql_literals",
    "resolve_db_system",
    "resolve_effective_db_system",
)

_QQUOTE_PATTERN: Final[str] = (
    r"(?P<qquote>(?<![A-Za-z0-9_])[nN]?[qQ]'(?:"
    r"\[(?>[^]]+|](?!'))*(?:\]'|\Z)"
    r"|\{(?>[^}]+|\}(?!'))*(?:\}'|\Z)"
    r"|<(?>[^>]+|>(?!'))*(?:>'|\Z)"
    r"|\((?>[^)]+|\)(?!'))*(?:\)'|\Z)"
    r"|(?P<qdelim>[^\s\[{<(])(?:(?!(?P=qdelim)')[\s\S])*(?:(?P=qdelim)'|\Z)"
    r"))"
)
_COMMON_SQL_HEAD: Final[str] = (
    r"(?P<ident>(?:[ \t\r\n,;()=+*<>!|]+|(?:[ACDF-MOPR-WYZ_acdf-mopr-wyz]|[eEbBxXqQ](?!')|[nN](?![qQ]?'))[A-Za-z0-9_]*)+)"
    r"|(?P<comment>--[^\n]*(?:\n|\Z)|/\*[\s\S]*?(?:\*/|\Z))"
    r"|(?P<dquote>\"(?>[^\"]+|\"\")*(?:\"|\Z))"
    r"|(?P<bquote>`(?>[^`]+|``)*(?:`|\Z))"
    r"|(?P<dollar>\$(?P<tag>(?:[A-Za-z_][A-Za-z0-9_]*)?)\$[\s\S]*?(?:\$(?P=tag)\$|\Z))"
    f"|{_QQUOTE_PATTERN}"
    r"|(?P<equote>(?<![A-Za-z0-9_])[eE]'(?>[^'\\]+|''|\\[\s\S])*(?:'|\\?\Z))"
)
_DQUOTE_IDENTIFIER_ALTERNATIVE: Final[str] = r"|(?P<dquote>\"(?>[^\"]+|\"\")*(?:\"|\Z))"
_DQUOTE_STRING_BACKSLASH_ALTERNATIVE: Final[str] = r"|(?P<dstring>\"(?>[^\"\\]+|\"\"|\\[\s\S])*(?:\"|\\?\Z))"
_DQUOTE_STRING_ANSI_ALTERNATIVE: Final[str] = r"|(?P<dstring>\"(?>[^\"]+|\"\")*(?:\"|\Z))"
_NUMERIC_LITERAL_PATTERN: Final[str] = (
    r"|(?P<number>(?<![A-Za-z0-9_.$:?])(?:"
    r"0[xX][0-9A-Fa-f]+(?:_[0-9A-Fa-f]+)*"
    r"|(?:[0-9]+(?:_[0-9]+)*\.[0-9]*(?:_[0-9]+)*|\.[0-9]+(?:_[0-9]+)*|[0-9]+(?:_[0-9]+)*)"
    r"(?:[eE][+-]?[0-9]+(?:_[0-9]+)*)?"
    r")(?![A-Za-z_.]))"
)
_SQL_REDACT_BACKSLASH_PATTERN: Final[re.Pattern[str]] = re.compile(
    f"{_COMMON_SQL_HEAD}"
    r"|(?P<squote>(?:(?<![A-Za-z0-9_])[nNbBxX])?'(?>[^'\\]+|''|\\[\s\S])*(?:'|\\?\Z))"
    f"{_NUMERIC_LITERAL_PATTERN}"
)
_SQL_REDACT_ANSI_PATTERN: Final[re.Pattern[str]] = re.compile(
    f"{_COMMON_SQL_HEAD}"
    r"|(?P<squote>(?:(?<![A-Za-z0-9_])[nNbBxX])?'(?>[^']+|'')*(?:'|\Z))"
    f"{_NUMERIC_LITERAL_PATTERN}"
)
_SQL_REDACT_DQ_STRING_BACKSLASH_PATTERN: Final[re.Pattern[str]] = re.compile(
    _COMMON_SQL_HEAD.replace(_DQUOTE_IDENTIFIER_ALTERNATIVE, _DQUOTE_STRING_BACKSLASH_ALTERNATIVE)
    + r"|(?P<squote>(?:(?<![A-Za-z0-9_])[nNbBxX])?'(?>[^'\\]+|''|\\[\s\S])*(?:'|\\?\Z))"
    + _NUMERIC_LITERAL_PATTERN
)
_SQL_REDACT_DQ_STRING_ANSI_PATTERN: Final[re.Pattern[str]] = re.compile(
    _COMMON_SQL_HEAD.replace(_DQUOTE_IDENTIFIER_ALTERNATIVE, _DQUOTE_STRING_ANSI_ALTERNATIVE)
    + r"|(?P<squote>(?:(?<![A-Za-z0-9_])[nNbBxX])?'(?>[^']+|'')*(?:'|\Z))"
    + _NUMERIC_LITERAL_PATTERN
)
_DQUOTE_STRING_DB_SYSTEMS: Final[frozenset[str]] = frozenset({"mysql", "mariadb", "bigquery"})

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


def _resolve_otel_trace() -> Any:
    """Resolve and cache the optional ``opentelemetry.trace`` module once per process."""
    global _OTEL_TRACE_MODULE, _OTEL_TRACE_RESOLVED
    trace_mod = sys.modules.get("opentelemetry.trace")
    if trace_mod is not None:
        return trace_mod
    otel_pkg = sys.modules.get("opentelemetry")
    if otel_pkg is not None:
        otel_trace = getattr(otel_pkg, "trace", None)
        if otel_trace is not None:
            return otel_trace
    if _OTEL_TRACE_RESOLVED:
        return _OTEL_TRACE_MODULE
    try:
        from opentelemetry import trace

        _OTEL_TRACE_MODULE = trace
    except ImportError:
        _OTEL_TRACE_MODULE = None
    _OTEL_TRACE_RESOLVED = True
    return _OTEL_TRACE_MODULE


def get_trace_context() -> "tuple[str | None, str | None]":
    """Extract trace_id and span_id from the current OTel context.

    Returns:
        Tuple of trace_id and span_id if available, otherwise (None, None).
    """
    trace = _resolve_otel_trace()
    if trace is None:
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


def redact_sql_literals(sql: str, *, dialect: str | None = None) -> str:
    """Mask string, dollar-quoted, Oracle alternative-quoted, and bare numeric SQL literals.

    Args:
        sql: SQL text to redact.
        dialect: Optional dialect, adapter, or db system name. For MySQL, MariaDB, and BigQuery,
            double-quoted spans are string literals and are masked; otherwise they stay visible
            as identifiers.

    Returns:
        SQL text with literal values masked.
    """
    if dialect is not None and resolve_db_system(dialect) in _DQUOTE_STRING_DB_SYSTEMS:
        backslash_pattern = _SQL_REDACT_DQ_STRING_BACKSLASH_PATTERN
        ansi_pattern = _SQL_REDACT_DQ_STRING_ANSI_PATTERN
    else:
        backslash_pattern = _SQL_REDACT_BACKSLASH_PATTERN
        ansi_pattern = _SQL_REDACT_ANSI_PATTERN
    spans, saw_bs_quote = _find_literal_ranges(sql, backslash_pattern)
    if saw_bs_quote:
        ansi_spans, _ = _find_literal_ranges(sql, ansi_pattern)
        spans = _merge_literal_ranges(spans, ansi_spans)
    return _mask_literal_ranges(sql, spans)


_OTEL_TRACE_MODULE: Any = None
_OTEL_TRACE_RESOLVED = False
_SQLCOMMENTER_CONTEXT: Any = None


def get_w3c_trace_context() -> tuple[str | None, str]:
    """Extract W3C headers from recording or propagated OpenTelemetry spans."""
    trace = _resolve_otel_trace()
    if trace is not None:
        span = trace.get_current_span()
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


def _find_literal_ranges(sql: str, pattern: re.Pattern[str]) -> tuple[list[tuple[int, int, bool]], bool]:
    r"""Collect literal redaction intervals and detect ``\'`` sequences inside single-quoted spans."""
    spans: list[tuple[int, int, bool]] = []
    saw_bs_quote = False
    for match in pattern.finditer(sql):
        group = match.lastgroup
        if group in {"ident", "comment", "dquote", "bquote"}:
            continue
        if group == "number":
            spans.append((match.start(), match.end(), False))
            continue
        if not saw_bs_quote and (
            (group == "squote" and "\\'" in match.group(0)) or (group == "dstring" and '\\"' in match.group(0))
        ):
            saw_bs_quote = True
        spans.append((match.start(), match.end(), True))
    return spans, saw_bs_quote


def _merge_literal_ranges(
    primary_spans: list[tuple[int, int, bool]], secondary_spans: list[tuple[int, int, bool]]
) -> list[tuple[int, int, bool]]:
    """Compute the merged sorted union of backslash and ANSI literal intervals."""
    combined = sorted(primary_spans + secondary_spans, key=lambda item: (item[0], item[1]))
    if not combined:
        return []
    merged: list[tuple[int, int, bool]] = []
    cur_start, cur_end, cur_is_string = combined[0]
    for start, end, is_string in combined[1:]:
        if start < cur_end or (start == cur_end and cur_is_string == is_string):
            cur_end = max(cur_end, end)
            cur_is_string = cur_is_string or is_string
        else:
            merged.append((cur_start, cur_end, cur_is_string))
            cur_start, cur_end, cur_is_string = start, end, is_string
    merged.append((cur_start, cur_end, cur_is_string))
    return merged


def _mask_literal_ranges(sql: str, spans: list[tuple[int, int, bool]]) -> str:
    """Replace sorted disjoint intervals with string or numeric redaction masks."""
    if not spans:
        return sql
    parts: list[str] = []
    pos = 0
    for start, end, is_string in spans:
        if start > pos:
            parts.append(sql[pos:start])
        parts.append("'***'" if is_string else "***")
        pos = end
    if pos < len(sql):
        parts.append(sql[pos:])
    return "".join(parts)
