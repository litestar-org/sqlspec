"""Shared utilities for observability instrumentation."""

import functools
import hashlib
import re
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
    r"(?P<comment>--[^\n]*(?:\n|\Z)|/\*[\s\S]*?(?:\*/|\Z))"
    r"|(?P<dquote>\"(?>[^\"]+|\"\")*(?:\"|\Z))"
    r"|(?P<bquote>`(?>[^`]+|``)*(?:`|\Z))"
    r"|(?P<dollar>\$(?P<tag>(?:[A-Za-z_][A-Za-z0-9_]*)?)\$[\s\S]*?(?:\$(?P=tag)\$|\Z))"
    f"|{_QQUOTE_PATTERN}"
    r"|(?P<equote>(?<![A-Za-z0-9_])[eE]'(?>[^'\\]+|''|\\[\s\S])*(?:'|\\?\Z))"
)
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
_TRAILING_BS_SPLIT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?<!\\)(?:\\\\)*\\'[ \t]*(?:,[ \t]*(?:'|[eEnNbBxXqQ]'|[0-9])|[);])"
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
    """Mask string, dollar-quoted, Oracle alternative-quoted, and bare numeric SQL literals."""
    redacted, unclosed, saw_bs_quote, saw_bs_split = _scan_sql_literals(sql, _SQL_REDACT_BACKSLASH_PATTERN, ansi=False)
    if saw_bs_quote and (unclosed or saw_bs_split):
        ansi_redacted, ansi_unclosed, _, ansi_sep_bs = _scan_sql_literals(sql, _SQL_REDACT_ANSI_PATTERN, ansi=True)
        if not ansi_unclosed or ansi_sep_bs:
            return ansi_redacted
    return redacted


_OTEL_TRACE_MODULE: Any = None
_OTEL_TRACE_RESOLVED = False
_SQLCOMMENTER_CONTEXT: Any = None


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


def _has_odd_trailing_backslashes(token: str) -> bool:
    """Return True when ``token`` ends with an odd number of backslashes before its closing quote."""
    body_start = token.find("'") + 1
    if len(token) < body_start + 2 or not token.endswith("'"):
        return False
    backslashes = 0
    idx = len(token) - 2
    while idx >= body_start and token[idx] == "\\":
        backslashes += 1
        idx -= 1
    return backslashes % 2 == 1


def _is_unclosed_squote(token: str, *, ansi: bool) -> bool:
    """Return True when a matched single-quoted literal reached EOF without closing cleanly."""
    body_start = token.find("'") + 1
    if len(token) == body_start or not token.endswith("'"):
        return True
    if ansi:
        return False
    return _has_odd_trailing_backslashes(token)


def _scan_sql_literals(sql: str, pattern: re.Pattern[str], *, ansi: bool) -> tuple[str, bool, bool, bool]:
    """Scan ``sql`` linearly, returning redacted text and single-quote termination signals."""
    parts: list[str] = []
    pos = 0
    unclosed = False
    saw_bs_quote = False
    saw_bs_split = False
    sql_len = len(sql)
    for match in pattern.finditer(sql):
        if match.start() > pos:
            parts.append(sql[pos : match.start()])
        group = match.lastgroup
        token = match.group(0)
        if group in {"comment", "dquote", "bquote"}:
            parts.append(token)
        elif group == "number":
            parts.append("***")
        elif group == "squote":
            parts.append("'***'")
            if "\\" in token:
                if ansi:
                    if _has_odd_trailing_backslashes(token):
                        next_idx = match.end()
                        while next_idx < sql_len and sql[next_idx] in {" ", "\t"}:
                            next_idx += 1
                        if next_idx == sql_len or sql[next_idx] in {",", ")", ";"}:
                            saw_bs_split = True
                elif "\\'" in token:
                    saw_bs_quote = True
                    if _TRAILING_BS_SPLIT_PATTERN.search(token) is not None:
                        saw_bs_split = True
            if match.end() == sql_len and _is_unclosed_squote(token, ansi=ansi):
                unclosed = True
        else:
            parts.append("'***'")
        pos = match.end()
    if pos < sql_len:
        parts.append(sql[pos:])
    return "".join(parts), unclosed, saw_bs_quote, saw_bs_split
