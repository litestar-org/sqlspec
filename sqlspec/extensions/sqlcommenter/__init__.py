"""Google SQLCommenter support — structured SQL comments for query attribution.

Implements the `SQLCommenter spec <https://google.github.io/sqlcommenter/spec/>`_:
URL-encode keys/values, escape meta-characters, sort lexicographically,
and append as ``/* key='value' */`` SQL comments.
"""

import re
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, ClassVar, TypedDict
from urllib.parse import quote, unquote

from sqlspec.observability import get_trace_context

__all__ = (
    "SQLCommenterAttributes",
    "SQLCommenterContext",
    "append_comment",
    "create_sqlcommenter_transformer",
    "generate_comment",
    "parse_comment",
)

# Matches a trailing sqlcommenter block comment, optionally followed by a semicolon.
_TRAILING_COMMENT_RE = re.compile(r"\s*/\*(.+?)\*/\s*;?\s*$", re.DOTALL)

# Matches individual key='value' pairs inside a comment.
_PAIR_RE = re.compile(r"((?:[^,=]|(?<=\\),)+?)='((?:[^']|\\')*?)'")

# Detects any block comment anywhere in the SQL.
_HAS_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


class SQLCommenterAttributes(TypedDict, total=False):
    """Structured attributes appended as SQL comments for query attribution."""

    db_driver: str
    framework: str
    route: str
    controller: str
    action: str
    traceparent: str
    tracestate: str


_sqlcommenter_ctx: ContextVar[dict[str, str] | None] = ContextVar("_sqlcommenter_ctx", default=None)


class SQLCommenterContext:
    """Request-scoped storage for sqlcommenter attributes via contextvars.

    Framework middlewares set attributes per-request, and the sqlcommenter
    transformer reads them at compile time.
    """

    _var: ClassVar[ContextVar[dict[str, str] | None]] = _sqlcommenter_ctx

    @classmethod
    def get(cls) -> dict[str, str] | None:
        """Get the current request-scoped attributes."""
        return cls._var.get()

    @classmethod
    def set(cls, attrs: dict[str, str] | None) -> None:
        """Set request-scoped attributes."""
        cls._var.set(attrs)

    @classmethod
    @contextmanager
    def scope(cls, attrs: dict[str, str]) -> Generator[None, None, None]:
        """Context manager that sets attributes for the duration of a block."""
        previous = cls._var.get()
        cls._var.set(attrs)
        try:
            yield
        finally:
            cls._var.set(previous)


def _encode_key(key: str) -> str:
    """URL-encode a key (single quotes become ``%27``)."""
    return quote(key, safe="")


def _encode_value(value: str) -> str:
    """URL-encode a value and wrap in single quotes."""
    return f"'{quote(value, safe='')}'"


def _decode(raw: str) -> str:
    """Reverse URL-encoding."""
    return unquote(raw)


def generate_comment(attrs: dict[str, str | None]) -> str:
    """Serialize attributes into a sqlcommenter comment body.

    Args:
        attrs: Key-value pairs to serialize. ``None`` values are skipped.

    Returns:
        Comma-separated ``key='value'`` pairs sorted lexicographically, or
        empty string if no attributes.
    """
    pairs: list[str] = []
    for key in sorted(attrs):
        value = attrs[key]
        if value is None:
            continue
        pairs.append(f"{_encode_key(key)}={_encode_value(value)}")
    return ",".join(pairs)


def append_comment(sql: str, attrs: dict[str, str | None]) -> str:
    """Append a sqlcommenter comment to a SQL statement.

    Per the spec, if the statement already contains a ``/* */`` comment,
    it is returned unchanged.

    Args:
        sql: The SQL statement.
        attrs: Attributes to serialize into the comment.

    Returns:
        The SQL statement with appended comment, or unchanged if it already
        contains a comment or attributes are empty.
    """
    comment_body = generate_comment(attrs)
    if not comment_body:
        return sql
    if _HAS_COMMENT_RE.search(sql):
        return sql

    stripped = sql.rstrip()
    has_semicolon = stripped.endswith(";")
    if has_semicolon:
        stripped = stripped[:-1].rstrip()
    result = f"{stripped} /*{comment_body}*/"
    if has_semicolon:
        result += ";"
    return result


def parse_comment(sql: str) -> tuple[str, dict[str, str]]:
    """Extract sqlcommenter attributes from a SQL statement.

    Args:
        sql: SQL statement potentially containing a trailing sqlcommenter comment.

    Returns:
        Tuple of (sql_without_comment, parsed_attributes). If no valid
        sqlcommenter comment is found, returns the original SQL and empty dict.
    """
    match = _TRAILING_COMMENT_RE.search(sql)
    if not match:
        return sql, {}

    comment_content = match.group(1).strip()
    pairs = _PAIR_RE.findall(comment_content)
    if not pairs:
        return sql, {}

    # Verify the pairs reconstruct the full comment (not a plain-text comment).
    reconstructed = ",".join(f"{k}='{v}'" for k, v in pairs)
    if reconstructed != comment_content:
        return sql, {}

    attrs: dict[str, str] = {}
    for raw_key, raw_value in pairs:
        attrs[_decode(raw_key)] = _decode(raw_value)

    # Strip the comment from the SQL.
    clean_sql = sql[: match.start()].rstrip()
    return clean_sql, attrs


def _build_traceparent(trace_id: str, span_id: str) -> str:
    """Build a W3C traceparent header value from trace and span IDs."""
    return f"00-{trace_id}-{span_id}-01"


def create_sqlcommenter_transformer(
    *, attributes: dict[str, str | None] | None = None, enable_traceparent: bool = False, enable_context: bool = False
) -> Callable[[str, Any], tuple[str, Any]]:
    """Create an ``output_transformer`` that appends sqlcommenter attributes.

    Static attributes are pre-serialized at creation time for zero per-call
    overhead. When ``enable_traceparent`` is True, the current OpenTelemetry
    trace context is captured at each call. When ``enable_context`` is True,
    attributes from :class:`SQLCommenterContext` are merged at each call.

    Args:
        attributes: Static key-value pairs to include in every comment.
        enable_traceparent: If True, auto-populate ``traceparent`` from the
            current OpenTelemetry span context on each invocation.
        enable_context: If True, read request-scoped attributes from
            :class:`SQLCommenterContext` and merge them with static attributes.

    Returns:
        A callable suitable for ``StatementConfig(output_transformer=...)``.
    """
    static_attrs: dict[str, str | None] = dict(attributes) if attributes else {}
    is_dynamic = enable_traceparent or enable_context

    if not is_dynamic and not static_attrs:

        def _noop_transformer(sql: str, params: Any) -> tuple[str, Any]:
            return sql, params

        return _noop_transformer

    if not is_dynamic:
        # Pure static path — pre-generate the comment body once.
        precomputed_body = generate_comment(static_attrs)

        def _static_transformer(sql: str, params: Any) -> tuple[str, Any]:
            if not precomputed_body or _HAS_COMMENT_RE.search(sql):
                return sql, params
            stripped = sql.rstrip()
            has_semi = stripped.endswith(";")
            if has_semi:
                stripped = stripped[:-1].rstrip()
            result = f"{stripped} /*{precomputed_body}*/"
            if has_semi:
                result += ";"
            return result, params

        return _static_transformer

    def _dynamic_transformer(sql: str, params: Any) -> tuple[str, Any]:
        merged: dict[str, str | None] = {}
        if enable_context:
            ctx_attrs = SQLCommenterContext.get()
            if ctx_attrs:
                merged.update(ctx_attrs)
        # Static attrs override context attrs
        merged.update(static_attrs)
        if enable_traceparent:
            trace_id, span_id = get_trace_context()
            if trace_id and span_id:
                merged["traceparent"] = _build_traceparent(trace_id, span_id)
        return append_comment(sql, merged), params

    return _dynamic_transformer
