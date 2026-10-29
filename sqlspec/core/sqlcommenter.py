"""Google SQLCommenter support — structured SQL comments for query attribution.

Implements the `SQLCommenter spec <https://google.github.io/sqlcommenter/spec/>`_
using sqlglot AST-level comment manipulation. Comments are added to the parsed
expression tree and coexist with existing comments and optimizer hints.
"""

from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, ClassVar, Final, TypedDict, final
from urllib.parse import quote, unquote

from sqlglot import exp

from sqlspec.observability import get_trace_context
from sqlspec.utils.correlation import CorrelationContext

__all__ = (
    "SQLCommenterAttributes",
    "SQLCommenterContext",
    "append_comment",
    "append_sqlcomment",
    "append_wire_sqlcomment",
    "create_sqlcommenter_statement_transformer",
    "format_wire_sqlcomment",
    "generate_comment",
    "parse_comment",
    "split_sqlcomment",
    "strip_sqlcomment",
)


class SQLCommenterAttributes(TypedDict, total=False):
    """Structured attributes appended as SQL comments for query attribution."""

    db_driver: str
    """Database driver identifier (such as ``asyncpg`` or ``oracledb``)."""
    framework: str
    """Application web framework name (such as ``litestar`` or ``fastapi``)."""
    route: str
    """Matched HTTP route template or URL pattern."""
    controller: str
    """Controller or router class handling the active request."""
    action: str
    """Endpoint handler or action name handling the active request."""
    correlation_id: str
    """Request correlation identifier propagated from ``CorrelationContext``."""
    traceparent: str
    """W3C Trace Context ``traceparent`` header value for distributed tracing."""
    tracestate: str
    """W3C Trace Context ``tracestate`` vendor state string."""


_sqlcommenter_ctx: Final[ContextVar[dict[str, str] | None]] = ContextVar("_sqlcommenter_ctx", default=None)


class SQLCommenterContext:
    """Request-scoped storage for sqlcommenter attributes via contextvars.

    Framework middlewares set attributes per-request, and the sqlcommenter
    statement transformer reads them at compile time.
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


def generate_comment(attrs: Mapping[str, str | None]) -> str:
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


def append_comment(expression: exp.Expr, attrs: Mapping[str, str | None]) -> exp.Expr:
    """Add sqlcommenter attributes as a comment on a parsed expression.

    Uses sqlglot's ``add_comments()`` API so the comment coexists with
    existing comments and optimizer hints.

    Args:
        expression: Parsed sqlglot expression tree.
        attrs: Attributes to serialize into the comment.

    Returns:
        The expression with the sqlcommenter comment added (mutated in place).
    """
    comment_body = generate_comment(attrs)
    if not comment_body:
        return expression
    expression.add_comments([comment_body])
    return expression


def parse_comment(expression: exp.Expr) -> tuple[exp.Expr, dict[str, str]]:
    """Extract sqlcommenter attributes from a parsed expression's comments.

    Identifies sqlcommenter comments by their ``key='value'`` structure,
    extracts the attributes, and removes the sqlcommenter comment from the
    expression while preserving other comments.

    Args:
        expression: Parsed sqlglot expression tree.

    Returns:
        Tuple of (expression_without_sqlcommenter_comment, parsed_attributes).
        If no sqlcommenter comment is found, returns the expression unchanged
        and an empty dict.
    """
    if not expression.comments:
        return expression, {}

    attrs: dict[str, str] = {}
    remaining_comments: list[str] = []

    for comment in expression.comments:
        stripped = comment.strip()
        if _is_sqlcommenter_comment(stripped):
            for pair in stripped.split(","):
                eq_idx = pair.find("='")
                if eq_idx == -1:
                    continue
                raw_key = pair[:eq_idx]
                raw_value = pair[eq_idx + 2 :]
                raw_value = raw_value.removesuffix("'")
                attrs[_decode(raw_key)] = _decode(raw_value)
        else:
            remaining_comments.append(comment)

    if remaining_comments:
        expression.comments = remaining_comments
    else:
        expression.comments = None

    return expression, attrs


def split_sqlcomment(sql: str) -> tuple[str, str]:
    """Split a leading or trailing sqlcommenter block comment from SQL text.

    Args:
        sql: SQL text that may contain a sqlcommenter block comment.

    Returns:
        Tuple of ``(base_sql, comment_block)`` where ``comment_block`` is the
        full ``/* ... */`` comment or ``""`` when no sqlcommenter comment is present.
    """
    if "/*" not in sql:
        return sql, ""

    stripped = sql.rstrip()
    trailing_semi = stripped.endswith(";")
    candidate = stripped[:-1].rstrip() if trailing_semi else stripped
    if candidate.endswith("*/"):
        open_idx = candidate.rfind("/*")
        if open_idx != -1:
            inner = candidate[open_idx + 2 : -2].strip()
            if _is_sqlcommenter_comment(inner):
                comment_block = candidate[open_idx:].strip()
                prefix = candidate[:open_idx].rstrip()
                base_sql = f"{prefix};" if trailing_semi else prefix
                return base_sql, comment_block

    lstripped = sql.lstrip()
    if lstripped.startswith("/*"):
        close_idx = lstripped.find("*/")
        if close_idx != -1:
            inner = lstripped[2:close_idx].strip()
            if _is_sqlcommenter_comment(inner):
                comment_block = lstripped[: close_idx + 2].strip()
                rest = lstripped[close_idx + 2 :].lstrip()
                return rest, comment_block

    return sql, ""


def strip_sqlcomment(sql: str) -> str:
    """Remove a leading or trailing sqlcommenter block comment from SQL text.

    Args:
        sql: SQL text that may contain a sqlcommenter block comment.

    Returns:
        Clean SQL text without the sqlcommenter block comment.
    """
    base_sql, _ = split_sqlcomment(sql)
    return base_sql


def append_sqlcomment(sql: str, attrs: Mapping[str, str | None]) -> str:
    """Append or replace a sqlcommenter block comment on rendered SQL text.

    Args:
        sql: Rendered SQL text.
        attrs: Attributes to serialize into the comment.

    Returns:
        SQL text with the formatted sqlcommenter comment appended.
    """
    base_sql = strip_sqlcomment(sql)
    return _append_comment(base_sql, attrs)


def format_wire_sqlcomment(
    sql: str,
    config: Any = None,
    *,
    attributes: Mapping[str, str | None] | None = None,
    enable_traceparent: bool = False,
    enable_context: bool = False,
    enable_correlation_id: bool = False,
) -> str:
    """Format wire-level SQL with static and dynamic sqlcommenter attributes.

    Args:
        sql: Compiled SQL string.
        config: Optional ``StatementConfig`` carrying sqlcommenter settings.
        attributes: Optional static attributes when ``config`` is omitted.
        enable_traceparent: Include OpenTelemetry ``traceparent`` when ``config`` is omitted.
        enable_context: Include ``SQLCommenterContext`` and ``CorrelationContext`` when ``config`` is omitted.
        enable_correlation_id: Include ``CorrelationContext`` when ``config`` is omitted.

    Returns:
        SQL string with wire-level sqlcommenter comment applied.
    """
    if config is not None:
        if not getattr(config, "enable_sqlcommenter", False):
            return sql
        static_attrs: Mapping[str, str | None] = getattr(config, "sqlcommenter_attributes", None) or {}
        use_traceparent = bool(getattr(config, "sqlcommenter_enable_traceparent", False))
        use_context = bool(getattr(config, "sqlcommenter_enable_context", True))
        use_correlation_id = bool(getattr(config, "sqlcommenter_enable_correlation_id", False))
    else:
        static_attrs = attributes or {}
        use_traceparent = enable_traceparent
        use_context = enable_context
        use_correlation_id = enable_correlation_id

    merged = _comment_attributes(
        static_attrs,
        enable_traceparent=use_traceparent,
        enable_context=use_context,
        enable_correlation_id=use_correlation_id,
    )
    base_sql = strip_sqlcomment(sql)
    return _append_comment(base_sql, merged)


def append_wire_sqlcomment(
    sql: str,
    config: Any = None,
    *,
    attributes: Mapping[str, str | None] | None = None,
    enable_traceparent: bool = False,
    enable_context: bool = False,
    enable_correlation_id: bool = False,
) -> str:
    """Append wire-level sqlcommenter comment to compiled SQL text.

    Args:
        sql: Compiled SQL string.
        config: Optional ``StatementConfig`` carrying sqlcommenter settings.
        attributes: Optional static attributes when ``config`` is omitted.
        enable_traceparent: Include OpenTelemetry ``traceparent`` when ``config`` is omitted.
        enable_context: Include ``SQLCommenterContext`` and ``CorrelationContext`` when ``config`` is omitted.
        enable_correlation_id: Include ``CorrelationContext`` when ``config`` is omitted.

    Returns:
        SQL string with wire-level sqlcommenter comment applied.
    """
    return format_wire_sqlcomment(
        sql,
        config,
        attributes=attributes,
        enable_traceparent=enable_traceparent,
        enable_context=enable_context,
        enable_correlation_id=enable_correlation_id,
    )


def create_sqlcommenter_statement_transformer(
    *,
    attributes: dict[str, str | None] | None = None,
    enable_traceparent: bool = False,
    enable_context: bool = False,
    enable_correlation_id: bool = False,
) -> Callable[[exp.Expr, Any], tuple[exp.Expr, Any]]:
    """Create a ``statement_transformer`` that adds sqlcommenter comments to the AST.

    Static attributes are pre-serialized at creation time. When
    ``enable_traceparent``, ``enable_context``, or ``enable_correlation_id`` is True,
    dynamic attributes are resolved per invocation.

    Args:
        attributes: Static key-value pairs to include in every comment.
        enable_traceparent: If True, auto-populate ``traceparent`` from the
            current OpenTelemetry span context on each invocation.
        enable_context: If True, read request-scoped attributes from
            :class:`SQLCommenterContext` and merge them with static attributes.
        enable_correlation_id: If True, read ``correlation_id`` from
            :class:`CorrelationContext` when not already present.

    Returns:
        A callable suitable for ``StatementConfig(statement_transformers=[...])``.
    """
    static_attrs: dict[str, str | None] = dict(attributes) if attributes else {}
    is_dynamic = enable_traceparent or enable_context or enable_correlation_id

    if not is_dynamic and not static_attrs:
        return _NOOP_SQLCOMMENTER_TRANSFORMER

    if not is_dynamic:
        return _StaticSQLCommenterTransformer(static_attrs)

    return _DynamicSQLCommenterTransformer(
        static_attrs,
        enable_traceparent=enable_traceparent,
        enable_context=enable_context,
        enable_correlation_id=enable_correlation_id,
    )


def _encode_key(key: str) -> str:
    """URL-encode a key (single quotes become ``%27``)."""
    return quote(key, safe="")


def _encode_value(value: str) -> str:
    """URL-encode a value and wrap in single quotes."""
    return f"'{quote(value, safe='')}'"


def _decode(raw: str) -> str:
    """Reverse URL-encoding."""
    return unquote(raw)


def _is_sqlcommenter_comment(comment: str) -> bool:
    """Check whether a comment string looks like a sqlcommenter payload."""
    stripped = comment.strip()
    return "='" in stripped and stripped.endswith("'")


def _append_comment(sql: str, attrs: Mapping[str, str | None]) -> str:
    """Append a sqlcommenter block to rendered SQL text."""
    comment_body = generate_comment(attrs)
    if not comment_body:
        return sql

    stripped_sql = sql.rstrip()
    if not stripped_sql:
        return sql
    trailing_whitespace = sql[len(stripped_sql) :]
    comment = f"/* {comment_body} */"

    if stripped_sql.endswith(";"):
        before_semicolon = stripped_sql[:-1]
        statement = before_semicolon.rstrip()
        semicolon_padding = before_semicolon[len(statement) :]
        return f"{statement} {comment}{semicolon_padding};{trailing_whitespace}"

    return f"{stripped_sql} {comment}{trailing_whitespace}"


def _comment_attributes(
    static_attrs: Mapping[str, str | None] | None = None,
    *,
    enable_traceparent: bool = False,
    enable_context: bool = False,
    enable_correlation_id: bool = False,
) -> dict[str, str | None]:
    """Resolve static and dynamic sqlcommenter attributes for the current call."""
    merged: dict[str, str | None] = {}
    if enable_context:
        ctx_attrs = SQLCommenterContext.get()
        if ctx_attrs:
            merged.update(ctx_attrs)
    if enable_context or enable_correlation_id:
        correlation_id = CorrelationContext.get()
        if correlation_id and "correlation_id" not in merged:
            merged["correlation_id"] = correlation_id
    if static_attrs:
        merged.update(static_attrs)
    if enable_traceparent:
        trace_id, span_id = get_trace_context()
        if trace_id and span_id:
            merged["traceparent"] = _traceparent(trace_id, span_id)
    return merged


def _traceparent(trace_id: str, span_id: str) -> str:
    """Build a W3C traceparent header value from trace and span IDs."""
    return f"00-{trace_id}-{span_id}-01"


@final
class _NoOpSQLCommenterTransformer:
    __slots__ = ()

    def __call__(self, expression: exp.Expr, params: Any) -> tuple[exp.Expr, Any]:
        return expression, params


@final
class _StaticSQLCommenterTransformer:
    __slots__ = ("_comment_body",)

    def __init__(self, attrs: Mapping[str, str | None]) -> None:
        self._comment_body = generate_comment(attrs)

    def __call__(self, expression: exp.Expr, params: Any) -> tuple[exp.Expr, Any]:
        if self._comment_body:
            expression.add_comments([self._comment_body])
        return expression, params


@final
class _DynamicSQLCommenterTransformer:
    __slots__ = ("_enable_context", "_enable_correlation_id", "_enable_traceparent", "_static_attrs")

    def __init__(
        self,
        static_attrs: Mapping[str, str | None],
        *,
        enable_traceparent: bool,
        enable_context: bool,
        enable_correlation_id: bool = False,
    ) -> None:
        self._static_attrs = dict(static_attrs)
        self._enable_traceparent = enable_traceparent
        self._enable_context = enable_context
        self._enable_correlation_id = enable_correlation_id

    def __call__(self, expression: exp.Expr, params: Any) -> tuple[exp.Expr, Any]:
        merged = _comment_attributes(
            self._static_attrs,
            enable_traceparent=self._enable_traceparent,
            enable_context=self._enable_context,
            enable_correlation_id=self._enable_correlation_id,
        )
        return append_comment(expression, merged), params


_NOOP_SQLCOMMENTER_TRANSFORMER: Final = _NoOpSQLCommenterTransformer()
