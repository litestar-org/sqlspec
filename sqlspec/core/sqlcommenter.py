"""Google SQLCommenter support — structured SQL comments for query attribution.

Implements the `SQLCommenter spec <https://google.github.io/sqlcommenter/spec/>`_
using sqlglot AST-level comment manipulation. Comments are added to the parsed
expression tree and coexist with existing comments and optimizer hints.
"""

from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, ClassVar, TypedDict
from urllib.parse import quote, unquote

from sqlglot import exp

from sqlspec.observability import get_trace_context

__all__ = (
    "SQLCommenterAttributes",
    "SQLCommenterContext",
    "append_comment",
    "create_sqlcommenter_statement_transformer",
    "generate_comment",
    "parse_comment",
)

_SQLCOMMENTER_PREFIX = "sqlcommenter:"


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


def _is_sqlcommenter_comment(comment: str) -> bool:
    """Check whether a comment string looks like a sqlcommenter payload."""
    stripped = comment.strip()
    # sqlcommenter comments have key='value' pairs
    return "='" in stripped and stripped.endswith("'")


def append_comment(expression: exp.Expression, attrs: dict[str, str | None]) -> exp.Expression:
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


def parse_comment(expression: exp.Expression) -> tuple[exp.Expression, dict[str, str]]:
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
            # Parse key='value' pairs
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


def _build_traceparent(trace_id: str, span_id: str) -> str:
    """Build a W3C traceparent header value from trace and span IDs."""
    return f"00-{trace_id}-{span_id}-01"


def create_sqlcommenter_statement_transformer(
    *, attributes: dict[str, str | None] | None = None, enable_traceparent: bool = False, enable_context: bool = False
) -> Callable[[exp.Expression, Any], tuple[exp.Expression, Any]]:
    """Create a ``statement_transformer`` that adds sqlcommenter comments to the AST.

    Static attributes are pre-serialized at creation time. When
    ``enable_traceparent`` or ``enable_context`` is True, dynamic attributes
    are resolved per invocation.

    Args:
        attributes: Static key-value pairs to include in every comment.
        enable_traceparent: If True, auto-populate ``traceparent`` from the
            current OpenTelemetry span context on each invocation.
        enable_context: If True, read request-scoped attributes from
            :class:`SQLCommenterContext` and merge them with static attributes.

    Returns:
        A callable suitable for ``StatementConfig(statement_transformers=[...])``.
    """
    static_attrs: dict[str, str | None] = dict(attributes) if attributes else {}
    is_dynamic = enable_traceparent or enable_context

    if not is_dynamic and not static_attrs:

        def _noop_transformer(expression: exp.Expression, params: Any) -> tuple[exp.Expression, Any]:
            return expression, params

        return _noop_transformer

    if not is_dynamic:
        # Pure static path — pre-generate the comment body once.
        precomputed_body = generate_comment(static_attrs)

        def _static_transformer(expression: exp.Expression, params: Any) -> tuple[exp.Expression, Any]:
            if precomputed_body:
                expression.add_comments([precomputed_body])
            return expression, params

        return _static_transformer

    def _dynamic_transformer(expression: exp.Expression, params: Any) -> tuple[exp.Expression, Any]:
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
        comment_body = generate_comment(merged)
        if comment_body:
            expression.add_comments([comment_body])
        return expression, params

    return _dynamic_transformer
