"""Db2 token and AST normalisation helpers.

Db2 select-statement tails (isolation, lock request, ``OPTIMIZE FOR``,
``FOR READ ONLY`` and ``SKIP LOCKED DATA``) are removed from the token stream
before sqlglot parses a statement and are stored as ``sqlspec_db2_*`` args on
the parsed query, where the Db2 generator handlers render them back.
"""

from typing import Final, final

from sqlglot import exp
from sqlglot.errors import ParseError
from sqlglot.tokenizer_core import Token, TokenType

__all__ = (
    "DB2_TAIL_ARG_KEYS",
    "Db2StatementTail",
    "apply_statement_tail",
    "apply_statement_tails",
    "mark_native_locks",
    "normalize_db2_expression",
    "split_statement_tail",
    "split_statement_tails",
)

DB2_TAIL_ARG_KEYS: Final[tuple[str, ...]] = (
    "sqlspec_db2_isolation",
    "sqlspec_db2_lock_request",
    "sqlspec_db2_optimize_rows",
    "sqlspec_db2_read_only",
    "sqlspec_db2_skip_locked",
)

_POSSTR_ARGUMENT_COUNT: Final[int] = 2
_ISOLATION_LEVELS: Final[frozenset[str]] = frozenset({"RR", "RS", "CS", "UR"})
_LOCK_REQUEST_ISOLATION_LEVELS: Final[frozenset[str]] = frozenset({"RR", "RS"})
_LOCK_REQUESTS: Final[frozenset[str]] = frozenset({"SHARE", "UPDATE", "EXCLUSIVE"})
_QUOTED_TOKEN_TYPES: Final[frozenset[TokenType]] = frozenset({
    TokenType.STRING,
    TokenType.IDENTIFIER,
    TokenType.NATIONAL_STRING,
    TokenType.RAW_STRING,
    TokenType.BYTE_STRING,
    TokenType.HEX_STRING,
    TokenType.BIT_STRING,
    TokenType.HEREDOC_STRING,
    TokenType.UNICODE_STRING,
})


@final
class Db2StatementTail:
    """Db2 select-statement clauses that follow ``FOR UPDATE`` / ``FETCH FIRST``."""

    __slots__ = ("isolation", "lock_request", "optimize_rows", "read_only", "skip_locked")

    def __init__(self) -> None:
        self.isolation: str | None = None
        self.lock_request: str | None = None
        self.optimize_rows: int | None = None
        self.read_only: bool = False
        self.skip_locked: bool = False


_SKIP_LOCKED_DATA: Final[tuple[frozenset[str] | None, ...]] = (
    frozenset({"SKIP"}),
    frozenset({"LOCKED"}),
    frozenset({"DATA"}),
)
_ISOLATION_WITH_LOCK_REQUEST: Final[tuple[frozenset[str] | None, ...]] = (
    frozenset({"WITH"}),
    _LOCK_REQUEST_ISOLATION_LEVELS,
    frozenset({"USE"}),
    frozenset({"AND"}),
    frozenset({"KEEP"}),
    _LOCK_REQUESTS,
    frozenset({"LOCKS"}),
)
_ISOLATION: Final[tuple[frozenset[str] | None, ...]] = (frozenset({"WITH"}), _ISOLATION_LEVELS)
_OPTIMIZE_FOR_ROWS: Final[tuple[frozenset[str] | None, ...]] = (
    frozenset({"OPTIMIZE"}),
    frozenset({"FOR"}),
    None,
    frozenset({"ROW", "ROWS"}),
)
_READ_ONLY: Final[tuple[frozenset[str] | None, ...]] = (
    frozenset({"FOR"}),
    frozenset({"READ", "FETCH"}),
    frozenset({"ONLY"}),
)


def _ends_with(words: "list[str]", end: int, pattern: "tuple[frozenset[str] | None, ...]") -> bool:
    start = end - len(pattern)
    if start < 0:
        return False
    return all(options is None or words[start + offset] in options for offset, options in enumerate(pattern))


def _words(tokens: "list[Token]") -> "list[str]":
    return ["" if token.token_type in _QUOTED_TOKEN_TYPES else token.text.upper() for token in tokens]


def split_statement_tail(tokens: "list[Token]") -> "tuple[list[Token], Db2StatementTail | None]":
    """Remove the Db2 select-statement tail from the tokens of one statement.

    Clauses are matched from the end in Db2 order: ``SKIP LOCKED DATA``, then
    ``WITH RR|RS|CS|UR`` (optionally ``USE AND KEEP SHARE|UPDATE|EXCLUSIVE LOCKS``
    after ``RR`` or ``RS``), then ``OPTIMIZE FOR n ROW|ROWS``, then
    ``FOR READ ONLY`` or ``FOR FETCH ONLY``.

    Args:
        tokens: Tokens of a single statement.

    Returns:
        The remaining tokens and the matched tail, or the original tokens and
        ``None`` when no tail clause is present.
    """
    words = _words(tokens)
    end = len(tokens)
    tail = Db2StatementTail()
    matched = False

    if _ends_with(words, end, _SKIP_LOCKED_DATA):
        tail.skip_locked = True
        end -= len(_SKIP_LOCKED_DATA)
        matched = True

    if _ends_with(words, end, _ISOLATION_WITH_LOCK_REQUEST):
        start = end - len(_ISOLATION_WITH_LOCK_REQUEST)
        tail.isolation = words[start + 1]
        tail.lock_request = words[start + 5]
        end = start
        matched = True
    elif _ends_with(words, end, _ISOLATION):
        tail.isolation = words[end - 1]
        end -= len(_ISOLATION)
        matched = True

    if _ends_with(words, end, _OPTIMIZE_FOR_ROWS):
        rows_token = tokens[end - 2]
        if rows_token.token_type == TokenType.NUMBER and rows_token.text.isdigit():
            tail.optimize_rows = int(rows_token.text)
            end -= len(_OPTIMIZE_FOR_ROWS)
            matched = True

    if _ends_with(words, end, _READ_ONLY):
        tail.read_only = True
        end -= len(_READ_ONLY)
        matched = True

    if not matched:
        return tokens, None
    return tokens[:end], tail


def split_statement_tails(tokens: "list[Token]") -> "tuple[list[Token], list[Db2StatementTail | None]]":
    """Remove Db2 statement tails from every statement in a token stream.

    Statements are chunked on semicolons the same way sqlglot's parser chunks
    them, so the returned tails align with the parsed expressions.

    Args:
        tokens: Tokens of one or more statements.

    Returns:
        The remaining tokens and one tail (or ``None``) per statement chunk.
    """
    remaining: list[Token] = []
    tails: list[Db2StatementTail | None] = []
    statement: list[Token] = []
    for token in tokens:
        if token.token_type != TokenType.SEMICOLON:
            statement.append(token)
            continue
        statement_tokens, tail = split_statement_tail(statement)
        remaining.extend(statement_tokens)
        tails.append(tail)
        if token.comments:
            tails.append(None)
        remaining.append(token)
        statement = []
    if not tokens or tokens[-1].token_type != TokenType.SEMICOLON:
        statement_tokens, tail = split_statement_tail(statement)
        remaining.extend(statement_tokens)
        tails.append(tail)
    return remaining, tails


def apply_statement_tail(expression: exp.Expr, tail: Db2StatementTail) -> None:
    """Store a Db2 statement tail on the parsed query as ``sqlspec_db2_*`` args.

    Args:
        expression: Top-level expression parsed from the statement.
        tail: Tail removed from the statement's tokens.

    Raises:
        ParseError: If the statement is not a query.
    """
    if not isinstance(expression, exp.Query):
        msg = "Db2 isolation, lock request, OPTIMIZE FOR and FOR READ ONLY clauses are supported on queries only."
        raise ParseError(msg)
    if tail.isolation is not None:
        expression.set("sqlspec_db2_isolation", tail.isolation)
    if tail.lock_request is not None:
        expression.set("sqlspec_db2_lock_request", tail.lock_request)
    if tail.optimize_rows is not None:
        expression.set("sqlspec_db2_optimize_rows", tail.optimize_rows)
    if tail.read_only:
        expression.set("sqlspec_db2_read_only", True)
    if tail.skip_locked:
        expression.set("sqlspec_db2_skip_locked", True)


def apply_statement_tails(
    expressions: "list[exp.Expr | None]", tails: "list[Db2StatementTail | None]"
) -> "list[exp.Expr | None]":
    """Attach per-statement tails to parsed expressions and normalise them.

    Args:
        expressions: Expressions parsed from the tail-stripped tokens.
        tails: Tails returned by ``split_statement_tails``.

    Returns:
        The normalised expressions.

    Raises:
        ParseError: If tails cannot be aligned with the parsed statements.
    """
    if len(expressions) != len(tails) and any(tail is not None for tail in tails):
        msg = "Db2 statement tail clauses could not be matched to the parsed statements."
        raise ParseError(msg)
    normalized: list[exp.Expr | None] = []
    for index, expression in enumerate(expressions):
        if expression is None:
            normalized.append(None)
            continue
        tail = tails[index] if index < len(tails) else None
        if tail is not None:
            apply_statement_tail(expression, tail)
        mark_native_locks(expression)
        normalized.append(normalize_db2_expression(expression))
    return normalized


def mark_native_locks(expression: exp.Expr) -> None:
    """Flag every lock in a Db2-parsed statement as written in Db2 syntax.

    Args:
        expression: Expression parsed from Db2 SQL.
    """
    for lock in expression.find_all(exp.Lock):
        lock.set("sqlspec_db2_native", True)


def _posstr_to_str_position(node: exp.Expr) -> exp.Expr:
    if (
        isinstance(node, exp.Anonymous)
        and str(node.this).upper() == "POSSTR"
        and len(node.expressions) == _POSSTR_ARGUMENT_COUNT
    ):
        haystack, needle = node.expressions
        return exp.StrPosition(this=haystack, substr=needle)
    return node


def normalize_db2_expression(expression: exp.Expr) -> exp.Expr:
    """Rewrite Db2-only function spellings into canonical sqlglot expressions.

    ``POSSTR(haystack, needle)`` becomes ``exp.StrPosition``.

    Args:
        expression: Expression parsed from Db2 SQL.

    Returns:
        The normalised expression.
    """
    return expression.transform(_posstr_to_str_position, copy=False)
