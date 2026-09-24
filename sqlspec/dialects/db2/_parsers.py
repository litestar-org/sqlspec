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
    "DB2_DURATION_UNITS",
    "DB2_SPECIAL_REGISTERS",
    "DB2_TAIL_ARG_KEYS",
    "Db2StatementTail",
    "apply_statement_tail",
    "apply_statement_tails",
    "mark_native_locks",
    "normalize_db2_expression",
    "normalize_db2_tokens",
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

DB2_SPECIAL_REGISTERS: Final[tuple[str, ...]] = (
    "CURRENT TIME ZONE",
    "CURRENT TIMEZONE",
    "CURRENT SERVER",
    "CURRENT PATH",
    "CURRENT DEGREE",
    "CURRENT ISOLATION",
    "CURRENT LOCK TIMEOUT",
    "CURRENT MEMBER",
    "CURRENT CLIENT_APPLNAME",
    "CURRENT CLIENT_USERID",
    "CURRENT CLIENT_WRKSTNNAME",
    "CURRENT CLIENT_ACCTNG",
)
DB2_DURATION_UNITS: Final[frozenset[str]] = frozenset({
    "YEAR",
    "YEARS",
    "MONTH",
    "MONTHS",
    "DAY",
    "DAYS",
    "HOUR",
    "HOURS",
    "MINUTE",
    "MINUTES",
    "SECOND",
    "SECONDS",
    "MICROSECOND",
    "MICROSECONDS",
})

_POSSTR_ARGUMENT_COUNT: Final[int] = 2
_REGISTER_WORDS: Final[tuple[tuple[str, ...], ...]] = tuple(
    sorted((tuple(register.split()) for register in DB2_SPECIAL_REGISTERS), key=len, reverse=True)
)
_LOB_TYPE_NAMES: Final[frozenset[str]] = frozenset({"BLOB", "CLOB", "DBCLOB"})
_LOB_SIZE_UNITS: Final[frozenset[str]] = frozenset({"K", "M", "G"})
_DURATION_OPERATORS: Final[frozenset[TokenType]] = frozenset({TokenType.PLUS, TokenType.DASH})
_IDENTIFIER_TOKEN_TYPES: Final[frozenset[TokenType]] = frozenset({TokenType.VAR, TokenType.IDENTIFIER})
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


def _canonical_db2_node(node: exp.Expr) -> exp.Expr:
    if (
        isinstance(node, exp.Column)
        and not node.table
        and isinstance(node.this, exp.Identifier)
        and not node.this.quoted
        and node.this.name.upper() == "CURRENT SCHEMA"
    ):
        return exp.CurrentSchema()
    if (
        isinstance(node, exp.Anonymous)
        and str(node.this).upper() == "POSSTR"
        and len(node.expressions) == _POSSTR_ARGUMENT_COUNT
    ):
        haystack, needle = node.expressions
        return exp.StrPosition(this=haystack, substr=needle)
    return node


def normalize_db2_expression(expression: exp.Expr) -> exp.Expr:
    """Rewrite Db2-only spellings into canonical sqlglot expressions.

    ``POSSTR(haystack, needle)`` becomes ``exp.StrPosition`` and the
    ``CURRENT SCHEMA`` special register becomes ``exp.CurrentSchema``.

    Args:
        expression: Expression parsed from Db2 SQL.

    Returns:
        The normalised expression.
    """
    return expression.transform(_canonical_db2_node, copy=False)


def _merged_token(token_type: TokenType, text: str, parts: "list[Token]") -> Token:
    first = parts[0]
    comments = [comment for part in parts for comment in part.comments]
    return Token(token_type, text, first.line, first.col, first.start, parts[-1].end, comments)


def _merge_special_registers(tokens: "list[Token]") -> "list[Token]":
    result: list[Token] = []
    index = 0
    total = len(tokens)
    while index < total:
        token = tokens[index]
        if token.token_type in _QUOTED_TOKEN_TYPES or not token.text.upper().startswith("CURRENT"):
            result.append(token)
            index += 1
            continue
        merged_end = -1
        merged_words: tuple[str, ...] = ()
        for register_words in _REGISTER_WORDS:
            words: list[str] = []
            end = index
            while (
                end < total and len(words) < len(register_words) and tokens[end].token_type not in _QUOTED_TOKEN_TYPES
            ):
                words.extend(tokens[end].text.upper().split())
                end += 1
            if tuple(words) == register_words:
                merged_end = end
                merged_words = register_words
                break
        if merged_end < 0:
            result.append(token)
            index += 1
            continue
        result.append(_merged_token(TokenType.VAR, " ".join(merged_words), tokens[index:merged_end]))
        index = merged_end
    return result


def _merge_lob_sizes(tokens: "list[Token]") -> "list[Token]":
    result: list[Token] = []
    index = 0
    total = len(tokens)
    while index < total:
        token = tokens[index]
        result.append(token)
        index += 1
        if (
            token.token_type not in _QUOTED_TOKEN_TYPES
            and token.text.upper() in _LOB_TYPE_NAMES
            and index + 3 < total
            and tokens[index].token_type == TokenType.L_PAREN
            and tokens[index + 1].token_type == TokenType.NUMBER
            and tokens[index + 2].token_type == TokenType.VAR
            and tokens[index + 2].text.upper() in _LOB_SIZE_UNITS
            and tokens[index + 3].token_type == TokenType.R_PAREN
        ):
            number, unit = tokens[index + 1], tokens[index + 2]
            result.append(tokens[index])
            result.append(_merged_token(TokenType.VAR, f"{number.text}{unit.text.upper()}", [number, unit]))
            index += 3
    return result


def _closing_paren_index(tokens: "list[Token]", index: int) -> int:
    depth = 0
    for position in range(index, len(tokens)):
        token_type = tokens[position].token_type
        if token_type == TokenType.L_PAREN:
            depth += 1
        elif token_type == TokenType.R_PAREN:
            depth -= 1
            if depth == 0:
                return position
    return -1


def _duration_operand_end(tokens: "list[Token]", index: int) -> int:
    total = len(tokens)
    token_type = tokens[index].token_type
    if token_type in {TokenType.NUMBER, TokenType.PLACEHOLDER}:
        return index + 1
    if token_type in {TokenType.PARAMETER, TokenType.COLON}:
        if index + 1 < total and tokens[index + 1].token_type in {TokenType.VAR, TokenType.NUMBER}:
            return index + 2
        return index + 1 if token_type == TokenType.PARAMETER else -1
    if token_type == TokenType.L_PAREN:
        closing = _closing_paren_index(tokens, index)
        return closing + 1 if closing >= 0 else -1
    if token_type not in _IDENTIFIER_TOKEN_TYPES:
        return -1
    end = index + 1
    while (
        end + 1 < total
        and tokens[end].token_type == TokenType.DOT
        and tokens[end + 1].token_type in _IDENTIFIER_TOKEN_TYPES
    ):
        end += 2
    if end < total and tokens[end].token_type == TokenType.L_PAREN:
        closing = _closing_paren_index(tokens, end)
        return closing + 1 if closing >= 0 else -1
    return end


def _mark_labeled_durations(tokens: "list[Token]") -> "list[Token]":
    insert_before: set[int] = set()
    for index in range(1, len(tokens)):
        if tokens[index - 1].token_type not in _DURATION_OPERATORS:
            continue
        end = _duration_operand_end(tokens, index)
        if (
            0 < end < len(tokens)
            and tokens[end].token_type == TokenType.VAR
            and tokens[end].text.upper() in DB2_DURATION_UNITS
        ):
            insert_before.add(index)
    if not insert_before:
        return tokens
    result: list[Token] = []
    for index, token in enumerate(tokens):
        if index in insert_before:
            result.append(Token(TokenType.INTERVAL, "INTERVAL", token.line, token.col, token.start, token.start))
        result.append(token)
    return result


def _wrap_values_rows(statement: "list[Token]") -> "list[Token]":
    if not statement or statement[0].token_type != TokenType.VALUES:
        return statement
    if len(statement) == 1 or statement[1].token_type == TokenType.L_PAREN:
        return statement
    rows: list[list[Token]] = [[]]
    depth = 0
    for token in statement[1:]:
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN:
            depth -= 1
        if token.token_type == TokenType.COMMA and depth == 0:
            rows.append([token])
            continue
        rows[-1].append(token)
    result: list[Token] = [statement[0]]
    for row in rows:
        separator = row[:1] if row and row[0].token_type == TokenType.COMMA else []
        items = row[len(separator) :]
        if not items:
            return statement
        result.extend(separator)
        first, last = items[0], items[-1]
        result.append(Token(TokenType.L_PAREN, "(", first.line, first.col, first.start, first.start))
        result.extend(items)
        result.append(Token(TokenType.R_PAREN, ")", last.line, last.col, last.end, last.end))
    return result


def _wrap_statement_values(tokens: "list[Token]") -> "list[Token]":
    result: list[Token] = []
    statement: list[Token] = []
    for token in tokens:
        if token.token_type == TokenType.SEMICOLON:
            result.extend(_wrap_values_rows(statement))
            result.append(token)
            statement = []
        else:
            statement.append(token)
    result.extend(_wrap_values_rows(statement))
    return result


def normalize_db2_tokens(tokens: "list[Token]") -> "list[Token]":
    """Rewrite Db2-only token sequences into forms sqlglot's parser accepts.

    Special registers without a sqlglot node (``CURRENT TIME ZONE``,
    ``CURRENT SERVER``, ...) become one identifier token carrying the register
    name; LOB sizes such as ``CLOB(1M)`` become one size token; labeled
    durations such as ``a + 1 DAYS`` gain an ``INTERVAL`` token; and a
    statement-level ``VALUES`` list without parentheses gets one parenthesised
    row per item.

    Args:
        tokens: Tokens produced by the Db2 tokenizer.

    Returns:
        The normalised token list.
    """
    tokens = _merge_special_registers(tokens)
    tokens = _merge_lob_sizes(tokens)
    tokens = _mark_labeled_durations(tokens)
    return _wrap_statement_values(tokens)
