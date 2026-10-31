"""Sybase ASE and SAP SQL Anywhere token and AST normalization helpers."""

from typing import Final

from sqlglot import exp
from sqlglot.tokenizer_core import Token, TokenType

__all__ = (
    "normalize_sqlanywhere_expression",
    "normalize_sqlanywhere_tokens",
    "normalize_sybase_expression",
    "normalize_sybase_tokens",
)

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
_CHARINDEX_MIN_ARGS: Final[int] = 2
_CHARINDEX_MAX_ARGS: Final[int] = 3
_TOP_START_AT_SPAN: Final[int] = 5


def normalize_sybase_tokens(tokens: "list[Token]") -> "list[Token]":
    """Rewrite Sybase ASE token sequences into forms accepted by the T-SQL parser.

    Trailing ``ROWS LIMIT <n> [OFFSET <m>]`` and ``ROWS OFFSET <m>`` sequences
    have their leading ``ROWS`` token removed so sqlglot parses them into
    canonical ``exp.Limit`` and ``exp.Offset`` nodes, and ``UNITEXT`` tokens
    are expanded to ``NVARCHAR(MAX)`` for round-trip preservation.
    """
    result: list[Token] = []
    index = 0
    total = len(tokens)
    while index < total:
        token = tokens[index]
        if (
            token.token_type == TokenType.ROWS
            and index + 1 < total
            and tokens[index + 1].token_type in {TokenType.LIMIT, TokenType.OFFSET}
        ):
            index += 1
            continue
        if token.token_type not in _QUOTED_TOKEN_TYPES and token.text.upper() == "UNITEXT":
            result.extend(_expand_nvarchar_max(token))
            index += 1
            continue
        result.append(token)
        index += 1
    return result


def normalize_sqlanywhere_tokens(tokens: "list[Token]") -> "list[Token]":
    """Rewrite SAP SQL Anywhere token sequences into forms accepted by the T-SQL parser.

    Normalizes ``LONG VARCHAR``, ``LONG NVARCHAR``, ``LONG BINARY``, and
    ``CURRENT UTC TIMESTAMP`` tokens, and rewrites Watcom-SQL
    ``SELECT [DISTINCT|ALL] TOP <n> START AT <m> ...`` headers into trailing
    0-based ``LIMIT <n> OFFSET <m - 1>`` clauses.
    """
    rewritten = _normalize_sqlanywhere_multiword_tokens(tokens)
    result: list[Token] = []
    statement: list[Token] = []
    for token in rewritten:
        if token.token_type in {TokenType.SEMICOLON, TokenType.UNION, TokenType.INTERSECT, TokenType.EXCEPT}:
            result.extend(_rewrite_top_start_at_statement(statement))
            result.append(token)
            statement = []
        else:
            statement.append(token)
    result.extend(_rewrite_top_start_at_statement(statement))
    return result


def normalize_sybase_expression(expression: exp.Expr) -> exp.Expr:
    """Normalize Sybase ASE function nodes into canonical sqlglot AST expressions."""
    return expression.transform(_canonical_sybase_node, copy=False)


def normalize_sqlanywhere_expression(expression: exp.Expr) -> exp.Expr:
    """Normalize SAP SQL Anywhere function nodes into canonical sqlglot AST expressions."""
    return expression.transform(_canonical_sybase_node, copy=False)


def _expand_nvarchar_max(token: Token) -> "list[Token]":
    return [
        Token(TokenType.NVARCHAR, "NVARCHAR", token.line, token.col, token.start, token.end, token.comments),
        Token(TokenType.L_PAREN, "(", token.line, token.col, token.start, token.end),
        Token(TokenType.VAR, "MAX", token.line, token.col, token.start, token.end),
        Token(TokenType.R_PAREN, ")", token.line, token.col, token.start, token.end),
    ]


def _normalize_sqlanywhere_multiword_tokens(tokens: "list[Token]") -> "list[Token]":
    result: list[Token] = []
    index = 0
    total = len(tokens)
    while index < total:
        token = tokens[index]
        if token.token_type not in _QUOTED_TOKEN_TYPES:
            upper = token.text.upper()
            if upper == "LONG" and index + 1 < total and tokens[index + 1].token_type not in _QUOTED_TOKEN_TYPES:
                second_upper = tokens[index + 1].text.upper()
                if second_upper == "VARCHAR":
                    result.append(
                        Token(TokenType.TEXT, "TEXT", token.line, token.col, token.start, tokens[index + 1].end)
                    )
                    index += 2
                    continue
                if second_upper == "NVARCHAR":
                    result.extend(_expand_nvarchar_max(token))
                    index += 2
                    continue
                if second_upper == "BINARY":
                    result.append(
                        Token(TokenType.BLOB, "BLOB", token.line, token.col, token.start, tokens[index + 1].end)
                    )
                    index += 2
                    continue
            if (
                upper == "CURRENT"
                and index + 2 < total
                and tokens[index + 1].token_type not in _QUOTED_TOKEN_TYPES
                and tokens[index + 2].token_type not in _QUOTED_TOKEN_TYPES
                and tokens[index + 1].text.upper() == "UTC"
                and tokens[index + 2].text.upper() in {"TIMESTAMP", "CURRENT_TIMESTAMP"}
            ):
                result.append(
                    Token(
                        TokenType.CURRENT_TIMESTAMP,
                        "CURRENT_TIMESTAMP",
                        token.line,
                        token.col,
                        token.start,
                        tokens[index + 2].end,
                    )
                )
                index += 3
                continue
        result.append(token)
        index += 1
    return result


def _rewrite_top_start_at_statement(statement: "list[Token]") -> "list[Token]":
    if len(statement) < _TOP_START_AT_SPAN + 1 or statement[0].token_type != TokenType.SELECT:
        return statement
    header_index = 1
    if statement[header_index].token_type in {TokenType.DISTINCT, TokenType.ALL}:
        header_index += 1
    if header_index + _TOP_START_AT_SPAN > len(statement):
        return statement
    top_tok = statement[header_index]
    limit_tok = statement[header_index + 1]
    start_word_tok = statement[header_index + 2]
    at_word_tok = statement[header_index + 3]
    start_val_tok = statement[header_index + 4]
    if (
        top_tok.token_type != TokenType.TOP
        or start_word_tok.token_type in _QUOTED_TOKEN_TYPES
        or at_word_tok.token_type in _QUOTED_TOKEN_TYPES
        or start_word_tok.text.upper() != "START"
        or at_word_tok.text.upper() != "AT"
    ):
        return statement
    if start_val_tok.token_type == TokenType.NUMBER and start_val_tok.text.isdigit():
        offset_str = str(max(int(start_val_tok.text) - 1, 0))
        offset_tokens = [
            Token(
                TokenType.NUMBER,
                offset_str,
                start_val_tok.line,
                start_val_tok.col,
                start_val_tok.start,
                start_val_tok.end,
            )
        ]
    else:
        offset_tokens = [
            Token(
                TokenType.L_PAREN, "(", start_val_tok.line, start_val_tok.col, start_val_tok.start, start_val_tok.start
            ),
            start_val_tok,
            Token(TokenType.DASH, "-", start_val_tok.line, start_val_tok.col, start_val_tok.start, start_val_tok.end),
            Token(TokenType.NUMBER, "1", start_val_tok.line, start_val_tok.col, start_val_tok.end, start_val_tok.end),
            Token(TokenType.R_PAREN, ")", start_val_tok.line, start_val_tok.col, start_val_tok.end, start_val_tok.end),
        ]
    body = statement[header_index + _TOP_START_AT_SPAN :]
    last = statement[-1]
    tail = [
        Token(TokenType.LIMIT, "LIMIT", last.line, last.col, last.end, last.end),
        limit_tok,
        Token(TokenType.OFFSET, "OFFSET", last.line, last.col, last.end, last.end),
        *offset_tokens,
    ]
    insert_idx = len(body)
    for idx in range(len(body) - 1, -1, -1):
        if body[idx].token_type == TokenType.FOR and idx + 1 < len(body):
            next_text = body[idx + 1].text.upper()
            if next_text in {"UPDATE", "READ"}:
                insert_idx = idx
                break
    return [*statement[:header_index], *body[:insert_idx], *tail, *body[insert_idx:]]


def _canonical_sybase_node(node: exp.Expr) -> exp.Expr:
    if isinstance(node, exp.Anonymous):
        func_name = str(node.this).upper()
        if func_name == "CHAR_LENGTH" and len(node.expressions) == 1:
            return exp.Length(this=node.expressions[0])
        if func_name == "CHARINDEX" and _CHARINDEX_MIN_ARGS <= len(node.expressions) <= _CHARINDEX_MAX_ARGS:
            substr = node.expressions[0]
            this = node.expressions[1]
            position = node.expressions[2] if len(node.expressions) == _CHARINDEX_MAX_ARGS else None
            return exp.StrPosition(this=this, substr=substr, position=position)
        if func_name in {"GETUTCDATE", "CURRENT UTC TIMESTAMP"}:
            return exp.UtcTimestamp()
    return node
