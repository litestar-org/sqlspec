"""Token and operator helpers for PostgreSQL extension dialects.

Compiled sqlglot parsers cannot be subclassed safely from sqlspec when sqlglot[c]
is installed. Instead, we reuse the compiled Postgres parser and register extra
FACTOR entries that construct built-in compiled ``exp.Operator`` nodes.
"""

from collections.abc import Callable
from typing import Any, Final

from sqlglot import exp
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokenizer_core import TokenType

__all__ = (
    "PARADEDB_OPERATOR_TOKENS",
    "PGVECTOR_OPERATOR_TOKENS",
    "PG_TEXTSEARCH_OPERATOR_TOKENS",
    "is_postgres_extension_operator",
    "postgres_extension_operator",
    "register_postgres_extension_operators",
)

_CUSTOM_OPERATOR_META_KEY: Final[str] = "sqlspec_postgres_extension_operator"
_OPERATOR_PARSERS_REGISTERED_ATTR: Final[str] = "_sqlspec_postgres_operator_parsers"
_POSTGRES_EXTENSION_DIALECT_NAMES: Final[frozenset[str]] = frozenset({
    "Postgres",
    "PGVector",
    "ParadeDB",
    "PGTextSearch",
})

PGVECTOR_OPERATOR_TOKENS: Final[dict[str, TokenType]] = {
    "<#>": TokenType.GEOGRAPHYPOINT,
    "<=>": TokenType.HLLSKETCH,
    "<+>": TokenType.ROWVERSION,
    "<~>": TokenType.IPPREFIX,
    "<%>": TokenType.IPV4,
}
PARADEDB_OPERATOR_TOKENS: Final[dict[str, TokenType]] = {
    "@@@": TokenType.IPV6,
    "&&&": TokenType.ENUM8,
    "|||": TokenType.ENUM16,
    "===": TokenType.FIXEDSTRING,
    "###": TokenType.LOWCARDINALITY,
    "##": TokenType.NESTED,
    "##>": TokenType.AGGREGATEFUNCTION,
}
PG_TEXTSEARCH_OPERATOR_TOKENS: Final[dict[str, TokenType]] = {"<@>": TokenType.RING}


def register_postgres_extension_operators() -> None:
    """Patch the compiled Postgres parser with PostgreSQL extension operators."""
    if getattr(PostgresParser, _OPERATOR_PARSERS_REGISTERED_ATTR, False):
        return

    operators: dict[TokenType, Any] = dict(PostgresParser.JSON_OPERATORS)
    extension_tokens = {**PGVECTOR_OPERATOR_TOKENS, **PARADEDB_OPERATOR_TOKENS, **PG_TEXTSEARCH_OPERATOR_TOKENS}
    for operator, token in extension_tokens.items():
        operators[token] = _build_operator_factory(operator, operators.get(token))

    setattr(PostgresParser, "JSON_OPERATORS", operators)
    setattr(PostgresParser, _OPERATOR_PARSERS_REGISTERED_ATTR, True)


def is_postgres_extension_operator(expression: object) -> bool:
    """Return True when an Operator node came from a PostgreSQL extension token."""
    return isinstance(expression, exp.Operator) and _CUSTOM_OPERATOR_META_KEY in expression.meta


def postgres_extension_operator(expression: exp.Operator) -> str:
    """Get the original infix operator text for an extension operator node."""
    operator = expression.meta.get(_CUSTOM_OPERATOR_META_KEY)
    if isinstance(operator, str):
        return operator
    return expression.text("operator")


def _build_operator_factory(
    operator: str, original: Any = None
) -> Callable[[PostgresParser | None, exp.Expr | None, exp.Expr | None], exp.Expr]:
    def _factory(parser: PostgresParser | None, this: exp.Expr | None, expression: exp.Expr | None) -> exp.Expr:
        if parser is not None:
            dialect = getattr(parser, "dialect", None)
            if (
                dialect is not None
                and type(dialect).__name__ not in _POSTGRES_EXTENSION_DIALECT_NAMES
                and original is not None
            ):
                return original(parser, this, expression)  # type: ignore[no-any-return]
        node = exp.Operator(this=this, expression=expression, operator=operator)
        node.meta[_CUSTOM_OPERATOR_META_KEY] = operator
        return node

    return _factory
