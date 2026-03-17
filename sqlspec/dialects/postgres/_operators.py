"""Token and operator helpers for PostgreSQL extension dialects.

Compiled sqlglot parsers cannot be subclassed safely from sqlspec when sqlglot[c]
is installed. Instead, we reuse the compiled Postgres parser and register extra
FACTOR entries that construct built-in compiled ``exp.Operator`` nodes.
"""

from collections.abc import Callable
from typing import Any, Final

from sqlglot import exp
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokens import TokenType  # type: ignore[attr-defined]  # pyright: ignore[reportPrivateImportUsage]

__all__ = (
    "PARADEDB_OPERATOR_TOKENS",
    "PGVECTOR_OPERATOR_TOKENS",
    "is_postgres_extension_operator",
    "postgres_extension_operator",
    "register_postgres_extension_operators",
)

_CUSTOM_OPERATOR_META_KEY: Final[str] = "sqlspec_postgres_extension_operator"

# These token slots are not emitted by the base Postgres tokenizer and are only
# produced by the extension tokenizers below.
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

_REGISTERED = False


def _build_operator_factory(operator: str) -> Callable[[exp.Expr | None, exp.Expr | None], exp.Operator]:
    def _factory(this: exp.Expr | None, expression: exp.Expr | None) -> exp.Operator:
        node = exp.Operator(this=this, expression=expression, operator=operator)
        node.meta[_CUSTOM_OPERATOR_META_KEY] = operator
        return node

    return _factory


def register_postgres_extension_operators() -> None:
    """Patch the compiled Postgres parser with pgvector and ParadeDB operators."""
    global _REGISTERED

    if _REGISTERED:
        return

    factor: dict[TokenType, Any] = dict(PostgresParser.FACTOR)
    for operator, token in {**PGVECTOR_OPERATOR_TOKENS, **PARADEDB_OPERATOR_TOKENS}.items():
        factor[token] = _build_operator_factory(operator)

    setattr(PostgresParser, "FACTOR", factor)
    _REGISTERED = True


def is_postgres_extension_operator(expression: object) -> bool:
    """Return True when an Operator node came from a PostgreSQL extension token."""
    return isinstance(expression, exp.Operator) and _CUSTOM_OPERATOR_META_KEY in expression.meta


def postgres_extension_operator(expression: exp.Operator) -> str:
    """Get the original infix operator text for an extension operator node."""
    operator = expression.meta.get(_CUSTOM_OPERATOR_META_KEY)
    if isinstance(operator, str):
        return operator
    return expression.text("operator")
