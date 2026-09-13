"""Unit tests for PostgreSQL extension operator tokens and FACTOR registration."""

from sqlglot import exp
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokenizer_core import TokenType

from sqlspec.dialects.postgres._operators import (
    PARADEDB_OPERATOR_TOKENS,
    PG_TEXTSEARCH_OPERATOR_TOKENS,
    PGVECTOR_OPERATOR_TOKENS,
    is_postgres_extension_operator,
    postgres_extension_operator,
    register_postgres_extension_operators,
)


def test_pg_textsearch_operator_tokens_definition() -> None:
    """Verify pg_textsearch operator token mapping."""
    assert "<@>" in PG_TEXTSEARCH_OPERATOR_TOKENS
    assert PG_TEXTSEARCH_OPERATOR_TOKENS["<@>"] == TokenType.RING


def test_pg_textsearch_factor_registration() -> None:
    """Verify pg_textsearch operator is registered in PostgresParser.FACTOR."""
    register_postgres_extension_operators()
    ring_token = PG_TEXTSEARCH_OPERATOR_TOKENS["<@>"]
    assert ring_token in PostgresParser.FACTOR

    factory = PostgresParser.FACTOR[ring_token]
    left = exp.var("content")
    right = exp.Literal.string("search query")
    node = factory(left, right)

    assert isinstance(node, exp.Operator)
    assert is_postgres_extension_operator(node)
    assert postgres_extension_operator(node) == "<@>"


def test_no_operator_token_collisions() -> None:
    """Verify extension operator sets are mutually disjoint."""
    all_operators = [
        *PGVECTOR_OPERATOR_TOKENS.keys(),
        *PARADEDB_OPERATOR_TOKENS.keys(),
        *PG_TEXTSEARCH_OPERATOR_TOKENS.keys(),
    ]
    assert len(all_operators) == len(set(all_operators))

    all_tokens = [
        *PGVECTOR_OPERATOR_TOKENS.values(),
        *PARADEDB_OPERATOR_TOKENS.values(),
        *PG_TEXTSEARCH_OPERATOR_TOKENS.values(),
    ]
    assert len(all_tokens) == len(set(all_tokens))
