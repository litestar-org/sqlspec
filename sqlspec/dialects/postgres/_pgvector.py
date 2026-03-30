"""PGVector dialect extending Postgres with vector distance operators.

Adds support for pgvector distance operators:
- <-> : L2 (Euclidean) distance (already in base Postgres)
- <#> : Negative inner product
- <=> : Cosine distance
- <+> : L1 (Taxicab/Manhattan) distance
- <~> : Hamming distance (binary vectors)
- <%> : Jaccard distance (binary vectors)
"""

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.postgres import PostgresGenerator

from sqlspec.builder._vector_distance import (
    is_vector_distance_expression,
    render_vector_distance_postgres,
    vector_distance_metric,
)
from sqlspec.dialects.postgres._operators import (
    PGVECTOR_OPERATOR_TOKENS,
    is_postgres_extension_operator,
    postgres_extension_operator,
    register_postgres_extension_operators,
)

__all__ = ("PGVector",)

register_postgres_extension_operators()

_BASE_OPERATOR_TRANSFORM = Postgres.Generator.TRANSFORMS[exp.Operator]


def _postgres_extension_operator_sql(generator: PostgresGenerator, expression: exp.Operator) -> str:
    if is_vector_distance_expression(expression):
        return render_vector_distance_postgres(
            generator.sql(expression, "this"),
            generator.sql(expression, "expression"),
            vector_distance_metric(expression),
        )

    if is_postgres_extension_operator(expression):
        return (
            f"{generator.sql(expression, 'this')} "
            f"{postgres_extension_operator(expression)} "
            f"{generator.sql(expression, 'expression')}"
        )

    return _BASE_OPERATOR_TRANSFORM(generator, expression)


class PGVectorTokenizer(Postgres.Tokenizer):
    """Tokenizer with pgvector distance operators."""

    KEYWORDS = {**Postgres.Tokenizer.KEYWORDS, **PGVECTOR_OPERATOR_TOKENS}


class PGVectorGenerator(PostgresGenerator):
    """Generator that renders pgvector and SQLSpec vector-distance operators."""


PGVectorGenerator.TRANSFORMS[exp.Operator] = _postgres_extension_operator_sql


class PGVector(Postgres):
    """PostgreSQL dialect with pgvector extension support."""

    Tokenizer = PGVectorTokenizer
    Generator = PGVectorGenerator


Dialect.classes["pgvector"] = PGVector
