"""PGVector dialect extending Postgres with vector distance operators.

Adds support for pgvector distance operators:
- <-> : L2 (Euclidean) distance (already in base Postgres)
- <#> : Negative inner product
- <=> : Cosine distance
- <+> : L1 (Taxicab/Manhattan) distance
- <~> : Hamming distance (binary vectors)
- <%> : Jaccard distance (binary vectors)
"""

from __future__ import annotations

from sqlglot.dialects.dialect import Dialect
from sqlglot.dialects.postgres import Postgres

from sqlspec.dialects.postgres._parsers import _PGVECTOR_DISTANCE_TOKEN, PGVectorParser, VectorDistance

__all__ = ("PGVector",)


class PGVectorTokenizer(Postgres.Tokenizer):
    """Tokenizer with pgvector distance operators."""

    KEYWORDS = {
        **Postgres.Tokenizer.KEYWORDS,
        "<#>": _PGVECTOR_DISTANCE_TOKEN,
        "<=>": _PGVECTOR_DISTANCE_TOKEN,
        "<+>": _PGVECTOR_DISTANCE_TOKEN,
        "<~>": _PGVECTOR_DISTANCE_TOKEN,
        "<%>": _PGVECTOR_DISTANCE_TOKEN,
    }


class PGVectorGenerator(Postgres.Generator):
    """Generator that renders pgvector distance operators."""

    def vectordistance_sql(self, expression: VectorDistance) -> str:
        op = expression.args.get("operator", "<->")
        left = self.sql(expression, "this")
        right = self.sql(expression, "expression")
        return f"{left} {op} {right}"


class PGVector(Postgres):
    """PostgreSQL dialect with pgvector extension support."""

    Tokenizer = PGVectorTokenizer
    Parser = PGVectorParser
    Generator = PGVectorGenerator


Dialect.classes["pgvector"] = PGVector
