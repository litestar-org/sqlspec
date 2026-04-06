"""PGVector dialect extending Postgres with vector distance operators.

Adds support for pgvector distance operators:
- <-> : L2 (Euclidean) distance (already in base Postgres)
- <#> : Negative inner product
- <=> : Cosine distance
- <+> : L1 (Taxicab/Manhattan) distance
- <~> : Hamming distance (binary vectors)
- <%> : Jaccard distance (binary vectors)
"""

from sqlglot.dialects.dialect import Dialect
from sqlglot.dialects.postgres import Postgres

from sqlspec.dialects.postgres._generators import PGVectorGenerator
from sqlspec.dialects.postgres._operators import PGVECTOR_OPERATOR_TOKENS, register_postgres_extension_operators

__all__ = ("PGVector",)

register_postgres_extension_operators()


class PGVectorTokenizer(Postgres.Tokenizer):
    """Tokenizer with pgvector distance operators."""

    KEYWORDS = {**Postgres.Tokenizer.KEYWORDS, **PGVECTOR_OPERATOR_TOKENS}


class PGVector(Postgres):
    """PostgreSQL dialect with pgvector extension support."""

    Tokenizer = PGVectorTokenizer
    Generator = PGVectorGenerator


Dialect.classes["pgvector"] = PGVector
