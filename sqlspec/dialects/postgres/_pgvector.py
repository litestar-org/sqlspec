"""PGVector dialect extending Postgres with vector distance operators.

Adds support for pgvector distance operators (pgvector 0.7.0+ set; ``halfvec``
and ``sparsevec`` reuse the first four, ``bit`` vectors use the last two):
    - <-> : L2 (Euclidean) distance (already in base Postgres)
    - <#> : Negative inner product
    - <=> : Cosine distance
    - <+> : L1 (Taxicab/Manhattan) distance
    - <~> : Hamming distance (binary vectors)
    - <%> : Jaccard distance (binary vectors)

Registered with sqlglot through the ``sqlglot.dialects`` entry-point group in
``pyproject.toml`` and by the ``Dialect`` metaclass on import.
"""

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
