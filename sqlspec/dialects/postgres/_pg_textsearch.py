"""PGTextSearch dialect extending Postgres with pg_textsearch BM25 operators.

Adds support for PostgreSQL and AlloyDB pg_textsearch BM25 operators:
    - <@> : Relevance ranking operator (returns negative BM25 score for ASC index scans)

Scoring is handled via the <@> operator. Text search configuration and saturation
parameters (k1, b) are specified via index parameters in USING bm25.

Also inherits the pgvector distance operators for seamless hybrid search.
Registered with sqlglot through the ``sqlglot.dialects`` entry-point group in
``pyproject.toml`` and by the ``Dialect`` metaclass on import.
"""

from sqlglot.dialects.postgres import Postgres

from sqlspec.dialects.postgres._generators import PGTextSearchGenerator
from sqlspec.dialects.postgres._operators import (
    PG_TEXTSEARCH_OPERATOR_TOKENS,
    PGVECTOR_OPERATOR_TOKENS,
    register_postgres_extension_operators,
)

__all__ = ("PGTextSearch",)

register_postgres_extension_operators()


class PGTextSearchTokenizer(Postgres.Tokenizer):
    """Tokenizer with pg_textsearch BM25 ranking operators and pgvector distance operators."""

    KEYWORDS = {**Postgres.Tokenizer.KEYWORDS, **PG_TEXTSEARCH_OPERATOR_TOKENS, **PGVECTOR_OPERATOR_TOKENS}


class PGTextSearch(Postgres):
    """PostgreSQL dialect with pg_textsearch and pgvector extension support."""

    Tokenizer = PGTextSearchTokenizer
    Generator = PGTextSearchGenerator
