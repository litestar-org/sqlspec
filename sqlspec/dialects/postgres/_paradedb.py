"""ParadeDB dialect extending PGVector with pg_search BM25/search operators.

Adds support for ParadeDB search operators:
- @@@  : BM25 full-text search
- &&&  : Boolean AND search
- |||  : Boolean OR search
- ===  : Exact term match
- ###  : Score/rank retrieval
- ##   : Snippet/highlight retrieval
- ##>  : Snippet/highlight with options

Also inherits pgvector distance operators from PGVector:
- <->  : L2 (Euclidean) distance
- <#>  : Negative inner product
- <=>  : Cosine distance
- <+>  : L1 (Taxicab/Manhattan) distance
- <~>  : Hamming distance (binary vectors)
- <%>  : Jaccard distance (binary vectors)
"""

from sqlglot.dialects.dialect import Dialect
from sqlglot.dialects.postgres import Postgres

from sqlspec.dialects.postgres._generators import ParadeDBGenerator
from sqlspec.dialects.postgres._operators import (
    PARADEDB_OPERATOR_TOKENS,
    PGVECTOR_OPERATOR_TOKENS,
    register_postgres_extension_operators,
)
from sqlspec.dialects.postgres._pgvector import PGVector, PGVectorTokenizer

__all__ = ("ParadeDB",)

register_postgres_extension_operators()


class ParadeDBTokenizer(PGVectorTokenizer):
    """Tokenizer with ParadeDB search operators and pgvector distance operators."""

    KEYWORDS = {**Postgres.Tokenizer.KEYWORDS, **PGVECTOR_OPERATOR_TOKENS, **PARADEDB_OPERATOR_TOKENS}


class ParadeDB(PGVector):
    """ParadeDB dialect with pg_search and pgvector extension support."""

    Tokenizer = ParadeDBTokenizer
    Generator = ParadeDBGenerator


Dialect.classes["paradedb"] = ParadeDB
