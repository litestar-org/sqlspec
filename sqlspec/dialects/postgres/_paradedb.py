"""ParadeDB dialect extending PGVector with pg_search operators.

Adds support for ParadeDB pg_search operators (pg_search 0.18.0+):
    - @@@ : Full-text search / complex query expressions (BM25)
    - &&& : Match conjunction (all tokenized terms must match)
    - ||| : Match disjunction (any tokenized term matches)
    - === : Exact term match (no tokenization of the right-hand side)
    - ### : Exact phrase match (token order and position enforced)
    - ##  : Proximity match, any order (``'a' ## n ## 'b'``)
    - ##> : Ordered proximity match (left term must appear first)

Scoring and snippets are plain functions in ParadeDB (``pdb.score()``,
``pdb.snippet()``), not operators, so they need no dialect support.

Also inherits the pgvector distance operators from PGVector. Registered with
sqlglot through the ``sqlglot.dialects`` entry-point group in
``pyproject.toml`` and by the ``Dialect`` metaclass on import.
"""

from sqlspec.dialects.postgres._generators import ParadeDBGenerator
from sqlspec.dialects.postgres._operators import PARADEDB_OPERATOR_TOKENS, register_postgres_extension_operators
from sqlspec.dialects.postgres._pgvector import PGVector, PGVectorTokenizer

__all__ = ("ParadeDB",)

register_postgres_extension_operators()


class ParadeDBTokenizer(PGVectorTokenizer):
    """Tokenizer with ParadeDB search operators and pgvector distance operators."""

    KEYWORDS = {**PGVectorTokenizer.KEYWORDS, **PARADEDB_OPERATOR_TOKENS}


class ParadeDB(PGVector):
    """ParadeDB dialect with pg_search and pgvector extension support."""

    Tokenizer = ParadeDBTokenizer
    Generator = ParadeDBGenerator
