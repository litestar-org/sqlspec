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

from __future__ import annotations

from sqlglot.dialects.dialect import Dialect

from sqlspec.dialects.postgres._parsers import _PARADEDB_SEARCH_TOKEN, ParadeDBParser, SearchOperator
from sqlspec.dialects.postgres._pgvector import PGVector, PGVectorGenerator, PGVectorTokenizer

__all__ = ("ParadeDB",)


class ParadeDBTokenizer(PGVectorTokenizer):
    """Tokenizer with ParadeDB search operators and pgvector distance operators."""

    KEYWORDS = {
        **PGVectorTokenizer.KEYWORDS,
        "@@@": _PARADEDB_SEARCH_TOKEN,
        "&&&": _PARADEDB_SEARCH_TOKEN,
        "|||": _PARADEDB_SEARCH_TOKEN,
        "===": _PARADEDB_SEARCH_TOKEN,
        "###": _PARADEDB_SEARCH_TOKEN,
        "##": _PARADEDB_SEARCH_TOKEN,
        "##>": _PARADEDB_SEARCH_TOKEN,
    }


class ParadeDBGenerator(PGVectorGenerator):
    """Generator that renders ParadeDB search operators and pgvector distance operators."""

    def searchoperator_sql(self, expression: SearchOperator) -> str:
        op = expression.args.get("operator", "@@@")
        left = self.sql(expression, "this")
        right = self.sql(expression, "expression")
        return f"{left} {op} {right}"


class ParadeDB(PGVector):
    """ParadeDB dialect with pg_search and pgvector extension support."""

    Tokenizer = ParadeDBTokenizer
    Parser = ParadeDBParser
    Generator = ParadeDBGenerator


Dialect.classes["paradedb"] = ParadeDB
