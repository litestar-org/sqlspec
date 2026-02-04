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

from sqlglot import exp
from sqlglot.tokens import TokenType

from sqlspec.adapters.asyncpg.dialect._pgvector import PGVector, PGVectorGenerator, PGVectorParser, PGVectorTokenizer

__all__ = ("ParadeDB",)

_PARADEDB_SEARCH_TOKEN = TokenType.DAT


class SearchOperator(exp.Binary):
    """ParadeDB search operation that preserves the original operator."""

    arg_types = {"this": True, "expression": True, "operator": True}


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


class ParadeDBParser(PGVectorParser):
    """Parser with ParadeDB search operators and pgvector distance operators."""

    FACTOR = {
        **PGVectorParser.FACTOR,
        _PARADEDB_SEARCH_TOKEN: SearchOperator,
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
