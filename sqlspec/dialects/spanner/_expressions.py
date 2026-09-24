"""Custom AST expressions for Cloud Spanner dialects."""

from sqlglot import exp

__all__ = (
    "ApproxCosineDistance",
    "CosineDistance",
    "DotProduct",
    "EuclideanDistance",
    "GetNextSequenceValue",
    "Score",
    "Search",
    "SearchSubstring",
    "TokenizeFulltext",
    "TokenizeNgrams",
    "TokenizeSubstring",
)


class CosineDistance(exp.Expression, exp.Func):
    """Cosine distance between two vector embeddings."""

    arg_types = {"this": True, "expression": True}


class EuclideanDistance(exp.Expression, exp.Func):
    """Euclidean distance between two vector embeddings."""

    arg_types = {"this": True, "expression": True}


class DotProduct(exp.Expression, exp.Func):
    """Dot product between two vector embeddings."""

    arg_types = {"this": True, "expression": True}


class ApproxCosineDistance(exp.Expression, exp.Func):
    """Approximate cosine distance with optional neighbor count options."""

    arg_types = {"this": True, "expression": True, "options": False}


class Search(exp.Expression, exp.Func):
    """Spanner full-text search function SEARCH(tokens, query)."""

    arg_types = {"this": True, "expression": True}


class SearchSubstring(exp.Expression, exp.Func):
    """Spanner full-text substring search function SEARCH_SUBSTRING(tokens, subquery)."""

    arg_types = {"this": True, "expression": True}


class Score(exp.Expression, exp.Func):
    """Spanner full-text search score function SCORE(tokens, query)."""

    arg_types = {"this": True, "expression": True}


class TokenizeFulltext(exp.Expression, exp.Func):
    """Spanner full-text tokenization function TOKENIZE_FULLTEXT."""

    arg_types = {"this": True, "expressions": False}


class TokenizeSubstring(exp.Expression, exp.Func):
    """Spanner substring tokenization function TOKENIZE_SUBSTRING."""

    arg_types = {"this": True, "expressions": False}


class TokenizeNgrams(exp.Expression, exp.Func):
    """Spanner ngram tokenization function TOKENIZE_NGRAMS."""

    arg_types = {"this": True, "expressions": False}


class GetNextSequenceValue(exp.Expression, exp.Func):
    """Spanner sequence function GET_NEXT_SEQUENCE_VALUE(SEQUENCE sequence_name)."""

    arg_types = {"this": True}
