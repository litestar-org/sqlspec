"""Custom AST expressions for Cloud Spanner dialects."""

from typing import Any

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
    "approx_cosine_distance",
    "get_next_sequence_value",
    "score",
    "search_substring",
    "tokenize_fulltext",
    "tokenize_ngrams",
    "tokenize_substring",
)

CosineDistance = exp.CosineDistance
EuclideanDistance = exp.EuclideanDistance
DotProduct = exp.DotProduct
Search = exp.Search


def approx_cosine_distance(this: Any, expression: Any, options: Any = None) -> exp.Anonymous:
    """Build an APPROX_COSINE_DISTANCE function call."""
    exprs = [this, expression]
    if options is not None:
        exprs.append(options)
    return exp.Anonymous(this="APPROX_COSINE_DISTANCE", expressions=exprs)


def search_substring(this: Any, expression: Any) -> exp.Anonymous:
    """Build a SEARCH_SUBSTRING function call."""
    return exp.Anonymous(this="SEARCH_SUBSTRING", expressions=[this, expression])


def score(this: Any, expression: Any) -> exp.Anonymous:
    """Build a SCORE function call."""
    return exp.Anonymous(this="SCORE", expressions=[this, expression])


def tokenize_fulltext(this: Any, *expressions: Any) -> exp.Anonymous:
    """Build a TOKENIZE_FULLTEXT function call."""
    return exp.Anonymous(this="TOKENIZE_FULLTEXT", expressions=[this, *expressions])


def tokenize_substring(this: Any) -> exp.Anonymous:
    """Build a TOKENIZE_SUBSTRING function call."""
    return exp.Anonymous(this="TOKENIZE_SUBSTRING", expressions=[this])


def tokenize_ngrams(this: Any) -> exp.Anonymous:
    """Build a TOKENIZE_NGRAMS function call."""
    return exp.Anonymous(this="TOKENIZE_NGRAMS", expressions=[this])


def get_next_sequence_value(this: Any) -> exp.Anonymous:
    """Build a GET_NEXT_SEQUENCE_VALUE function call."""
    return exp.Anonymous(this="GET_NEXT_SEQUENCE_VALUE", expressions=[this])


ApproxCosineDistance = approx_cosine_distance
SearchSubstring = search_substring
Score = score
TokenizeFulltext = tokenize_fulltext
TokenizeSubstring = tokenize_substring
TokenizeNgrams = tokenize_ngrams
GetNextSequenceValue = get_next_sequence_value
