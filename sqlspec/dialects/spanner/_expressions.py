"""Custom AST expressions for Cloud Spanner dialects."""

from typing import TYPE_CHECKING, Any

from sqlglot import exp, parse_one

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "ApproxCosineDistance",
    "CosineDistance",
    "DotProduct",
    "EuclideanDistance",
    "GetNextSequenceValue",
    "Score",
    "ScoreNgrams",
    "Search",
    "SearchSubstring",
    "Snippet",
    "SpannerGraphTable",
    "SpannerPropertyGraph",
    "TokenizeFulltext",
    "TokenizeNgrams",
    "TokenizeSubstring",
    "approx_cosine_distance",
    "cosine_distance",
    "dot_product",
    "euclidean_distance",
    "get_next_sequence_value",
    "graph_table",
    "score",
    "score_ngrams",
    "search",
    "search_substring",
    "snippet",
    "tokenize_fulltext",
    "tokenize_ngrams",
    "tokenize_substring",
)

CosineDistance = exp.CosineDistance
EuclideanDistance = exp.EuclideanDistance
DotProduct = exp.DotProduct
Search = exp.Search
cosine_distance = exp.CosineDistance
euclidean_distance = exp.EuclideanDistance
dot_product = exp.DotProduct
search = exp.Search


class SpannerPropertyGraph(exp.Expression):
    """AST node for a Cloud Spanner PROPERTY GRAPH definition."""

    arg_types = {"this": True, "node_tables": True, "edge_tables": False}


class SpannerGraphTable(exp.Expression):
    """AST node for a Cloud Spanner GRAPH_TABLE(...) query expression."""

    arg_types = {"this": True, "match": True, "where": False, "columns": True}


def approx_cosine_distance(this: Any, expression: Any, options: Any = None) -> exp.Anonymous:
    """Build an APPROX_COSINE_DISTANCE function call."""
    exprs = [this, expression]
    if options is not None:
        opt_expr = options if isinstance(options, exp.Kwarg) else exp.Kwarg(this=exp.var("options"), expression=options)
        exprs.append(opt_expr)
    return exp.Anonymous(this="APPROX_COSINE_DISTANCE", expressions=exprs)


def search_substring(this: Any, expression: Any) -> exp.Anonymous:
    """Build a SEARCH_SUBSTRING function call."""
    return exp.Anonymous(this="SEARCH_SUBSTRING", expressions=[this, expression])


def score(this: Any, expression: Any) -> exp.Anonymous:
    """Build a SCORE function call."""
    return exp.Anonymous(this="SCORE", expressions=[this, expression])


def score_ngrams(this: Any, expression: Any, *expressions: Any) -> exp.Anonymous:
    """Build a SCORE_NGRAMS function call."""
    return exp.Anonymous(this="SCORE_NGRAMS", expressions=[this, expression, *expressions])


def snippet(this: Any, expression: Any, *expressions: Any) -> exp.Anonymous:
    """Build a SNIPPET function call."""
    return exp.Anonymous(this="SNIPPET", expressions=[this, expression, *expressions])


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


def graph_table(
    graph: "str | exp.Expr",
    match: "str | exp.Expr",
    columns: "Sequence[str | exp.Expr]",
    where: "str | exp.Expr | None" = None,
) -> SpannerGraphTable:
    """Build a Spanner GRAPH_TABLE(...) expression."""
    graph_expr = exp.var(graph) if isinstance(graph, str) else graph
    match_expr = exp.var(match) if isinstance(match, str) else match
    where_expr = parse_one(where, read="spanner") if isinstance(where, str) else where
    column_exprs = [parse_one(col, read="spanner") if isinstance(col, str) else col for col in columns]
    return SpannerGraphTable(this=graph_expr, match=match_expr, where=where_expr, columns=column_exprs)


ApproxCosineDistance = approx_cosine_distance
SearchSubstring = search_substring
Score = score
ScoreNgrams = score_ngrams
Snippet = snippet
TokenizeFulltext = tokenize_fulltext
TokenizeSubstring = tokenize_substring
TokenizeNgrams = tokenize_ngrams
GetNextSequenceValue = get_next_sequence_value
