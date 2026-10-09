"""Custom AST expressions for Cloud Spanner dialects."""

from typing import TYPE_CHECKING, Any, cast

from sqlglot import exp, parse_one

if TYPE_CHECKING:
    from collections.abc import Sequence

    from typing_extensions import Self

    _SpannerAnonymousBase = exp.Anonymous
else:
    _SpannerAnonymousBase = object

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


class _SpannerAnonymousNodeMeta(type):
    """Metaclass enabling isinstance() and AST find() against tagged exp.Anonymous nodes."""

    _anonymous_name: str = ""
    _required_arg: str = ""

    def __instancecheck__(cls, instance: Any) -> bool:
        return (
            isinstance(instance, exp.Anonymous)
            and str(instance.this).upper() == cls._anonymous_name
            and (not cls._required_arg or cls._required_arg in instance.args)
        )


class SpannerPropertyGraph(_SpannerAnonymousBase, metaclass=_SpannerAnonymousNodeMeta):
    """AST node builder and type matcher for a Cloud Spanner PROPERTY GRAPH definition."""

    _anonymous_name = "SPANNER_PROPERTY_GRAPH"
    _required_arg = "node_tables"

    def __new__(cls, *, this: Any, node_tables: Any, edge_tables: Any = None) -> "Self":
        return cast(
            "Self",
            exp.Anonymous(this="SPANNER_PROPERTY_GRAPH", graph=this, node_tables=node_tables, edge_tables=edge_tables),
        )


class SpannerGraphTable(_SpannerAnonymousBase, metaclass=_SpannerAnonymousNodeMeta):
    """AST node builder and type matcher for a Cloud Spanner GRAPH_TABLE(...) query expression."""

    _anonymous_name = "GRAPH_TABLE"
    _required_arg = "match"

    def __new__(cls, *, this: Any, match: Any, columns: Any, where: Any = None) -> "Self":
        return cast("Self", exp.Anonymous(this="GRAPH_TABLE", graph=this, match=match, where=where, columns=columns))


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
