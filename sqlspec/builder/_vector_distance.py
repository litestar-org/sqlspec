"""Vector distance helpers and SQL generator registration."""

# ruff: noqa: N802
# pyright: ignore[reportConstantRedefinition]
import math
import re
from collections.abc import Callable, MutableMapping, Sequence
from typing import TYPE_CHECKING, Any, Final, cast

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError
from sqlglot.tokenizer_core import TokenType

from sqlspec.builder._generation import generator_class_for_dialect, invalidate_generator_dispatch
from sqlspec.core.splitter import contains_statement_terminator
from sqlspec.exceptions import DialectNotSupportedError

if TYPE_CHECKING:
    from sqlglot.generator import Generator

__all__ = (
    "VectorDistance",
    "build_oracle_hnsw_subquery",
    "build_oracle_hybrid_rrf_query",
    "build_oracle_hybrid_rrf_sql",
    "has_vector_distance_ancestor",
    "is_vector_distance_expression",
    "register_vector_distance_generator",
    "render_vector_distance_bigquery",
    "render_vector_distance_duckdb",
    "render_vector_distance_generic",
    "render_vector_distance_mysql",
    "render_vector_distance_oracle",
    "render_vector_distance_postgres",
    "validate_vector_elements",
    "vector_distance_metric",
)

_VECTOR_DISTANCE_META_KEY: Final[str] = "sqlspec_vector_distance_metric"
_VALID_VECTOR_METRICS: Final[frozenset[str]] = frozenset({"euclidean", "cosine", "inner_product", "euclidean_squared"})
_VALID_ORACLE_METRICS: Final[frozenset[str]] = frozenset({
    "EUCLIDEAN",
    "COSINE",
    "DOT",
    "INNER_PRODUCT",
    "EUCLIDEAN_SQUARED",
    "MANHATTAN",
    "HAMMING",
})
_ORACLE_IDENTIFIER_SEGMENT_RE: Final["re.Pattern[str]"] = re.compile(
    r'^(?:[A-Za-z_][A-Za-z0-9_$#]*|"(?:[^"\x00]|"")+")$'
)
_ORACLE_BIND_RE: Final["re.Pattern[str]"] = re.compile(r"^:?(?:[A-Za-z_][A-Za-z0-9_$#]*|\d+)$")
_FORBIDDEN_ORACLE_WHERE_NODES: Final[tuple[type[exp.Expr], ...]] = (
    exp.Block,
    exp.Command,
    exp.DDL,
    exp.DML,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Select,
    exp.SetOperation,
    exp.Subquery,
)
_POSTGRES_OPERATOR_MAP: Final[dict[str, str]] = {"euclidean": "<->", "cosine": "<=>", "inner_product": "<#>"}
_MYSQL_METRIC_MAP: Final[dict[str, str]] = {"euclidean": "EUCLIDEAN", "cosine": "COSINE", "inner_product": "DOT"}
_ORACLE_METRIC_MAP: Final[dict[str, str]] = {
    "euclidean": "EUCLIDEAN",
    "cosine": "COSINE",
    "inner_product": "DOT",
    "euclidean_squared": "EUCLIDEAN_SQUARED",
}
_BIGQUERY_FUNCTION_MAP: Final[dict[str, str]] = {
    "euclidean": "EUCLIDEAN_DISTANCE",
    "cosine": "COSINE_DISTANCE",
    "inner_product": "DOT_PRODUCT",
}
_DUCKDB_FUNCTION_MAP: Final[dict[str, str]] = {
    "euclidean": "array_distance",
    "cosine": "array_cosine_distance",
    "inner_product": "array_negative_inner_product",
}
_OperatorTransform = Callable[[Any, exp.Operator], str]
_SQLGLOT_VECTOR_DISTANCE_REGISTERED = False
_BASE_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_POSTGRES_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_MYSQL_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_ORACLE_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_BIGQUERY_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_DUCKDB_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_CHECKED_VECTOR_DIALECTS: set[str] = set()
_UNSUPPORTED_VECTOR_DIALECTS: Final[frozenset[str]] = frozenset({"TSQL"})


_ORACLE_MIN_ACCURACY: Final[int] = 1
_ORACLE_MAX_ACCURACY: Final[int] = 100


def validate_vector_elements(value: Any) -> list[int | float]:
    """Validate and materialize a 1-D finite numeric vector sequence.

    Supports Python sequences and 1-D array-like objects exposing ``dtype`` and
    ``tolist()`` without importing optional array libraries.

    Args:
        value: Input vector sequence or 1-D array.

    Returns:
        Validated list of finite ``int | float`` values.

    Raises:
        TypeError: If ``value`` is not a valid vector container or holds non-numeric/boolean values.
        ValueError: If ``value`` is empty, multi-dimensional, or holds ``NaN``/``Inf`` values.
    """
    if hasattr(value, "dtype") and hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
        if hasattr(value, "ndim") and getattr(value, "ndim", 1) != 1:
            msg = "Vector must be 1-dimensional"
            raise ValueError(msg)
        dtype = getattr(value, "dtype", None)
        kind = getattr(dtype, "kind", None)
        if kind is not None and kind not in {"i", "u", "f"}:
            msg = f"Unsupported vector element dtype: {dtype}"
            raise TypeError(msg)
        items_raw = value.tolist()
        if not isinstance(items_raw, list):
            msg = f"Unsupported vector type: {type(value)}"
            raise TypeError(msg)
        if not items_raw:
            msg = "Vector cannot be empty"
            raise ValueError(msg)
        validated_array: list[int | float] = []
        for item in items_raw:
            if isinstance(item, bool) or not isinstance(item, (int, float)):
                msg = f"Vector elements must be int or float, got {type(item).__name__}"
                raise TypeError(msg)
            if not math.isfinite(item):
                msg = f"Vector elements must be finite numbers, got {item!r}"
                raise ValueError(msg)
            validated_array.append(item)
        return validated_array

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if not value:
            msg = "Vector cannot be empty"
            raise ValueError(msg)
        validated_seq: list[int | float] = []
        for item in value:
            if isinstance(item, bool) or not isinstance(item, (int, float)):
                msg = f"Vector elements must be int or float, got {type(item).__name__}"
                raise TypeError(msg)
            if not math.isfinite(item):
                msg = f"Vector elements must be finite numbers, got {item!r}"
                raise ValueError(msg)
            validated_seq.append(item)
        return validated_seq

    msg = f"Unsupported vector type: {type(value)}"
    raise TypeError(msg)


def is_vector_distance_expression(expression: object) -> bool:
    """Return True when an Operator node is a SQLSpec vector-distance expression."""
    return isinstance(expression, exp.Operator) and _VECTOR_DISTANCE_META_KEY in expression.meta


def has_vector_distance_ancestor(expression: exp.Expr) -> bool:
    """Return True when any ancestor is a SQLSpec vector-distance expression."""
    parent = expression.parent
    while parent is not None:
        if is_vector_distance_expression(parent):
            return True
        parent = parent.parent
    return False


def vector_distance_metric(expression: object) -> str:
    """Get the normalized vector-distance metric from an Operator node."""
    if not isinstance(expression, exp.Operator):
        msg = f"Expected sqlglot Operator, got {type(expression)}"
        raise TypeError(msg)
    metric = expression.meta.get(_VECTOR_DISTANCE_META_KEY)
    if isinstance(metric, str):
        return metric
    operator = expression.args.get("operator")
    return str(operator).lower() if operator is not None else "euclidean"


def VectorDistance(*, this: exp.Expr, expression: exp.Expr, metric: Any = "euclidean") -> exp.Operator:
    """Build a SQLSpec vector-distance expression."""
    _register_with_sqlglot()
    return _build_vector_distance(this=this, expression=expression, metric=metric)


def register_vector_distance_generator(dialect: "str | None") -> None:
    """Install vector-distance rendering on a dialect generator created before registration.

    Dialect generators copy ``Generator.TRANSFORMS`` when their module is imported, so
    a dialect imported before the first ``VectorDistance`` call would otherwise keep
    sqlglot's ``OPERATOR(...)`` rendering. Each dialect is checked once.

    Args:
        dialect: Normalized target dialect name, or ``None`` for the base generator.
    """
    if not _SQLGLOT_VECTOR_DISTANCE_REGISTERED or dialect is None or dialect in _CHECKED_VECTOR_DIALECTS:
        return
    generator_class = generator_class_for_dialect(dialect)
    transforms = cast("MutableMapping[type[exp.Expr], _OperatorTransform]", generator_class.TRANSFORMS)
    if transforms.get(exp.Operator) is _BASE_OPERATOR_TRANSFORM:
        _register_operator_transform(transforms, _operator_sql_base)
        invalidate_generator_dispatch(generator_class)
    _CHECKED_VECTOR_DIALECTS.add(dialect)


def render_vector_distance_postgres(left: str, right: str, metric: str) -> str:
    """Render PostgreSQL pgvector operator syntax."""
    operator = _POSTGRES_OPERATOR_MAP.get(metric)
    if operator:
        if right.startswith("ARRAY["):
            right = f"CAST({right} AS VECTOR)"
        return f"{left} {operator} {right}"

    return render_vector_distance_generic(left, right, metric)


def render_vector_distance_mysql(left: str, right: str, metric: str) -> str:
    """Render MySQL DISTANCE function syntax."""
    mysql_metric = _MYSQL_METRIC_MAP.get(metric, "EUCLIDEAN")

    if "STRING_TO_VECTOR" not in right:
        if right.startswith("ARRAY[") and right.endswith("]"):
            inner = right[len("ARRAY[") : -1]
            right = f"STRING_TO_VECTOR('[{inner}]')"
        elif right.startswith("[") and right.endswith("]"):
            right = f"STRING_TO_VECTOR('{right}')"
        elif right.startswith("'[") and right.endswith("]'"):
            right = f"STRING_TO_VECTOR({right})"

    return f"DISTANCE({left}, {right}, '{mysql_metric}')"


def render_vector_distance_oracle(left: str, right: str, metric: str) -> str:
    """Render Oracle VECTOR_DISTANCE function syntax."""
    oracle_metric = _ORACLE_METRIC_MAP.get(metric, "EUCLIDEAN")

    if "TO_VECTOR" not in right:
        if right.startswith("ARRAY[") and right.endswith("]"):
            inner = right[len("ARRAY[") : -1]
            right = f"TO_VECTOR('[{inner}]')"
        elif right.startswith("[") and right.endswith("]"):
            right = f"TO_VECTOR('{right}')"
        elif right.startswith("'[") and right.endswith("]'"):
            right = f"TO_VECTOR({right})"

    return f"VECTOR_DISTANCE({left}, {right}, {oracle_metric})"


def render_vector_distance_bigquery(left: str, right: str, metric: str) -> str:
    """Render BigQuery vector distance function syntax."""
    function_name = _BIGQUERY_FUNCTION_MAP.get(metric)
    if function_name:
        return f"{function_name}({left}, {right})"

    return render_vector_distance_generic(left, right, metric)


def render_vector_distance_duckdb(left: str, right: str, metric: str, *, dimension: int | None = None) -> str:
    """Render DuckDB VSS extension function syntax."""
    function_name = _DUCKDB_FUNCTION_MAP.get(metric)
    if function_name:
        target_type = f"DOUBLE[{dimension}]" if dimension is not None else "DOUBLE[]"
        return f"{function_name}({left}, CAST({right} AS {target_type}))"

    return render_vector_distance_generic(left, right, metric)


def render_vector_distance_generic(left: str, right: str, metric: str) -> str:
    """Render generic VECTOR_DISTANCE function syntax."""
    return f"VECTOR_DISTANCE({left}, {right}, '{metric.upper()}')"


def build_oracle_hnsw_subquery(
    table: str,
    vector_column: str = "embedding",
    vector_param: str = ":query_vec",
    *,
    id_column: str = "id",
    select_columns: Sequence[str] = ("id",),
    metric: str = "COSINE",
    top_k_param: str = ":top_k",
    query_vector_bind: str | None = None,
    top_k_bind: str | None = None,
    target_accuracy: int | None = None,
    approximate: bool = True,
    where_clause: str | None = None,
    rank_alias: str = "vector_rank",
    distance_alias: str = "vector_distance",
) -> str:
    """Build an Oracle 26ai HNSW-preserving or exact top-K subquery using outer ``ROWNUM``.

    Oracle 26ai CBO disables ``VECTOR INDEX HNSW SCAN`` when ``ROW_NUMBER() OVER (...)``
    appears in the same query block as ``ORDER BY VECTOR_DISTANCE(...) FETCH APPROX FIRST :k ROWS ONLY``.
    This helper isolates the ``ORDER BY VECTOR_DISTANCE(...)`` fetch clause inside
    an inner derived table and assigns ``ROWNUM`` in the outer query block. When
    ``approximate=False``, ``FETCH FIRST :k ROWS ONLY`` is emitted instead of
    ``FETCH APPROX FIRST`` for unindexed tables or exact top-K evaluation.
    """
    table = _validate_oracle_identifier(table, allow_qualified=True)
    vector_column = _validate_oracle_identifier(vector_column)
    id_column = _validate_oracle_identifier(id_column)
    rank_alias = _validate_oracle_identifier(rank_alias)
    distance_alias = _validate_oracle_identifier(distance_alias)
    oracle_metric = _validate_oracle_metric(metric)
    vec_bind = _format_oracle_bind(query_vector_bind if query_vector_bind is not None else vector_param)
    limit_bind = _format_oracle_bind(top_k_bind if top_k_bind is not None else top_k_param)
    if (
        isinstance(select_columns, (str, bytes, bytearray))
        or not isinstance(select_columns, Sequence)
        or not select_columns
    ):
        msg = "select_columns must be a non-empty sequence of column identifiers"
        raise ValueError(msg)
    raw_cols = (id_column,) if tuple(select_columns) == ("id",) and id_column != "id" else tuple(select_columns)
    cols = tuple(_validate_oracle_identifier(col, allow_qualified=True, allow_star=True) for col in raw_cols)
    cols_sql = ", ".join(cols)
    where_sql = f" WHERE ({_validate_oracle_where_clause(where_clause)})" if where_clause is not None else ""
    accuracy_sql = ""
    if target_accuracy is not None:
        if (
            isinstance(target_accuracy, bool)
            or not isinstance(target_accuracy, int)
            or not (_ORACLE_MIN_ACCURACY <= target_accuracy <= _ORACLE_MAX_ACCURACY)
        ):
            msg = f"target_accuracy must be an integer between 1 and 100, got {target_accuracy!r}"
            raise ValueError(msg)
        if approximate:
            accuracy_sql = f" WITH TARGET ACCURACY {target_accuracy}"
    fetch_clause = (
        f"FETCH APPROX FIRST {limit_bind} ROWS ONLY{accuracy_sql}"
        if approximate
        else f"FETCH FIRST {limit_bind} ROWS ONLY"
    )
    return (
        f"SELECT {cols_sql}, {distance_alias}, ROWNUM AS {rank_alias} "
        f"FROM ("
        f"SELECT {cols_sql}, VECTOR_DISTANCE({vector_column}, {vec_bind}, {oracle_metric}) AS {distance_alias} "
        f"FROM {table}{where_sql} "
        f"ORDER BY VECTOR_DISTANCE({vector_column}, {vec_bind}, {oracle_metric}) "
        f"{fetch_clause}"
        f") vec_top"
    )


def build_oracle_hybrid_rrf_sql(
    table: str,
    vector_column: str = "embedding",
    text_column: str = "content_text",
    *,
    id_column: str = "id",
    key_column: str | None = None,
    select_columns: Sequence[str] | None = None,
    vector_param: str = ":query_vec",
    text_param: str = ":fts_query",
    metric: str = "COSINE",
    rrf_k: int = 60,
    rrf_k_param: str | None = None,
    candidate_limit_param: str = ":candidate_limit",
    final_limit_param: str = ":final_limit",
    top_k_param: str | None = None,
    target_accuracy: int | None = None,
    approximate: bool = True,
    where_clause: str | None = None,
) -> str:
    """Build an Oracle 26ai Hybrid Search query fusing vector similarity and Oracle Text FTS via RRF."""
    table = _validate_oracle_identifier(table, allow_qualified=True)
    vector_column = _validate_oracle_identifier(vector_column)
    text_column = _validate_oracle_identifier(text_column)
    resolved_id_col = _validate_oracle_identifier(key_column if key_column is not None else id_column)
    _validate_oracle_metric(metric)
    if top_k_param is not None:
        if candidate_limit_param == ":candidate_limit":
            candidate_limit_param = top_k_param
        if final_limit_param == ":final_limit":
            final_limit_param = top_k_param
    cand_bind = _format_oracle_bind(candidate_limit_param)
    final_bind = _format_oracle_bind(final_limit_param)
    txt_bind = _format_oracle_bind(text_param)
    if rrf_k_param is not None:
        rrf_k_expr = _format_oracle_bind(rrf_k_param)
    else:
        if isinstance(rrf_k, bool) or not isinstance(rrf_k, (int, float)) or not math.isfinite(rrf_k) or rrf_k <= 0:
            msg = f"rrf_k must be a positive finite number, got {rrf_k!r}"
            raise ValueError(msg)
        rrf_k_expr = str(rrf_k)

    validated_where = _validate_oracle_where_clause(where_clause) if where_clause is not None else None

    vec_subquery = build_oracle_hnsw_subquery(
        table=table,
        vector_column=vector_column,
        vector_param=vector_param,
        id_column=resolved_id_col,
        select_columns=(resolved_id_col,),
        metric=metric,
        top_k_param=cand_bind,
        target_accuracy=target_accuracy,
        approximate=approximate,
        where_clause=validated_where,
        rank_alias="vector_rank",
        distance_alias="vector_distance",
    )
    fts_where_parts = [f"CONTAINS({text_column}, {txt_bind}, 1) > 0"]
    if validated_where is not None:
        fts_where_parts.append(f"({validated_where})")
    fts_where_sql = " AND ".join(fts_where_parts)
    fts_subquery = (
        f"SELECT {resolved_id_col}, fts_score, ROWNUM AS fts_rank "
        f"FROM ("
        f"SELECT {resolved_id_col}, SCORE(1) AS fts_score "
        f"FROM {table} "
        f"WHERE {fts_where_sql} "
        f"ORDER BY SCORE(1) DESC "
        f"FETCH FIRST {cand_bind} ROWS ONLY"
        f") fts_top"
    )

    if select_columns is not None:
        if (
            isinstance(select_columns, (str, bytes, bytearray))
            or not isinstance(select_columns, Sequence)
            or not select_columns
        ):
            msg = "select_columns must be a non-empty sequence of column identifiers"
            raise ValueError(msg)
        validated_select_cols = [
            _validate_oracle_identifier(col, allow_qualified=True, allow_star=True) for col in select_columns
        ]
        base_cols = ", ".join(col if "." in col or col == "*" else f"base.{col}" for col in validated_select_cols)
    else:
        base_cols = "base.*"

    return (
        f"WITH vec_candidates AS ({vec_subquery}), "
        f"fts_candidates AS ({fts_subquery}), "
        f"rrf_fused AS ("
        f"SELECT COALESCE(v.{resolved_id_col}, f.{resolved_id_col}) AS {resolved_id_col}, "
        f"v.vector_distance, v.vector_rank, f.fts_score, f.fts_rank, "
        f"(NVL(1.0 / ({rrf_k_expr} + v.vector_rank), 0) + NVL(1.0 / ({rrf_k_expr} + f.fts_rank), 0)) AS rrf_score "
        f"FROM vec_candidates v "
        f"FULL OUTER JOIN fts_candidates f ON v.{resolved_id_col} = f.{resolved_id_col}"
        f") "
        f"SELECT {base_cols}, rrf.vector_distance, rrf.vector_rank, rrf.fts_score, rrf.fts_rank, rrf.rrf_score "
        f"FROM rrf_fused rrf "
        f"JOIN {table} base ON base.{resolved_id_col} = rrf.{resolved_id_col} "
        f"ORDER BY rrf.rrf_score DESC "
        f"FETCH FIRST {final_bind} ROWS ONLY"
    )


def build_oracle_hybrid_rrf_query(
    table: str,
    vector_column: str = "embedding",
    text_column: str = "content_text",
    *,
    id_column: str = "id",
    key_column: str | None = None,
    select_columns: Sequence[str] | None = None,
    vector_param: str = ":query_vec",
    text_param: str = ":fts_query",
    metric: str = "COSINE",
    rrf_k: int = 60,
    rrf_k_param: str | None = None,
    candidate_limit_param: str = ":candidate_limit",
    final_limit_param: str = ":final_limit",
    top_k_param: str | None = None,
    target_accuracy: int | None = None,
    approximate: bool = True,
    where_clause: str | None = None,
) -> str:
    """Alias for :func:`build_oracle_hybrid_rrf_sql`."""
    return build_oracle_hybrid_rrf_sql(
        table,
        vector_column,
        text_column,
        id_column=id_column,
        key_column=key_column,
        select_columns=select_columns,
        vector_param=vector_param,
        text_param=text_param,
        metric=metric,
        rrf_k=rrf_k,
        rrf_k_param=rrf_k_param,
        candidate_limit_param=candidate_limit_param,
        final_limit_param=final_limit_param,
        top_k_param=top_k_param,
        target_accuracy=target_accuracy,
        approximate=approximate,
        where_clause=where_clause,
    )


def _validate_oracle_identifier(name: str, *, allow_qualified: bool = False, allow_star: bool = False) -> str:
    if not isinstance(name, str) or not name.strip() or "\x00" in name:
        msg = f"Invalid Oracle identifier: {name!r}"
        raise ValueError(msg)
    stripped = name.strip()
    if allow_star and stripped == "*":
        return stripped
    parts = stripped.split(".")
    max_parts = 2 if allow_qualified else 1
    if not (1 <= len(parts) <= max_parts):
        msg = f"Invalid Oracle identifier: {name!r}"
        raise ValueError(msg)
    for index, part in enumerate(parts):
        if allow_star and len(parts) == max_parts and index == 1 and part == "*":
            continue
        if _ORACLE_IDENTIFIER_SEGMENT_RE.fullmatch(part) is None:
            msg = f"Invalid Oracle identifier: {name!r}"
            raise ValueError(msg)
    return stripped


def _validate_oracle_metric(metric: str) -> str:
    if not isinstance(metric, str) or not metric.strip():
        msg = f"Invalid Oracle vector metric: {metric!r}"
        raise ValueError(msg)
    stripped = metric.strip()
    upper = stripped.upper()
    if upper not in _VALID_ORACLE_METRICS:
        msg = f"Invalid Oracle vector metric: {metric!r}"
        raise ValueError(msg)
    return _ORACLE_METRIC_MAP.get(stripped.lower(), upper)


def _validate_oracle_where_clause(where_clause: str) -> str:
    if not isinstance(where_clause, str) or not where_clause.strip() or "\x00" in where_clause:
        msg = f"Invalid Oracle WHERE clause: {where_clause!r}"
        raise ValueError(msg)
    stripped = where_clause.strip()
    if contains_statement_terminator(stripped, dialect="oracle"):
        msg = f"Invalid Oracle WHERE clause: {where_clause!r}"
        raise ValueError(msg)
    try:
        tokens = list(Dialect.get_or_raise("oracle").tokenize(stripped))
        statements = sqlglot.parse(stripped, read="oracle")
    except SqlglotError as exc:
        msg = f"Invalid Oracle WHERE clause: {where_clause!r}"
        raise ValueError(msg) from exc
    if (
        not tokens
        or any(token.comments or token.token_type == TokenType.SEMICOLON for token in tokens)
        or len(statements) != 1
        or statements[0] is None
    ):
        msg = f"Invalid Oracle WHERE clause: {where_clause!r}"
        raise ValueError(msg)
    parsed = statements[0]
    if isinstance(parsed, _FORBIDDEN_ORACLE_WHERE_NODES) or parsed.find(*_FORBIDDEN_ORACLE_WHERE_NODES) is not None:
        msg = f"Invalid Oracle WHERE clause: {where_clause!r}"
        raise ValueError(msg)
    return stripped


def _format_oracle_bind(param: str) -> str:
    if not isinstance(param, str) or "\x00" in param:
        msg = f"Invalid Oracle bind parameter: {param!r}"
        raise ValueError(msg)
    stripped = param.strip()
    if not stripped or (stripped.isdigit() and not stripped.startswith(":")):
        msg = f"Invalid Oracle bind parameter: {param!r}"
        raise ValueError(msg)
    if _ORACLE_BIND_RE.fullmatch(stripped) is None:
        msg = f"Invalid Oracle bind parameter: {param!r}"
        raise ValueError(msg)
    return stripped if stripped.startswith(":") else f":{stripped}"


def _extract_array_number_literal(item: exp.Expr) -> str:
    negated = False
    target = item
    if isinstance(target, exp.Neg) and isinstance(target.this, exp.Expr):
        negated = True
        target = target.this
    if not isinstance(target, exp.Literal) or target.is_string:
        msg = f"Vector array literal elements must be numeric literals, got {item!r}"
        raise ValueError(msg)
    raw_text = str(target.this).strip()
    try:
        numeric_val = float(raw_text)
    except ValueError as exc:
        msg = f"Vector array literal elements must be numeric literals, got {item!r}"
        raise ValueError(msg) from exc
    if not math.isfinite(numeric_val):
        msg = f"Vector array literal elements must be finite numbers, got {item!r}"
        raise ValueError(msg)
    if negated and not raw_text.startswith("-"):
        return f"-{raw_text}"
    return raw_text


def _render_with_metric(generator: "Generator", expression: exp.Operator, dialect: str) -> str:
    left_sql = generator.sql(expression, "this")
    right_sql = generator.sql(expression, "expression")
    metric = vector_distance_metric(expression)

    dialect_class = getattr(generator.dialect, "__class__", None)
    if dialect_class is not None and dialect_class.__name__ == "Spangres":
        dialect = "bigquery"
    if dialect_class is not None and dialect_class.__name__ in _UNSUPPORTED_VECTOR_DIALECTS:
        msg = f"Vector distance is not supported for dialect '{dialect_class.__name__.lower()}'."
        raise DialectNotSupportedError(msg)

    if dialect == "postgres":
        return render_vector_distance_postgres(left_sql, right_sql, metric)
    if dialect == "mysql":
        if isinstance(expression.expression, exp.Array):
            if not expression.expression.expressions:
                msg = "Vector cannot be empty"
                raise ValueError(msg)
            values = [_extract_array_number_literal(item) for item in expression.expression.expressions]
            right_sql = f"STRING_TO_VECTOR('[{', '.join(values)}]')"
        return render_vector_distance_mysql(left_sql, right_sql, metric)
    if dialect == "oracle":
        if isinstance(expression.expression, exp.Array):
            if not expression.expression.expressions:
                msg = "Vector cannot be empty"
                raise ValueError(msg)
            values = [_extract_array_number_literal(item) for item in expression.expression.expressions]
            right_sql = f"TO_VECTOR('[{', '.join(values)}]')"
        return render_vector_distance_oracle(left_sql, right_sql, metric)
    if dialect == "bigquery":
        return render_vector_distance_bigquery(left_sql, right_sql, metric)
    if dialect == "duckdb":
        dimension = len(expression.expression.expressions) if isinstance(expression.expression, exp.Array) else None
        return render_vector_distance_duckdb(left_sql, right_sql, metric, dimension=dimension)

    return render_vector_distance_generic(left_sql, right_sql, metric)


def _operator_sql_base(generator: "Generator", expression: exp.Operator) -> str:
    if is_vector_distance_expression(expression):
        return _render_with_metric(generator, expression, "generic")
    return _require_operator_transform(_BASE_OPERATOR_TRANSFORM)(generator, expression)


def _operator_sql_postgres(generator: "Generator", expression: exp.Operator) -> str:
    if is_vector_distance_expression(expression):
        return _render_with_metric(generator, expression, "postgres")
    return _require_operator_transform(_POSTGRES_OPERATOR_TRANSFORM)(generator, expression)


def _operator_sql_mysql(generator: "Generator", expression: exp.Operator) -> str:
    if is_vector_distance_expression(expression):
        return _render_with_metric(generator, expression, "mysql")
    return _require_operator_transform(_MYSQL_OPERATOR_TRANSFORM)(generator, expression)


def _operator_sql_oracle(generator: "Generator", expression: exp.Operator) -> str:
    if is_vector_distance_expression(expression):
        return _render_with_metric(generator, expression, "oracle")
    return _require_operator_transform(_ORACLE_OPERATOR_TRANSFORM)(generator, expression)


def _operator_sql_bigquery(generator: "Generator", expression: exp.Operator) -> str:
    if is_vector_distance_expression(expression):
        return _render_with_metric(generator, expression, "bigquery")
    return _require_operator_transform(_BIGQUERY_OPERATOR_TRANSFORM)(generator, expression)


def _operator_sql_duckdb(generator: "Generator", expression: exp.Operator) -> str:
    if is_vector_distance_expression(expression):
        return _render_with_metric(generator, expression, "duckdb")
    return _require_operator_transform(_DUCKDB_OPERATOR_TRANSFORM)(generator, expression)


def _require_operator_transform(transform: _OperatorTransform | None) -> _OperatorTransform:
    """Return a registered fallback transform or fail loudly."""
    if transform is None:
        msg = "Vector-distance SQLGlot transforms have not been registered"
        raise RuntimeError(msg)
    return transform


def _register_operator_transform(
    transforms: MutableMapping[type[exp.Expr], "_OperatorTransform"], wrapper: "_OperatorTransform"
) -> None:
    """Install an operator transform wrapper."""
    transforms[exp.Operator] = wrapper


def _register_with_sqlglot() -> None:
    """Register vector-distance Operator rendering with SQLGlot generators."""
    global \
        _SQLGLOT_VECTOR_DISTANCE_REGISTERED, \
        _BASE_OPERATOR_TRANSFORM, \
        _POSTGRES_OPERATOR_TRANSFORM, \
        _MYSQL_OPERATOR_TRANSFORM, \
        _ORACLE_OPERATOR_TRANSFORM, \
        _BIGQUERY_OPERATOR_TRANSFORM, \
        _DUCKDB_OPERATOR_TRANSFORM

    if _SQLGLOT_VECTOR_DISTANCE_REGISTERED:
        return

    from sqlglot.dialects.bigquery import BigQuery
    from sqlglot.dialects.duckdb import DuckDB
    from sqlglot.dialects.mysql import MySQL
    from sqlglot.dialects.oracle import Oracle
    from sqlglot.dialects.postgres import Postgres
    from sqlglot.generator import Generator

    _BASE_OPERATOR_TRANSFORM = cast("_OperatorTransform", Generator.TRANSFORMS[exp.Operator])
    _POSTGRES_OPERATOR_TRANSFORM = cast("_OperatorTransform", Postgres.Generator.TRANSFORMS[exp.Operator])
    _MYSQL_OPERATOR_TRANSFORM = cast("_OperatorTransform", MySQL.Generator.TRANSFORMS[exp.Operator])
    _ORACLE_OPERATOR_TRANSFORM = cast("_OperatorTransform", Oracle.Generator.TRANSFORMS[exp.Operator])
    _BIGQUERY_OPERATOR_TRANSFORM = cast("_OperatorTransform", BigQuery.Generator.TRANSFORMS[exp.Operator])
    _DUCKDB_OPERATOR_TRANSFORM = cast("_OperatorTransform", DuckDB.Generator.TRANSFORMS[exp.Operator])

    _register_operator_transform(Generator.TRANSFORMS, _operator_sql_base)
    _register_operator_transform(Postgres.Generator.TRANSFORMS, _operator_sql_postgres)
    _register_operator_transform(MySQL.Generator.TRANSFORMS, _operator_sql_mysql)
    _register_operator_transform(Oracle.Generator.TRANSFORMS, _operator_sql_oracle)
    _register_operator_transform(BigQuery.Generator.TRANSFORMS, _operator_sql_bigquery)
    _register_operator_transform(DuckDB.Generator.TRANSFORMS, _operator_sql_duckdb)

    invalidate_generator_dispatch(
        Generator, Postgres.Generator, MySQL.Generator, Oracle.Generator, BigQuery.Generator, DuckDB.Generator
    )

    _SQLGLOT_VECTOR_DISTANCE_REGISTERED = True


def _normalize_metric(metric: Any) -> str:
    """Normalize and validate vector metrics to a lowercase string."""
    if isinstance(metric, exp.Literal):
        if not metric.is_string:
            msg = f"Invalid metric type: {type(metric).__name__}"
            raise TypeError(msg)
        normalized = str(metric.this).strip().lower()
    elif isinstance(metric, exp.Identifier):
        identifier = metric.this
        if not isinstance(identifier, str):
            msg = f"Invalid metric type: {type(metric).__name__}"
            raise TypeError(msg)
        normalized = identifier.strip().lower()
    elif isinstance(metric, str):
        normalized = metric.strip().lower()
    else:
        msg = f"Invalid metric type: {type(metric).__name__}"
        raise TypeError(msg)
    if normalized not in _VALID_VECTOR_METRICS:
        msg = f"Invalid metric: {metric}. Must be one of {sorted(_VALID_VECTOR_METRICS)}"
        raise ValueError(msg)
    return normalized


def _build_vector_distance(this: exp.Expr, expression: exp.Expr, metric: Any = "euclidean") -> exp.Operator:
    normalized_metric = _normalize_metric(metric)
    node = exp.Operator(this=this, expression=expression, operator=normalized_metric)
    node.meta[_VECTOR_DISTANCE_META_KEY] = normalized_metric
    return node
