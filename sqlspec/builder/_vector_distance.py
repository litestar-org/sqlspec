"""Vector distance helpers and SQL generator registration."""

# ruff: noqa: N802
# pyright: ignore[reportConstantRedefinition]
from collections.abc import Callable, MutableMapping, Sequence
from typing import TYPE_CHECKING, Any, Final, cast

from sqlglot import exp

from sqlspec.builder._generation import invalidate_generator_dispatch

if TYPE_CHECKING:
    from sqlglot.generator import Generator

__all__ = (
    "VectorDistance",
    "build_oracle_hnsw_subquery",
    "build_oracle_hybrid_rrf_query",
    "build_oracle_hybrid_rrf_sql",
    "has_vector_distance_ancestor",
    "is_vector_distance_expression",
    "render_vector_distance_bigquery",
    "render_vector_distance_duckdb",
    "render_vector_distance_generic",
    "render_vector_distance_mysql",
    "render_vector_distance_oracle",
    "render_vector_distance_postgres",
    "vector_distance_metric",
)

_VECTOR_DISTANCE_META_KEY: Final[str] = "sqlspec_vector_distance_metric"
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


_ORACLE_MIN_ACCURACY: Final[int] = 1
_ORACLE_MAX_ACCURACY: Final[int] = 100


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
    where_clause: str | None = None,
    rank_alias: str = "vector_rank",
    distance_alias: str = "vector_distance",
) -> str:
    """Build an Oracle 26ai HNSW-preserving top-K subquery using outer ``ROWNUM``.

    Oracle 26ai CBO disables ``VECTOR INDEX HNSW SCAN`` when ``ROW_NUMBER() OVER (...)``
    appears in the same query block as ``ORDER BY VECTOR_DISTANCE(...) FETCH APPROX FIRST :k ROWS ONLY``.
    This helper isolates the ``ORDER BY VECTOR_DISTANCE(...) FETCH APPROX FIRST`` clause inside
    an inner derived table and assigns ``ROWNUM`` in the outer query block.
    """
    oracle_metric = _ORACLE_METRIC_MAP.get(metric.lower(), metric.upper())
    vec_bind = _format_oracle_bind(query_vector_bind if query_vector_bind is not None else vector_param)
    limit_bind = _format_oracle_bind(top_k_bind if top_k_bind is not None else top_k_param)
    cols = (id_column,) if tuple(select_columns) == ("id",) and id_column != "id" else tuple(select_columns)
    cols_sql = ", ".join(cols)
    where_sql = f" WHERE {where_clause}" if where_clause else ""
    accuracy_sql = ""
    if target_accuracy is not None:
        if (
            isinstance(target_accuracy, bool)
            or not isinstance(target_accuracy, int)
            or not (_ORACLE_MIN_ACCURACY <= target_accuracy <= _ORACLE_MAX_ACCURACY)
        ):
            msg = f"target_accuracy must be an integer between 1 and 100, got {target_accuracy!r}"
            raise ValueError(msg)
        accuracy_sql = f" WITH TARGET ACCURACY {target_accuracy}"
    return (
        f"SELECT {cols_sql}, {distance_alias}, ROWNUM AS {rank_alias} "
        f"FROM ("
        f"SELECT {cols_sql}, VECTOR_DISTANCE({vector_column}, {vec_bind}, {oracle_metric}) AS {distance_alias} "
        f"FROM {table}{where_sql} "
        f"ORDER BY VECTOR_DISTANCE({vector_column}, {vec_bind}, {oracle_metric}) "
        f"FETCH APPROX FIRST {limit_bind} ROWS ONLY{accuracy_sql}"
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
    where_clause: str | None = None,
) -> str:
    """Build an Oracle 26ai Hybrid Search query fusing HNSW vector similarity and Oracle Text FTS via RRF."""
    resolved_id_col = key_column if key_column is not None else id_column
    if top_k_param is not None:
        if candidate_limit_param == ":candidate_limit":
            candidate_limit_param = top_k_param
        if final_limit_param == ":final_limit":
            final_limit_param = top_k_param
    cand_bind = _format_oracle_bind(candidate_limit_param)
    final_bind = _format_oracle_bind(final_limit_param)
    txt_bind = _format_oracle_bind(text_param)
    rrf_k_expr = _format_oracle_bind(rrf_k_param) if rrf_k_param is not None else str(rrf_k)

    vec_subquery = build_oracle_hnsw_subquery(
        table=table,
        vector_column=vector_column,
        vector_param=vector_param,
        id_column=resolved_id_col,
        select_columns=(resolved_id_col,),
        metric=metric,
        top_k_param=cand_bind,
        target_accuracy=target_accuracy,
        where_clause=where_clause,
        rank_alias="vector_rank",
        distance_alias="vector_distance",
    )
    fts_where_parts = [f"CONTAINS({text_column}, {txt_bind}, 1) > 0"]
    if where_clause:
        fts_where_parts.append(f"({where_clause})")
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

    if select_columns:
        base_cols = ", ".join(col if "." in col or col == "*" else f"base.{col}" for col in select_columns)
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
        where_clause=where_clause,
    )


def _format_oracle_bind(param: str) -> str:
    stripped = param.strip()
    if stripped.startswith(":") or stripped.isdigit() or "(" in stripped:
        return stripped
    return f":{stripped}"


def _render_with_metric(generator: "Generator", expression: exp.Operator, dialect: str) -> str:
    left_sql = generator.sql(expression, "this")
    right_sql = generator.sql(expression, "expression")
    metric = vector_distance_metric(expression)

    dialect_class = getattr(generator.dialect, "__class__", None)
    if dialect_class is not None and dialect_class.__name__ == "Spangres":
        dialect = "bigquery"

    if dialect == "postgres":
        return render_vector_distance_postgres(left_sql, right_sql, metric)
    if dialect == "mysql":
        if isinstance(expression.expression, exp.Array):
            values = [
                str(item.this) if isinstance(item, exp.Literal) else generator.sql(item)
                for item in expression.expression.expressions
            ]
            right_sql = f"STRING_TO_VECTOR('[{', '.join(values)}]')"
        return render_vector_distance_mysql(left_sql, right_sql, metric)
    if dialect == "oracle":
        if isinstance(expression.expression, exp.Array):
            values = [
                str(item.this) if isinstance(item, exp.Literal) else generator.sql(item)
                for item in expression.expression.expressions
            ]
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
    """Normalize vector metrics to a lowercase string."""
    if isinstance(metric, exp.Literal):
        return str(metric.this).lower()
    if isinstance(metric, exp.Identifier):
        identifier = metric.this
        return identifier.lower() if isinstance(identifier, str) else "euclidean"
    if isinstance(metric, str):
        return metric.lower()
    return "euclidean"


def _build_vector_distance(this: exp.Expr, expression: exp.Expr, metric: Any = "euclidean") -> exp.Operator:
    normalized_metric = _normalize_metric(metric)
    node = exp.Operator(this=this, expression=expression, operator=normalized_metric)
    node.meta[_VECTOR_DISTANCE_META_KEY] = normalized_metric
    return node
