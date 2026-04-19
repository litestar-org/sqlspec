"""Vector distance helpers and SQL generator registration."""

# ruff: noqa: N802
# pyright: ignore[reportConstantRedefinition]
from collections.abc import Callable, MutableMapping
from typing import TYPE_CHECKING, Any, Final, TypeAlias, cast

from sqlglot import exp

__all__ = (
    "VectorDistance",
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

if TYPE_CHECKING:
    from sqlglot.dialects.bigquery import BigQuery
    from sqlglot.dialects.duckdb import DuckDB
    from sqlglot.dialects.mysql import MySQL
    from sqlglot.dialects.oracle import Oracle
    from sqlglot.dialects.postgres import Postgres
    from sqlglot.generator import Generator

    from sqlspec.dialects.spanner import Spangres, Spanner

SupportedVectorDistanceDialect: TypeAlias = "BigQuery | DuckDB | MySQL | Oracle | Postgres | Spangres | Spanner"

_VECTOR_DISTANCE_META_KEY: Final[str] = "sqlspec_vector_distance_metric"
_OperatorTransform = Callable[[Any, exp.Operator], str]
_SQLGLOT_VECTOR_DISTANCE_REGISTERED = False
_BASE_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_POSTGRES_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_MYSQL_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_ORACLE_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_BIGQUERY_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_DUCKDB_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_SPANNER_OPERATOR_TRANSFORM: _OperatorTransform | None = None
_SPANGRES_OPERATOR_TRANSFORM: _OperatorTransform | None = None


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


def _build_vector_distance(this: exp.Expr, expression: exp.Expr, metric: Any = "euclidean") -> exp.Operator:
    normalized_metric = _normalize_metric(metric)
    node = exp.Operator(this=this, expression=expression, operator=normalized_metric)
    node.meta[_VECTOR_DISTANCE_META_KEY] = normalized_metric
    return node


def VectorDistance(*, this: exp.Expr, expression: exp.Expr, metric: Any = "euclidean") -> exp.Operator:
    """Build a SQLSpec vector-distance expression."""
    _register_with_sqlglot()
    return _build_vector_distance(this=this, expression=expression, metric=metric)


def render_vector_distance_postgres(left: str, right: str, metric: str) -> str:
    """Render PostgreSQL pgvector operator syntax."""
    operator_map = {"euclidean": "<->", "cosine": "<=>", "inner_product": "<#>"}

    operator = operator_map.get(metric)
    if operator:
        return f"{left} {operator} {right}"

    return render_vector_distance_generic(left, right, metric)


def render_vector_distance_mysql(left: str, right: str, metric: str) -> str:
    """Render MySQL DISTANCE function syntax."""
    metric_map = {"euclidean": "EUCLIDEAN", "cosine": "COSINE", "inner_product": "DOT"}

    mysql_metric = metric_map.get(metric, "EUCLIDEAN")

    if ("ARRAY" in right or "[" in right) and "STRING_TO_VECTOR" not in right:
        right = f"STRING_TO_VECTOR({right})"

    return f"DISTANCE({left}, {right}, '{mysql_metric}')"


def render_vector_distance_oracle(left: str, right: str, metric: str) -> str:
    """Render Oracle VECTOR_DISTANCE function syntax."""
    metric_map = {
        "euclidean": "EUCLIDEAN",
        "cosine": "COSINE",
        "inner_product": "DOT",
        "euclidean_squared": "EUCLIDEAN_SQUARED",
    }

    oracle_metric = metric_map.get(metric, "EUCLIDEAN")

    if ("[" in right or "ARRAY" in right) and "TO_VECTOR" not in right:
        right = f"TO_VECTOR({right})"

    return f"VECTOR_DISTANCE({left}, {right}, {oracle_metric})"


def render_vector_distance_bigquery(left: str, right: str, metric: str) -> str:
    """Render BigQuery vector distance function syntax."""
    function_map = {"euclidean": "EUCLIDEAN_DISTANCE", "cosine": "COSINE_DISTANCE", "inner_product": "DOT_PRODUCT"}

    function_name = function_map.get(metric)
    if function_name:
        return f"{function_name}({left}, {right})"

    return render_vector_distance_generic(left, right, metric)


def render_vector_distance_duckdb(left: str, right: str, metric: str) -> str:
    """Render DuckDB VSS extension function syntax."""
    function_map = {
        "euclidean": "array_distance",
        "cosine": "array_cosine_distance",
        "inner_product": "array_negative_inner_product",
    }
    function_name = function_map.get(metric)
    if function_name:
        return f"{function_name}({left}, CAST({right} AS DOUBLE[]))"

    return render_vector_distance_generic(left, right, metric)


def render_vector_distance_generic(left: str, right: str, metric: str) -> str:
    """Render generic VECTOR_DISTANCE function syntax."""
    return f"VECTOR_DISTANCE({left}, {right}, '{metric.upper()}')"


def _render_with_metric(generator: "Generator", expression: exp.Operator, dialect: str) -> str:
    left_sql = generator.sql(expression, "this")
    right_sql = generator.sql(expression, "expression")
    metric = vector_distance_metric(expression)

    if dialect == "postgres":
        return render_vector_distance_postgres(left_sql, right_sql, metric)
    if dialect == "mysql":
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
        if isinstance(expression.expression, exp.Array) and expression.expression.expressions:
            target_type = f"DOUBLE[{len(expression.expression.expressions)}]"
        else:
            target_type = "DOUBLE[]"
        function_map = {
            "euclidean": "array_distance",
            "cosine": "array_cosine_distance",
            "inner_product": "array_negative_inner_product",
        }
        function_name = function_map.get(metric)
        if function_name:
            return f"{function_name}({left_sql}, CAST({right_sql} AS {target_type}))"
        return render_vector_distance_generic(left_sql, right_sql, metric)

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


def _operator_sql_spanner(generator: "Generator", expression: exp.Operator) -> str:
    if is_vector_distance_expression(expression):
        return _render_with_metric(generator, expression, "bigquery")
    return _require_operator_transform(_SPANNER_OPERATOR_TRANSFORM)(generator, expression)


def _operator_sql_spangres(generator: "Generator", expression: exp.Operator) -> str:
    if is_vector_distance_expression(expression):
        return _render_with_metric(generator, expression, "postgres")
    return _require_operator_transform(_SPANGRES_OPERATOR_TRANSFORM)(generator, expression)


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
        _DUCKDB_OPERATOR_TRANSFORM, \
        _SPANNER_OPERATOR_TRANSFORM, \
        _SPANGRES_OPERATOR_TRANSFORM

    if _SQLGLOT_VECTOR_DISTANCE_REGISTERED:
        return

    from sqlglot.dialects.bigquery import BigQuery
    from sqlglot.dialects.duckdb import DuckDB
    from sqlglot.dialects.mysql import MySQL
    from sqlglot.dialects.oracle import Oracle
    from sqlglot.dialects.postgres import Postgres
    from sqlglot.generator import Generator

    from sqlspec.dialects.spanner import Spangres, Spanner

    _BASE_OPERATOR_TRANSFORM = cast("_OperatorTransform", Generator.TRANSFORMS[exp.Operator])
    _POSTGRES_OPERATOR_TRANSFORM = cast("_OperatorTransform", Postgres.Generator.TRANSFORMS[exp.Operator])
    _MYSQL_OPERATOR_TRANSFORM = cast("_OperatorTransform", MySQL.Generator.TRANSFORMS[exp.Operator])
    _ORACLE_OPERATOR_TRANSFORM = cast("_OperatorTransform", Oracle.Generator.TRANSFORMS[exp.Operator])
    _BIGQUERY_OPERATOR_TRANSFORM = cast("_OperatorTransform", BigQuery.Generator.TRANSFORMS[exp.Operator])
    _DUCKDB_OPERATOR_TRANSFORM = cast("_OperatorTransform", DuckDB.Generator.TRANSFORMS[exp.Operator])
    _SPANNER_OPERATOR_TRANSFORM = cast("_OperatorTransform", Spanner.Generator.TRANSFORMS[exp.Operator])
    _SPANGRES_OPERATOR_TRANSFORM = cast("_OperatorTransform", Spangres.Generator.TRANSFORMS[exp.Operator])

    _register_operator_transform(Generator.TRANSFORMS, _operator_sql_base)
    _register_operator_transform(Postgres.Generator.TRANSFORMS, _operator_sql_postgres)
    _register_operator_transform(MySQL.Generator.TRANSFORMS, _operator_sql_mysql)
    _register_operator_transform(Oracle.Generator.TRANSFORMS, _operator_sql_oracle)
    _register_operator_transform(BigQuery.Generator.TRANSFORMS, _operator_sql_bigquery)
    _register_operator_transform(DuckDB.Generator.TRANSFORMS, _operator_sql_duckdb)
    _register_operator_transform(Spanner.Generator.TRANSFORMS, _operator_sql_spanner)
    _register_operator_transform(Spangres.Generator.TRANSFORMS, _operator_sql_spangres)

    # sqlglot caches the dispatch table (built from TRANSFORMS) per Generator class
    # in _DISPATCH_CACHE. We must invalidate stale entries so the next instantiation
    # picks up our new Operator transforms.
    from sqlglot.generator import _DISPATCH_CACHE  # pyright: ignore[reportPrivateUsage]

    for gen_cls in (
        Generator,
        Postgres.Generator,
        MySQL.Generator,
        Oracle.Generator,
        BigQuery.Generator,
        DuckDB.Generator,
        Spanner.Generator,
        Spangres.Generator,
    ):
        _DISPATCH_CACHE.pop(gen_cls, None)

    _SQLGLOT_VECTOR_DISTANCE_REGISTERED = True
