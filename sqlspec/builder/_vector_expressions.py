"""Custom SQLGlot expressions for vector distance operations.

Provides dialect-specific SQL generation for vector similarity search
across PostgreSQL (pgvector), MySQL 9+, and Oracle 23ai+.
"""

from typing import Any

from sqlglot import exp

__all__ = ("VectorDistance",)


class VectorDistance(exp.Expression):
    """Vector distance expression with dialect-specific generation.

    Generates database-specific SQL for vector distance calculations:
    - PostgreSQL (pgvector): Operators <->, <=>, <#>
    - MySQL 9+: DISTANCE(col, vec, 'METRIC') function
    - Oracle 23ai+: VECTOR_DISTANCE(col, vec, METRIC) function
    - Generic: VECTOR_DISTANCE(col, vec, 'METRIC') function

    The metric is stored as a raw string attribute (not parametrized) and drives
    dialect-specific generation at SQL build time.
    """

    arg_types = {"this": True, "expression": True, "metric": False}

    def __init__(self, **args: Any) -> None:
        """Initialize VectorDistance with metric stored in args."""
        metric_value = args.get("metric", "euclidean")
        if isinstance(metric_value, exp.Literal):
            metric_value = str(metric_value.this).lower()
        elif isinstance(metric_value, exp.Identifier):
            metric_value = metric_value.this.lower()
        elif isinstance(metric_value, str):
            metric_value = metric_value.lower()
        else:
            metric_value = "euclidean"

        args["metric"] = exp.Identifier(this=metric_value)
        super().__init__(**args)

    @property
    def left(self) -> "exp.Expression":
        """Get the left operand (column)."""
        return self.this

    @property
    def right(self) -> "exp.Expression":
        """Get the right operand (vector value)."""
        return self.expression

    @property
    def metric(self) -> str:
        """Get the distance metric as raw string (not parametrized)."""
        metric_expr = self.args.get("metric")
        if isinstance(metric_expr, exp.Identifier):
            return metric_expr.this.lower()
        return "euclidean"

    def sql(self, dialect: "Any | None" = None, **opts: Any) -> str:
        """Generate dialect-specific SQL.

        This overrides the default sql() method to provide custom
        dialect-specific generation for vector distance operations.

        Args:
            dialect: Target SQL dialect (postgres, mysql, oracle, bigquery, duckdb, etc.)
            **opts: Additional SQL generation options

        Returns:
            Dialect-specific SQL string
        """
        dialect_name = str(dialect).lower() if dialect else "generic"

        left_sql = self.left.sql(dialect=dialect, **opts)
        right_sql = self.right.sql(dialect=dialect, **opts)
        metric = self.metric

        if dialect_name in {"postgres", "postgresql"}:
            return self._sql_postgres(left_sql, right_sql, metric)

        if dialect_name == "mysql":
            return self._sql_mysql(left_sql, right_sql, metric)

        if dialect_name == "oracle":
            return self._sql_oracle(left_sql, right_sql, metric)

        if dialect_name == "bigquery":
            return self._sql_bigquery(left_sql, right_sql, metric)

        if dialect_name == "duckdb":
            return self._sql_duckdb(left_sql, right_sql, metric)

        return self._sql_generic(left_sql, right_sql, metric)

    def _sql_postgres(self, left: str, right: str, metric: str) -> str:
        """Generate PostgreSQL pgvector operator syntax."""
        operator_map = {"euclidean": "<->", "cosine": "<=>", "inner_product": "<#>"}

        operator = operator_map.get(metric)
        if operator:
            return f"{left} {operator} {right}"

        return self._sql_generic(left, right, metric)

    def _sql_mysql(self, left: str, right: str, metric: str) -> str:
        """Generate MySQL DISTANCE function syntax."""
        metric_map = {"euclidean": "EUCLIDEAN", "cosine": "COSINE", "inner_product": "DOT"}

        mysql_metric = metric_map.get(metric, "EUCLIDEAN")

        if ("ARRAY" in right or "[" in right) and "STRING_TO_VECTOR" not in right:
            right = f"STRING_TO_VECTOR({right})"

        return f"DISTANCE({left}, {right}, '{mysql_metric}')"

    def _sql_oracle(self, left: str, right: str, metric: str) -> str:
        """Generate Oracle VECTOR_DISTANCE function syntax."""
        metric_map = {
            "euclidean": "EUCLIDEAN",
            "cosine": "COSINE",
            "inner_product": "DOT",
            "euclidean_squared": "EUCLIDEAN_SQUARED",
        }

        oracle_metric = metric_map.get(metric, "EUCLIDEAN")

        if ("ARRAY" in right or "[" in right) and "TO_VECTOR" not in right:
            right = f"TO_VECTOR({right})"

        return f"VECTOR_DISTANCE({left}, {right}, {oracle_metric})"

    def _sql_bigquery(self, left: str, right: str, metric: str) -> str:
        """Generate BigQuery vector distance function syntax."""
        function_map = {"euclidean": "EUCLIDEAN_DISTANCE", "cosine": "COSINE_DISTANCE", "inner_product": "DOT_PRODUCT"}

        function_name = function_map.get(metric)
        if function_name:
            return f"{function_name}({left}, {right})"

        return self._sql_generic(left, right, metric)

    def _sql_duckdb(self, left: str, right: str, metric: str) -> str:
        """Generate DuckDB vector distance function syntax.

        DuckDB's array_distance() only accepts 2 parameters and computes euclidean distance.
        For other metrics or more control, use the generic VECTOR_DISTANCE fallback.
        """
        return self._sql_generic(left, right, metric)

    def _sql_generic(self, left: str, right: str, metric: str) -> str:
        """Generate generic VECTOR_DISTANCE function syntax."""
        return f"VECTOR_DISTANCE({left}, {right}, '{metric.upper()}')"


def _register_with_sqlglot() -> None:
    """Register VectorDistance with SQLGlot's generator dispatch system."""
    from sqlglot.dialects.bigquery import BigQuery
    from sqlglot.dialects.duckdb import DuckDB
    from sqlglot.dialects.mysql import MySQL
    from sqlglot.dialects.oracle import Oracle
    from sqlglot.dialects.postgres import Postgres
    from sqlglot.generator import Generator

    def vector_distance_sql_base(generator: "Generator", expression: "VectorDistance") -> str:
        """Base generator for VectorDistance expressions."""
        return expression._sql_generic(generator.sql(expression.left), generator.sql(expression.right), expression.metric)

    def vector_distance_sql_postgres(generator: "Generator", expression: "VectorDistance") -> str:
        """PostgreSQL generator for VectorDistance expressions."""
        return expression._sql_postgres(generator.sql(expression.left), generator.sql(expression.right), expression.metric)

    def vector_distance_sql_mysql(generator: "Generator", expression: "VectorDistance") -> str:
        """MySQL generator for VectorDistance expressions."""
        return expression._sql_mysql(generator.sql(expression.left), generator.sql(expression.right), expression.metric)

    def vector_distance_sql_oracle(generator: "Generator", expression: "VectorDistance") -> str:
        """Oracle generator for VectorDistance expressions."""
        return expression._sql_oracle(generator.sql(expression.left), generator.sql(expression.right), expression.metric)

    def vector_distance_sql_bigquery(generator: "Generator", expression: "VectorDistance") -> str:
        """BigQuery generator for VectorDistance expressions."""
        return expression._sql_bigquery(generator.sql(expression.left), generator.sql(expression.right), expression.metric)

    def vector_distance_sql_duckdb(generator: "Generator", expression: "VectorDistance") -> str:
        """DuckDB generator for VectorDistance expressions."""
        return expression._sql_duckdb(generator.sql(expression.left), generator.sql(expression.right), expression.metric)

    Generator.TRANSFORMS[VectorDistance] = vector_distance_sql_base

    Postgres.Generator.TRANSFORMS[VectorDistance] = vector_distance_sql_postgres
    MySQL.Generator.TRANSFORMS[VectorDistance] = vector_distance_sql_mysql
    Oracle.Generator.TRANSFORMS[VectorDistance] = vector_distance_sql_oracle
    BigQuery.Generator.TRANSFORMS[VectorDistance] = vector_distance_sql_bigquery
    DuckDB.Generator.TRANSFORMS[VectorDistance] = vector_distance_sql_duckdb


_register_with_sqlglot()
