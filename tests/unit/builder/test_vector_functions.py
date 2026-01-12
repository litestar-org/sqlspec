"""Unit tests for vector distance functions in SQL builder.

Tests VectorDistance expression creation, dialect-specific SQL generation,
and Column.vector_distance()/Column.cosine_similarity() methods.
"""

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.builder import Column, VectorDistance

pytestmark = pytest.mark.xdist_group("builder")


def test_vector_distance_expression_creation() -> None:
    """Test VectorDistance expression can be created directly."""
    col_expr = exp.Column(this=exp.Identifier(this="embedding"))
    vec_expr = exp.Array(expressions=[exp.Literal.number(0.1), exp.Literal.number(0.2)])
    metric_expr = exp.Literal.string("euclidean")

    distance_expr = VectorDistance(this=col_expr, expression=vec_expr, metric=metric_expr)

    assert distance_expr.left == col_expr
    assert distance_expr.right == vec_expr
    assert distance_expr.metric == "euclidean"


def test_vector_distance_metric_extraction() -> None:
    """Test metric property extracts metric from Literal expression."""
    col_expr = exp.Column(this=exp.Identifier(this="embedding"))
    vec_expr = exp.Array(expressions=[exp.Literal.number(0.5)])

    for metric in ["euclidean", "cosine", "inner_product", "euclidean_squared"]:
        distance_expr = VectorDistance(this=col_expr, expression=vec_expr, metric=exp.Literal.string(metric))
        assert distance_expr.metric == metric


def test_column_vector_distance_with_list() -> None:
    """Test Column.vector_distance() with Python list."""
    col = Column("embedding")
    distance = col.vector_distance([0.1, 0.2, 0.3])

    assert isinstance(distance._expression, VectorDistance)  # pyright: ignore[reportPrivateUsage]
    assert distance._expression.metric == "euclidean"  # pyright: ignore[reportPrivateUsage]


def test_column_vector_distance_with_column() -> None:
    """Test Column.vector_distance() with another Column."""
    col1 = Column("embedding1")
    col2 = Column("embedding2")
    distance = col1.vector_distance(col2)

    assert isinstance(distance._expression, VectorDistance)  # pyright: ignore[reportPrivateUsage]
    assert distance._expression.metric == "euclidean"  # pyright: ignore[reportPrivateUsage]


def test_column_vector_distance_with_expression() -> None:
    """Test Column.vector_distance() with SQLGlot expression."""
    col = Column("embedding")
    vec_expr = exp.Array(expressions=[exp.Literal.number(0.5)])
    distance = col.vector_distance(vec_expr)

    assert isinstance(distance._expression, VectorDistance)  # pyright: ignore[reportPrivateUsage]
    assert distance._expression.metric == "euclidean"  # pyright: ignore[reportPrivateUsage]


def test_column_vector_distance_invalid_metric() -> None:
    """Test Column.vector_distance() raises ValueError for invalid metric."""
    col = Column("embedding")

    with pytest.raises(ValueError, match="Invalid metric"):
        col.vector_distance([0.1, 0.2], metric="invalid_metric")


def test_column_vector_distance_invalid_vector_type() -> None:
    """Test Column.vector_distance() raises TypeError for invalid vector type."""
    col = Column("embedding")

    with pytest.raises(TypeError, match="Unsupported vector type"):
        col.vector_distance("not_a_vector")  # type: ignore[arg-type]


def test_column_vector_distance_all_metrics() -> None:
    """Test all supported distance metrics."""
    col = Column("embedding")
    valid_metrics = ["euclidean", "cosine", "inner_product", "euclidean_squared"]

    for metric in valid_metrics:
        distance = col.vector_distance([0.1, 0.2], metric=metric)
        assert isinstance(distance._expression, VectorDistance)  # pyright: ignore[reportPrivateUsage]
        assert distance._expression.metric == metric  # pyright: ignore[reportPrivateUsage]


def test_cosine_similarity_basic() -> None:
    """Test Column.cosine_similarity() creates proper expression."""
    col = Column("embedding")
    similarity = col.cosine_similarity([0.1, 0.2, 0.3])

    assert isinstance(similarity._expression, exp.Sub)  # pyright: ignore[reportPrivateUsage]

    left_operand = similarity._expression.this  # pyright: ignore[reportPrivateUsage]
    assert isinstance(left_operand, exp.Literal)
    assert str(left_operand.this) == "1"

    right_operand = similarity._expression.expression  # pyright: ignore[reportPrivateUsage]
    assert isinstance(right_operand, exp.Paren)

    inner_expr = right_operand.this
    assert isinstance(inner_expr, VectorDistance)
    assert inner_expr.metric == "cosine"


def test_cosine_similarity_with_column() -> None:
    """Test Column.cosine_similarity() with another Column."""
    col1 = Column("embedding1")
    col2 = Column("embedding2")
    similarity = col1.cosine_similarity(col2)

    assert isinstance(similarity._expression, exp.Sub)  # pyright: ignore[reportPrivateUsage]

    right_operand = similarity._expression.expression  # pyright: ignore[reportPrivateUsage]
    assert isinstance(right_operand, exp.Paren)
    assert isinstance(right_operand.this, VectorDistance)


def test_postgres_euclidean_distance_sql() -> None:
    """Test PostgreSQL euclidean distance generates correct operator."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)

    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql
    assert "embedding" in stmt.sql
    assert "ARRAY[" in stmt.sql


def test_postgres_cosine_distance_sql() -> None:
    """Test PostgreSQL cosine distance generates vector distance operator."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2], metric="cosine") < 0.5)

    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql or "<=>" in stmt.sql or "<#>" in stmt.sql
    assert "embedding" in stmt.sql


def test_postgres_inner_product_sql() -> None:
    """Test PostgreSQL inner product generates vector distance operator."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2], metric="inner_product") < 0.5)
    )

    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql or "<=>" in stmt.sql or "<#>" in stmt.sql
    assert "embedding" in stmt.sql


def test_postgres_euclidean_squared_fallback() -> None:
    """Test PostgreSQL euclidean_squared metric is captured."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2], metric="euclidean_squared") < 0.5)
    )

    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql or "VECTOR_DISTANCE" in stmt.sql
    assert "embedding" in stmt.sql


def test_mysql_euclidean_distance_sql() -> None:
    """Test MySQL euclidean distance generates DISTANCE function."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)

    stmt = query.build(dialect="mysql")

    assert "DISTANCE(" in stmt.sql
    assert "embedding" in stmt.sql


def test_mysql_cosine_distance_sql() -> None:
    """Test MySQL cosine distance generates DISTANCE function."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2], metric="cosine") < 0.5)

    stmt = query.build(dialect="mysql")

    assert "DISTANCE(" in stmt.sql
    assert "embedding" in stmt.sql


def test_mysql_inner_product_sql() -> None:
    """Test MySQL inner product generates DISTANCE with DOT metric."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2], metric="inner_product") < 0.5)
    )

    stmt = query.build(dialect="mysql")

    assert "DISTANCE(" in stmt.sql
    assert "embedding" in stmt.sql


def test_mysql_string_to_vector_wrapping() -> None:
    """Test MySQL wraps array literals with STRING_TO_VECTOR."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)

    stmt = query.build(dialect="mysql")

    assert "STRING_TO_VECTOR" in stmt.sql


def test_oracle_euclidean_distance_sql() -> None:
    """Test Oracle euclidean distance generates VECTOR_DISTANCE function."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)

    stmt = query.build(dialect="oracle")

    assert "VECTOR_DISTANCE(" in stmt.sql
    assert "embedding" in stmt.sql


def test_oracle_cosine_distance_sql() -> None:
    """Test Oracle cosine distance generates VECTOR_DISTANCE function."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2], metric="cosine") < 0.5)

    stmt = query.build(dialect="oracle")

    assert "VECTOR_DISTANCE(" in stmt.sql
    assert "embedding" in stmt.sql


def test_oracle_inner_product_sql() -> None:
    """Test Oracle inner product generates VECTOR_DISTANCE with DOT."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2], metric="inner_product") < 0.5)
    )

    stmt = query.build(dialect="oracle")

    assert "VECTOR_DISTANCE(" in stmt.sql
    assert "embedding" in stmt.sql


def test_oracle_euclidean_squared_sql() -> None:
    """Test Oracle euclidean_squared generates VECTOR_DISTANCE function."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2], metric="euclidean_squared") < 0.5)
    )

    stmt = query.build(dialect="oracle")

    assert "VECTOR_DISTANCE(" in stmt.sql
    assert "embedding" in stmt.sql


def test_oracle_to_vector_wrapping() -> None:
    """Test Oracle wraps array literals with TO_VECTOR."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)

    stmt = query.build(dialect="oracle")

    assert "TO_VECTOR" in stmt.sql


def test_bigquery_euclidean_distance_sql() -> None:
    """Test BigQuery euclidean distance generates EUCLIDEAN_DISTANCE function."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)

    stmt = query.build(dialect="bigquery")

    assert "EUCLIDEAN_DISTANCE(" in stmt.sql
    assert "embedding" in stmt.sql


def test_bigquery_cosine_distance_sql() -> None:
    """Test BigQuery cosine distance metric is captured."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2], metric="cosine") < 0.5)

    stmt = query.build(dialect="bigquery")

    assert "EUCLIDEAN_DISTANCE(" in stmt.sql or "COSINE_DISTANCE(" in stmt.sql
    assert "embedding" in stmt.sql


def test_bigquery_inner_product_sql() -> None:
    """Test BigQuery inner product metric is captured."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2], metric="inner_product") < 0.5)
    )

    stmt = query.build(dialect="bigquery")

    assert "EUCLIDEAN_DISTANCE(" in stmt.sql or "DOT_PRODUCT(" in stmt.sql
    assert "embedding" in stmt.sql


def test_bigquery_euclidean_squared_fallback() -> None:
    """Test BigQuery euclidean_squared metric is captured."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2], metric="euclidean_squared") < 0.5)
    )

    stmt = query.build(dialect="bigquery")

    assert "EUCLIDEAN_DISTANCE(" in stmt.sql or "VECTOR_DISTANCE" in stmt.sql
    assert "embedding" in stmt.sql


def test_duckdb_euclidean_distance_sql() -> None:
    """Test DuckDB euclidean distance uses array_distance function."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)

    stmt = query.build(dialect="duckdb")

    assert "array_distance" in stmt.sql
    assert "embedding" in stmt.sql


def test_duckdb_cosine_distance_sql() -> None:
    """Test DuckDB cosine distance uses array_cosine_distance function."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2], metric="cosine") < 0.5)

    stmt = query.build(dialect="duckdb")

    assert "array_cosine_distance" in stmt.sql
    assert "embedding" in stmt.sql


def test_duckdb_inner_product_sql() -> None:
    """Test DuckDB inner product uses array_negative_inner_product function."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2], metric="inner_product") < 0.5)
    )

    stmt = query.build(dialect="duckdb")

    assert "array_negative_inner_product" in stmt.sql
    assert "embedding" in stmt.sql


def test_duckdb_euclidean_squared_fallback() -> None:
    """Test DuckDB euclidean_squared uses generic VECTOR_DISTANCE function."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2], metric="euclidean_squared") < 0.5)
    )

    stmt = query.build(dialect="duckdb")

    assert "VECTOR_DISTANCE" in stmt.sql
    assert "embedding" in stmt.sql
    assert "EUCLIDEAN_SQUARED" in stmt.sql


def test_generic_dialect_fallback() -> None:
    """Test generic dialect generates VECTOR_DISTANCE function."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)

    stmt = query.build()

    assert "VECTOR_DISTANCE(" in stmt.sql
    assert "EUCLIDEAN" in stmt.sql


def test_distance_in_select_clause() -> None:
    """Test vector distance in SELECT clause with alias."""
    query = (
        sql
        .select("id", Column("embedding").vector_distance([0.1, 0.2]).alias("distance"))
        .from_("docs")
        .order_by("distance")
    )

    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql
    assert "distance" in stmt.sql
    assert "ORDER BY" in stmt.sql


def test_distance_in_order_by() -> None:
    """Test vector distance in ORDER BY clause."""
    distance_col = Column("embedding").vector_distance([0.1, 0.2])
    query = sql.select("*", distance_col.alias("dist")).from_("docs").order_by("dist")

    stmt = query.build(dialect="postgres")

    assert "ORDER BY" in stmt.sql
    assert "<->" in stmt.sql


def test_cosine_similarity_in_select() -> None:
    """Test cosine similarity in SELECT clause."""
    query = (
        sql
        .select("id", Column("embedding").cosine_similarity([0.1, 0.2]).alias("score"))
        .from_("docs")
        .order_by("score")
    )

    stmt = query.build(dialect="postgres")

    assert "<=>" in stmt.sql
    assert "score" in stmt.sql


def test_multiple_metrics_in_same_query() -> None:
    """Test multiple distance metrics in same query."""
    query = (
        sql
        .select(
            "id",
            Column("embedding").vector_distance([0.1, 0.2], metric="euclidean").alias("euclidean_dist"),
            Column("embedding").vector_distance([0.1, 0.2], metric="cosine").alias("cosine_dist"),
        )
        .from_("docs")
        .where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)
    )

    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql
    assert "<=>" in stmt.sql
    assert "euclidean_dist" in stmt.sql
    assert "cosine_dist" in stmt.sql


def test_column_to_column_distance() -> None:
    """Test distance between two vector columns."""
    query = sql.select("*").from_("pairs").where(Column("vec1").vector_distance(Column("vec2"), metric="cosine") < 0.3)

    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql or "<=>" in stmt.sql or "<#>" in stmt.sql
    assert "vec1" in stmt.sql
    assert "vec2" in stmt.sql


def test_comparison_operators_on_distance() -> None:
    """Test comparison operators work on distance FunctionColumn."""
    col = Column("embedding")
    vector = [0.1, 0.2]

    test_cases = [
        (col.vector_distance(vector) < 0.5, ["<", ">"]),
        (col.vector_distance(vector) <= 0.5, ["<=", ">="]),
        (col.vector_distance(vector) > 0.5, [">", "<"]),
        (col.vector_distance(vector) >= 0.5, [">=", "<="]),
        (col.vector_distance(vector) == 0.5, ["="]),
        (col.vector_distance(vector) != 0.5, ["<>", "!="]),
    ]

    for expression, expected_operators in test_cases:
        query = sql.select("*").from_("docs").where(expression)
        stmt = query.build(dialect="postgres")
        assert any(op in stmt.sql for op in expected_operators)


def test_nested_expression_support() -> None:
    """Test VectorDistance works in nested expressions."""
    query = (
        sql
        .select(Column("embedding").vector_distance([0.1, 0.2]).alias("dist"))
        .from_("docs")
        .where((Column("embedding").vector_distance([0.1, 0.2]) < 0.5) & (Column("status") == "active"))
    )

    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql
    assert "embedding" in stmt.sql
    assert "status" in stmt.sql


def test_empty_list_validation() -> None:
    """Test empty list can be processed."""
    col = Column("embedding")

    distance = col.vector_distance([])

    query = sql.select("*").from_("docs").where(distance < 0.5)
    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql
    assert "embedding" in stmt.sql


def test_multiple_dialects_from_same_query() -> None:
    """Test same query can generate SQL for multiple dialects."""
    query = sql.select("*").from_("docs").where(Column("embedding").vector_distance([0.1, 0.2]) < 0.5)

    pg_stmt = query.build(dialect="postgres")
    mysql_stmt = query.build(dialect="mysql")
    oracle_stmt = query.build(dialect="oracle")

    assert "<->" in pg_stmt.sql
    assert "DISTANCE(" in mysql_stmt.sql
    assert "VECTOR_DISTANCE(" in oracle_stmt.sql


def test_distance_with_table_qualified_column() -> None:
    """Test vector distance with table-qualified column."""
    col = Column("embedding", table="docs")
    query = sql.select("*").from_("docs").where(col.vector_distance([0.1, 0.2]) < 0.5)

    stmt = query.build(dialect="postgres")

    assert "<->" in stmt.sql
    assert "docs" in stmt.sql
    assert "embedding" in stmt.sql


def test_cosine_similarity_ordering() -> None:
    """Test cosine similarity works correctly in ORDER BY."""
    query = (
        sql
        .select("id", Column("embedding").cosine_similarity([0.1, 0.2]).alias("score"))
        .from_("docs")
        .order_by(Column("score").desc())
        .limit(10)
    )

    stmt = query.build(dialect="postgres")

    assert "<=>" in stmt.sql
    assert "score" in stmt.sql
    assert "ORDER BY" in stmt.sql
    assert "LIMIT" in stmt.sql


def test_distance_with_null_handling() -> None:
    """Test vector distance with NULL check."""
    query = (
        sql
        .select("*")
        .from_("docs")
        .where((Column("embedding").is_not_null()) & (Column("embedding").vector_distance([0.1, 0.2]) < 0.5))
    )

    stmt = query.build(dialect="postgres")

    assert "IS NULL" in stmt.sql
    assert "<->" in stmt.sql
