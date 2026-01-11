"""Integration tests for vector distance functions with Oracle 23ai+ VECTOR support.

Tests actual execution of vector distance queries using Oracle Database 23ai+
VECTOR_DISTANCE function with various distance metrics.
"""

import math
from collections.abc import AsyncGenerator

import pytest

from sqlspec import sql
from sqlspec.adapters.oracledb import OracleAsyncDriver
from sqlspec.builder import Column

pytestmark = [pytest.mark.xdist_group("oracle")]


@pytest.fixture
async def oracle_vector_session(oracle_async_session: OracleAsyncDriver) -> AsyncGenerator[OracleAsyncDriver, None]:
    """Create Oracle session with VECTOR support and test table."""
    try:
        await oracle_async_session.execute_script(
            """
            CREATE TABLE vector_docs_oracle (
                id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                content VARCHAR2(100) NOT NULL,
                embedding VECTOR(3, FLOAT32)
            )
            """
        )

        await oracle_async_session.execute(
            "INSERT INTO vector_docs_oracle (content, embedding) VALUES (:1, :2)", ("doc1", "[0.1, 0.2, 0.3]")
        )
        await oracle_async_session.execute(
            "INSERT INTO vector_docs_oracle (content, embedding) VALUES (:1, :2)", ("doc2", "[0.4, 0.5, 0.6]")
        )
        await oracle_async_session.execute(
            "INSERT INTO vector_docs_oracle (content, embedding) VALUES (:1, :2)", ("doc3", "[0.7, 0.8, 0.9]")
        )

        yield oracle_async_session
    finally:
        await oracle_async_session.execute_script("DROP TABLE vector_docs_oracle")


async def test_oracle_euclidean_distance_execution(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test Oracle VECTOR_DISTANCE euclidean metric execution."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_oracle")
        .order_by("distance")
    )

    result = await oracle_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"
    assert result[2]["content"] == "doc3"

    assert result[0]["distance"] < result[1]["distance"]
    assert result[1]["distance"] < result[2]["distance"]


async def test_oracle_euclidean_distance_threshold(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test Oracle euclidean distance with threshold filter."""
    query = (
        sql
        .select("content")
        .from_("vector_docs_oracle")
        .where(sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 0.3)
    )

    result = await oracle_vector_session.execute(query)

    assert len(result) == 1
    assert result[0]["content"] == "doc1"


async def test_oracle_cosine_distance_execution(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test Oracle VECTOR_DISTANCE cosine metric execution."""
    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance"))
        .from_("vector_docs_oracle")
        .order_by("distance")
    )

    result = await oracle_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"


async def test_oracle_inner_product_execution(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test Oracle VECTOR_DISTANCE inner_product (DOT) metric execution."""
    query = (
        sql
        .select(
            "content",
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="inner_product").alias("distance"),
        )
        .from_("vector_docs_oracle")
        .order_by("distance")
    )

    result = await oracle_vector_session.execute(query)

    assert len(result) == 3


async def test_oracle_euclidean_squared_metric(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test Oracle-specific EUCLIDEAN_SQUARED metric."""
    query = (
        sql
        .select(
            "content",
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean_squared").alias("distance"),
        )
        .from_("vector_docs_oracle")
        .order_by("distance")
    )

    result = await oracle_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"


async def test_oracle_cosine_similarity_execution(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test Oracle cosine similarity calculation."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_oracle")
        .order_by(sql.column("score").desc())
    )

    result = await oracle_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"

    assert result[0]["score"] > result[1]["score"]
    assert result[1]["score"] > result[2]["score"]


async def test_oracle_similarity_top_k_results(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test top-K similarity search."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_oracle")
        .order_by(sql.column("score").desc())
    )
    query = query.limit(2)

    result = await oracle_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"


async def test_oracle_multiple_distance_metrics(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test multiple distance metrics in same query."""
    query = sql.select(
        "content",
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean").alias("euclidean_dist"),
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("cosine_dist"),
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean_squared").alias("euclidean_sq"),
    ).from_("vector_docs_oracle")

    result = await oracle_vector_session.execute(query)

    assert len(result) == 3
    for row in result:
        assert "euclidean_dist" in row
        assert "cosine_dist" in row
        assert "euclidean_sq" in row
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None
        assert row["euclidean_sq"] is not None


async def test_oracle_distance_with_null_vectors(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test vector distance handles NULL vectors correctly."""
    await oracle_vector_session.execute(
        "INSERT INTO vector_docs_oracle (content, embedding) VALUES (:1, NULL)", ("doc_null",)
    )

    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_oracle")
        .where(sql.column("embedding").is_not_null())
        .order_by("distance")
    )

    result = await oracle_vector_session.execute(query)

    assert len(result) == 3
    assert all(row["content"] != "doc_null" for row in result)


async def test_oracle_combined_filters_and_distance(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test combining distance threshold with other filters."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_oracle")
        .where((Column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0) & (Column("content").in_(["doc1", "doc2"])))
        .order_by("distance")
    )

    result = await oracle_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] in ["doc1", "doc2"]
    assert result[1]["content"] in ["doc1", "doc2"]


async def test_oracle_similarity_score_range(oracle_vector_session: OracleAsyncDriver) -> None:
    """Test cosine similarity returns values in expected range."""
    query = sql.select("content", Column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score")).from_(
        "vector_docs_oracle"
    )

    result = await oracle_vector_session.execute(query)

    for row in result:
        score = row["score"]
        assert (
            -1 <= score <= 1
            or math.isclose(score, 1.0, rel_tol=1e-9, abs_tol=1e-9)
            or math.isclose(score, -1.0, rel_tol=1e-9, abs_tol=1e-9)
        )
