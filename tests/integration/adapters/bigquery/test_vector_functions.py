"""Integration tests for vector distance functions with Google BigQuery.

Tests actual execution of vector distance queries using BigQuery's native
distance functions (EUCLIDEAN_DISTANCE, COSINE_DISTANCE, DOT_PRODUCT).
"""

import contextlib
from collections.abc import Generator

import pytest

from sqlspec import sql
from sqlspec.adapters.bigquery import BigQueryDriver
from sqlspec.builder import Column

pytestmark = [pytest.mark.xdist_group("bigquery")]


@pytest.fixture
def bigquery_vector_session(bigquery_session: BigQueryDriver) -> Generator[BigQueryDriver, None, None]:
    """Create BigQuery session with test table containing array columns."""
    table_id = "vector_docs_bigquery"

    try:
        try:
            bigquery_session.execute("SELECT EUCLIDEAN_DISTANCE([0.1, 0.2], [0.1, 0.2]) AS ok")
        except Exception:  # pragma: no cover - guard for emulator limitations
            pytest.skip("BigQuery vector functions unavailable")

        bigquery_session.execute_script(
            f"""
            CREATE OR REPLACE TABLE {table_id} (
                id INT64,
                content STRING,
                embedding ARRAY<FLOAT64>
            )
            """
        )

        bigquery_session.execute(f"INSERT INTO {table_id} (id, content, embedding) VALUES (1, 'doc1', [0.1, 0.2, 0.3])")
        bigquery_session.execute(f"INSERT INTO {table_id} (id, content, embedding) VALUES (2, 'doc2', [0.4, 0.5, 0.6])")
        bigquery_session.execute(f"INSERT INTO {table_id} (id, content, embedding) VALUES (3, 'doc3', [0.7, 0.8, 0.9])")

        yield bigquery_session
    finally:
        with contextlib.suppress(Exception):
            bigquery_session.execute_script(f"DROP TABLE IF EXISTS {table_id}")


def test_bigquery_euclidean_distance_execution(bigquery_vector_session: BigQueryDriver) -> None:
    """Test BigQuery EUCLIDEAN_DISTANCE function execution."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_bigquery")
        .order_by("distance")
    )

    result = bigquery_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"
    assert result[2]["content"] == "doc3"

    assert result[0]["distance"] < result[1]["distance"]
    assert result[1]["distance"] < result[2]["distance"]


def test_bigquery_euclidean_distance_threshold(bigquery_vector_session: BigQueryDriver) -> None:
    """Test BigQuery euclidean distance with threshold filter."""
    query = (
        sql
        .select("content")
        .from_("vector_docs_bigquery")
        .where(sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 0.3)
    )

    result = bigquery_vector_session.execute(query)

    assert len(result) == 1
    assert result[0]["content"] == "doc1"


def test_bigquery_cosine_distance_execution(bigquery_vector_session: BigQueryDriver) -> None:
    """Test BigQuery COSINE_DISTANCE function execution."""
    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance"))
        .from_("vector_docs_bigquery")
        .order_by("distance")
    )

    result = bigquery_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"


def test_bigquery_inner_product_execution(bigquery_vector_session: BigQueryDriver) -> None:
    """Test BigQuery DOT_PRODUCT function execution."""
    query = (
        sql
        .select(
            "content",
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="inner_product").alias("distance"),
        )
        .from_("vector_docs_bigquery")
        .order_by("distance")
    )

    result = bigquery_vector_session.execute(query)

    assert len(result) == 3


def test_bigquery_cosine_similarity_execution(bigquery_vector_session: BigQueryDriver) -> None:
    """Test BigQuery cosine similarity calculation."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_bigquery")
        .order_by(sql.column("score").desc())
    )

    result = bigquery_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"

    assert result[0]["score"] > result[1]["score"]
    assert result[1]["score"] > result[2]["score"]


def test_bigquery_similarity_top_k_results(bigquery_vector_session: BigQueryDriver) -> None:
    """Test top-K similarity search."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_bigquery")
        .order_by(sql.column("score").desc())
        .limit(2)
    )

    result = bigquery_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"


def test_bigquery_multiple_distance_metrics(bigquery_vector_session: BigQueryDriver) -> None:
    """Test multiple distance metrics in same query."""
    query = sql.select(
        "content",
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean").alias("euclidean_dist"),
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("cosine_dist"),
    ).from_("vector_docs_bigquery")

    result = bigquery_vector_session.execute(query)

    assert len(result) == 3
    for row in result:
        assert "euclidean_dist" in row
        assert "cosine_dist" in row
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None


def test_bigquery_distance_with_null_vectors(bigquery_vector_session: BigQueryDriver) -> None:
    """Test vector distance handles NULL vectors correctly."""
    bigquery_vector_session.execute(
        "INSERT INTO vector_docs_bigquery (id, content, embedding) VALUES (4, 'doc_null', NULL)"
    )

    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_bigquery")
        .where(sql.column("embedding").is_not_null())
        .order_by("distance")
    )

    result = bigquery_vector_session.execute(query)

    assert len(result) == 3
    assert all(row["content"] != "doc_null" for row in result)


def test_bigquery_combined_filters_and_distance(bigquery_vector_session: BigQueryDriver) -> None:
    """Test combining distance threshold with other filters."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_bigquery")
        .where((Column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0) & (Column("content").in_(["doc1", "doc2"])))
        .order_by("distance")
    )

    result = bigquery_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] in ["doc1", "doc2"]
    assert result[1]["content"] in ["doc1", "doc2"]


def test_bigquery_similarity_score_range(bigquery_vector_session: BigQueryDriver) -> None:
    """Test cosine similarity returns values in expected range."""
    query = sql.select("content", Column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score")).from_(
        "vector_docs_bigquery"
    )

    result = bigquery_vector_session.execute(query)

    for row in result:
        score = row["score"]
        assert -1 <= score <= 1
