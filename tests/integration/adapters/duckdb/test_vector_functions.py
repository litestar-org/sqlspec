"""Integration tests for vector distance functions with DuckDB.

Tests actual execution of vector distance queries using DuckDB array functions.
"""

from collections.abc import Generator

import pytest

from sqlspec import sql
from sqlspec.adapters.duckdb import DuckDBDriver
from sqlspec.builder import Column

pytestmark = pytest.mark.xdist_group("duckdb")


@pytest.fixture
def duckdb_vector_session(duckdb_basic_session: DuckDBDriver) -> Generator[DuckDBDriver, None, None]:
    """Create DuckDB session with VSS extension and vector test data."""
    try:
        # Install and load VSS extension for vector distance functions
        duckdb_basic_session.execute_script("INSTALL vss")
        duckdb_basic_session.execute_script("LOAD vss")
    except Exception:
        pytest.skip("DuckDB VSS unavailable")

    try:
        duckdb_basic_session.execute_script(
            """
            CREATE TABLE IF NOT EXISTS vector_docs (
                id INTEGER PRIMARY KEY,
                content TEXT NOT NULL,
                embedding DOUBLE[3]
            )
            """
        )

        duckdb_basic_session.execute_script("DELETE FROM vector_docs")

        duckdb_basic_session.execute(
            "INSERT INTO vector_docs (id, content, embedding) VALUES (?, ?, ?)", (1, "doc1", [0.1, 0.2, 0.3])
        )
        duckdb_basic_session.execute(
            "INSERT INTO vector_docs (id, content, embedding) VALUES (?, ?, ?)", (2, "doc2", [0.4, 0.5, 0.6])
        )
        duckdb_basic_session.execute(
            "INSERT INTO vector_docs (id, content, embedding) VALUES (?, ?, ?)", (3, "doc3", [0.7, 0.8, 0.9])
        )

        yield duckdb_basic_session
    finally:
        duckdb_basic_session.execute_script("DROP TABLE IF EXISTS vector_docs")


def test_duckdb_euclidean_distance_execution(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB euclidean distance using array functions."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs")
        .order_by("distance")
    )

    result = duckdb_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"
    assert result[2]["content"] == "doc3"

    assert result[0]["distance"] < result[1]["distance"]
    assert result[1]["distance"] < result[2]["distance"]


def test_duckdb_euclidean_distance_threshold(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB euclidean distance with threshold filter."""
    query = sql.select("content").from_("vector_docs").where(Column("embedding").vector_distance([0.1, 0.2, 0.3]) < 0.1)

    result = duckdb_vector_session.execute(query)

    assert len(result) == 1
    assert result[0]["content"] == "doc1"


def test_duckdb_cosine_distance_execution(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB cosine distance using array functions."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance"))
        .from_("vector_docs")
        .order_by("distance")
    )

    result = duckdb_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"


def test_duckdb_inner_product_execution(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB inner product using negative dot product."""
    query = (
        sql
        .select(
            "content", Column("embedding").vector_distance([0.1, 0.2, 0.3], metric="inner_product").alias("distance")
        )
        .from_("vector_docs")
        .order_by("distance")
    )

    result = duckdb_vector_session.execute(query)

    assert len(result) == 3


def test_duckdb_cosine_similarity_execution(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB cosine similarity calculation."""
    query = (
        sql
        .select("content", Column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs")
        .order_by(Column("score").desc())
    )

    result = duckdb_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"

    assert result[0]["score"] > result[1]["score"]
    assert result[1]["score"] > result[2]["score"]


def test_duckdb_similarity_top_k_results(duckdb_vector_session: DuckDBDriver) -> None:
    """Test top-K similarity search with DuckDB."""
    query = (
        sql
        .select("content", Column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs")
        .order_by(Column("score").desc())
        .limit(2)
    )

    result = duckdb_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"


def test_duckdb_multiple_distance_metrics(duckdb_vector_session: DuckDBDriver) -> None:
    """Test multiple distance metrics in same DuckDB query."""
    query = sql.select(
        "content",
        Column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean").alias("euclidean_dist"),
        Column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("cosine_dist"),
    ).from_("vector_docs")

    result = duckdb_vector_session.execute(query)

    assert len(result) == 3
    for row in result:
        assert "euclidean_dist" in row
        assert "cosine_dist" in row
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None


def test_duckdb_distance_with_null_vectors(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB vector distance handles NULL vectors correctly."""
    duckdb_vector_session.execute(
        "INSERT INTO vector_docs (id, content, embedding) VALUES (?, ?, ?)", (4, "doc_null", None)
    )

    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs")
        .where(Column("embedding").is_not_null())
        .order_by("distance")
    )

    result = duckdb_vector_session.execute(query)

    assert len(result) == 3
    assert all(row["content"] != "doc_null" for row in result)


def test_duckdb_combined_filters_and_distance(duckdb_vector_session: DuckDBDriver) -> None:
    """Test combining distance threshold with other filters in DuckDB."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs")
        .where((Column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0) & (Column("content").in_(["doc1", "doc2"])))
        .order_by("distance")
    )

    result = duckdb_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] in ["doc1", "doc2"]
    assert result[1]["content"] in ["doc1", "doc2"]


def test_duckdb_similarity_score_range(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB cosine similarity returns values in expected range."""
    query = sql.select("content", Column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score")).from_(
        "vector_docs"
    )

    result = duckdb_vector_session.execute(query)

    for row in result:
        score = row["score"]
        assert -1 <= score <= 1


def test_duckdb_distance_zero_vector(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB distance calculation with zero vector."""
    duckdb_vector_session.execute(
        "INSERT INTO vector_docs (id, content, embedding) VALUES (?, ?, ?)", (5, "zero_vec", [0.0, 0.0, 0.0])
    )

    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.0, 0.0, 0.0]).alias("distance"))
        .from_("vector_docs")
        .order_by("distance")
    )

    result = duckdb_vector_session.execute(query)

    assert len(result) == 4
    assert result[0]["content"] == "zero_vec"
    assert result[0]["distance"] == 0.0


def test_duckdb_large_vectors(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB distance with larger dimensional vectors."""
    duckdb_vector_session.execute_script(
        """
        CREATE TABLE IF NOT EXISTS large_vectors (
            id INTEGER PRIMARY KEY,
            content TEXT NOT NULL,
            embedding DOUBLE[10]
        )
        """
    )

    try:
        duckdb_vector_session.execute(
            "INSERT INTO large_vectors (id, content, embedding) VALUES (?, ?, ?)", (1, "large1", [0.1] * 10)
        )
        duckdb_vector_session.execute(
            "INSERT INTO large_vectors (id, content, embedding) VALUES (?, ?, ?)", (2, "large2", [0.5] * 10)
        )

        query = (
            sql
            .select("content", Column("embedding").vector_distance([0.1] * 10).alias("distance"))
            .from_("large_vectors")
            .order_by("distance")
        )

        result = duckdb_vector_session.execute(query)

        assert len(result) == 2
        assert result[0]["content"] == "large1"
        assert result[0]["distance"] < result[1]["distance"]
    finally:
        duckdb_vector_session.execute_script("DROP TABLE IF EXISTS large_vectors")


def test_duckdb_distance_with_aggregation(duckdb_vector_session: DuckDBDriver) -> None:
    """Test DuckDB vector distance with aggregation functions."""
    subquery = sql.select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance")).from_(
        "vector_docs"
    )
    query = sql.select("MIN(distance) AS min_distance", "MAX(distance) AS max_distance").from_(
        subquery, alias="distances"
    )

    result = duckdb_vector_session.execute(query)

    assert len(result) == 1
    assert "min_distance" in result[0]
    assert "max_distance" in result[0]
    assert result[0]["min_distance"] < result[0]["max_distance"]
