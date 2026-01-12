"""Integration tests for vector distance functions with Psycopg + pgvector.

Tests actual execution of vector distance queries using PostgreSQL pgvector extension
with the synchronous Psycopg driver.
"""

from collections.abc import Generator

import pytest

from sqlspec import sql
from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.builder import Column
from sqlspec.typing import PGVECTOR_INSTALLED

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not PGVECTOR_INSTALLED, reason="pgvector not installed"),
]


@pytest.fixture
def psycopg_vector_session(psycopg_sync_config: PsycopgSyncConfig) -> Generator[PsycopgSyncDriver, None, None]:
    """Create psycopg session with pgvector extension and test table."""
    with psycopg_sync_config.provide_session() as session:
        try:
            session.execute_script("CREATE EXTENSION IF NOT EXISTS vector")
        except Exception:
            pytest.skip("pgvector extension unavailable")

        try:
            session.execute_script(
                """
                CREATE TABLE IF NOT EXISTS vector_docs_psycopg (
                    id SERIAL PRIMARY KEY,
                    content TEXT NOT NULL,
                    embedding vector(3)
                )
                """
            )

            session.execute_script("TRUNCATE TABLE vector_docs_psycopg")

            session.execute(
                "INSERT INTO vector_docs_psycopg (content, embedding) VALUES (%s, %s)", ("doc1", "[0.1, 0.2, 0.3]")
            )
            session.execute(
                "INSERT INTO vector_docs_psycopg (content, embedding) VALUES (%s, %s)", ("doc2", "[0.4, 0.5, 0.6]")
            )
            session.execute(
                "INSERT INTO vector_docs_psycopg (content, embedding) VALUES (%s, %s)", ("doc3", "[0.7, 0.8, 0.9]")
            )

            yield session
        finally:
            session.execute_script("DROP TABLE IF EXISTS vector_docs_psycopg")


def test_psycopg_euclidean_distance_execution(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL euclidean distance operator execution with Psycopg."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_psycopg")
        .order_by("distance")
    )

    result = psycopg_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"
    assert result[2]["content"] == "doc3"

    assert result[0]["distance"] < result[1]["distance"]
    assert result[1]["distance"] < result[2]["distance"]


def test_psycopg_euclidean_distance_threshold(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL euclidean distance with threshold filter."""
    query = (
        sql
        .select("content")
        .from_("vector_docs_psycopg")
        .where(sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 0.3)
    )

    result = psycopg_vector_session.execute(query)

    assert len(result) == 1
    assert result[0]["content"] == "doc1"


def test_psycopg_cosine_distance_execution(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL cosine distance operator execution."""
    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance"))
        .from_("vector_docs_psycopg")
        .order_by("distance")
    )

    result = psycopg_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"


def test_psycopg_inner_product_execution(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL inner product operator execution."""
    query = (
        sql
        .select(
            "content",
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="inner_product").alias("distance"),
        )
        .from_("vector_docs_psycopg")
        .order_by("distance")
    )

    result = psycopg_vector_session.execute(query)

    assert len(result) == 3


def test_psycopg_cosine_similarity_execution(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL cosine similarity calculation."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_psycopg")
        .order_by(sql.column("score").desc())
    )

    result = psycopg_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"

    assert result[0]["score"] > result[1]["score"]
    assert result[1]["score"] > result[2]["score"]


def test_psycopg_similarity_top_k_results(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test top-K similarity search."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_psycopg")
        .order_by(sql.column("score").desc())
        .limit(2)
    )

    result = psycopg_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"


def test_psycopg_multiple_distance_metrics(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test multiple distance metrics in same query."""
    query = sql.select(
        "content",
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean").alias("euclidean_dist"),
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("cosine_dist"),
    ).from_("vector_docs_psycopg")

    result = psycopg_vector_session.execute(query)

    assert len(result) == 3
    for row in result:
        assert "euclidean_dist" in row
        assert "cosine_dist" in row
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None


def test_psycopg_distance_with_null_vectors(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test vector distance handles NULL vectors correctly."""
    psycopg_vector_session.execute(
        "INSERT INTO vector_docs_psycopg (content, embedding) VALUES (%s, NULL)", ("doc_null",)
    )

    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_psycopg")
        .where(sql.column("embedding").is_not_null())
        .order_by("distance")
    )

    result = psycopg_vector_session.execute(query)

    assert len(result) == 3
    assert all(row["content"] != "doc_null" for row in result)


def test_psycopg_combined_filters_and_distance(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test combining distance threshold with other filters."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_psycopg")
        .where((Column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0) & (Column("content").in_(["doc1", "doc2"])))
        .order_by("distance")
    )

    result = psycopg_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] in ["doc1", "doc2"]
    assert result[1]["content"] in ["doc1", "doc2"]


def test_psycopg_similarity_score_range(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test cosine similarity returns values in expected range."""
    query = sql.select("content", Column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score")).from_(
        "vector_docs_psycopg"
    )

    result = psycopg_vector_session.execute(query)

    for row in result:
        score = row["score"]
        assert -1 <= score <= 1


def test_psycopg_distance_with_cast(psycopg_vector_session: PsycopgSyncDriver) -> None:
    """Test vector distance with explicit type casting."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).cast("FLOAT").alias("distance"))
        .from_("vector_docs_psycopg")
        .order_by("distance")
    )

    result = psycopg_vector_session.execute(query)

    assert len(result) == 3
    assert isinstance(result[0]["distance"], float)
