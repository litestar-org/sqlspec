"""Integration tests for vector distance functions with Psqlpy + pgvector.

Tests actual execution of vector distance queries using PostgreSQL pgvector extension
with the Rust-based Psqlpy driver.
"""

from collections.abc import AsyncGenerator

import pytest

from sqlspec import sql
from sqlspec.adapters.psqlpy import PsqlpyDriver
from sqlspec.builder import Column
from sqlspec.typing import PGVECTOR_INSTALLED

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not PGVECTOR_INSTALLED, reason="pgvector not installed"),
]


@pytest.fixture
async def psqlpy_vector_session(psqlpy_driver: PsqlpyDriver) -> AsyncGenerator[PsqlpyDriver, None]:
    """Create psqlpy session with pgvector extension and test table."""
    try:
        await psqlpy_driver.execute_script("CREATE EXTENSION IF NOT EXISTS vector")
    except Exception:
        pytest.skip("pgvector extension unavailable")

    try:
        await psqlpy_driver.execute_script(
            """
            CREATE TABLE IF NOT EXISTS vector_docs_psqlpy (
                id SERIAL PRIMARY KEY,
                content TEXT NOT NULL,
                embedding vector(3)
            )
            """
        )

        await psqlpy_driver.execute_script("TRUNCATE TABLE vector_docs_psqlpy")

        await psqlpy_driver.execute(
            "INSERT INTO vector_docs_psqlpy (content, embedding) VALUES ($1, $2)", ("doc1", "[0.1, 0.2, 0.3]")
        )
        await psqlpy_driver.execute(
            "INSERT INTO vector_docs_psqlpy (content, embedding) VALUES ($1, $2)", ("doc2", "[0.4, 0.5, 0.6]")
        )
        await psqlpy_driver.execute(
            "INSERT INTO vector_docs_psqlpy (content, embedding) VALUES ($1, $2)", ("doc3", "[0.7, 0.8, 0.9]")
        )

        yield psqlpy_driver
    finally:
        await psqlpy_driver.execute_script("DROP TABLE IF EXISTS vector_docs_psqlpy")


async def test_psqlpy_euclidean_distance_execution(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test PostgreSQL euclidean distance operator execution with Psqlpy."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_psqlpy")
        .order_by("distance")
    )

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"
    assert result[2]["content"] == "doc3"

    assert result[0]["distance"] < result[1]["distance"]
    assert result[1]["distance"] < result[2]["distance"]


async def test_psqlpy_euclidean_distance_threshold(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test PostgreSQL euclidean distance with threshold filter."""
    query = (
        sql
        .select("content")
        .from_("vector_docs_psqlpy")
        .where(sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 0.3)
    )

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 1
    assert result[0]["content"] == "doc1"


async def test_psqlpy_cosine_distance_execution(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test PostgreSQL cosine distance operator execution."""
    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance"))
        .from_("vector_docs_psqlpy")
        .order_by("distance")
    )

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"


async def test_psqlpy_inner_product_execution(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test PostgreSQL inner product operator execution."""
    query = (
        sql
        .select(
            "content",
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="inner_product").alias("distance"),
        )
        .from_("vector_docs_psqlpy")
        .order_by("distance")
    )

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 3


async def test_psqlpy_cosine_similarity_execution(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test PostgreSQL cosine similarity calculation."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_psqlpy")
        .order_by(sql.column("score").desc())
    )

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"

    assert result[0]["score"] > result[1]["score"]
    assert result[1]["score"] > result[2]["score"]


async def test_psqlpy_similarity_top_k_results(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test top-K similarity search."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_psqlpy")
        .order_by(sql.column("score").desc())
        .limit(2)
    )

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"


async def test_psqlpy_multiple_distance_metrics(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test multiple distance metrics in same query."""
    query = sql.select(
        "content",
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean").alias("euclidean_dist"),
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("cosine_dist"),
    ).from_("vector_docs_psqlpy")

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 3
    for row in result:
        assert "euclidean_dist" in row
        assert "cosine_dist" in row
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None


async def test_psqlpy_distance_with_null_vectors(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test vector distance handles NULL vectors correctly."""
    await psqlpy_vector_session.execute(
        "INSERT INTO vector_docs_psqlpy (content, embedding) VALUES ($1, NULL)", ("doc_null",)
    )

    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_psqlpy")
        .where(sql.column("embedding").is_not_null())
        .order_by("distance")
    )

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 3
    assert all(row["content"] != "doc_null" for row in result)


async def test_psqlpy_combined_filters_and_distance(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test combining distance threshold with other filters."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_psqlpy")
        .where((Column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0) & (Column("content").in_(["doc1", "doc2"])))
        .order_by("distance")
    )

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] in ["doc1", "doc2"]
    assert result[1]["content"] in ["doc1", "doc2"]


async def test_psqlpy_similarity_score_range(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test cosine similarity returns values in expected range."""
    query = sql.select("content", Column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score")).from_(
        "vector_docs_psqlpy"
    )

    result = await psqlpy_vector_session.execute(query)

    for row in result:
        score = row["score"]
        assert -1 <= score <= 1


async def test_psqlpy_distance_with_cast(psqlpy_vector_session: PsqlpyDriver) -> None:
    """Test vector distance with explicit type casting."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).cast("FLOAT").alias("distance"))
        .from_("vector_docs_psqlpy")
        .order_by("distance")
    )

    result = await psqlpy_vector_session.execute(query)

    assert len(result) == 3
    assert isinstance(result[0]["distance"], float)
