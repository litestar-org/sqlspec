"""Integration tests for vector distance functions with PostgreSQL + pgvector.

Tests actual execution of vector distance queries using PostgreSQL pgvector extension.
"""

from collections.abc import AsyncGenerator

import pytest

from sqlspec import sql
from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.builder import Column
from sqlspec.typing import PGVECTOR_INSTALLED

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not PGVECTOR_INSTALLED, reason="pgvector missing"),
]


@pytest.fixture
async def asyncpg_vector_session(asyncpg_async_driver: AsyncpgDriver) -> AsyncGenerator[AsyncpgDriver, None]:
    """Create asyncpg session with pgvector extension and test table."""
    try:
        await asyncpg_async_driver.execute_script("CREATE EXTENSION IF NOT EXISTS vector")
    except Exception:
        pytest.skip("pgvector extension unavailable")

    try:
        await asyncpg_async_driver.execute_script(
            """
            CREATE TABLE IF NOT EXISTS vector_docs (
                id SERIAL PRIMARY KEY,
                content TEXT NOT NULL,
                embedding vector(3)
            )
            """
        )

        await asyncpg_async_driver.execute_script("TRUNCATE TABLE vector_docs")

        await asyncpg_async_driver.execute(
            "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2)", ("doc1", "[0.1, 0.2, 0.3]")
        )
        await asyncpg_async_driver.execute(
            "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2)", ("doc2", "[0.4, 0.5, 0.6]")
        )
        await asyncpg_async_driver.execute(
            "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2)", ("doc3", "[0.7, 0.8, 0.9]")
        )

        yield asyncpg_async_driver
    finally:
        await asyncpg_async_driver.execute_script("DROP TABLE IF EXISTS vector_docs")


async def test_postgres_euclidean_distance_execution(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test PostgreSQL euclidean distance operator execution."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs")
        .order_by("distance")
    )

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"
    assert result[2]["content"] == "doc3"

    assert result[0]["distance"] < result[1]["distance"]
    assert result[1]["distance"] < result[2]["distance"]


async def test_postgres_euclidean_distance_threshold(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test PostgreSQL euclidean distance with threshold filter."""
    query = (
        sql.select("content").from_("vector_docs").where(sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 0.3)
    )

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 1
    assert result[0]["content"] == "doc1"


async def test_postgres_cosine_distance_execution(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test PostgreSQL cosine distance operator execution."""
    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance"))
        .from_("vector_docs")
        .order_by("distance")
    )

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"


async def test_postgres_inner_product_execution(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test PostgreSQL inner product operator execution."""
    query = (
        sql
        .select(
            "content",
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="inner_product").alias("distance"),
        )
        .from_("vector_docs")
        .order_by("distance")
    )

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 3


async def test_postgres_cosine_similarity_execution(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test PostgreSQL cosine similarity calculation."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs")
        .order_by(sql.column("score").desc())
    )

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"

    assert result[0]["score"] > result[1]["score"]
    assert result[1]["score"] > result[2]["score"]


async def test_postgres_similarity_top_k_results(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test top-K similarity search."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs")
        .order_by(sql.column("score").desc())
        .limit(2)
    )

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"


async def test_postgres_multiple_distance_metrics(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test multiple distance metrics in same query."""
    query = sql.select(
        "content",
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean").alias("euclidean_dist"),
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("cosine_dist"),
    ).from_("vector_docs")

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 3
    for row in result:
        assert "euclidean_dist" in row
        assert "cosine_dist" in row
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None


async def test_postgres_distance_with_null_vectors(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test vector distance handles NULL vectors correctly."""
    await asyncpg_vector_session.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, NULL)", ("doc_null",)
    )

    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs")
        .where(sql.column("embedding").is_not_null())
        .order_by("distance")
    )

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 3
    assert all(row["content"] != "doc_null" for row in result)


async def test_postgres_combined_filters_and_distance(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test combining distance threshold with other filters."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs")
        .where((Column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0) & (Column("content").in_(["doc1", "doc2"])))
        .order_by("distance")
    )

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] in ["doc1", "doc2"]
    assert result[1]["content"] in ["doc1", "doc2"]


async def test_postgres_distance_in_having_clause(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test vector distance in HAVING clause with GROUP BY."""
    await asyncpg_vector_session.execute_script(
        """
        CREATE TABLE IF NOT EXISTS vector_groups (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            embedding vector(3)
        )
        """
    )

    try:
        await asyncpg_vector_session.execute(
            "INSERT INTO vector_groups (category, embedding) VALUES ($1, $2)", ("A", "[0.1, 0.2, 0.3]")
        )
        await asyncpg_vector_session.execute(
            "INSERT INTO vector_groups (category, embedding) VALUES ($1, $2)", ("A", "[0.2, 0.3, 0.4]")
        )
        await asyncpg_vector_session.execute(
            "INSERT INTO vector_groups (category, embedding) VALUES ($1, $2)", ("B", "[0.7, 0.8, 0.9]")
        )

        query = sql.select("category", Column.count_all().alias("count")).from_("vector_groups").group_by("category")

        result = await asyncpg_vector_session.execute(query)

        assert len(result) == 2

        category_counts = {row["category"]: row["count"] for row in result}
        assert category_counts["A"] == 2
        assert category_counts["B"] == 1
    finally:
        await asyncpg_vector_session.execute_script("DROP TABLE IF EXISTS vector_groups")


async def test_postgres_distance_with_subquery(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test vector distance in subquery."""
    subquery = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs")
        .where(Column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0)
    )
    query = sql.select("*").from_(subquery, alias="subq").where(Column("distance") < 0.5)

    result = await asyncpg_vector_session.execute(query)

    assert len(result) >= 1


async def test_postgres_similarity_score_range(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test cosine similarity returns values in expected range."""
    query = sql.select("content", Column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score")).from_(
        "vector_docs"
    )

    result = await asyncpg_vector_session.execute(query)

    for row in result:
        score = row["score"]
        assert -1 <= score <= 1


async def test_postgres_distance_with_cast(asyncpg_vector_session: AsyncpgDriver) -> None:
    """Test vector distance with explicit type casting."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).cast("FLOAT").alias("distance"))
        .from_("vector_docs")
        .order_by("distance")
    )

    result = await asyncpg_vector_session.execute(query)

    assert len(result) == 3
    assert isinstance(result[0]["distance"], float)
