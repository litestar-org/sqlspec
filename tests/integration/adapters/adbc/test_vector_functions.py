"""Integration tests for vector distance functions with ADBC drivers.

Tests actual execution of vector distance queries using ADBC with multiple backends:
- PostgreSQL with pgvector extension
- DuckDB with native array functions
"""

from collections.abc import Generator

import pytest

from sqlspec import sql
from sqlspec.adapters.adbc import AdbcDriver
from sqlspec.builder import Column
from sqlspec.typing import PGVECTOR_INSTALLED

# PostgreSQL ADBC tests
pytestmark_postgres = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not PGVECTOR_INSTALLED, reason="pgvector missing"),
]


@pytest.fixture
def adbc_postgres_vector_session(adbc_sync_driver: AdbcDriver) -> Generator[AdbcDriver, None, None]:
    """Create ADBC PostgreSQL session with pgvector extension and test table."""
    try:
        adbc_sync_driver.execute_script("CREATE EXTENSION IF NOT EXISTS vector")
    except Exception:
        pytest.skip("pgvector extension unavailable")

    try:
        adbc_sync_driver.execute_script(
            """
            CREATE TABLE IF NOT EXISTS vector_docs_adbc_pg (
                id SERIAL PRIMARY KEY,
                content TEXT NOT NULL,
                embedding vector(3)
            )
            """
        )

        adbc_sync_driver.execute_script("TRUNCATE TABLE vector_docs_adbc_pg")

        adbc_sync_driver.execute(
            "INSERT INTO vector_docs_adbc_pg (content, embedding) VALUES (?, ?)", ("doc1", "[0.1, 0.2, 0.3]")
        )
        adbc_sync_driver.execute(
            "INSERT INTO vector_docs_adbc_pg (content, embedding) VALUES (?, ?)", ("doc2", "[0.4, 0.5, 0.6]")
        )
        adbc_sync_driver.execute(
            "INSERT INTO vector_docs_adbc_pg (content, embedding) VALUES (?, ?)", ("doc3", "[0.7, 0.8, 0.9]")
        )

        yield adbc_sync_driver
    finally:
        adbc_sync_driver.execute_script("DROP TABLE IF EXISTS vector_docs_adbc_pg")


@pytest.mark.postgres
@pytest.mark.skipif(not PGVECTOR_INSTALLED, reason="pgvector missing")
def test_adbc_postgres_euclidean_distance_execution(adbc_postgres_vector_session: AdbcDriver) -> None:
    """Test ADBC PostgreSQL euclidean distance operator execution."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_adbc_pg")
        .order_by("distance")
    )

    result = adbc_postgres_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"
    assert result[2]["content"] == "doc3"

    assert result[0]["distance"] < result[1]["distance"]
    assert result[1]["distance"] < result[2]["distance"]


@pytest.mark.postgres
@pytest.mark.skipif(not PGVECTOR_INSTALLED, reason="pgvector missing")
def test_adbc_postgres_cosine_distance_execution(adbc_postgres_vector_session: AdbcDriver) -> None:
    """Test ADBC PostgreSQL cosine distance operator execution."""
    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance"))
        .from_("vector_docs_adbc_pg")
        .order_by("distance")
    )

    result = adbc_postgres_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"


@pytest.mark.postgres
@pytest.mark.skipif(not PGVECTOR_INSTALLED, reason="pgvector missing")
def test_adbc_postgres_cosine_similarity_execution(adbc_postgres_vector_session: AdbcDriver) -> None:
    """Test ADBC PostgreSQL cosine similarity calculation."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_adbc_pg")
        .order_by(sql.column("score").desc())
    )

    result = adbc_postgres_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"

    assert result[0]["score"] > result[1]["score"]
    assert result[1]["score"] > result[2]["score"]


# DuckDB ADBC tests
pytestmark_duckdb = [pytest.mark.xdist_group("duckdb")]


@pytest.fixture
def adbc_duckdb_vector_session(adbc_duckdb_driver: AdbcDriver) -> Generator[AdbcDriver, None, None]:
    """Create ADBC DuckDB session with VSS extension and test table."""
    try:
        # Install and load VSS extension for vector distance functions
        adbc_duckdb_driver.execute_script("INSTALL vss")
        adbc_duckdb_driver.execute_script("LOAD vss")
    except Exception as e:
        pytest.skip(f"DuckDB VSS extension missing: {e}")

    try:
        adbc_duckdb_driver.execute_script(
            """
            CREATE TABLE IF NOT EXISTS vector_docs_adbc_duckdb (
                id INTEGER PRIMARY KEY,
                content VARCHAR NOT NULL,
                embedding DOUBLE[3]
            )
            """
        )

        adbc_duckdb_driver.execute(
            "INSERT INTO vector_docs_adbc_duckdb (id, content, embedding) VALUES (?, ?, ?)",
            (1, "doc1", [0.1, 0.2, 0.3]),
        )
        adbc_duckdb_driver.execute(
            "INSERT INTO vector_docs_adbc_duckdb (id, content, embedding) VALUES (?, ?, ?)",
            (2, "doc2", [0.4, 0.5, 0.6]),
        )
        adbc_duckdb_driver.execute(
            "INSERT INTO vector_docs_adbc_duckdb (id, content, embedding) VALUES (?, ?, ?)",
            (3, "doc3", [0.7, 0.8, 0.9]),
        )

        yield adbc_duckdb_driver
    finally:
        adbc_duckdb_driver.execute_script("DROP TABLE IF EXISTS vector_docs_adbc_duckdb")


@pytest.mark.duckdb
def test_adbc_duckdb_euclidean_distance_execution(adbc_duckdb_vector_session: AdbcDriver) -> None:
    """Test ADBC DuckDB euclidean distance array function execution."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_adbc_duckdb")
        .order_by("distance")
    )

    result = adbc_duckdb_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"
    assert result[2]["content"] == "doc3"

    assert result[0]["distance"] < result[1]["distance"]
    assert result[1]["distance"] < result[2]["distance"]


@pytest.mark.duckdb
def test_adbc_duckdb_euclidean_distance_threshold(adbc_duckdb_vector_session: AdbcDriver) -> None:
    """Test ADBC DuckDB euclidean distance with threshold filter."""
    query = (
        sql
        .select("content")
        .from_("vector_docs_adbc_duckdb")
        .where(sql.column("embedding").vector_distance([0.1, 0.2, 0.3]) < 0.3)
    )

    result = adbc_duckdb_vector_session.execute(query)

    assert len(result) == 1
    assert result[0]["content"] == "doc1"


@pytest.mark.duckdb
def test_adbc_duckdb_cosine_distance_execution(adbc_duckdb_vector_session: AdbcDriver) -> None:
    """Test ADBC DuckDB cosine distance array function execution."""
    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("distance"))
        .from_("vector_docs_adbc_duckdb")
        .order_by("distance")
    )

    result = adbc_duckdb_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"


@pytest.mark.duckdb
def test_adbc_duckdb_inner_product_execution(adbc_duckdb_vector_session: AdbcDriver) -> None:
    """Test ADBC DuckDB inner product array function execution."""
    query = (
        sql
        .select(
            "content",
            sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="inner_product").alias("distance"),
        )
        .from_("vector_docs_adbc_duckdb")
        .order_by("distance")
    )

    result = adbc_duckdb_vector_session.execute(query)

    assert len(result) == 3


@pytest.mark.duckdb
def test_adbc_duckdb_cosine_similarity_execution(adbc_duckdb_vector_session: AdbcDriver) -> None:
    """Test ADBC DuckDB cosine similarity calculation."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_adbc_duckdb")
        .order_by(sql.column("score").desc())
    )

    result = adbc_duckdb_vector_session.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"

    assert result[0]["score"] > result[1]["score"]
    assert result[1]["score"] > result[2]["score"]


@pytest.mark.duckdb
def test_adbc_duckdb_similarity_top_k_results(adbc_duckdb_vector_session: AdbcDriver) -> None:
    """Test top-K similarity search with ADBC DuckDB."""
    query = (
        sql
        .select("content", sql.column("embedding").cosine_similarity([0.1, 0.2, 0.3]).alias("score"))
        .from_("vector_docs_adbc_duckdb")
        .order_by(sql.column("score").desc())
        .limit(2)
    )

    result = adbc_duckdb_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] == "doc1"
    assert result[1]["content"] == "doc2"


@pytest.mark.duckdb
def test_adbc_duckdb_multiple_distance_metrics(adbc_duckdb_vector_session: AdbcDriver) -> None:
    """Test multiple distance metrics in same query with ADBC DuckDB."""
    query = sql.select(
        "content",
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="euclidean").alias("euclidean_dist"),
        sql.column("embedding").vector_distance([0.1, 0.2, 0.3], metric="cosine").alias("cosine_dist"),
    ).from_("vector_docs_adbc_duckdb")

    result = adbc_duckdb_vector_session.execute(query)

    assert len(result) == 3
    for row in result:
        assert "euclidean_dist" in row
        assert "cosine_dist" in row
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None


@pytest.mark.duckdb
def test_adbc_duckdb_distance_with_null_vectors(adbc_duckdb_vector_session: AdbcDriver) -> None:
    """Test vector distance handles NULL vectors correctly with ADBC DuckDB."""
    adbc_duckdb_vector_session.execute(
        "INSERT INTO vector_docs_adbc_duckdb (id, content, embedding) VALUES (?, ?, ?)", (4, "doc_null", None)
    )

    query = (
        sql
        .select("content", sql.column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_adbc_duckdb")
        .where(sql.column("embedding").is_not_null())
        .order_by("distance")
    )

    result = adbc_duckdb_vector_session.execute(query)

    assert len(result) == 3
    assert all(row["content"] != "doc_null" for row in result)


@pytest.mark.duckdb
def test_adbc_duckdb_combined_filters_and_distance(adbc_duckdb_vector_session: AdbcDriver) -> None:
    """Test combining distance threshold with other filters."""
    query = (
        sql
        .select("content", Column("embedding").vector_distance([0.1, 0.2, 0.3]).alias("distance"))
        .from_("vector_docs_adbc_duckdb")
        .where((Column("embedding").vector_distance([0.1, 0.2, 0.3]) < 1.0) & (Column("content").in_(["doc1", "doc2"])))
        .order_by("distance")
    )

    result = adbc_duckdb_vector_session.execute(query)

    assert len(result) == 2
    assert result[0]["content"] in ["doc1", "doc2"]
    assert result[1]["content"] in ["doc1", "doc2"]
