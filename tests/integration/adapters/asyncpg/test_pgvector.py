"""Integration tests for asyncpg driver with pgvector extension."""

from collections.abc import AsyncGenerator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import sql
from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.builder import Column

pytestmark = pytest.mark.xdist_group("pgvector")


@pytest.fixture(scope="session")
def pgvector_asyncpg_connection_config(pgvector_service: "PostgresService") -> "dict[str, Any]":
    """Base pool configuration for AsyncPG tests with pgvector."""
    return {
        "host": pgvector_service.host,
        "port": pgvector_service.port,
        "user": pgvector_service.user,
        "password": pgvector_service.password,
        "database": pgvector_service.database,
    }


@pytest.fixture(scope="function")
async def pgvector_asyncpg_config(
    pgvector_asyncpg_connection_config: "dict[str, Any]",
) -> "AsyncGenerator[AsyncpgConfig, None]":
    """Provide an AsyncpgConfig instance connected to pgvector postgres."""
    # Enable the pgvector extension before creating the pool
    import asyncpg

    conn = await asyncpg.connect(**pgvector_asyncpg_connection_config)
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    finally:
        await conn.close()

    config = AsyncpgConfig(connection_config=dict(pgvector_asyncpg_connection_config))
    try:
        yield config
    finally:
        pool = config.connection_instance
        if pool is not None:
            await pool.close()
            config.connection_instance = None


@pytest.fixture(scope="function")
async def pgvector_asyncpg_driver(pgvector_asyncpg_config: "AsyncpgConfig") -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG driver connected to pgvector postgres."""
    async with pgvector_asyncpg_config.provide_session() as session:
        yield session


@pytest.fixture(scope="function")
async def pgvector_table(pgvector_asyncpg_driver: "AsyncpgDriver") -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create a test table with vector column for pgvector tests."""
    await pgvector_asyncpg_driver.execute_script("""
        DROP TABLE IF EXISTS vector_docs CASCADE;
        CREATE TABLE vector_docs (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            embedding vector(3)
        );
    """)

    # Insert test data — use Python lists so asyncpg's pgvector codec encodes them correctly
    await pgvector_asyncpg_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2)", ("doc1", [0.1, 0.2, 0.3])
    )
    await pgvector_asyncpg_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2)", ("doc2", [0.4, 0.5, 0.6])
    )
    await pgvector_asyncpg_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2)", ("doc3", [0.7, 0.8, 0.9])
    )

    try:
        yield pgvector_asyncpg_driver
    finally:
        await pgvector_asyncpg_driver.execute_script("DROP TABLE IF EXISTS vector_docs CASCADE")


# --- Extension Detection Tests ---


@pytest.mark.integration
async def test_pgvector_extension_detected(pgvector_asyncpg_config: "AsyncpgConfig") -> None:
    """Verify pgvector extension is detected and dialect is updated."""
    async with pgvector_asyncpg_config.provide_session() as session:
        await session.execute("SELECT 1")

    assert pgvector_asyncpg_config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
    # ParadeDB not available on pgvector-only image
    assert pgvector_asyncpg_config._paradedb_available is False  # pyright: ignore[reportPrivateUsage]
    assert pgvector_asyncpg_config.statement_config.dialect == "pgvector"


@pytest.mark.integration
async def test_pgvector_first_session_uses_detected_dialect(pgvector_asyncpg_config: "AsyncpgConfig") -> None:
    """The first session should use pgvector without a prior pool bootstrap call."""
    async with pgvector_asyncpg_config.provide_session() as session:
        assert session.statement_config.dialect == "pgvector"


# --- Raw SQL Tests ---


@pytest.mark.integration
async def test_pgvector_euclidean_distance_raw_sql(pgvector_table: "AsyncpgDriver") -> None:
    """Test pgvector euclidean distance using raw SQL with <-> operator."""
    driver = pgvector_table

    result = await driver.execute("""
        SELECT content, embedding <-> '[0.1, 0.2, 0.3]'::vector AS distance
        FROM vector_docs
        ORDER BY distance
    """)

    assert result.data is not None
    assert len(result.data) == 3
    # doc1 should be closest (exact match)
    assert result.data[0]["content"] == "doc1"
    assert result.data[0]["distance"] < 0.01


@pytest.mark.integration
async def test_pgvector_cosine_distance_raw_sql(pgvector_table: "AsyncpgDriver") -> None:
    """Test pgvector cosine distance using raw SQL with <=> operator."""
    driver = pgvector_table

    result = await driver.execute("""
        SELECT content, embedding <=> '[0.1, 0.2, 0.3]'::vector AS distance
        FROM vector_docs
        ORDER BY distance
    """)

    assert result.data is not None
    assert len(result.data) == 3
    assert result.data[0]["content"] == "doc1"


@pytest.mark.integration
async def test_pgvector_inner_product_raw_sql(pgvector_table: "AsyncpgDriver") -> None:
    """Test pgvector inner product using raw SQL with <#> operator.

    Note: The <#> operator is the negative inner product in pgvector.
    We use execute_script to bypass SQL parsing which may interfere with the operator.
    """
    driver = pgvector_table

    # First verify we have data
    check = await driver.execute("SELECT count(*) as cnt FROM vector_docs")
    assert check.data is not None
    assert check.data[0]["cnt"] == 3

    # Test inner product using direct connection to avoid parsing issues
    # The <#> operator may conflict with dialect parsing
    connection = driver.connection
    records = await connection.fetch(
        "SELECT content, (embedding <#> $1::vector) AS neg_inner_product FROM vector_docs ORDER BY neg_inner_product",
        [0.1, 0.2, 0.3],
    )

    assert len(records) == 3


# --- Query Builder Tests ---
# Note: Query builder generates ARRAY[...] which pgvector doesn't accept directly.
# These tests use Column-to-Column comparison which works correctly.


@pytest.mark.integration
async def test_pgvector_column_comparison_builder(pgvector_table: "AsyncpgDriver") -> None:
    """Test pgvector distance between two vector columns using query builder."""
    driver = pgvector_table

    # Add another vector column for comparison
    await driver.execute_script("""
        ALTER TABLE vector_docs ADD COLUMN IF NOT EXISTS embedding2 vector(3);
        UPDATE vector_docs SET embedding2 = '[0.1, 0.2, 0.3]'::vector;
    """)

    # Query builder works when comparing two columns (not array literals)
    query = (
        sql
        .select("content", Column("embedding").vector_distance(Column("embedding2")).alias("distance"))
        .from_("vector_docs")
        .order_by("distance")
    )

    result = await driver.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"  # Exact match with embedding2
    assert result[0]["distance"] < 0.01


@pytest.mark.integration
async def test_pgvector_order_by_distance_raw(pgvector_table: "AsyncpgDriver") -> None:
    """Test ordering by vector distance with parametrized query."""
    driver = pgvector_table

    # Using parametrized query with explicit cast
    result = await driver.execute(
        "SELECT content FROM vector_docs ORDER BY embedding <-> $1::vector LIMIT 2", ([0.1, 0.2, 0.3],)
    )

    assert result.data is not None
    assert len(result.data) == 2
    assert result.data[0]["content"] == "doc1"


@pytest.mark.integration
async def test_pgvector_distance_threshold_raw(pgvector_table: "AsyncpgDriver") -> None:
    """Test filtering by distance threshold."""
    driver = pgvector_table

    result = await driver.execute("""
        SELECT content
        FROM vector_docs
        WHERE embedding <-> '[0.1, 0.2, 0.3]'::vector < 0.3
    """)

    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["content"] == "doc1"


@pytest.mark.integration
async def test_pgvector_multiple_metrics_raw(pgvector_table: "AsyncpgDriver") -> None:
    """Test multiple distance metrics in same query.

    Note: The <#> operator (inner product) conflicts with SQL parsing,
    so we use the direct connection for this test.
    """
    driver = pgvector_table

    # Use direct connection to avoid parsing issues with <#> operator
    connection = driver.connection
    records = await connection.fetch(
        "SELECT content, "
        "embedding <-> $1::vector AS euclidean_dist, "
        "embedding <=> $1::vector AS cosine_dist, "
        "embedding <#> $1::vector AS neg_inner_product "
        "FROM vector_docs",
        [0.1, 0.2, 0.3],
    )

    assert len(records) == 3
    for row in records:
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None
        assert row["neg_inner_product"] is not None
