"""Integration tests for psqlpy driver with pgvector extension."""

from collections.abc import AsyncGenerator

import pytest
from psqlpy import ConnectionPool
from psqlpy.extra_types import PgVector
from pytest_databases.docker.postgres import PostgresService

from sqlspec import sql
from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver, PsqlpyPoolParams
from sqlspec.builder import Column

pytestmark = pytest.mark.xdist_group("pgvector")


@pytest.fixture(scope="session")
def pgvector_psqlpy_connection_config(pgvector_service: "PostgresService") -> "PsqlpyPoolParams":
    """Base pool configuration for Psqlpy tests with pgvector."""
    return PsqlpyPoolParams(
        dsn=f"postgres://{pgvector_service.user}:{pgvector_service.password}@{pgvector_service.host}:{pgvector_service.port}/{pgvector_service.database}"
    )


@pytest.fixture(scope="function")
async def pgvector_psqlpy_config(
    pgvector_psqlpy_connection_config: "PsqlpyPoolParams",
) -> "AsyncGenerator[PsqlpyConfig, None]":
    """Provide a PsqlpyConfig instance connected to pgvector postgres."""
    # Enable the pgvector extension before creating the pool
    pool = ConnectionPool(**pgvector_psqlpy_connection_config)
    async with pool.acquire() as conn:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    pool.close()

    config = PsqlpyConfig(connection_config=PsqlpyPoolParams(**pgvector_psqlpy_connection_config))
    try:
        yield config
    finally:
        pool = config.connection_instance
        if pool is not None:
            pool.close()
            config.connection_instance = None


@pytest.fixture(scope="function")
async def pgvector_psqlpy_driver(pgvector_psqlpy_config: "PsqlpyConfig") -> "AsyncGenerator[PsqlpyDriver, None]":
    """Create a Psqlpy driver connected to pgvector postgres."""
    async with pgvector_psqlpy_config.provide_session() as session:
        yield session


@pytest.fixture(scope="function")
async def pgvector_table(pgvector_psqlpy_driver: "PsqlpyDriver") -> "AsyncGenerator[PsqlpyDriver, None]":
    """Create a test table with vector column for pgvector tests."""
    await pgvector_psqlpy_driver.execute_script("""
        DROP TABLE IF EXISTS vector_docs CASCADE;
        CREATE TABLE vector_docs (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            embedding vector(3)
        );
    """)

    # Insert test data using PgVector type for proper parameter handling
    await pgvector_psqlpy_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2)", "doc1", PgVector([0.1, 0.2, 0.3])
    )
    await pgvector_psqlpy_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2)", "doc2", PgVector([0.4, 0.5, 0.6])
    )
    await pgvector_psqlpy_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2)", "doc3", PgVector([0.7, 0.8, 0.9])
    )

    try:
        yield pgvector_psqlpy_driver
    finally:
        await pgvector_psqlpy_driver.execute_script("DROP TABLE IF EXISTS vector_docs CASCADE")


# --- Extension Detection Tests ---


@pytest.mark.integration
async def test_pgvector_extension_detected(pgvector_psqlpy_config: "PsqlpyConfig") -> None:
    """Verify pgvector extension is detected and dialect is updated."""
    async with pgvector_psqlpy_config.provide_session() as session:
        await session.execute("SELECT 1")

    assert pgvector_psqlpy_config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
    # ParadeDB not available on pgvector-only image
    assert pgvector_psqlpy_config._paradedb_available is False  # pyright: ignore[reportPrivateUsage]
    assert pgvector_psqlpy_config.statement_config.dialect == "pgvector"


@pytest.mark.integration
async def test_pgvector_first_session_uses_detected_dialect(pgvector_psqlpy_config: "PsqlpyConfig") -> None:
    """The first session should use pgvector without a prior pool bootstrap call."""
    async with pgvector_psqlpy_config.provide_session() as session:
        assert session.statement_config.dialect == "pgvector"


# --- Raw SQL Tests ---


@pytest.mark.integration
async def test_pgvector_euclidean_distance_raw_sql(pgvector_table: "PsqlpyDriver") -> None:
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
    assert result[0]["content"] == "doc1"
    assert result[0]["distance"] < 0.01


@pytest.mark.integration
async def test_pgvector_cosine_distance_raw_sql(pgvector_table: "PsqlpyDriver") -> None:
    """Test pgvector cosine distance using raw SQL with <=> operator."""
    driver = pgvector_table

    result = await driver.execute("""
        SELECT content, embedding <=> '[0.1, 0.2, 0.3]'::vector AS distance
        FROM vector_docs
        ORDER BY distance
    """)

    assert result.data is not None
    assert len(result.data) == 3
    assert result[0]["content"] == "doc1"


@pytest.mark.integration
async def test_pgvector_inner_product_raw_sql(pgvector_table: "PsqlpyDriver") -> None:
    """Test pgvector inner product using raw SQL with <#> operator.

    Note: The <#> operator is the negative inner product in pgvector.
    We use direct connection to bypass SQL parsing which may interfere with the operator.
    """
    driver = pgvector_table

    # First verify we have data
    check = await driver.execute("SELECT count(*) as cnt FROM vector_docs")
    assert check.data is not None
    assert check[0]["cnt"] == 3

    # Test inner product using direct connection to avoid parsing issues
    # The <#> operator may conflict with dialect parsing
    connection = driver.connection
    result = await connection.fetch(
        "SELECT content, (embedding <#> $1) AS neg_inner_product FROM vector_docs ORDER BY neg_inner_product",
        [PgVector([0.1, 0.2, 0.3])],
    )
    records = result.result()

    assert len(records) == 3


# --- Query Builder Tests ---
# Note: Query builder generates ARRAY[...] which pgvector doesn't accept directly.
# These tests use Column-to-Column comparison which works correctly.


@pytest.mark.integration
async def test_pgvector_column_comparison_builder(pgvector_table: "PsqlpyDriver") -> None:
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
async def test_pgvector_order_by_distance_raw(pgvector_table: "PsqlpyDriver") -> None:
    """Test ordering by vector distance with parametrized query."""
    driver = pgvector_table

    # Using PgVector type for proper parameter handling
    result = await driver.execute(
        "SELECT content FROM vector_docs ORDER BY embedding <-> $1 LIMIT 2", PgVector([0.1, 0.2, 0.3])
    )

    assert result.data is not None
    assert len(result.data) == 2
    assert result[0]["content"] == "doc1"


@pytest.mark.integration
async def test_pgvector_distance_threshold_raw(pgvector_table: "PsqlpyDriver") -> None:
    """Test filtering by distance threshold."""
    driver = pgvector_table

    result = await driver.execute("""
        SELECT content
        FROM vector_docs
        WHERE embedding <-> '[0.1, 0.2, 0.3]'::vector < 0.3
    """)

    assert result.data is not None
    assert len(result.data) == 1
    assert result[0]["content"] == "doc1"


@pytest.mark.integration
async def test_pgvector_multiple_metrics_raw(pgvector_table: "PsqlpyDriver") -> None:
    """Test multiple distance metrics in same query.

    Note: The <#> operator (inner product) conflicts with SQL parsing,
    so we use the direct connection for this test.
    """
    driver = pgvector_table

    # Use direct connection to avoid parsing issues with <#> operator
    connection = driver.connection
    result = await connection.fetch(
        "SELECT content, "
        "embedding <-> $1 AS euclidean_dist, "
        "embedding <=> $1 AS cosine_dist, "
        "embedding <#> $1 AS neg_inner_product "
        "FROM vector_docs",
        [PgVector([0.1, 0.2, 0.3])],
    )
    records = result.result()

    assert len(records) == 3
    for row in records:
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None
        assert row["neg_inner_product"] is not None
