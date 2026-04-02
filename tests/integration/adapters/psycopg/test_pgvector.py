"""Integration tests for psycopg driver with pgvector extension."""

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import sql
from sqlspec.adapters.psycopg import PsycopgPoolParams, PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.builder import Column

pytestmark = pytest.mark.xdist_group("pgvector")


@pytest.fixture(scope="session")
def pgvector_psycopg_connection_config(pgvector_service: "PostgresService") -> "PsycopgPoolParams":
    """Base pool configuration for Psycopg tests with pgvector."""
    return PsycopgPoolParams(
        conninfo=f"postgresql://{pgvector_service.user}:{pgvector_service.password}@{pgvector_service.host}:{pgvector_service.port}/{pgvector_service.database}"
    )


@pytest.fixture(scope="session")
def pgvector_psycopg_config(
    pgvector_psycopg_connection_config: "PsycopgPoolParams",
) -> "Generator[PsycopgSyncConfig, None, None]":
    """Provide a PsycopgSyncConfig instance connected to pgvector postgres."""
    # Enable the pgvector extension before creating the pool
    import psycopg

    with psycopg.connect(**pgvector_psycopg_connection_config) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        conn.commit()

    config = PsycopgSyncConfig(
        connection_config=PsycopgPoolParams(**pgvector_psycopg_connection_config), pool_config={"min_size": 1}
    )
    try:
        yield config
    finally:
        pool = config.connection_instance
        if pool is not None:
            config.close_pool()
            config.connection_instance = None


@pytest.fixture(scope="function")
def pgvector_psycopg_driver(pgvector_psycopg_config: "PsycopgSyncConfig") -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a Psycopg driver connected to pgvector postgres."""
    with pgvector_psycopg_config.provide_session() as session:
        yield session


@pytest.fixture(scope="function")
def pgvector_table(pgvector_psycopg_driver: "PsycopgSyncDriver") -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a test table with vector column for pgvector tests."""
    pgvector_psycopg_driver.execute_script("""
        DROP TABLE IF EXISTS vector_docs CASCADE;
        CREATE TABLE vector_docs (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            embedding vector(3)
        );
    """)

    # Insert test data
    pgvector_psycopg_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES (%s, %s)", "doc1", "[0.1, 0.2, 0.3]"
    )
    pgvector_psycopg_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES (%s, %s)", "doc2", "[0.4, 0.5, 0.6]"
    )
    pgvector_psycopg_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES (%s, %s)", "doc3", "[0.7, 0.8, 0.9]"
    )

    try:
        yield pgvector_psycopg_driver
    finally:
        pgvector_psycopg_driver.execute_script("DROP TABLE IF EXISTS vector_docs CASCADE")


# --- Extension Detection Tests ---


@pytest.mark.integration
def test_pgvector_extension_detected(pgvector_psycopg_config: "PsycopgSyncConfig") -> None:
    """Verify pgvector extension is detected and dialect is updated."""
    with pgvector_psycopg_config.provide_session() as session:
        session.execute("SELECT 1")

    assert pgvector_psycopg_config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
    # ParadeDB not available on pgvector-only image
    assert pgvector_psycopg_config._paradedb_available is False  # pyright: ignore[reportPrivateUsage]
    assert pgvector_psycopg_config.statement_config.dialect == "pgvector"


@pytest.mark.integration
def test_pgvector_first_session_uses_detected_dialect(pgvector_psycopg_config: "PsycopgSyncConfig") -> None:
    """The first session should use pgvector without a prior pool bootstrap call."""
    with pgvector_psycopg_config.provide_session() as session:
        assert session.statement_config.dialect == "pgvector"


# --- Raw SQL Tests ---


@pytest.mark.integration
def test_pgvector_euclidean_distance_raw_sql(pgvector_table: "PsycopgSyncDriver") -> None:
    """Test pgvector euclidean distance using raw SQL with <-> operator."""
    driver = pgvector_table

    result = driver.execute("""
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
def test_pgvector_cosine_distance_raw_sql(pgvector_table: "PsycopgSyncDriver") -> None:
    """Test pgvector cosine distance using raw SQL with <=> operator."""
    driver = pgvector_table

    result = driver.execute("""
        SELECT content, embedding <=> '[0.1, 0.2, 0.3]'::vector AS distance
        FROM vector_docs
        ORDER BY distance
    """)

    assert result.data is not None
    assert len(result.data) == 3
    assert result[0]["content"] == "doc1"


@pytest.mark.integration
def test_pgvector_inner_product_raw_sql(pgvector_table: "PsycopgSyncDriver") -> None:
    """Test pgvector inner product using raw SQL with <#> operator.

    Note: The <#> operator is the negative inner product in pgvector.
    We use direct cursor to bypass SQL parsing which may interfere with the operator.
    """
    driver = pgvector_table

    # First verify we have data
    check = driver.execute("SELECT count(*) as cnt FROM vector_docs")
    assert check.data is not None
    assert check[0]["cnt"] == 3

    # Test inner product using direct cursor to avoid parsing issues
    # The <#> operator may conflict with dialect parsing
    cursor = driver.connection.cursor()
    cursor.execute(
        "SELECT content, (embedding <#> %s::vector) AS neg_inner_product FROM vector_docs ORDER BY neg_inner_product",
        ("[0.1, 0.2, 0.3]",),
    )
    records = cursor.fetchall()

    assert len(records) == 3


# --- Query Builder Tests ---
# Note: Query builder generates ARRAY[...] which pgvector doesn't accept directly.
# These tests use Column-to-Column comparison which works correctly.


@pytest.mark.integration
def test_pgvector_column_comparison_builder(pgvector_table: "PsycopgSyncDriver") -> None:
    """Test pgvector distance between two vector columns using query builder."""
    driver = pgvector_table

    # Add another vector column for comparison
    driver.execute_script("""
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

    result = driver.execute(query)

    assert len(result) == 3
    assert result[0]["content"] == "doc1"  # Exact match with embedding2
    assert result[0]["distance"] < 0.01


@pytest.mark.integration
def test_pgvector_order_by_distance_raw(pgvector_table: "PsycopgSyncDriver") -> None:
    """Test ordering by vector distance with parametrized query."""
    driver = pgvector_table

    # Using parametrized query with explicit cast
    result = driver.execute(
        "SELECT content FROM vector_docs ORDER BY embedding <-> %s::vector LIMIT 2", "[0.1, 0.2, 0.3]"
    )

    assert result.data is not None
    assert len(result.data) == 2
    assert result[0]["content"] == "doc1"


@pytest.mark.integration
def test_pgvector_distance_threshold_raw(pgvector_table: "PsycopgSyncDriver") -> None:
    """Test filtering by distance threshold."""
    driver = pgvector_table

    result = driver.execute("""
        SELECT content
        FROM vector_docs
        WHERE embedding <-> '[0.1, 0.2, 0.3]'::vector < 0.3
    """)

    assert result.data is not None
    assert len(result.data) == 1
    assert result[0]["content"] == "doc1"


@pytest.mark.integration
def test_pgvector_multiple_metrics_raw(pgvector_table: "PsycopgSyncDriver") -> None:
    """Test multiple distance metrics in same query.

    Note: The <#> operator (inner product) conflicts with SQL parsing,
    so we use the direct cursor for this test.
    """
    driver = pgvector_table

    # Use direct cursor to avoid parsing issues with <#> operator
    cursor = driver.connection.cursor()
    cursor.execute(
        "SELECT content, "
        "embedding <-> %s::vector AS euclidean_dist, "
        "embedding <=> %s::vector AS cosine_dist, "
        "embedding <#> %s::vector AS neg_inner_product "
        "FROM vector_docs",
        ("[0.1, 0.2, 0.3]", "[0.1, 0.2, 0.3]", "[0.1, 0.2, 0.3]"),
    )
    records = cursor.fetchall()

    assert len(records) == 3
    for row in records:
        assert row[1] is not None  # euclidean_dist
        assert row[2] is not None  # cosine_dist
        assert row[3] is not None  # neg_inner_product
