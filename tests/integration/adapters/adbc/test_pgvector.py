"""Integration tests for ADBC driver with pgvector extension."""

from collections.abc import Generator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec import sql
from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.builder import Column

pytestmark = pytest.mark.xdist_group("pgvector")


@pytest.fixture(scope="session")
def pgvector_adbc_connection_config(pgvector_service: "PostgresService") -> "dict[str, Any]":
    """ADBC connection config pointing at the pgvector Docker service."""
    return {
        "uri": (
            f"postgresql://{pgvector_service.user}:{pgvector_service.password}"
            f"@{pgvector_service.host}:{pgvector_service.port}/{pgvector_service.database}"
        )
    }


@pytest.fixture(scope="session")
def _ensure_pgvector_extension(pgvector_adbc_connection_config: "dict[str, Any]") -> None:
    """Ensure the pgvector extension exists before any config creates a detection connection."""
    from sqlspec.adapters.adbc.core import build_connection_config, resolve_driver_connect_func

    conn = resolve_driver_connect_func(None, pgvector_adbc_connection_config["uri"])(
        **build_connection_config(pgvector_adbc_connection_config)
    )
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
        finally:
            cursor.close()
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(scope="function")
def pgvector_adbc_config(
    pgvector_adbc_connection_config: "dict[str, Any]", _ensure_pgvector_extension: None
) -> "Generator[AdbcConfig, None, None]":
    """Provide an AdbcConfig instance connected to pgvector postgres."""
    config = AdbcConfig(connection_config=dict(pgvector_adbc_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def pgvector_adbc_driver(pgvector_adbc_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC driver connected to pgvector postgres."""
    with pgvector_adbc_config.provide_session() as session:
        yield session


@pytest.fixture(scope="function")
def pgvector_table(pgvector_adbc_driver: "AdbcDriver") -> "Generator[AdbcDriver, None, None]":
    """Create a test table with vector column for pgvector tests."""
    pgvector_adbc_driver.execute_script("""
        DROP TABLE IF EXISTS vector_docs CASCADE;
        CREATE TABLE vector_docs (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            embedding vector(3)
        );
    """)

    # Insert test data — explicit ::vector cast required for ADBC text params
    pgvector_adbc_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2::vector)", ("doc1", "[0.1, 0.2, 0.3]")
    )
    pgvector_adbc_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2::vector)", ("doc2", "[0.4, 0.5, 0.6]")
    )
    pgvector_adbc_driver.execute(
        "INSERT INTO vector_docs (content, embedding) VALUES ($1, $2::vector)", ("doc3", "[0.7, 0.8, 0.9]")
    )

    try:
        yield pgvector_adbc_driver
    finally:
        pgvector_adbc_driver.execute_script("DROP TABLE IF EXISTS vector_docs CASCADE")


# --- Extension Detection Tests ---


@pytest.mark.integration
def test_pgvector_extension_detected(pgvector_adbc_config: "AdbcConfig") -> None:
    """Verify pgvector extension is detected and dialect is updated."""
    with pgvector_adbc_config.provide_session() as session:
        session.execute("SELECT 1")

    assert pgvector_adbc_config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
    # ParadeDB not available on pgvector-only image
    assert pgvector_adbc_config._paradedb_available is False  # pyright: ignore[reportPrivateUsage]
    assert pgvector_adbc_config.statement_config.dialect == "pgvector"


@pytest.mark.integration
def test_pgvector_first_session_uses_detected_dialect(pgvector_adbc_config: "AdbcConfig") -> None:
    """The first session should use pgvector dialect after detection."""
    with pgvector_adbc_config.provide_session() as session:
        assert session.statement_config.dialect == "pgvector"


# --- Raw SQL Tests ---


@pytest.mark.integration
def test_pgvector_euclidean_distance_raw_sql(pgvector_table: "AdbcDriver") -> None:
    """Test pgvector euclidean distance using raw SQL with <-> operator."""
    driver = pgvector_table

    result = driver.execute("""
        SELECT content, embedding <-> '[0.1, 0.2, 0.3]'::vector AS distance
        FROM vector_docs
        ORDER BY distance
    """)
    rows = result.get_data()

    assert len(rows) == 3
    assert rows[0]["content"] == "doc1"
    assert rows[0]["distance"] < 0.01


@pytest.mark.integration
def test_pgvector_cosine_distance_raw_sql(pgvector_table: "AdbcDriver") -> None:
    """Test pgvector cosine distance using raw SQL with <=> operator."""
    driver = pgvector_table

    result = driver.execute("""
        SELECT content, embedding <=> '[0.1, 0.2, 0.3]'::vector AS distance
        FROM vector_docs
        ORDER BY distance
    """)
    rows = result.get_data()

    assert len(rows) == 3
    assert rows[0]["content"] == "doc1"


@pytest.mark.integration
def test_pgvector_inner_product_raw_sql(pgvector_table: "AdbcDriver") -> None:
    """Test pgvector negative inner product using raw SQL with <#> operator."""
    driver = pgvector_table

    result = driver.execute("""
        SELECT content, embedding <#> '[0.1, 0.2, 0.3]'::vector AS neg_inner_product
        FROM vector_docs
        ORDER BY neg_inner_product
    """)
    rows = result.get_data()

    assert len(rows) == 3


# --- Query Builder Tests ---


@pytest.mark.integration
def test_pgvector_column_comparison_builder(pgvector_table: "AdbcDriver") -> None:
    """Test pgvector distance between two vector columns using query builder."""
    driver = pgvector_table

    driver.execute_script("""
        ALTER TABLE vector_docs ADD COLUMN IF NOT EXISTS embedding2 vector(3);
        UPDATE vector_docs SET embedding2 = '[0.1, 0.2, 0.3]'::vector;
    """)

    query = (
        sql
        .select("content", Column("embedding").vector_distance(Column("embedding2")).alias("distance"))
        .from_("vector_docs")
        .order_by("distance")
    )

    result = driver.execute(query)
    rows = result.get_data()

    assert len(rows) == 3
    assert rows[0]["content"] == "doc1"
    assert rows[0]["distance"] < 0.01


@pytest.mark.integration
def test_pgvector_order_by_distance_raw(pgvector_table: "AdbcDriver") -> None:
    """Test ordering by vector distance with LIMIT."""
    driver = pgvector_table

    result = driver.execute("""
        SELECT content FROM vector_docs
        ORDER BY embedding <-> '[0.1, 0.2, 0.3]'::vector
        LIMIT 2
    """)
    rows = result.get_data()

    assert len(rows) == 2
    assert rows[0]["content"] == "doc1"


@pytest.mark.integration
def test_pgvector_distance_threshold_raw(pgvector_table: "AdbcDriver") -> None:
    """Test filtering by distance threshold."""
    driver = pgvector_table

    result = driver.execute("""
        SELECT content
        FROM vector_docs
        WHERE embedding <-> '[0.1, 0.2, 0.3]'::vector < 0.3
    """)
    rows = result.get_data()

    assert len(rows) == 1
    assert rows[0]["content"] == "doc1"


@pytest.mark.integration
def test_pgvector_multiple_metrics_raw(pgvector_table: "AdbcDriver") -> None:
    """Test multiple distance metrics in same query."""
    driver = pgvector_table

    result = driver.execute("""
        SELECT content,
               embedding <-> '[0.1, 0.2, 0.3]'::vector AS euclidean_dist,
               embedding <=> '[0.1, 0.2, 0.3]'::vector AS cosine_dist,
               embedding <#> '[0.1, 0.2, 0.3]'::vector AS neg_inner_product
        FROM vector_docs
    """)
    rows = result.get_data()

    assert len(rows) == 3
    for row in rows:
        assert row["euclidean_dist"] is not None
        assert row["cosine_dist"] is not None
        assert row["neg_inner_product"] is not None
