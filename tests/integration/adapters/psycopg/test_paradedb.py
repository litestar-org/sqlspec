"""Integration tests for psycopg driver with ParadeDB (pgvector + pg_search)."""

from collections.abc import Generator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver

pytestmark = pytest.mark.xdist_group("paradedb")


@pytest.fixture(scope="session")
def paradedb_psycopg_connection_config(paradedb_service: "PostgresService") -> "dict[str, Any]":
    """Base pool configuration for Psycopg tests with ParadeDB."""
    return {
        "conninfo": f"postgresql://{paradedb_service.user}:{paradedb_service.password}@{paradedb_service.host}:{paradedb_service.port}/{paradedb_service.database}"
    }


@pytest.fixture(scope="function")
def paradedb_psycopg_config(
    paradedb_psycopg_connection_config: "dict[str, Any]",
) -> "Generator[PsycopgSyncConfig, None, None]":
    """Provide a PsycopgSyncConfig instance connected to ParadeDB."""
    config = PsycopgSyncConfig(connection_config=dict(paradedb_psycopg_connection_config))
    try:
        yield config
    finally:
        pool = config.connection_instance
        if pool is not None:
            config.close_pool()
            config.connection_instance = None


@pytest.fixture(scope="function")
def paradedb_psycopg_driver(paradedb_psycopg_config: "PsycopgSyncConfig") -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a Psycopg driver connected to ParadeDB."""
    with paradedb_psycopg_config.provide_session() as session:
        yield session


@pytest.mark.integration
def test_extensions_enabled_on_paradedb(paradedb_psycopg_config: "PsycopgSyncConfig") -> None:
    """Verify pgvector and paradedb extensions are detected on ParadeDB.

    ParadeDB includes both the 'vector' (pgvector) and 'pg_search' extensions,
    so the driver should detect these and switch to the 'paradedb' dialect.
    """
    paradedb_psycopg_config.create_pool()

    assert paradedb_psycopg_config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
    assert paradedb_psycopg_config._paradedb_available is True  # pyright: ignore[reportPrivateUsage]
    assert paradedb_psycopg_config.statement_config.dialect == "paradedb"


@pytest.mark.integration
def test_paradedb_first_session_uses_detected_dialect(paradedb_psycopg_config: "PsycopgSyncConfig") -> None:
    """The first session should use the ParadeDB dialect without pre-creating the pool."""
    with paradedb_psycopg_config.provide_session() as session:
        assert session.statement_config.dialect == "paradedb"


@pytest.fixture(scope="function")
def paradedb_search_table(paradedb_psycopg_driver: "PsycopgSyncDriver") -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a test table with BM25 index for ParadeDB search tests."""
    paradedb_psycopg_driver.execute_script("""
        DROP TABLE IF EXISTS test_search_items CASCADE;
        CREATE TABLE test_search_items (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            category TEXT,
            rating INTEGER
        );
    """)

    # Insert test data with varied content for testing different operators
    paradedb_psycopg_driver.execute_many(
        "INSERT INTO test_search_items (description, category, rating) VALUES (%s, %s, %s)",
        [
            ("comfortable running shoes for athletes", "footwear", 5),
            ("leather dress shoes formal", "footwear", 4),
            ("casual sneakers everyday wear", "footwear", 4),
            ("hiking boots waterproof", "footwear", 5),
            ("summer sandals beach", "footwear", 3),
            ("running shorts athletic", "apparel", 4),
            ("formal dress pants", "apparel", 3),
        ],
    )

    # Create BM25 index
    paradedb_psycopg_driver.execute_script("""
        CREATE INDEX test_search_items_idx ON test_search_items
        USING bm25 (id, description, category)
        WITH (key_field = 'id');
    """)

    try:
        yield paradedb_psycopg_driver
    finally:
        paradedb_psycopg_driver.execute_script("DROP TABLE IF EXISTS test_search_items CASCADE")


@pytest.mark.integration
def test_paradedb_bm25_search_operator(paradedb_search_table: "PsycopgSyncDriver") -> None:
    """Test ParadeDB BM25 search using @@@ operator.

    The @@@ operator performs general BM25 full-text search.
    """
    driver = paradedb_search_table

    # Test BM25 search for single term
    result = driver.execute("SELECT id, description FROM test_search_items WHERE description @@@ 'running'")
    assert result.data is not None
    assert len(result.data) >= 1
    # Should match "running shoes" and "running shorts"
    for i in range(len(result.data)):
        assert "running" in result[i]["description"].lower()


@pytest.mark.integration
def test_paradedb_conjunction_match(paradedb_search_table: "PsycopgSyncDriver") -> None:
    """Test ParadeDB match conjunction using pdb.match with conjunction_mode.

    Conjunction mode requires ALL tokens to match (AND semantics).
    """
    driver = paradedb_search_table

    # "running shoes" with conjunction_mode should only match rows containing BOTH terms
    result = driver.execute(
        "SELECT id, description FROM test_search_items "
        "WHERE description @@@ pdb.match('running shoes', conjunction_mode => true)"
    )
    assert result.data is not None
    assert len(result.data) >= 1
    for i in range(len(result.data)):
        desc = result[i]["description"].lower()
        assert "running" in desc and "shoes" in desc


@pytest.mark.integration
def test_paradedb_disjunction_match(paradedb_search_table: "PsycopgSyncDriver") -> None:
    """Test ParadeDB match disjunction (default OR semantics).

    Default match query uses OR semantics - matches ANY token.
    """
    driver = paradedb_search_table

    # "running boots" should match rows with "running" OR "boots"
    result = driver.execute(
        "SELECT id, description FROM test_search_items WHERE description @@@ pdb.match('running boots')"
    )
    assert result.data is not None
    assert len(result.data) >= 2  # Should match running items AND hiking boots
    descriptions = [result[i]["description"].lower() for i in range(len(result.data))]
    assert any("running" in d for d in descriptions)
    assert any("boots" in d for d in descriptions)


@pytest.mark.integration
def test_paradedb_phrase_query(paradedb_search_table: "PsycopgSyncDriver") -> None:
    """Test ParadeDB phrase query using pdb.phrase.

    Phrase query matches exact sequence of tokens.
    """
    driver = paradedb_search_table

    # "running shoes" as phrase should match exact sequence
    result = driver.execute(
        "SELECT id, description FROM test_search_items WHERE description @@@ pdb.phrase('running shoes')"
    )
    assert result.data is not None
    assert len(result.data) >= 1
    for i in range(len(result.data)):
        assert "running shoes" in result[i]["description"].lower()

    # "shoes running" should NOT match (wrong order)
    result_wrong_order = driver.execute(
        "SELECT id, description FROM test_search_items WHERE description @@@ pdb.phrase('shoes running')"
    )
    assert result_wrong_order.data is not None
    assert len(result_wrong_order.data) == 0


@pytest.mark.integration
def test_paradedb_term_query(paradedb_search_table: "PsycopgSyncDriver") -> None:
    """Test ParadeDB term query using pdb.term.

    Term query performs exact token match.
    """
    driver = paradedb_search_table

    # Exact token "running" should match
    result = driver.execute("SELECT id, description FROM test_search_items WHERE description @@@ pdb.term('running')")
    assert result.data is not None
    assert len(result.data) >= 1

    # Test on category field - exact match "footwear"
    result_category = driver.execute(
        "SELECT id, category FROM test_search_items WHERE category @@@ pdb.term('footwear')"
    )
    assert result_category.data is not None
    assert len(result_category.data) >= 1
    for i in range(len(result_category.data)):
        assert result_category[i]["category"] == "footwear"


@pytest.mark.integration
def test_paradedb_pgvector_operations(paradedb_psycopg_driver: "PsycopgSyncDriver") -> None:
    """Test pgvector operations work through ParadeDB."""
    # Create a table with vector column
    paradedb_psycopg_driver.execute_script("""
        DROP TABLE IF EXISTS test_vectors CASCADE;
        CREATE TABLE test_vectors (
            id SERIAL PRIMARY KEY,
            embedding vector(3)
        );
    """)

    try:
        # Insert vector data
        paradedb_psycopg_driver.execute("INSERT INTO test_vectors (embedding) VALUES (%s::vector)", "[1.0, 2.0, 3.0]")
        paradedb_psycopg_driver.execute("INSERT INTO test_vectors (embedding) VALUES (%s::vector)", "[4.0, 5.0, 6.0]")
        paradedb_psycopg_driver.execute("INSERT INTO test_vectors (embedding) VALUES (%s::vector)", "[1.1, 2.1, 3.1]")

        # Test cosine similarity search
        result = paradedb_psycopg_driver.execute("""
            SELECT id, embedding <=> '[1.0, 2.0, 3.0]'::vector AS distance
            FROM test_vectors
            ORDER BY distance
            LIMIT 2
        """)
        assert result.data is not None
        assert len(result.data) == 2
        # First result should be exact match (distance ~0)
        assert result[0]["distance"] < 0.01

    finally:
        paradedb_psycopg_driver.execute_script("DROP TABLE IF EXISTS test_vectors CASCADE")
