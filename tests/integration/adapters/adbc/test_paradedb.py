"""Integration tests for ADBC driver with ParadeDB (pgvector + pg_search)."""

from collections.abc import Generator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver

pytestmark = pytest.mark.xdist_group("paradedb")


@pytest.fixture(scope="session")
def paradedb_adbc_connection_config(paradedb_service: "PostgresService") -> "dict[str, Any]":
    """ADBC connection config pointing at the ParadeDB Docker service."""
    return {
        "uri": (
            f"postgresql://{paradedb_service.user}:{paradedb_service.password}"
            f"@{paradedb_service.host}:{paradedb_service.port}/{paradedb_service.database}"
        )
    }


@pytest.fixture(scope="function")
def paradedb_adbc_config(paradedb_adbc_connection_config: "dict[str, Any]") -> "Generator[AdbcConfig, None, None]":
    """Provide an AdbcConfig instance connected to ParadeDB."""
    config = AdbcConfig(connection_config=dict(paradedb_adbc_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="function")
def paradedb_adbc_driver(paradedb_adbc_config: "AdbcConfig") -> "Generator[AdbcDriver, None, None]":
    """Create an ADBC driver connected to ParadeDB."""
    with paradedb_adbc_config.provide_session() as session:
        yield session


# --- Extension Detection Tests ---


@pytest.mark.integration
def test_extensions_enabled_on_paradedb(paradedb_adbc_config: "AdbcConfig") -> None:
    """Verify pgvector and paradedb extensions are detected on ParadeDB.

    ParadeDB includes both the 'vector' (pgvector) and 'pg_search' extensions,
    so the driver should detect these and switch to the 'paradedb' dialect.
    """
    with paradedb_adbc_config.provide_session() as session:
        session.execute("SELECT 1")

    assert paradedb_adbc_config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
    assert paradedb_adbc_config._paradedb_available is True  # pyright: ignore[reportPrivateUsage]
    assert paradedb_adbc_config.statement_config.dialect == "paradedb"


@pytest.mark.integration
def test_paradedb_first_session_uses_detected_dialect(paradedb_adbc_config: "AdbcConfig") -> None:
    """The first session should use the ParadeDB dialect after detection."""
    with paradedb_adbc_config.provide_session() as session:
        assert session.statement_config.dialect == "paradedb"


# --- BM25 Search Tests ---


@pytest.fixture(scope="function")
def paradedb_search_table(paradedb_adbc_driver: "AdbcDriver") -> "Generator[AdbcDriver, None, None]":
    """Create a test table with BM25 index for ParadeDB search tests."""
    paradedb_adbc_driver.execute_script("""
        DROP TABLE IF EXISTS test_search_items CASCADE;
        CREATE TABLE test_search_items (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            category TEXT,
            rating INTEGER
        );
    """)

    paradedb_adbc_driver.execute(
        "INSERT INTO test_search_items (description, category, rating) VALUES ($1, $2, $3)",
        ("comfortable running shoes for athletes", "footwear", 5),
    )
    paradedb_adbc_driver.execute(
        "INSERT INTO test_search_items (description, category, rating) VALUES ($1, $2, $3)",
        ("leather dress shoes formal", "footwear", 4),
    )
    paradedb_adbc_driver.execute(
        "INSERT INTO test_search_items (description, category, rating) VALUES ($1, $2, $3)",
        ("casual sneakers everyday wear", "footwear", 4),
    )
    paradedb_adbc_driver.execute(
        "INSERT INTO test_search_items (description, category, rating) VALUES ($1, $2, $3)",
        ("hiking boots waterproof", "footwear", 5),
    )
    paradedb_adbc_driver.execute(
        "INSERT INTO test_search_items (description, category, rating) VALUES ($1, $2, $3)",
        ("summer sandals beach", "footwear", 3),
    )
    paradedb_adbc_driver.execute(
        "INSERT INTO test_search_items (description, category, rating) VALUES ($1, $2, $3)",
        ("running shorts athletic", "apparel", 4),
    )
    paradedb_adbc_driver.execute(
        "INSERT INTO test_search_items (description, category, rating) VALUES ($1, $2, $3)",
        ("formal dress pants", "apparel", 3),
    )

    # Create BM25 index
    paradedb_adbc_driver.execute_script("""
        CREATE INDEX test_search_items_idx ON test_search_items
        USING bm25 (id, description, category)
        WITH (key_field = 'id');
    """)

    try:
        yield paradedb_adbc_driver
    finally:
        paradedb_adbc_driver.execute_script("DROP TABLE IF EXISTS test_search_items CASCADE")


@pytest.mark.integration
def test_paradedb_bm25_search_operator(paradedb_search_table: "AdbcDriver") -> None:
    """Test ParadeDB BM25 search using @@@ operator."""
    driver = paradedb_search_table

    result = driver.execute("SELECT id, description FROM test_search_items WHERE description @@@ 'running'")
    rows = result.get_data()
    assert len(rows) >= 1
    for row in rows:
        assert "running" in row["description"].lower()


@pytest.mark.integration
def test_paradedb_conjunction_match(paradedb_search_table: "AdbcDriver") -> None:
    """Test ParadeDB match conjunction requiring ALL tokens to match."""
    driver = paradedb_search_table

    result = driver.execute(
        "SELECT id, description FROM test_search_items "
        "WHERE description @@@ pdb.match('running shoes', conjunction_mode => true)"
    )
    rows = result.get_data()
    assert len(rows) >= 1
    for row in rows:
        desc = row["description"].lower()
        assert "running" in desc and "shoes" in desc


@pytest.mark.integration
def test_paradedb_disjunction_match(paradedb_search_table: "AdbcDriver") -> None:
    """Test ParadeDB match disjunction (default OR semantics)."""
    driver = paradedb_search_table

    result = driver.execute(
        "SELECT id, description FROM test_search_items WHERE description @@@ pdb.match('running boots')"
    )
    rows = result.get_data()
    assert len(rows) >= 2
    descriptions = [row["description"].lower() for row in rows]
    assert any("running" in d for d in descriptions)
    assert any("boots" in d for d in descriptions)


@pytest.mark.integration
def test_paradedb_phrase_query(paradedb_search_table: "AdbcDriver") -> None:
    """Test ParadeDB phrase query matching exact sequence of tokens."""
    driver = paradedb_search_table

    result = driver.execute(
        "SELECT id, description FROM test_search_items WHERE description @@@ pdb.phrase('running shoes')"
    )
    rows = result.get_data()
    assert len(rows) >= 1
    for row in rows:
        assert "running shoes" in row["description"].lower()

    result_wrong_order = driver.execute(
        "SELECT id, description FROM test_search_items WHERE description @@@ pdb.phrase('shoes running')"
    )
    assert len(result_wrong_order.get_data()) == 0


@pytest.mark.integration
def test_paradedb_term_query(paradedb_search_table: "AdbcDriver") -> None:
    """Test ParadeDB term query for exact token match."""
    driver = paradedb_search_table

    result = driver.execute("SELECT id, description FROM test_search_items WHERE description @@@ pdb.term('running')")
    assert len(result.get_data()) >= 1

    result_category = driver.execute(
        "SELECT id, category FROM test_search_items WHERE category @@@ pdb.term('footwear')"
    )
    rows = result_category.get_data()
    assert len(rows) >= 1
    for row in rows:
        assert row["category"] == "footwear"


# --- pgvector via ParadeDB ---


@pytest.mark.integration
def test_paradedb_pgvector_operations(paradedb_adbc_driver: "AdbcDriver") -> None:
    """Test pgvector operations work through ParadeDB."""
    paradedb_adbc_driver.execute_script("""
        DROP TABLE IF EXISTS test_vectors CASCADE;
        CREATE TABLE test_vectors (
            id SERIAL PRIMARY KEY,
            embedding vector(3)
        );
    """)

    try:
        paradedb_adbc_driver.execute("INSERT INTO test_vectors (embedding) VALUES ($1::vector)", ("[1.0, 2.0, 3.0]",))
        paradedb_adbc_driver.execute("INSERT INTO test_vectors (embedding) VALUES ($1::vector)", ("[4.0, 5.0, 6.0]",))
        paradedb_adbc_driver.execute("INSERT INTO test_vectors (embedding) VALUES ($1::vector)", ("[1.1, 2.1, 3.1]",))

        result = paradedb_adbc_driver.execute("""
            SELECT id, embedding <=> '[1.0, 2.0, 3.0]'::vector AS distance
            FROM test_vectors
            ORDER BY distance
            LIMIT 2
        """)
        rows = result.get_data()
        assert len(rows) == 2
        assert rows[0]["distance"] < 0.01

    finally:
        paradedb_adbc_driver.execute_script("DROP TABLE IF EXISTS test_vectors CASCADE")
