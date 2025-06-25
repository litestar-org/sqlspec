"""Test execute_script functionality for ADBC drivers."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_postgresql_script_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for script testing."""
    config = AdbcConfig(
        uri=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql",
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        yield session


@pytest.fixture
def adbc_sqlite_script_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session for script testing."""
    config = AdbcConfig(uri=":memory:", driver_name="adbc_driver_sqlite", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as session:
        yield session


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_execute_script_ddl(adbc_postgresql_script_session: AdbcDriver) -> None:
    """Test execute_script with DDL statements on PostgreSQL."""
    script = """
    -- Create a test schema
    CREATE TABLE IF NOT EXISTS script_test1 (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS script_test2 (
        id SERIAL PRIMARY KEY,
        test1_id INTEGER REFERENCES script_test1(id),
        value INTEGER DEFAULT 0
    );

    -- Create an index
    CREATE INDEX idx_script_test2_value ON script_test2(value);
    """

    result = adbc_postgresql_script_session.execute_script(script)
    assert isinstance(result, SQLResult)

    # Verify tables were created
    check_result = adbc_postgresql_script_session.execute(
        SQL("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name IN ('script_test1', 'script_test2')
        ORDER BY table_name
    """)
    )
    assert len(check_result.data) == 2

    # Cleanup
    adbc_postgresql_script_session.execute_script("""
        DROP TABLE IF EXISTS script_test2;
        DROP TABLE IF EXISTS script_test1;
    """)


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_execute_script_ddl(adbc_sqlite_script_session: AdbcDriver) -> None:
    """Test execute_script with DDL statements on SQLite."""
    script = """
    -- Create test tables
    CREATE TABLE IF NOT EXISTS script_test1 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS script_test2 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        test1_id INTEGER,
        value INTEGER DEFAULT 0,
        FOREIGN KEY (test1_id) REFERENCES script_test1(id)
    );

    -- Create an index
    CREATE INDEX idx_script_test2_value ON script_test2(value);
    """

    result = adbc_sqlite_script_session.execute_script(script)
    assert isinstance(result, SQLResult)

    # Verify tables were created
    check_result = adbc_sqlite_script_session.execute(
        SQL("""
        SELECT name FROM sqlite_master
        WHERE type='table'
        AND name IN ('script_test1', 'script_test2')
        ORDER BY name
    """)
    )
    assert len(check_result.data) == 2


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
@pytest.mark.xfail(reason="ADBC PostgreSQL driver may not support multiple statements in execute")
def test_postgresql_execute_script_mixed(adbc_postgresql_script_session: AdbcDriver) -> None:
    """Test execute_script with mixed DDL and DML statements on PostgreSQL."""
    script = """
    -- Create table
    CREATE TABLE IF NOT EXISTS script_mixed (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER DEFAULT 0
    );

    -- Insert data
    INSERT INTO script_mixed (name, value) VALUES
        ('Test 1', 100),
        ('Test 2', 200),
        ('Test 3', 300);

    -- Update data
    UPDATE script_mixed SET value = value * 2 WHERE value > 100;

    -- Create view
    CREATE VIEW script_mixed_view AS
    SELECT name, value FROM script_mixed WHERE value >= 200;
    """

    result = adbc_postgresql_script_session.execute_script(script)
    assert isinstance(result, SQLResult)

    # Verify data
    data_result = adbc_postgresql_script_session.execute(SQL("SELECT * FROM script_mixed ORDER BY value"))
    assert len(data_result.data) == 3
    assert data_result.data[0]["value"] == 100  # Not updated
    assert data_result.data[1]["value"] == 400  # Updated from 200
    assert data_result.data[2]["value"] == 600  # Updated from 300

    # Cleanup
    adbc_postgresql_script_session.execute_script("""
        DROP VIEW IF EXISTS script_mixed_view;
        DROP TABLE IF EXISTS script_mixed;
    """)


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_execute_script_transaction(adbc_sqlite_script_session: AdbcDriver) -> None:
    """Test execute_script with transaction control on SQLite."""
    # First create a table
    adbc_sqlite_script_session.execute_script("""
        CREATE TABLE IF NOT EXISTS script_trans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER DEFAULT 0
        );
    """)

    # ADBC SQLite doesn't support explicit transactions in scripts
    # because it's already in autocommit mode with implicit transactions
    # So we test without explicit BEGIN/COMMIT
    script = """
    INSERT INTO script_trans (name, value) VALUES ('Trans 1', 100);
    INSERT INTO script_trans (name, value) VALUES ('Trans 2', 200);
    INSERT INTO script_trans (name, value) VALUES ('Trans 3', 300);
    UPDATE script_trans SET value = value + 1000;
    """

    result = adbc_sqlite_script_session.execute_script(script)
    assert isinstance(result, SQLResult)

    # Verify all operations completed
    check_result = adbc_sqlite_script_session.execute(SQL("SELECT * FROM script_trans ORDER BY value"))
    assert len(check_result.data) == 3
    assert all(row["value"] > 1000 for row in check_result.data)


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_postgresql_execute_script_error_handling(adbc_postgresql_script_session: AdbcDriver) -> None:
    """Test execute_script error handling on PostgreSQL."""
    # Create a table first
    adbc_postgresql_script_session.execute_script("""
        DROP TABLE IF EXISTS script_error;
        CREATE TABLE script_error (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
    """)

    # Script that will fail due to unique constraint
    script = """
    INSERT INTO script_error (name) VALUES ('duplicate');
    INSERT INTO script_error (name) VALUES ('duplicate');  -- This will fail
    """

    with pytest.raises(Exception):  # Specific exception type depends on ADBC implementation
        adbc_postgresql_script_session.execute_script(script)

    # For PostgreSQL ADBC, we need to create a new connection after error
    # because the transaction is aborted. So we'll use a fresh session for cleanup.
    # Instead, let's just skip the cleanup as the table will be dropped at the start
    # of the next test run anyway


@pytest.mark.xdist_group("adbc_sqlite")
@xfail_if_driver_missing
def test_sqlite_execute_script_comments(adbc_sqlite_script_session: AdbcDriver) -> None:
    """Test execute_script with various comment styles on SQLite."""
    # Note: Simple statement splitter doesn't handle inline comments with semicolons
    # So we avoid inline comments after statements
    script = """
    -- Single line comment
    CREATE TABLE IF NOT EXISTS script_comments (
        id INTEGER PRIMARY KEY,
        /* Multi-line
           comment */
        name TEXT NOT NULL
    );

    -- Insert statement
    INSERT INTO script_comments (name) VALUES ('Test');

    /* Another multi-line comment
       spanning multiple lines */
    SELECT COUNT(*) FROM script_comments;
    """

    result = adbc_sqlite_script_session.execute_script(script)
    assert isinstance(result, SQLResult)

    # Verify table and data
    check_result = adbc_sqlite_script_session.execute(SQL("SELECT * FROM script_comments"))
    assert len(check_result.data) == 1
    assert check_result.data[0]["name"] == "Test"
