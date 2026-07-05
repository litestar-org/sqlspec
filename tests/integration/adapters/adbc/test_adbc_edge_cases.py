"""ADBC edge-case residuals outside the shared contract matrix.

The contract hooks own ADBC NULLs, repeated parameters, RETURNING, backend
types, and Arrow-backed type materialization. This module keeps ADBC script
parsing around comments/empty statements and post-error connection recovery.
"""

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.core import SQLResult


@pytest.fixture
def adbc_postgresql_session(postgres_service: "PostgresService") -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session for edge-case testing."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "driver_name": "adbc_driver_postgresql",
        }
    )

    with config.provide_session() as session:
        yield session


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_execute_script_edge_cases(adbc_postgresql_session: AdbcDriver) -> None:
    """ADBC PostgreSQL handles comments, empty statements, and explicit transaction scripts."""
    mixed_script = """
        CREATE TABLE IF NOT EXISTS script_test_adbc (
            id SERIAL PRIMARY KEY,
            data TEXT
        );

        INSERT INTO script_test_adbc (data) VALUES ('script_data1');
        INSERT INTO script_test_adbc (data) VALUES ('script_data2');

        UPDATE script_test_adbc SET data = 'updated_' || data WHERE id = 1;

        -- Comment in script
        SELECT COUNT(*) FROM script_test_adbc;
    """

    result = adbc_postgresql_session.execute_script(mixed_script)
    assert result is None or isinstance(result, (str, SQLResult))

    verify_result = adbc_postgresql_session.execute("SELECT data FROM script_test_adbc ORDER BY id")
    assert verify_result.get_data() == [{"data": "updated_script_data1"}, {"data": "script_data2"}]

    comment_script = """
        -- This is a comment
        ;  -- Empty statement

        INSERT INTO script_test_adbc (data) VALUES ('comment_test');

        ; -- Another empty statement
        -- Final comment
    """
    comment_result = adbc_postgresql_session.execute_script(comment_script)
    assert comment_result is None or isinstance(comment_result, (str, SQLResult))

    transaction_script = """
        BEGIN;
        INSERT INTO script_test_adbc (data) VALUES ('transaction_test1');
        INSERT INTO script_test_adbc (data) VALUES ('transaction_test2');
        COMMIT;
    """
    trans_result = adbc_postgresql_session.execute_script(transaction_script)
    assert trans_result is None or isinstance(trans_result, (str, SQLResult))

    final_count = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM script_test_adbc")
    assert final_count.get_data()[0]["count"] >= 4

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS script_test_adbc")


@pytest.mark.xdist_group("postgres")
@pytest.mark.adbc
def test_connection_resilience(adbc_postgresql_session: AdbcDriver) -> None:
    """ADBC PostgreSQL can recover after syntax and constraint errors."""
    with pytest.raises(Exception):
        adbc_postgresql_session.execute("INVALID SQL SYNTAX HERE")

    recovery_result = adbc_postgresql_session.execute("SELECT 1 as recovery_test")
    assert recovery_result.get_data()[0]["recovery_test"] == 1

    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS constraint_test_adbc (
            id SERIAL PRIMARY KEY,
            unique_value TEXT UNIQUE
        )
    """)
    adbc_postgresql_session.execute("INSERT INTO constraint_test_adbc (unique_value) VALUES ($1)", ("unique1",))

    with pytest.raises(Exception):
        adbc_postgresql_session.execute("INSERT INTO constraint_test_adbc (unique_value) VALUES ($1)", ("unique1",))

    post_error_result = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM constraint_test_adbc")
    assert post_error_result.get_data()[0]["count"] == 1

    adbc_postgresql_session.execute_script("DROP TABLE IF EXISTS constraint_test_adbc")
