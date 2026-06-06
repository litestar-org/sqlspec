"""Exception handling integration tests for oracledb adapter."""

from collections.abc import Generator

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleSyncConfig, OracleSyncDriver
from sqlspec.exceptions import SQLParsingError

pytestmark = pytest.mark.xdist_group("oracle")


@pytest.fixture
def oracle_sync_exception_session(oracle_service: OracleService) -> Generator[OracleSyncDriver, None, None]:
    """Create an Oracle sync session for exception testing."""
    config = OracleSyncConfig(
        connection_config={
            "user": oracle_service.user,
            "password": oracle_service.password,
            "dsn": f"{oracle_service.host}:{oracle_service.port}/{oracle_service.service_name}",
        }
    )

    try:
        with config.provide_session() as session:
            yield session
    finally:
        config.close_pool()


def test_sync_sql_parsing_error(oracle_sync_exception_session: OracleSyncDriver) -> None:
    """Test syntax error raises SQLParsingError (sync)."""
    with pytest.raises(SQLParsingError) as exc_info:
        oracle_sync_exception_session.execute("SELCT * FROM nonexistent_table")

    assert "sql" in str(exc_info.value).lower()
