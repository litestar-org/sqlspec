# pyright: ignore
"""Test ADBC connection with PostgreSQL."""

from __future__ import annotations

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_connection(postgres_service: PostgresService) -> None:
    """Test ADBC connection to PostgreSQL."""
    # Test direct connection
    config = AdbcConfig(
        uri=f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql.dbapi.connect",
    )

    with config.create_connection() as conn:
        assert conn is not None
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")  # pyright: ignore
            result = cur.fetchone()  # pyright: ignore
            assert result == (1,)
