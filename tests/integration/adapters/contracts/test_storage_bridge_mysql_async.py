"""MySQL async-family storage bridge contract tests."""

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.mysql import MySQLService

from tests.integration.adapters.contracts._mysql_async import (
    MYSQL_ASYNC_ADAPTERS,
    close_mysql_async_config,
    mysql_async_config,
)

pytestmark = [pytest.mark.xdist_group("mysql")]


@pytest.fixture(params=MYSQL_ASYNC_ADAPTERS)
async def mysql_async_storage_driver(
    request: pytest.FixtureRequest, mysql_service: MySQLService
) -> AsyncGenerator[Any, None]:
    """Create a MySQL async-family driver for storage bridge tests."""
    config = mysql_async_config(str(request.param), mysql_service)
    try:
        async with config.provide_session() as driver:
            yield driver
    finally:
        await close_mysql_async_config(config)


async def _fetch_rows(driver: Any, table: str) -> list[dict[str, object]]:
    rows = await driver.select(f"SELECT id, name FROM {table} ORDER BY id")
    assert isinstance(rows, list)
    return rows


async def test_mysql_async_load_from_arrow(mysql_async_storage_driver: Any) -> None:
    """MySQL async-family drivers load Arrow tables into destination tables."""
    table_name = "storage_bridge_users"
    await mysql_async_storage_driver.execute(f"DROP TABLE IF EXISTS {table_name}")
    await mysql_async_storage_driver.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY, name VARCHAR(64))")

    arrow_table = pa.table({"id": [1, 2], "name": ["alpha", "beta"]})

    job = await mysql_async_storage_driver.load_from_arrow(table_name, arrow_table, overwrite=True)

    assert job.telemetry["rows_processed"] == arrow_table.num_rows
    assert job.telemetry["destination"] == table_name

    rows = await _fetch_rows(mysql_async_storage_driver, table_name)
    assert rows == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]

    await mysql_async_storage_driver.execute(f"DROP TABLE IF EXISTS {table_name}")


async def test_mysql_async_load_from_storage(tmp_path: Path, mysql_async_storage_driver: Any) -> None:
    """MySQL async-family drivers load Parquet files through the storage bridge."""
    await mysql_async_storage_driver.execute("DROP TABLE IF EXISTS storage_bridge_scores")
    await mysql_async_storage_driver.execute(
        "CREATE TABLE storage_bridge_scores (id INT PRIMARY KEY, score DECIMAL(5,2))"
    )

    arrow_table = pa.table({"id": [5, 6], "score": [12.5, 99.1]})
    destination = tmp_path / "scores.parquet"
    pq.write_table(arrow_table, destination)

    job = await mysql_async_storage_driver.load_from_storage(
        "storage_bridge_scores", str(destination), file_format="parquet", overwrite=True
    )

    assert job.telemetry["destination"] == "storage_bridge_scores"
    assert job.telemetry["extra"]["source"]["destination"].endswith("scores.parquet")  # type: ignore[index]
    assert job.telemetry["extra"]["source"]["backend"]  # type: ignore[index]

    rows = await mysql_async_storage_driver.select("SELECT id, score FROM storage_bridge_scores ORDER BY id")
    assert len(rows) == 2
    assert rows[0]["id"] == 5
    assert float(rows[0]["score"]) == pytest.approx(12.5)
    assert rows[1]["id"] == 6
    assert float(rows[1]["score"]) == pytest.approx(99.1)

    await mysql_async_storage_driver.execute("DROP TABLE IF EXISTS storage_bridge_scores")
