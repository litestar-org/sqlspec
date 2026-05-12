"""Storage bridge integration tests for AsyncPG using RustFS."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.storage.registry import storage_registry
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from tests.fixtures.rustfs import rustfs_object_size
from tests.integration.adapters._storage_bridge_helpers import register_rustfs_alias

if TYPE_CHECKING:  # pragma: no cover
    from pytest_databases.docker.rustfs import RustfsService

pytestmark = [
    pytest.mark.asyncpg,
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


async def test_asyncpg_storage_bridge_with_rustfs(
    asyncpg_async_driver: AsyncpgDriver, rustfs_service: "RustfsService", rustfs_bucket_name: str
) -> None:
    alias = "storage_bridge_asyncpg"
    destination_path = "alias://storage_bridge_asyncpg/asyncpg/export.parquet"
    source_table = "storage_bridge_asyncpg_source"
    target_table = "storage_bridge_asyncpg_target"

    storage_registry.clear()
    try:
        prefix = register_rustfs_alias(alias, rustfs_service, rustfs_bucket_name)

        await asyncpg_async_driver.execute(f"DROP TABLE IF EXISTS {source_table} CASCADE")
        await asyncpg_async_driver.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE")
        await asyncpg_async_driver.execute(f"CREATE TABLE {source_table} (id INT PRIMARY KEY, label TEXT NOT NULL)")
        await asyncpg_async_driver.execute(
            f"INSERT INTO {source_table} (id, label) VALUES (1, 'north'), (2, 'south'), (3, 'east')"
        )

        export_job = await asyncpg_async_driver.select_to_storage(
            f"SELECT id, label FROM {source_table} ORDER BY id", destination_path, format_hint="parquet"
        )
        assert export_job.telemetry["rows_processed"] == 3

        await asyncpg_async_driver.execute(f"CREATE TABLE {target_table} (id INT PRIMARY KEY, label TEXT NOT NULL)")
        load_job = await asyncpg_async_driver.load_from_storage(
            target_table, destination_path, file_format="parquet", overwrite=True
        )
        assert load_job.telemetry["rows_processed"] == 3

        result = await asyncpg_async_driver.execute(f"SELECT id, label FROM {target_table} ORDER BY id")
        rows = [(row["id"], row["label"]) for row in result]
        assert rows == [(1, "north"), (2, "south"), (3, "east")]

        object_name = f"{prefix}/asyncpg/export.parquet"
        assert rustfs_object_size(rustfs_service, rustfs_bucket_name, object_name) > 0
    finally:
        storage_registry.clear()
        await asyncpg_async_driver.execute(f"DROP TABLE IF EXISTS {source_table} CASCADE")
        await asyncpg_async_driver.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE")
