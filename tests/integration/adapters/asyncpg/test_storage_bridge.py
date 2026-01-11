"""Storage bridge integration tests for AsyncPG using MinIO."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.storage.registry import storage_registry
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from tests.integration.adapters._storage_bridge_helpers import register_minio_alias

if TYPE_CHECKING:  # pragma: no cover
    from minio import Minio
    from pytest_databases.docker.minio import MinioService

pytestmark = [
    pytest.mark.asyncpg,
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


@pytest.mark.asyncio(loop_scope="function")
async def test_asyncpg_storage_bridge_with_minio(
    asyncpg_async_driver: AsyncpgDriver,
    minio_service: "MinioService",
    minio_client: "Minio",
    minio_default_bucket_name: str,
) -> None:
    alias = "storage_bridge_asyncpg"
    destination_path = "alias://storage_bridge_asyncpg/asyncpg/export.parquet"
    source_table = "storage_bridge_asyncpg_source"
    target_table = "storage_bridge_asyncpg_target"

    storage_registry.clear()
    try:
        prefix = register_minio_alias(alias, minio_service, minio_default_bucket_name)

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
        stat = minio_client.stat_object(bucket_name=minio_default_bucket_name, object_name=object_name)
        object_size = stat.size if stat.size is not None else 0
        assert object_size > 0
    finally:
        storage_registry.clear()
        await asyncpg_async_driver.execute(f"DROP TABLE IF EXISTS {source_table} CASCADE")
        await asyncpg_async_driver.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE")
