"""Storage bridge integration tests for PSQLPy driver."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver
from sqlspec.storage.registry import storage_registry
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from tests.integration.adapters._storage_bridge_helpers import register_minio_alias

if TYPE_CHECKING:  # pragma: no cover
    from minio import Minio
    from pytest_databases.docker.minio import MinioService

pytestmark = [
    pytest.mark.xdist_group("postgres"),
    pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


@pytest.mark.anyio
async def test_psqlpy_storage_bridge_with_minio(
    psqlpy_driver: PsqlpyDriver, minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> None:
    alias = "storage_bridge_psqlpy"
    destination = f"alias://{alias}/psqlpy/export.parquet"
    source_table = "storage_bridge_psqlpy_source"
    target_table = "storage_bridge_psqlpy_target"

    storage_registry.clear()
    try:
        prefix = register_minio_alias(alias, minio_service, minio_default_bucket_name)

        await psqlpy_driver.execute(f"DROP TABLE IF EXISTS {source_table}")
        await psqlpy_driver.execute(f"DROP TABLE IF EXISTS {target_table}")
        await psqlpy_driver.execute(f"CREATE TABLE {source_table} (id INT PRIMARY KEY, label TEXT NOT NULL)")
        for idx, label in enumerate(["delta", "omega", "zeta"], start=1):
            await psqlpy_driver.execute(f"INSERT INTO {source_table} (id, label) VALUES (?, ?)", (idx, label))

        export_job = await psqlpy_driver.select_to_storage(
            f"SELECT id, label FROM {source_table} ORDER BY id", destination, format_hint="parquet"
        )
        assert export_job.telemetry["rows_processed"] == 3

        await psqlpy_driver.execute(f"CREATE TABLE {target_table} (id INT PRIMARY KEY, label TEXT NOT NULL)")
        load_job = await psqlpy_driver.load_from_storage(
            target_table, destination, file_format="parquet", overwrite=True
        )
        assert load_job.telemetry["rows_processed"] == 3

        rows = await psqlpy_driver.select(f"SELECT id, label FROM {target_table} ORDER BY id")
        assert rows == [{"id": 1, "label": "delta"}, {"id": 2, "label": "omega"}, {"id": 3, "label": "zeta"}]

        object_name = f"{prefix}/psqlpy/export.parquet"
        stat = minio_client.stat_object(bucket_name=minio_default_bucket_name, object_name=object_name)
        object_size = stat.size if stat.size is not None else 0
        assert object_size > 0
    finally:
        storage_registry.clear()
        await psqlpy_driver.execute(f"DROP TABLE IF EXISTS {source_table}")
        await psqlpy_driver.execute(f"DROP TABLE IF EXISTS {target_table}")
