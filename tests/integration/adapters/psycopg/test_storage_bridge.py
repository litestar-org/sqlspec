"""Storage bridge integration tests for psycopg drivers."""

from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgAsyncDriver, PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.core import SQLResult
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


@pytest.fixture
def psycopg_sync_session(psycopg_sync_config: PsycopgSyncConfig) -> Generator[PsycopgSyncDriver, None, None]:
    with psycopg_sync_config.provide_session() as session:
        yield session


@pytest.fixture
async def psycopg_async_session(psycopg_async_config: PsycopgAsyncConfig) -> AsyncGenerator[PsycopgAsyncDriver, None]:
    async with psycopg_async_config.provide_session() as session:
        yield session


def test_psycopg_sync_storage_bridge_with_minio(
    psycopg_sync_config: PsycopgSyncConfig,
    minio_service: "MinioService",
    minio_client: "Minio",
    minio_default_bucket_name: str,
) -> None:
    alias = "storage_bridge_psycopg_sync"
    destination = f"alias://{alias}/psycopg_sync/export.parquet"
    source_table = "storage_bridge_psycopg_sync_source"
    target_table = "storage_bridge_psycopg_sync_target"

    storage_registry.clear()
    try:
        prefix = register_minio_alias(alias, minio_service, minio_default_bucket_name)

        with psycopg_sync_config.provide_session() as session:
            session.execute_script(f"DROP TABLE IF EXISTS {source_table} CASCADE")
            session.execute_script(f"DROP TABLE IF EXISTS {target_table} CASCADE")
            session.execute_script(f"CREATE TABLE {source_table} (id INT PRIMARY KEY, label TEXT NOT NULL)")
            session.commit()
            session.execute(
                f"INSERT INTO {source_table} (id, label) VALUES (%s, %s), (%s, %s), (%s, %s)",
                1,
                "alpha",
                2,
                "beta",
                3,
                "gamma",
            )
            session.commit()

            export_job = session.select_to_storage(
                f"SELECT id, label FROM {source_table} WHERE label IN ($1, $2, $3) ORDER BY id",
                destination,
                "alpha",
                "beta",
                "gamma",
                format_hint="parquet",
            )
            assert export_job.telemetry["rows_processed"] == 3

            session.execute_script(f"CREATE TABLE {target_table} (id INT PRIMARY KEY, label TEXT NOT NULL)")
            session.commit()
            load_job = session.load_from_storage(target_table, destination, file_format="parquet", overwrite=True)
            assert load_job.telemetry["rows_processed"] == 3

            result = session.execute(f"SELECT id, label FROM {target_table} ORDER BY id")
            assert isinstance(result, SQLResult)
            assert result.data == [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}, {"id": 3, "label": "gamma"}]

        object_name = f"{prefix}/psycopg_sync/export.parquet"
        stat = minio_client.stat_object(bucket_name=minio_default_bucket_name, object_name=object_name)
        object_size = stat.size if stat.size is not None else 0
        assert object_size > 0
    finally:
        storage_registry.clear()
        with psycopg_sync_config.provide_session() as cleanup:
            cleanup.execute_script(f"DROP TABLE IF EXISTS {source_table} CASCADE")
            cleanup.execute_script(f"DROP TABLE IF EXISTS {target_table} CASCADE")
            cleanup.commit()


@pytest.mark.anyio
async def test_psycopg_async_storage_bridge_with_minio(
    psycopg_async_session: PsycopgAsyncDriver,
    minio_service: "MinioService",
    minio_client: "Minio",
    minio_default_bucket_name: str,
) -> None:
    alias = "storage_bridge_psycopg_async"
    destination = f"alias://{alias}/psycopg_async/export.parquet"
    source_table = "storage_bridge_psycopg_async_source"
    target_table = "storage_bridge_psycopg_async_target"

    storage_registry.clear()
    try:
        prefix = register_minio_alias(alias, minio_service, minio_default_bucket_name)

        await psycopg_async_session.execute_script(f"DROP TABLE IF EXISTS {source_table} CASCADE")
        await psycopg_async_session.execute_script(f"DROP TABLE IF EXISTS {target_table} CASCADE")
        await psycopg_async_session.execute_script(
            f"CREATE TABLE {source_table} (id INT PRIMARY KEY, label TEXT NOT NULL)"
        )
        for idx, label in enumerate(["north", "south", "east"], start=1):
            await psycopg_async_session.execute(f"INSERT INTO {source_table} (id, label) VALUES (%s, %s)", idx, label)

        export_job = await psycopg_async_session.select_to_storage(
            f"SELECT id, label FROM {source_table} WHERE label IN ($1, $2, $3) ORDER BY id",
            destination,
            "north",
            "south",
            "east",
            format_hint="parquet",
        )
        assert export_job.telemetry["rows_processed"] == 3

        await psycopg_async_session.execute_script(
            f"CREATE TABLE {target_table} (id INT PRIMARY KEY, label TEXT NOT NULL)"
        )
        load_job = await psycopg_async_session.load_from_storage(
            target_table, destination, file_format="parquet", overwrite=True
        )
        assert load_job.telemetry["rows_processed"] == 3

        rows = await psycopg_async_session.select(f"SELECT id, label FROM {target_table} ORDER BY id")
        assert rows == [{"id": 1, "label": "north"}, {"id": 2, "label": "south"}, {"id": 3, "label": "east"}]

        object_name = f"{prefix}/psycopg_async/export.parquet"
        stat = minio_client.stat_object(bucket_name=minio_default_bucket_name, object_name=object_name)
        object_size = stat.size if stat.size is not None else 0
        assert object_size > 0
    finally:
        storage_registry.clear()
        await psycopg_async_session.execute_script(f"DROP TABLE IF EXISTS {source_table} CASCADE")
        await psycopg_async_session.execute_script(f"DROP TABLE IF EXISTS {target_table} CASCADE")
