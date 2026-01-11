"""Storage bridge integration tests for DuckDB using MinIO."""

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.duckdb import DuckDBDriver
from sqlspec.storage.registry import storage_registry
from sqlspec.typing import FSSPEC_INSTALLED, PYARROW_INSTALLED
from tests.integration.adapters._storage_bridge_helpers import register_minio_alias

if TYPE_CHECKING:  # pragma: no cover
    from minio import Minio
    from pytest_databases.docker.minio import MinioService

pytestmark = [
    pytest.mark.xdist_group("storage"),
    pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec not installed"),
    pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed"),
]


def test_duckdb_storage_bridge_with_minio(
    duckdb_basic_session: DuckDBDriver,
    minio_service: "MinioService",
    minio_client: "Minio",
    minio_default_bucket_name: str,
) -> None:
    alias = "storage_bridge_duckdb"
    destination_path = "alias://storage_bridge_duckdb/duckdb/export.parquet"

    storage_registry.clear()
    try:
        prefix = register_minio_alias(alias, minio_service, minio_default_bucket_name)

        duckdb_basic_session.execute("DROP TABLE IF EXISTS storage_bridge_duckdb_source")
        duckdb_basic_session.execute("DROP TABLE IF EXISTS storage_bridge_duckdb_target")
        duckdb_basic_session.execute(
            "CREATE TABLE storage_bridge_duckdb_source (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
        )
        duckdb_basic_session.execute(
            "INSERT INTO storage_bridge_duckdb_source VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')"
        )

        export_job = duckdb_basic_session.select_to_storage(
            "SELECT id, label FROM storage_bridge_duckdb_source ORDER BY id", destination_path, format_hint="parquet"
        )
        assert export_job.telemetry["rows_processed"] == 3

        duckdb_basic_session.execute(
            "CREATE TABLE storage_bridge_duckdb_target (id INTEGER PRIMARY KEY, label TEXT NOT NULL)"
        )
        load_job = duckdb_basic_session.load_from_storage(
            "storage_bridge_duckdb_target", destination_path, file_format="parquet", overwrite=True
        )
        assert load_job.telemetry["rows_processed"] == 3

        result = duckdb_basic_session.execute("SELECT id, label FROM storage_bridge_duckdb_target ORDER BY id")
        rows = [(row["id"], row["label"]) for row in result.get_data()]
        assert rows == [(1, "alpha"), (2, "beta"), (3, "gamma")]

        object_name = f"{prefix}/duckdb/export.parquet"
        stat = minio_client.stat_object(bucket_name=minio_default_bucket_name, object_name=object_name)
        object_size = stat.size if stat.size is not None else 0
        assert object_size > 0
    finally:
        storage_registry.clear()
        duckdb_basic_session.execute("DROP TABLE IF EXISTS storage_bridge_duckdb_source")
        duckdb_basic_session.execute("DROP TABLE IF EXISTS storage_bridge_duckdb_target")
