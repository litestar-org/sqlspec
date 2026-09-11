"""Local extension and S3-compatible behavior behind native storage eligibility."""

from pathlib import Path
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.rustfs import RustfsService

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.core import _native_storage_eligible
from sqlspec.storage import StorageRegistry, SyncStoragePipeline
from tests.fixtures.rustfs import ensure_rustfs_bucket, rustfs_filesystem, rustfs_obstore_kwargs


@pytest.mark.parametrize("autoload", [False, True])
def test_unavailable_extension_is_distinct_from_object_failure(tmp_path: Path, autoload: bool) -> None:
    with duckdb.connect(
        config={
            "extension_directory": str(tmp_path),
            "autoload_known_extensions": autoload,
            "autoinstall_known_extensions": autoload,
            "autoinstall_extension_repository": "http://127.0.0.1:9",
        }
    ) as connection:
        with pytest.raises(duckdb.Error, match=r"extension.*httpfs") as exc:
            connection.execute("SELECT * FROM read_parquet(?)", ["s3://sqlspec-probe/missing.parquet"])
        assert type(exc.value) is duckdb.Error
        assert connection.execute("SELECT 1").fetchone() == (1,)


def test_pool_storage_snapshot_tracks_actual_connection(tmp_path: Path) -> None:
    config = DuckDBConfig(
        connection_config={"database": f":memory:storage_{uuid4().hex}", "autoload_known_extensions": False},
        driver_features={"extensions": [{"name": "httpfs", "required": True}]},
    )
    try:
        with config.provide_session() as session:
            assert session.driver_features["_duckdb_storage_extensions"] == frozenset({"httpfs"})
        pool = config.provide_pool()
        pool.close()
        pool._extensions.clear()
        with config.provide_session() as session:
            assert session.driver_features["_duckdb_storage_extensions"] == frozenset()
    finally:
        config.close_pool()


def test_best_effort_extension_failure_does_not_enable_native_storage(tmp_path: Path) -> None:
    config = DuckDBConfig(
        connection_config={
            "database": f":memory:storage_{uuid4().hex}",
            "extension_directory": str(tmp_path),
            "autoload_known_extensions": False,
            "autoinstall_known_extensions": False,
        },
        driver_features={"extensions": [{"name": "httpfs"}]},
    )
    try:
        with config.provide_session() as session:
            assert session.driver_features.get("_duckdb_storage_extensions", frozenset()) == frozenset()
            assert session.connection.execute("SELECT 1").fetchone() == (1,)
    finally:
        config.close_pool()


def _local_config(service: RustfsService, *, wrong_key: bool = False) -> DuckDBConfig:
    return DuckDBConfig(
        connection_config={
            "database": f":memory:duckdb_storage_{uuid4().hex}",
            "autoload_known_extensions": False,
            "autoinstall_known_extensions": False,
        },
        driver_features={
            "extensions": [{"name": "httpfs", "required": True}],
            "secrets": [
                {
                    "name": "local_storage",
                    "secret_type": "s3",
                    "required": True,
                    "value": {
                        "key_id": "invalid-local-key" if wrong_key else service.access_key,
                        "secret": service.secret_key,
                        "region": "us-east-1",
                        "endpoint": service.endpoint,
                        "use_ssl": service.secure,
                        "url_style": "path",
                    },
                }
            ],
        },
    )


def test_local_object_success_missing_and_auth_errors(
    rustfs_service: RustfsService, rustfs_bucket_name: str, tmp_path: Path
) -> None:
    bucket = ensure_rustfs_bucket(rustfs_service, rustfs_bucket_name)
    key = f"duckdb-native-probe/{uuid4().hex}.parquet"
    path = tmp_path / "source.parquet"
    pq.write_table(pa.table({"value": [1, 2]}), path)
    fs = rustfs_filesystem(rustfs_service)
    fs.put_file(str(path), f"{bucket}/{key}")
    uri = f"s3://{bucket}/{key}"
    good = _local_config(rustfs_service)
    wrong = _local_config(rustfs_service, wrong_key=True)
    try:
        with good.provide_session() as session:
            registry = StorageRegistry()
            options = rustfs_obstore_kwargs(rustfs_service)
            registry.register_alias("native_probe", f"s3://{bucket}", backend="obstore", **options)
            target = SyncStoragePipeline(registry=registry).resolve_destination(f"alias://native_probe/{key}")
            assert target.uri == uri
            assert _native_storage_eligible(target.uri, target.protocol, options, session.driver_features, write=False)
            assert session.connection.execute("SELECT * FROM read_parquet(?)", [uri]).fetchall() == [(1,), (2,)]
            with pytest.raises(duckdb.Error) as missing:
                session.connection.execute("SELECT * FROM read_parquet(?)", [uri + ".missing"])
            assert "404" in str(missing.value)
        with wrong.provide_session() as session:
            with pytest.raises(duckdb.Error) as denied:
                session.connection.execute("SELECT * FROM read_parquet(?)", [uri])
            assert "403" in str(denied.value)
    finally:
        good.close_pool()
        wrong.close_pool()
        fs.rm(f"{bucket}/{key}")
