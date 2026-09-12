"""Local extension and S3-compatible behavior behind native storage eligibility."""

import json
import os
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any, cast
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.rustfs import RustfsService

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.core import _native_storage_eligible
from sqlspec.adapters.duckdb.driver import DuckDBDriver
from sqlspec.storage import StorageFormat, StorageRegistry, SyncStoragePipeline
from sqlspec.storage.registry import storage_registry
from tests.fixtures.rustfs import ensure_rustfs_bucket, rustfs_filesystem, rustfs_obstore_kwargs


def test_native_http_parquet_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pq.write_table(pa.table({"id": [1, 2], "name": ["001", None]}), tmp_path / "data.parquet")
    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(tmp_path)))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = DuckDBConfig(
        connection_config={
            "database": f":memory:http_{uuid4().hex}",
            "extension_directory": str(tmp_path / "extensions"),
        },
        driver_features={"extensions": [{"name": "httpfs", "install": True, "required": True}]},
    )

    def reject_arrow(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("HTTP Parquet import unexpectedly materialized Arrow")

    monkeypatch.setattr(DuckDBDriver, "load_from_arrow", reject_arrow)
    try:
        with config.provide_session() as session:
            session.connection.execute("CREATE TABLE target(id INTEGER, name VARCHAR)")
            job = session.load_from_storage(
                "target", f"http://127.0.0.1:{server.server_port}/data.parquet", file_format="parquet"
            )
            assert job.telemetry["backend"] == "duckdb"
            assert job.telemetry["rows_processed"] == 2
            assert session.connection.execute("SELECT * FROM target").fetchall() == [(1, "001"), (2, None)]
    finally:
        config.close_pool()
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.skipif(not os.getenv("SQLSPEC_DUCKDB_STORAGE_BENCHMARK"), reason="Opt-in local storage benchmark")
def test_native_storage_benchmark(
    rustfs_service: RustfsService, rustfs_bucket_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tools.scripts.bench_duckdb_storage import run_benchmark

    bucket = ensure_rustfs_bucket(rustfs_service, rustfs_bucket_name)
    monkeypatch.setenv("SQLSPEC_STORAGE_ENDPOINT", f"http://{rustfs_service.endpoint}")
    monkeypatch.setenv("SQLSPEC_STORAGE_BUCKET", bucket)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", rustfs_service.access_key)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", rustfs_service.secret_key)
    results = run_benchmark()
    Path(os.environ["SQLSPEC_DUCKDB_STORAGE_BENCHMARK"]).write_text(json.dumps(results, indent=2) + "\n")
    assert len(results["results"]) == 3


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
        driver_features={"extensions": [{"name": "httpfs", "install": True, "required": True}]},
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
            "extensions": [{"name": "httpfs", "install": True, "required": True}],
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


@pytest.mark.parametrize("file_format", ["parquet", "csv"])
def test_arrow_and_native_copy_replace_single_local_object(
    rustfs_service: RustfsService, rustfs_bucket_name: str, file_format: str
) -> None:
    bucket = ensure_rustfs_bucket(rustfs_service, rustfs_bucket_name)
    alias = f"duckdb_copy_{uuid4().hex}"
    key = f"duckdb-copy-probe/{uuid4().hex}.{file_format}"
    options = rustfs_obstore_kwargs(rustfs_service)
    storage_registry.register_alias(alias, f"s3://{bucket}", backend="obstore", **options)
    config = _local_config(rustfs_service)
    uri = f"s3://{bucket}/{key}"
    fs = rustfs_filesystem(rustfs_service)
    try:
        with config.provide_session() as session:
            for value in [1, 2]:
                job = session.select_to_storage(
                    "SELECT ? AS value",
                    f"alias://{alias}/{key}",
                    value,
                    format_hint=cast("StorageFormat", file_format),
                    arrow_schema=pa.schema([("value", pa.int32())]),
                )
                assert job.telemetry["rows_processed"] == 1
                assert job.telemetry["bytes_processed"] > 0
                assert job.telemetry["destination"] == key
            copy_sql = (
                "COPY (SELECT ? AS value) TO ? (FORMAT PARQUET)"
                if file_format == "parquet"
                else "COPY (SELECT ? AS value) TO ? (FORMAT CSV, HEADER TRUE)"
            )
            read_sql = "SELECT * FROM read_parquet(?)" if file_format == "parquet" else "SELECT * FROM read_csv(?)"
            for value in [3, 4]:
                assert session.connection.execute(copy_sql, [uri, value]).fetchone() == (1,)
                assert session.connection.execute(read_sql, [uri]).fetchall() == [(value,)]
    finally:
        config.close_pool()
        fs.rm(f"{bucket}/{key}")
        storage_registry.clear_cache(alias)


def test_native_parquet_append_and_overwrite_fallback(
    rustfs_service: RustfsService, rustfs_bucket_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    bucket = ensure_rustfs_bucket(rustfs_service, rustfs_bucket_name)
    alias = f"duckdb_import_{uuid4().hex}"
    key = f"duckdb-native-import/{uuid4().hex}.parquet"
    destination = f"alias://{alias}/{key}"
    storage_registry.register_alias(alias, f"s3://{bucket}", backend="obstore", **rustfs_obstore_kwargs(rustfs_service))
    config = _local_config(rustfs_service)
    fs = rustfs_filesystem(rustfs_service)

    def reject_arrow(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("native append must not materialize Arrow")

    try:
        with config.provide_session() as session:
            session.select_to_storage("SELECT 1 AS id, NULL::VARCHAR AS name", destination)
            session.connection.execute('CREATE TABLE "target space" (id INTEGER, name VARCHAR)')
            with monkeypatch.context() as patch:
                patch.setattr(DuckDBDriver, "load_from_arrow", reject_arrow)
                for _ in range(2):
                    job = session.load_from_storage('"target space"', destination, file_format="parquet")
                    assert job.telemetry["rows_processed"] == 1
                    assert job.telemetry["backend"] == "duckdb"
                    assert "bytes_processed" not in job.telemetry
            assert session.connection.execute('SELECT * FROM "target space"').fetchall() == [(1, None), (1, None)]
            job = session.load_from_storage('"target space"', destination, file_format="parquet", overwrite=True)
            assert job.telemetry["rows_processed"] == 1
            assert session.connection.execute('SELECT * FROM "target space"').fetchall() == [(1, None)]
    finally:
        config.close_pool()
        if fs.exists(f"{bucket}/{key}"):
            fs.rm(f"{bucket}/{key}")
        storage_registry.clear_cache(alias)


@pytest.mark.parametrize("file_format", ["parquet", "csv"])
def test_native_export_does_not_materialize_arrow(
    rustfs_service: RustfsService, rustfs_bucket_name: str, file_format: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    bucket = ensure_rustfs_bucket(rustfs_service, rustfs_bucket_name)
    alias = f"duckdb_export_{uuid4().hex}"
    key = f"duckdb-native-export/{uuid4().hex}-quote'.{file_format}"
    storage_registry.register_alias(alias, f"s3://{bucket}", backend="obstore", **rustfs_obstore_kwargs(rustfs_service))
    config = _local_config(rustfs_service)
    fs = rustfs_filesystem(rustfs_service)

    def reject_arrow(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("native export must not materialize Arrow")

    monkeypatch.setattr(DuckDBDriver, "select_to_arrow", reject_arrow)
    try:
        with config.provide_session() as session:
            job = session.select_to_storage(
                "SELECT :first AS first, :second AS second, :first AS repeated",
                f"alias://{alias}/{key}",
                {"first": "quote' and ?", "second": 42},
                format_hint=cast("StorageFormat", file_format),
                telemetry={"extra": {"marker": "retained"}},
            )
            assert job.telemetry["rows_processed"] == 1
            assert "bytes_processed" not in job.telemetry
            assert job.telemetry["duration_s"] >= 0
            assert job.telemetry["backend"] == "duckdb"
            source_telemetry = cast("dict[str, Any]", job.telemetry["extra"]["source"])
            assert source_telemetry["extra"]["marker"] == "retained"
            read_sql = "SELECT * FROM read_parquet(?)" if file_format == "parquet" else "SELECT * FROM read_csv(?)"
            assert session.connection.execute(read_sql, [f"s3://{bucket}/{key}"]).fetchall() == [
                ("quote' and ?", 42, "quote' and ?")
            ]
            replacement = session.select_to_storage(
                "SELECT :first AS first, :second AS second, :first AS repeated",
                f"alias://{alias}/{key}",
                {"first": "replaced", "second": 7},
                format_hint=cast("StorageFormat", file_format),
            )
            assert replacement.telemetry["rows_processed"] == 1
            assert session.connection.execute(read_sql, [f"s3://{bucket}/{key}"]).fetchall() == [
                ("replaced", 7, "replaced")
            ]
    finally:
        config.close_pool()
        if fs.exists(f"{bucket}/{key}"):
            fs.rm(f"{bucket}/{key}")
        storage_registry.clear_cache(alias)
