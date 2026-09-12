"""Compare native Parquet transfers with Arrow against a provisioned local S3 service.

Provision RustFS with pytest-databases; export SQLSPEC_STORAGE_ENDPOINT,
SQLSPEC_STORAGE_BUCKET, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. This script
does not create services or cloud resources. Run with ``python -m
tools.scripts.bench_duckdb_storage --output results.json``.
"""

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import duckdb
import pyarrow as pa

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.storage import SyncStoragePipeline
from sqlspec.storage.registry import storage_registry

__all__ = ("run_benchmark",)


def _measure(action: Callable[[], None], warmup: int, iterations: int) -> dict[str, Any]:
    for _ in range(warmup):
        action()
    samples = []
    for _ in range(iterations):
        start = perf_counter()
        action()
        samples.append(perf_counter() - start)
    return {
        "median_s": statistics.median(samples),
        "min_s": min(samples),
        "max_s": max(samples),
        "stdev_s": statistics.stdev(samples) if len(samples) > 1 else 0.0,
        "samples_s": samples,
    }


def run_benchmark(*, sizes: Sequence[int] = (100, 1000, 10000), warmup: int = 4, iterations: int = 8) -> dict[str, Any]:
    """Measure equivalent native and Arrow Parquet transfers on a local endpoint.

    Args:
        sizes: Dataset row counts.
        warmup: Untimed repetitions per scenario.
        iterations: Timed repetitions per scenario.

    Returns:
        JSON-compatible environment, verification and timing results.

    Raises:
        ValueError: Endpoint is not local or sample counts are invalid.
        KeyError: Required environment configuration is absent.
    """
    endpoint = urlsplit(os.environ["SQLSPEC_STORAGE_ENDPOINT"])
    if endpoint.hostname not in {"127.0.0.1", "localhost", "::1"} or endpoint.scheme not in {"http", "https"}:
        msg = "The benchmark requires a local HTTP(S) object-store endpoint."
        raise ValueError(msg)
    if warmup < 0 or iterations < 1 or not sizes or min(sizes) < 1:
        msg = "Dataset sizes and iterations must be positive; warmup must be nonnegative."
        raise ValueError(msg)
    bucket = os.environ["SQLSPEC_STORAGE_BUCKET"]
    key = os.environ["AWS_ACCESS_KEY_ID"]
    secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    alias = f"duckdb_bench_{uuid4().hex}"
    storage_registry.register_alias(
        alias,
        f"s3://{bucket}",
        backend="obstore",
        aws_endpoint=endpoint.geturl(),
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_virtual_hosted_style_request=False,
        client_options={"allow_http": endpoint.scheme == "http"},
    )
    config = DuckDBConfig(
        connection_config={"database": f":memory:benchmark_{uuid4().hex}"},
        driver_features={
            "extensions": [{"name": "httpfs", "install": True, "required": True}],
            "secrets": [
                {
                    "name": "benchmark",
                    "secret_type": "s3",
                    "required": True,
                    "value": {
                        "key_id": key,
                        "secret": secret,
                        "endpoint": endpoint.netloc,
                        "region": "us-east-1",
                        "url_style": "path",
                        "use_ssl": endpoint.scheme == "https",
                    },
                }
            ],
        },
    )
    pipeline = SyncStoragePipeline()
    objects: list[str] = []
    results: list[dict[str, Any]] = []
    cold_start = perf_counter()
    try:
        with config.provide_session() as driver:
            provisioning_s = perf_counter() - cold_start
            for rows in sizes:
                driver.connection.execute(
                    "CREATE OR REPLACE TABLE dataset AS SELECT i AS id, 'value-' || i AS label FROM range(?) t(i)",
                    [rows],
                )
                driver.connection.execute("CREATE OR REPLACE TABLE imported AS SELECT * FROM dataset WHERE false")
                native_key = f"sqlspec-benchmark/{alias}/{rows}-native.parquet"
                arrow_key = f"sqlspec-benchmark/{alias}/{rows}-arrow.parquet"
                objects.extend((native_key, arrow_key))
                native_uri = f"alias://{alias}/{native_key}"
                arrow_uri = f"alias://{alias}/{arrow_key}"

                def native_export(native_uri: str = native_uri, rows: int = rows) -> None:
                    job = driver.select_to_storage("SELECT * FROM dataset", native_uri)
                    assert job.telemetry["backend"] == "duckdb"
                    assert job.telemetry["rows_processed"] == rows

                def arrow_export(arrow_uri: str = arrow_uri) -> None:
                    table = driver.select_to_arrow("SELECT * FROM dataset").get_data()
                    pipeline.write_arrow(table, arrow_uri, compression="snappy")

                native_export()
                arrow_export()
                native_table, native_meta = pipeline.read_arrow(native_uri, file_format="parquet")
                arrow_table, arrow_meta = pipeline.read_arrow(arrow_uri, file_format="parquet")
                assert native_table.equals(arrow_table)

                def native_import(native_uri: str = native_uri, rows: int = rows) -> None:
                    job = driver.load_from_storage("imported", native_uri, file_format="parquet")
                    assert job.telemetry["backend"] == "duckdb"
                    assert job.telemetry["rows_processed"] == rows

                def arrow_import(arrow_uri: str = arrow_uri) -> None:
                    table, meta = pipeline.read_arrow(arrow_uri, file_format="parquet")
                    driver.load_from_arrow("imported", table, telemetry=meta)

                measurements: dict[str, Any] = {}
                for name, action in (
                    ("native_export", native_export),
                    ("arrow_export", arrow_export),
                    ("native_import", native_import),
                    ("arrow_import", arrow_import),
                ):
                    driver.connection.execute("TRUNCATE imported")
                    measurements[name] = _measure(action, warmup, iterations)
                    if name.endswith("import"):
                        count = driver.connection.execute("SELECT count(*) FROM imported").fetchone()
                        assert count == (rows * (warmup + iterations),)
                        differences = driver.connection.execute(
                            "SELECT * FROM imported EXCEPT SELECT * FROM dataset"
                        ).fetchall()
                        assert differences == []
                results.append({
                    "rows": rows,
                    "arrow_memory_bytes": native_table.nbytes,
                    "native_object_bytes": native_meta["bytes_processed"],
                    "arrow_object_bytes": arrow_meta["bytes_processed"],
                    "measurements": measurements,
                })
    finally:
        config.close_pool()
        backend = storage_registry.get(alias)
        for object_key in objects:
            backend.delete_sync(object_key)
        storage_registry.clear_cache(alias)
    git = shutil.which("git")
    if git is None:
        msg = "Git is required to record the benchmark source revision."
        raise RuntimeError(msg)
    revision = subprocess.check_output([git, "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output([git, "status", "--porcelain"], text=True).strip())
    return {
        "source_revision": revision,
        "working_tree_dirty": dirty,
        "machine": platform.platform(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "pyarrow": pa.__version__,
        "format": "parquet",
        "compression": "snappy",
        "warmup": warmup,
        "iterations": iterations,
        "cold_connection_and_extension_s": provisioning_s,
        "cloud_verified": False,
        "csv_import": "Arrow fallback; inference differs",
        "results": results,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    arguments.output.write_text(json.dumps(run_benchmark(), indent=2) + "\n")
