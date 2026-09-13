"""Compare native and inherited Cockroach Parquet storage on local services.

Configure SQLSPEC_COCKROACH_DSN, SQLSPEC_COCKROACH_NATIVE_URI (server-accessible
credentials/endpoint), SQLSPEC_COCKROACH_CLIENT_URI (same bucket/prefix), and
SQLSPEC_COCKROACH_STORAGE_OPTIONS (JSON fsspec credentials/endpoint). Secrets are
never printed. Each run owns a random prefix and tables, removed on completion.
"""

import argparse
import json
import os
import statistics
import sys
import threading
import time
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
from psycopg import sql

from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgSyncConfig, CockroachPsycopgSyncDriver
from sqlspec.storage import StorageRegistry, SyncStoragePipeline


def _append_uri(uri: str, suffix: str) -> str:
    parts = urlsplit(uri)
    return urlunsplit(parts._replace(path=parts.path.rstrip("/") + "/" + suffix))


def _monitor(
    dsn: str, table: str, stop: threading.Event, ready: threading.Event, interval: float, samples: list[dict[str, Any]]
) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute("SET statement_timeout = '250ms'")
        ready.set()
        while not stop.is_set():
            started = time.perf_counter()
            try:
                connection.execute(sql.SQL("SELECT 1 FROM {} LIMIT 1").format(sql.Identifier(table)))
            except psycopg.Error as exc:
                samples.append({"at": started, "duration_s": time.perf_counter() - started, "sqlstate": exc.sqlstate})
            stop.wait(interval)


def run_benchmark(sizes: list[int], warmup: int, iterations: int, poll_interval: float) -> dict[str, Any]:
    """Run paired export/import samples; report polling uncertainty separately."""
    dsn = os.environ["SQLSPEC_COCKROACH_DSN"]
    native_base = os.environ["SQLSPEC_COCKROACH_NATIVE_URI"]
    client_base = os.environ["SQLSPEC_COCKROACH_CLIENT_URI"]
    options = json.loads(os.environ.get("SQLSPEC_COCKROACH_STORAGE_OPTIONS", "{}"))
    registry = StorageRegistry()

    class BenchmarkPipeline(SyncStoragePipeline):
        __slots__ = ()

        def __init__(self) -> None:
            super().__init__(registry=registry)

    class BenchmarkDriver(CockroachPsycopgSyncDriver):
        __slots__ = ()
        storage_pipeline_factory = BenchmarkPipeline

        def select_to_arrow(self, *args: Any, **kwargs: Any) -> Any:
            if self.driver_features["enable_native_storage"]:
                msg = "Native benchmark unexpectedly selected client-side export"
                raise RuntimeError(msg)
            return super().select_to_arrow(*args, **kwargs)

    prefix = "sqlspec_bench_" + uuid4().hex
    alias = prefix
    registry.register_alias(alias, _append_uri(client_base, prefix), backend="fsspec", **options)
    backend = registry.get(alias)
    results: list[dict[str, Any]] = []
    try:
        with psycopg.connect(dsn, autocommit=True) as connection:
            version = connection.execute("SELECT version()").fetchone()
            for size in sizes:
                for native in (False, True):
                    config = CockroachPsycopgSyncConfig(
                        connection_config={"autocommit": True}, driver_features={"enable_native_storage": native}
                    )
                    driver = BenchmarkDriver(
                        connection, statement_config=config.statement_config, driver_features=config.driver_features
                    )
                    for iteration in range(-warmup, iterations):
                        sample_name = uuid4().hex
                        destination = (
                            _append_uri(native_base, prefix + "/" + sample_name)
                            if native
                            else "alias://" + alias + "/" + sample_name + ".parquet"
                        )
                        query = "SELECT i::INT8 AS id, repeat('x', 64)::STRING AS label FROM generate_series(1, :rows) AS t(i)"
                        if native and not driver._native_storage_ready():
                            msg = (
                                "Native benchmark connection not ready: autocommit="
                                + str(connection.autocommit)
                                + ", status="
                                + str(connection.info.transaction_status)
                            )
                            raise RuntimeError(msg)
                        started = time.perf_counter()
                        exported = driver.select_to_storage(query, destination, {"rows": size}, format_hint="parquet")
                        export_s = time.perf_counter() - started
                        if exported.telemetry["rows_processed"] != size:
                            msg = "Export row count mismatch"
                            raise RuntimeError(msg)
                        sources = (
                            [_append_uri(destination, str(name)) for name in exported.telemetry["extra"]["files"]]
                            if native
                            else [destination]
                        )
                        table = prefix + "_" + sample_name
                        connection.execute(
                            sql.SQL("CREATE TABLE {} (id INT8 PRIMARY KEY, label STRING)").format(sql.Identifier(table))
                        )
                        stop, ready = threading.Event(), threading.Event()
                        unavailable: list[dict[str, Any]] = []
                        monitor = threading.Thread(
                            target=_monitor, args=(dsn, table, stop, ready, poll_interval, unavailable), daemon=True
                        )
                        monitor.start()
                        try:
                            if not ready.wait(10):
                                msg = "Offline monitor failed to connect"
                                raise RuntimeError(msg)
                            started = time.perf_counter()
                            imported_rows = 0
                            for source in sources:
                                imported_rows += driver.load_from_storage(
                                    table, source, file_format="parquet"
                                ).telemetry["rows_processed"]
                            import_s = time.perf_counter() - started
                            if imported_rows != size or connection.execute(
                                sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))
                            ).fetchone() != (size,):
                                msg = "Import row count mismatch"
                                raise RuntimeError(msg)
                        finally:
                            stop.set()
                            monitor.join(5)
                            connection.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(table)))
                        if iteration >= 0:
                            span = (
                                unavailable[-1]["at"] - unavailable[0]["at"] + unavailable[-1]["duration_s"]
                                if unavailable
                                else 0.0
                            )
                            results.append({
                                "rows": size,
                                "native": native,
                                "iteration": iteration,
                                "export_s": export_s,
                                "import_s": import_s,
                                "unavailable_samples": len(unavailable),
                                "observed_unavailable_span_s": span,
                                "unavailable_sqlstates": sorted({str(item["sqlstate"]) for item in unavailable}),
                            })
        summaries = []
        for size in sizes:
            for native in (False, True):
                group = [item for item in results if item["rows"] == size and item["native"] == native]
                for operation in ("export", "import"):
                    values = [item[operation + "_s"] for item in group]
                    summaries.append({
                        "scenario": operation,
                        "rows": size,
                        "native": native,
                        "samples": len(values),
                        "median_s": statistics.median(values),
                        "min_s": min(values),
                        "max_s": max(values),
                    })
        return {
            "server_version": version[0] if version else None,
            "poll_interval_s": poll_interval,
            "probe_timeout_s": 0.25,
            "offline_measurement": "Observed failed-probe span only; polling and query latency bound resolution. Zero samples does not prove zero offline time.",
            "samples": results,
            "summaries": summaries,
        }
    finally:
        if backend.list_objects_sync():
            backend.delete_sync("", recursive=True)
        registry.clear()


def main() -> None:
    """Run the explicitly configured local benchmark."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", nargs="+", type=int, default=[100, 1000, 10000])
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--poll-interval", type=float, default=0.02)
    args = parser.parse_args()
    if min(args.rows) <= 0 or args.warmup < 0 or args.iterations <= 0 or args.poll_interval <= 0:
        parser.error("rows, iterations and poll interval must be positive; warmup must be nonnegative")
    try:
        result = run_benchmark(args.rows, args.warmup, args.iterations, args.poll_interval)
    except Exception as exc:
        parser.exit(1, "Benchmark failed (credentials omitted): " + type(exc).__name__ + "\n")
    sys.stdout.write(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
