"""Measure complete PostgreSQL ADBC sessions with unpooled, cloned and pooled connections."""

import hashlib
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from threading import Barrier, BrokenBarrierError, Event, Lock
from time import perf_counter_ns
from typing import TYPE_CHECKING
from uuid import uuid4

import click

from sqlspec.adapters.adbc import AdbcConfig

if TYPE_CHECKING:
    from adbc_driver_manager.dbapi import Connection


class CloneConfig(AdbcConfig):
    """Close each session clone while retaining its original caller-owned seed."""

    __slots__ = ("_clone_lock", "_seed")

    def __init__(self, uri: str, seed: "Connection") -> None:
        super().__init__(
            connection_config={"driver_name": "postgresql", "uri": uri, "autocommit": False},
            driver_features=_features(),
        )
        self._seed = seed
        self._clone_lock = Lock()

    def create_connection(self) -> "Connection":
        with self._clone_lock:
            return self._seed.adbc_clone()


@click.command()
@click.option("--uri-env", default="ADBC_POSTGRESQL_TEST_URI", show_default=True)
@click.option("--mode", type=click.Choice(["all", "unpooled", "clone", "pooled"]), default="all", show_default=True)
@click.option("--workers", type=click.Choice(["1", "4"]), default="1", show_default=True)
@click.option("--warmup", type=click.IntRange(min=1), default=100, show_default=True)
@click.option("--samples", type=click.IntRange(min=1), default=1000, show_default=True)
@click.option("--repeats", type=click.IntRange(min=1), default=7, show_default=True)
@click.option("--output", type=click.Path(path_type=Path, dir_okay=False))
def main(uri_env: str, mode: str, workers: str, warmup: int, samples: int, repeats: int, output: Path | None) -> None:
    """Compare per-session latency on independent worker rows in a disposable schema.

    Copy this script outside the checkout to run installed source/baseline wheels.
    Set SQLSPEC_BENCH_REVISION to the verified source SHA for an installed wheel.
    URI values and credentials are never included in the JSON report.
    """
    uri = os.environ.get(uri_env)
    if not uri:
        raise click.ClickException(f"Set the connection environment variable {uri_env} before running")
    provenance = _provenance()
    worker_count = int(workers)
    schema = "sqlspec_adbc_pool_bench_" + uuid4().hex
    if re.fullmatch(r"sqlspec_adbc_pool_bench_[0-9a-f]{32}", schema) is None:
        raise RuntimeError("Invalid generated benchmark schema")
    table = schema + ".sessions"
    unpooled = AdbcConfig(
        connection_config={"driver_name": "postgresql", "uri": uri, "autocommit": False}, driver_features=_features()
    )
    modes = ["unpooled", "clone", "pooled"] if mode == "all" else [mode]
    results: list[dict[str, object]] = []
    combined: dict[str, list[int]] = {name: [] for name in modes}
    with ExitStack() as cleanup:
        configs = {"unpooled": unpooled}
        if "clone" in modes:
            seed = unpooled.create_connection()
            cleanup.callback(seed.close)
            configs["clone"] = CloneConfig(uri, seed)
        if "pooled" in modes:
            pooled = AdbcConfig(
                connection_config={"driver_name": "postgresql", "uri": uri, "autocommit": False},
                driver_features=_features(),
                pool_config={"max_size": worker_count, "timeout": 30.0},
            )
            cleanup.callback(pooled.close_pool)
            pooled.create_pool()
            configs["pooled"] = pooled
        with unpooled.provide_session() as session:
            server_version = str(session.select_value("SELECT current_setting('server_version')"))
            server_identity = str(
                session.select_value("SELECT inet_server_addr()::text || ':' || inet_server_port()::text")
            )
            session.commit()
        cleanup.callback(_drop_schema, unpooled, schema)
        with unpooled.provide_session() as session:
            session.execute(f"CREATE SCHEMA {schema}")
            session.execute(
                f"CREATE TABLE {table} (worker_id INTEGER PRIMARY KEY, value BIGINT NOT NULL, payload VARCHAR(32) NOT NULL)"
            )
            for worker_id in range(1, worker_count + 1):
                session.execute(
                    f"INSERT INTO {table} (worker_id, value, payload) VALUES (?, ?, ?)", worker_id, 0, "sqlspec"
                )
            session.commit()
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for repeat in range(repeats):
                ordered = modes[repeat % len(modes) :] + modes[: repeat % len(modes)]
                for name in ordered:
                    with unpooled.provide_session() as session:
                        session.execute(f"UPDATE {table} SET value = 0")
                        session.commit()
                    timings = _measure(executor, configs[name], table, worker_count, warmup, samples)
                    with unpooled.provide_session() as session:
                        rows = session.select(f"SELECT worker_id, value, payload FROM {table} ORDER BY worker_id")
                        expected = [
                            {"worker_id": worker_id, "value": warmup + samples, "payload": "sqlspec"}
                            for worker_id in range(1, worker_count + 1)
                        ]
                        if rows != expected:
                            raise RuntimeError("Benchmark row increment or payload verification failed")
                        session.commit()
                    combined[name].extend(timings)
                    results.append({
                        "mode": name,
                        "repeat": repeat + 1,
                        **_summary(timings),
                        "verified_rows": worker_count,
                    })
    report = {
        "metadata": {
            **provenance,
            "timestamp": datetime.now(UTC).isoformat(),
            "server_version": server_version,
            "server_identifier_sha256": hashlib.sha256(server_identity.encode()).hexdigest(),
            "workers": worker_count,
            "warmup_per_worker": warmup,
            "samples_per_worker": samples,
            "repeats": repeats,
            "timing_unit": "nanoseconds",
            "workload": "begin/read/update/read/commit/session-release",
        },
        "results": results,
        "aggregate": {name: _summary(timings) for name, timings in combined.items()},
    }
    encoded = json.dumps(report, indent=2, allow_nan=False) + "\n"
    if output is None:
        click.echo(encoded, nl=False)
    else:
        output.write_text(encoded, encoding="utf-8")
        click.echo(f"Wrote benchmark results to {output}")


def _features() -> dict[str, bool]:
    return {
        "enable_pgvector": False,
        "enable_paradedb": False,
        "enable_pg_textsearch": False,
        "enable_statement_profiling": False,
    }


def _drop_schema(config: AdbcConfig, schema: str) -> None:
    with config.provide_session() as session:
        session.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        session.commit()


def _measure(
    executor: ThreadPoolExecutor, config: AdbcConfig, table: str, workers: int, warmup: int, samples: int
) -> list[int]:
    barrier = Barrier(workers, timeout=60)
    stop = Event()

    def worker(worker_id: int) -> list[int]:
        try:
            for _ in range(warmup):
                if stop.is_set():
                    raise BrokenBarrierError("A benchmark peer failed")
                transact(worker_id)
            barrier.wait()
            timings = []
            for _ in range(samples):
                if stop.is_set():
                    raise BrokenBarrierError("A benchmark peer failed")
                start = perf_counter_ns()
                transact(worker_id)
                timings.append(perf_counter_ns() - start)
            return timings
        except BaseException:
            stop.set()
            barrier.abort()
            raise

    def transact(worker_id: int) -> None:
        with config.provide_session() as session:
            session.begin()
            before = int(session.select_value(f"SELECT value FROM {table} WHERE worker_id = ?", worker_id))
            session.execute(f"UPDATE {table} SET value = ? WHERE worker_id = ?", before + 1, worker_id)
            after = session.select_value(f"SELECT value FROM {table} WHERE worker_id = ?", worker_id)
            if after != before + 1:
                raise RuntimeError("Transactional read-after-write verification failed")
            session.commit()

    futures = [executor.submit(worker, worker_id) for worker_id in range(1, workers + 1)]
    timings: list[int] = []
    failures: list[BaseException] = []
    for future in futures:
        try:
            timings.extend(future.result())
        except BaseException as exc:
            failures.append(exc)
    if failures:
        raise next((exc for exc in failures if not isinstance(exc, BrokenBarrierError)), failures[0])
    return timings


def _summary(timings: list[int]) -> dict[str, object]:
    if not timings or any(value <= 0 for value in timings):
        raise RuntimeError("Missing or invalid session timings")
    ordered = sorted(timings)
    median = statistics.median(ordered)
    deviation = statistics.pstdev(ordered)
    return {
        "count": len(ordered),
        "median_ns": median,
        "p95_ns": ordered[math.ceil(0.95 * len(ordered)) - 1],
        "stddev_ns": deviation,
        "noise_ratio": deviation / median,
        "samples_ns": timings,
    }


def _provenance() -> dict[str, object]:
    import sqlspec
    from sqlspec.driver import _sync

    origin = Path(sqlspec.__file__ or "").resolve()
    source_root = origin.parent.parent
    revision = os.environ.get("SQLSPEC_BENCH_REVISION")
    dirty: bool | None = None
    if (source_root / ".git").exists():
        git = shutil.which("git")
        if git is None:
            raise click.ClickException("Git is required to establish source-checkout provenance")
        revision = subprocess.check_output([git, "-C", str(source_root), "rev-parse", "HEAD"], text=True).strip()
        dirty = bool(subprocess.check_output([git, "-C", str(source_root), "status", "--porcelain"], text=True).strip())
    if not revision or re.fullmatch(r"[0-9a-f]{7,40}", revision) is None:
        raise click.ClickException("Set SQLSPEC_BENCH_REVISION to the verified source SHA for installed packages")
    versions = {}
    for distribution in ("sqlspec", "adbc-driver-manager", "adbc-driver-postgresql", "pyarrow", "sqlglot"):
        try:
            versions[distribution] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            versions[distribution] = "not-installed"
    return {
        "revision": revision,
        "source_dirty": dirty,
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "sqlspec_origin": str(origin),
        "driver_origin": _sync.__file__,
        "mypyc_compiled": (_sync.__file__ or "").endswith((".so", ".pyd")),
        "python": sys.version,
        "executable": sys.executable,
        "platform": platform.platform(),
        "versions": versions,
    }


if __name__ == "__main__":
    main()
