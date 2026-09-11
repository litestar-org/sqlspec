"""Local paired native/inherited Parquet benchmark acceptance run."""

import json
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

import pytest
from tools.scripts.bench_cockroach_storage import run_benchmark

from tests.fixtures.rustfs import rustfs_fsspec_kwargs
from tests.integration.adapters.cockroach.test_native_storage_capabilities import (
    native_storage_uri as native_storage_uri,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from pytest_databases.docker.cockroachdb import CockroachDBService
    from pytest_databases.docker.rustfs import RustfsService


def test_native_storage_paired_benchmark(
    cockroachdb_service: "CockroachDBService",
    rustfs_service: "RustfsService",
    native_storage_uri: str,
    monkeypatch: pytest.MonkeyPatch,
    record_property: "Callable[[str, object], None]",
) -> None:
    monkeypatch.setenv(
        "SQLSPEC_COCKROACH_DSN",
        f"host={cockroachdb_service.host} port={cockroachdb_service.port} dbname={cockroachdb_service.database} user=root sslmode=disable",
    )
    monkeypatch.setenv("SQLSPEC_COCKROACH_NATIVE_URI", native_storage_uri)
    monkeypatch.setenv("SQLSPEC_COCKROACH_CLIENT_URI", urlunsplit(urlsplit(native_storage_uri)._replace(query="")))
    monkeypatch.setenv("SQLSPEC_COCKROACH_STORAGE_OPTIONS", json.dumps(rustfs_fsspec_kwargs(rustfs_service)))
    result = run_benchmark([100, 1000, 10000], warmup=1, iterations=3, poll_interval=0.02)
    assert len(result["samples"]) == 18
    assert len(result["summaries"]) == 12
    assert all(item["samples"] == 3 for item in result["summaries"])
    record_property("cockroach_native_benchmark", json.dumps(result))
