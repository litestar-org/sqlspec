"""Local harness contracts; fake timings do not measure provider performance."""

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest
from tools.scripts.bench_bigquery_storage import QUERY, run_benchmark


def test_benchmark_controls_sizes_accounting_and_closes_resources() -> None:
    calls: list[tuple[bool, int]] = []
    closed: list[bool] = []

    @contextmanager
    def factory(native: bool) -> Any:
        def export(query: str, destination: str, parameters: dict[str, int], **kwargs: Any) -> Any:
            assert query == QUERY
            assert kwargs["format_hint"] == "parquet"
            calls.append((native, parameters["row_count"]))
            telemetry: dict[str, Any] = {"destination": destination, "extra": {"native_export": native}}
            if not native:
                telemetry["bytes_processed"] = 42
            return SimpleNamespace(telemetry=telemetry)

        try:
            yield cast(Any, SimpleNamespace(select_to_storage=export))
        finally:
            closed.append(native)

    records = run_benchmark(factory, destination="gs://bucket/bench", environment="fake-contract", repetitions=2)
    assert len(records) == 12
    assert len(calls) == 18
    assert closed == [True, False]
    assert {record["input_rows"] for record in records} == {100, 1000, 10000}
    for record in records:
        assert record["status"] == "ok"
        assert record["scenario"] == "bigquery_storage_export"
        assert record["environment"] == "fake-contract"
        assert record["total_s"] >= 0
        assert record["cloud_performance_verified"] is False
        if record["route"] == "native":
            assert "output_bytes" not in record
        else:
            assert record["output_bytes"] == 42
            assert record["bytes_source"] == "storage_telemetry"


@pytest.mark.parametrize("mode", ["exception", "fallback", "missing", "bad_bytes"])
def test_benchmark_reports_failure_or_unsupported_without_successful_native_timing(mode: str) -> None:
    closed: list[bool] = []

    @contextmanager
    def factory(native: bool) -> Any:
        telemetry: dict[str, Any] = {"destination": "gs://bucket/out", "extra": {"native_export": native}}
        if mode == "fallback":
            telemetry["extra"]["native_export"] = False
        elif mode == "missing":
            telemetry.pop("destination")
        elif mode == "bad_bytes":
            telemetry["bytes_processed"] = "unknown"
        export = Mock(return_value=SimpleNamespace(telemetry=telemetry))
        if mode == "exception":
            export.side_effect = RuntimeError("native job failed")
        try:
            yield cast(Any, SimpleNamespace(select_to_storage=export))
        finally:
            closed.append(native)

    records = run_benchmark(factory, destination="gs://bucket/bench", environment="fake-contract", warmup=0)
    assert closed == [True, False]
    native_records = [record for record in records if record["route"] == "native"]
    assert all(record["status"] == ("unsupported" if mode == "fallback" else "failed") for record in native_records)
    if mode == "fallback":
        assert all("total_s" not in record for record in native_records)
