"""Tests for benchmark runner helpers."""

import asyncio
import importlib.util
import inspect
from pathlib import Path
from types import ModuleType

import pytest


def _load_script_module(filename: str, module_name: str) -> ModuleType:
    module_path = Path(__file__).resolve().parents[3] / "tools" / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bench_defaults_are_statistically_useful() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    assert module.DEFAULT_BENCH_ITERATIONS == 7
    assert module.DEFAULT_BENCH_WARMUP == 3


def test_bench_summary_reports_stddev_iqr_and_noise() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    summary = module._summarize_times([1.0, 1.0, 1.1, 1.2])

    assert summary["time"] == 1.05
    assert summary["stddev"] > 0
    assert summary["iqr"] > 0
    assert summary["noise_ratio"] > 0
    assert summary["noisy"] is False


def test_async_benchmark_iterations_reuse_one_event_loop() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")
    loop_ids: list[int] = []

    async def scenario() -> None:
        loop_ids.append(id(asyncio.get_running_loop()))

    times = module._run_benchmark_iterations(scenario, is_async=True, iterations=3, warmup=2)

    assert len(times) == 3
    assert len(loop_ids) == 5
    assert len(set(loop_ids)) == 1


def test_bench_compare_formats_optional_stddev() -> None:
    module = _load_script_module("bench_compare.py", "bench_compare_for_tests")

    assert module._format_time({"time": 1.23456, "stddev": 0.01}) == "1.2346 +/- 0.0100"


def test_bench_driver_matrix_covers_pr_c_adapter_surfaces() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    expected_drivers = {
        "sqlite",
        "aiosqlite",
        "duckdb",
        "asyncpg",
        "psycopg",
        "psycopg_async",
        "psqlpy",
        "cockroach_asyncpg",
        "cockroach_psycopg",
        "cockroach_psycopg_async",
        "aiomysql",
        "asyncmy",
        "pymysql",
        "mysqlconnector",
        "mysqlconnector_async",
        "oracledb",
        "oracledb_async",
        "adbc",
        "spanner",
        "bigquery",
    }

    assert set(module.BENCHMARK_DRIVER_MATRIX) == expected_drivers
    assert set(module.expand_driver_selection(("all",))) == expected_drivers


def test_cockroach_benchmarks_require_explicit_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SQLSPEC_COCKROACH_DSN", raising=False)
    module = _load_script_module("bench.py", "bench_for_tests")

    with pytest.raises(module.BenchmarkSkipError):
        module._postgres_connection_config("cockroach_psycopg")


def test_bench_registry_has_core_sqlspec_workloads_for_all_pr_c_drivers() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    for driver, config in module.BENCHMARK_DRIVER_MATRIX.items():
        assert config["scenarios"] == module.CORE_SCENARIOS
        assert "sqlspec" in config["libraries"]
        for scenario in module.CORE_SCENARIOS:
            assert ("sqlspec", driver, scenario) in module.SCENARIO_REGISTRY


def test_bench_registry_has_raw_driver_workloads_for_container_drivers() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    container_drivers = {
        "asyncpg",
        "psycopg",
        "psycopg_async",
        "psqlpy",
        "cockroach_asyncpg",
        "cockroach_psycopg",
        "cockroach_psycopg_async",
        "aiomysql",
        "asyncmy",
        "pymysql",
        "mysqlconnector",
        "mysqlconnector_async",
        "oracledb",
        "oracledb_async",
        "adbc",
    }

    for driver in container_drivers:
        config = module.BENCHMARK_DRIVER_MATRIX[driver]
        assert "raw" in config["libraries"]
        for scenario in module.CORE_SCENARIOS:
            assert ("raw", driver, scenario) in module.SCENARIO_REGISTRY


def test_async_raw_driver_workloads_are_executable_coroutines() -> None:
    """Async raw PR-C driver entries should run driver workloads, not sync skip sentinels."""
    module = _load_script_module("bench.py", "bench_for_tests")

    async_raw_drivers = {
        "aiomysql",
        "asyncmy",
        "psqlpy",
        "mysqlconnector_async",
        "oracledb_async",
        "psycopg_async",
        "cockroach_asyncpg",
        "cockroach_psycopg_async",
    }

    for driver in async_raw_drivers:
        for scenario in module.CORE_SCENARIOS:
            workload = module.SCENARIO_REGISTRY[("raw", driver, scenario)]
            assert inspect.iscoroutinefunction(workload)
