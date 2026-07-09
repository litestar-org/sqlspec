"""Tests for benchmark runner helpers."""

import asyncio
import importlib.util
from pathlib import Path
from types import ModuleType


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
    assert module.POOL_SIZE == 1


def test_bench_summary_reports_stddev_iqr_and_noise() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    summary = module._summarize_times([1.0, 1.0, 1.1, 1.2])

    assert summary["time"] == 1.05
    assert summary["stddev"] > 0
    assert summary["iqr"] > 0
    assert summary["noise_ratio"] > 0
    assert summary["noisy"] is False


def test_schema_type_numpy_benchmark_scenario_is_registered_and_runnable() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    assert module.SCENARIO_REGISTRY[("raw", "sqlite", "schema_type_numpy")] is module.raw_sqlite_schema_type_numpy
    assert module.SCENARIO_REGISTRY[("sqlspec", "sqlite", "schema_type_numpy")] is (
        module.sqlspec_sqlite_schema_type_numpy
    )

    module.raw_sqlite_schema_type_numpy()
    module.sqlspec_sqlite_schema_type_numpy()
    module.assert_schema_type_numpy_vector_fallback()


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


def test_extended_benchmark_uses_only_registered_driver_scenarios() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    errors: list[str] = []
    results = module.run_extended_benchmark("unsupported", errors, iterations=1, warmup=0)

    assert results == []
    assert errors == []


def test_unconfigured_optional_adapter_is_absent_from_results(monkeypatch) -> None:
    module = _load_script_module("bench.py", "bench_for_tests")
    for name in (
        "SQLSPEC_BENCH_ADBC_URI",
        "SQLSPEC_BENCH_ADBC_DRIVER_NAME",
        "SQLSPEC_BENCH_MYSQL_HOST",
        "SQLSPEC_BENCH_MYSQL_PORT",
        "SQLSPEC_BENCH_MYSQL_USER",
        "SQLSPEC_BENCH_MYSQL_PASSWORD",
        "SQLSPEC_BENCH_MYSQL_DATABASE",
        "SQLSPEC_BENCH_SPANNER_PROJECT",
        "SQLSPEC_BENCH_SPANNER_INSTANCE_ID",
        "SQLSPEC_BENCH_SPANNER_DATABASE_ID",
        "SQLSPEC_BENCH_SPANNER_API_ENDPOINT",
        "SQLSPEC_BENCH_POSTGRES_DSN",
        "SQLSPEC_BENCH_COCKROACH_DSN",
    ):
        monkeypatch.delenv(name, raising=False)

    for driver in (
        "adbc",
        "mysqlconnector",
        "spanner",
        "psycopg_sync",
        "psycopg_async",
        "asyncpg",
        "cockroach_psycopg_sync",
        "cockroach_psycopg_async",
        "cockroach_asyncpg",
    ):
        errors: list[str] = []
        assert module.run_extended_benchmark(driver, errors, iterations=1, warmup=0) == []
        assert errors == []


def test_cross_adapter_benchmark_scenarios_are_registered() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    expected = {
        ("raw", "spanner", "strings"),
        ("sqlspec", "spanner", "strings"),
        ("raw", "mysqlconnector", "json_rows"),
        ("sqlspec", "mysqlconnector", "json_rows"),
        ("raw", "adbc", "rows"),
        ("sqlspec", "adbc", "rows"),
        ("raw", "duckdb", "bulk"),
        ("sqlspec", "duckdb", "bulk"),
        ("raw", "psycopg_sync", "rows"),
        ("sqlspec", "psycopg_sync", "rows"),
        ("raw", "psycopg_async", "rows"),
        ("sqlspec", "psycopg_async", "rows"),
        ("raw", "asyncpg", "rows"),
        ("sqlspec", "asyncpg", "rows"),
        ("raw", "cockroach_psycopg_sync", "rows"),
        ("sqlspec", "cockroach_psycopg_sync", "rows"),
        ("raw", "cockroach_psycopg_async", "rows"),
        ("sqlspec", "cockroach_psycopg_async", "rows"),
        ("raw", "cockroach_asyncpg", "rows"),
        ("sqlspec", "cockroach_asyncpg", "rows"),
    }

    assert expected <= module.SCENARIO_REGISTRY.keys()
    assert module.EXTENDED_SCENARIOS_BY_DRIVER["spanner"] == (("raw", "strings"), ("sqlspec", "strings"))
    assert module.EXTENDED_SCENARIOS_BY_DRIVER["mysqlconnector"] == (("raw", "json_rows"), ("sqlspec", "json_rows"))
    assert module.EXTENDED_SCENARIOS_BY_DRIVER["adbc"] == (("raw", "rows"), ("sqlspec", "rows"))
    assert module.EXTENDED_SCENARIOS_BY_DRIVER["duckdb"] == (("raw", "bulk"), ("sqlspec", "bulk"))
    for driver in (
        "psycopg_sync",
        "psycopg_async",
        "asyncpg",
        "cockroach_psycopg_sync",
        "cockroach_psycopg_async",
        "cockroach_asyncpg",
    ):
        assert module.EXTENDED_SCENARIOS_BY_DRIVER[driver] == (("raw", "rows"), ("sqlspec", "rows"))


def test_duckdb_bulk_benchmark_scenarios_run() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")

    module.raw_duckdb_bulk()
    module.sqlspec_duckdb_bulk()


def test_benchmark_results_keep_stable_library_keys() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")
    output = Path("/tmp/bench-stable-key-test.json")
    try:
        module._write_json_results(
            [
                {
                    "driver": "sqlite",
                    "library": "sqlspec (mypyc)",
                    "library_key": "sqlspec",
                    "scenario": "read_heavy",
                    "time": 1.0,
                }
            ],
            str(output),
            rows=1,
            pool_size=1,
            iterations=1,
        )
        payload = output.read_text()
    finally:
        output.unlink(missing_ok=True)

    assert payload.count('"library_key": "sqlspec"') == 1


def test_bench_compare_uses_library_key_and_enforces_required_scenarios() -> None:
    module = _load_script_module("bench_compare.py", "bench_compare_for_tests")

    baseline = {
        "results": [
            {
                "driver": "sqlite",
                "library": "sqlspec (mypyc)",
                "library_key": "sqlspec",
                "scenario": "read_heavy",
                "time": 1.0,
            }
        ]
    }
    current = {
        "results": [
            {"driver": "sqlite", "library": "sqlspec", "library_key": "sqlspec", "scenario": "read_heavy", "time": 1.0}
        ]
    }

    assert set(module._build_lookup(baseline)) == set(module._build_lookup(current))
    assert module._missing_required_scenarios(current, {("sqlite", "sqlspec", "read_heavy")}) == set()
    assert module._missing_required_scenarios(current, {("sqlite", "sqlspec", "write_heavy")}) == {
        ("sqlite", "sqlspec", "write_heavy")
    }


def test_bench_compare_fails_when_a_baseline_scenario_is_missing(tmp_path: Path) -> None:
    module = _load_script_module("bench_compare.py", "bench_compare_for_tests")
    baseline = tmp_path / "baseline.json"
    current = tmp_path / "current.json"
    baseline.write_text(
        '{"results": [{"driver": "sqlite", "library": "raw", "library_key": "raw", "scenario": "read_heavy", "time": 1.0}]}'
    )
    current.write_text('{"results": []}')

    from click.testing import CliRunner

    result = CliRunner().invoke(module.main, [str(baseline), str(current)])

    assert result.exit_code == 1
    assert "Missing required benchmark scenario" in result.output


def test_adapter_baselines_cover_registered_extended_scenarios() -> None:
    module = _load_script_module("bench.py", "bench_for_tests")
    baseline_dir = Path(__file__).resolve().parents[3] / "tools" / "perf_baselines"

    for driver, entries in module.EXTENDED_SCENARIOS_BY_DRIVER.items():
        if driver == "sqlite" or driver == "oracle":
            continue
        baseline_path = baseline_dir / f"{driver}.json"
        assert baseline_path.exists()
        payload = module.json.loads(baseline_path.read_text())
        identities = {
            (result["driver"], result.get("library_key", result["library"]), result["scenario"])
            for result in payload["results"]
        }
        expected = {(driver, library, scenario) for library, scenario in entries}
        assert expected <= identities
