"""Unit tests for benchmark comparison, metadata parity, and environment collection."""

import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner
from tools.scripts.bench import (
    EXTENDED_SCENARIOS_BY_DRIVER,
    SCENARIO_REGISTRY,
    _collect_environment_metadata,
    _write_json_results,
)
from tools.scripts.bench_compare import compute_comparison_rows, format_run_args, validate_metadata_parity
from tools.scripts.bench_compare import main as bench_compare_main

__all__ = (
    "test_bench_compare_cli_exits_2_on_hard_metadata_mismatch",
    "test_collect_environment_metadata_and_write_json_results",
    "test_compute_comparison_rows_ratio_mode_and_fallback",
    "test_extended_scenarios_registered_for_all_benchmarked_drivers",
    "test_format_run_args_and_cli_print_run_args",
    "test_validate_metadata_parity_detects_hard_and_soft_mismatches",
)


def test_extended_scenarios_registered_for_all_benchmarked_drivers() -> None:
    """All hot-path extended scenarios should be registered for local, Postgres, and Oracle drivers."""
    expected_scenarios = (
        "scalar_param_select",
        "no_param_select",
        "complex_query",
        "session_per_request",
        "observability_select",
    )
    target_drivers = ("sqlite", "aiosqlite", "duckdb", "asyncpg", "psycopg_sync", "psycopg_async", "psqlpy", "oracle")
    for drv in target_drivers:
        assert drv in EXTENDED_SCENARIOS_BY_DRIVER
        driver_scenarios = set(EXTENDED_SCENARIOS_BY_DRIVER[drv])
        for scenario in expected_scenarios:
            assert ("raw", scenario) in driver_scenarios
            assert ("sqlspec", scenario) in driver_scenarios
            assert callable(SCENARIO_REGISTRY["raw", drv, scenario])
            assert callable(SCENARIO_REGISTRY["sqlspec", drv, scenario])
    assert ("raw", "rows") in set(EXTENDED_SCENARIOS_BY_DRIVER["psqlpy"])
    assert ("sqlspec", "rows") in set(EXTENDED_SCENARIOS_BY_DRIVER["psqlpy"])


def test_validate_metadata_parity_detects_hard_and_soft_mismatches() -> None:
    """Hard metadata keys should be reported separately from soft metadata keys."""
    baseline = _sample_payload(rows=1000, pool_size=1, iterations=3, warmup=1, mypyc_compiled=False)
    current = _sample_payload(rows=10000, pool_size=2, iterations=7, warmup=3, mypyc_compiled=True)

    hard_mismatches, soft_warnings = validate_metadata_parity(baseline, current)

    assert len(hard_mismatches) == 3
    assert any("rows:" in item for item in hard_mismatches)
    assert any("pool_size:" in item for item in hard_mismatches)
    assert any("mypyc_compiled:" in item for item in hard_mismatches)
    assert len(soft_warnings) == 2
    assert any("iterations:" in item for item in soft_warnings)
    assert any("warmup:" in item for item in soft_warnings)


def test_bench_compare_cli_exits_2_on_hard_metadata_mismatch(tmp_path: Path) -> None:
    """CLI should exit with status 2 on hard metadata mismatch unless overridden."""
    base_path = tmp_path / "base.json"
    curr_path = tmp_path / "curr.json"
    base_path.write_text(json.dumps(_sample_payload(rows=1000)), encoding="utf-8")
    curr_path.write_text(json.dumps(_sample_payload(rows=10000)), encoding="utf-8")

    runner = CliRunner()
    res = runner.invoke(bench_compare_main, [str(base_path), str(curr_path)])
    assert res.exit_code == 2
    assert "Metadata mismatch" in res.output

    res_allowed = runner.invoke(bench_compare_main, [str(base_path), str(curr_path), "--allow-metadata-mismatch"])
    assert res_allowed.exit_code == 0


def test_format_run_args_and_cli_print_run_args(tmp_path: Path) -> None:
    """format_run_args and --print-run-args should emit matching CLI flags from baseline metadata."""
    payload = _sample_payload(rows=1000, iterations=3, pool_size=1, warmup=2)
    assert format_run_args(payload) == "--rows 1000 --iterations 3 --pool-size 1 --warmup 2"

    base_path = tmp_path / "base.json"
    base_path.write_text(json.dumps(payload), encoding="utf-8")

    runner = CliRunner()
    res = runner.invoke(bench_compare_main, [str(base_path), "--print-run-args"])
    assert res.exit_code == 0
    assert res.output.strip() == "--rows 1000 --iterations 3 --pool-size 1 --warmup 2"


def test_compute_comparison_rows_ratio_mode_and_fallback() -> None:
    """Ratio mode should compare sqlspec/raw ratios and fall back when raw is absent."""
    baseline = _sample_payload(raw_time=1.0, sqlspec_time=1.2)
    current_same_ratio = _sample_payload(raw_time=2.0, sqlspec_time=2.4)

    rows = compute_comparison_rows(baseline, current_same_ratio, mode="ratio", threshold=5.0)
    sqlspec_row = next(r for r in rows if r["library_key"] == "sqlspec")
    assert sqlspec_row["metric_kind"] == "ratio"
    assert abs(sqlspec_row["pct_change"]) < 1e-6
    assert sqlspec_row["status_text"] == "unchanged"

    current_regressed_ratio = _sample_payload(raw_time=1.0, sqlspec_time=1.8)
    reg_rows = compute_comparison_rows(baseline, current_regressed_ratio, mode="ratio", threshold=10.0)
    reg_sqlspec_row = next(r for r in reg_rows if r["library_key"] == "sqlspec")
    assert reg_sqlspec_row["status_text"] == "REGRESSED"
    assert reg_sqlspec_row["pct_change"] > 40.0

    no_raw_base = _sample_payload(sqlspec_time=1.0, include_raw=False)
    no_raw_curr = _sample_payload(sqlspec_time=1.5, include_raw=False)
    fb_rows = compute_comparison_rows(no_raw_base, no_raw_curr, mode="ratio", threshold=10.0)
    assert fb_rows[0]["metric_kind"] == "abs-fallback"
    assert fb_rows[0]["status_text"] == "REGRESSED"


def test_collect_environment_metadata_and_write_json_results(tmp_path: Path) -> None:
    """bench.py should record warmup and environment metadata in JSON output."""
    env_meta = _collect_environment_metadata()
    assert "python_version" in env_meta
    assert "platform" in env_meta
    assert isinstance(env_meta["packages"], dict)
    for pkg in ("duckdb", "sqlglot", "aiosqlite", "pyarrow", "sqlspec"):
        assert pkg in env_meta["packages"]

    out_file = tmp_path / "out.json"
    _write_json_results(
        [
            {
                "driver": "sqlite",
                "library": "sqlspec",
                "library_key": "sqlspec",
                "scenario": "initialization",
                "time": 0.01,
            }
        ],
        str(out_file),
        rows=500,
        pool_size=1,
        iterations=3,
        warmup=2,
    )
    loaded = json.loads(out_file.read_text(encoding="utf-8"))
    assert loaded["metadata"]["warmup"] == 2
    assert "environment" in loaded["metadata"]
    assert "python_version" in loaded["metadata"]["environment"]


def _sample_payload(
    *,
    rows: int = 1000,
    pool_size: int = 1,
    iterations: int = 3,
    warmup: int = 1,
    mypyc_compiled: bool = False,
    raw_time: float = 0.50,
    sqlspec_time: float = 0.55,
    include_raw: bool = True,
) -> dict[str, Any]:
    """Create a synthetic benchmark JSON payload for comparison tests."""
    results: list[dict[str, Any]] = []
    if include_raw:
        results.append({
            "driver": "duckdb",
            "library": "raw",
            "library_key": "raw",
            "scenario": "repeated_queries",
            "time": raw_time,
            "stddev": 0.01,
        })
    results.append({
        "driver": "duckdb",
        "library": "sqlspec",
        "library_key": "sqlspec",
        "scenario": "repeated_queries",
        "time": sqlspec_time,
        "stddev": 0.01,
    })
    return {
        "metadata": {
            "rows": rows,
            "pool_size": pool_size,
            "iterations": iterations,
            "warmup": warmup,
            "mypyc_compiled": mypyc_compiled,
            "timestamp": "2026-10-13T18:00:00+0000",
        },
        "results": results,
    }
