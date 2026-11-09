"""Performance regression gate script.

Runs SQLite core benchmarks plus the declared-parameter scenario pair via the
bench.py infrastructure, evaluates raw-versus-SQLSpec overhead budgets, and
fails closed (exits 1) on any over-budget row, missing or duplicate required
tier row, benchmark execution error, nonfinite/nonpositive/boolean median,
invalid noise statistic, nonfinite threshold, noise ratio above
``--max-noise-ratio``, or uncompiled build under ``--require-compiled``.

Thresholds define the maximum acceptable overhead (%) of sqlspec vs raw driver
(equality passes; noise never waives a failure):
- iterative_inserts: <= 100% overhead
- repeated_queries: <= 5% overhead
- write_heavy: <= 10% overhead
- read_heavy: <= 12% overhead
- declared_parameters: <= 15% overhead

Run with::

    uv run python tools/scripts/bench_gate.py
    uv run python tools/scripts/bench_gate.py --rows 5000 --iterations 5
    uv run python tools/scripts/bench_gate.py --threshold-declared 15 --max-noise-ratio 0.10 --require-compiled
"""

import importlib.util
import json
import math
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import click
from rich.console import Console
from rich.table import Table

__all__ = (
    "BENCHMARK_SCENARIO_MATRIX",
    "CHAPTER_ROLLOUT_ORDER",
    "DEFAULT_THRESHOLDS",
    "GATE_SCENARIOS",
    "MODULE_ADMISSION_CRITERIA",
    "main",
    "print_gate_table",
    "run_gate",
)

_bench_path = Path(__file__).parent / "bench.py"
_spec = importlib.util.spec_from_file_location("bench", _bench_path)
assert _spec is not None
assert _spec.loader is not None
bench_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_mod)

DEFAULT_THRESHOLDS: dict[str, float] = {
    "iterative_inserts": 100.0,
    "repeated_queries": 5.0,
    "write_heavy": 10.0,
    "read_heavy": 12.0,
    "declared_parameters": 15.0,
}

GATE_SCENARIOS = ["iterative_inserts", "repeated_queries", "write_heavy", "read_heavy", "declared_parameters"]

BENCHMARK_SCENARIO_MATRIX: dict[str, dict[str, str | tuple[str, ...]]] = {
    "parameter_pipeline": {
        "tracked_by": "tools/scripts/bench_subsystems.py + tools/scripts/bench.py",
        "goal": "Placeholder conversion, parameter preparation, and execute-many shaping",
        "scenarios": (
            "prepare_driver_parameters (tuple)",
            "prepare_driver_parameters (dict)",
            "_driver_parameters (3 params)",
            "_check_declared_parameters (2 declared)",
            "_check_declared_parameters (none declared)",
            "complex_parameters",
            "declared_parameters",
        ),
    },
    "coercion_engine": {
        "tracked_by": "tools/scripts/bench.py",
        "goal": "Schema mapping, key transformation, and JSON-heavy coercion paths",
        "scenarios": ("schema_mapping", "dict_key_transform", "complex_parameters"),
    },
    "adapter_runtime_boundaries": {
        "tracked_by": "tools/scripts/bench.py + tools/scripts/bench_subsystems.py",
        "goal": "Startup/runtime setup and end-to-end thin execution path overhead",
        "scenarios": ("initialization", "session.execute() - full path"),
    },
    "storage_runtime_expansion": {
        "tracked_by": "tools/scripts/bench.py + tools/scripts/bench_subsystems.py",
        "goal": "Storage registry/runtime write overhead and JSONL-to-Arrow boundary crossings",
        "scenarios": (
            "write_heavy",
            "read_heavy",
            "StorageRegistry.get() - cached alias",
            "SyncStoragePipeline.write_rows() - local jsonl",
            "_decode_arrow_payload() - jsonl",
        ),
    },
    "exclusion_revalidation": {
        "tracked_by": "tools/scripts/bench.py",
        "goal": "Regression proof that narrowing exclusions does not harm hot-path throughput",
        "scenarios": ("thin_path_stress", "repeated_queries"),
    },
}

MODULE_ADMISSION_CRITERIA: dict[str, str] = {
    "benchmark_delta": "measurable or neutral",
    "mypy_mypyc": "must compile cleanly",
    "segfaults": "no new crashes or segfaults",
    "any_boundaries": "explicitly justified",
    "unsafe_surfaces": "keep Arrow/metaclass-heavy paths interpreted",
}

CHAPTER_ROLLOUT_ORDER: tuple[str, ...] = (
    "compile-boundary-guardrails",
    "compiled-parameter-pipeline",
    "compiled-coercion-engine",
    "adapter-runtime-boundaries",
    "storage-runtime-expansion",
    "exclusion-revalidation",
)


def run_gate(
    *, rows: int, iterations: int, warmup: int, thresholds: dict[str, float], max_noise_ratio: float | None = None
) -> tuple[list[dict[str, Any]], bool]:
    """Run benchmark gate scenarios and check thresholds.

    Uses the bench.py run_benchmark infrastructure to execute core scenarios
    plus the declared_parameters scenario pair, then evaluates raw vs sqlspec
    medians and noise statistics for all gated scenarios.

    Args:
        rows: Number of rows for each scenario.
        iterations: Number of timed iterations per scenario.
        warmup: Number of warmup iterations (not timed).
        thresholds: Dictionary mapping scenario name to max overhead %.
        max_noise_ratio: Optional maximum allowed noise_ratio per gated tier.

    Returns:
        Tuple of (results list, all_passed boolean).
    """
    cast("Any", bench_mod).ROWS_TO_INSERT = rows

    errors: list[str] = []
    bench_results = bench_mod.run_benchmark("sqlite", errors, iterations=iterations, warmup=warmup)
    declared_results = _run_gate_scenario("declared_parameters", errors, iterations=iterations, warmup=warmup)
    measured_rows = [*bench_results, *declared_results]

    gate_results = [
        _evaluate_scenario(
            scenario,
            measured_rows,
            thresholds.get(scenario, DEFAULT_THRESHOLDS[scenario]),
            max_noise_ratio=max_noise_ratio,
        )
        for scenario in GATE_SCENARIOS
    ]
    all_passed = all(row["passed"] for row in gate_results) and not errors

    if errors:
        for err in errors:
            click.secho(f"  Bench error: {err}", fg="yellow")

    return gate_results, all_passed


def print_gate_table(results: list[dict[str, Any]]) -> None:
    """Print a rich-formatted gate results table.

    Args:
        results: List of gate result dictionaries.
    """
    console = Console()

    table = Table(title="Performance Regression Gate Results (sqlite)")
    table.add_column("Scenario", style="cyan", no_wrap=True)
    table.add_column("Raw (s)", justify="right", style="yellow")
    table.add_column("sqlspec (s)", justify="right", style="yellow")
    table.add_column("Overhead %", justify="right")
    table.add_column("Threshold %", justify="right", style="dim")
    table.add_column("Noise", justify="center")
    table.add_column("Status", justify="center")

    for row in results:
        scenario = row["scenario"]
        raw_t = row["raw_time"]
        sqlspec_t = row["sqlspec_time"]
        overhead = row["overhead_pct"]
        threshold = row["threshold_pct"]
        noisy = row.get("noisy")
        passed = row["passed"]

        raw_str = f"{raw_t:.4f}" if raw_t is not None else "ERROR"
        sqlspec_str = f"{sqlspec_t:.4f}" if sqlspec_t is not None else "ERROR"

        if overhead is not None:
            overhead_style = "green" if overhead <= threshold else "red"
            overhead_str = f"[{overhead_style}]{overhead:+.1f}%[/{overhead_style}]"
        else:
            overhead_str = "[red]ERROR[/red]"

        threshold_str = f"<= {threshold:g}%"
        noise_str = "[red]ERROR[/red]" if noisy is None else ("yes" if noisy else "")
        status_str = "[bold green]PASS[/bold green]" if passed else "[bold red]FAIL[/bold red]"

        table.add_row(scenario, raw_str, sqlspec_str, overhead_str, threshold_str, noise_str, status_str)

    console.print(table)


@click.command()
@click.option("--rows", default=10_000, show_default=True, help="Number of rows per scenario")
@click.option(
    "--iterations",
    default=bench_mod.DEFAULT_BENCH_ITERATIONS,
    show_default=True,
    help="Number of timed iterations per scenario",
)
@click.option(
    "--warmup",
    default=bench_mod.DEFAULT_BENCH_WARMUP,
    show_default=True,
    help="Number of warmup iterations (not timed)",
)
@click.option("--json-output", default=None, type=click.Path(), help="Write gate results to a JSON file")
@click.option(
    "--include-subsystems/--no-include-subsystems",
    default=False,
    show_default=True,
    help="Also run and display subsystem and stub-driver micro-benchmarks (report-only)",
)
@click.option(
    "--threshold-iterative",
    "threshold_iterative",
    default=DEFAULT_THRESHOLDS["iterative_inserts"],
    show_default=True,
    help="Max overhead % for iterative_inserts",
)
@click.option(
    "--threshold-repeated",
    "threshold_repeated",
    default=DEFAULT_THRESHOLDS["repeated_queries"],
    show_default=True,
    help="Max overhead % for repeated_queries",
)
@click.option(
    "--threshold-write",
    "threshold_write",
    default=DEFAULT_THRESHOLDS["write_heavy"],
    show_default=True,
    help="Max overhead % for write_heavy",
)
@click.option(
    "--threshold-read",
    "threshold_read",
    default=DEFAULT_THRESHOLDS["read_heavy"],
    show_default=True,
    help="Max overhead % for read_heavy",
)
@click.option(
    "--threshold-declared",
    "threshold_declared",
    default=DEFAULT_THRESHOLDS["declared_parameters"],
    show_default=True,
    help="Max overhead % for declared_parameters",
)
@click.option(
    "--max-noise-ratio",
    type=float,
    default=None,
    help="Fail any gated row whose raw or SQLSpec noise_ratio exceeds this value",
)
@click.option(
    "--require-compiled", is_flag=True, default=False, help="Refuse to run unless sqlspec is a mypyc-compiled build"
)
def main(
    rows: int,
    iterations: int,
    warmup: int,
    include_subsystems: bool,
    threshold_iterative: float,
    threshold_repeated: float,
    threshold_write: float,
    threshold_read: float,
    threshold_declared: float,
    max_noise_ratio: float | None,
    require_compiled: bool,
    json_output: str | None,
) -> None:
    """Run performance regression gate.

    Executes core benchmark scenarios plus declared_parameters (sqlite) and
    checks against configured overhead thresholds. Exits with code 0 if all
    pass, non-zero if any threshold or gate invariant fails.
    """
    thresholds = {
        "iterative_inserts": threshold_iterative,
        "repeated_queries": threshold_repeated,
        "write_heavy": threshold_write,
        "read_heavy": threshold_read,
        "declared_parameters": threshold_declared,
    }
    for scenario_name, threshold_val in thresholds.items():
        if not math.isfinite(threshold_val):
            msg = f"Threshold for {scenario_name} must be finite, got {threshold_val!r}."
            raise click.ClickException(msg)

    if max_noise_ratio is not None and (not math.isfinite(max_noise_ratio) or max_noise_ratio < 0.0):
        msg = f"--max-noise-ratio must be a finite non-negative number, got {max_noise_ratio!r}."
        raise click.ClickException(msg)

    if require_compiled and not bench_mod._is_compiled():
        msg = "--require-compiled set, but sqlspec is not mypyc-compiled."
        raise click.ClickException(msg)

    click.echo(f"Running performance gate (rows={rows}, iterations={iterations}, warmup={warmup})")
    click.echo(f"Thresholds: {thresholds}")
    if bench_mod._is_compiled():
        click.secho("mypyc compilation detected", fg="green")
    click.echo()

    results, all_passed = run_gate(
        rows=rows, iterations=iterations, warmup=warmup, thresholds=thresholds, max_noise_ratio=max_noise_ratio
    )

    print_gate_table(results)
    if include_subsystems:
        _run_report_only_subsystems(iterations=max(100, rows // 10), warmup=max(5, warmup))

    if json_output is not None:
        _write_json_results(
            results,
            json_output,
            rows=rows,
            iterations=iterations,
            warmup=warmup,
            thresholds=thresholds,
            all_passed=all_passed,
            max_noise_ratio=max_noise_ratio,
            require_compiled=require_compiled,
        )
        click.secho(f"\nGate report written to {json_output}", fg="green")

    if all_passed:
        click.secho("\nAll scenarios within threshold. Gate PASSED.", fg="green")
        sys.exit(0)

    failed = [r["scenario"] for r in results if not r["passed"]]
    if failed:
        click.secho(f"\nGate FAILED. Scenarios exceeding threshold or failing gate: {', '.join(failed)}", fg="red")
    else:
        click.secho("\nGate FAILED due to benchmark execution errors.", fg="red")
    sys.exit(1)


def _run_gate_scenario(scenario: str, errors: list[str], *, iterations: int, warmup: int) -> list[dict[str, Any]]:
    """Run the raw and sqlspec registry entries for one sqlite scenario, shaped like bench run_benchmark rows."""
    results: list[dict[str, Any]] = []
    for lib in ("raw", "sqlspec"):
        func = bench_mod.SCENARIO_REGISTRY.get((lib, "sqlite", scenario))
        if func is None:
            errors.append(f"No implementation for library={lib}, driver=sqlite, scenario={scenario}")
            continue
        try:
            times = bench_mod._run_benchmark_iterations(func, is_async=False, iterations=iterations, warmup=warmup)
            stats = bench_mod._summarize_times(times)
            results.append({
                "driver": "sqlite",
                "library": bench_mod._benchmark_library_label(lib),
                "library_key": lib,
                "scenario": scenario,
                "times": times,
                **stats,
            })
        except bench_mod.BenchmarkUnavailableError:
            continue
        except Exception as exc:
            errors.append(f"{lib}/sqlite/{scenario}: {exc}")
    return results


def _evaluate_scenario(
    scenario: str, rows: list[dict[str, Any]], threshold: float, max_noise_ratio: float | None = None
) -> dict[str, Any]:
    """Build one gate row from every measured row, failing closed on invalid input."""
    raw_row, raw_err = _select_tier(rows, scenario, "raw")
    sqlspec_row, sqlspec_err = _select_tier(rows, scenario, "sqlspec")
    noise_dict, noise_err = _noise_fields(raw_row, sqlspec_row)

    raw_time_diag = _finite_or_none(raw_row.get("time")) if raw_row is not None else None
    sqlspec_time_diag = _finite_or_none(sqlspec_row.get("time")) if sqlspec_row is not None else None

    eval_errors: list[str] = []
    raw_median_ok = False
    sqlspec_median_ok = False

    if raw_err is not None:
        eval_errors.append(raw_err)
    elif raw_row is not None:
        if _is_valid_median(raw_row.get("time")):
            raw_median_ok = True
        else:
            eval_errors.append(f"invalid raw median for {scenario}: {raw_row.get('time')!r}")

    if sqlspec_err is not None:
        eval_errors.append(sqlspec_err)
    elif sqlspec_row is not None:
        if _is_valid_median(sqlspec_row.get("time")):
            sqlspec_median_ok = True
        else:
            eval_errors.append(f"invalid sqlspec median for {scenario}: {sqlspec_row.get('time')!r}")

    if noise_err is not None:
        eval_errors.append(noise_err)

    overhead_pct: float | None = None
    if raw_row is not None and sqlspec_row is not None and raw_median_ok and sqlspec_median_ok:
        raw_t = float(raw_row["time"])
        sqlspec_t = float(sqlspec_row["time"])
        overhead_pct = (sqlspec_t - raw_t) * 100.0 / raw_t

    if max_noise_ratio is not None:
        raw_nr = noise_dict["raw_noise_ratio"]
        if isinstance(raw_nr, float) and raw_nr > max_noise_ratio:
            eval_errors.append(f"raw noise_ratio {raw_nr:.4f} exceeds max_noise_ratio {max_noise_ratio:.4f}")
        sqlspec_nr = noise_dict["sqlspec_noise_ratio"]
        if isinstance(sqlspec_nr, float) and sqlspec_nr > max_noise_ratio:
            eval_errors.append(f"sqlspec noise_ratio {sqlspec_nr:.4f} exceeds max_noise_ratio {max_noise_ratio:.4f}")

    error = "; ".join(eval_errors) if eval_errors else None
    passed = error is None and overhead_pct is not None and overhead_pct <= threshold

    return {
        "scenario": scenario,
        "raw_time": raw_time_diag,
        "sqlspec_time": sqlspec_time_diag,
        "overhead_pct": overhead_pct,
        "threshold_pct": threshold,
        "passed": passed,
        **noise_dict,
        "error": error,
    }


def _select_tier(
    rows: list[dict[str, Any]], scenario: str, library_key: str
) -> tuple[dict[str, Any] | None, str | None]:
    """Return the single (scenario, library_key) row, or None with a missing or duplicate error."""
    matches = [row for row in rows if row.get("scenario") == scenario and row.get("library_key") == library_key]
    if not matches:
        return None, f"missing {library_key} row for {scenario}"
    if len(matches) > 1:
        return None, f"duplicate {library_key} rows ({len(matches)}) for {scenario}"
    return matches[0], None


def _is_valid_median(value: object) -> bool:
    """Return True for a finite positive int or float, excluding bool."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    return math.isfinite(value) and value > 0.0


def _noise_fields(
    raw: dict[str, Any] | None, sqlspec: dict[str, Any] | None
) -> tuple[dict[str, bool | float | None], str | None]:
    """Return per-tier and aggregate noise fields, or an error for an invalid noise statistic."""
    errors: list[str] = []

    raw_noisy: bool | None = None
    raw_ratio: float | None = None
    raw_valid = False
    if raw is not None:
        r_noisy = raw.get("noisy")
        r_ratio = _finite_or_none(raw.get("noise_ratio"))
        if isinstance(r_noisy, bool) and r_ratio is not None and r_ratio >= 0.0:
            raw_noisy = r_noisy
            raw_ratio = r_ratio
            raw_valid = True
        else:
            raw_noisy = r_noisy if isinstance(r_noisy, bool) else None
            raw_ratio = r_ratio if r_ratio is not None and r_ratio >= 0.0 else None
            errors.append("invalid raw noise statistics")

    sqlspec_noisy: bool | None = None
    sqlspec_ratio: float | None = None
    sqlspec_valid = False
    if sqlspec is not None:
        s_noisy = sqlspec.get("noisy")
        s_ratio = _finite_or_none(sqlspec.get("noise_ratio"))
        if isinstance(s_noisy, bool) and s_ratio is not None and s_ratio >= 0.0:
            sqlspec_noisy = s_noisy
            sqlspec_ratio = s_ratio
            sqlspec_valid = True
        else:
            sqlspec_noisy = s_noisy if isinstance(s_noisy, bool) else None
            sqlspec_ratio = s_ratio if s_ratio is not None and s_ratio >= 0.0 else None
            errors.append("invalid sqlspec noise statistics")

    if (
        raw_valid
        and sqlspec_valid
        and raw_noisy is not None
        and sqlspec_noisy is not None
        and raw_ratio is not None
        and sqlspec_ratio is not None
    ):
        agg_noisy: bool | None = raw_noisy or sqlspec_noisy
        agg_ratio: float | None = max(raw_ratio, sqlspec_ratio)
    else:
        agg_noisy = None
        agg_ratio = None

    fields: dict[str, bool | float | None] = {
        "raw_noisy": raw_noisy,
        "raw_noise_ratio": raw_ratio,
        "sqlspec_noisy": sqlspec_noisy,
        "sqlspec_noise_ratio": sqlspec_ratio,
        "noisy": agg_noisy,
        "noise_ratio": agg_ratio,
    }
    return fields, ("; ".join(errors) if errors else None)


def _finite_or_none(value: object) -> float | None:
    """Return a finite numeric diagnostic, otherwise None so JSON stays standards-valid."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) else None


def _run_report_only_subsystems(*, iterations: int, warmup: int) -> None:
    """Run subsystem micro-benchmarks in report-only mode."""
    from tools.scripts.bench_subsystems import _make_temp_db, _setup_test_table, print_results_table, run_benchmarks

    db_path = _make_temp_db()
    try:
        _setup_test_table(db_path)
        click.echo("\nRunning report-only subsystem micro-benchmarks...")
        sub_results = run_benchmarks(db_path, iterations=iterations, warmup=warmup, profile=False)
        print_results_table(sub_results)
    finally:
        with suppress(OSError):
            db_path.unlink()


def _write_json_results(
    results: list[dict[str, Any]],
    output_path: str | Path,
    *,
    rows: int,
    iterations: int,
    warmup: int,
    thresholds: dict[str, float],
    all_passed: bool,
    max_noise_ratio: float | None = None,
    require_compiled: bool = False,
) -> None:
    """Write a performance gate report to JSON."""
    output = {
        "metadata": {
            "rows": rows,
            "iterations": iterations,
            "warmup": warmup,
            "mypyc_compiled": bench_mod._is_compiled(),
            "max_noise_ratio": max_noise_ratio,
            "require_compiled": require_compiled,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        },
        "thresholds": thresholds,
        "all_passed": all_passed,
        "results": results,
    }
    Path(output_path).write_text(f"{json.dumps(output, indent=2, sort_keys=True, allow_nan=False)}\n", encoding="utf-8")


if __name__ == "__main__":
    main()
