"""Performance regression gate script.

Runs core benchmarks via the bench.py infrastructure, compares against
configured thresholds, and reports threshold failures by default. Pass
``--fail-on-regression`` to make threshold failures exit non-zero.

Thresholds define the maximum acceptable overhead (%) of sqlspec vs raw driver:
- iterative_inserts: < 100% overhead
- repeated_queries: < 5% overhead
- write_heavy: < 10% overhead
- read_heavy: < 12% overhead

Run with::

    uv run python tools/scripts/bench_gate.py
    uv run python tools/scripts/bench_gate.py --rows 5000 --iterations 5
    uv run python tools/scripts/bench_gate.py --threshold-iterative 50  # stricter
"""

import importlib.util
import json
import sys
import time
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
    "THRESHOLD_OWNERSHIP",
    "main",
    "print_gate_table",
    "run_gate",
)


# Import bench.py from the same directory, regardless of working directory
_bench_path = Path(__file__).parent / "bench.py"
_spec = importlib.util.spec_from_file_location("bench", _bench_path)
assert _spec is not None
assert _spec.loader is not None
bench_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_mod)

# ---------------------------------------------------------------------------
# Default overhead thresholds (percentage of raw time)
# ---------------------------------------------------------------------------

DEFAULT_THRESHOLDS: dict[str, float] = {
    "iterative_inserts": 100.0,
    "repeated_queries": 5.0,
    "write_heavy": 10.0,
    "read_heavy": 12.0,
}

# Core scenarios to gate on
GATE_SCENARIOS = ["iterative_inserts", "repeated_queries", "write_heavy", "read_heavy"]

THRESHOLD_OWNERSHIP: dict[str, str] = {
    "owner": "SQLSpec maintainers",
    "variance_policy": (
        "PR and release smoke runs are advisory; scheduled artifacts own threshold changes after noisy runs are repeated."
    ),
    "shared_core_attribution": (
        "Regressions across SQLite, DuckDB, ADBC SQLite, and multiple SQLSpec libraries point to shared-core paths."
    ),
    "driver_local_attribution": (
        "Regressions isolated to one adapter family belong to that adapter's bind, cursor, pool, or dialect layer."
    ),
}

# PRD benchmark matrix for mypyc expansion work. This keeps the benchmark
# expectations next to the scripts that actually exercise them.
BENCHMARK_SCENARIO_MATRIX: dict[str, dict[str, str | tuple[str, ...]]] = {
    "parameter_pipeline": {
        "tracked_by": "tools/scripts/bench_subsystems.py + tools/scripts/bench.py",
        "goal": "Placeholder conversion, parameter preparation, and execute-many shaping",
        "scenarios": (
            "prepare_driver_parameters (tuple)",
            "prepare_driver_parameters (dict)",
            "_format_parameter_set (3 params)",
            "complex_parameters",
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
    *, driver: str, rows: int, iterations: int, warmup: int, thresholds: dict[str, float]
) -> tuple[list[dict[str, Any]], bool]:
    """Run benchmark gate scenarios and check thresholds.

    Uses the bench.py run_benchmark infrastructure to execute scenarios,
    then extracts raw vs sqlspec times for the gated scenarios.

    Args:
        driver: Database driver to benchmark.
        rows: Number of rows for each scenario.
        iterations: Number of timed iterations per scenario.
        warmup: Number of warmup iterations (not timed).
        thresholds: Dictionary mapping scenario name to max overhead %.

    Returns:
        Tuple of (results list, all_passed boolean).
    """
    errors: list[str] = []
    benchmark_module = cast("Any", bench_mod)
    original_rows = benchmark_module.ROWS_TO_INSERT
    benchmark_module.ROWS_TO_INSERT = rows
    try:
        # Run benchmarks using the same infrastructure as bench.py
        bench_results = bench_mod.run_benchmark(driver, errors, iterations=iterations, warmup=warmup)
    finally:
        benchmark_module.ROWS_TO_INSERT = original_rows

    # Build lookup: (library, scenario) -> median time
    time_lookup: dict[tuple[str, str], float] = {}
    for result in bench_results:
        key = (result["library"], result["scenario"])
        time_lookup[key] = result["time"]

    # Determine sqlspec label (may include "(mypyc)")
    sqlspec_label = bench_mod.SQLSPEC_LABEL

    gate_results: list[dict[str, Any]] = []
    all_passed = True

    for scenario in GATE_SCENARIOS:
        threshold = thresholds.get(scenario, 100.0)

        raw_time = time_lookup.get(("raw", scenario))
        sqlspec_time = time_lookup.get((sqlspec_label, scenario))

        if raw_time is None or sqlspec_time is None:
            gate_results.append({
                "driver": driver,
                "scenario": scenario,
                "raw_time": raw_time,
                "sqlspec_time": sqlspec_time,
                "overhead_pct": None,
                "threshold_pct": threshold,
                "passed": False,
            })
            all_passed = False
            continue

        overhead_pct = (sqlspec_time - raw_time) / raw_time * 100.0 if raw_time > 0 else 0.0

        passed = overhead_pct <= threshold

        if not passed:
            all_passed = False

        gate_results.append({
            "driver": driver,
            "scenario": scenario,
            "raw_time": raw_time,
            "sqlspec_time": sqlspec_time,
            "overhead_pct": overhead_pct,
            "threshold_pct": threshold,
            "passed": passed,
        })

    if errors:
        for err in errors:
            click.secho(f"  Bench error: {err}", fg="yellow")

    return gate_results, all_passed


def print_gate_table(results: list[dict[str, Any]], *, driver: str = "sqlite") -> None:
    """Print a rich-formatted gate results table.

    Args:
        results: List of gate result dictionaries.
    """
    console = Console()

    table = Table(title=f"Performance Regression Gate Results ({driver})")
    table.add_column("Scenario", style="cyan", no_wrap=True)
    table.add_column("Raw (s)", justify="right", style="yellow")
    table.add_column("sqlspec (s)", justify="right", style="yellow")
    table.add_column("Overhead %", justify="right")
    table.add_column("Threshold %", justify="right", style="dim")
    table.add_column("Status", justify="center")

    for row in results:
        scenario = row["scenario"]
        raw_t = row["raw_time"]
        sqlspec_t = row["sqlspec_time"]
        overhead = row["overhead_pct"]
        threshold = row["threshold_pct"]
        passed = row["passed"]

        raw_str = f"{raw_t:.4f}" if raw_t is not None else "ERROR"
        sqlspec_str = f"{sqlspec_t:.4f}" if sqlspec_t is not None else "ERROR"

        if overhead is not None:
            overhead_style = "green" if overhead <= threshold else "red"
            overhead_str = f"[{overhead_style}]{overhead:+.1f}%[/{overhead_style}]"
        else:
            overhead_str = "[red]ERROR[/red]"

        threshold_str = f"<= {threshold:.0f}%"

        status_str = "[bold green]PASS[/bold green]" if passed else "[bold red]FAIL[/bold red]"

        table.add_row(scenario, raw_str, sqlspec_str, overhead_str, threshold_str, status_str)

    console.print(table)


def _write_json_results(
    results: list[dict[str, Any]],
    output_path: str | Path,
    *,
    rows: int,
    iterations: int,
    warmup: int,
    thresholds: dict[str, float],
    all_passed: bool,
) -> None:
    """Write a performance gate report to JSON."""
    output = {
        "metadata": {
            "driver": results[0].get("driver") if results else None,
            "rows": rows,
            "iterations": iterations,
            "warmup": warmup,
            "mypyc_compiled": bench_mod._is_compiled(),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        },
        "threshold_ownership": THRESHOLD_OWNERSHIP,
        "thresholds": thresholds,
        "all_passed": all_passed,
        "results": results,
    }
    Path(output_path).write_text(json.dumps(output, indent=2, sort_keys=True))


@click.command()
@click.option("--driver", default="sqlite", show_default=True, help="Driver name to benchmark")
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
@click.option("--fail-on-regression", is_flag=True, default=False, help="Exit non-zero when thresholds fail")
def main(
    driver: str,
    rows: int,
    iterations: int,
    warmup: int,
    threshold_iterative: float,
    threshold_repeated: float,
    threshold_write: float,
    threshold_read: float,
    fail_on_regression: bool,
    json_output: str | None,
) -> None:
    """Run performance regression gate.

    Executes core benchmark scenarios for the selected driver and checks against
    configured overhead thresholds. Threshold failures are report-only by
    default and exit non-zero only with ``--fail-on-regression``.
    """
    thresholds = {
        "iterative_inserts": threshold_iterative,
        "repeated_queries": threshold_repeated,
        "write_heavy": threshold_write,
        "read_heavy": threshold_read,
    }

    click.echo(f"Running performance gate (driver={driver}, rows={rows}, iterations={iterations}, warmup={warmup})")
    click.echo(f"Thresholds: {thresholds}")
    if bench_mod._is_compiled():
        click.secho("mypyc compilation detected", fg="green")
    click.echo()

    results, all_passed = run_gate(
        driver=driver, rows=rows, iterations=iterations, warmup=warmup, thresholds=thresholds
    )

    print_gate_table(results, driver=driver)
    if json_output is not None:
        _write_json_results(
            results,
            json_output,
            rows=rows,
            iterations=iterations,
            warmup=warmup,
            thresholds=thresholds,
            all_passed=all_passed,
        )
        click.secho(f"\nGate report written to {json_output}", fg="green")

    if all_passed:
        click.secho("\nAll scenarios within threshold. Gate PASSED.", fg="green")
        sys.exit(0)
    failed = [r["scenario"] for r in results if not r["passed"]]
    if fail_on_regression:
        click.secho(f"\nGate FAILED. Scenarios exceeding threshold: {', '.join(failed)}", fg="red")
        sys.exit(1)
    click.secho(f"\nPerformance regression report found threshold failures: {', '.join(failed)}", fg="yellow")
    sys.exit(0)


if __name__ == "__main__":
    main()
