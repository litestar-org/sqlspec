"""Benchmark comparison script for comparing two benchmark runs.

Accepts two JSON result files (baseline and current) produced by
``bench.py --json-output`` and produces a rich table showing per-scenario
comparison with absolute or relative ratio changes and metadata parity checks.

Run with::

    uv run python tools/scripts/bench_compare.py baseline.json current.json
"""

import json
import sys
from pathlib import Path
from typing import Any, cast

import click
from rich.console import Console
from rich.table import Table

BenchmarkKey = tuple[str, str, str]

HARD_METADATA_KEYS: tuple[str, ...] = ("rows", "pool_size", "mypyc_compiled")
SOFT_METADATA_KEYS: tuple[str, ...] = ("iterations", "warmup", "environment")

__all__ = (
    "HARD_METADATA_KEYS",
    "SOFT_METADATA_KEYS",
    "compute_comparison_rows",
    "format_run_args",
    "main",
    "print_comparison_table",
    "validate_metadata_parity",
)


def validate_metadata_parity(baseline: dict[str, Any], current: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Compare run metadata between baseline and current benchmark payloads.

    Args:
        baseline: Parsed baseline benchmark JSON payload.
        current: Parsed current benchmark JSON payload.

    Returns:
        Tuple of (hard_mismatches, soft_warnings).
    """
    base_meta = baseline.get("metadata", {})
    curr_meta = current.get("metadata", {})
    hard_mismatches = [
        f"{key}: baseline={base_meta[key]!r} != current={curr_meta[key]!r}"
        for key in HARD_METADATA_KEYS
        if key in base_meta and key in curr_meta and base_meta[key] != curr_meta[key]
    ]
    soft_warnings = [
        f"{key}: baseline={base_meta[key]!r} != current={curr_meta[key]!r}"
        for key in SOFT_METADATA_KEYS
        if key in base_meta and key in curr_meta and base_meta[key] != curr_meta[key]
    ]
    return hard_mismatches, soft_warnings


def format_run_args(baseline: dict[str, Any]) -> str:
    """Build bench.py CLI flags matching a baseline JSON file's metadata.

    Args:
        baseline: Parsed baseline benchmark JSON payload.

    Returns:
        Space-joined CLI argument string for ``bench.py``.
    """
    meta = baseline.get("metadata", {})
    args: list[str] = []
    if "rows" in meta:
        args.extend(("--rows", str(meta["rows"])))
    if "iterations" in meta:
        args.extend(("--iterations", str(meta["iterations"])))
    if "pool_size" in meta:
        args.extend(("--pool-size", str(meta["pool_size"])))
    if "warmup" in meta:
        args.extend(("--warmup", str(meta["warmup"])))
    return " ".join(args)


def compute_comparison_rows(
    baseline: dict[str, Any], current: dict[str, Any], *, mode: str = "absolute", threshold: float = 5.0
) -> list[dict[str, Any]]:
    """Compute per-scenario comparison rows in absolute or ratio mode.

    Args:
        baseline: Parsed baseline benchmark JSON data.
        current: Parsed current benchmark JSON data.
        mode: Comparison mode (``"absolute"`` or ``"ratio"``).
        threshold: Percentage threshold for flagging changes as significant.

    Returns:
        List of comparison row dictionaries ordered by (driver, library_key, scenario).
    """
    baseline_lookup = _build_lookup(baseline)
    current_lookup = _build_lookup(current)
    all_keys = sorted(set(baseline_lookup.keys()) | set(current_lookup.keys()))

    rows: list[dict[str, Any]] = []
    for key in all_keys:
        driver, library_key, scenario = key
        baseline_result = baseline_lookup.get(key)
        current_result = current_lookup.get(key)
        library_label = (current_result or baseline_result or {}).get("library", library_key)

        if baseline_result is None:
            assert current_result is not None
            rows.append({
                "driver": driver,
                "library_key": library_key,
                "library_label": library_label,
                "scenario": scenario,
                "baseline_result": None,
                "current_result": current_result,
                "metric_kind": "new",
                "baseline_metric": None,
                "current_metric": float(current_result["time"]),
                "abs_change": None,
                "pct_change": None,
                "status_text": "NEW",
                "status_style": "dim",
            })
            continue

        if current_result is None:
            rows.append({
                "driver": driver,
                "library_key": library_key,
                "library_label": library_label,
                "scenario": scenario,
                "baseline_result": baseline_result,
                "current_result": None,
                "metric_kind": "removed",
                "baseline_metric": float(baseline_result["time"]),
                "current_metric": None,
                "abs_change": None,
                "pct_change": None,
                "status_text": "REMOVED",
                "status_style": "dim",
            })
            continue

        baseline_time = float(baseline_result["time"])
        current_time = float(current_result["time"])
        abs_change = current_time - baseline_time

        metric_kind = "absolute"
        baseline_metric = baseline_time
        current_metric = current_time

        if mode == "ratio":
            raw_key = (driver, "raw", scenario)
            base_raw = baseline_lookup.get(raw_key)
            curr_raw = current_lookup.get(raw_key)
            if library_key == "raw":
                metric_kind = "raw-ref"
            elif (
                base_raw is not None
                and curr_raw is not None
                and float(base_raw["time"]) > 0
                and float(curr_raw["time"]) > 0
            ):
                metric_kind = "ratio"
                baseline_metric = baseline_time / float(base_raw["time"])
                current_metric = current_time / float(curr_raw["time"])
            else:
                metric_kind = "abs-fallback"

        if metric_kind == "raw-ref":
            pct_change = ((current_time - baseline_time) / baseline_time * 100.0) if baseline_time > 0 else 0.0
            status_text, status_style = "unchanged", "dim"
        else:
            delta_metric = current_metric - baseline_metric
            pct_change = (delta_metric / baseline_metric * 100.0) if baseline_metric > 0 else 0.0
            status_text, status_style = _status_text(pct_change, threshold)

        rows.append({
            "driver": driver,
            "library_key": library_key,
            "library_label": library_label,
            "scenario": scenario,
            "baseline_result": baseline_result,
            "current_result": current_result,
            "metric_kind": metric_kind,
            "baseline_metric": baseline_metric,
            "current_metric": current_metric,
            "abs_change": abs_change,
            "pct_change": pct_change,
            "status_text": status_text,
            "status_style": status_style,
        })

    return rows


def print_comparison_table(
    baseline: dict[str, Any], current: dict[str, Any], *, threshold: float = 5.0, mode: str = "absolute"
) -> None:
    """Print a rich comparison table between two benchmark runs.

    Args:
        baseline: Parsed baseline benchmark JSON data.
        current: Parsed current benchmark JSON data.
        threshold: Percentage threshold for flagging changes as significant.
        mode: Comparison mode (``"absolute"`` or ``"ratio"``).
    """
    console = Console()
    rows = compute_comparison_rows(baseline, current, mode=mode, threshold=threshold)

    if not rows:
        console.print("[yellow]No benchmark results to compare.[/yellow]")
        return

    title_suffix = " (Ratio Mode)" if mode == "ratio" else ""
    table = Table(title=f"Benchmark Comparison: Baseline vs Current{title_suffix}")
    table.add_column("Driver", style="cyan", no_wrap=True)
    table.add_column("Library", style="magenta")
    table.add_column("Scenario", style="green")
    table.add_column("Baseline (s)", justify="right", style="yellow")
    table.add_column("Current (s)", justify="right", style="yellow")
    table.add_column("Change (s)", justify="right")
    table.add_column("Change (%)", justify="right")
    table.add_column("Status", justify="center")

    improved_count = 0
    regressed_count = 0
    unchanged_count = 0

    for row in rows:
        driver = row["driver"]
        library_label = row["library_label"]
        scenario = row["scenario"]
        baseline_result = row["baseline_result"]
        current_result = row["current_result"]
        status_text = row["status_text"]
        status_style = row["status_style"]

        if baseline_result is None and current_result is not None:
            table.add_row(
                driver, library_label, scenario, "n/a", _format_time(current_result), "n/a", "n/a", "[dim]NEW[/dim]"
            )
            continue

        if current_result is None and baseline_result is not None:
            table.add_row(
                driver,
                library_label,
                scenario,
                _format_time(baseline_result),
                "n/a",
                "n/a",
                "n/a",
                "[dim]REMOVED[/dim]",
            )
            continue

        abs_change = float(row["abs_change"])
        pct_change = float(row["pct_change"])

        if status_text == "IMPROVED":
            improved_count += 1
        elif status_text == "REGRESSED":
            regressed_count += 1
        else:
            unchanged_count += 1

        change_style = "green" if pct_change < 0 else ("red" if pct_change > 0 else "dim")

        table.add_row(
            driver,
            library_label,
            scenario,
            _format_time(baseline_result),
            _format_time(current_result),
            f"[{change_style}]{abs_change:+.4f}[/{change_style}]",
            f"[{change_style}]{pct_change:+.1f}%[/{change_style}]",
            f"[{status_style}]{status_text}[/{status_style}]",
        )

    console.print(table)

    console.print()
    total = improved_count + regressed_count + unchanged_count
    console.print(f"  Total scenarios compared: {total}")
    console.print(f"  [green]Improved: {improved_count}[/green]")
    console.print(f"  [red]Regressed: {regressed_count}[/red]")
    console.print(f"  [dim]Unchanged: {unchanged_count}[/dim]")

    baseline_meta = baseline.get("metadata", {})
    current_meta = current.get("metadata", {})
    if baseline_meta or current_meta:
        console.print()
        console.print("  [bold]Run Metadata:[/bold]")
        console.print(
            f"    Baseline: rows={baseline_meta.get('rows', '?')}, "
            f"iterations={baseline_meta.get('iterations', '?')}, "
            f"mypyc={baseline_meta.get('mypyc_compiled', '?')}, "
            f"timestamp={baseline_meta.get('timestamp', '?')}"
        )
        console.print(
            f"    Current:  rows={current_meta.get('rows', '?')}, "
            f"iterations={current_meta.get('iterations', '?')}, "
            f"mypyc={current_meta.get('mypyc_compiled', '?')}, "
            f"timestamp={current_meta.get('timestamp', '?')}"
        )


@click.command()
@click.argument("baseline", type=click.Path(exists=True))
@click.argument("current", required=False, type=click.Path(exists=True))
@click.option(
    "--threshold", default=5.0, show_default=True, help="Percentage threshold for flagging changes as significant"
)
@click.option(
    "--mode",
    type=click.Choice(["absolute", "ratio"]),
    default="absolute",
    show_default=True,
    help="Comparison mode: absolute seconds or relative sqlspec/raw ratio",
)
@click.option(
    "--allow-metadata-mismatch",
    is_flag=True,
    default=False,
    help="Proceed even when hard metadata keys (rows, pool_size, mypyc_compiled) differ",
)
@click.option(
    "--print-run-args", is_flag=True, default=False, help="Print bench.py CLI flags matching BASELINE metadata and exit"
)
def main(
    baseline: str, current: str | None, threshold: float, mode: str, allow_metadata_mismatch: bool, print_run_args: bool
) -> None:
    """Compare two benchmark result JSON files or print baseline run args.

    BASELINE is the path to the baseline benchmark JSON file.
    CURRENT is the path to the current benchmark JSON file.

    Both files should be produced by ``bench.py --json-output <file>``.
    """
    baseline_data = _load_results(baseline)
    if print_run_args:
        click.echo(format_run_args(baseline_data))
        return

    if current is None:
        raise click.ClickException("Missing argument 'CURRENT'.")

    current_data = _load_results(current)
    hard_mismatches, soft_warnings = validate_metadata_parity(baseline_data, current_data)

    for warning in soft_warnings:
        click.secho(f"Metadata warning: {warning}", fg="yellow")

    if hard_mismatches:
        for mismatch in hard_mismatches:
            click.secho(f"Metadata mismatch: {mismatch}", fg="red")
        if not allow_metadata_mismatch:
            sys.exit(2)

    print_comparison_table(baseline_data, current_data, threshold=threshold, mode=mode)

    baseline_lookup = _build_lookup(baseline_data)
    missing = _missing_required_scenarios(current_data, set(baseline_lookup))
    if missing:
        missing_text = ", ".join("/".join(key) for key in sorted(missing))
        click.secho(f"\nMissing required benchmark scenario(s): {missing_text}", fg="red")
        sys.exit(1)

    rows = compute_comparison_rows(baseline_data, current_data, mode=mode, threshold=threshold)
    regressions = sum(1 for row in rows if row["status_text"] == "REGRESSED")

    if regressions > 0:
        click.secho(f"\n{regressions} regression(s) detected (>{threshold}% slower).", fg="red")
        sys.exit(1)
    click.secho("\nNo regressions detected.", fg="green")


def _load_results(path: str) -> dict[str, Any]:
    """Load and validate a benchmark JSON file.

    Args:
        path: Path to the JSON file.

    Returns:
        Parsed JSON data.

    Raises:
        click.ClickException: If the file cannot be loaded or is invalid.
    """
    try:
        with Path(path).open() as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        msg = f"Failed to load {path}: {exc}"
        raise click.ClickException(msg) from exc

    if "results" not in data:
        msg = f"Invalid benchmark file {path}: missing 'results' key"
        raise click.ClickException(msg)

    return cast("dict[str, Any]", data)


def _build_lookup(data: dict[str, Any]) -> dict[BenchmarkKey, dict[str, Any]]:
    """Build a lookup table from benchmark results.

    Args:
        data: Parsed benchmark JSON data.

    Returns:
        Dictionary mapping (driver, library, scenario) to result payload.
    """
    lookup: dict[BenchmarkKey, dict[str, Any]] = {}
    for result in data["results"]:
        key = (result["driver"], result.get("library_key", result["library"]), result["scenario"])
        lookup[key] = result
    return lookup


def _missing_required_scenarios(data: dict[str, Any], required: set[BenchmarkKey]) -> set[BenchmarkKey]:
    """Return required benchmark identities absent from a result payload."""
    return required - set(_build_lookup(data))


def _format_time(result: dict[str, Any]) -> str:
    """Format a benchmark result's time and optional stddev."""
    time_value = float(result["time"])
    stddev = result.get("stddev")
    if isinstance(stddev, int | float):
        return f"{time_value:.4f} +/- {float(stddev):.4f}"
    return f"{time_value:.4f}"


def _status_text(change_pct: float, threshold: float = 5.0) -> tuple[str, str]:
    """Determine status text and style based on percentage change.

    Args:
        change_pct: Percentage change (positive = slower, negative = faster).
        threshold: Percentage threshold for considering a change significant.

    Returns:
        Tuple of (status_text, rich_style).
    """
    if change_pct < -threshold:
        return "IMPROVED", "bold green"
    if change_pct > threshold:
        return "REGRESSED", "bold red"
    return "unchanged", "dim"


if __name__ == "__main__":
    main()
