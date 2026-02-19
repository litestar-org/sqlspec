"""Benchmark comparison script for comparing two benchmark runs.

Accepts two JSON result files (baseline and current) produced by
``bench.py --json-output`` and produces a rich table showing per-scenario
comparison with absolute and percentage changes.

Run with::

    uv run python tools/scripts/bench_compare.py baseline.json current.json
"""

import json
import sys
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.table import Table

__all__ = ("main", "print_comparison_table")


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

    return data


def _build_lookup(data: dict[str, Any]) -> dict[tuple[str, str, str], float]:
    """Build a lookup table from benchmark results.

    Args:
        data: Parsed benchmark JSON data.

    Returns:
        Dictionary mapping (driver, library, scenario) to median time.
    """
    lookup: dict[tuple[str, str, str], float] = {}
    for result in data["results"]:
        key = (result["driver"], result["library"], result["scenario"])
        lookup[key] = result["time"]
    return lookup


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


def print_comparison_table(baseline: dict[str, Any], current: dict[str, Any], *, threshold: float = 5.0) -> None:
    """Print a rich comparison table between two benchmark runs.

    Args:
        baseline: Parsed baseline benchmark JSON data.
        current: Parsed current benchmark JSON data.
        threshold: Percentage threshold for flagging changes as significant.
    """
    console = Console()

    baseline_lookup = _build_lookup(baseline)
    current_lookup = _build_lookup(current)

    # Collect all keys from both runs
    all_keys = sorted(set(baseline_lookup.keys()) | set(current_lookup.keys()))

    if not all_keys:
        console.print("[yellow]No benchmark results to compare.[/yellow]")
        return

    table = Table(title="Benchmark Comparison: Baseline vs Current")
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

    for key in all_keys:
        driver, library, scenario = key
        baseline_time = baseline_lookup.get(key)
        current_time = current_lookup.get(key)

        if baseline_time is None:
            table.add_row(driver, library, scenario, "n/a", f"{current_time:.4f}", "n/a", "n/a", "[dim]NEW[/dim]")
            continue

        if current_time is None:
            table.add_row(driver, library, scenario, f"{baseline_time:.4f}", "n/a", "n/a", "n/a", "[dim]REMOVED[/dim]")
            continue

        abs_change = current_time - baseline_time
        pct_change = abs_change / baseline_time * 100.0 if baseline_time > 0 else 0.0

        status_text, status_style = _status_text(pct_change, threshold)

        if status_text == "IMPROVED":
            improved_count += 1
        elif status_text == "REGRESSED":
            regressed_count += 1
        else:
            unchanged_count += 1

        # Color the change values
        change_style = "green" if abs_change < 0 else ("red" if abs_change > 0 else "dim")

        table.add_row(
            driver,
            library,
            scenario,
            f"{baseline_time:.4f}",
            f"{current_time:.4f}",
            f"[{change_style}]{abs_change:+.4f}[/{change_style}]",
            f"[{change_style}]{pct_change:+.1f}%[/{change_style}]",
            f"[{status_style}]{status_text}[/{status_style}]",
        )

    console.print(table)

    # Print summary
    console.print()
    total = improved_count + regressed_count + unchanged_count
    console.print(f"  Total scenarios compared: {total}")
    console.print(f"  [green]Improved: {improved_count}[/green]")
    console.print(f"  [red]Regressed: {regressed_count}[/red]")
    console.print(f"  [dim]Unchanged: {unchanged_count}[/dim]")

    # Print metadata comparison
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
@click.argument("current", type=click.Path(exists=True))
@click.option(
    "--threshold", default=5.0, show_default=True, help="Percentage threshold for flagging changes as significant"
)
def main(baseline: str, current: str, threshold: float) -> None:
    """Compare two benchmark result JSON files.

    BASELINE is the path to the baseline benchmark JSON file.
    CURRENT is the path to the current benchmark JSON file.

    Both files should be produced by ``bench.py --json-output <file>``.
    """
    baseline_data = _load_results(baseline)
    current_data = _load_results(current)

    print_comparison_table(baseline_data, current_data, threshold=threshold)

    # Exit with non-zero if any regressions detected
    current_lookup = _build_lookup(current_data)
    baseline_lookup = _build_lookup(baseline_data)
    regressions = 0
    for key in set(baseline_lookup.keys()) & set(current_lookup.keys()):
        bt = baseline_lookup[key]
        ct = current_lookup[key]
        if bt > 0 and ((ct - bt) / bt) * 100.0 > threshold:
            regressions += 1

    if regressions > 0:
        click.secho(f"\n{regressions} regression(s) detected (>{threshold}% slower).", fg="red")
        sys.exit(1)
    else:
        click.secho("\nNo regressions detected.", fg="green")


if __name__ == "__main__":
    main()
