"""Benchmark comparison script for comparing two benchmark runs.

Accepts two JSON result files (baseline and current) produced by
``bench.py --json-output`` and produces a rich table showing per-scenario
comparison with absolute or relative ratio changes and metadata parity checks.

Run with::

    uv run python tools/scripts/bench_compare.py baseline.json current.json
"""

import json
import math
import sys
from pathlib import Path
from typing import Any, cast

import click
from rich.console import Console
from rich.table import Table

BenchmarkKey = tuple[str, str, str]

HARD_METADATA_KEYS: tuple[str, ...] = ("rows", "pool_size", "mypyc_compiled")
SOFT_METADATA_KEYS: tuple[str, ...] = ("iterations", "warmup", "environment")
STRICT_METADATA_KEYS: tuple[str, ...] = ("iterations", "warmup")
STRICT_ENVIRONMENT_KEYS: tuple[str, ...] = ("platform", "python_version", "cpu_count", "driver_versions", "service_id")
GATE_IDENTITY_PARTS = 3

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
@click.option(
    "--gate-identity",
    "gate_identities",
    multiple=True,
    help="driver/library_key/scenario allowed to fail the run; repeatable",
)
@click.option(
    "--max-gate-noise", type=float, default=None, help="Maximum noise_ratio for gated identities in either file"
)
@click.option(
    "--strict-parity",
    is_flag=True,
    default=False,
    help="Treat iterations, warmup and host, service and driver environment differences as hard mismatches",
)
def main(
    baseline: str,
    current: str | None,
    threshold: float,
    mode: str,
    allow_metadata_mismatch: bool,
    print_run_args: bool,
    gate_identities: tuple[str, ...] = (),
    max_gate_noise: float | None = None,
    strict_parity: bool = False,
) -> None:
    """Compare two benchmark result JSON files or print baseline run args.

    BASELINE is the path to the baseline benchmark JSON file.
    CURRENT is the path to the current benchmark JSON file.

    Both files should be produced by ``bench.py --json-output <file>``.
    """
    gated = _validate_gate_options(gate_identities, max_gate_noise, strict_parity, allow_metadata_mismatch)

    baseline_data = _load_results(baseline)
    if print_run_args:
        click.echo(format_run_args(baseline_data))
        return

    if current is None:
        raise click.ClickException("Missing argument 'CURRENT'.")

    current_data = _load_results(current)
    _enforce_metadata_parity(
        baseline_data, current_data, strict_parity=strict_parity, allow_metadata_mismatch=allow_metadata_mismatch
    )

    print_comparison_table(baseline_data, current_data, threshold=threshold, mode=mode)

    baseline_lookup = _build_lookup(baseline_data)
    current_lookup = _build_lookup(current_data)
    if gated:
        missing_gated = [key for key in sorted(gated) if key not in baseline_lookup or key not in current_lookup]
        if missing_gated:
            for key in missing_gated:
                click.secho(f"Missing gated identity: {'/'.join(key)}", fg="red")
            sys.exit(1)

    missing = _missing_required_scenarios(current_data, set(baseline_lookup))
    if missing:
        missing_text = ", ".join("/".join(key) for key in sorted(missing))
        click.secho(f"\nMissing required benchmark scenario(s): {missing_text}", fg="red")
        sys.exit(1)

    rows = compute_comparison_rows(baseline_data, current_data, mode=mode, threshold=threshold)
    regressions = _report_and_count_regressions(rows, gated, baseline_lookup, current_lookup)

    noise_errors: list[str] = []
    if max_gate_noise is not None:
        noise_errors.extend(_gated_noise_errors(baseline_data, gated, max_gate_noise, "baseline"))
        noise_errors.extend(_gated_noise_errors(current_data, gated, max_gate_noise, "current"))
        for err in noise_errors:
            click.secho(f"Gated noise error: {err}", fg="red")

    if regressions > 0:
        click.secho(f"\n{regressions} regression(s) detected (>{threshold}% slower).", fg="red")
    if regressions > 0 or noise_errors:
        sys.exit(1)
    click.secho("\nNo regressions detected.", fg="green")


def _validate_gate_options(
    gate_identities: tuple[str, ...], max_gate_noise: float | None, strict_parity: bool, allow_metadata_mismatch: bool
) -> set[BenchmarkKey]:
    """Validate CLI gate options and return parsed gated benchmark identities."""
    gated = _parse_gate_identities(gate_identities)
    if max_gate_noise is not None:
        if not gated:
            msg = "--max-gate-noise requires at least one --gate-identity."
            raise click.ClickException(msg)
        if isinstance(max_gate_noise, bool) or not math.isfinite(max_gate_noise) or max_gate_noise < 0.0:
            msg = f"--max-gate-noise must be a finite non-negative number, got {max_gate_noise!r}."
            raise click.ClickException(msg)
    if strict_parity and allow_metadata_mismatch:
        msg = "--strict-parity cannot be combined with --allow-metadata-mismatch."
        raise click.ClickException(msg)
    return gated


def _enforce_metadata_parity(
    baseline_data: dict[str, Any], current_data: dict[str, Any], *, strict_parity: bool, allow_metadata_mismatch: bool
) -> None:
    """Print metadata warnings and exit 2 on unallowed hard or strict-parity mismatches."""
    hard_mismatches, soft_warnings = validate_metadata_parity(baseline_data, current_data)
    strict_mismatches = _strict_parity_mismatches(baseline_data, current_data) if strict_parity else []

    for warning in soft_warnings:
        click.secho(f"Metadata warning: {warning}", fg="yellow")

    all_hard_mismatches = [*hard_mismatches, *strict_mismatches]
    if all_hard_mismatches:
        for mismatch in all_hard_mismatches:
            click.secho(f"Metadata mismatch: {mismatch}", fg="red")
        if strict_parity:
            click.secho("Recapture the baseline and current run on one host and service.", fg="red")
        if not allow_metadata_mismatch:
            sys.exit(2)


def _report_and_count_regressions(
    rows: list[dict[str, Any]],
    gated: set[BenchmarkKey],
    baseline_lookup: dict[BenchmarkKey, dict[str, Any]],
    current_lookup: dict[BenchmarkKey, dict[str, Any]],
) -> int:
    """Print gated summary and report-only regressions, returning blocking regression count."""
    if not gated:
        return sum(1 for row in rows if row["status_text"] == "REGRESSED")

    rows_by_key = {(row["driver"], row["library_key"], row["scenario"]): row for row in rows}
    for key in sorted(gated):
        base_res = baseline_lookup.get(key)
        curr_res = current_lookup.get(key)
        comp_row = rows_by_key.get(key)
        if base_res is not None and curr_res is not None and comp_row is not None:
            pct_val = float(comp_row["pct_change"])
            click.echo(
                f"Gated {'/'.join(key)}: baseline noise {_format_noise_value(base_res.get('noise_ratio'))}, "
                f"current noise {_format_noise_value(curr_res.get('noise_ratio'))}, delta {pct_val:+.1f}%"
            )

    regressions = 0
    for row in rows:
        if row["status_text"] != "REGRESSED":
            continue
        row_key = (row["driver"], row["library_key"], row["scenario"])
        if row_key in gated:
            regressions += 1
        else:
            pct_val = float(row["pct_change"])
            click.secho(f"Report-only over threshold: {'/'.join(row_key)} ({pct_val:+.1f}%)", fg="yellow")
    return regressions


def _parse_gate_identities(values: tuple[str, ...]) -> set[BenchmarkKey]:
    """Parse driver/library_key/scenario values, raising ClickException on a malformed value."""
    parsed: set[BenchmarkKey] = set()
    for raw_val in values:
        parts = raw_val.split("/")
        if len(parts) != GATE_IDENTITY_PARTS or any(not part or part.strip() != part for part in parts):
            msg = f"Malformed --gate-identity {raw_val!r}; expected 'driver/library_key/scenario'."
            raise click.ClickException(msg)
        parsed.add((parts[0], parts[1], parts[2]))
    return parsed


def _gated_noise_errors(data: dict[str, Any], gated: set[BenchmarkKey], max_noise: float, label: str) -> list[str]:
    """Return one message per gated row whose time or noise_ratio is invalid or above max_noise."""
    lookup = _build_lookup(data)
    errors: list[str] = []
    for key in sorted(gated):
        key_str = "/".join(key)
        row = lookup.get(key)
        if row is None:
            errors.append(f"{label} {key_str}: missing gated row")
            continue
        time_val = row.get("time")
        if (
            isinstance(time_val, bool)
            or not isinstance(time_val, int | float)
            or not math.isfinite(time_val)
            or float(time_val) <= 0.0
        ):
            errors.append(f"{label} {key_str}: invalid time {time_val!r}")
        noise_val = row.get("noise_ratio")
        if (
            isinstance(noise_val, bool)
            or not isinstance(noise_val, int | float)
            or not math.isfinite(noise_val)
            or float(noise_val) < 0.0
        ):
            errors.append(f"{label} {key_str}: invalid noise_ratio {noise_val!r}")
        elif float(noise_val) > max_noise:
            errors.append(
                f"{label} {key_str}: noise_ratio {float(noise_val):.4f} exceeds max_gate_noise {max_noise:.4f}"
            )
    return errors


def _strict_parity_mismatches(baseline: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Return one message per differing or absent iterations, warmup, or parity environment key."""
    raw_base_meta = baseline.get("metadata")
    raw_curr_meta = current.get("metadata")
    base_meta = raw_base_meta if isinstance(raw_base_meta, dict) else {}
    curr_meta = raw_curr_meta if isinstance(raw_curr_meta, dict) else {}

    mismatches: list[str] = []
    for key in STRICT_METADATA_KEYS:
        if key not in base_meta or key not in curr_meta:
            mismatches.append(
                f"{key}: absent (baseline={'present' if key in base_meta else 'absent'}, "
                f"current={'present' if key in curr_meta else 'absent'})"
            )
        elif not _values_strictly_equal(base_meta[key], curr_meta[key]):
            mismatches.append(f"{key}: baseline={base_meta[key]!r} != current={curr_meta[key]!r}")

    raw_base_env = base_meta.get("environment")
    raw_curr_env = curr_meta.get("environment")
    base_env = raw_base_env if isinstance(raw_base_env, dict) else {}
    curr_env = raw_curr_env if isinstance(raw_curr_env, dict) else {}

    for env_key in STRICT_ENVIRONMENT_KEYS:
        if env_key not in base_env or env_key not in curr_env:
            mismatches.append(
                f"environment.{env_key}: absent (baseline={'present' if env_key in base_env else 'absent'}, "
                f"current={'present' if env_key in curr_env else 'absent'})"
            )
        elif not _values_strictly_equal(base_env[env_key], curr_env[env_key]):
            mismatches.append(f"environment.{env_key}: baseline={base_env[env_key]!r} != current={curr_env[env_key]!r}")

    return mismatches


def _values_strictly_equal(left: Any, right: Any) -> bool:
    """Compare two metadata values while distinguishing bool from int."""
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_values_strictly_equal(left[k], right[k]) for k in left)
    return bool(left == right)


def _format_noise_value(value: object) -> str:
    """Format a gated row's noise_ratio for summary output."""
    if not isinstance(value, bool) and isinstance(value, int | float) and math.isfinite(value):
        return f"{float(value):.4f}"
    return repr(value)


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
