"""Measure cold SQLSpec import time and optional-dependency leakage.

Usage:
    python tools/scripts/import_time.py --samples 5 --baseline-ms 681
"""

import argparse
import re
import subprocess
import sys
from statistics import median

__all__ = ("calculate_reduction", "main", "parse_import_times")

_IMPORT_TIME_PATTERN = re.compile(r"^import time:\s+\d+\s+\|\s+(\d+)\s+\|\s+(.+?)\s*$")
_FORBIDDEN = ("pandas", "polars", "pyarrow", "litestar", "pydantic", "opentelemetry", "prometheus_client")


def parse_import_times(output: str) -> "dict[str, int]":
    """Extract cumulative microseconds by module from ``-X importtime`` output."""
    timings: dict[str, int] = {}
    for line in output.splitlines():
        match = _IMPORT_TIME_PATTERN.match(line)
        if match is not None:
            timings[match.group(2)] = int(match.group(1))
    return timings


def calculate_reduction(baseline_ms: float, current_ms: float) -> float:
    """Return the percentage reduction from a positive baseline."""
    if baseline_ms <= 0:
        msg = "baseline_ms must be greater than zero"
        raise ValueError(msg)
    return (baseline_ms - current_ms) / baseline_ms * 100


def _measure_once(python: str) -> "tuple[float, float, tuple[str, ...]]":
    probe = f"import sys, sqlspec; print(','.join(name for name in {_FORBIDDEN!r} if name in sys.modules))"
    result = subprocess.run([python, "-X", "importtime", "-c", probe], capture_output=True, text=True, check=True)
    timings = parse_import_times(result.stderr)
    try:
        total_ms = timings["sqlspec"] / 1000
        typing_ms = timings["sqlspec._typing"] / 1000
    except KeyError as exc:
        msg = f"missing import-time entry for {exc.args[0]}"
        raise RuntimeError(msg) from exc
    leaked = tuple(name for name in result.stdout.strip().split(",") if name)
    return total_ms, typing_ms, leaked


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=5, help="Number of fresh interpreter samples")
    parser.add_argument("--baseline-ms", type=float, help="Optional historical baseline for reduction reporting")
    parser.add_argument("--python", default=sys.executable, help="Python interpreter to measure")
    return parser


def main() -> int:
    """Run cold-import samples and print median timings."""
    parser = _build_parser()
    args = parser.parse_args()
    if args.samples < 1:
        parser.error("--samples must be greater than zero")

    measurements = [_measure_once(args.python) for _ in range(args.samples)]
    total_samples = [sample[0] for sample in measurements]
    typing_samples = [sample[1] for sample in measurements]
    leaked = sorted({name for sample in measurements for name in sample[2]})
    total_ms = median(total_samples)
    typing_ms = median(typing_samples)

    output = [
        f"sqlspec median: {total_ms:.3f} ms",
        f"sqlspec._typing median: {typing_ms:.3f} ms",
        f"samples: {', '.join(f'{sample:.3f}' for sample in total_samples)} ms",
        f"forbidden modules: {', '.join(leaked) if leaked else 'none'}",
    ]
    if args.baseline_ms is not None:
        reduction = calculate_reduction(args.baseline_ms, total_ms)
        output.append(f"reduction from {args.baseline_ms:.3f} ms: {reduction:.2f}%")
    sys.stdout.write("\n".join(output) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
