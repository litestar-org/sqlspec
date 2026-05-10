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
