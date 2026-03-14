"""Tests for benchmark matrix metadata in bench_gate.py."""

import importlib.util
from pathlib import Path
from types import ModuleType


def _load_bench_gate_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[3] / "tools" / "scripts" / "bench_gate.py"
    spec = importlib.util.spec_from_file_location("bench_gate_for_tests", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_benchmark_scenario_matrix_covers_prd_hot_paths() -> None:
    module = _load_bench_gate_module()

    assert set(module.BENCHMARK_SCENARIO_MATRIX) == {
        "parameter_pipeline",
        "coercion_engine",
        "adapter_runtime_boundaries",
        "storage_runtime_expansion",
        "exclusion_revalidation",
    }

    for entry in module.BENCHMARK_SCENARIO_MATRIX.values():
        assert entry["tracked_by"]
        assert entry["goal"]
        assert entry["scenarios"]


def test_module_admission_criteria_records_required_evidence() -> None:
    module = _load_bench_gate_module()

    assert module.MODULE_ADMISSION_CRITERIA["benchmark_delta"] == "measurable or neutral"
    assert module.MODULE_ADMISSION_CRITERIA["mypy_mypyc"] == "must compile cleanly"
    assert module.MODULE_ADMISSION_CRITERIA["segfaults"] == "no new crashes or segfaults"
    assert module.MODULE_ADMISSION_CRITERIA["any_boundaries"] == "explicitly justified"
    assert module.MODULE_ADMISSION_CRITERIA["unsafe_surfaces"] == "keep Arrow/metaclass-heavy paths interpreted"


def test_chapter_rollout_order_starts_with_guardrails_and_parameter_work() -> None:
    module = _load_bench_gate_module()

    assert module.CHAPTER_ROLLOUT_ORDER[0] == "compile-boundary-guardrails"
    assert module.CHAPTER_ROLLOUT_ORDER[1:4] == (
        "compiled-parameter-pipeline",
        "compiled-coercion-engine",
        "adapter-runtime-boundaries",
    )
