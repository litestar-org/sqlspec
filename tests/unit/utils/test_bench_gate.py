"""Tests for benchmark matrix metadata in bench_gate.py."""

import importlib.util
from pathlib import Path
from types import ModuleType

from click.testing import CliRunner


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


def test_gate_json_report_records_thresholds_and_result(tmp_path: Path) -> None:
    module = _load_bench_gate_module()
    output_path = tmp_path / "gate.json"

    module._write_json_results(
        [
            {
                "scenario": "read_heavy",
                "raw_time": 0.1,
                "sqlspec_time": 0.11,
                "overhead_pct": 10.0,
                "threshold_pct": 12.0,
                "passed": True,
            }
        ],
        output_path,
        rows=100,
        iterations=1,
        warmup=0,
        thresholds={"read_heavy": 12.0},
        all_passed=True,
    )

    payload = output_path.read_text()

    assert '"all_passed": true' in payload
    assert '"thresholds": {' in payload
    assert '"read_heavy": 12.0' in payload


def test_run_gate_passes_requested_driver_to_benchmark_runner(monkeypatch) -> None:
    module = _load_bench_gate_module()
    captured: dict[str, str] = {}

    def fake_run_benchmark(driver: str, errors: list[str], *, iterations: int, warmup: int) -> list[dict[str, object]]:
        captured["driver"] = driver
        return [
            {"library": "raw", "scenario": "read_heavy", "time": 1.0},
            {"library": module.bench_mod.SQLSPEC_LABEL, "scenario": "read_heavy", "time": 1.05},
        ]

    monkeypatch.setattr(module.bench_mod, "run_benchmark", fake_run_benchmark)
    monkeypatch.setattr(module, "GATE_SCENARIOS", ["read_heavy"])

    results, all_passed = module.run_gate(
        driver="duckdb", rows=100, iterations=1, warmup=0, thresholds={"read_heavy": 12.0}
    )

    assert captured == {"driver": "duckdb"}
    assert results[0]["driver"] == "duckdb"
    assert all_passed is True


def test_run_gate_applies_rows_to_benchmark_runner(monkeypatch) -> None:
    module = _load_bench_gate_module()
    original_rows = module.bench_mod.ROWS_TO_INSERT
    observed_rows: list[int] = []

    def fake_run_benchmark(driver: str, errors: list[str], *, iterations: int, warmup: int) -> list[dict[str, object]]:
        observed_rows.append(module.bench_mod.ROWS_TO_INSERT)
        return [
            {"library": "raw", "scenario": "read_heavy", "time": 1.0},
            {"library": module.bench_mod.SQLSPEC_LABEL, "scenario": "read_heavy", "time": 1.05},
        ]

    monkeypatch.setattr(module.bench_mod, "run_benchmark", fake_run_benchmark)
    monkeypatch.setattr(module, "GATE_SCENARIOS", ["read_heavy"])

    module.run_gate(driver="sqlite", rows=123, iterations=1, warmup=0, thresholds={"read_heavy": 12.0})

    assert observed_rows == [123]
    assert module.bench_mod.ROWS_TO_INSERT == original_rows


def test_cli_reports_regressions_without_failing_by_default(monkeypatch) -> None:
    module = _load_bench_gate_module()

    def fake_run_gate(**kwargs) -> tuple[list[dict[str, object]], bool]:
        return (
            [
                {
                    "driver": kwargs["driver"],
                    "scenario": "read_heavy",
                    "raw_time": 1.0,
                    "sqlspec_time": 2.0,
                    "overhead_pct": 100.0,
                    "threshold_pct": 12.0,
                    "passed": False,
                }
            ],
            False,
        )

    monkeypatch.setattr(module, "run_gate", fake_run_gate)

    result = CliRunner().invoke(module.main, ["--driver", "duckdb", "--rows", "100", "--iterations", "1"])

    assert result.exit_code == 0
    assert "regression report found threshold failures" in result.output


def test_cli_can_fail_explicitly_on_regression(monkeypatch) -> None:
    module = _load_bench_gate_module()

    def fake_run_gate(**kwargs) -> tuple[list[dict[str, object]], bool]:
        return (
            [
                {
                    "driver": kwargs["driver"],
                    "scenario": "read_heavy",
                    "raw_time": 1.0,
                    "sqlspec_time": 2.0,
                    "overhead_pct": 100.0,
                    "threshold_pct": 12.0,
                    "passed": False,
                }
            ],
            False,
        )

    monkeypatch.setattr(module, "run_gate", fake_run_gate)

    result = CliRunner().invoke(
        module.main, ["--driver", "duckdb", "--rows", "100", "--iterations", "1", "--fail-on-regression"]
    )

    assert result.exit_code == 1
    assert "Gate FAILED" in result.output


def test_threshold_ownership_documents_variance_and_attribution() -> None:
    module = _load_bench_gate_module()

    assert module.THRESHOLD_OWNERSHIP["owner"] == "SQLSpec maintainers"
    assert module.THRESHOLD_OWNERSHIP["variance_policy"]
    assert module.THRESHOLD_OWNERSHIP["shared_core_attribution"]
    assert module.THRESHOLD_OWNERSHIP["driver_local_attribution"]
