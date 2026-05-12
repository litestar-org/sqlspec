"""Tests for subsystem benchmark registration."""

import importlib.util
from pathlib import Path
from types import ModuleType


def _load_bench_subsystems_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[3] / "tools" / "scripts" / "bench_subsystems.py"
    spec = importlib.util.spec_from_file_location("bench_subsystems_for_tests", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_storage_subsystem_benchmarks_are_registered() -> None:
    module = _load_bench_subsystems_module()
    db_path = module._make_temp_db()
    module._setup_test_table(db_path)
    benchmarks = []

    try:
        benchmarks = module._build_benchmarks(db_path, iterations=1)
        names = {benchmark.name for benchmark in benchmarks}
        assert "StorageRegistry.get() - cached alias" in names
        assert "SyncStoragePipeline.write_rows() - local jsonl" in names
        assert "_decode_arrow_payload() - jsonl" in names
    finally:
        for benchmark in benchmarks:
            if benchmark.name == "_cleanup_" and benchmark.setup_fn is not None:
                benchmark.setup_fn()
        db_path.unlink(missing_ok=True)


def test_librt_string_candidate_benchmarks_are_registered() -> None:
    module = _load_bench_subsystems_module()
    db_path = module._make_temp_db()
    module._setup_test_table(db_path)
    benchmarks = []

    try:
        benchmarks = module._build_benchmarks(db_path, iterations=1)
        names = {benchmark.name for benchmark in benchmarks}
        assert {
            "librt ParameterConverter baseline",
            "librt ParameterConverter StringWriter",
            "librt splitter join baseline",
            "librt splitter StringWriter",
            "librt psqlpy copy baseline",
            "librt psqlpy copy StringWriter",
        }.issubset(names)
        for benchmark in benchmarks:
            if benchmark.name.startswith("librt "):
                benchmark.bench_fn()
    finally:
        for benchmark in benchmarks:
            if benchmark.name == "_cleanup_" and benchmark.setup_fn is not None:
                benchmark.setup_fn()
        db_path.unlink(missing_ok=True)


def test_subsystem_json_report_records_included_cases(tmp_path: Path) -> None:
    module = _load_bench_subsystems_module()
    output_path = tmp_path / "subsystems.json"

    module._write_json_results(
        [
            {
                "name": "StorageRegistry.get() - cached alias",
                "time_per_op_us": 1.0,
                "ops_per_sec": 1_000_000.0,
                "total_time": 0.001,
                "iterations": 1000,
                "description": "Resolve cached storage alias",
            }
        ],
        output_path,
        iterations=1000,
        warmup=10,
        profile=False,
    )

    payload = output_path.read_text()

    assert '"StorageRegistry.get() - cached alias"' in payload
    assert '"included_cases": [' in payload
