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
