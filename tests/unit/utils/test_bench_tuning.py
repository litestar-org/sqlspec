"""Tests for adapter tuning benchmark metadata."""

import importlib.util
from pathlib import Path
from types import ModuleType


def _load_bench_tuning_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[3] / "tools" / "scripts" / "bench_tuning.py"
    spec = importlib.util.spec_from_file_location("bench_tuning_for_tests", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bench_tuning_defaults_match_runtime_tuning_plan() -> None:
    module = _load_bench_tuning_module()
    args = module.build_parser().parse_args([])

    assert module.DEFAULT_ITERATIONS == 15
    assert module.DEFAULT_WARMUP == 3
    assert args.iterations == 15
    assert args.warmup == 3


def test_bench_tuning_documents_conditional_scenarios() -> None:
    module = _load_bench_tuning_module()

    assert set(module.SCENARIOS) == {"asyncpg_stmt_cache", "oracle_stmtcache", "arrow_odbc_fetch"}
    for scenario in module.SCENARIOS.values():
        assert scenario.requires
        assert scenario.description
        assert scenario.skip_message
