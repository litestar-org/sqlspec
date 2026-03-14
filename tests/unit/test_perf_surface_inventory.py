"""Tests for the workload-matrix foundation surface inventory."""

import json
import re
from pathlib import Path
from typing import TypedDict, cast


class BenchmarkScriptInventory(TypedDict):
    drivers: list[str]


class BuildWorkflowInventory(TypedDict):
    exercises_three_stage_pgo: bool
    uses_packaged_training_module: bool


class AdapterInventory(TypedDict):
    name: str
    execution_surfaces: list[str]
    config_surface: str
    integration_root: str | None
    current_infra_family: str
    bench_harness_driver: bool
    regression_gate_driver: bool
    notes: str


class PerfSurfaceInventory(TypedDict):
    benchmark_scripts: dict[str, BenchmarkScriptInventory]
    build_workflows: dict[str, BuildWorkflowInventory]
    adapters: list[AdapterInventory]


REPO_ROOT = Path(__file__).resolve().parents[2]
INVENTORY_PATH = REPO_ROOT / "performance_surface_inventory.json"
BENCH_DRIVER_PATTERN = re.compile(r'\("(?:raw|sqlspec|sqlalchemy)",\s*"(?P<driver>[^"]+)",\s*"[^"]+"\)')
GATE_DRIVER_PATTERN = re.compile(r'run_benchmark\("(?P<driver>[^"]+)"')


def _load_inventory() -> PerfSurfaceInventory:
    return cast(PerfSurfaceInventory, json.loads(INVENTORY_PATH.read_text()))


def _bench_drivers() -> list[str]:
    bench_text = (REPO_ROOT / "tools" / "scripts" / "bench.py").read_text()
    return sorted({match.group("driver") for match in BENCH_DRIVER_PATTERN.finditer(bench_text)})


def test_perf_surface_inventory_covers_all_adapter_configs_and_integration_roots() -> None:
    inventory = _load_inventory()
    actual_adapters = sorted(path.parent.name for path in (REPO_ROOT / "sqlspec" / "adapters").glob("*/config.py"))
    inventory_adapters = sorted(entry["name"] for entry in inventory["adapters"])

    assert INVENTORY_PATH.is_file()
    assert inventory_adapters == actual_adapters

    allowed_families = {
        "bridge/underlying-engine",
        "cloud-managed",
        "file-local",
        "mock-only",
        "server-backed",
    }

    for entry in inventory["adapters"]:
        assert entry["execution_surfaces"]
        assert set(entry["execution_surfaces"]) <= {"async", "sync"}
        assert entry["current_infra_family"] in allowed_families
        assert entry["notes"]
        assert (REPO_ROOT / entry["config_surface"]).is_file()

        if entry["integration_root"] is None:
            assert entry["name"] == "mock"
        else:
            assert (REPO_ROOT / entry["integration_root"]).is_dir()


def test_perf_surface_inventory_records_expected_execution_surfaces_and_perf_state() -> None:
    inventory = _load_inventory()
    lookup = {entry["name"]: entry for entry in inventory["adapters"]}

    assert lookup["sqlite"]["execution_surfaces"] == ["sync"]
    assert lookup["aiosqlite"]["execution_surfaces"] == ["async"]
    assert lookup["psycopg"]["execution_surfaces"] == ["sync", "async"]
    assert lookup["mysqlconnector"]["execution_surfaces"] == ["sync", "async"]
    assert lookup["oracledb"]["execution_surfaces"] == ["sync", "async"]
    assert lookup["adbc"]["current_infra_family"] == "bridge/underlying-engine"
    assert lookup["bigquery"]["current_infra_family"] == "cloud-managed"
    assert lookup["spanner"]["current_infra_family"] == "cloud-managed"
    assert lookup["mock"]["current_infra_family"] == "mock-only"

    bench_drivers = {entry["name"] for entry in inventory["adapters"] if entry["bench_harness_driver"]}
    gate_drivers = {entry["name"] for entry in inventory["adapters"] if entry["regression_gate_driver"]}

    assert bench_drivers == {"aiosqlite", "asyncpg", "duckdb", "sqlite"}
    assert gate_drivers == {"sqlite"}


def test_perf_surface_inventory_matches_current_perf_scripts_and_build_workflows() -> None:
    inventory = _load_inventory()
    bench_gate_text = (REPO_ROOT / "tools" / "scripts" / "bench_gate.py").read_text()
    bench_subsystems_text = (REPO_ROOT / "tools" / "scripts" / "bench_subsystems.py").read_text()
    publish_text = (REPO_ROOT / ".github" / "workflows" / "publish.yml").read_text()
    test_build_text = (REPO_ROOT / ".github" / "workflows" / "test-build.yml").read_text()

    assert inventory["benchmark_scripts"]["bench.py"]["drivers"] == _bench_drivers()

    gate_match = GATE_DRIVER_PATTERN.search(bench_gate_text)
    assert gate_match is not None
    assert inventory["benchmark_scripts"]["bench_gate.py"]["drivers"] == [gate_match.group("driver")]
    assert inventory["benchmark_scripts"]["bench_subsystems.py"]["drivers"] == ["sqlite"]
    assert "Subsystem Micro-Benchmarks (sqlite)" in bench_subsystems_text

    assert inventory["build_workflows"]["publish.yml"]["uses_packaged_training_module"] == (
        "python -m sqlspec._pgo_training" in publish_text
    )
    assert inventory["build_workflows"]["publish.yml"]["exercises_three_stage_pgo"] == (
        "fprofile-generate" in publish_text or "fprofile-instr-generate" in publish_text
    )
    assert inventory["build_workflows"]["test-build.yml"]["uses_packaged_training_module"] == (
        "python -m sqlspec._pgo_training" in test_build_text
    )
    assert inventory["build_workflows"]["test-build.yml"]["exercises_three_stage_pgo"] == (
        "fprofile-generate" in test_build_text or "fprofile-instr-generate" in test_build_text
    )
