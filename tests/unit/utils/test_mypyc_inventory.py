"""Tests for mypyc inventory and smoke-gate tooling."""

import json
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_inventory_cli_default_json_summary_names_live_surfaces() -> None:
    """The inventory CLI should emit stable machine-readable output."""
    script_path = PROJECT_ROOT / "tools" / "scripts" / "mypyc_inventory.py"

    completed = subprocess.run(
        [sys.executable, str(script_path)], check=True, cwd=PROJECT_ROOT, capture_output=True, text=True
    )

    payload = json.loads(completed.stdout)

    assert payload["summary"]["compiled_count"] > 0
    assert payload["summary"]["interpreted_count"] > 0
    assert payload["summary"]["total_modules"] == (
        payload["summary"]["compiled_count"] + payload["summary"]["interpreted_count"]
    )
    assert set(payload["surface_counts"]) == {"candidate", "compiled", "interpreted", "keep_interpreted"}
    assert "sqlspec/utils/serializers.py" not in payload["hot_surfaces"]
    assert all((PROJECT_ROOT / module_path).is_file() for module_path in payload["hot_surfaces"])


def test_inventory_cli_markdown_summary_includes_surface_column() -> None:
    """The markdown mode should produce a citation-friendly hot-surface table."""
    script_path = PROJECT_ROOT / "tools" / "scripts" / "mypyc_inventory.py"

    completed = subprocess.run(
        [sys.executable, str(script_path), "--format", "markdown"],
        check=True,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    assert "Compiled modules:" in completed.stdout
    assert "| Module | Surface | Status | Classification | Reason |" in completed.stdout
    assert "sqlspec/utils/serializers.py" not in completed.stdout


def test_makefile_test_mypyc_targets_live_smoke_modules() -> None:
    """The smoke target should compile representative live modules only."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    target_match = re.search(r"^test-mypyc:.*?(?=^\S)", makefile, flags=re.MULTILINE | re.DOTALL)
    assert target_match is not None

    smoke_invocations = re.findall(
        r"uv run mypyc --check-untyped-defs --no-warn-unused-configs (\S+)", target_match.group(0)
    )

    assert smoke_invocations == [
        "sqlspec/utils/text.py",
        "sqlspec/utils/sync_tools.py",
        "sqlspec/core/cache.py",
        "sqlspec/core/hashing.py",
        "sqlspec/core/parameters/_processor.py",
        "sqlspec/core/result/_base.py",
        "sqlspec/driver/_query_cache.py",
        "sqlspec/adapters/sqlite/core.py",
        "sqlspec/adapters/sqlite/pool.py",
        "sqlspec/storage/_paths.py",
        "sqlspec/data_dictionary/_loader.py",
        "sqlspec/data_dictionary/dialects/bigquery.py",
        "sqlspec/data_dictionary/dialects/cockroachdb.py",
        "sqlspec/data_dictionary/dialects/duckdb.py",
        "sqlspec/data_dictionary/dialects/mysql.py",
        "sqlspec/data_dictionary/dialects/oracle.py",
        "sqlspec/data_dictionary/dialects/postgres.py",
        "sqlspec/data_dictionary/dialects/spanner.py",
        "sqlspec/data_dictionary/dialects/sqlite.py",
        "sqlspec/migrations/version.py",
    ]
    assert all((PROJECT_ROOT / path).is_file() for path in smoke_invocations)


def test_inventory_records_rest_of_mypyc_boundary_decisions() -> None:
    """Inventory output should show admitted modules and retained dynamic boundaries."""
    script_path = PROJECT_ROOT / "tools" / "scripts" / "mypyc_inventory.py"

    completed = subprocess.run(
        [sys.executable, str(script_path)], check=True, cwd=PROJECT_ROOT, capture_output=True, text=True
    )
    payload = json.loads(completed.stdout)

    assert "sqlspec/storage/pipeline.py" in payload["compiled_modules"]
    assert "sqlspec/storage/_paths.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/_loader.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/bigquery.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/cockroachdb.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/duckdb.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/mysql.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/oracle.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/postgres.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/spanner.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/sqlite.py" in payload["compiled_modules"]
    assert "sqlspec/migrations/runner.py" in payload["compiled_modules"]
    assert "sqlspec/adapters/sqlite/driver.py" in payload["interpreted_modules"]
    assert "sqlspec/adapters/aiosqlite/driver.py" in payload["interpreted_modules"]
    assert "sqlspec/storage/_arrow_payload.py" in payload["interpreted_modules"]
    assert "sqlspec/observability/_formatting.py" in payload["interpreted_modules"]
    assert payload["adapter_pool_runtimes"]["status"] == "compiled"
    assert payload["adapter_driver_shells"]["classification"] == "prove_separately"
    assert payload["adapter_driver_shells"]["status"] == "blocked"
