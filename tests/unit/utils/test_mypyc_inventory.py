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

    smoke_paths = re.findall(r"uv run mypyc --check-untyped-defs (\S+)", target_match.group(0))

    assert smoke_paths == [
        "sqlspec/utils/text.py",
        "sqlspec/utils/sync_tools.py",
        "sqlspec/core/cache.py",
        "sqlspec/core/hashing.py",
        "sqlspec/driver/_query_cache.py",
        "sqlspec/adapters/sqlite/core.py",
    ]
    assert all((PROJECT_ROOT / path).is_file() for path in smoke_paths)
