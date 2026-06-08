"""Tests for mypyc inventory and smoke-gate tooling."""

import json
import re
import subprocess
import sys
from pathlib import Path

import sqlspec.utils.correlation as correlation_module
import sqlspec.utils.schema as schema_module
import sqlspec.utils.sync_tools as sync_tools_module
from sqlspec.utils.correlation import CorrelationContext

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib
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
    assert (
        payload["summary"]["total_modules"]
        == payload["summary"]["compiled_count"] + payload["summary"]["interpreted_count"]
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
    target_match = re.search("^test-mypyc:.*?(?=^\\S)", makefile, flags=re.MULTILINE | re.DOTALL)
    assert target_match is not None
    smoke_invocations = re.findall(
        "uv run mypyc --check-untyped-defs --no-warn-unused-configs (\\S+)", target_match.group(0)
    )
    assert smoke_invocations == [
        "sqlspec/utils/text.py",
        "sqlspec/utils/sync_tools.py",
        "sqlspec/utils/module_loader.py",
        "sqlspec/core/cache.py",
        "sqlspec/core/hashing.py",
        "sqlspec/core/parameters/_processor.py",
        "sqlspec/core/result/_base.py",
        "sqlspec/core/splitter.py",
        "sqlspec/driver/_query_cache.py",
        "sqlspec/adapters/sqlite/core.py",
        "sqlspec/adapters/psqlpy/core.py",
        "sqlspec/adapters/sqlite/pool.py",
        "sqlspec/storage/_paths.py",
        "sqlspec/storage/_utils.py",
        "sqlspec/data_dictionary/_loader.py",
        "sqlspec/data_dictionary/dialects/bigquery.py",
        "sqlspec/data_dictionary/dialects/cockroachdb.py",
        "sqlspec/data_dictionary/dialects/duckdb.py",
        "sqlspec/data_dictionary/dialects/mysql.py",
        "sqlspec/data_dictionary/dialects/oracle.py",
        "sqlspec/data_dictionary/dialects/postgres.py",
        "sqlspec/data_dictionary/dialects/spanner.py",
        "sqlspec/data_dictionary/dialects/sqlite.py",
        "sqlspec/dialects/postgres/_generators.py",
        "sqlspec/dialects/postgres/_operators.py",
        "sqlspec/dialects/spanner/_generators.py",
        "sqlspec/extensions/events/_hints.py",
        "sqlspec/extensions/events/_models.py",
        "sqlspec/extensions/events/_names.py",
        "sqlspec/extensions/events/_payload.py",
        "sqlspec/extensions/events/_queue.py",
        "sqlspec/extensions/adk/_types.py",
        "sqlspec/extensions/adk/memory/_types.py",
        "sqlspec/extensions/adk/artifact/_types.py",
        "sqlspec/migrations/version.py",
        "sqlspec/observability/_formatting.py",
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
    assert "sqlspec/storage/_utils.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/_loader.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/bigquery.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/cockroachdb.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/duckdb.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/mysql.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/oracle.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/postgres.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/spanner.py" in payload["compiled_modules"]
    assert "sqlspec/data_dictionary/dialects/sqlite.py" in payload["compiled_modules"]
    assert "sqlspec/dialects/postgres/_generators.py" in payload["compiled_modules"]
    assert "sqlspec/dialects/postgres/_operators.py" in payload["compiled_modules"]
    assert "sqlspec/dialects/spanner/_generators.py" in payload["compiled_modules"]
    assert "sqlspec/extensions/events/_hints.py" in payload["compiled_modules"]
    assert "sqlspec/extensions/events/_models.py" in payload["compiled_modules"]
    assert "sqlspec/extensions/events/_names.py" in payload["compiled_modules"]
    assert "sqlspec/extensions/events/_payload.py" in payload["compiled_modules"]
    assert "sqlspec/extensions/events/_queue.py" in payload["compiled_modules"]
    assert "sqlspec/extensions/adk/_types.py" in payload["compiled_modules"]
    assert "sqlspec/extensions/adk/memory/_types.py" in payload["compiled_modules"]
    assert "sqlspec/extensions/adk/artifact/_types.py" in payload["compiled_modules"]
    assert "sqlspec/migrations/runner.py" in payload["compiled_modules"]
    assert "sqlspec/observability/_formatting.py" in payload["compiled_modules"]
    assert "sqlspec/adapters/asyncpg/driver.py" in payload["interpreted_modules"]
    assert "sqlspec/adapters/psycopg/driver.py" in payload["interpreted_modules"]
    assert "sqlspec/adapters/cockroach_asyncpg/driver.py" in payload["interpreted_modules"]
    assert "sqlspec/adapters/cockroach_psycopg/driver.py" in payload["interpreted_modules"]
    assert "sqlspec/adapters/sqlite/driver.py" in payload["interpreted_modules"]
    assert "sqlspec/adapters/aiosqlite/driver.py" in payload["interpreted_modules"]
    assert "sqlspec/extensions/events/_channel.py" in payload["interpreted_modules"]
    assert "sqlspec/dialects/postgres/_paradedb.py" in payload["interpreted_modules"]
    assert "sqlspec/dialects/postgres/_pgvector.py" in payload["interpreted_modules"]
    assert "sqlspec/dialects/spanner/_spangres.py" in payload["interpreted_modules"]
    assert "sqlspec/dialects/spanner/_spanner.py" in payload["interpreted_modules"]
    assert "sqlspec/extensions/adk/converters.py" in payload["interpreted_modules"]
    assert "sqlspec/extensions/fastapi/providers.py" in payload["interpreted_modules"]
    assert "sqlspec/extensions/litestar/providers.py" in payload["interpreted_modules"]
    assert "sqlspec/storage/_arrow_payload.py" in payload["interpreted_modules"]
    assert "sqlspec/extensions/adk/converters.py" in payload["preserved_exclusions"]
    assert payload["adapter_pool_runtimes"]["status"] == "compiled"
    assert payload["adapter_driver_shells"]["classification"] == "prove_separately"
    assert payload["adapter_driver_shells"]["status"] == "blocked"
    assert "sqlspec/adapters/asyncpg/driver.py" in payload["adapter_driver_shells"]["modules"]
    assert "sqlspec/adapters/psycopg/driver.py" in payload["adapter_driver_shells"]["modules"]
    assert "sqlspec/adapters/cockroach_asyncpg/driver.py" in payload["adapter_driver_shells"]["modules"]
    assert "sqlspec/adapters/cockroach_psycopg/driver.py" in payload["adapter_driver_shells"]["modules"]


def test_mypy_2_toolchain_policy_is_explicit_and_parallel_gate_is_default() -> None:
    """The mypy 2.0 cutover should keep parallel checking in the default type gate."""
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    build_dependencies = pyproject["dependency-groups"]["build"]
    lint_dependencies = pyproject["dependency-groups"]["lint"]
    mypyc_dependencies = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["hooks"]["mypyc"]["dependencies"]
    mypy_config = pyproject["tool"]["mypy"]
    assert "mypy>=2.0.0" in build_dependencies
    assert "mypy>=2.0.0" in lint_dependencies
    assert "mypy>=2.0.0" in mypyc_dependencies
    assert mypy_config["local_partial_types"] is True
    assert mypy_config["strict_bytes"] is True
    assert mypy_config["allow_redefinition"] is False
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    assert re.search("^mypy:.*?uv run mypy -n \\$\\(MYPY_WORKERS\\)", makefile, flags=re.MULTILINE | re.DOTALL)
    assert re.search("^dmypy:.*?## Run mypy daemon", makefile, flags=re.MULTILINE) is not None
    assert re.search("^mypy-parallel:.*?##", makefile, flags=re.MULTILINE) is not None
    assert re.search("^type-check:\\s+mypy pyright\\s+##", makefile, flags=re.MULTILINE) is not None


def test_dead_code_removal_c13_return_value_helper_removed() -> None:
    assert not hasattr(sync_tools_module, "_return_value")


def test_dead_code_removal_c13_detect_schema_type_helper_removed() -> None:
    assert not hasattr(schema_module, "_detect_schema_type")


def test_correlation_context_function_is_public() -> None:
    """correlation_context is a public helper (imported by downstream consumers)."""
    assert "correlation_context" in correlation_module.__all__
    assert hasattr(correlation_module, "correlation_context")
    with correlation_module.correlation_context("request-id") as correlation_id:
        assert correlation_id == "request-id"
        assert CorrelationContext.get() == "request-id"
