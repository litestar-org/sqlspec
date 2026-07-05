"""Inventory the current mypyc compiled vs interpreted module surface."""

import argparse
import json
import sys
from collections.abc import Sequence
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

__all__ = (
    "HOT_SURFACE_CLASSIFICATIONS",
    "build_inventory",
    "classify_module",
    "classify_surface",
    "format_markdown",
    "list_sqlspec_modules",
    "load_mypyc_patterns",
    "main",
)

try:
    import tomllib  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib


CANDIDATE_CLASSIFICATIONS = {"candidate", "compile_now", "helper_split_first", "prove_separately"}
SURFACE_ORDER = ("compiled", "candidate", "hard_block", "keep_interpreted", "interpreted")

HOT_SURFACE_CLASSIFICATIONS: dict[str, dict[str, str]] = {
    "sqlspec/config.py": {
        "classification": "helper_split_first",
        "reason": "Owns runtime hooks, migration setup, and observability/bootstrap orchestration.",
    },
    "sqlspec/base.py": {
        "classification": "helper_split_first",
        "reason": "Registry/session wrappers still manage runtime pool and telemetry orchestration.",
    },
    "sqlspec/storage/pipeline.py": {
        "classification": "compile_now",
        "reason": "Storage orchestration compiles after PyArrow payload encode/decode moved to _arrow_payload.py.",
    },
    "sqlspec/storage/_paths.py": {
        "classification": "compile_now",
        "reason": "Pure path resolution split away from optional PyArrow import helpers.",
    },
    "sqlspec/storage/_arrow_payload.py": {
        "classification": "hard_block",
        "reason": "Direct PyArrow encode/decode boundary remains interpreted.",
    },
    "sqlspec/storage/registry.py": {
        "classification": "compile_now",
        "reason": "Pure routing/cache logic with backend selection only.",
    },
    "sqlspec/storage/errors.py": {
        "classification": "compile_now",
        "reason": "Storage error normalization is typed runtime logic with no Arrow dependence.",
    },
    "sqlspec/storage/_utils.py": {
        "classification": "compile_now",
        "reason": "Optional PyArrow import shims compile cleanly and avoid runtime path logic.",
    },
    "sqlspec/data_dictionary/_loader.py": {
        "classification": "compile_now",
        "reason": "Uses importlib.resources instead of direct __file__ path discovery.",
    },
    "sqlspec/data_dictionary/dialects/postgres.py": {
        "classification": "compile_now",
        "reason": "Shared Postgres JSON type helper for ADBC-as-Postgres, asyncpg, psqlpy, and psycopg dictionaries.",
    },
    "sqlspec/data_dictionary/dialects/sqlite.py": {
        "classification": "compile_now",
        "reason": "Shared SQLite JSON and feature-list helpers for sqlite, aiosqlite, and ADBC-as-SQLite dictionaries.",
    },
    "sqlspec/data_dictionary/dialects/mysql.py": {
        "classification": "compile_now",
        "reason": "Shared MySQL JSON type helper for mysqlconnector, pymysql, aiomysql, asyncmy, and ADBC-as-MySQL dictionaries.",
    },
    "sqlspec/data_dictionary/dialects/cockroachdb.py": {
        "classification": "compile_now",
        "reason": "Shared CockroachDB JSON type helper for cockroach_asyncpg, cockroach_psycopg, and ADBC-as-Cockroach dictionaries.",
    },
    "sqlspec/data_dictionary/dialects/duckdb.py": {
        "classification": "compile_now",
        "reason": "DuckDB data-dictionary dialect configuration is in the compiled dialect surface.",
    },
    "sqlspec/data_dictionary/dialects/oracle.py": {
        "classification": "compile_now",
        "reason": "Shared Oracle version, JSON, feature, and table-list helpers for oracledb sync and async dictionaries.",
    },
    "sqlspec/data_dictionary/dialects/spanner.py": {
        "classification": "compile_now",
        "reason": "Spanner data-dictionary dialect configuration is in the compiled dialect surface.",
    },
    "sqlspec/data_dictionary/dialects/bigquery.py": {
        "classification": "compile_now",
        "reason": "Shared BigQuery INFORMATION_SCHEMA formatting helpers for native BigQuery and ADBC-as-BigQuery dictionaries.",
    },
    "sqlspec/dialects/postgres/_generators.py": {
        "classification": "compile_now",
        "reason": "Postgres extension generator hooks compile with sqlglot[c] and patch the compiled base generator directly.",
    },
    "sqlspec/dialects/postgres/_operators.py": {
        "classification": "compile_now",
        "reason": "Postgres extension operator registry is pure token/expression dispatch used by compiled custom dialects.",
    },
    "sqlspec/dialects/postgres/_paradedb.py": {
        "classification": "hard_block",
        "reason": "SQLGlot subclass/registration module fails native class import under mypyc; compiled helpers stay in _generators/_operators.",
    },
    "sqlspec/dialects/postgres/_pgvector.py": {
        "classification": "hard_block",
        "reason": "SQLGlot tokenizer/dialect subclass module fails native class import under mypyc; compiled helpers stay in _generators/_operators.",
    },
    "sqlspec/dialects/spanner/_generators.py": {
        "classification": "compile_now",
        "reason": "Spanner SQL rendering helpers compile with the custom Spanner dialect surface.",
    },
    "sqlspec/dialects/spanner/_spangres.py": {
        "classification": "hard_block",
        "reason": "SQLGlot subclass/registration module fails native class import under mypyc; compiled helpers stay in _generators.",
    },
    "sqlspec/dialects/spanner/_spanner.py": {
        "classification": "hard_block",
        "reason": "SQLGlot tokenizer/dialect subclass module fails native class import under mypyc; compiled render helpers stay in _generators.",
    },
    "sqlspec/extensions/events/_models.py": {
        "classification": "compile_now",
        "reason": "EventMessage has concrete datetime annotations and slot dataclass layout compatible with mypyc.",
    },
    "sqlspec/extensions/events/_hints.py": {
        "classification": "compile_now",
        "reason": "Event runtime hint resolution is pure adapter-name/default selection logic.",
    },
    "sqlspec/extensions/events/_names.py": {
        "classification": "compile_now",
        "reason": "Event channel and table validators are pure regex checks shared by compiled queue code.",
    },
    "sqlspec/extensions/events/_payload.py": {
        "classification": "compile_now",
        "reason": "Event JSON payload encode/decode helpers are typed and independent of optional native backends.",
    },
    "sqlspec/extensions/events/_queue.py": {
        "classification": "compile_now",
        "reason": "Table-backed queue class flags use ClassVar and concrete queue classes are final.",
    },
    "sqlspec/extensions/events/_channel.py": {
        "classification": "hard_block",
        "reason": "Owns dynamic native backend imports, listener thread/task lifecycle, and protocol dispatch.",
    },
    "sqlspec/extensions/events/_store.py": {
        "classification": "keep_interpreted",
        "reason": "Base event queue store remains interpreted because adapter-specific event stores subclass it.",
    },
    "sqlspec/extensions/adk/_types.py": {
        "classification": "compile_now",
        "reason": "ADK session/event record TypedDict definitions have no optional ADK runtime import.",
    },
    "sqlspec/extensions/adk/memory/_types.py": {
        "classification": "compile_now",
        "reason": "ADK memory record TypedDict definition has no optional ADK runtime import.",
    },
    "sqlspec/extensions/adk/artifact/_types.py": {
        "classification": "compile_now",
        "reason": "ADK artifact record TypedDict definition has no optional ADK runtime import.",
    },
    "sqlspec/extensions/adk/converters.py": {
        "classification": "keep_interpreted",
        "reason": "Imports Google ADK models at module import time and keeps Pydantic model reconstruction interpreted.",
    },
    "sqlspec/extensions/fastapi/providers.py": {
        "classification": "helper_split_first",
        "reason": (
            "Dynamic provider signatures stay interpreted until a pure helper split and compiled-wheel construction "
            "smoke prove the framework boundary."
        ),
    },
    "sqlspec/extensions/litestar/providers.py": {
        "classification": "helper_split_first",
        "reason": (
            "Dynamic provider signatures stay interpreted until a pure helper split and compiled-wheel construction "
            "smoke prove the framework boundary."
        ),
    },
    "sqlspec/extensions/prometheus/__init__.py": {
        "classification": "helper_split_first",
        "reason": (
            "Prometheus observer implementation can move behind a compiled private module while __init__ stays a "
            "thin optional-dependency re-export."
        ),
    },
    "sqlspec/migrations/commands.py": {
        "classification": "keep_interpreted",
        "reason": "CLI command surface keeps dynamic imports and rich-click behavior interpreted.",
    },
    "sqlspec/migrations/runner.py": {
        "classification": "compile_now",
        "reason": "Migration runtime is compiled while command dispatch remains excluded.",
    },
    "sqlspec/observability/_formatting.py": {
        "classification": "compile_now",
        "reason": "Logging Formatter subclass compiles with the same boundary as compiled utility formatters.",
    },
    "sqlspec/core/_pagination.py": {
        "classification": "keep_interpreted",
        "reason": "Litestar OpenAPI requires class annotations that mypyc strips from dataclass pagination models.",
    },
    "sqlspec/adapters/sqlite/pool.py": {
        "classification": "compile_now",
        "reason": "Pool runtime compiles without adapter driver exception-handler subclasses.",
    },
    "sqlspec/adapters/aiosqlite/pool.py": {
        "classification": "compile_now",
        "reason": "Async pool runtime compiles without adapter driver exception-handler subclasses.",
    },
    "sqlspec/adapters/sqlite/driver.py": {
        "classification": "prove_separately",
        "reason": "Native compiled driver construction segfaulted in installed-wheel SqliteConfig session smoke; keep interpreted pending driver layout work.",
    },
    "sqlspec/adapters/aiosqlite/driver.py": {
        "classification": "prove_separately",
        "reason": "Async SQLite driver shares the same native adapter-driver layout risk as the sync driver and stays out of the compiled baseline.",
    },
    "sqlspec/adapters/asyncpg/driver.py": {
        "classification": "prove_separately",
        "reason": "Optional async adapter drivers remain outside baseline smoke until vendor dependencies and exception-handler paths are proven.",
    },
    "sqlspec/utils/module_loader.py": {
        "classification": "compile_now",
        "reason": "Optional dependency probing helper compiles cleanly and is called by many import paths.",
    },
    "sqlspec/utils/serializers/_json.py": {
        "classification": "compile_now",
        "reason": "Runtime JSON serializer selection and encode/decode dispatch are hot compiled utility paths.",
    },
    "sqlspec/utils/serializers/_schema.py": {
        "classification": "compile_now",
        "reason": "Schema dump and struct-aware conversion are already compiled and actively optimized.",
    },
    "sqlspec/utils/sync_tools.py": {
        "classification": "compile_now",
        "reason": "Hot async bridge helpers are already in the include set.",
    },
    "sqlspec/utils/env.py": {
        "classification": "compile_now",
        "reason": "Typed environment parsing utility is pure runtime logic and feeds compiled async bridge configuration.",
    },
    "sqlspec/utils/schema.py": {
        "classification": "compile_now",
        "reason": "Core schema conversion path is already compiled and actively optimized.",
    },
    "sqlspec/utils/type_converters.py": {
        "classification": "compile_now",
        "reason": "Compiled adapter coercion helpers are on the hot path.",
    },
    "sqlspec/storage/backends/base.py": {
        "classification": "compile_now",
        "reason": "Storage backend base classes and async iterator adapters are in the compiled include set.",
    },
    "sqlspec/storage/backends/fsspec.py": {
        "classification": "candidate",
        "reason": "Backend imports no vendor module at import time, but fsspec client construction and streaming paths need installed-wheel proof.",
    },
    "sqlspec/storage/backends/local.py": {
        "classification": "candidate",
        "reason": "Local backend has no vendor import at import time and is the lowest-risk storage backend promotion candidate.",
    },
    "sqlspec/storage/backends/obstore.py": {
        "classification": "candidate",
        "reason": "Backend defers obstore imports until construction, but object-store streaming paths need installed-wheel proof.",
    },
    "sqlspec/utils/arrow_helpers.py": {
        "classification": "hard_block",
        "reason": "Direct PyArrow boundary with historical mypyc segfault risk.",
    },
}


def load_mypyc_patterns(root: Path) -> tuple[list[str], list[str]]:
    """Load mypyc include/exclude glob patterns from pyproject.toml."""

    config = tomllib.loads((root / "pyproject.toml").read_text())
    mypyc_config = config["tool"]["hatch"]["build"]["targets"]["wheel"]["hooks"]["mypyc"]
    return list(mypyc_config["include"]), list(mypyc_config["exclude"])


def list_sqlspec_modules(root: Path) -> list[str]:
    """Return all Python module paths under sqlspec/."""

    return sorted(str(path.relative_to(root)).replace("\\", "/") for path in (root / "sqlspec").rglob("*.py"))


def classify_module(module_path: str, include_patterns: list[str], exclude_patterns: list[str]) -> str:
    """Return whether a module is currently compiled or interpreted."""

    included = any(_matches_hatch_glob(module_path, pattern) for pattern in include_patterns)
    excluded = any(_matches_hatch_glob(module_path, pattern) for pattern in exclude_patterns)
    return "compiled" if included and not excluded else "interpreted"


def _matches_hatch_glob(module_path: str, pattern: str) -> bool:
    """Return whether a module matches hatch-style recursive glob semantics."""
    if fnmatch(module_path, pattern):
        return True
    return "**/" in pattern and fnmatch(module_path, pattern.replace("**/", ""))


def classify_surface(status: str, classification: str | None = None) -> str:
    """Return the planning surface for an inventory entry."""
    if status == "compiled":
        return "compiled"
    if classification == "hard_block":
        return "hard_block"
    if classification == "keep_interpreted":
        return "keep_interpreted"
    if classification in CANDIDATE_CLASSIFICATIONS:
        return "candidate"
    return "interpreted"


def build_inventory(root: Path | None = None) -> dict[str, Any]:
    """Build the current module inventory and hot-surface classification."""

    project_root = root or Path(__file__).resolve().parents[2]
    include_patterns, exclude_patterns = load_mypyc_patterns(project_root)
    modules = list_sqlspec_modules(project_root)
    module_set = set(modules)

    compiled: list[str] = []
    interpreted: list[str] = []
    surfaces: dict[str, list[str]] = {surface: [] for surface in SURFACE_ORDER}
    for module in modules:
        status = classify_module(module, include_patterns, exclude_patterns)
        hot_details = HOT_SURFACE_CLASSIFICATIONS.get(module)
        surface = classify_surface(status, hot_details["classification"] if hot_details else None)
        surfaces[surface].append(module)
        if status == "compiled":
            compiled.append(module)
        else:
            interpreted.append(module)

    hot_surfaces: dict[str, dict[str, str]] = {}
    for module_path, details in HOT_SURFACE_CLASSIFICATIONS.items():
        if module_path not in module_set:
            continue
        status = classify_module(module_path, include_patterns, exclude_patterns)
        hot_surfaces[module_path] = {
            "surface": classify_surface(status, details["classification"]),
            "status": status,
            "classification": details["classification"],
            "reason": details["reason"],
        }

    adapter_configs = sorted(
        module for module in modules if module.startswith("sqlspec/adapters/") and module.endswith("/config.py")
    )
    adapter_cores = sorted(
        module for module in modules if module.startswith("sqlspec/adapters/") and module.endswith("/core.py")
    )
    adapter_drivers = sorted(
        module for module in modules if module.startswith("sqlspec/adapters/") and module.endswith("/driver.py")
    )
    adapter_pools = sorted(
        module for module in modules if module.startswith("sqlspec/adapters/") and module.endswith("/pool.py")
    )

    return {
        "summary": {
            "compiled_count": len(compiled),
            "interpreted_count": len(interpreted),
            "total_modules": len(modules),
        },
        "surface_counts": {surface: len(surfaces[surface]) for surface in sorted(surfaces)},
        "surfaces": {surface: surfaces[surface] for surface in sorted(surfaces)},
        "compiled_modules": compiled,
        "interpreted_modules": interpreted,
        "adapter_config_shells": {
            "count": len(adapter_configs),
            "modules": adapter_configs,
            "status": "interpreted",
            "classification": "helper_split_first",
        },
        "adapter_core_helpers": {
            "count": len(adapter_cores),
            "modules": adapter_cores,
            "status": "compiled",
            "classification": "compile_now",
        },
        "adapter_driver_shells": {
            "count": len(adapter_drivers),
            "modules": adapter_drivers,
            "status": "blocked",
            "classification": "prove_separately",
            "reason": "SQLite sync/async driver admission compiled but failed installed-wheel session construction; driver.py files stay interpreted pending layout work.",
        },
        "adapter_pool_runtimes": {
            "count": len(adapter_pools),
            "modules": adapter_pools,
            "status": "compiled",
            "classification": "compile_now",
        },
        "preserved_exclusions": sorted(
            pattern
            for pattern in exclude_patterns
            if pattern
            in {
                "sqlspec/dialects/postgres/_paradedb.py",
                "sqlspec/dialects/postgres/_pgvector.py",
                "sqlspec/dialects/spanner/_spangres.py",
                "sqlspec/dialects/spanner/_spanner.py",
                "sqlspec/utils/arrow_helpers.py",
                "sqlspec/storage/_arrow_payload.py",
                "sqlspec/adapters/**/data_dictionary.py",
                "sqlspec/observability/_formatting.py",
                "sqlspec/migrations/commands.py",
                "sqlspec/extensions/events/_channel.py",
                "sqlspec/extensions/events/_models.py",
                "sqlspec/extensions/events/_queue.py",
                "sqlspec/extensions/events/_store.py",
                "sqlspec/extensions/adk/converters.py",
                "sqlspec/config.py",
            }
        ),
        "hot_surfaces": hot_surfaces,
    }


def format_markdown(inventory: dict[str, Any]) -> str:
    """Format inventory output as markdown."""
    summary = inventory["summary"]
    surface_counts = inventory["surface_counts"]
    lines = [
        "# mypyc Inventory",
        "",
        f"Compiled modules: {summary['compiled_count']}",
        f"Interpreted modules: {summary['interpreted_count']}",
        f"Total Python modules: {summary['total_modules']}",
        "",
        "## Surface Counts",
        "",
        "| Surface | Count |",
        "|---|---:|",
    ]
    lines.extend(f"| {surface} | {surface_counts[surface]} |" for surface in sorted(surface_counts))

    lines.extend([
        "",
        "## Hot Surfaces",
        "",
        "| Module | Surface | Status | Classification | Reason |",
        "|---|---|---|---|---|",
    ])
    for module_path, details in sorted(inventory["hot_surfaces"].items()):
        lines.append(
            f"| `{module_path}` | {details['surface']} | {details['status']} | "
            f"{details['classification']} | {details['reason']} |"
        )

    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the mypyc inventory CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Output format. JSON is stable for downstream tooling.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="Project root containing pyproject.toml and sqlspec/.",
    )
    args = parser.parse_args(argv)

    inventory = build_inventory(args.root)
    if args.format == "markdown":
        sys.stdout.write(format_markdown(inventory))
        sys.stdout.write("\n")
    else:
        json.dump(inventory, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
