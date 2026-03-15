"""Map current interpreted/compiled hot boundaries for mypyc rollout work."""

import ast
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

__all__ = (
    "ANY_AUDIT_SEAMS",
    "CONFIG_RUNTIME_BOUNDARIES",
    "EXCLUSION_REVALIDATION_SEED",
    "HELPER_SPLIT_DESIGNS",
    "ROLLOUT_FEEDBACK",
    "STORAGE_ARROW_BOUNDARIES",
    "build_boundary_map",
    "classify_module",
    "collect_adapter_core_boundaries",
    "collect_serializer_bridges",
    "load_mypyc_patterns",
)


CONFIG_RUNTIME_BOUNDARIES: tuple[dict[str, Any], ...] = (
    {
        "from_module": "sqlspec/config.py",
        "to_module": "sqlspec/core/config_runtime.py",
        "sites": [
            {"line": 11, "symbol": "config_runtime import"},
            {"line": 1210, "symbol": "build_default_statement_config"},
            {"line": 1211, "symbol": "seed_runtime_driver_features"},
            {"line": 1555, "symbol": "create_sync_pool"},
            {"line": 1568, "symbol": "close_sync_pool"},
            {"line": 1752, "symbol": "create_async_pool"},
            {"line": 1765, "symbol": "close_async_pool"},
        ],
        "classification": "interpreted_runtime_helper_boundary",
        "reason": "Base config shells stay interpreted and currently delegate statement defaults, driver feature seeding, and pool helpers to another interpreted runtime helper layer.",
    },
    {
        "from_module": "sqlspec/config.py",
        "to_module": "sqlspec/utils/module_loader.py",
        "sites": [
            {"line": 22, "symbol": "ensure_pyarrow import"},
            {"line": 824, "symbol": "_build_storage_capabilities"},
            {"line": 828, "symbol": "_dependency_available(ensure_pyarrow)"},
        ],
        "classification": "interpreted_optional_dependency_boundary",
        "reason": "Storage capability detection remains interpreted because it probes optional PyArrow availability at runtime.",
    },
)


STORAGE_ARROW_BOUNDARIES: tuple[dict[str, Any], ...] = (
    {
        "from_module": "sqlspec/storage/pipeline.py",
        "to_module": "sqlspec/storage/_utils.py",
        "sites": [
            {"line": 13, "symbol": "import_pyarrow/import_pyarrow_csv/import_pyarrow_parquet"},
            {"line": 211, "symbol": "_encode_arrow_payload"},
            {"line": 256, "symbol": "_decode_arrow_payload"},
            {"line": 357, "symbol": "SyncStoragePipeline.write_arrow"},
            {"line": 382, "symbol": "SyncStoragePipeline.read_arrow"},
            {"line": 500, "symbol": "AsyncStoragePipeline.write_arrow"},
            {"line": 578, "symbol": "AsyncStoragePipeline.read_arrow_async"},
        ],
        "classification": "interpreted_to_interpreted_arrow_boundary",
        "reason": "Pipeline orchestration is still interpreted and delegates Arrow imports/codecs to `_utils.py`.",
    },
    {
        "from_module": "sqlspec/storage/_utils.py",
        "to_module": "sqlspec/utils/module_loader.py",
        "sites": [
            {"line": 5, "symbol": "ensure_pyarrow import"},
            {"line": 18, "symbol": "import_pyarrow"},
            {"line": 31, "symbol": "import_pyarrow_parquet"},
            {"line": 44, "symbol": "import_pyarrow_csv"},
        ],
        "classification": "interpreted_optional_dependency_boundary",
        "reason": "Arrow helpers remain isolated behind optional-dependency probes in `module_loader.py`.",
    },
    {
        "from_module": "sqlspec/utils/serializers.py",
        "to_module": "sqlspec/_serialization.py",
        "sites": [
            {"line": 11, "symbol": "decode_json/encode_json import"},
            {"line": 103, "symbol": "to_json"},
            {"line": 126, "symbol": "from_json"},
        ],
        "classification": "compiled_to_interpreted_json_boundary",
        "reason": "Compiled serializer helpers still terminate in the interpreted fallback serializers defined in `_serialization.py`.",
    },
)


ANY_AUDIT_SEAMS: tuple[dict[str, Any], ...] = (
    {
        "module": "sqlspec/config.py",
        "line": 86,
        "symbol": "_DriverFeatureHookWrapper.__init__",
        "annotation": "Callable[..., Any]",
        "reason": "Lifecycle hook callbacks accept heterogeneous driver/pool/session payloads.",
    },
    {
        "module": "sqlspec/config.py",
        "line": 107,
        "symbol": "LifecycleConfig",
        "annotation": "Callable[[Any], None] and query hooks with dict[str, Any]",
        "reason": "Observability lifecycle hooks bridge raw driver objects and event payload maps.",
    },
    {
        "module": "sqlspec/_serialization.py",
        "line": 23,
        "symbol": "_type_to_string",
        "annotation": "Any -> Any",
        "reason": "Serializer fallback path handles arbitrary runtime values and optional third-party model types.",
    },
    {
        "module": "sqlspec/_serialization.py",
        "line": 274,
        "symbol": "encode_json",
        "annotation": "data: Any",
        "reason": "Top-level JSON encoding surface is intentionally untyped because it serves every adapter/runtime layer.",
    },
    {
        "module": "sqlspec/storage/pipeline.py",
        "line": 198,
        "symbol": "_encode_row_payload",
        "annotation": "list[Any]",
        "reason": "Storage bridge accepts pre-serialized row payloads without schema specialization.",
    },
    {
        "module": "sqlspec/storage/pipeline.py",
        "line": 216,
        "symbol": "_encode_arrow_payload",
        "annotation": "write_options: dict[str, Any] | None",
        "reason": "CSV/Parquet writer options pass backend-specific dictionaries through unchanged.",
    },
    {
        "module": "sqlspec/adapters/psqlpy/config.py",
        "line": 79,
        "symbol": "PsqlpyPoolParams.configure",
        "annotation": "Callable[..., Any]",
        "reason": "psqlpy exposes raw driver configure callbacks that are opaque to the shared config shell.",
    },
    {
        "module": "sqlspec/adapters/psqlpy/config.py",
        "line": 126,
        "symbol": "_PsqlpySessionFactory._ctx",
        "annotation": "Any | None",
        "reason": "Pool acquire context objects are driver-owned and not weakref/Protocol-friendly.",
    },
)


EXCLUSION_REVALIDATION_SEED: dict[str, dict[str, str]] = {
    "sqlspec/utils/arrow_helpers.py": {
        "bucket": "hard_block",
        "reason": "Direct PyArrow table/batch boundary with prior segfault history.",
    },
    "sqlspec/adapters/**/data_dictionary.py": {
        "bucket": "hard_block",
        "reason": "Still carries native_class=False and inline cache patterns to avoid mypyc crashes.",
    },
    "sqlspec/builder/_vector_expressions.py": {
        "bucket": "helper_split",
        "reason": "Current sqlglot Expression no longer has the old metaclass concern, but registration side effects remain.",
    },
    "sqlspec/data_dictionary/_loader.py": {
        "bucket": "helper_split",
        "reason": "Path discovery is the risky piece; cache/query wrapper logic is otherwise straightforward.",
    },
    "sqlspec/dialects/**": {
        "bucket": "low_roi",
        "reason": "Dialect metaclass/plugin surfaces remain mostly registration code, not hot loops.",
    },
    "sqlspec/observability/_formatting.py": {
        "bucket": "low_roi",
        "reason": "Small logging formatter module with negligible performance upside.",
    },
    "sqlspec/migrations/commands.py": {
        "bucket": "low_roi",
        "reason": "Large CLI/orchestration shell with dynamic inspection and Rich output, not a hot path.",
    },
}


HELPER_SPLIT_DESIGNS: tuple[dict[str, Any], ...] = (
    {
        "surface": "sqlspec/builder/_vector_expressions.py",
        "split_kind": "extract_pure_renderers",
        "extract_module": "sqlspec/builder/_vector_renderers.py",
        "compile_target": "sqlspec/builder/_vector_renderers.py",
        "safe_symbols": (
            "_normalize_metric_name",
            "_coerce_oracle_vector_literal",
            "_maybe_wrap_mysql_vector_literal",
            "_duckdb_target_type",
            "render_postgres_vector_distance",
            "render_mysql_vector_distance",
            "render_oracle_vector_distance",
            "render_bigquery_vector_distance",
            "render_duckdb_vector_distance",
            "render_generic_vector_distance",
        ),
        "keep_interpreted_symbols": (
            "VectorDistance",
            "_register_with_sqlglot",
            "_vector_distance_sql_base",
            "_vector_distance_sql_postgres",
            "_vector_distance_sql_mysql",
            "_vector_distance_sql_oracle",
            "_vector_distance_sql_bigquery",
            "_vector_distance_sql_spanner",
            "_vector_distance_sql_duckdb",
        ),
        "reason": "The expression subclass and sqlglot registration side effects remain unsafe, but dialect-specific string rendering and metric normalization are pure helpers.",
        "feeds_chapter": "adapter-runtime-boundaries",
    },
    {
        "surface": "sqlspec/data_dictionary/_loader.py",
        "split_kind": "extract_loader_state_and_path_resolution",
        "extract_module": "sqlspec/data_dictionary/_loader_core.py",
        "compile_target": "sqlspec/data_dictionary/_loader_core.py",
        "safe_symbols": (
            "build_sql_dir_path",
            "ensure_dialect_path",
            "list_sql_dialects",
            "get_or_create_loader",
            "mark_dialect_loaded",
            "is_dialect_loaded",
        ),
        "keep_interpreted_symbols": (
            "SQL_DIR",
            "DataDictionaryLoader._ensure_dialect_loaded",
            "DataDictionaryLoader.get_query",
            "DataDictionaryLoader.get_query_text",
            "get_data_dictionary_loader",
        ),
        "reason": "Path discovery and loader-cache mutation are straightforward helpers; keep singleton lifecycle and SQLFileLoader orchestration interpreted.",
        "feeds_chapter": "exclusion-revalidation",
    },
    {
        "surface": "sqlspec/adapters/**/data_dictionary.py",
        "split_kind": "extract_query_plans_and_version_resolution",
        "extract_module": "sqlspec/data_dictionary/_plans.py",
        "compile_target": "sqlspec/data_dictionary/_plans.py",
        "safe_symbols": (
            "resolve_schema_name",
            "resolve_feature_flag_from_version",
            "resolve_optimal_type_from_version",
            "build_query_plan",
            "build_sqlite_query_text_plan",
            "collect_index_columns_metadata",
        ),
        "keep_interpreted_symbols": (
            "SyncDataDictionaryBase subclasses",
            "AsyncDataDictionaryBase subclasses",
            "get_version",
            "get_tables",
            "get_columns",
            "get_indexes",
            "get_foreign_keys",
        ),
        "reason": "Cross-module inheritance and driver I/O stay unsafe, but repeated schema resolution, feature gating, and query-plan assembly can be centralized into compiled helpers.",
        "feeds_chapter": "storage-runtime-expansion",
    },
)


ROLLOUT_FEEDBACK: tuple[dict[str, str], ...] = (
    {
        "task_id": "sqlspec-k1a.4",
        "recommendation": "Do not reopen adapter runtime compilation for dialect/vector registration; only revisit if a pure renderer helper module is extracted first.",
    },
    {
        "task_id": "sqlspec-k1a.5",
        "recommendation": "Keep Arrow boundaries interpreted and only route data-dictionary query-plan helpers toward future storage/runtime widening.",
    },
    {
        "task_id": "sqlspec-k1a.6.3",
        "recommendation": "Prioritize `_loader_core.py` and shared data-dictionary plan helpers before any file-level exclusion removal.",
    },
)


def load_mypyc_patterns(root: Path) -> tuple[list[str], list[str]]:
    """Load mypyc include/exclude globs from pyproject.toml."""

    config = tomllib.loads((root / "pyproject.toml").read_text())
    mypyc_config = config["tool"]["hatch"]["build"]["targets"]["wheel"]["hooks"]["mypyc"]
    return list(mypyc_config["include"]), list(mypyc_config["exclude"])


def classify_module(module_path: str, include_patterns: list[str], exclude_patterns: list[str]) -> str:
    """Classify a module as currently compiled or interpreted."""

    included = any(fnmatch(module_path, pattern) for pattern in include_patterns)
    excluded = any(fnmatch(module_path, pattern) for pattern in exclude_patterns)
    return "compiled" if included and not excluded else "interpreted"


def _module_path_from_file(root: Path, file_path: Path) -> str:
    return str(file_path.relative_to(root)).replace("\\", "/")


def _read_ast(file_path: Path) -> ast.Module:
    return ast.parse(file_path.read_text(), filename=str(file_path))


def collect_adapter_core_boundaries(root: Path) -> list[dict[str, Any]]:
    """Collect adapter config.py imports that cross into core.py helpers."""

    include_patterns, exclude_patterns = load_mypyc_patterns(root)
    boundaries: list[dict[str, Any]] = []

    for config_path in sorted((root / "sqlspec" / "adapters").glob("*/config.py")):
        module_path = _module_path_from_file(root, config_path)
        tree = _read_ast(config_path)

        for node in tree.body:
            if not isinstance(node, ast.ImportFrom) or node.module is None:
                continue
            if not node.module.startswith("sqlspec.adapters.") or not node.module.endswith(".core"):
                continue

            target_module = f"{node.module.replace('.', '/')}.py"
            imported_symbols = sorted(alias.name for alias in node.names if alias.name != "*")
            boundaries.append({
                "from_module": module_path,
                "from_status": classify_module(module_path, include_patterns, exclude_patterns),
                "to_module": target_module,
                "to_status": classify_module(target_module, include_patterns, exclude_patterns),
                "import_line": node.lineno,
                "helpers": imported_symbols,
                "classification": "interpreted_to_compiled"
                if classify_module(module_path, include_patterns, exclude_patterns) == "interpreted"
                and classify_module(target_module, include_patterns, exclude_patterns) == "compiled"
                else "same_mode_import",
            })

    return boundaries


def collect_serializer_bridges(root: Path) -> list[dict[str, Any]]:
    """Collect compiled helper modules that import JSON helpers from utils.serializers."""

    include_patterns, exclude_patterns = load_mypyc_patterns(root)
    bridges: list[dict[str, Any]] = []

    for module_path in sorted(
        str(path.relative_to(root)).replace("\\", "/") for path in (root / "sqlspec").rglob("*.py")
    ):
        if classify_module(module_path, include_patterns, exclude_patterns) != "compiled":
            continue

        file_path = root / module_path
        tree = _read_ast(file_path)
        for node in tree.body:
            if not isinstance(node, ast.ImportFrom) or node.module != "sqlspec.utils.serializers":
                continue
            imported_symbols = sorted(alias.name for alias in node.names if alias.name != "*")
            bridges.append({
                "from_module": module_path,
                "from_status": "compiled",
                "via_module": "sqlspec/utils/serializers.py",
                "via_status": classify_module("sqlspec/utils/serializers.py", include_patterns, exclude_patterns),
                "terminal_module": "sqlspec/_serialization.py",
                "terminal_status": classify_module("sqlspec/_serialization.py", include_patterns, exclude_patterns),
                "import_line": node.lineno,
                "helpers": imported_symbols,
                "classification": "compiled_to_interpreted_json_boundary",
            })
            break

    return bridges


def build_boundary_map(root: Path | None = None) -> dict[str, Any]:
    """Build the current hot boundary map for mypyc rollout planning."""

    project_root = root or Path(__file__).resolve().parents[2]
    include_patterns, exclude_patterns = load_mypyc_patterns(project_root)

    config_boundaries = [
        {
            **entry,
            "from_status": classify_module(entry["from_module"], include_patterns, exclude_patterns),
            "to_status": classify_module(entry["to_module"], include_patterns, exclude_patterns),
        }
        for entry in CONFIG_RUNTIME_BOUNDARIES
    ]
    storage_boundaries = [
        {
            **entry,
            "from_status": classify_module(entry["from_module"], include_patterns, exclude_patterns),
            "to_status": classify_module(entry["to_module"], include_patterns, exclude_patterns),
        }
        for entry in STORAGE_ARROW_BOUNDARIES
    ]
    adapter_boundaries = collect_adapter_core_boundaries(project_root)
    serializer_bridges = collect_serializer_bridges(project_root)

    interpreted_to_compiled_adapter_edges = [
        entry for entry in adapter_boundaries if entry["classification"] == "interpreted_to_compiled"
    ]

    return {
        "summary": {
            "config_runtime_edges": len(config_boundaries),
            "adapter_config_core_edges": len(adapter_boundaries),
            "interpreted_to_compiled_adapter_edges": len(interpreted_to_compiled_adapter_edges),
            "serializer_bridges": len(serializer_bridges),
            "storage_arrow_edges": len(storage_boundaries),
            "any_audit_seams": len(ANY_AUDIT_SEAMS),
            "exclusion_revalidation_buckets": len(EXCLUSION_REVALIDATION_SEED),
            "helper_split_designs": len(HELPER_SPLIT_DESIGNS),
            "rollout_feedback_entries": len(ROLLOUT_FEEDBACK),
        },
        "config_runtime_boundaries": config_boundaries,
        "adapter_config_core_boundaries": adapter_boundaries,
        "serializer_bridges": serializer_bridges,
        "storage_arrow_boundaries": storage_boundaries,
        "any_audit_matrix": list(ANY_AUDIT_SEAMS),
        "exclusion_revalidation_seed": EXCLUSION_REVALIDATION_SEED,
        "helper_split_designs": list(HELPER_SPLIT_DESIGNS),
        "rollout_feedback": list(ROLLOUT_FEEDBACK),
    }


if __name__ == "__main__":  # pragma: no cover
    pass
