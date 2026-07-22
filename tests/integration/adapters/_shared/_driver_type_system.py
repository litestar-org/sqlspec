"""Driver-type-system registry and source-equality helpers for contract tests."""

import ast
import difflib
import importlib
import inspect
import textwrap
from dataclasses import dataclass
from typing import Any, cast

__all__ = (
    "DRIVER_FEATURE_CONSUMED_KEYS",
    "DRIVER_FEATURE_CONTRACT_ENABLED",
    "DRIVER_FEATURE_CONTRACT_GROUPS",
    "DRIVER_FEATURE_TYPED_DICTS",
    "SOURCE_EQUIVALENCE_ALLOWLIST",
    "SOURCE_EQUIVALENCE_CASES",
    "DriverFeatureContractGroup",
    "SourceEquivalenceCase",
    "assert_source_equivalent",
    "get_driver_feature_consumed_keys",
    "get_driver_feature_keys",
    "normalize_source",
)


@dataclass(frozen=True)
class DriverFeatureContractGroup:
    """Registry entry for a disabled-first contract group."""

    description: str
    enabled: bool = False
    enabled_adapters: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceEquivalenceCase:
    """One source-equality tripwire across adapter helper implementations."""

    case_id: str
    group: str
    symbol: str
    module_paths: tuple[str, ...]
    allowlist_key: str


DRIVER_FEATURE_TYPED_DICTS: dict[str, tuple[str, str]] = {
    "adbc": ("sqlspec.adapters.adbc.config", "AdbcDriverFeatures"),
    "aiomysql": ("sqlspec.adapters.aiomysql.config", "AiomysqlDriverFeatures"),
    "aiosqlite": ("sqlspec.adapters.aiosqlite.config", "AiosqliteDriverFeatures"),
    "arrow_odbc": ("sqlspec.adapters.arrow_odbc.config", "ArrowOdbcDriverFeatures"),
    "asyncmy": ("sqlspec.adapters.asyncmy.config", "AsyncmyDriverFeatures"),
    "asyncpg": ("sqlspec.adapters.asyncpg.config", "AsyncpgDriverFeatures"),
    "bigquery": ("sqlspec.adapters.bigquery.config", "BigQueryDriverFeatures"),
    "cockroach_asyncpg": ("sqlspec.adapters.cockroach_asyncpg.config", "CockroachAsyncpgDriverFeatures"),
    "cockroach_psycopg": ("sqlspec.adapters.cockroach_psycopg.config", "CockroachPsycopgDriverFeatures"),
    "duckdb": ("sqlspec.adapters.duckdb.config", "DuckDBDriverFeatures"),
    "mssql_python": ("sqlspec.adapters.mssql_python.config", "MssqlPythonDriverFeatures"),
    "mysqlconnector": ("sqlspec.adapters.mysqlconnector.config", "MysqlConnectorDriverFeatures"),
    "oracledb": ("sqlspec.adapters.oracledb.config", "OracleDriverFeatures"),
    "psqlpy": ("sqlspec.adapters.psqlpy.config", "PsqlpyDriverFeatures"),
    "psycopg": ("sqlspec.adapters.psycopg.config", "PsycopgDriverFeatures"),
    "pymssql": ("sqlspec.adapters.pymssql.config", "PymssqlDriverFeatures"),
    "pymysql": ("sqlspec.adapters.pymysql.config", "PyMysqlDriverFeatures"),
    "spanner": ("sqlspec.adapters.spanner.config", "SpannerDriverFeatures"),
    "sqlite": ("sqlspec.adapters.sqlite.config", "SqliteDriverFeatures"),
}

_MIGRATED_DRIVER_FEATURE_ADAPTERS = (
    "aiomysql",
    "aiosqlite",
    "arrow_odbc",
    "asyncmy",
    "bigquery",
    "duckdb",
    "mssql_python",
    "mysqlconnector",
    "psqlpy",
    "psycopg",
    "pymssql",
    "pymysql",
    "spanner",
    "sqlite",
)

# Downstream migrations populate the consumed-key registry adapter by adapter.
DRIVER_FEATURE_CONSUMED_KEYS: dict[str, tuple[str, ...]] = dict.fromkeys(DRIVER_FEATURE_TYPED_DICTS, ())
DRIVER_FEATURE_CONSUMED_KEYS.update({
    "aiomysql": (
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "enable_local_infile_bulk_load",
    ),
    "aiosqlite": (
        "enable_custom_adapters",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "custom_functions",
        "custom_collations",
        "custom_aggregates",
        "authorizer_callback",
        "trace_callback",
        "progress_handler",
        "progress_handler_interval",
        "row_factory",
        "text_factory",
        "pragmas",
        "extensions",
    ),
    "arrow_odbc": (
        "chunk_size",
        "max_bytes_per_batch",
        "max_text_size",
        "max_binary_size",
        "fetch_concurrently",
        "query_timeout_sec",
        "connection_string",
        "dbms_name",
        "json_serializer",
        "json_deserializer",
        "enable_events",
        "on_connection_create",
    ),
    "asyncmy": ("json_serializer", "json_deserializer", "on_connection_create", "enable_events", "events_backend"),
    "bigquery": (
        "connection_instance",
        "on_connection_create",
        "json_serializer",
        "enable_uuid_conversion",
        "enable_events",
        "events_backend",
        "job_retry_deadline",
        "job_result_timeout",
        "use_query_and_wait",
        "request_timeout",
        "query_page_size",
        "query_max_results",
        "enable_storage_write_api",
    ),
    "duckdb": (
        "extensions",
        "secrets",
        "on_connection_create",
        "json_serializer",
        "enable_uuid_conversion",
        "extension_flags",
        "enable_events",
        "events_backend",
    ),
    "mssql_python": ("use_pool", "json_serializer", "json_deserializer", "on_connection_create", "enable_events"),
    "mysqlconnector": (
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "cursor_options",
        "enable_local_infile_bulk_load",
    ),
    "psqlpy": (
        "enable_cast_detection",
        "enable_pgvector",
        "enable_paradedb",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
    ),
    "psycopg": (
        "enable_pgvector",
        "enable_paradedb",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "enable_alloydb",
        "alloydb_instance_uri",
        "enable_alloydb_iam_auth",
        "alloydb_ip_type",
    ),
    "pymssql": ("json_serializer", "json_deserializer", "on_connection_create", "enable_events", "events_backend"),
    "pymysql": (
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "enable_local_infile_bulk_load",
        "enable_cloud_sql",
        "cloud_sql_instance",
        "cloud_sql_enable_iam_auth",
        "cloud_sql_ip_type",
    ),
    "spanner": (
        "enable_uuid_conversion",
        "json_serializer",
        "json_deserializer",
        "retry",
        "timeout",
        "request_options",
        "directed_read_options",
        "session_labels",
        "enable_events",
        "events_backend",
        "enable_batch_write_api",
    ),
    "sqlite": (
        "enable_custom_adapters",
        "json_serializer",
        "json_deserializer",
        "on_connection_create",
        "enable_events",
        "events_backend",
        "custom_functions",
        "custom_collations",
        "custom_aggregates",
        "authorizer_callback",
        "trace_callback",
        "progress_handler",
        "progress_handler_interval",
        "row_factory",
        "text_factory",
        "pragmas",
        "extensions",
    ),
})

DRIVER_FEATURE_CONTRACT_GROUPS: dict[str, DriverFeatureContractGroup] = {
    "feature_honesty": DriverFeatureContractGroup(
        description="TypedDict keys must be consumed by the owning adapter implementation.",
        enabled=True,
        enabled_adapters=_MIGRATED_DRIVER_FEATURE_ADAPTERS,
    ),
    "enable_false_semantics": DriverFeatureContractGroup(
        description="enable_*=False must be inert and not leak process-global state.",
        enabled=True,
        enabled_adapters=("aiosqlite", "sqlite"),
    ),
    "parity": DriverFeatureContractGroup(
        description="Buffered, streamed, and Arrow values must stay equal on canonical typed fixtures.",
        enabled=True,
        enabled_adapters=(
            "adbc",
            "aiomysql",
            "arrow_odbc",
            "asyncmy",
            "bigquery",
            "duckdb",
            "mysqlconnector",
            "pymysql",
            "spanner",
        ),
    ),
    "row_format": DriverFeatureContractGroup(
        description="The first materialized row shape must match the row_format tag.",
        enabled=True,
        enabled_adapters=("aiosqlite", "psycopg", "sqlite"),
    ),
}

DRIVER_FEATURE_CONTRACT_ENABLED: dict[str, bool] = {
    group_name: group.enabled for group_name, group in DRIVER_FEATURE_CONTRACT_GROUPS.items()
}

SOURCE_EQUIVALENCE_CASES: tuple[SourceEquivalenceCase, ...] = (
    SourceEquivalenceCase(
        case_id="mysql_four_way_format_identifier",
        group="mysql_four_way",
        symbol="format_identifier",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:format_identifier",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_build_insert_statement",
        group="mysql_four_way",
        symbol="build_insert_statement",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:build_insert_statement",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_normalize_execute_parameters",
        group="mysql_four_way",
        symbol="normalize_execute_parameters",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:normalize_execute_parameters",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_normalize_execute_many_parameters",
        group="mysql_four_way",
        symbol="normalize_execute_many_parameters",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:normalize_execute_many_parameters",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_resolve_column_names",
        group="mysql_four_way",
        symbol="resolve_column_names",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:resolve_column_names",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_detect_json_columns_from_description",
        group="mysql_four_way",
        symbol="detect_json_columns_from_description",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:detect_json_columns_from_description",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_detect_json_columns",
        group="mysql_four_way",
        symbol="detect_json_columns",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:detect_json_columns",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_resolve_row_plan",
        group="mysql_four_way",
        symbol="resolve_row_plan",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:resolve_row_plan",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_collect_rows",
        group="mysql_four_way",
        symbol="collect_rows",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:collect_rows",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_collect_stream_rows",
        group="mysql_four_way",
        symbol="collect_stream_rows",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:collect_stream_rows",
    ),
    SourceEquivalenceCase(
        case_id="mysql_four_way_create_mapped_exception",
        group="mysql_four_way",
        symbol="create_mapped_exception",
        module_paths=(
            "sqlspec.adapters.aiomysql.core",
            "sqlspec.adapters.asyncmy.core",
            "sqlspec.adapters.mysqlconnector.core",
            "sqlspec.adapters.pymysql.core",
        ),
        allowlist_key="mysql_four_way:create_mapped_exception",
    ),
    SourceEquivalenceCase(
        case_id="sqlite_pair_apply_driver_features",
        group="sqlite_pair",
        symbol="apply_driver_features",
        module_paths=("sqlspec.adapters.sqlite.core", "sqlspec.adapters.aiosqlite.core"),
        allowlist_key="sqlite_pair:apply_driver_features",
    ),
    SourceEquivalenceCase(
        case_id="sqlite_pair_build_insert_statement",
        group="sqlite_pair",
        symbol="build_insert_statement",
        module_paths=("sqlspec.adapters.sqlite.core", "sqlspec.adapters.aiosqlite.core"),
        allowlist_key="sqlite_pair:build_insert_statement",
    ),
    SourceEquivalenceCase(
        case_id="sqlite_pair_normalize_execute_parameters",
        group="sqlite_pair",
        symbol="normalize_execute_parameters",
        module_paths=("sqlspec.adapters.sqlite.core", "sqlspec.adapters.aiosqlite.core"),
        allowlist_key="sqlite_pair:normalize_execute_parameters",
    ),
    SourceEquivalenceCase(
        case_id="sqlite_pair_normalize_execute_many_parameters",
        group="sqlite_pair",
        symbol="normalize_execute_many_parameters",
        module_paths=("sqlspec.adapters.sqlite.core", "sqlspec.adapters.aiosqlite.core"),
        allowlist_key="sqlite_pair:normalize_execute_many_parameters",
    ),
    SourceEquivalenceCase(
        case_id="mssql_pair_create_mapped_exception",
        group="mssql_pair",
        symbol="create_mapped_exception",
        module_paths=("sqlspec.adapters.mssql_python.core", "sqlspec.adapters.pymssql.core"),
        allowlist_key="mssql_pair:create_mapped_exception",
    ),
)

SOURCE_EQUIVALENCE_ALLOWLIST: dict[str, frozenset[str]] = {
    case.allowlist_key: frozenset() for case in SOURCE_EQUIVALENCE_CASES
}
# mysqlconnector intentionally resolves error codes from errno instead of args[0].
SOURCE_EQUIVALENCE_ALLOWLIST["mysql_four_way:create_mapped_exception"] = frozenset({
    "sqlspec.adapters.mysqlconnector.core"
})


def _load_symbol(module_path: str, symbol_name: str) -> Any:
    try:
        module = importlib.import_module(module_path)
        return getattr(module, symbol_name)
    except (AttributeError, ImportError) as exc:
        msg = f"Unable to load {symbol_name} from {module_path}: {exc}"
        raise AssertionError(msg) from exc


def get_driver_feature_keys(adapter: str) -> tuple[str, ...]:
    """Return the canonical DriverFeatures keys declared by an adapter TypedDict."""
    module_path, typed_dict_name = DRIVER_FEATURE_TYPED_DICTS[adapter]
    typed_dict = _load_symbol(module_path, typed_dict_name)
    annotations = getattr(typed_dict, "__annotations__", None)
    if not isinstance(annotations, dict):
        msg = f"{typed_dict_name} in {module_path} does not expose TypedDict annotations"
        raise AssertionError(msg)
    return tuple(annotations)


def get_driver_feature_consumed_keys(adapter: str) -> tuple[str, ...]:
    """Return the registry-tracked keys consumed by an adapter."""
    return DRIVER_FEATURE_CONSUMED_KEYS[adapter]


class _DocstringStripper(ast.NodeTransformer):
    def _strip_body(self, body: list[ast.stmt]) -> list[ast.stmt]:
        if body and isinstance(body[0], ast.Expr):
            value = body[0].value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                return body[1:]
        return body

    def visit_Module(self, node: ast.Module) -> ast.Module:
        node.body = self._strip_body(node.body)
        return cast("ast.Module", self.generic_visit(node))

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        node.body = self._strip_body(node.body)
        return cast("ast.FunctionDef", self.generic_visit(node))

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AsyncFunctionDef:
        node.body = self._strip_body(node.body)
        return cast("ast.AsyncFunctionDef", self.generic_visit(node))

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.ClassDef:
        node.body = self._strip_body(node.body)
        return cast("ast.ClassDef", self.generic_visit(node))


def normalize_source(source: str) -> str:
    """Normalize Python source so source-equality checks ignore docstrings and formatting."""
    normalized = textwrap.dedent(source).strip()
    tree = ast.parse(normalized)
    stripped = _DocstringStripper().visit(tree)
    ast.fix_missing_locations(stripped)
    return ast.unparse(stripped).strip()


def _load_symbol_source(module_path: str, symbol_name: str) -> str:
    symbol = _load_symbol(module_path, symbol_name)
    try:
        return inspect.getsource(symbol)
    except OSError as exc:
        msg = f"Unable to read source for {symbol_name} from {module_path}: {exc}"
        raise AssertionError(msg) from exc


def assert_source_equivalent(case: SourceEquivalenceCase) -> None:
    """Assert that every unallowlisted module exports the same normalized source."""
    allowed_modules = SOURCE_EQUIVALENCE_ALLOWLIST.get(case.allowlist_key, frozenset())
    normalized_sources: dict[str, str] = {}
    for module_path in case.module_paths:
        if module_path in allowed_modules:
            continue
        normalized_sources[module_path] = normalize_source(_load_symbol_source(module_path, case.symbol))

    if len(normalized_sources) < 2:
        msg = f"{case.case_id} does not have enough unallowlisted modules to compare"
        raise AssertionError(msg)

    baseline_module, baseline_source = next(iter(normalized_sources.items()))
    mismatches: list[str] = []
    for module_path, source in normalized_sources.items():
        if source == baseline_source:
            continue
        diff = "\n".join(
            difflib.unified_diff(
                baseline_source.splitlines(),
                source.splitlines(),
                fromfile=baseline_module,
                tofile=module_path,
                lineterm="",
            )
        )
        mismatches.append(diff)

    if mismatches:
        details = "\n\n".join(mismatches)
        msg = f"{case.case_id} source mismatch for {case.symbol}:\n{details}"
        raise AssertionError(msg)
