"""Generate Markdown reference documentation for all SQLSpec data dictionary dialects and SQL catalogs."""

import argparse
import re
import sys
from pathlib import Path

import msgspec

from sqlspec.data_dictionary._registry import get_dialect_config
from sqlspec.data_dictionary._types import DialectConfig

__all__ = (
    "DEFAULT_OUTPUT_DIR",
    "DIALECT_SPECS",
    "DialectDocSpec",
    "main",
    "render_all_dialect_docs",
    "render_dialect_index",
    "render_dialect_page",
)

DEFAULT_OUTPUT_DIR = Path("docs/reference/data-dictionary/dialects")
DIALECTS_SOURCE_DIR = Path("sqlspec/data_dictionary/dialects")

_QUERY_NAME_PATTERN = re.compile(r"^--\s*name:\s*([A-Za-z0-9_-]+)\s*$", re.MULTILINE)
_COLON_PARAM_PATTERN = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")
_AT_PARAM_PATTERN = re.compile(r"@([a-zA-Z_][a-zA-Z0-9_]*)")


class DialectDocSpec(msgspec.Struct, frozen=True, gc=False):
    """Metadata describing a data dictionary dialect pack and its adapter classes.

    Attributes:
        dialect: Canonical dialect directory name under ``sqlspec/data_dictionary/dialects/``.
        title: Human-readable engine title.
        ddl_strategy: Primary DDL extraction strategy description.
        guide_link: Relative link to the corresponding database guide.
        summary: Concise description of the dialect's system catalog introspection.
        adapter_classes: Fully qualified adapter DataDictionary class paths.
    """

    dialect: str
    title: str
    ddl_strategy: str
    guide_link: str
    summary: str
    adapter_classes: tuple[str, ...]


DIALECT_SPECS: tuple[DialectDocSpec, ...] = (
    DialectDocSpec(
        dialect="postgres",
        title="PostgreSQL",
        ddl_strategy="AST synthesis + `pg_get_*def` catalog functions",
        guide_link="../../../databases/postgres/index.md",
        summary="Introspects `pg_catalog` (`pg_class`, `pg_attribute`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_type`, `pg_extension`) and `information_schema` across `asyncpg`, `psycopg`, and `psqlpy`.",
        adapter_classes=(
            "sqlspec.adapters.asyncpg.data_dictionary.AsyncpgDataDictionary",
            "sqlspec.adapters.psycopg.data_dictionary.PsycopgSyncDataDictionary",
            "sqlspec.adapters.psycopg.data_dictionary.PsycopgAsyncDataDictionary",
            "sqlspec.adapters.psqlpy.data_dictionary.PsqlpyDataDictionary",
        ),
    ),
    DialectDocSpec(
        dialect="cockroachdb",
        title="CockroachDB",
        ddl_strategy="Native `crdb_internal.create_statement` + AST fallback",
        guide_link="../../../databases/cockroachdb.md",
        summary="Introspects `crdb_internal` (`create_statements`, `tables`) and `pg_catalog` compatibility views across `cockroach_asyncpg` and `cockroach_psycopg`.",
        adapter_classes=(
            "sqlspec.adapters.cockroach_asyncpg.data_dictionary.CockroachAsyncpgDataDictionary",
            "sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgSyncDataDictionary",
            "sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgAsyncDataDictionary",
        ),
    ),
    DialectDocSpec(
        dialect="sqlite",
        title="SQLite",
        ddl_strategy="Native `sqlite_master.sql` + `PRAGMA` synthesis",
        guide_link="../../../databases/sqlite.md",
        summary="Introspects `sqlite_master` and table-valued `pragma_table_xinfo`, `pragma_index_list`, `pragma_index_info`, and `pragma_foreign_key_list` functions across `sqlite` and `aiosqlite`.",
        adapter_classes=(
            "sqlspec.adapters.sqlite.data_dictionary.SqliteDataDictionary",
            "sqlspec.adapters.aiosqlite.data_dictionary.AiosqliteDataDictionary",
        ),
    ),
    DialectDocSpec(
        dialect="duckdb",
        title="DuckDB",
        ddl_strategy="Native `duckdb_*().sql` catalog functions + AST synthesis",
        guide_link="../../../databases/duckdb.md",
        summary="Introspects DuckDB table functions (`duckdb_tables()`, `duckdb_columns()`, `duckdb_indexes()`, `duckdb_constraints()`, `duckdb_views()`, `duckdb_sequences()`, `duckdb_types()`, `duckdb_extensions()`).",
        adapter_classes=(
            "sqlspec.adapters.duckdb.data_dictionary.DuckDBDataDictionary",
            "sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary",
            "sqlspec.adapters.arrow_odbc.data_dictionary.ArrowOdbcDataDictionary",
        ),
    ),
    DialectDocSpec(
        dialect="mysql",
        title="MySQL",
        ddl_strategy="Native `SHOW CREATE` + `information_schema` synthesis",
        guide_link="../../../databases/mysql.md",
        summary="Introspects `information_schema` (`TABLES`, `COLUMNS`, `STATISTICS`, `KEY_COLUMN_USAGE`, `REFERENTIAL_CONSTRAINTS`, `CHECK_CONSTRAINTS`, `ROUTINES`, `TRIGGERS`) across `asyncmy`, `aiomysql`, `pymysql`, and `mysqlconnector`.",
        adapter_classes=(
            "sqlspec.adapters.asyncmy.data_dictionary.AsyncmyDataDictionary",
            "sqlspec.adapters.aiomysql.data_dictionary.AiomysqlDataDictionary",
            "sqlspec.adapters.pymysql.data_dictionary.PyMysqlDataDictionary",
            "sqlspec.adapters.mysqlconnector.data_dictionary.MysqlConnectorSyncDataDictionary",
            "sqlspec.adapters.mysqlconnector.data_dictionary.MysqlConnectorAsyncDataDictionary",
        ),
    ),
    DialectDocSpec(
        dialect="mariadb",
        title="MariaDB",
        ddl_strategy="Native `SHOW CREATE` + MariaDB sequence & constraint reflection",
        guide_link="../../../databases/mysql.md",
        summary="Extends MySQL `information_schema` catalog queries with MariaDB native `SEQUENCE` objects (`TABLE_TYPE = 'SEQUENCE'`), native `UUID` / `VECTOR` types, and MariaDB version detection.",
        adapter_classes=(),
    ),
    DialectDocSpec(
        dialect="oracle",
        title="Oracle Database",
        ddl_strategy="Native `DBMS_METADATA.GET_DDL` + AST fallback",
        guide_link="../../../databases/oracledb.md",
        summary="Introspects `ALL_TABLES`, `ALL_TAB_COLS`, `ALL_INDEXES`, `ALL_IND_COLUMNS`, `ALL_CONSTRAINTS`, `ALL_CONS_COLUMNS`, `ALL_VIEWS`, `ALL_OBJECTS`, `ALL_TRIGGERS`, `ALL_SEQUENCES`, and `DBMS_METADATA`.",
        adapter_classes=(
            "sqlspec.adapters.oracledb.data_dictionary.OracledbSyncDataDictionary",
            "sqlspec.adapters.oracledb.data_dictionary.OracledbAsyncDataDictionary",
            "sqlspec.adapters.oracledb.data_dictionary.OracleVersionInfo",
            "sqlspec.adapters.oracledb.data_dictionary.JSONStorageType",
        ),
    ),
    DialectDocSpec(
        dialect="mssql",
        title="SQL Server",
        ddl_strategy="AST synthesis + `OBJECT_DEFINITION()` for views/routines/triggers",
        guide_link="../../../databases/mssql.md",
        summary="Introspects `sys.tables`, `sys.columns`, `sys.types`, `sys.indexes`, `sys.foreign_keys`, `sys.check_constraints`, `sys.default_constraints`, `sys.sequences`, and `sys.sql_modules` across `mssql_python` and `pymssql`.",
        adapter_classes=(
            "sqlspec.adapters.mssql_python.data_dictionary.MssqlPythonSyncDataDictionary",
            "sqlspec.adapters.pymssql.data_dictionary.PymssqlSyncDataDictionary",
        ),
    ),
    DialectDocSpec(
        dialect="sybase",
        title="Sybase ASE",
        ddl_strategy="AST synthesis + `dbo.syscomments` object text reassembly",
        guide_link="../../../databases/sybase.md",
        summary="Introspects SAP Adaptive Server Enterprise system tables (`dbo.sysobjects`, `dbo.syscolumns`, `dbo.sysindexes`, `dbo.sysreferences`, `dbo.syscomments`, `dbo.systypes`, `dbo.sysusers`).",
        adapter_classes=("sqlspec.adapters.sybase.data_dictionary.SybaseSyncDataDictionary",),
    ),
    DialectDocSpec(
        dialect="sqlanywhere",
        title="SAP SQL Anywhere",
        ddl_strategy="AST synthesis + `SYS.SYSTAB` / `SYS.SYSPROCEDURE` definitions",
        guide_link="../../../databases/sqlanywhere.md",
        summary="Introspects SAP SQL Anywhere `SYS` catalog views (`SYS.SYSTAB`, `SYS.SYSTABCOL`, `SYS.SYSIDX`, `SYS.SYSIDXCOL`, `SYS.SYSFKEY`, `SYS.SYSCONSTRAINT`, `SYS.SYSVIEW`, `SYS.SYSPROCEDURE`, `SYS.SYSTRIGGER`, `SYS.SYSSEQUENCE`).",
        adapter_classes=("sqlspec.adapters.sqlanywhere.data_dictionary.SqlanywhereSyncDataDictionary",),
    ),
    DialectDocSpec(
        dialect="db2",
        title="IBM Db2",
        ddl_strategy="AST synthesis + `SYSCAT` catalog text definitions",
        guide_link="../../../databases/db2.md",
        summary="Introspects IBM Db2 `SYSCAT` views (`SYSCAT.TABLES`, `SYSCAT.COLUMNS`, `SYSCAT.INDEXES`, `SYSCAT.REFERENCES`, `SYSCAT.TABCONST`, `SYSCAT.VIEWS`, `SYSCAT.ROUTINES`, `SYSCAT.TRIGGERS`, `SYSCAT.SEQUENCES`, `SYSCAT.DATATYPES`).",
        adapter_classes=(
            "sqlspec.adapters.db2.data_dictionary.Db2SyncDataDictionary",
            "sqlspec.adapters.db2.data_dictionary.Db2AsyncDataDictionary",
        ),
    ),
    DialectDocSpec(
        dialect="bigquery",
        title="Google BigQuery",
        ddl_strategy="Native `INFORMATION_SCHEMA.TABLES.ddl` + AST fallback",
        guide_link="../../../databases/bigquery.md",
        summary="Introspects dataset-scoped `INFORMATION_SCHEMA` views (`TABLES`, `COLUMNS`, `COLUMN_FIELD_PATHS`, `TABLE_OPTIONS`, `TABLE_CONSTRAINTS`, `KEY_COLUMN_USAGE`, `CONSTRAINT_COLUMN_USAGE`, `VIEWS`, `ROUTINES`).",
        adapter_classes=("sqlspec.adapters.bigquery.data_dictionary.BigQueryDataDictionary",),
    ),
    DialectDocSpec(
        dialect="spanner",
        title="Google Cloud Spanner",
        ddl_strategy="AST synthesis with `INTERLEAVE IN PARENT` & `ROW DELETION POLICY` (`GoogleSQL` & `PostgreSQL` modes)",
        guide_link="../../../databases/spanner.md",
        summary="Provides dual SQL catalog packs (`googlesql` and `postgresql`) introspecting `INFORMATION_SCHEMA` tables, columns, indexes (`STORING`, `SEARCH`, `VECTOR`), foreign keys, parent-child interleaving, and row deletion policies.",
        adapter_classes=(
            "sqlspec.adapters.spanner.data_dictionary.SpannerSyncDataDictionary",
            "sqlspec.adapters.spanner.data_dictionary.SpannerAsyncDataDictionary",
        ),
    ),
)


def render_all_dialect_docs() -> dict[str, str]:
    """Render the dialect index and all 13 per-dialect Markdown pages.

    Returns:
        Mapping of filename (such as ``"index.md"`` or ``"postgres.md"``) to Markdown content.
    """
    rendered: dict[str, str] = {"index.md": render_dialect_index()}
    for spec in DIALECT_SPECS:
        rendered[f"{spec.dialect}.md"] = render_dialect_page(spec)
    return rendered


def render_dialect_index() -> str:
    """Render the cross-dialect overview and SQL catalog matrix page.

    Returns:
        Markdown string for ``docs/reference/data-dictionary/dialects/index.md``.
    """
    lines: list[str] = [
        "# Dialect SQL Catalogs",
        "",
        "```python",
        "from sqlspec.data_dictionary import DialectConfig, get_data_dictionary_loader, get_dialect_config",
        "```",
        "",
        "SQLSpec ships 13 dialect-specific SQL catalog packs under `sqlspec/data_dictionary/dialects/` containing named [`SQLFileLoader`](../../loader.md) queries (`-- name: ...`), version-gated feature profiles (`DialectConfig`), and logical-to-physical type mappings. See the [Data Dictionary Guide](../../../learn/data-dictionary.md) and [Data Dictionary Reference](../../data-dictionary.md) for programmatic usage.",
        "",
        "## Dialect Catalog Matrix",
        "",
        "| Dialect | Default Schema | DDL Resolution | Domain `.sql` Files | Named Queries | Reference |",
        "| --- | --- | --- | --- | --- | --- |",
    ]

    total_files = 0
    total_queries = 0
    for spec in DIALECT_SPECS:
        config = get_dialect_config(spec.dialect)
        sql_files = _collect_sql_files(spec.dialect)
        file_count = len(sql_files)
        query_count = sum(len(_extract_query_names(path.read_text(encoding="utf-8"))) for _, path in sql_files)
        total_files += file_count
        total_queries += query_count
        default_schema = f"`{config.default_schema}`" if config.default_schema else "`None`"
        lines.append(
            f"| **{spec.title}** (`{spec.dialect}`) | {default_schema} | {spec.ddl_strategy} | `{file_count}` | `{query_count}` | [{spec.title} Catalog]({spec.dialect}.md) |"
        )

    lines.extend([
        f"| **Total (`{len(DIALECT_SPECS)}` dialects)** | - | - | **`{total_files}`** | **`{total_queries}`** | - |",
        "",
        "## Cross-Dialect Capability Matrix",
        "",
        "Each dialect pack declares boolean `feature_flags` and version-gated `feature_versions` inside its `DialectConfig`:",
        "",
        "| Dialect | Schemas | Returning | Upsert | Window | CTE | JSON | Arrays | Vectors | Transactions |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ])

    feature_cols = (
        "supports_schemas",
        "supports_returning",
        "supports_upsert",
        "supports_window_functions",
        "supports_cte",
        "supports_json",
        "supports_arrays",
        "supports_vectors",
        "supports_transactions",
    )
    for spec in DIALECT_SPECS:
        config = get_dialect_config(spec.dialect)
        cells = [_format_capability_cell(config, feat) for feat in feature_cols]
        lines.append(f"| [{spec.title}]({spec.dialect}.md) | " + " | ".join(cells) + " |")

    lines.extend([
        "",
        "## Logical-to-Physical Type Mappings",
        "",
        "`driver.data_dictionary.get_optimal_type(driver, type_category)` resolves portable logical type categories using each dialect's `DialectConfig.type_mappings` and active engine version:",
        "",
        "| Dialect | `json` | `uuid` | `boolean` | `timestamp` | `text` | `blob` | `vector` |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ])

    type_cols = ("json", "uuid", "boolean", "timestamp", "text", "blob", "vector")
    for spec in DIALECT_SPECS:
        config = get_dialect_config(spec.dialect)
        cells = [f"`{config.type_mappings.get(col, '-')}`" for col in type_cols]
        lines.append(f"| [{spec.title}]({spec.dialect}.md) | " + " | ".join(cells) + " |")

    return "\n".join(lines).rstrip() + "\n"


def render_dialect_page(spec: DialectDocSpec) -> str:
    """Render a single dialect's configuration, adapter classes, and SQL catalog queries.

    Args:
        spec: Dialect documentation specification.

    Returns:
        Markdown string for ``docs/reference/data-dictionary/dialects/<dialect>.md``.
    """
    config = get_dialect_config(spec.dialect)
    sql_files = _collect_sql_files(spec.dialect)
    total_queries = sum(len(_extract_query_names(path.read_text(encoding="utf-8"))) for _, path in sql_files)
    default_schema = f'`"{config.default_schema}"`' if config.default_schema else "`None`"

    lines: list[str] = [
        f"# {spec.title} Data Dictionary & SQL Catalog",
        "",
        "```python",
        f'from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config\n\nconfig = get_dialect_config("{spec.dialect}")',
        "```",
        "",
        f"{spec.summary} See the [{spec.title} Guide]({spec.guide_link}) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.",
        "",
        "## Dialect Configuration",
        "",
        "| Setting | Value |",
        "| --- | --- |",
        f"| **Dialect Name** | `{config.name}` |",
        f"| **Default Schema** | {default_schema} |",
        f"| **Version Pattern** | `{config.version_pattern.pattern}` |",
        f"| **DDL Strategy** | {spec.ddl_strategy} |",
        f"| **Catalog `.sql` Files** | `{len(sql_files)}` files (`{total_queries}` named queries) |",
        "",
    ]

    lines.extend(_render_feature_tables(config))

    if spec.adapter_classes:
        lines.extend(["## Adapter Data Dictionary Classes", ""])
        for cls_path in spec.adapter_classes:
            lines.extend([f"::: {cls_path}", "    options:", "      heading_level: 3", ""])

    lines.extend([
        "## Catalog SQL Queries",
        "",
        f"The `{spec.dialect}` dialect pack defines `{total_queries}` named queries across `{len(sql_files)}` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/{spec.dialect}/`):",
        "",
        "| Domain Pack | Named Queries | Bind Parameters |",
        "| --- | --- | --- |",
    ])

    for label, path in sql_files:
        text = path.read_text(encoding="utf-8")
        query_names = _extract_query_names(text)
        params = _extract_bind_params(text)
        queries_cell = ", ".join(f"`{name}`" for name in query_names) if query_names else "-"
        params_cell = ", ".join(f"`{p}`" for p in params) if params else "*none*"
        lines.append(f"| `{label}` | {queries_cell} | {params_cell} |")

    lines.append("")

    for label, path in sql_files:
        text = path.read_text(encoding="utf-8").strip()
        query_names = _extract_query_names(text)
        lines.extend([f'??? code "`{label}` ({len(query_names)} named queries)"', "", "    ```sql"])
        lines.extend(f"    {sql_line}" if sql_line else "" for sql_line in text.splitlines())
        lines.extend(["    ```", ""])

    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for generating or checking Data Dictionary dialect reference pages.

    Args:
        argv: Optional command-line arguments.

    Returns:
        Process exit code (0 on success, 1 when stale or missing under ``--check``).
    """
    parser = argparse.ArgumentParser(
        description="Generate or check docs/reference/data-dictionary/dialects/*.md reference pages."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Target directory for generated dialect Markdown pages.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify that all generated dialect Markdown files are up to date without writing.",
    )
    args = parser.parse_args(argv)
    output_dir: Path = args.output_dir
    expected_files = render_all_dialect_docs()

    if args.check:
        stale: list[str] = []
        for filename, expected in expected_files.items():
            target = output_dir / filename
            if not target.is_file():
                stale.append(f"missing: {target}")
                continue
            actual = target.read_text(encoding="utf-8")
            if actual != expected:
                stale.append(f"out of date: {target}")
        if stale:
            for item in stale:
                sys.stderr.write(f"Data dictionary dialect docs {item}\n")
            sys.stderr.write("Run 'uv run python -m tools.docs.generate_data_dictionary_docs' to regenerate.\n")
            return 1
        sys.stdout.write(
            f"Data dictionary dialect docs are up to date ({len(expected_files)} files in {output_dir}).\n"
        )
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in expected_files.items():
        (output_dir / filename).write_text(content, encoding="utf-8")
    sys.stdout.write(f"Wrote {len(expected_files)} data dictionary dialect docs to {output_dir}\n")
    return 0


def _collect_sql_files(dialect: str) -> list[tuple[str, Path]]:
    """Collect all domain .sql files for a dialect in deterministic order."""
    base_dir = DIALECTS_SOURCE_DIR / dialect
    sql_dir = base_dir / "sql"
    if sql_dir.is_dir():
        return [(path.name, path) for path in sorted(sql_dir.glob("*.sql"))]

    collected: list[tuple[str, Path]] = []
    for submode_dir in sorted(p for p in base_dir.iterdir() if p.is_dir() and (p / "sql").is_dir()):
        collected.extend(
            (f"{submode_dir.name}/{path.name}", path) for path in sorted((submode_dir / "sql").glob("*.sql"))
        )
    return collected


def _extract_query_names(sql_text: str) -> list[str]:
    """Extract all -- name: identifiers from a SQL catalog file."""
    return _QUERY_NAME_PATTERN.findall(sql_text)


def _extract_bind_params(sql_text: str) -> list[str]:
    """Extract sorted bind parameter placeholders (:param or @param) from SQL text."""
    lines_without_comments = [line for line in sql_text.splitlines() if not line.lstrip().startswith("--")]
    body = "\n".join(lines_without_comments)
    params: set[str] = set()
    for match in _COLON_PARAM_PATTERN.findall(body):
        params.add(f":{match}")
    for match in _AT_PARAM_PATTERN.findall(body):
        params.add(f"@{match}")
    return sorted(params)


def _format_capability_cell(config: DialectConfig, feature: str) -> str:
    """Format a capability matrix cell combining static feature_flags and version gates."""
    if feature in config.feature_versions:
        return f"`>={config.feature_versions[feature]}`"
    if feature in config.feature_flags:
        return "`Yes`" if config.feature_flags[feature] else "`No`"
    return "`No`"


def _render_feature_tables(config: DialectConfig) -> list[str]:
    """Render Markdown tables for feature flags, version gates, and type mappings."""
    lines: list[str] = [
        "### Capabilities & Type Mappings",
        "",
        "| Capability | Status / Minimum Version | Logical Type | Target Physical Type |",
        "| --- | --- | --- | --- |",
    ]
    all_features: list[tuple[str, str]] = []
    for key, flag in sorted(config.feature_flags.items()):
        all_features.append((f"`{key}`", "`True`" if flag else "`False`"))
    for key, ver in sorted(config.feature_versions.items()):
        all_features.append((f"`{key}`", f"`>= {ver}`"))

    all_types = [(f"`{k}`", f"`{v}`") for k, v in sorted(config.type_mappings.items())]
    max_rows = max(len(all_features), len(all_types), 1)
    for idx in range(max_rows):
        feat_name, feat_val = all_features[idx] if idx < len(all_features) else ("-", "-")
        type_name, type_val = all_types[idx] if idx < len(all_types) else ("-", "-")
        lines.append(f"| {feat_name} | {feat_val} | {type_name} | {type_val} |")
    lines.append("")
    return lines


if __name__ == "__main__":
    raise SystemExit(main())
