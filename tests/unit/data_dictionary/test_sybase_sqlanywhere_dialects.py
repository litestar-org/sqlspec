"""Unit tests for Sybase ASE and SAP SQL Anywhere data dictionary configurations and SQL packs."""

import sqlite3
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

import pytest

from sqlspec.data_dictionary import DataDictionaryLoader, TableMetadata, VersionInfo, get_dialect_config
from sqlspec.data_dictionary.dialects import SQLANYWHERE_CONFIG, SYBASE_CONFIG
from sqlspec.data_dictionary.dialects.sqlanywhere import (
    SQLANYWHERE_METADATA_DOMAINS,
    SqlanywhereVersionInfo,
    build_sqlanywhere_metadata_capability_profile,
    extract_sqlanywhere_version_value,
    list_sqlanywhere_available_features,
    merge_sqlanywhere_table_lists,
    resolve_sqlanywhere_feature_flag,
)
from sqlspec.data_dictionary.dialects.sybase import (
    SYBASE_METADATA_DOMAINS,
    SybaseVersionInfo,
    build_sybase_metadata_capability_profile,
    extract_sybase_version_value,
    list_sybase_available_features,
    merge_sybase_table_lists,
    resolve_sybase_feature_flag,
)

if TYPE_CHECKING:
    from sqlspec.adapters.sybase import SybaseDriver


_CursorT = TypeVar("_CursorT", bound=sqlite3.Cursor)


class SybaseCatalogCursor(sqlite3.Cursor):
    """Execute the real driver's bound catalog queries on a local DB-API substitute."""

    def execute(self, sql: str, parameters: Any = ()) -> "SybaseCatalogCursor":
        super().execute(sql.replace("%s", "?"), parameters)
        return self


class SybaseCatalogConnection(sqlite3.Connection):
    """Provide a pymssql-compatible cursor over the local catalog fixture."""

    @overload
    def cursor(self, factory: None = None) -> SybaseCatalogCursor: ...

    @overload
    def cursor(self, factory: Callable[[sqlite3.Connection], _CursorT]) -> _CursorT: ...

    def cursor(
        self, factory: Callable[[sqlite3.Connection], sqlite3.Cursor] | None = SybaseCatalogCursor
    ) -> sqlite3.Cursor:
        return super().cursor(factory=factory)


@pytest.fixture
def sybase_catalog_driver() -> "Iterator[tuple[SybaseDriver, sqlite3.Connection]]":
    """Substitute only the native DB-API boundary while exercising real catalog SQL."""
    pytest.importorskip("pymssql")
    from sqlspec.adapters.sybase import SybaseDriver

    connection = sqlite3.connect(":memory:", factory=SybaseCatalogConnection)
    connection.create_function("USER_NAME", -1, lambda *_args: "dbo")
    connection.create_function("DB_NAME", 0, lambda: "app")
    connection.create_function("index_col", 3, lambda *_args: None)
    key_columns = ", ".join(f"fokey{position} INTEGER, refkey{position} INTEGER" for position in range(1, 17))
    connection.executescript("""
        ATTACH DATABASE ':memory:' AS dbo;
        CREATE TABLE dbo.sysusers (uid INTEGER, name TEXT);
        CREATE TABLE dbo.sysobjects (id INTEGER, uid INTEGER, name TEXT, type TEXT, sysstat INTEGER);
        CREATE TABLE dbo.systypes (usertype INTEGER, name TEXT);
        CREATE TABLE dbo.syscolumns (
            id INTEGER, name TEXT, usertype INTEGER, status INTEGER DEFAULT 0,
            status2 INTEGER DEFAULT 0, status3 INTEGER DEFAULT 0, cdefault INTEGER DEFAULT 0,
            computedcol INTEGER DEFAULT 0, colid INTEGER, length INTEGER DEFAULT 4,
            prec INTEGER DEFAULT 10, scale INTEGER DEFAULT 0
        );
        CREATE TABLE dbo.syscomments (
            id INTEGER, text TEXT, number INTEGER DEFAULT 0, colid2 INTEGER DEFAULT 0,
            colid INTEGER DEFAULT 1, texttype INTEGER DEFAULT 0, status INTEGER DEFAULT 0
        );
        CREATE TABLE dbo.sysconstraints (tableid INTEGER, constrid INTEGER, status INTEGER);
        CREATE TABLE dbo.sysindexes (id INTEGER, name TEXT, indid INTEGER, status INTEGER);
        INSERT INTO dbo.sysusers VALUES (1, 'dbo'), (2, 'audit');
        INSERT INTO dbo.systypes VALUES (1, 'int');
    """)
    connection.execute(
        "CREATE TABLE dbo.sysreferences (tableid INTEGER, reftabid INTEGER, constrid INTEGER, "
        "keycnt INTEGER, pmrydbname TEXT, frgndbname TEXT, " + key_columns + ")"
    )
    try:
        yield SybaseDriver(connection=cast("Any", connection)), connection
    finally:
        connection.close()


@pytest.mark.parametrize("table", [None, "items"])
def test_sybase_catalog_columns_preserve_flags_and_complete_expressions(
    sybase_catalog_driver: "tuple[SybaseDriver, sqlite3.Connection]", table: str | None
) -> None:
    """Reflect writable flags and every definition chunk through both column queries."""
    driver, connection = sybase_catalog_driver
    connection.execute("INSERT INTO dbo.sysobjects VALUES (10, 1, 'items', 'U', 3)")
    connection.executemany(
        "INSERT INTO dbo.syscolumns (id, name, usertype, colid, status, status2, status3, cdefault, computedcol) "
        "VALUES (10, ?, 1, ?, ?, ?, ?, ?, ?)",
        [
            ("id", 1, 128, 0, 0, 0, 0),
            ("note", 2, 8, 0, 0, 200, 0),
            ("computed", 3, 0, 16, 0, 0, 400),
            ("hidden", 4, 0, 0, 1, 0, 0),
            ("materialized", 5, 0, 32, 0, 0, 500),
            ("view_computed", 6, 0, 64, 0, 0, 0),
        ],
    )
    connection.executemany(
        "INSERT INTO dbo.syscomments (id, text, colid2, colid, texttype, status) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (200, "'" + "x" * 254, 0, 1, 0, 0),
            (200, "x" * 255, 0, 2, 0, 0),
            (200, "tail'", 1, 1, 0, 0),
            (200, "user comment", 0, 1, 1, 0),
            (400, " id + ", 0, 1, 0, 0),
            (400, "1 + " * 70, 0, 2, 0, 0),
            (400, "1 + " * 70, 0, 3, 0, 0),
            (400, "0", 1, 1, 0, 0),
            (500, "obfuscated text", 0, 1, 0, 1),
        ],
    )

    columns = driver.data_dictionary.get_columns(driver, table=table, schema="dbo")

    assert [column["column_name"] for column in columns] == [
        "id",
        "note",
        "computed",
        "hidden",
        "materialized",
        "view_computed",
    ]
    assert columns[0]["is_nullable"] is False
    assert columns[0]["is_identity"] is True
    assert columns[0]["identity_generation"] == "ALWAYS"
    assert columns[1]["is_nullable"] is True
    assert columns[1]["column_default"] == "'" + "x" * 509 + "tail'"
    assert columns[2]["is_generated"] is True
    assert columns[2]["generation_expression"] == " id + " + "1 + " * 140 + "0"
    assert columns[3]["is_hidden"] is True
    assert columns[4]["is_generated"] is True
    assert columns[4]["generation_expression"] is None
    assert columns[5]["is_generated"] is True


@pytest.mark.parametrize("table", [None, "items"])
def test_sybase_catalog_constraints_filter_defaults_and_reassemble_checks(
    sybase_catalog_driver: "tuple[SybaseDriver, sqlite3.Connection]", table: str | None
) -> None:
    """Return actual constraints without defaults, computed columns, or delete triggers."""
    driver, connection = sybase_catalog_driver
    connection.executemany(
        "INSERT INTO dbo.sysobjects VALUES (?, 1, ?, ?, ?)",
        [
            (10, "items", "U", 3),
            (100, "ck_items", "R", 10),
            (101, "fk_items", "RI", 9),
            (102, "df_items", "D", 6),
            (103, "computed_items", "C", 13),
            (104, "tr_items", "TR", 8),
        ],
    )
    connection.executemany(
        "INSERT INTO dbo.sysconstraints VALUES (10, ?, ?)", [(100, 128), (101, 64), (102, 0), (103, 0), (104, 128)]
    )
    connection.executemany(
        "INSERT INTO dbo.sysindexes VALUES (10, ?, ?, ?)", [("pk_items", 1, 2048), ("uq_items", 2, 2)]
    )
    connection.executemany(
        "INSERT INTO dbo.syscomments (id, text, colid) VALUES (100, ?, ?)",
        [("CHECK (" + "id > 0 AND " * 30, 1), ("id > 0 AND " * 30, 2), ("1 = 1)", 3)],
    )
    connection.execute("INSERT INTO dbo.syscomments (id, text, texttype) VALUES (100, 'not a definition', 1)")

    result = driver.data_dictionary.get_constraints(driver, table=table, schema="dbo")

    constraints = {str(item["constraint_name"]): item for item in cast("tuple[dict[str, object], ...]", result.items)}
    assert set(constraints) == {"ck_items", "fk_items", "pk_items", "uq_items"}
    assert constraints["ck_items"]["constraint_type"] == "CHECK"
    assert constraints["ck_items"]["check_clause"] == "CHECK (" + "id > 0 AND " * 60 + "1 = 1)"
    assert constraints["fk_items"]["constraint_type"] == "FOREIGN KEY"
    assert constraints["fk_items"]["check_clause"] is None
    assert constraints["pk_items"]["constraint_type"] == "PRIMARY KEY"
    assert constraints["uq_items"]["constraint_type"] == "UNIQUE"
    assert set(constraints["ck_items"]) == {
        "schema_name",
        "table_name",
        "constraint_name",
        "constraint_type",
        "check_clause",
        "is_enforced",
    }


def test_sybase_catalog_views_preserve_definition_chunks_and_hidden_text(
    sybase_catalog_driver: "tuple[SybaseDriver, sqlite3.Connection]",
) -> None:
    """Reconstruct long views in catalog counter order without exposing hidden text."""
    driver, connection = sybase_catalog_driver
    connection.executemany(
        "INSERT INTO dbo.sysobjects VALUES (?, 1, ?, 'V', 2)",
        [(100, "items_view"), (101, "hidden_view"), (102, "empty_view")],
    )
    connection.executemany(
        "INSERT INTO dbo.syscomments (id, text, number, colid2, colid, texttype, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (100, "tail", 0, 1, 1, 0, 0),
            (100, "CREATE VIEW items_view AS " + " SELECT 1 UNION ALL" * 20, 0, 0, 1, 0, 0),
            (100, " SELECT 2 ", 0, 0, 2, 0, 0),
            (100, "ignored", 0, 0, 1, 1, 0),
            (101, "obfuscated", 0, 0, 1, 0, 1),
        ],
    )

    result = driver.data_dictionary.get_views(driver, schema="dbo")

    views = {str(item["view_name"]): item for item in cast("tuple[dict[str, object], ...]", result.items)}
    assert set(views) == {"items_view", "hidden_view", "empty_view"}
    assert views["items_view"]["definition"] == (
        "CREATE VIEW items_view AS " + " SELECT 1 UNION ALL" * 20 + " SELECT 2 tail"
    )
    assert views["hidden_view"]["definition"] is None
    assert views["empty_view"]["definition"] is None
    assert set(views["items_view"]) == {"schema_name", "view_name", "definition", "is_read_only", "is_valid"}


def test_sybase_catalog_tables_follow_foreign_keys_without_per_table_queries(
    sybase_catalog_driver: "tuple[SybaseDriver, sqlite3.Connection]",
) -> None:
    """Order roots and descendants and retain cycles, views, and cross-schema children."""
    driver, connection = sybase_catalog_driver
    connection.executemany(
        "INSERT INTO dbo.sysobjects VALUES (?, ?, ?, ?, ?)",
        [
            (10, 1, "z_parent", "U", 3),
            (11, 1, "b_child", "U", 3),
            (12, 1, "a_grandchild", "U", 3),
            (13, 1, "c_self", "U", 3),
            (14, 1, "d_cycle", "U", 3),
            (15, 1, "e_cycle", "U", 3),
            (16, 1, "f_external", "U", 3),
            (17, 2, "audit_parent", "U", 3),
            (18, 1, "a_view", "V", 2),
            (19, 1, "g_remote", "U", 3),
        ],
    )
    connection.executemany(
        "INSERT INTO dbo.syscolumns (id, name, usertype, colid) VALUES (?, 'id', 1, 1)",
        [(identifier,) for identifier in range(10, 20)],
    )
    connection.executemany(
        "INSERT INTO dbo.syscolumns (id, name, usertype, colid) VALUES (?, 'other_id', 1, 2)", [(10,), (11,)]
    )
    connection.executemany(
        "INSERT INTO dbo.sysobjects VALUES (?, 1, ?, 'RI', 9)",
        [(100 + index, "fk_" + str(index)) for index in range(8)],
    )
    connection.executemany(
        "INSERT INTO dbo.sysreferences "
        "(tableid, reftabid, constrid, keycnt, fokey1, refkey1, pmrydbname, frgndbname) "
        "VALUES (?, ?, ?, 1, 1, 1, ?, ?)",
        [
            (11, 10, 100, None, None),
            (12, 11, 101, "app", "app"),
            (13, 13, 102, None, None),
            (14, 15, 103, None, None),
            (15, 14, 104, None, None),
            (16, 17, 105, None, None),
            (19, 12, 106, "other_database", "app"),
            (11, 10, 107, None, None),
        ],
    )
    connection.execute("UPDATE dbo.sysreferences SET keycnt = 2, fokey2 = 2, refkey2 = 2 WHERE constrid = 100")
    statements: list[str] = []
    connection.set_trace_callback(statements.append)

    tables = driver.data_dictionary.get_tables(driver, schema="dbo")

    assert [row["table_name"] for row in tables] == [
        "c_self",
        "f_external",
        "g_remote",
        "z_parent",
        "b_child",
        "a_grandchild",
        "d_cycle",
        "e_cycle",
        "a_view",
    ]
    assert {row["table_name"]: row["dependency_level"] for row in tables} == {
        "z_parent": 0,
        "b_child": 1,
        "a_grandchild": 2,
        "c_self": 0,
        "d_cycle": 0,
        "e_cycle": 0,
        "f_external": 0,
        "a_view": 0,
        "g_remote": 0,
    }
    assert all(row["level"] == row["dependency_level"] for row in tables)
    assert len([sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]) == 3
    foreign_keys = driver.data_dictionary.get_foreign_keys(driver, table="b_child", schema="dbo")
    assert [(key.table_name, key.referenced_table) for key in foreign_keys] == [
        ("b_child", "z_parent"),
        ("b_child", "z_parent"),
        ("b_child", "z_parent"),
    ]


def test_sybase_catalog_empty_schema(sybase_catalog_driver: "tuple[SybaseDriver, sqlite3.Connection]") -> None:
    """Reflect an empty schema without an unnecessary foreign-key query."""
    driver, connection = sybase_catalog_driver
    statements: list[str] = []
    connection.set_trace_callback(statements.append)

    assert driver.data_dictionary.get_tables(driver, schema="dbo") == []
    assert driver.data_dictionary.get_columns(driver, schema="dbo") == []
    assert driver.data_dictionary.get_constraints(driver, schema="dbo").items == ()
    assert driver.data_dictionary.get_views(driver, schema="dbo").items == ()
    assert len([sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]) == 5


def test_sybase_and_sqlanywhere_dialect_configs_registered() -> None:
    """Both sybase and sqlanywhere dialect configs should resolve by canonical name and alias."""
    assert get_dialect_config("sybase") is SYBASE_CONFIG
    assert get_dialect_config("ase") is SYBASE_CONFIG
    assert get_dialect_config("sqlanywhere") is SQLANYWHERE_CONFIG
    assert get_dialect_config("sa") is SQLANYWHERE_CONFIG

    assert SYBASE_CONFIG.name == "sybase"
    assert SYBASE_CONFIG.default_schema == "dbo"
    assert SYBASE_CONFIG.get_feature_flag("supports_cte") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_window_functions") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_merge") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_returning") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_transactional_ddl") is False
    assert SYBASE_CONFIG.get_feature_flag("supports_transactions") is True
    assert SYBASE_CONFIG.get_optimal_type("boolean") == "BIT"
    assert SYBASE_CONFIG.get_optimal_type("timestamp") == "BIGDATETIME"
    assert SYBASE_CONFIG.get_optimal_type("blob") == "IMAGE"

    assert SQLANYWHERE_CONFIG.name == "sqlanywhere"
    assert SQLANYWHERE_CONFIG.default_schema == "DBA"
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_cte") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_window_functions") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_merge") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_for_update") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_skip_locked") is True
    assert SQLANYWHERE_CONFIG.get_feature_flag("supports_transactional_ddl") is False
    assert SQLANYWHERE_CONFIG.get_optimal_type("uuid") == "UNIQUEIDENTIFIER"
    assert SQLANYWHERE_CONFIG.get_optimal_type("text") == "LONG VARCHAR"
    assert SQLANYWHERE_CONFIG.get_optimal_type("blob") == "LONG BINARY"


def test_sybase_and_sqlanywhere_version_patterns() -> None:
    """Version regexes should extract major, minor, and patch components for ASE and SQL Anywhere."""
    ase_banner = (
        "Adaptive Server Enterprise/16.0 SP04 PL04/EBF 30626 SMP/P/x86_64/"
        "Enterprise Linux/ase160sp04pl04x/0/64-bit/FBO/Thu"
    )
    ase_match = SYBASE_CONFIG.version_pattern.search(ase_banner)
    assert ase_match is not None
    assert tuple(int(part or 0) for part in ase_match.groups()) == (16, 0, 4)

    sa_banner = "17.0.11.7458"
    sa_match = SQLANYWHERE_CONFIG.version_pattern.search(sa_banner)
    assert sa_match is not None
    assert tuple(int(part or 0) for part in sa_match.groups()) == (17, 0, 11)


def test_sybase_and_sqlanywhere_reflection_helpers() -> None:
    """Reflection helpers should extract version values, resolve features, and merge table lists."""
    ase_version = SybaseVersionInfo(16, 0, 4, "SP04")
    assert str(ase_version) == "16.0.4 (SP04)"
    assert extract_sybase_version_value({"version_string": "Adaptive Server Enterprise/16.0 SP04"}) == (
        "Adaptive Server Enterprise/16.0 SP04"
    )
    assert resolve_sybase_feature_flag("supports_transactions", ase_version) is True
    assert resolve_sybase_feature_flag("supports_cte", ase_version) is False
    assert "supports_transactions" in list_sybase_available_features()

    sa_version = SqlanywhereVersionInfo(17, 0, 11, "7458")
    assert str(sa_version) == "17.0.11 (7458)"
    assert extract_sqlanywhere_version_value({"version_string": "17.0.11.7458"}) == "17.0.11.7458"
    assert resolve_sqlanywhere_feature_flag("supports_cte", sa_version) is True
    assert resolve_sqlanywhere_feature_flag("supports_window_functions", VersionInfo(12, 0, 0)) is True
    assert "supports_cte" in list_sqlanywhere_available_features()

    ordered: list[TableMetadata] = [{"schema_name": "dbo", "table_name": "users"}]
    all_rows: list[TableMetadata] = [
        {"schema_name": "dbo", "table_name": "posts"},
        {"schema_name": "dbo", "table_name": "users"},
    ]
    assert merge_sybase_table_lists(ordered, all_rows) == [
        {"schema_name": "dbo", "table_name": "users"},
        {"schema_name": "dbo", "table_name": "posts"},
    ]
    assert merge_sqlanywhere_table_lists(ordered, all_rows) == [
        {"schema_name": "dbo", "table_name": "users"},
        {"schema_name": "dbo", "table_name": "posts"},
    ]

    sybase_profile = build_sybase_metadata_capability_profile("sybase")
    assert sybase_profile.dialect == "sybase"
    assert len(sybase_profile.capabilities) == len(SYBASE_METADATA_DOMAINS)

    sa_profile = build_sqlanywhere_metadata_capability_profile("sqlanywhere")
    assert sa_profile.dialect == "sqlanywhere"
    assert len(sa_profile.capabilities) == len(SQLANYWHERE_METADATA_DOMAINS)


@pytest.mark.parametrize("dialect", ["sybase", "sqlanywhere"])
@pytest.mark.parametrize(
    ("domain", "query_names"),
    [
        ("version", ("current",)),
        ("schemas", ("by_schema", "current")),
        ("tables", ("by_schema", "all_by_schema", "names_by_schema")),
        ("columns", ("by_schema", "by_table")),
        ("indexes", ("by_schema", "by_table")),
        ("foreign_keys", ("by_schema", "by_table")),
        ("constraints", ("by_schema", "by_table")),
        ("objects", ("by_schema",)),
        ("views", ("by_schema",)),
    ],
)
def test_all_domain_queries_load_for_sybase_and_sqlanywhere(
    dialect: str, domain: str, query_names: tuple[str, ...]
) -> None:
    """DataDictionaryLoader should load all 9 domains and named queries for sybase and sqlanywhere."""
    loader = DataDictionaryLoader()
    assert loader.has_domain_queries(dialect, domain) is True
    for query_name in query_names:
        query = loader.get_domain_query(dialect, domain, query_name)
        assert query.is_supported is True
        assert query.query_text is not None
        assert query.query_text.strip() != ""


def test_sybase_and_sqlanywhere_catalog_tables_referenced() -> None:
    """Sybase queries should use dbo.sys* catalogs and SQL Anywhere queries should use SYS.SYS* catalogs."""
    loader = DataDictionaryLoader()

    sybase_tables = loader.get_domain_query_text("sybase", "tables", "by_schema") or ""
    sybase_columns = loader.get_domain_query_text("sybase", "columns", "by_table") or ""
    sybase_fks = loader.get_domain_query_text("sybase", "foreign_keys", "by_table") or ""
    assert "dbo.sysobjects" in sybase_tables
    assert "dbo.syscolumns" in sybase_columns
    assert "dbo.systypes" in sybase_columns
    assert "dbo.sysreferences" in sybase_fks

    sa_tables = loader.get_domain_query_text("sqlanywhere", "tables", "by_schema") or ""
    sa_columns = loader.get_domain_query_text("sqlanywhere", "columns", "by_table") or ""
    sa_fks = loader.get_domain_query_text("sqlanywhere", "foreign_keys", "by_table") or ""
    assert "SYS.SYSTAB" in sa_tables
    assert "SYS.SYSTABCOL" in sa_columns
    assert "SYS.SYSDOMAIN" in sa_columns
    assert "SYS.SYSFKEY" in sa_fks
