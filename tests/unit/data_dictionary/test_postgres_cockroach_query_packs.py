"""PostgreSQL-family data-dictionary query-pack contracts."""

import importlib
from typing import Any, cast

import pytest

import sqlspec.data_dictionary.dialects.cockroachdb as cockroachdb_pkg
from sqlspec.data_dictionary import (
    DataDictionaryLoader,
    DDLResult,
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    SystemMetadataResult,
    VersionInfo,
)
from sqlspec.data_dictionary.dialects.cockroachdb import (
    COCKROACH_METADATA_DOMAINS,
    CockroachDBAsyncDataDictionary,
    CockroachDBSyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.cockroachdb import dictionary as cockroachdb_dictionary_module

POSTGRES_DOMAIN_QUERIES = (
    ("schemas", "list"),
    ("objects", "by_schema"),
    ("tables", "by_schema"),
    ("columns", "by_schema"),
    ("constraints", "by_schema"),
    ("indexes", "by_schema"),
    ("views", "by_schema"),
    ("materialized_views", "by_schema"),
    ("sequences", "by_schema"),
    ("routines", "by_schema"),
    ("triggers", "by_schema"),
    ("comments", "by_schema"),
    ("privileges", "by_schema"),
    ("dependencies", "by_schema"),
    ("extensions", "list"),
    ("partitions", "by_schema"),
    ("ddl", "by_object"),
    ("system", "settings"),
    ("system", "table_stats"),
    ("system", "pg_stat_statements"),
)

COCKROACH_STABLE_DOMAIN_QUERIES = (
    ("schemas", "list"),
    ("objects", "by_schema"),
    ("tables", "by_schema"),
    ("columns", "by_schema"),
    ("constraints", "by_schema"),
    ("indexes", "by_schema"),
    ("views", "by_schema"),
    ("sequences", "by_schema"),
    ("comments", "by_schema"),
    ("privileges", "by_schema"),
    ("dependencies", "by_schema"),
    ("ddl", "by_object"),
)


@pytest.mark.parametrize(("domain", "query_name"), POSTGRES_DOMAIN_QUERIES)
def test_postgres_replacement_domain_query_pack_is_available(domain: str, query_name: str) -> None:
    """PostgreSQL exposes every C3 metadata domain through direct domain paths."""
    query = DataDictionaryLoader().get_domain_query("postgresql", domain, query_name)

    assert query.is_supported is True
    assert query.dialect == "postgres"
    assert query.domain == domain
    assert query.name == query_name
    assert query.capability.source == MetadataSource.CATALOG
    assert query.query_text is not None
    assert "pg_catalog" in query.query_text or domain == "system"


def test_postgres_query_pack_uses_catalogs_needed_for_ddl_grade_metadata() -> None:
    """DDL-grade PostgreSQL domains use pg_catalog objects, not information_schema-only queries."""
    loader = DataDictionaryLoader()

    columns = loader.get_domain_query_text("postgres", "columns", "by_schema")
    constraints = loader.get_domain_query_text("postgres", "constraints", "by_schema")
    indexes = loader.get_domain_query_text("postgres", "indexes", "by_schema")
    dependencies = loader.get_domain_query_text("postgres", "dependencies", "by_schema")
    ddl = loader.get_domain_query_text("postgres", "ddl", "by_object")

    assert columns is not None and "pg_catalog.pg_attribute" in columns
    assert columns is not None and "pg_catalog.format_type" in columns
    assert constraints is not None and "pg_catalog.pg_constraint" in constraints
    assert constraints is not None and "pg_catalog.pg_get_constraintdef" in constraints
    assert indexes is not None and "pg_catalog.pg_index" in indexes
    assert indexes is not None and "pg_catalog.pg_get_indexdef" in indexes
    assert indexes is not None and "ix.indispartial" not in indexes
    assert (
        indexes is not None
        and "pg_catalog.pg_get_expr(ix.indpred, ix.indrelid)::text IS NOT NULL AS is_partial" in indexes
    )
    assert indexes is not None and "::text" in indexes
    assert dependencies is not None and "pg_catalog.pg_depend" in dependencies
    assert ddl is not None and "pg_catalog.pg_get_" in ddl


@pytest.mark.parametrize(("domain", "query_name"), COCKROACH_STABLE_DOMAIN_QUERIES)
def test_cockroach_replacement_stable_query_pack_is_available(domain: str, query_name: str) -> None:
    """CockroachDB default metadata uses stable information_schema/pg_catalog surfaces."""
    query = DataDictionaryLoader().get_domain_query("cockroach", domain, query_name)

    assert query.is_supported is True
    assert query.dialect == "cockroachdb"
    assert query.domain == domain
    assert query.query_text is not None
    assert "crdb_internal" not in query.query_text


def test_cockroach_crdb_internal_query_is_feature_gated_by_default() -> None:
    """CockroachDB internal metadata must be explicit opt-in, not default metadata."""
    query = DataDictionaryLoader().get_domain_query(
        "cockroachdb", "crdb_internal", "ranges", required_features=("supports_crdb_internal_metadata",)
    )

    assert query.is_supported is False
    assert query.sql is None
    assert query.capability.support == MetadataSupport.UNSUPPORTED
    assert query.capability.risks == (MetadataRisk.VERSION_GATED,)
    assert query.capability.warnings == ("cockroachdb/crdb_internal/ranges requires supports_crdb_internal_metadata",)


def test_postgres_and_cockroach_foreign_keys_and_indexes_contracts() -> None:
    """PostgreSQL and CockroachDB foreign_keys, tables, and indexes avoid Cartesian joins and project required columns."""
    loader = DataDictionaryLoader()

    pg_tables = loader.get_domain_query_text("postgres", "tables", "by_schema")
    pg_fks = loader.get_domain_query_text("postgres", "foreign_keys", "by_schema")
    crdb_fks = loader.get_domain_query_text("cockroachdb", "foreign_keys", "by_schema")
    crdb_indexes = loader.get_domain_query_text("cockroachdb", "indexes", "by_schema")
    crdb_indexes_by_table = loader.get_domain_query_text("cockroachdb", "indexes", "by_table")

    assert pg_tables is not None
    assert "DISTINCT ON (t.oid)" not in pg_tables
    assert "fk.confrelid <> fk.conrelid" in pg_tables
    assert "LEFT JOIN dependency_levels" in pg_tables
    assert pg_fks is not None
    assert "pg_catalog.pg_constraint" in pg_fks
    assert "unnest(con.conkey, con.confkey) WITH ORDINALITY" in pg_fks
    assert crdb_fks is not None
    assert "information_schema.referential_constraints" in crdb_fks
    assert "pk_kcu.ordinal_position" in crdb_fks
    assert crdb_indexes is not None and "AS columns" in crdb_indexes and "AS is_unique" in crdb_indexes
    assert (
        crdb_indexes_by_table is not None
        and "AS columns" in crdb_indexes_by_table
        and "AS is_unique" in crdb_indexes_by_table
    )

    pg_columns_by_table = loader.get_domain_query_text("postgres", "columns", "by_table")
    crdb_columns_by_table = loader.get_domain_query_text("cockroachdb", "columns", "by_table")
    assert pg_columns_by_table is not None
    assert "AS sequence_name" in pg_columns_by_table
    assert "AS identity_generation" in pg_columns_by_table
    assert "AS is_primary" in pg_columns_by_table
    assert "AS is_generated" in pg_columns_by_table
    assert "AS generation_expression" in pg_columns_by_table
    assert "COALESCE(" in pg_columns_by_table
    assert crdb_columns_by_table is not None
    assert "AS sequence_name" in crdb_columns_by_table
    assert "AS identity_generation" in crdb_columns_by_table
    assert "AS is_primary" in crdb_columns_by_table
    assert "AS generation_expression" in crdb_columns_by_table
    assert "COALESCE(" in crdb_columns_by_table


def test_postgres_system_and_constraints_query_contracts() -> None:
    """PostgreSQL pg_stat_statements omits pg_catalog prefix and constraints pre-aggregate columns."""
    loader = DataDictionaryLoader()
    sys_query = loader.get_domain_query_text("postgres", "system", "pg_stat_statements")
    assert sys_query is not None
    assert "FROM pg_catalog.pg_stat_statements" not in sys_query
    assert "FROM pg_stat_statements" in sys_query

    con_query = loader.get_domain_query_text("postgres", "constraints", "by_schema")
    assert con_query is not None
    assert "WITH con_cols AS (" in con_query
    assert "CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY" in con_query
    assert "ARRAY(SELECT att.attname" not in con_query


def test_cockroach_constraints_and_columns_query_contracts() -> None:
    """CockroachDB constraints and columns queries pre-aggregate and factor scalar subqueries."""
    loader = DataDictionaryLoader()
    con_query = loader.get_domain_query_text("cockroachdb", "constraints", "by_schema")
    assert con_query is not None
    assert "WITH con_cols AS (" in con_query
    assert "array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position)" in con_query
    assert "ARRAY(SELECT kcu.column_name" not in con_query

    cols_query = loader.get_domain_query_text("cockroachdb", "columns", "by_schema")
    assert cols_query is not None
    assert "WITH resolved_table_schema AS (" in cols_query
    assert "FROM resolved_table_schema" in cols_query


class _FakeCockroachSyncDriver:
    """Minimal sync driver stub recording CockroachDB data-dictionary queries."""

    def __init__(
        self,
        *,
        connection: object | None = None,
        version_value: Any = None,
        select_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.connection = connection
        self.version_value = version_value
        self.select_rows = select_rows
        self.select_calls: list[tuple[Any, dict[str, Any]]] = []
        self.select_value_calls: list[Any] = []

    def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        if self.select_rows is not None:
            return list(self.select_rows)
        return [{"schema_name": kwargs.get("schema_name"), "table_name": kwargs.get("table_name", "users")}]

    def select_value_or_none(self, statement: Any, **kwargs: Any) -> Any:
        _ = kwargs
        self.select_value_calls.append(statement)
        return self.version_value


class _FakeCockroachAsyncDriver:
    """Minimal async driver stub recording CockroachDB data-dictionary queries."""

    def __init__(
        self,
        *,
        connection: object | None = None,
        version_value: Any = None,
        select_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.connection = connection
        self.version_value = version_value
        self.select_rows = select_rows
        self.select_calls: list[tuple[Any, dict[str, Any]]] = []
        self.select_value_calls: list[Any] = []

    async def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.select_calls.append((statement, kwargs))
        if self.select_rows is not None:
            return list(self.select_rows)
        return [{"schema_name": kwargs.get("schema_name"), "table_name": kwargs.get("table_name", "users")}]

    async def select_value_or_none(self, statement: Any, **kwargs: Any) -> Any:
        _ = kwargs
        self.select_value_calls.append(statement)
        return self.version_value


@pytest.mark.parametrize("cls_name", ("CockroachDBSyncDataDictionary", "CockroachDBAsyncDataDictionary"))
def test_cockroachdb_dialect_package_lazy_exports_and_identity(cls_name: str) -> None:
    """CockroachDB dialect package exports sync and async dictionary bases via PEP 562."""
    assert cls_name in dir(cockroachdb_pkg)
    assert cls_name in cockroachdb_pkg.__all__
    reloaded = importlib.import_module("sqlspec.data_dictionary.dialects.cockroachdb")
    exported = getattr(reloaded, cls_name)
    direct = getattr(cockroachdb_dictionary_module, cls_name)
    assert exported is direct
    assert getattr(reloaded, cls_name) is direct


def test_cockroachdb_dialect_package_unknown_attribute_raises_attribute_error() -> None:
    """Unknown attribute lookups on the cockroachdb dialect package raise AttributeError."""
    with pytest.raises(AttributeError, match="has no attribute 'NonExistentExport'"):
        getattr(cockroachdb_pkg, "NonExistentExport")


def test_cockroachdb_sync_capability_profile() -> None:
    """Sync CockroachDB capability profile marks ddl as LOSSY and crdb_internal/system as UNSUPPORTED."""
    driver = _FakeCockroachSyncDriver()
    data_dictionary = CockroachDBSyncDataDictionary()

    profile = data_dictionary.get_metadata_capabilities(
        cast("Any", driver), domains=("schemas", "ddl", "crdb_internal", "system", "unknown_domain")
    )

    assert profile.dialect == "cockroachdb"
    assert profile.adapter == "CockroachDBSyncDataDictionary"
    by_domain = {cap.domain: cap for cap in profile.capabilities}
    assert by_domain["schemas"].support == MetadataSupport.SUPPORTED
    assert by_domain["schemas"].fidelity == MetadataFidelity.PARTIAL
    assert by_domain["ddl"].support == MetadataSupport.SUPPORTED
    assert by_domain["ddl"].fidelity == MetadataFidelity.LOSSY
    assert by_domain["crdb_internal"].support == MetadataSupport.UNSUPPORTED
    assert by_domain["system"].support == MetadataSupport.UNSUPPORTED
    assert by_domain["unknown_domain"].support == MetadataSupport.UNSUPPORTED

    full_profile = data_dictionary.get_metadata_capabilities(cast("Any", driver))
    assert tuple(cap.domain for cap in full_profile.capabilities) == COCKROACH_METADATA_DOMAINS

    sys_caps = data_dictionary.get_system_metadata_capabilities(cast("Any", driver))
    assert len(sys_caps) == 1
    assert sys_caps[0].domain == "system"
    assert sys_caps[0].support == MetadataSupport.UNSUPPORTED


async def test_cockroachdb_async_capability_profile() -> None:
    """Async CockroachDB capability profile marks ddl as LOSSY and crdb_internal/system as UNSUPPORTED."""
    driver = _FakeCockroachAsyncDriver()
    data_dictionary = CockroachDBAsyncDataDictionary()

    profile = await data_dictionary.get_metadata_capabilities(
        cast("Any", driver), domains=("schemas", "ddl", "crdb_internal", "system", "unknown_domain")
    )

    assert profile.dialect == "cockroachdb"
    assert profile.adapter == "CockroachDBAsyncDataDictionary"
    by_domain = {cap.domain: cap for cap in profile.capabilities}
    assert by_domain["schemas"].support == MetadataSupport.SUPPORTED
    assert by_domain["schemas"].fidelity == MetadataFidelity.PARTIAL
    assert by_domain["ddl"].support == MetadataSupport.SUPPORTED
    assert by_domain["ddl"].fidelity == MetadataFidelity.LOSSY
    assert by_domain["crdb_internal"].support == MetadataSupport.UNSUPPORTED
    assert by_domain["system"].support == MetadataSupport.UNSUPPORTED
    assert by_domain["unknown_domain"].support == MetadataSupport.UNSUPPORTED

    full_profile = await data_dictionary.get_metadata_capabilities(cast("Any", driver))
    assert tuple(cap.domain for cap in full_profile.capabilities) == COCKROACH_METADATA_DOMAINS

    sys_caps = await data_dictionary.get_system_metadata_capabilities(cast("Any", driver), domains=("settings",))
    assert len(sys_caps) == 1
    assert sys_caps[0].domain == "settings"
    assert sys_caps[0].support == MetadataSupport.UNSUPPORTED


def test_cockroachdb_sync_unsupported_routines_types_and_system() -> None:
    """Sync routines, types, and system metadata stay fail-closed without querying the driver."""
    driver = _FakeCockroachSyncDriver()
    data_dictionary = CockroachDBSyncDataDictionary()

    routines = data_dictionary.get_routines(cast("Any", driver))
    types_result = data_dictionary.get_types(cast("Any", driver))
    gated_system = data_dictionary.get_system_metadata(cast("Any", driver), "settings")
    opted_in_system = data_dictionary.get_system_metadata(cast("Any", driver), "settings", include_system=True)

    assert routines.capability.support == MetadataSupport.UNSUPPORTED
    assert routines.items == ()
    assert types_result.capability.support == MetadataSupport.UNSUPPORTED
    assert isinstance(gated_system, SystemMetadataResult)
    assert gated_system.capability.support == MetadataSupport.GATED
    assert gated_system.rows == ()
    assert isinstance(opted_in_system, SystemMetadataResult)
    assert opted_in_system.capability.support == MetadataSupport.UNSUPPORTED
    assert opted_in_system.rows == ()
    assert driver.select_calls == []


async def test_cockroachdb_async_unsupported_routines_types_and_system() -> None:
    """Async routines, types, and system metadata stay fail-closed without querying the driver."""
    driver = _FakeCockroachAsyncDriver()
    data_dictionary = CockroachDBAsyncDataDictionary()

    routines = await data_dictionary.get_routines(cast("Any", driver))
    types_result = await data_dictionary.get_types(cast("Any", driver))
    gated_system = await data_dictionary.get_system_metadata(cast("Any", driver), "settings")
    opted_in_system = await data_dictionary.get_system_metadata(cast("Any", driver), "settings", include_system=True)

    assert routines.capability.support == MetadataSupport.UNSUPPORTED
    assert routines.items == ()
    assert types_result.capability.support == MetadataSupport.UNSUPPORTED
    assert isinstance(gated_system, SystemMetadataResult)
    assert gated_system.capability.support == MetadataSupport.GATED
    assert gated_system.rows == ()
    assert isinstance(opted_in_system, SystemMetadataResult)
    assert opted_in_system.capability.support == MetadataSupport.UNSUPPORTED
    assert opted_in_system.rows == ()
    assert driver.select_calls == []


@pytest.mark.parametrize(
    ("version_value", "expected_version", "expected_json_type"),
    (
        ("CockroachDB CCL v23.1.0", VersionInfo(23, 1, 0), "JSONB"),
        ("CockroachDB v20.1.0", VersionInfo(20, 1, 0), "JSONB"),
        ("CockroachDB CCL v19.2.0", VersionInfo(19, 2, 0), "TEXT"),
        ("unrecognized banner", None, "TEXT"),
        (None, None, "TEXT"),
    ),
)
def test_cockroachdb_sync_version_cache_and_json_resolution(
    version_value: str | None, expected_version: VersionInfo | None, expected_json_type: str
) -> None:
    """Sync version detection caches once per physical connection and resolves optimal JSON type."""
    shared_connection = object()
    first_driver = _FakeCockroachSyncDriver(connection=shared_connection, version_value=version_value)
    second_driver = _FakeCockroachSyncDriver(connection=shared_connection, version_value="CockroachDB CCL v24.1.0")
    data_dictionary = CockroachDBSyncDataDictionary()

    assert data_dictionary.get_optimal_type(cast("Any", first_driver), "json") == expected_json_type
    assert data_dictionary.get_version(cast("Any", first_driver)) == expected_version
    assert data_dictionary.get_version(cast("Any", second_driver)) == expected_version
    assert data_dictionary.get_optimal_type(cast("Any", second_driver), "uuid") == "UUID"
    assert data_dictionary.get_feature_flag(cast("Any", first_driver), "supports_json") == (
        expected_version is not None and expected_version >= VersionInfo(20, 1, 0)
    )
    assert len(first_driver.select_value_calls) == 1
    assert len(second_driver.select_value_calls) == 0


@pytest.mark.parametrize(
    ("version_value", "expected_version", "expected_json_type"),
    (
        ("CockroachDB CCL v23.1.0", VersionInfo(23, 1, 0), "JSONB"),
        ("CockroachDB v20.1.0", VersionInfo(20, 1, 0), "JSONB"),
        ("CockroachDB CCL v19.2.0", VersionInfo(19, 2, 0), "TEXT"),
        ("unrecognized banner", None, "TEXT"),
        (None, None, "TEXT"),
    ),
)
async def test_cockroachdb_async_version_cache_and_json_resolution(
    version_value: str | None, expected_version: VersionInfo | None, expected_json_type: str
) -> None:
    """Async version detection caches once per physical connection and resolves optimal JSON type."""
    shared_connection = object()
    first_driver = _FakeCockroachAsyncDriver(connection=shared_connection, version_value=version_value)
    second_driver = _FakeCockroachAsyncDriver(connection=shared_connection, version_value="CockroachDB CCL v24.1.0")
    data_dictionary = CockroachDBAsyncDataDictionary()

    assert await data_dictionary.get_optimal_type(cast("Any", first_driver), "json") == expected_json_type
    assert await data_dictionary.get_version(cast("Any", first_driver)) == expected_version
    assert await data_dictionary.get_version(cast("Any", second_driver)) == expected_version
    assert await data_dictionary.get_optimal_type(cast("Any", second_driver), "uuid") == "UUID"
    assert await data_dictionary.get_feature_flag(cast("Any", first_driver), "supports_json") == (
        expected_version is not None and expected_version >= VersionInfo(20, 1, 0)
    )
    assert len(first_driver.select_value_calls) == 1
    assert len(second_driver.select_value_calls) == 0


@pytest.mark.parametrize(("method_name", "domain"), (("get_columns", "columns"), ("get_indexes", "indexes")))
@pytest.mark.parametrize(
    ("table", "schema", "expected_query_name", "expected_schema", "expected_table"),
    (
        (None, None, "by_schema", "public", None),
        ("users", None, "by_table", None, "users"),
        ("Users", "Reporting", "by_table", "reporting", "users"),
    ),
)
def test_cockroachdb_sync_columns_and_indexes_query_selection(
    method_name: str,
    domain: str,
    table: str | None,
    schema: str | None,
    expected_query_name: str,
    expected_schema: str | None,
    expected_table: str | None,
) -> None:
    """Sync get_columns and get_indexes select by_table when table is provided and bind schema_name cleanly."""
    driver = _FakeCockroachSyncDriver()
    data_dictionary = CockroachDBSyncDataDictionary()
    expected_query = DataDictionaryLoader().get_domain_query("cockroachdb", domain, expected_query_name).sql
    assert expected_query is not None

    getattr(data_dictionary, method_name)(cast("Any", driver), table=table, schema=schema)

    assert len(driver.select_calls) == 1
    statement, kwargs = driver.select_calls[0]
    assert statement.sql == expected_query.sql
    assert kwargs["schema_name"] == expected_schema
    assert kwargs["table_name"] == expected_table


@pytest.mark.parametrize(("method_name", "domain"), (("get_columns", "columns"), ("get_indexes", "indexes")))
@pytest.mark.parametrize(
    ("table", "schema", "expected_query_name", "expected_schema", "expected_table"),
    (
        (None, None, "by_schema", "public", None),
        ("users", None, "by_table", None, "users"),
        ("Users", "Reporting", "by_table", "reporting", "users"),
    ),
)
async def test_cockroachdb_async_columns_and_indexes_query_selection(
    method_name: str,
    domain: str,
    table: str | None,
    schema: str | None,
    expected_query_name: str,
    expected_schema: str | None,
    expected_table: str | None,
) -> None:
    """Async get_columns and get_indexes select by_table when table is provided and bind schema_name cleanly."""
    driver = _FakeCockroachAsyncDriver()
    data_dictionary = CockroachDBAsyncDataDictionary()
    expected_query = DataDictionaryLoader().get_domain_query("cockroachdb", domain, expected_query_name).sql
    assert expected_query is not None

    await getattr(data_dictionary, method_name)(cast("Any", driver), table=table, schema=schema)

    assert len(driver.select_calls) == 1
    statement, kwargs = driver.select_calls[0]
    assert statement.sql == expected_query.sql
    assert kwargs["schema_name"] == expected_schema
    assert kwargs["table_name"] == expected_table


@pytest.mark.parametrize(
    ("rows", "expected_fidelity", "expected_ddl", "expected_warning"),
    (
        ([], MetadataFidelity.LOSSY, None, None),
        (
            [{"schema_name": "public", "ddl": None, "warning": "SHOW CREATE unavailable"}],
            MetadataFidelity.LOSSY,
            None,
            "SHOW CREATE unavailable",
        ),
        (
            [
                {
                    "schema_name": "public",
                    "ddl": "CREATE VIEW public.v_orders AS SELECT 1",
                    "fidelity": MetadataFidelity.NATIVE,
                }
            ],
            MetadataFidelity.NATIVE,
            "CREATE VIEW public.v_orders AS SELECT 1",
            None,
        ),
    ),
)
def test_cockroachdb_sync_get_ddl_envelope(
    rows: list[dict[str, Any]], expected_fidelity: str, expected_ddl: str | None, expected_warning: str | None
) -> None:
    """Sync get_ddl returns a lossy DDLResult when ddl is absent and native DDL when present."""
    driver = _FakeCockroachSyncDriver(select_rows=rows)
    data_dictionary = CockroachDBSyncDataDictionary()

    result = data_dictionary.get_ddl(cast("Any", driver), "v_orders", schema="public", object_type="view")

    assert isinstance(result, DDLResult)
    assert result.fidelity == expected_fidelity
    assert result.ddl == expected_ddl
    assert result.identity.dialect == "cockroachdb"
    assert result.identity.name == "v_orders"
    assert result.identity.schema == "public"
    assert result.identity.object_type == "view"
    if expected_warning is not None:
        assert expected_warning in result.warnings


@pytest.mark.parametrize(
    ("rows", "expected_fidelity", "expected_ddl", "expected_warning"),
    (
        ([], MetadataFidelity.LOSSY, None, None),
        (
            [{"schema_name": "public", "ddl": None, "warning": "SHOW CREATE unavailable"}],
            MetadataFidelity.LOSSY,
            None,
            "SHOW CREATE unavailable",
        ),
        (
            [
                {
                    "schema_name": "public",
                    "ddl": "CREATE VIEW public.v_orders AS SELECT 1",
                    "fidelity": MetadataFidelity.NATIVE,
                }
            ],
            MetadataFidelity.NATIVE,
            "CREATE VIEW public.v_orders AS SELECT 1",
            None,
        ),
    ),
)
async def test_cockroachdb_async_get_ddl_envelope(
    rows: list[dict[str, Any]], expected_fidelity: str, expected_ddl: str | None, expected_warning: str | None
) -> None:
    """Async get_ddl returns a lossy DDLResult when ddl is absent and native DDL when present."""
    driver = _FakeCockroachAsyncDriver(select_rows=rows)
    data_dictionary = CockroachDBAsyncDataDictionary()

    result = await data_dictionary.get_ddl(cast("Any", driver), "v_orders", schema="public", object_type="view")

    assert isinstance(result, DDLResult)
    assert result.fidelity == expected_fidelity
    assert result.ddl == expected_ddl
    assert result.identity.dialect == "cockroachdb"
    assert result.identity.name == "v_orders"
    assert result.identity.schema == "public"
    assert result.identity.object_type == "view"
    if expected_warning is not None:
        assert expected_warning in result.warnings


def test_cockroachdb_sync_domain_methods_and_foreign_keys() -> None:
    """Sync CockroachDB base delegates all supported metadata domains and foreign keys."""
    fk_row = {
        "table_schema": "public",
        "table_name": "orders",
        "column_name": "user_id",
        "referenced_table_schema": "public",
        "referenced_table_name": "users",
        "referenced_column_name": "id",
        "constraint_name": "fk_orders_user",
        "delete_rule": "CASCADE",
        "update_rule": "NO ACTION",
    }
    driver = _FakeCockroachSyncDriver(select_rows=[fk_row])
    data_dictionary = CockroachDBSyncDataDictionary()

    assert data_dictionary.get_schemas(cast("Any", driver)).domain == "schemas"
    assert data_dictionary.get_objects(cast("Any", driver), schema="Public").domain == "objects"
    assert data_dictionary.get_table_details(cast("Any", driver), "Orders").domain == "tables"
    assert data_dictionary.get_constraints(cast("Any", driver), table="Orders").domain == "constraints"
    assert data_dictionary.get_views(cast("Any", driver)).domain == "views"
    assert data_dictionary.get_privileges(cast("Any", driver), object_name="Orders").domain == "privileges"
    assert data_dictionary.get_dependencies(cast("Any", driver), object_name="Orders").domain == "dependencies"
    assert data_dictionary.get_sequences(cast("Any", driver)).domain == "sequences"
    assert len(data_dictionary.get_tables(cast("Any", driver))) == 1

    schema_fks = data_dictionary.get_foreign_keys(cast("Any", driver))
    table_fks = data_dictionary.get_foreign_keys(cast("Any", driver), table="Orders")
    assert len(schema_fks) == 1
    assert schema_fks[0].constraint_name == "fk_orders_user"
    assert len(table_fks) == 1
    assert table_fks[0].referenced_table == "users"


async def test_cockroachdb_async_domain_methods_and_foreign_keys() -> None:
    """Async CockroachDB base delegates all supported metadata domains and foreign keys."""
    fk_row = {
        "table_schema": "public",
        "table_name": "orders",
        "column_name": "user_id",
        "referenced_table_schema": "public",
        "referenced_table_name": "users",
        "referenced_column_name": "id",
        "constraint_name": "fk_orders_user",
        "delete_rule": "CASCADE",
        "update_rule": "NO ACTION",
    }
    driver = _FakeCockroachAsyncDriver(select_rows=[fk_row])
    data_dictionary = CockroachDBAsyncDataDictionary()

    assert (await data_dictionary.get_schemas(cast("Any", driver))).domain == "schemas"
    assert (await data_dictionary.get_objects(cast("Any", driver), schema="Public")).domain == "objects"
    assert (await data_dictionary.get_table_details(cast("Any", driver), "Orders")).domain == "tables"
    assert (await data_dictionary.get_constraints(cast("Any", driver), table="Orders")).domain == "constraints"
    assert (await data_dictionary.get_views(cast("Any", driver))).domain == "views"
    assert (await data_dictionary.get_privileges(cast("Any", driver), object_name="Orders")).domain == "privileges"
    assert (await data_dictionary.get_dependencies(cast("Any", driver), object_name="Orders")).domain == "dependencies"
    assert (await data_dictionary.get_sequences(cast("Any", driver))).domain == "sequences"
    assert len(await data_dictionary.get_tables(cast("Any", driver))) == 1

    schema_fks = await data_dictionary.get_foreign_keys(cast("Any", driver))
    table_fks = await data_dictionary.get_foreign_keys(cast("Any", driver), table="Orders")
    assert len(schema_fks) == 1
    assert schema_fks[0].constraint_name == "fk_orders_user"
    assert len(table_fks) == 1
    assert table_fks[0].referenced_table == "users"


def test_cockroachdb_missing_domain_query_raises_runtime_error() -> None:
    """Missing CockroachDB query pack lookup raises a contextual RuntimeError."""
    with pytest.raises(RuntimeError, match="Missing CockroachDB data-dictionary query: nonexistent/missing"):
        cockroachdb_dictionary_module._cockroach_domain_sql("nonexistent", "missing")
