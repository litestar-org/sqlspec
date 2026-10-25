"""Contract metadata guardrails for adapter case records."""

from pathlib import Path
from typing import cast

import msgspec
import pytest
from _pytest.mark.structures import ParameterSet

from tests.integration.adapters._shared._adk_cases import AdkStoreCase, adk_store_params_with
from tests.integration.adapters._shared._cases import (
    ACTIVE_DRIVER_CASES,
    ASYNC_LIFECYCLE_DRIVER_PARAMS,
    LIFECYCLE_CAPABILITIES,
    SYNC_LIFECYCLE_DRIVER_PARAMS,
    DriverCase,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared._inputs import (
    PARAMETER_STYLE_EXECUTE_MANY_PARAMS,
    PARAMETER_STYLE_EXECUTE_PARAMS,
    PARAMETER_STYLE_PARAMS,
    ParameterStyleCase,
)
from tests.integration.adapters._shared._migration_cases import ACTIVE_MIGRATION_CASES
from tests.integration.adapters._shared._postgres_extension_cases import (
    ASYNC_POSTGRES_EXTENSION_CASES,
    SYNC_POSTGRES_EXTENSION_CASES,
)
from tests.integration.adapters.conftest import _case_family, pytest_collection_modifyitems

MYSQL_ROW_LOCKING_CASE_IDS = {
    "aiomysql-async",
    "asyncmy-async",
    "mysqlconnector-async",
    "mysqlconnector-sync",
    "pymysql-sync",
}
DATA_DICTIONARY_CASE_IDS = {
    "adbc-duckdb-sync",
    "adbc-postgres-sync",
    "adbc-sqlite-sync",
    "aiosqlite-async",
    "asyncpg-async",
    "duckdb-sync",
    "psycopg-async",
    "psycopg-sync",
    "psqlpy-async",
    "sqlite-sync",
}
DEFAULT_SCHEMA_MIGRATION_CASE_IDS = {
    "adbc-postgres-sync",
    "asyncpg-async",
    "duckdb-sync",
    "mssql-python-sync",
    "psqlpy-async",
    "psycopg-async",
    "psycopg-sync",
    "pymssql-sync",
}
MIGRATION_LIFECYCLE_CASE_IDS = {
    "adbc-postgres-sync",
    "adbc-sqlite-sync",
    "aiomysql-async",
    "aiosqlite-async",
    "asyncmy-async",
    "asyncpg-async",
    "db2-async",
    "db2-sync",
    "duckdb-sync",
    "mssql-python-sync",
    "mysqlconnector-async",
    "oracledb-async",
    "oracledb-sync",
    "psqlpy-async",
    "psycopg-async",
    "psycopg-sync",
    "pymssql-sync",
    "pymysql-sync",
    "spanner-async",
    "spanner-sync",
    "sqlite-sync",
}
MERGE_CASE_IDS = {"asyncpg-async", "oracledb-async", "oracledb-sync", "psqlpy-async", "psycopg-async", "psycopg-sync"}
MERGE_BULK_CASE_IDS = {"asyncpg-async", "oracledb-async", "psqlpy-async", "psycopg-async", "psycopg-sync"}
VECTOR_CASE_IDS = {"adbc-duckdb-sync", "duckdb-sync", "oracledb-async", "oracledb-sync"}
STORAGE_BRIDGE_DECIMAL_CASE_IDS = {"aiomysql-async", "asyncmy-async"}
UNSUPPORTED_EXCEPTION_TRANSLATION_CASE_IDS = {"adbc-sqlite-sync", "arrow-odbc-sync", "bigquery-sync"}
ORACLE_NATIVE_STATEMENT_STACK_CASE_IDS = {"oracledb-async"}
POSTGRES_EXTENSION_CASE_IDS = {
    "adbc-paradedb-sync",
    "adbc-pgvector-sync",
    "asyncpg-paradedb-async",
    "asyncpg-pgvector-async",
    "psqlpy-paradedb-async",
    "psqlpy-pgvector-async",
    "psycopg-paradedb-sync",
    "psycopg-pgvector-sync",
}
CONTRACT_OWNED_REMOVED_TEST_PATHS = {
    "adbc/test_adbc_results.py",
    "adbc/test_data_dictionary.py",
    "adbc/test_exceptions.py",
    "adbc/test_migrations.py",
    "adbc/test_paradedb.py",
    "adbc/test_pgvector.py",
    "adbc/test_vector_functions.py",
    "aiomysql/test_storage_bridge.py",
    "aiosqlite/test_data_dictionary.py",
    "asyncmy/test_storage_bridge.py",
    "asyncpg/extensions/events/test_queue_backend.py",
    "asyncpg/test_data_dictionary.py",
    "asyncpg/test_merge.py",
    "asyncpg/test_merge_bulk.py",
    "asyncpg/test_paradedb.py",
    "asyncpg/test_pgvector.py",
    "bigquery/test_exceptions.py",
    "duckdb/test_migrations.py",
    "duckdb/test_vector_functions.py",
    "mysql/aiomysql/extensions/adk/test_store.py",
    "mysql/asyncmy/extensions/adk/test_store.py",
    "mysql/mysqlconnector/extensions/adk/test_store.py",
    "mysqlconnector/test_driver_async.py",
    "oracledb/extensions/events/test_queue_backend.py",
    "oracledb/extensions/litestar/test_store_async.py",
    "oracledb/extensions/litestar/test_store_sync.py",
    "oracledb/test_exceptions.py",
    "oracledb/test_merge.py",
    "oracledb/test_merge_bulk.py",
    "oracledb/test_stack.py",
    "oracledb/test_vector_functions.py",
    "postgres/test_cockroach_retry.py",
    "psqlpy/test_data_dictionary.py",
    "psqlpy/test_merge_bulk.py",
    "psqlpy/test_migrations.py",
    "psqlpy/test_paradedb.py",
    "psqlpy/test_pgvector.py",
    "psycopg/test_data_dictionary.py",
    "psycopg/test_merge_bulk.py",
    "psycopg/test_migrations.py",
    "psycopg/test_paradedb.py",
    "psycopg/test_pgvector.py",
    "sqlite/test_data_dictionary.py",
}


def test_active_driver_cases_do_not_use_string_deviations() -> None:
    """Active cases use typed capability metadata instead of broad string deviations."""
    cases_with_deviations = {case.id: case.deviations for case in ACTIVE_DRIVER_CASES if case.deviations}
    assert cases_with_deviations == {}


def test_lifecycle_params_are_capability_filtered() -> None:
    """Lifecycle contract params collect only cases that can run at least one lifecycle behavior."""
    sync_cases = _driver_cases(SYNC_LIFECYCLE_DRIVER_PARAMS)
    async_cases = _driver_cases(ASYNC_LIFECYCLE_DRIVER_PARAMS)
    assert all(any(getattr(case, name) for name in LIFECYCLE_CAPABILITIES) for case in sync_cases)
    assert all(any(getattr(case, name) for name in LIFECYCLE_CAPABILITIES) for case in async_cases)


def test_capability_params_match_requested_capability() -> None:
    """Capability-filtered params collect only cases that declare the requested capability."""
    for capability_name in (
        *LIFECYCLE_CAPABILITIES,
        "supports_arrow",
        "supports_exception_translation",
        "supports_execute_many",
        "supports_for_update",
        "supports_grouped_subquery",
        "supports_data_dictionary",
        "supports_data_dictionary_topology",
        "supports_merge",
        "supports_merge_bulk",
        "supports_native_bulk_ingest",
        "supports_native_metadata",
        "supports_schema_qualified_data_dictionary",
        "supports_storage_bridge",
        "supports_vector",
    ):
        sync_cases = _driver_cases(sync_driver_params_with(capability_name))
        async_cases = _driver_cases(async_driver_params_with(capability_name))
        assert all(getattr(case, capability_name) for case in sync_cases)
        assert all(getattr(case, capability_name) for case in async_cases)


def test_mysql_row_locking_cases_are_contract_owned() -> None:
    """MySQL row-locking behavior belongs to the shared driver contract."""
    cases = {case.id: case for case in ACTIVE_DRIVER_CASES if case.id in MYSQL_ROW_LOCKING_CASE_IDS}
    assert set(cases) == MYSQL_ROW_LOCKING_CASE_IDS
    assert all(case.supports_for_update for case in cases.values())
    assert all(case.supports_for_share for case in cases.values())


def test_data_dictionary_cases_are_contract_owned() -> None:
    """Data-dictionary behavior belongs to the shared metadata contract."""
    cases = {case.id: case for case in ACTIVE_DRIVER_CASES if case.id in DATA_DICTIONARY_CASE_IDS}
    assert set(cases) == DATA_DICTIONARY_CASE_IDS
    assert all(case.supports_data_dictionary for case in cases.values())


def test_data_dictionary_query_limits_are_contract_metadata() -> None:
    """Data-dictionary batch/N+1 behavior is guarded by typed case metadata."""
    cases = {case.id: case for case in ACTIVE_DRIVER_CASES if case.id in DATA_DICTIONARY_CASE_IDS}

    assert all(case.data_dictionary_schema_query_limit >= 1 for case in cases.values())
    assert all(case.data_dictionary_index_query_limit >= 1 for case in cases.values())


def test_default_schema_migration_cases_are_contract_owned() -> None:
    """Default-schema migration behavior belongs to the shared migration contract."""
    cases = {case.id: case for case in ACTIVE_MIGRATION_CASES if case.id in DEFAULT_SCHEMA_MIGRATION_CASE_IDS}
    assert set(cases) == DEFAULT_SCHEMA_MIGRATION_CASE_IDS
    assert all(case.supports_default_schema for case in cases.values())


def test_migration_lifecycle_cases_are_contract_owned() -> None:
    """Migration lifecycle behavior belongs to the shared migration contract."""
    cases = {case.id: case for case in ACTIVE_MIGRATION_CASES}
    assert set(cases) == MIGRATION_LIFECYCLE_CASE_IDS


def test_merge_cases_are_contract_owned() -> None:
    """MERGE builder runtime behavior belongs to the shared driver contract."""
    cases = {case.id: case for case in ACTIVE_DRIVER_CASES if case.id in MERGE_CASE_IDS}
    assert set(cases) == MERGE_CASE_IDS
    assert all(case.supports_merge for case in cases.values())


def test_merge_bulk_cases_are_contract_owned() -> None:
    """Bulk MERGE strategy behavior belongs to the shared driver contract."""
    cases = {case.id: case for case in ACTIVE_DRIVER_CASES if case.id in MERGE_BULK_CASE_IDS}
    assert set(cases) == MERGE_BULK_CASE_IDS
    assert all(case.supports_merge_bulk for case in cases.values())


def test_plain_vector_cases_are_contract_owned() -> None:
    """Plain vector-distance execution belongs to the shared driver contract."""
    cases = {case.id: case for case in ACTIVE_DRIVER_CASES if case.id in VECTOR_CASE_IDS}
    assert set(cases) == VECTOR_CASE_IDS
    assert all(case.supports_vector for case in cases.values())


def test_storage_bridge_decimal_cases_are_contract_owned() -> None:
    """MySQL DECIMAL storage-bridge behavior belongs to the shared driver contract."""
    cases = {case.id: case for case in ACTIVE_DRIVER_CASES if "storage_bridge:mysql_decimal" in case.extra_assertions}
    assert set(cases) == STORAGE_BRIDGE_DECIMAL_CASE_IDS
    assert all(case.supports_storage_bridge for case in cases.values())


def test_exception_translation_gated_cases_are_explicit() -> None:
    """Cases excluded from the exception contract are explicit capability decisions."""
    cases = {case.id: case for case in ACTIVE_DRIVER_CASES if not case.supports_exception_translation}
    assert set(cases) == UNSUPPORTED_EXCEPTION_TRANSLATION_CASE_IDS


def test_oracle_native_statement_stack_case_is_contract_owned() -> None:
    """Oracle native statement-stack behavior belongs to the shared driver contract."""
    cases = {case.id: case for case in ACTIVE_DRIVER_CASES if "statement_stack:oracle_native" in case.extra_assertions}
    assert set(cases) == ORACLE_NATIVE_STATEMENT_STACK_CASE_IDS


def test_postgres_extension_cases_are_contract_owned() -> None:
    """pgvector/ParadeDB extension behavior belongs to the shared extension contract."""
    case_ids = {case.id for case in (*SYNC_POSTGRES_EXTENSION_CASES, *ASYNC_POSTGRES_EXTENSION_CASES)}
    assert case_ids == POSTGRES_EXTENSION_CASE_IDS


def test_contract_owned_adapter_local_test_files_do_not_reappear() -> None:
    """Adapter-local files removed by consolidation stay contract-owned."""
    assert all(not path.startswith("tests/") for path in CONTRACT_OWNED_REMOVED_TEST_PATHS)
    adapters_root = Path(__file__).resolve().parents[1]
    existing = sorted(path for path in CONTRACT_OWNED_REMOVED_TEST_PATHS if (adapters_root / path).exists())
    assert existing == []


def test_adk_capability_params_match_requested_capability() -> None:
    """ADK capability-filtered params collect only cases that declare the requested capability."""
    cases = _adk_cases(adk_store_params_with("supports_atomic_state_update"))
    assert all(case.supports_atomic_state_update for case in cases)


def test_parameter_style_params_are_method_filtered() -> None:
    """Parameter-style params separate execute and execute_many cases before driver selection."""
    execute_cases = _parameter_style_cases(PARAMETER_STYLE_EXECUTE_PARAMS)
    execute_many_cases = _parameter_style_cases(PARAMETER_STYLE_EXECUTE_MANY_PARAMS)
    all_cases = _parameter_style_cases(PARAMETER_STYLE_PARAMS)
    assert all(case.method == "execute" for case in execute_cases)
    assert all(case.method == "execute_many" for case in execute_many_cases)
    assert {case.id for case in execute_cases} | {case.id for case in execute_many_cases} == {
        case.id for case in all_cases
    }


def test_case_family_routes_adbc_dialects() -> None:
    """ADBC case identifiers route to their backing database family instead of defaulting to postgres."""
    expected_routes = {
        "adbc-oracle": "oracle",
        "adbc-db2": "db2",
        "adbc-snowflake": "snowflake",
        "adbc-bigquery": "bigquery",
        "adbc-duckdb": "duckdb",
        "adbc-sqlite": "sqlite",
        "adbc-postgres": "postgres",
        "adbc-pgvector": "postgres",
        "adbc-gizmosql-duckdb": "gizmosql",
    }
    observed_routes = {case_id: _case_family(_StubCaseId(id=case_id)) for case_id in expected_routes}
    assert observed_routes == expected_routes


def test_collection_preserves_container_distinct_xdist_groups(
    request: pytest.FixtureRequest, pytestconfig: pytest.Config
) -> None:
    """Container-distinct xdist_group markers survive family xdist normalization."""
    adapters_root = Path(__file__).resolve().parents[1]
    preserved_groups = (
        ("postgres", "alloydb"),
        ("gizmosql", "gizmosql_sqlite"),
        ("mysql", "mariadb"),
        ("oracle", "oracle_18c"),
        ("oracle", "oracle_adb"),
        ("postgres", "paradedb"),
        ("postgres", "pgvector"),
        ("snowflake", "snowflake"),
    )
    items: list[pytest.Item] = []
    for family, group_name in preserved_groups:
        item = _StubAdapterItem.from_parent(
            request.session, name=f"test_{group_name}", path=adapters_root / family / f"test_{group_name}.py"
        )
        item.add_marker(pytest.mark.xdist_group(group_name))
        items.append(item)

    ungrouped_postgres = _StubAdapterItem.from_parent(
        request.session, name="test_ungrouped_postgres", path=adapters_root / "postgres" / "test_driver.py"
    )
    overridden_postgres = _StubAdapterItem.from_parent(
        request.session, name="test_overridden_postgres", path=adapters_root / "postgres" / "test_driver.py"
    )
    overridden_postgres.add_marker(pytest.mark.xdist_group("custom_transient_group"))
    ungrouped_cockroach = _StubAdapterItem.from_parent(
        request.session, name="test_ungrouped_cockroach", path=adapters_root / "cockroach" / "test_retry.py"
    )
    ungrouped_snowflake = _StubAdapterItem.from_parent(
        request.session, name="test_ungrouped_snowflake", path=adapters_root / "snowflake" / "test_adbc.py"
    )
    items.extend((ungrouped_postgres, overridden_postgres, ungrouped_cockroach, ungrouped_snowflake))

    pytest_collection_modifyitems(pytestconfig, items)

    for item, (_, expected_group) in zip(items[: len(preserved_groups)], preserved_groups, strict=True):
        assert _xdist_groups(item) == [expected_group]
    assert _xdist_groups(ungrouped_postgres) == ["postgres"]
    assert _xdist_groups(overridden_postgres) == ["postgres"]
    assert _xdist_groups(ungrouped_cockroach) == ["cockroachdb"]
    assert _xdist_groups(ungrouped_snowflake) == ["snowflake"]


class _StubCaseId(msgspec.Struct, frozen=True, gc=False):
    id: str


class _StubAdapterItem(pytest.Item):
    def runtest(self) -> None:
        return None


def _xdist_groups(item: pytest.Item) -> list[str]:
    return [str(mark.args[0]) for mark in item.iter_markers(name="xdist_group") if mark.args]


def _driver_cases(params: tuple[ParameterSet, ...]) -> tuple[DriverCase, ...]:
    return tuple(cast("DriverCase", param.values[0]) for param in params)


def _adk_cases(params: tuple[ParameterSet, ...]) -> tuple[AdkStoreCase, ...]:
    return tuple(cast("AdkStoreCase", param.values[0]) for param in params)


def _parameter_style_cases(params: tuple[ParameterSet, ...]) -> tuple[ParameterStyleCase, ...]:
    return tuple(cast("ParameterStyleCase", param.values[0]) for param in params)
