"""Contract metadata guardrails for adapter case records."""

from typing import cast

from _pytest.mark.structures import ParameterSet

from tests.integration.adapters.contracts._adk_cases import AdkStoreCase, adk_store_params_with
from tests.integration.adapters.contracts._cases import (
    ACTIVE_DRIVER_CASES,
    ASYNC_LIFECYCLE_DRIVER_PARAMS,
    LIFECYCLE_CAPABILITIES,
    SYNC_LIFECYCLE_DRIVER_PARAMS,
    DriverCase,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters.contracts._inputs import (
    PARAMETER_STYLE_EXECUTE_MANY_PARAMS,
    PARAMETER_STYLE_EXECUTE_PARAMS,
    PARAMETER_STYLE_PARAMS,
    ParameterStyleCase,
)
from tests.integration.adapters.contracts._migration_cases import ACTIVE_MIGRATION_CASES
from tests.integration.adapters.contracts._postgres_extension_cases import (
    ASYNC_POSTGRES_EXTENSION_CASES,
    SYNC_POSTGRES_EXTENSION_CASES,
)

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
    "psqlpy-async",
    "psycopg-async",
    "psycopg-sync",
}
MIGRATION_LIFECYCLE_CASE_IDS = {
    "adbc-postgres-sync",
    "adbc-sqlite-sync",
    "aiomysql-async",
    "aiosqlite-async",
    "asyncmy-async",
    "asyncpg-async",
    "duckdb-sync",
    "mysqlconnector-async",
    "oracledb-async",
    "oracledb-sync",
    "psqlpy-async",
    "psycopg-async",
    "psycopg-sync",
    "pymysql-sync",
    "sqlite-sync",
}
MERGE_CASE_IDS = {"asyncpg-async", "oracledb-async", "oracledb-sync", "psqlpy-async", "psycopg-async", "psycopg-sync"}
MERGE_BULK_CASE_IDS = {"asyncpg-async", "oracledb-async", "psqlpy-async", "psycopg-async", "psycopg-sync"}
VECTOR_CASE_IDS = {"adbc-duckdb-sync", "duckdb-sync", "oracledb-async", "oracledb-sync"}
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


def _driver_cases(params: tuple[ParameterSet, ...]) -> tuple[DriverCase, ...]:
    return tuple(cast("DriverCase", param.values[0]) for param in params)


def _adk_cases(params: tuple[ParameterSet, ...]) -> tuple[AdkStoreCase, ...]:
    return tuple(cast("AdkStoreCase", param.values[0]) for param in params)


def _parameter_style_cases(params: tuple[ParameterSet, ...]) -> tuple[ParameterStyleCase, ...]:
    return tuple(cast("ParameterStyleCase", param.values[0]) for param in params)


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


def test_postgres_extension_cases_are_contract_owned() -> None:
    """pgvector/ParadeDB extension behavior belongs to the shared extension contract."""
    case_ids = {case.id for case in (*SYNC_POSTGRES_EXTENSION_CASES, *ASYNC_POSTGRES_EXTENSION_CASES)}
    assert case_ids == POSTGRES_EXTENSION_CASE_IDS


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
