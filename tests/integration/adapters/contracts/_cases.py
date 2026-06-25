"""Case records for shared adapter contract tests."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import pytest
from _pytest.mark.structures import Mark, MarkDecorator

from sqlspec.config import DatabaseConfigProtocol
from tests.integration.adapters.contracts._schema import (
    DEFAULT_CONTRACT_TABLE,
    DUCKDB_CONTRACT_TABLE,
    MYSQL_CONTRACT_TABLE,
    ORACLE_CONTRACT_TABLE,
    POSTGRES_CONTRACT_TABLE,
    ContractTable,
)


@dataclass(frozen=True)
class DriverCase:
    """Driver fixture and capability metadata for contract tests."""

    id: str
    fixture_name: str
    adapter: str
    dialect: str
    mode: Literal["sync", "async"]
    marks: tuple[Mark | MarkDecorator, ...] = ()
    integration_status: Literal["active", "local-only", "unit-only", "deferred"] = "active"
    reason: str | None = None
    table: ContractTable = DEFAULT_CONTRACT_TABLE
    table_fixture: str | None = None
    supports_arrow: bool = False
    supports_arrow_streaming: bool = False
    supports_native_row_streaming: bool = False
    streaming_row_count: int = 10_000
    supports_native_arrow: bool = False
    arrow_reader_honors_batch_size: bool = False
    supports_explain: bool = False
    supports_execute_many: bool = True
    supports_execute_script: bool = True
    supports_filtered_statement: bool = True
    supports_loader_input: bool = True
    supports_migrations: bool = False
    supports_schema_qualified_ddl: bool = False
    supports_storage_bridge: bool = False
    supports_transactions: bool = True
    supports_for_update: bool = False
    supports_returning: bool = False
    supports_json: bool = False
    supports_arrays: bool = False
    supports_vector: bool = False
    supports_exception_translation: bool = True
    supports_lob: bool = False
    supports_native_array_codec: bool = False
    supports_json_native: bool = False
    supports_merge: bool = False
    supports_copy: bool = False
    supports_pooling: bool = False
    supports_connection_hook: bool = False
    supports_connection_instance: bool = False
    supports_lowercase_columns: bool = False
    supports_uuid_feature: bool = False
    supports_custom_json_serializer: bool = False
    supports_custom_type_adapters: bool = False
    supports_multi_schema_migrations: bool = False
    supports_data_dictionary: bool = False
    config_factory_fixture: str | None = None
    deviations: tuple[str, ...] = ()
    extra_assertions: tuple[str, ...] = ()


@dataclass(frozen=True)
class DriverCaseContext:
    """Resolved driver instance paired with its case metadata."""

    case: DriverCase
    driver: object
    make_config: "Callable[..., DatabaseConfigProtocol[Any, Any, Any]] | None" = None


SQLITE_XDIST_MARK = pytest.mark.xdist_group("sqlite")
DUCKDB_XDIST_MARK = pytest.mark.xdist_group("duckdb")
MYSQL_XDIST_MARK = pytest.mark.xdist_group("mysql")
POSTGRES_XDIST_MARK = pytest.mark.xdist_group("postgres")
COCKROACH_XDIST_MARK = pytest.mark.xdist_group("cockroachdb")
ADBC_MARK = pytest.mark.adbc
ORACLE_XDIST_MARK = pytest.mark.xdist_group("oracle")
BIGQUERY_MARK = pytest.mark.bigquery
BIGQUERY_XDIST_MARK = pytest.mark.xdist_group("bigquery")

SYNC_DRIVER_CASES = (
    DriverCase(
        id="sqlite-sync",
        fixture_name="contract_sqlite_driver",
        adapter="sqlite",
        dialect="sqlite",
        mode="sync",
        marks=(SQLITE_XDIST_MARK,),
        supports_arrow=True,
        supports_native_row_streaming=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_sqlite",
        supports_connection_instance=True,
        supports_custom_json_serializer=True,
        supports_custom_type_adapters=True,
        extra_assertions=("driver_basics:noop", "streaming_native:sqlite"),
    ),
    DriverCase(
        id="duckdb-sync",
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        arrow_reader_honors_batch_size=True,
        fixture_name="contract_duckdb_driver",
        adapter="duckdb",
        dialect="duckdb",
        mode="sync",
        marks=(DUCKDB_XDIST_MARK,),
        table=DUCKDB_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_duckdb",
        supports_connection_instance=True,
        supports_uuid_feature=True,
        supports_custom_json_serializer=True,
        extra_assertions=(
            "explain_modifiers:duckdb",
            "arrow_specifics:duckdb",
            "execute_many_specifics:duckdb",
            "param_codecs:duckdb",
            "driver_features:duckdb_set_variable",
        ),
    ),
    DriverCase(
        id="mysqlconnector-sync",
        fixture_name="contract_mysqlconnector_sync_driver",
        adapter="mysqlconnector",
        dialect="mysql",
        mode="sync",
        marks=(MYSQL_XDIST_MARK,),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_mysqlconnector_sync",
        supports_custom_json_serializer=True,
        supports_native_row_streaming=True,
        deviations=("no-returning", "autocommit-ddl"),
        extra_assertions=("param_codecs:mysql", "streaming_native:mysqlconnector"),
    ),
    DriverCase(
        id="pymysql-sync",
        fixture_name="contract_pymysql_driver",
        adapter="pymysql",
        dialect="mysql",
        mode="sync",
        marks=(MYSQL_XDIST_MARK,),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_pymysql",
        supports_native_row_streaming=True,
        deviations=("no-returning", "autocommit-ddl"),
        extra_assertions=("param_codecs:mysql", "streaming_native:pymysql"),
    ),
    DriverCase(
        id="psycopg-sync",
        fixture_name="contract_psycopg_sync_driver",
        adapter="psycopg",
        dialect="postgres",
        mode="sync",
        marks=(POSTGRES_XDIST_MARK,),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_psycopg_sync",
        supports_native_row_streaming=True,
        extra_assertions=(
            "explain_modifiers:postgres",
            "arrow_specifics:postgres",
            "execute_many_specifics:postgres",
            "param_codecs:psycopg",
            "driver_features:psycopg_copy",
            "streaming_native:psycopg",
        ),
    ),
    DriverCase(
        id="cockroach-psycopg-sync",
        fixture_name="contract_cockroach_psycopg_sync_driver",
        adapter="cockroach_psycopg",
        dialect="postgres",
        mode="sync",
        marks=(COCKROACH_XDIST_MARK,),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_cockroach_psycopg_sync",
        supports_native_row_streaming=True,
        deviations=("cockroach-serializable-transactions",),
        extra_assertions=("param_codecs:cockroach_psycopg", "streaming_native:psycopg"),
    ),
    DriverCase(
        id="adbc-sqlite-sync",
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        fixture_name="contract_adbc_sqlite_driver",
        adapter="adbc",
        dialect="sqlite",
        mode="sync",
        marks=(ADBC_MARK,),
        supports_arrow=True,
        supports_explain=True,
        supports_storage_bridge=True,
        supports_exception_translation=False,
        deviations=("execute-rows-affected-unavailable",),
        extra_assertions=("param_codecs:adbc_sqlite",),
    ),
    DriverCase(
        id="adbc-duckdb-sync",
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        fixture_name="contract_adbc_duckdb_driver",
        adapter="adbc",
        dialect="duckdb",
        mode="sync",
        marks=(ADBC_MARK,),
        table=DUCKDB_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_storage_bridge=True,
        deviations=("execute-rows-affected-unavailable",),
        extra_assertions=("param_codecs:adbc_duckdb",),
    ),
    DriverCase(
        id="adbc-postgres-sync",
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        fixture_name="contract_adbc_postgres_driver",
        adapter="adbc",
        dialect="postgres",
        mode="sync",
        marks=(ADBC_MARK, POSTGRES_XDIST_MARK),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        deviations=("execute-rows-affected-unavailable", "explain-copy-incompatible"),
        extra_assertions=("param_codecs:adbc_postgres",),
    ),
    DriverCase(
        id="oracledb-sync",
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        arrow_reader_honors_batch_size=True,
        fixture_name="contract_oracle_sync_driver",
        adapter="oracledb",
        dialect="oracle",
        mode="sync",
        marks=(ORACLE_XDIST_MARK,),
        table=ORACLE_CONTRACT_TABLE,
        supports_arrow=True,
        supports_execute_many=True,
        supports_explain=True,
        supports_for_update=True,
        supports_returning=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_oracle_sync",
        supports_lowercase_columns=True,
        supports_uuid_feature=True,
        supports_native_row_streaming=True,
        deviations=("no-for-share",),
        extra_assertions=(
            "explain_modifiers:oracle",
            "param_codecs:oracle",
            "driver_features:oracle_sequence",
            "driver_features:oracle_json_native",
            "streaming_native:oracledb",
        ),
    ),
    DriverCase(
        id="bigquery-sync",
        fixture_name="contract_bigquery_driver",
        adapter="bigquery",
        dialect="bigquery",
        mode="sync",
        marks=(BIGQUERY_MARK, BIGQUERY_XDIST_MARK),
        table_fixture="bigquery_contract_table",
        supports_execute_many=True,
        supports_exception_translation=False,
        supports_connection_hook=True,
        supports_native_row_streaming=True,
        streaming_row_count=600,
        config_factory_fixture="lifecycle_config_bigquery",
        deviations=(
            "execute-rows-affected-unavailable",
            "emulator-no-grouped-subquery",
            "emulator-no-search-filter",
            "emulator-retries-invalid-sql",
            "emulator-streaming-reopen-hangs",
            "streaming-page-size-advisory",
        ),
        extra_assertions=(
            "param_codecs:bigquery",
            "driver_features:bigquery_sql_features",
            "driver_features:bigquery_job_controls",
            "streaming_native:bigquery",
        ),
    ),
)

ASYNC_DRIVER_CASES = (
    DriverCase(
        id="aiosqlite-async",
        fixture_name="contract_aiosqlite_driver",
        adapter="aiosqlite",
        dialect="sqlite",
        mode="async",
        marks=(SQLITE_XDIST_MARK, pytest.mark.anyio),
        supports_arrow=True,
        supports_native_row_streaming=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_aiosqlite",
        supports_connection_instance=True,
        extra_assertions=("driver_basics:noop", "arrow_specifics:sqlite", "streaming_native:aiosqlite"),
    ),
    DriverCase(
        id="aiomysql-async",
        fixture_name="contract_aiomysql_driver",
        adapter="aiomysql",
        dialect="mysql",
        mode="async",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_aiomysql",
        supports_custom_json_serializer=True,
        supports_native_row_streaming=True,
        deviations=("no-returning", "autocommit-ddl"),
        extra_assertions=(
            "explain_modifiers:mysql",
            "arrow_specifics:mysql",
            "param_codecs:mysql",
            "streaming_native:aiomysql",
        ),
    ),
    DriverCase(
        id="asyncmy-async",
        fixture_name="contract_asyncmy_driver",
        adapter="asyncmy",
        dialect="mysql",
        mode="async",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_asyncmy",
        supports_custom_json_serializer=True,
        supports_native_row_streaming=True,
        deviations=("no-returning", "autocommit-ddl"),
        extra_assertions=(
            "explain_modifiers:mysql",
            "arrow_specifics:mysql",
            "param_codecs:mysql",
            "streaming_native:asyncmy",
        ),
    ),
    DriverCase(
        id="mysqlconnector-async",
        fixture_name="contract_mysqlconnector_async_driver",
        adapter="mysqlconnector",
        dialect="mysql",
        mode="async",
        marks=(MYSQL_XDIST_MARK, pytest.mark.anyio),
        table=MYSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_storage_bridge=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_mysqlconnector_async",
        supports_custom_json_serializer=True,
        supports_native_row_streaming=True,
        deviations=("no-returning", "autocommit-ddl"),
        extra_assertions=("param_codecs:mysql", "streaming_native:mysqlconnector"),
    ),
    DriverCase(
        id="asyncpg-async",
        fixture_name="contract_asyncpg_driver",
        adapter="asyncpg",
        dialect="postgres",
        mode="async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_native_row_streaming=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_asyncpg",
        supports_connection_instance=True,
        extra_assertions=(
            "explain_modifiers:postgres",
            "arrow_specifics:postgres",
            "execute_many_specifics:postgres",
            "param_codecs:asyncpg",
            "streaming_native:asyncpg",
        ),
    ),
    DriverCase(
        id="psqlpy-async",
        fixture_name="contract_psqlpy_driver",
        adapter="psqlpy",
        dialect="postgres",
        mode="async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_psqlpy",
        supports_native_row_streaming=True,
        deviations=("execute-rows-affected-unavailable",),
        extra_assertions=(
            "explain_modifiers:postgres",
            "arrow_specifics:postgres",
            "param_codecs:psqlpy",
            "streaming_native:psqlpy",
        ),
    ),
    DriverCase(
        id="psycopg-async",
        fixture_name="contract_psycopg_async_driver",
        adapter="psycopg",
        dialect="postgres",
        mode="async",
        marks=(POSTGRES_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_psycopg_async",
        supports_native_row_streaming=True,
        extra_assertions=(
            "explain_modifiers:postgres",
            "arrow_specifics:postgres",
            "param_codecs:psycopg",
            "driver_features:psycopg_copy",
            "streaming_native:psycopg",
        ),
    ),
    DriverCase(
        id="cockroach-asyncpg-async",
        fixture_name="contract_cockroach_asyncpg_driver",
        adapter="cockroach_asyncpg",
        dialect="postgres",
        mode="async",
        marks=(COCKROACH_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_native_row_streaming=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_cockroach_asyncpg",
        deviations=("cockroach-serializable-transactions",),
        extra_assertions=("param_codecs:cockroach_asyncpg", "streaming_native:asyncpg"),
    ),
    DriverCase(
        id="cockroach-psycopg-async",
        fixture_name="contract_cockroach_psycopg_async_driver",
        adapter="cockroach_psycopg",
        dialect="postgres",
        mode="async",
        marks=(COCKROACH_XDIST_MARK, pytest.mark.anyio),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_execute_many=True,
        supports_migrations=True,
        supports_schema_qualified_ddl=True,
        supports_storage_bridge=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_cockroach_psycopg_async",
        supports_native_row_streaming=True,
        deviations=("cockroach-serializable-transactions",),
        extra_assertions=("param_codecs:cockroach_psycopg", "streaming_native:psycopg"),
    ),
    DriverCase(
        id="oracledb-async",
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        arrow_reader_honors_batch_size=True,
        fixture_name="contract_oracle_async_driver",
        adapter="oracledb",
        dialect="oracle",
        mode="async",
        marks=(ORACLE_XDIST_MARK, pytest.mark.anyio),
        table=ORACLE_CONTRACT_TABLE,
        supports_arrow=True,
        supports_execute_many=True,
        supports_explain=True,
        supports_for_update=True,
        supports_returning=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_oracle_async",
        supports_lowercase_columns=True,
        supports_uuid_feature=True,
        supports_native_row_streaming=True,
        deviations=("no-for-share",),
        extra_assertions=(
            "explain_modifiers:oracle",
            "arrow_specifics:oracle",
            "param_codecs:oracle",
            "driver_features:oracle_sequence",
            "driver_features:oracle_json_native",
            "streaming_native:oracledb",
        ),
    ),
)

DEFERRED_DRIVER_CASES = (
    DriverCase(
        "arrow-odbc-sync",
        "",
        "arrow_odbc",
        "odbc",
        "sync",
        integration_status="deferred",
        reason="No active integration fixture exists for arrow_odbc.",
    ),
    DriverCase(
        "mssql-python-sync",
        "",
        "mssql_python",
        "tsql",
        "sync",
        integration_status="deferred",
        reason="No active integration fixture exists for mssql_python.",
    ),
    DriverCase(
        "spanner-sync",
        "",
        "spanner",
        "spanner",
        "sync",
        integration_status="deferred",
        reason=(
            "Spanner session controls remain covered by unit and adapter-specific tests until "
            "the shared matrix has safe active opt-in gate wiring."
        ),
    ),
)

ACTIVE_DRIVER_CASES = SYNC_DRIVER_CASES + ASYNC_DRIVER_CASES
DRIVER_CASES = ACTIVE_DRIVER_CASES + DEFERRED_DRIVER_CASES
DRIVER_CASE_BY_ID = {case.id: case for case in DRIVER_CASES}
SYNC_DRIVER_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in SYNC_DRIVER_CASES)
ASYNC_DRIVER_PARAMS = tuple(pytest.param(case, id=case.id, marks=case.marks) for case in ASYNC_DRIVER_CASES)
DRIVER_PARAMS = SYNC_DRIVER_PARAMS


def get_driver_case(case_id: str) -> DriverCase:
    """Return a registered driver case by id."""
    return DRIVER_CASE_BY_ID[case_id]
