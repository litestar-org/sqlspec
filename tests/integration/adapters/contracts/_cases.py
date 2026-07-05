"""Case records for shared adapter contract tests."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import pytest
from _pytest.mark.structures import Mark, MarkDecorator, ParameterSet

from sqlspec.config import DatabaseConfigProtocol
from tests.integration.adapters.contracts._schema import (
    DEFAULT_CONTRACT_TABLE,
    DUCKDB_CONTRACT_TABLE,
    MSSQL_CONTRACT_TABLE,
    MYSQL_CONTRACT_TABLE,
    ORACLE_CONTRACT_TABLE,
    POSTGRES_CONTRACT_TABLE,
    ContractTable,
)

RowCountPolicy = Literal["exact", "unavailable", "non_negative"]
StreamChunkPolicy = Literal["bounded", "advisory"]
InvalidSqlErrorPolicy = Literal["raises", "emulator_retries"]


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
    execute_rowcount_policy: RowCountPolicy = "exact"
    execute_many_rowcount_policy: RowCountPolicy = "exact"
    supports_execute_script: bool = True
    supports_filtered_statement: bool = True
    supports_loader_input: bool = True
    supports_migrations: bool = False
    supports_schema_qualified_ddl: bool = False
    supports_storage_bridge: bool = False
    supports_native_bulk_ingest: bool = False
    supports_load_from_records: bool = False
    supports_transactions: bool = True
    supports_for_update: bool = False
    supports_for_share: bool = False
    supports_returning: bool = False
    supports_json: bool = False
    supports_arrays: bool = False
    supports_vector: bool = False
    supports_exception_translation: bool = True
    supports_lob: bool = False
    supports_native_array_codec: bool = False
    supports_json_native: bool = False
    supports_merge: bool = False
    supports_merge_bulk: bool = False
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
    supports_data_dictionary_topology: bool = False
    supports_schema_qualified_data_dictionary: bool = False
    supports_native_metadata: bool = False
    supports_native_statistics: bool = False
    supports_search_filter: bool = True
    supports_grouped_subquery: bool = True
    supports_stream_reopen_after_partial_iteration: bool = True
    stream_chunk_policy: StreamChunkPolicy = "bounded"
    invalid_sql_error_policy: InvalidSqlErrorPolicy = "raises"
    unsupported_explain_reason: str | None = None
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
MSSQL_XDIST_MARK = pytest.mark.xdist_group("mssql")
POSTGRES_XDIST_MARK = pytest.mark.xdist_group("postgres")
COCKROACH_XDIST_MARK = pytest.mark.xdist_group("cockroachdb")
ADBC_MARK = pytest.mark.adbc
ARROW_ODBC_MARK = pytest.mark.arrow_odbc
MSSQL_MARK = pytest.mark.mssql
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
        supports_data_dictionary=True,
        supports_data_dictionary_topology=True,
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
        supports_data_dictionary=True,
        supports_storage_bridge=True,
        supports_native_bulk_ingest=True,
        supports_load_from_records=True,
        supports_vector=True,
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
        supports_for_update=True,
        supports_for_share=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_mysqlconnector_sync",
        supports_connection_instance=True,
        supports_custom_json_serializer=True,
        supports_native_row_streaming=True,
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
        supports_for_update=True,
        supports_for_share=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_pymysql",
        supports_connection_instance=True,
        supports_native_row_streaming=True,
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
        supports_data_dictionary=True,
        supports_data_dictionary_topology=True,
        supports_schema_qualified_data_dictionary=True,
        supports_storage_bridge=True,
        supports_native_bulk_ingest=True,
        supports_load_from_records=True,
        supports_for_update=True,
        supports_for_share=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_merge=True,
        supports_merge_bulk=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_psycopg_sync",
        supports_connection_instance=True,
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
        supports_connection_instance=True,
        supports_native_row_streaming=True,
        extra_assertions=("param_codecs:cockroach_psycopg", "streaming_native:psycopg"),
    ),
    DriverCase(
        id="adbc-sqlite-sync",
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        supports_native_metadata=True,
        supports_data_dictionary=True,
        fixture_name="contract_adbc_sqlite_driver",
        adapter="adbc",
        dialect="sqlite",
        mode="sync",
        marks=(ADBC_MARK,),
        supports_arrow=True,
        supports_explain=True,
        supports_storage_bridge=True,
        supports_native_bulk_ingest=True,
        supports_load_from_records=True,
        supports_exception_translation=False,
        execute_rowcount_policy="unavailable",
        extra_assertions=("param_codecs:adbc_sqlite",),
    ),
    DriverCase(
        id="adbc-duckdb-sync",
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        supports_native_metadata=True,
        supports_data_dictionary=True,
        fixture_name="contract_adbc_duckdb_driver",
        adapter="adbc",
        dialect="duckdb",
        mode="sync",
        marks=(ADBC_MARK,),
        table=DUCKDB_CONTRACT_TABLE,
        supports_arrow=True,
        supports_explain=True,
        supports_storage_bridge=True,
        supports_native_bulk_ingest=True,
        supports_load_from_records=True,
        supports_vector=True,
        execute_rowcount_policy="unavailable",
        extra_assertions=("param_codecs:adbc_duckdb",),
    ),
    DriverCase(
        id="adbc-postgres-sync",
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        supports_native_metadata=True,
        supports_native_statistics=True,
        supports_data_dictionary=True,
        fixture_name="contract_adbc_postgres_driver",
        adapter="adbc",
        dialect="postgres",
        mode="sync",
        marks=(ADBC_MARK, POSTGRES_XDIST_MARK),
        table=POSTGRES_CONTRACT_TABLE,
        supports_arrow=True,
        supports_storage_bridge=True,
        supports_native_bulk_ingest=True,
        supports_load_from_records=True,
        supports_for_update=True,
        supports_for_share=True,
        execute_rowcount_policy="unavailable",
        unsupported_explain_reason="ADBC PostgreSQL EXPLAIN is incompatible with the driver's COPY result transfer",
        extra_assertions=("arrow_specifics:adbc_select_to_arrow_error", "param_codecs:adbc_postgres"),
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
        supports_connection_instance=True,
        supports_merge=True,
        supports_vector=True,
        supports_lowercase_columns=True,
        supports_uuid_feature=True,
        supports_native_row_streaming=True,
        extra_assertions=(
            "explain_modifiers:oracle",
            "param_codecs:oracle",
            "driver_features:oracle_sequence",
            "driver_features:oracle_json_native",
            "driver_features:oracle_batch_errors",
            "driver_features:oracle_plsql",
            "streaming_native:oracledb",
        ),
    ),
    DriverCase(
        id="arrow-odbc-sync",
        fixture_name="contract_arrow_odbc_mssql_driver",
        adapter="arrow_odbc",
        dialect="mssql",
        mode="sync",
        marks=(MSSQL_MARK, MSSQL_XDIST_MARK, ARROW_ODBC_MARK),
        table=MSSQL_CONTRACT_TABLE,
        supports_arrow=True,
        supports_arrow_streaming=True,
        supports_native_arrow=True,
        supports_native_bulk_ingest=True,
        supports_execute_many=False,
        supports_load_from_records=False,
        supports_exception_translation=False,
        execute_rowcount_policy="unavailable",
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
        execute_rowcount_policy="unavailable",
        supports_search_filter=False,
        supports_grouped_subquery=False,
        supports_stream_reopen_after_partial_iteration=False,
        stream_chunk_policy="advisory",
        invalid_sql_error_policy="emulator_retries",
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
        supports_data_dictionary=True,
        supports_data_dictionary_topology=True,
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
        supports_for_update=True,
        supports_for_share=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_aiomysql",
        supports_connection_instance=True,
        supports_custom_json_serializer=True,
        supports_native_row_streaming=True,
        extra_assertions=(
            "explain_modifiers:mysql",
            "arrow_specifics:mysql",
            "param_codecs:mysql",
            "storage_bridge:mysql_decimal",
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
        supports_for_update=True,
        supports_for_share=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_asyncmy",
        supports_connection_instance=True,
        supports_custom_json_serializer=True,
        supports_native_row_streaming=True,
        extra_assertions=(
            "explain_modifiers:mysql",
            "arrow_specifics:mysql",
            "param_codecs:mysql",
            "storage_bridge:mysql_decimal",
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
        supports_for_update=True,
        supports_for_share=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_mysqlconnector_async",
        supports_custom_json_serializer=True,
        supports_native_row_streaming=True,
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
        supports_data_dictionary=True,
        supports_data_dictionary_topology=True,
        supports_schema_qualified_data_dictionary=True,
        supports_storage_bridge=True,
        supports_native_bulk_ingest=True,
        supports_load_from_records=True,
        supports_for_update=True,
        supports_for_share=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_merge=True,
        supports_merge_bulk=True,
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
        supports_data_dictionary=True,
        supports_data_dictionary_topology=True,
        supports_schema_qualified_data_dictionary=True,
        supports_storage_bridge=True,
        supports_native_bulk_ingest=True,
        supports_load_from_records=True,
        supports_for_update=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_merge=True,
        supports_merge_bulk=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_psqlpy",
        supports_connection_instance=True,
        supports_native_row_streaming=True,
        execute_rowcount_policy="unavailable",
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
        supports_data_dictionary=True,
        supports_data_dictionary_topology=True,
        supports_schema_qualified_data_dictionary=True,
        supports_storage_bridge=True,
        supports_native_bulk_ingest=True,
        supports_load_from_records=True,
        supports_for_update=True,
        supports_for_share=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_merge=True,
        supports_merge_bulk=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_psycopg_async",
        supports_connection_instance=True,
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
        supports_connection_instance=True,
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
        supports_for_share=True,
        supports_returning=True,
        supports_json=True,
        supports_arrays=True,
        supports_pooling=True,
        supports_connection_hook=True,
        config_factory_fixture="lifecycle_config_cockroach_psycopg_async",
        supports_connection_instance=True,
        supports_native_row_streaming=True,
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
        supports_connection_instance=True,
        supports_merge=True,
        supports_merge_bulk=True,
        supports_vector=True,
        supports_lowercase_columns=True,
        supports_uuid_feature=True,
        supports_native_row_streaming=True,
        extra_assertions=(
            "explain_modifiers:oracle",
            "arrow_specifics:oracle",
            "param_codecs:oracle",
            "driver_features:oracle_sequence",
            "driver_features:oracle_json_native",
            "driver_features:oracle_batch_errors",
            "driver_features:oracle_plsql",
            "statement_stack:oracle_native",
            "streaming_native:oracledb",
        ),
    ),
)

DEFERRED_DRIVER_CASES = (
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
        "pymssql-sync",
        "",
        "pymssql",
        "tsql",
        "sync",
        integration_status="deferred",
        reason="No active SQL Server fixture exists for pymssql.",
        supports_execute_many=True,
        supports_migrations=True,
        supports_data_dictionary=True,
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
LIFECYCLE_CAPABILITIES = (
    "supports_pooling",
    "supports_connection_instance",
    "supports_connection_hook",
    "supports_lowercase_columns",
    "supports_uuid_feature",
    "supports_custom_json_serializer",
    "supports_custom_type_adapters",
)


def _driver_params(cases: tuple[DriverCase, ...]) -> "tuple[ParameterSet, ...]":
    return tuple(pytest.param(case, id=case.id, marks=case.marks) for case in cases)


def _driver_params_where(cases: tuple[DriverCase, ...], *capability_names: str) -> "tuple[ParameterSet, ...]":
    return _driver_params(tuple(case for case in cases if any(getattr(case, name) for name in capability_names)))


def _driver_params_without(cases: tuple[DriverCase, ...], *capability_names: str) -> "tuple[ParameterSet, ...]":
    return _driver_params(tuple(case for case in cases if not any(getattr(case, name) for name in capability_names)))


def sync_driver_params_with(*capability_names: str) -> "tuple[ParameterSet, ...]":
    """Return sync driver params that opt into at least one named capability."""
    return _driver_params_where(SYNC_DRIVER_CASES, *capability_names)


def async_driver_params_with(*capability_names: str) -> "tuple[ParameterSet, ...]":
    """Return async driver params that opt into at least one named capability."""
    return _driver_params_where(ASYNC_DRIVER_CASES, *capability_names)


def sync_driver_params_without(*capability_names: str) -> "tuple[ParameterSet, ...]":
    """Return sync driver params that opt out of all named capabilities."""
    return _driver_params_without(SYNC_DRIVER_CASES, *capability_names)


def async_driver_params_without(*capability_names: str) -> "tuple[ParameterSet, ...]":
    """Return async driver params that opt out of all named capabilities."""
    return _driver_params_without(ASYNC_DRIVER_CASES, *capability_names)


SYNC_DRIVER_PARAMS = _driver_params(SYNC_DRIVER_CASES)
ASYNC_DRIVER_PARAMS = _driver_params(ASYNC_DRIVER_CASES)
DRIVER_PARAMS = SYNC_DRIVER_PARAMS
SYNC_LIFECYCLE_DRIVER_PARAMS = tuple(sync_driver_params_with(*LIFECYCLE_CAPABILITIES))
ASYNC_LIFECYCLE_DRIVER_PARAMS = tuple(async_driver_params_with(*LIFECYCLE_CAPABILITIES))


def get_driver_case(case_id: str) -> DriverCase:
    """Return a registered driver case by id."""
    return DRIVER_CASE_BY_ID[case_id]
