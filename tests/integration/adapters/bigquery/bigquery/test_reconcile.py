"""End-to-end BigQuery emulator acceptance tests for extension schema reconciliation."""

from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest
from google.api_core import exceptions as api_exceptions
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials, Credentials

from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver
from sqlspec.adapters.bigquery.adk import BigQueryADKStore
from sqlspec.adapters.bigquery.data_dictionary import BigQueryDataDictionary, BigQueryMetadataScope
from sqlspec.adapters.bigquery.litestar import BigQueryStore
from sqlspec.config import ExtensionConfigs
from sqlspec.data_dictionary import ColumnMetadata, TableMetadata
from sqlspec.exceptions import MigrationError, SQLSpecError
from sqlspec.extensions.adk.migrations.reconcile import reconcile as adk_reconcile
from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
from sqlspec.migrations.context import MigrationContext

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService

pytestmark = [pytest.mark.bigquery, pytest.mark.integration, pytest.mark.xdist_group("bigquery")]

_INFORMATION_SCHEMA_VIEWS: tuple[str, ...] = (
    "INFORMATION_SCHEMA.KEY_COLUMN_USAGE",
    "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
    "INFORMATION_SCHEMA.TABLES",
    "INFORMATION_SCHEMA.COLUMNS",
)


class _EmulatorBigQueryDataDictionary(BigQueryDataDictionary):
    """Adapter over the INFORMATION_SCHEMA subset implemented by goccy/bigquery-emulator."""

    def get_tables(self, driver: BigQueryDriver, schema: str | None = None) -> list[TableMetadata]:
        scope = BigQueryMetadataScope.from_schema(schema)
        tables_view = scope.dataset_information_schema_table("TABLES")
        sql = f"SELECT table_name, table_type FROM {tables_view} ORDER BY table_name"
        return driver.select(sql, schema_type=TableMetadata)

    def get_columns(
        self, driver: BigQueryDriver, table: str | None = None, schema: str | None = None
    ) -> list[ColumnMetadata]:
        scope = BigQueryMetadataScope.from_schema(schema)
        columns_view = scope.dataset_information_schema_table("COLUMNS")
        sql = (
            f"SELECT column_name, data_type, is_nullable FROM {columns_view} "
            "WHERE table_name = @table_name ORDER BY ordinal_position"
        )
        return driver.select(sql, table_name=table, schema_type=ColumnMetadata)


def _anonymous_credentials() -> Credentials:
    factory = cast("Any", AnonymousCredentials)
    return cast("Credentials", factory())


def _skip_if_emulator_gap(exc: BaseException) -> None:
    message = str(exc)
    upper = message.upper()
    if "UNRECOGNIZED NAME: COLUMN_DEFAULT" in upper:
        pytest.skip(f"BigQuery emulator unsupported INFORMATION_SCHEMA.COLUMNS.column_default query: {exc}")
    for view_name in _INFORMATION_SCHEMA_VIEWS:
        if view_name in upper:
            pytest.skip(f"BigQuery emulator unsupported {view_name} query: {exc}")
    if "INFORMATION_SCHEMA" in upper:
        pytest.skip(f"BigQuery emulator unsupported INFORMATION_SCHEMA query: {exc}")
    if "GOT KEYWORD DEFAULT" in upper:
        pytest.skip(f"BigQuery emulator unsupported CREATE TABLE DEFAULT clause: {exc}")
    if "ALTER TABLE" in upper and ("ADD COLUMN" in upper or "UNSUPPORTED" in upper or "SYNTAX" in upper):
        pytest.skip(f"BigQuery emulator unsupported ALTER TABLE ADD COLUMN statement: {exc}")


def _emulator_column_names(config: BigQueryConfig, project: str, dataset: str, table_name: str) -> set[str]:
    with config.provide_session() as driver:
        rows = driver.select(
            f"SELECT column_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = @table_name",
            table_name=table_name,
        )
    return {str(row["column_name"]) for row in rows}


@pytest.mark.anyio
async def test_bigquery_emulator_reconcile_convergence_crud_and_strict_refusal(
    bigquery_service: "BigQueryService",
) -> None:
    """Reconcilers verify live BigQuery emulator tables idempotently, preserve rows, and refuse NOT NULL drift."""
    suffix = uuid4().hex[:8]
    project = bigquery_service.project
    dataset = f"reconcile_ds_{suffix}"

    litestar_table = f"ls_conv_{suffix}"
    events_table = f"ev_conv_{suffix}"
    session_table = f"adk_conv_sess_{suffix}"
    adk_events_table = f"adk_conv_ev_{suffix}"
    app_state_table = f"adk_conv_app_{suffix}"
    user_state_table = f"adk_conv_usr_{suffix}"
    metadata_table = f"adk_conv_meta_{suffix}"
    bad_session_table = f"adk_bad_sess_{suffix}"

    ext_config: ExtensionConfigs = {
        "litestar": {"session_table": litestar_table, "migration_mode": "reconcile"},
        "events": {"queue_table": events_table, "migration_mode": "reconcile"},
        "adk": {
            "migration_mode": "reconcile",
            "enable_memory": False,
            "owner_id_column": "owner_id STRING",
            "session_table": session_table,
            "events_table": adk_events_table,
            "app_state_table": app_state_table,
            "user_state_table": user_state_table,
            "metadata_table": metadata_table,
        },
    }
    config = BigQueryConfig(
        connection_config={
            "project": project,
            "dataset_id": dataset,
            "client_options": ClientOptions(api_endpoint=bigquery_service.endpoint),
            "credentials": _anonymous_credentials(),
        },
        driver_features={"job_result_timeout": 30.0, "job_retry_deadline": 0.0, "request_timeout": 15.0},
        extension_config=ext_config,
    )

    try:
        with config.provide_session() as driver:
            driver.connection.create_dataset(dataset, exists_ok=True)
            driver.execute_script(
                f"CREATE TABLE `{project}.{dataset}.{litestar_table}` ("
                "session_id STRING NOT NULL, data BYTES NOT NULL, expires_at TIMESTAMP, created_at TIMESTAMP)"
            )
            driver.execute_script(
                f"INSERT INTO `{project}.{dataset}.{litestar_table}` (session_id, data, expires_at, created_at) "
                "VALUES ('ls-1', CAST('payload' AS BYTES), NULL, CURRENT_TIMESTAMP())"
            )
            driver.execute_script(
                f"CREATE TABLE `{project}.{dataset}.{events_table}` ("
                "event_id STRING NOT NULL, channel STRING NOT NULL, payload_json JSON NOT NULL, "
                "metadata_json JSON, status STRING NOT NULL, available_at TIMESTAMP NOT NULL, "
                "lease_expires_at TIMESTAMP, attempts INT64 NOT NULL, created_at TIMESTAMP NOT NULL, "
                "acknowledged_at TIMESTAMP)"
            )
            driver.execute_script(
                f"CREATE TABLE `{project}.{dataset}.{session_table}` ("
                "id STRING NOT NULL, app_name STRING NOT NULL, user_id STRING NOT NULL, owner_id STRING, "
                "state JSON, create_time TIMESTAMP NOT NULL, update_time TIMESTAMP NOT NULL)"
            )
            driver.execute_script(
                f"INSERT INTO `{project}.{dataset}.{session_table}` "
                "(id, app_name, user_id, owner_id, state, create_time, update_time) "
                """VALUES ('s-1', 'app', 'user', 'tenant-1', PARSE_JSON('{"step": 1}'), """
                "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"
            )
            driver.execute_script(
                f"CREATE TABLE `{project}.{dataset}.{adk_events_table}` ("
                "id STRING NOT NULL, app_name STRING NOT NULL, user_id STRING NOT NULL, "
                "session_id STRING NOT NULL, invocation_id STRING, timestamp TIMESTAMP NOT NULL, event_data JSON)"
            )
            driver.execute_script(
                f"INSERT INTO `{project}.{dataset}.{adk_events_table}` "
                "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                """VALUES ('e-1', 'app', 'user', 's-1', 'inv-1', CURRENT_TIMESTAMP(), """
                """PARSE_JSON('{"author": "user", "text": "hi"}'))"""
            )
            driver.execute_script(
                f"CREATE TABLE `{project}.{dataset}.{app_state_table}` ("
                "app_name STRING NOT NULL, state JSON, update_time TIMESTAMP NOT NULL)"
            )
            driver.execute_script(
                f"CREATE TABLE `{project}.{dataset}.{user_state_table}` ("
                "app_name STRING NOT NULL, user_id STRING NOT NULL, state JSON, update_time TIMESTAMP NOT NULL)"
            )
            driver.execute_script(
                f"CREATE TABLE `{project}.{dataset}.{metadata_table}` (key STRING NOT NULL, value STRING NOT NULL)"
            )
            driver.execute_script(
                f"INSERT INTO `{project}.{dataset}.{metadata_table}` (key, value) VALUES ('schema_mode', 'reconcile')"
            )
            driver.execute_script(
                f"CREATE TABLE `{project}.{dataset}.{bad_session_table}` ("
                "id STRING NOT NULL, app_name STRING NOT NULL, user_id STRING NOT NULL, "
                "state JSON, create_time TIMESTAMP NOT NULL)"
            )

            driver._data_dictionary = _EmulatorBigQueryDataDictionary()
            for _ in range(2):
                ctx = MigrationContext(config=config, dialect="bigquery", driver=driver)
                assert litestar_reconcile(ctx) is None
                assert events_reconcile(ctx) is None
                assert adk_reconcile(ctx) is None

            bad_config = BigQueryConfig(
                connection_config=dict(config.connection_config),
                driver_features=dict(config.driver_features),
                extension_config={
                    "adk": {
                        "migration_mode": "reconcile",
                        "enable_memory": False,
                        "session_table": bad_session_table,
                        "events_table": adk_events_table,
                        "app_state_table": app_state_table,
                        "user_state_table": user_state_table,
                        "metadata_table": metadata_table,
                    }
                },
            )
            with pytest.raises(MigrationError, match="update_time") as exc_info:
                adk_reconcile(MigrationContext(config=bad_config, dialect="bigquery", driver=driver))
            assert 'migration_mode="versioned"' in str(exc_info.value)

        litestar_store = BigQueryStore(config)
        assert await litestar_store.exists("ls-1") is True
        assert await litestar_store.get("ls-1") == b"payload"

        adk_store = BigQueryADKStore(config)
        assert adk_store.get_metadata("schema_mode") == "reconcile"
        restored = adk_store.get_session("app", "user", "s-1")
        assert restored is not None
        assert restored["state"] == {"step": 1}
        assert len(adk_store.get_events("app", "user", "s-1")) == 1
    finally:
        with config.provide_session() as driver:
            driver.connection.delete_dataset(dataset, delete_contents=True, not_found_ok=True)


def test_bigquery_emulator_additive_alter_and_migrate_up_or_skip_emulator_gap(
    bigquery_service: "BigQueryService",
) -> None:
    """Additive ALTER TABLE ADD COLUMN and migrate_up() pass or skip naming the unsupported emulator query."""
    suffix = uuid4().hex[:8]
    project = bigquery_service.project
    dataset = bigquery_service.dataset
    qualified_dataset = f"{project}.{dataset}"

    session_table = f"adk_up_sess_{suffix}"
    adk_events_table = f"adk_up_ev_{suffix}"
    app_state_table = f"adk_up_app_{suffix}"
    user_state_table = f"adk_up_usr_{suffix}"
    metadata_table = f"adk_up_meta_{suffix}"
    ledger_table = f"ddl_mig_{suffix}"

    config = BigQueryConfig(
        connection_config={
            "project": project,
            "dataset_id": qualified_dataset,
            "client_options": ClientOptions(api_endpoint=bigquery_service.endpoint),
            "credentials": _anonymous_credentials(),
        },
        driver_features={"job_result_timeout": 30.0, "job_retry_deadline": 0.0, "request_timeout": 15.0},
        migration_config={"version_table_name": ledger_table},
        extension_config={
            "adk": {
                "migration_mode": "reconcile",
                "enable_memory": False,
                "owner_id_column": "owner_id STRING",
                "session_table": session_table,
                "events_table": adk_events_table,
                "app_state_table": app_state_table,
                "user_state_table": user_state_table,
                "metadata_table": metadata_table,
            }
        },
    )

    try:
        with config.provide_session() as driver:
            driver.execute_script(
                f"CREATE TABLE `{project}.{dataset}.{session_table}` ("
                "id STRING NOT NULL, app_name STRING NOT NULL, user_id STRING NOT NULL, "
                "state JSON, create_time TIMESTAMP NOT NULL, update_time TIMESTAMP NOT NULL)"
            )
            driver.execute(
                f"INSERT INTO `{project}.{dataset}.{session_table}` "
                "(id, app_name, user_id, create_time, update_time) "
                "VALUES (?, ?, ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
                ("s-keep", "app", "user"),
            )
            try:
                driver.data_dictionary.get_tables(driver, schema=dataset)
            except (SQLSpecError, api_exceptions.GoogleAPICallError) as exc:
                _skip_if_emulator_gap(exc)
                raise

        try:
            config.migrate_up()
            config.migrate_up()
        except (SQLSpecError, api_exceptions.GoogleAPICallError) as exc:
            _skip_if_emulator_gap(exc)
            raise

        cols = _emulator_column_names(config, project, dataset, session_table)
        if "owner_id" not in cols:
            pytest.skip("BigQuery emulator accepts ALTER TABLE ADD COLUMN without updating INFORMATION_SCHEMA.COLUMNS")
        adk_store = BigQueryADKStore(config)
        assert adk_store.get_session("app", "user", "s-keep") is not None

        with config.provide_session() as driver:
            tables = {str(r["table_name"]) for r in driver.data_dictionary.get_tables(driver, schema=dataset)}
            if ledger_table in tables:
                ledger_rows = driver.select(f"SELECT version_num FROM `{project}.{dataset}.{ledger_table}`")
                assert [r for r in ledger_rows if str(r["version_num"]).startswith("ext_adk_")] == []
    finally:
        with config.provide_session() as driver:
            for tbl in (
                metadata_table,
                user_state_table,
                app_state_table,
                adk_events_table,
                session_table,
                ledger_table,
            ):
                driver.execute_script(f"DROP TABLE IF EXISTS `{project}.{dataset}.{tbl}`")
