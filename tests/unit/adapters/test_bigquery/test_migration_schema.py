"""Unit coverage for BigQuery migration schema hooks."""

from typing import Any, cast

from google.cloud.bigquery import QueryJobConfig
from google.cloud.exceptions import NotFound

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.migrations.tracker import SyncMigrationTracker


class FakeBigQueryClient:
    project = "test-project"

    def __init__(self, datasets: set[str] | None = None) -> None:
        self.datasets = datasets or set()
        self.requested_datasets: list[str] = []

    def get_dataset(self, dataset_path: str) -> object:
        self.requested_datasets.append(dataset_path)
        if dataset_path not in self.datasets:
            raise NotFound("missing dataset")  # type: ignore[no-untyped-call]
        return object()


def _bigquery_driver(client: FakeBigQueryClient, default_config: QueryJobConfig | None = None) -> BigQueryDriver:
    config = BigQueryConfig(connection_config={"project": client.project}, connection_instance=client)
    features: dict[str, Any] = {}
    if default_config is not None:
        features["default_query_job_config"] = default_config
    return BigQueryDriver(
        client,  # type: ignore[arg-type]
        statement_config=config.statement_config,
        driver_features=features,
    )


def _compile_bigquery_tracker_create_table_sql() -> str:
    client = FakeBigQueryClient()
    driver = _bigquery_driver(client)
    tracker = SyncMigrationTracker("ddl_migrations", version_table_schema="tenant")
    statement = driver.prepare_statement(tracker._get_create_table_sql())  # pyright: ignore[reportPrivateUsage]
    sql, _ = driver._get_compiled_sql(statement, driver.statement_config)  # pyright: ignore[reportPrivateUsage]
    return sql


def test_bigquery_uses_shared_sync_migration_tracker() -> None:
    assert BigQueryConfig.migration_tracker_type is SyncMigrationTracker


def test_bigquery_migration_schema_sets_driver_default_dataset_without_mutating_base_config() -> None:
    client = FakeBigQueryClient()
    base_config = QueryJobConfig()
    base_config.use_query_cache = False
    driver = _bigquery_driver(client, base_config)

    driver.set_migration_session_schema("tenant")

    driver_config = cast("Any", driver)._default_query_job_config
    assert str(driver_config.default_dataset) == "test-project.tenant"
    assert driver_config.use_query_cache is False
    assert base_config.default_dataset is None
    assert BigQueryConfig.supports_migration_schemas is True


def test_bigquery_migration_schema_accepts_project_qualified_dataset() -> None:
    client = FakeBigQueryClient()
    driver = _bigquery_driver(client)

    driver.set_migration_session_schema("other-project.analytics")

    driver_config = cast("Any", driver)._default_query_job_config
    assert str(driver_config.default_dataset) == "other-project.analytics"


def test_bigquery_has_schema_uses_client_dataset_lookup() -> None:
    client = FakeBigQueryClient({"test-project.tenant"})
    driver = _bigquery_driver(client)

    assert driver.has_schema("tenant") is True
    assert driver.has_schema("missing") is False
    assert client.requested_datasets == ["test-project.tenant", "test-project.missing"]


def test_bigquery_shared_tracker_compiles_valid_create_table_ddl() -> None:
    create_sql = _compile_bigquery_tracker_create_table_sql()

    assert "CREATE TABLE IF NOT EXISTS `tenant`.`ddl_migrations`" in create_sql
    assert "version_num` STRING(32) PRIMARY KEY NOT ENFORCED" in create_sql
    assert "applied_at` DATETIME DEFAULT CURRENT_TIMESTAMP() NOT NULL" in create_sql
    assert "execution_sequence` INT64" in create_sql
