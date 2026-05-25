"""Unit coverage for BigQuery migration schema hooks."""

from typing import Any, cast

from google.cloud.bigquery import QueryJobConfig
from google.cloud.exceptions import NotFound

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.adapters.bigquery.migrations import BigQueryMigrationTracker


class FakeBigQueryClient:
    project = "test-project"

    def __init__(self, datasets: set[str] | None = None) -> None:
        self.datasets = datasets or set()
        self.requested_datasets: list[str] = []

    def get_dataset(self, dataset_path: str) -> object:
        self.requested_datasets.append(dataset_path)
        if dataset_path not in self.datasets:
            raise NotFound("missing dataset")
        return object()


class FakeResult:
    data = []

    def get_data(self) -> list[dict[str, Any]]:
        return []


class FakeMigrationDriver:
    driver_features = {"autocommit": True}

    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any | None = None) -> FakeResult:
        self.executed.append((sql, parameters))
        return FakeResult()


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


def test_bigquery_tracker_uses_backtick_qualified_table_path() -> None:
    tracker = BigQueryMigrationTracker("ddl_migrations", version_table_schema="test-project.tenant")
    driver = FakeMigrationDriver()

    tracker.ensure_tracking_table(driver)  # type: ignore[arg-type]

    assert tracker.version_table == "`test-project.tenant.ddl_migrations`"
    assert driver.executed
    create_sql = driver.executed[0][0]
    assert "CREATE TABLE IF NOT EXISTS `test-project.tenant.ddl_migrations`" in create_sql
    assert "version_num STRING NOT NULL" in create_sql
    assert "execution_sequence INT64" in create_sql
