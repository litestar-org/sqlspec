"""Shared BigQuery integration fixtures."""

from collections.abc import Generator
from typing import Any, cast

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials, Credentials
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver

__all__ = ("bigquery_config", "bigquery_session", "native_bigquery_service", "table_schema_prefix")


def _is_bigquery_emulator(service: "BigQueryService") -> bool:
    return getattr(service, "container", None) is not None


def _anonymous_credentials() -> "Credentials":
    factory = cast("Any", AnonymousCredentials)
    return cast("Credentials", factory())


def _bigquery_connection_config(service: "BigQueryService", dataset_id: str | None = None) -> "dict[str, Any]":
    return {
        "project": service.project,
        "dataset_id": dataset_id or f"`{service.project}`.`{service.dataset}`",
        "client_options": ClientOptions(api_endpoint=f"http://{service.host}:{service.port}"),
        "credentials": _anonymous_credentials(),
    }


@pytest.fixture(scope="session")
def native_bigquery_service(bigquery_service: "BigQueryService") -> "BigQueryService":
    """Require a native BigQuery service instead of the Docker emulator."""
    if _is_bigquery_emulator(bigquery_service):
        pytest.skip("BigQuery emulator does not support native BigQuery-only coverage")
    return bigquery_service


@pytest.fixture(scope="session")
def table_schema_prefix(bigquery_service: "BigQueryService") -> str:
    """Create a table schema prefix."""
    return f"`{bigquery_service.project}`.`{bigquery_service.dataset}`"


@pytest.fixture(scope="session")
def bigquery_config(bigquery_service: "BigQueryService", table_schema_prefix: str) -> "BigQueryConfig":
    """Create a BigQuery configuration with finite emulator timeouts."""
    driver_features: dict[str, Any] = (
        {"job_result_timeout": 30.0, "job_retry_deadline": 0.0, "request_timeout": 15.0}
        if _is_bigquery_emulator(bigquery_service)
        else {}
    )
    return BigQueryConfig(
        connection_config=_bigquery_connection_config(bigquery_service, table_schema_prefix),
        driver_features=driver_features,
    )


@pytest.fixture(scope="session")
def bigquery_session(bigquery_config: "BigQueryConfig") -> "Generator[BigQueryDriver, Any, None]":
    """Create a session-scoped BigQuery driver."""
    with bigquery_config.provide_session() as session:
        yield session
