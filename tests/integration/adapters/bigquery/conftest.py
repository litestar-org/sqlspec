"""BigQuery integration test fixtures."""

from collections.abc import Generator
from typing import TYPE_CHECKING, Any, cast

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials, Credentials

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.driver import BigQueryDriver

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService


def _is_bigquery_emulator(service: "BigQueryService") -> bool:
    """Return whether the pytest-databases service is a Docker-backed emulator."""
    return getattr(service, "container", None) is not None


@pytest.fixture
def native_bigquery_service(bigquery_service: "BigQueryService") -> "BigQueryService":
    """Require a native BigQuery service instead of the Docker emulator."""
    if _is_bigquery_emulator(bigquery_service):
        pytest.skip("BigQuery emulator does not support native BigQuery-only coverage")
    return bigquery_service


@pytest.fixture(scope="session")
def table_schema_prefix(bigquery_service: "BigQueryService") -> str:
    """Create a table schema prefix."""
    return f"`{bigquery_service.project}`.`{bigquery_service.dataset}`"


def _anonymous_credentials() -> "Credentials":
    """Create anonymous credentials for the emulator."""
    factory = cast("Any", AnonymousCredentials)
    return cast("Credentials", factory())


@pytest.fixture(scope="session")
def bigquery_config(bigquery_service: "BigQueryService", table_schema_prefix: str) -> "BigQueryConfig":
    """Create a BigQuery config object."""
    return BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": table_schema_prefix,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": _anonymous_credentials(),
        }
    )


@pytest.fixture(scope="session")
def bigquery_session(bigquery_config: "BigQueryConfig") -> "Generator[BigQueryDriver, Any, None]":
    """Create a session-scoped BigQuery sync session.

    Shared across all BigQuery tests in the xdist_group so the underlying
    HTTP client/transport is reused; emulator container teardown handles
    final cleanup so we avoid DDL on shutdown (which can hang against the
    goccy/bigquery-emulator).
    """

    with bigquery_config.provide_session() as session:
        yield session
