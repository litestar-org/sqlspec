"""BigQuery integration test fixtures."""

from collections.abc import Generator
from typing import TYPE_CHECKING, Any, cast

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials, Credentials

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.driver import BigQueryDriver
from tests.integration.adapters.bigquery._wedge import describe_wedge, is_emulator_wedge

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService

_emulator_wedge_reason: "str | None" = None


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(call: "pytest.CallInfo[None]") -> "Generator[None, Any, None]":
    """Record the first emulator wedge so remaining BigQuery tests skip fast."""
    yield
    global _emulator_wedge_reason
    if _emulator_wedge_reason is None and call.excinfo is not None and is_emulator_wedge(call.excinfo.value):
        _emulator_wedge_reason = describe_wedge(call.excinfo.value)


@pytest.fixture(autouse=True)
def _skip_after_emulator_wedge() -> None:
    """Skip once the emulator stopped responding instead of timing out per test."""
    if _emulator_wedge_reason is not None:
        pytest.skip(f"BigQuery emulator wedged earlier in this session ({_emulator_wedge_reason})")


def _is_bigquery_emulator(service: "BigQueryService") -> bool:
    """Return whether the pytest-databases service is a Docker-backed emulator."""
    return getattr(service, "container", None) is not None


@pytest.fixture(scope="session")
def native_bigquery_service(bigquery_service: "BigQueryService") -> "BigQueryService":
    """Require a native BigQuery service instead of the Docker emulator.

    Session-scoped so session-scoped table-setup fixtures can consume it.
    Skipping here propagates to every dependent test.
    """
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
    """Create a BigQuery config object.

    The emulator executes jobs synchronously inside the HTTP handler and can
    wedge, so requests get a finite timeout and retries are disabled: the
    client's built-in ``jobs.insert`` retry wrapper would otherwise re-POST a
    wedged endpoint for a fixed 600s deadline per statement.
    """
    driver_features: dict[str, Any] = (
        {"job_result_timeout": 30.0, "job_retry_deadline": 0.0, "request_timeout": 15.0}
        if _is_bigquery_emulator(bigquery_service)
        else {}
    )
    return BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": table_schema_prefix,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": _anonymous_credentials(),
        },
        driver_features=driver_features,
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
