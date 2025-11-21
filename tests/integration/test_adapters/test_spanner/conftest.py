from collections.abc import Generator
from typing import Any, cast

import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud import spanner

from sqlspec import SQLSpec
from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver


def _start_spanner_service() -> "Any | None":
    try:
        from pytest_databases.docker.spanner import SpannerService  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover - optional dependency
        return None

    service = cast("Any", SpannerService)()  # type: ignore[call-arg]
    if hasattr(service, "start"):
        service.start()
    return service


@pytest.fixture(scope="session")
def spanner_service() -> Generator[Any, None, None]:
    service = _start_spanner_service()
    if service is None:
        pytest.skip("pytest-databases spanner service not available")
    try:
        yield service
    finally:
        if hasattr(service, "stop"):
            service.stop()


@pytest.fixture(scope="session")
def spanner_client(spanner_service: Any) -> Generator[spanner.Client, None, None]:
    host = getattr(spanner_service, "host", "localhost")
    port = getattr(spanner_service, "port", 9010)
    project_id = getattr(spanner_service, "project", "test-project")
    endpoint = f"{host}:{port}"

    client = spanner.Client(
        project=project_id,
        credentials=cast(Any, AnonymousCredentials()),  # type: ignore[no-untyped-call]
        client_options={"api_endpoint": endpoint},
    )

    instance_id = getattr(spanner_service, "instance_id", getattr(spanner_service, "instance", "test-instance"))
    database_id = getattr(spanner_service, "database_id", getattr(spanner_service, "database", "test-database"))

    instance = client.instance(instance_id)
    if not instance.exists():
        config_name = f"{client.project_name}/instanceConfigs/emulator-config"
        instance = client.instance(instance_id, configuration_name=config_name)
        instance.create().result(300)

    database = instance.database(database_id)
    if not database.exists():
        database.create().result(300)

    yield client


@pytest.fixture
def spanner_config(spanner_service: Any, spanner_client: spanner.Client) -> SpannerSyncConfig:
    host = getattr(spanner_service, "host", "localhost")
    port = getattr(spanner_service, "port", 9010)
    project_id = getattr(spanner_service, "project", "test-project")
    instance_id = getattr(spanner_service, "instance_id", getattr(spanner_service, "instance", "test-instance"))
    database_id = getattr(spanner_service, "database_id", getattr(spanner_service, "database", "test-database"))
    api_endpoint = f"{host}:{port}"

    return SpannerSyncConfig(
        pool_config={
            "project": project_id,
            "instance_id": instance_id,
            "database_id": database_id,
            "credentials": cast(Any, AnonymousCredentials()),  # type: ignore[no-untyped-call]
            "client_options": {"api_endpoint": api_endpoint},
            "min_sessions": 1,
            "max_sessions": 5,
        }
    )


@pytest.fixture
def spanner_session(spanner_config: SpannerSyncConfig) -> Generator[SpannerSyncDriver, None, None]:
    sql = SQLSpec()
    sql.add_config(spanner_config)
    with sql.provide_session(spanner_config) as session:
        yield session
