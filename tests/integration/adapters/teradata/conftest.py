"""Environment-gated Teradata Vantage fixtures."""

import os
from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest

from sqlspec.adapters.teradata import TeradataConfig, TeradataDriver
from sqlspec.exceptions import NotFoundError

if TYPE_CHECKING:
    from pytest_databases import DockerService
    from pytest_databases.types import ServiceContainer


@pytest.fixture(scope="session")
def teradata_service_environment() -> dict[str, str]:
    """Return image-specific environment overrides for a custom Teradata image."""
    return {}


@pytest.fixture(scope="session")
def teradata_service(
    request: pytest.FixtureRequest, teradata_service_environment: dict[str, str]
) -> Generator[dict[str, Any], None, None]:
    """Run an explicitly configured Teradata image using pytest-databases.

    A licensed, runnable image must be supplied via TERADATA_TEST_IMAGE.
    The package owns dynamic port allocation, readiness and teardown.
    Override teradata_service_environment for the selected image's startup contract.
    """
    image = os.getenv("TERADATA_TEST_IMAGE", "").strip()
    if not image:
        pytest.skip("Set TERADATA_HOST or provide a runnable TERADATA_TEST_IMAGE")
    module = pytest.importorskip("teradatasql")
    docker_service = cast("DockerService", request.getfixturevalue("docker_service"))
    user = os.getenv("TERADATA_USER", "dbc")
    password = os.getenv("TERADATA_PASSWORD", "dbc")

    def ready(service: "ServiceContainer") -> bool:
        try:
            connection = module.connect(host=service.host, dbs_port=str(service.port), user=user, password=password)
            try:
                cursor = connection.cursor()
                try:
                    cursor.execute("SELECT 1")
                    row = cursor.fetchone()
                    return row is not None and row[0] == 1
                finally:
                    cursor.close()
            finally:
                connection.close()
        except (module.Error, OSError):
            return False

    worker = os.getenv("PYTEST_XDIST_WORKER", "main")
    with docker_service.run(
        image=image,
        container_port=1025,
        name=f"teradata_{worker}",
        env=teradata_service_environment,
        check=ready,
        timeout=int(os.getenv("TERADATA_STARTUP_TIMEOUT", "300")),
        pause=2,
        transient=True,
    ) as service:
        yield {"host": service.host, "dbs_port": str(service.port), "user": user, "password": password}


@pytest.fixture
def teradata_connection_params(request: pytest.FixtureRequest) -> dict[str, Any]:
    """Use an external endpoint or the opt-in custom container service."""
    host = os.getenv("TERADATA_HOST", "").strip()
    if host:
        pytest.importorskip("teradatasql")
        params = {
            "host": host,
            "dbs_port": os.getenv("TERADATA_PORT", "1025"),
            "user": os.getenv("TERADATA_USER", "dbc"),
            "password": os.getenv("TERADATA_PASSWORD", "dbc"),
        }
    else:
        params = dict(request.getfixturevalue("teradata_service"))
    for key in ("database", "logmech", "logdata", "sslmode", "encryptdata"):
        value = os.getenv(f"TERADATA_{key.upper()}")
        if value:
            params[key] = value
    return params


@pytest.fixture
def teradata_config(teradata_connection_params: dict[str, Any]) -> Generator[TeradataConfig, None, None]:
    config = TeradataConfig(connection_config=teradata_connection_params)
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def teradata_ansi_config(teradata_connection_params: dict[str, Any]) -> Generator[TeradataConfig, None, None]:
    config = TeradataConfig(connection_config={**teradata_connection_params, "tmode": "ANSI"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def teradata_session(teradata_config: TeradataConfig) -> Generator[TeradataDriver, None, None]:
    with teradata_config.provide_session() as session:
        yield session


@pytest.fixture
def teradata_table_factory(teradata_config: TeradataConfig) -> Generator[Callable[[str], str], None, None]:
    tables: list[str] = []

    def make_table(tag: str) -> str:
        name = f"sqlspec_td_{tag}_{uuid4().hex[:8]}"
        tables.append(name)
        return name

    yield make_table
    for name in reversed(tables):
        with teradata_config.provide_session() as session:
            try:
                session.execute_script(f"DROP TABLE {name}")
                session.commit()
            except NotFoundError:
                session.rollback()
