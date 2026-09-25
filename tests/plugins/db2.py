"""Pytest Docker service extension for IBM Db2."""

import contextlib
import socket
import tempfile
import time
from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import filelock
import ibm_db_dbi
import pytest
from docker.errors import NotFound
from pytest_databases._service import get_docker_client
from pytest_databases.helpers import get_xdist_worker_num
from pytest_databases.types import ServiceContainer

from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig

if TYPE_CHECKING:
    from docker import DockerClient
    from docker.models.containers import Container
    from pytest_databases._service import DockerService

    from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver

DB2_CONTAINER_PORT = 50000
DB2_READY_TIMEOUT = 600
DB2_READY_INTERVAL = 5.0


def db2_responsive(host: str, port: int, database: str, user: str, password: str) -> bool:
    """Report whether Db2 accepts a connection and answers a query.

    Args:
        host: Database host name.
        port: Database TCP port.
        database: Database name.
        user: User name.
        password: User password.

    Returns:
        ``True`` once ``SELECT 1 FROM SYSIBM.SYSDUMMY1`` returns 1; ``False`` while the port is
        closed or Db2 refuses the connection or query.
    """
    try:
        with socket.create_connection((host, port), timeout=1.0):
            pass
    except OSError:
        return False

    dsn = f"DATABASE={database};HOSTNAME={host};PORT={port};PROTOCOL=TCPIP;UID={user};PWD={password};"
    try:
        connection = ibm_db_dbi.connect(dsn, "", "", "", "", None)
    except ibm_db_dbi.Error:
        return False
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            row = cursor.fetchone()
        finally:
            cursor.close()
        return row is not None and row[0] == 1
    except ibm_db_dbi.Error:
        return False
    finally:
        connection.close()


@dataclass
class Db2Service(ServiceContainer):
    """Container representation for IBM Db2 Docker service."""

    user: str
    password: str
    database: str


def _find_running_container(client: "DockerClient", name: str) -> "Container | None":
    """Return the running container with exactly this name, if any."""
    for container in client.containers.list(filters={"name": name}):
        if container.name == name:
            return container
    return None


@contextlib.contextmanager
def _run_privileged_container(
    image: str,
    name: str,
    env: "dict[str, str]",
    container_port: int = DB2_CONTAINER_PORT,
    timeout: int = DB2_READY_TIMEOUT,
) -> "Generator[ServiceContainer, None, None]":
    """Run a privileged container, reusing a running container with the same name.

    Container creation is serialized across processes with a file lock. The container is stopped
    when the context exits.

    Args:
        image: Image reference.
        name: Container name suffix; the container is named ``pytest_databases_<name>``.
        env: Container environment.
        container_port: Container port to publish on a random host port.
        timeout: Seconds to wait for Docker to report the port mapping.

    Yields:
        The container with its published host and port.

    Raises:
        RuntimeError: If Docker never reports the published port.
    """
    container_name = f"pytest_databases_{name}"
    try:
        client = get_docker_client()
    except Exception as exc:
        pytest.skip(f"Docker client is unavailable: {exc}")
    try:
        with filelock.FileLock(Path(tempfile.gettempdir()) / f"{container_name}.lock"):
            try:
                container = _find_running_container(client, container_name)
            except Exception as exc:
                pytest.skip(f"Docker container check failed: {exc}")
            if container is None:
                try:
                    container = client.containers.run(
                        image,
                        detach=True,
                        remove=True,
                        privileged=True,
                        ports={f"{container_port}/tcp": None},
                        environment=env,
                        labels=["pytest_databases"],
                        name=container_name,
                    )
                except Exception as exc:
                    pytest.skip(f"Failed to run Db2 container: {exc}")
        binding_key = f"{container_port}/tcp"
        deadline = time.monotonic() + timeout
        while True:
            try:
                container.reload()
            except Exception as exc:
                pytest.skip(f"Db2 container reload failed: {exc}")
            bindings = container.ports.get(binding_key)
            if bindings:
                break
            if time.monotonic() >= deadline:
                pytest.skip(f"Service {container_name!r} never published port {binding_key}")
            time.sleep(0.5)
        try:
            yield ServiceContainer(container=container, host="127.0.0.1", port=int(bindings[0]["HostPort"]))
        finally:
            with contextlib.suppress(NotFound):
                container.stop()
    finally:
        client.close()


def _wait_for_db2(service: Db2Service, name: str, timeout: int = DB2_READY_TIMEOUT) -> None:
    """Block until Db2 answers a query.

    Args:
        service: Service to probe.
        name: Service name used in error messages.
        timeout: Seconds to wait.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if db2_responsive(service.host, service.port, service.database, service.user, service.password):
            return
        try:
            service.container.reload()
        except NotFound:
            pytest.skip(f"Service {name!r} failed to come online: the container exited")
        except Exception as exc:
            pytest.skip(f"Service {name!r} failed to reload: {exc}")
        if service.container.status != "running":
            pytest.skip(f"Service {name!r} failed to come online: the container is {service.container.status}")
        time.sleep(DB2_READY_INTERVAL)
    pytest.skip(f"Service {name!r} failed to come online within {timeout} seconds")


@contextlib.contextmanager
def _provide_db2_service(
    image: str, name: str, database: str, user: str, password: str
) -> "Generator[Db2Service, None, None]":
    """Run the Db2 Community container and wait until the database answers a query.

    Args:
        image: Db2 Community image reference.
        name: Container name suffix; xdist workers append their worker number.
        database: Database created on first boot.
        user: Instance owner.
        password: Instance owner password.

    Yields:
        The ready Db2 service.
    """
    worker_num = get_xdist_worker_num()
    if worker_num is not None:
        name = f"{name}_{worker_num}"

    env = {
        "LICENSE": "accept",
        "DB2INSTANCE": user,
        "DB2INST1_PASSWORD": password,
        "DBNAME": database,
        "BLU": "false",
        "ENABLE_ORACLE_COMPATIBILITY": "false",
        "AUTOCONFIG": "false",
        "ARCHIVE_LOGS": "false",
    }
    with _run_privileged_container(image=image, name=name, env=env) as container:
        service = Db2Service(
            container=container.container,
            host=container.host,
            port=container.port,
            user=user,
            password=password,
            database=database,
        )
        _wait_for_db2(service, name)
        yield service


@pytest.fixture(autouse=False, scope="session")
def db2_image() -> str:
    """Docker image for IBM Db2 Community Edition."""
    return "icr.io/db2_community/db2:11.5.9.0"


@pytest.fixture(autouse=False, scope="session")
def db2_database() -> str:
    """Default database name for Db2 container."""
    return "testdb"


@pytest.fixture(autouse=False, scope="session")
def db2_user() -> str:
    """Default instance owner and user for Db2 container."""
    return "db2inst1"


@pytest.fixture(autouse=False, scope="session")
def db2_password() -> str:
    """Default password for Db2 container user."""
    return "password"


@pytest.fixture(autouse=False, scope="session")
def db2_service(
    docker_service: "DockerService", db2_image: str, db2_database: str, db2_user: str, db2_password: str
) -> "Generator[Db2Service, None, None]":
    """Session-scoped Db2 service running in a privileged local container.

    Depends on ``docker_service`` so pytest-databases initializes Docker before the Db2 container
    starts.
    """
    try:
        with _provide_db2_service(
            image=db2_image, name="db2_test", database=db2_database, user=db2_user, password=db2_password
        ) as service:
            yield service
    except Exception as exc:
        pytest.skip(f"Db2 container service unavailable: {exc}")


@pytest.fixture(autouse=False, scope="session")
def db2_connection_config(db2_service: Db2Service) -> "dict[str, Any]":
    """Connection parameters for the Db2 service."""
    return {
        "database": db2_service.database,
        "hostname": db2_service.host,
        "port": db2_service.port,
        "user": db2_service.user,
        "password": db2_service.password,
    }


@pytest.fixture(autouse=False, scope="session")
def db2_sync_config(db2_connection_config: "dict[str, Any]") -> "Generator[Db2SyncConfig, None, None]":
    """Session-scoped Db2SyncConfig for the Db2 service."""
    config = Db2SyncConfig(connection_config=db2_connection_config)
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(autouse=False, scope="session")
async def db2_async_config(db2_connection_config: "dict[str, Any]") -> "AsyncGenerator[Db2AsyncConfig, None]":
    """Session-scoped Db2AsyncConfig for the Db2 service."""
    config = Db2AsyncConfig(connection_config=db2_connection_config)
    try:
        yield config
    finally:
        await config.close_pool()


@pytest.fixture(autouse=False, scope="function")
def db2_session(db2_sync_config: Db2SyncConfig) -> "Generator[Db2SyncDriver, None, None]":
    """Function-scoped Db2 sync driver session."""
    with db2_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture(autouse=False, scope="function")
async def db2_async_session(db2_async_config: Db2AsyncConfig) -> "AsyncGenerator[Db2AsyncDriver, None]":
    """Function-scoped Db2 async driver session."""
    async with db2_async_config.provide_session() as driver:
        yield driver
