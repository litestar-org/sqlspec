"""Pytest Docker service extension for IBM Db2."""

import contextlib
from collections.abc import Generator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import pytest
from pytest_databases._service import DockerService
from pytest_databases.helpers import get_xdist_worker_num
from pytest_databases.types import ServiceContainer

from sqlspec.adapters.db2.config import Db2Config

if TYPE_CHECKING:
    from sqlspec.adapters.db2.driver import Db2Driver


def db2_responsive(host: str, port: int, database: str, user: str, password: str) -> bool:
    """Check if IBM Db2 database service is ready to accept connections.

    Tries a TCP socket check first. If open, attempts an actual query if
    ibm_db_dbi is available.
    """
    import socket

    try:
        with socket.create_connection((host, port), timeout=1.0):
            pass
    except OSError:
        return False

    try:
        import ibm_db_dbi
    except (ImportError, AttributeError):
        return True

    if ibm_db_dbi is None:
        return True

    try:
        dsn = f"DATABASE={database};HOSTNAME={host};PORT={port};PROTOCOL=TCPIP;UID={user};PWD={password};"
        conn = ibm_db_dbi.connect(dsn, "", "")
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row is not None and row[0] == 1
    except Exception:
        return False


@dataclass
class Db2Service(ServiceContainer):
    """Container representation for IBM Db2 Docker service."""

    user: str
    password: str
    database: str


@contextlib.contextmanager
def _provide_db2_service(
    docker_service: DockerService, image: str, name: str, database: str, user: str, password: str
) -> Generator[Db2Service, None, None]:
    """Launch IBM Db2 Docker container service with xdist isolation."""

    def check(_service: ServiceContainer) -> bool:
        return db2_responsive(host=_service.host, port=_service.port, database=database, user=user, password=password)

    worker_num = get_xdist_worker_num()
    if worker_num is not None:
        name = f"{name}_{worker_num}"

    with docker_service.run(
        image=image,
        name=name,
        check=check,
        container_port=50000,
        timeout=180,
        env={
            "DB2INSTANCE": user,
            "DB2INST1_PASSWORD": password,
            "DBNAME": database,
            "BLU": "false",
            "ENABLE_ORACLE_COMPATIBILITY": "false",
            "AUTOCONFIG": "false",
        },
    ) as service:
        yield Db2Service(
            host=service.host,
            port=service.port,
            container=service.container,
            user=user,
            password=password,
            database=database,
        )


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
    docker_service: DockerService, db2_image: str, db2_database: str, db2_user: str, db2_password: str
) -> Generator[Db2Service, None, None]:
    """Session-scoped IBM Db2 container service fixture."""
    import os

    if host := os.environ.get("DB2_HOST"):
        port = int(os.environ.get("DB2_PORT", "50000"))
        database = os.environ.get("DB2_DATABASE", db2_database)
        user = os.environ.get("DB2_USER", db2_user)
        password = os.environ.get("DB2_PASSWORD", db2_password)
        yield Db2Service(
            container=cast("Any", None), host=host, port=port, user=user, password=password, database=database
        )
        return

    with _provide_db2_service(
        docker_service=docker_service,
        image=db2_image,
        name="db2_test",
        database=db2_database,
        user=db2_user,
        password=db2_password,
    ) as service:
        yield service


@pytest.fixture(autouse=False, scope="session")
def db2_connection_config(db2_service: Db2Service) -> dict[str, Any]:
    """Connection parameters mapping derived from Db2Service container."""
    return {
        "database": db2_service.database,
        "hostname": db2_service.host,
        "port": db2_service.port,
        "username": db2_service.user,
        "password": db2_service.password,
    }


@pytest.fixture(autouse=False, scope="session")
def db2_sync_config(db2_connection_config: dict[str, Any]) -> Generator[Db2Config, None, None]:
    """Session-scoped Db2Config initialized with container connection parameters."""
    config = Db2Config(connection_config=db2_connection_config)
    yield config
    config.close_pool()


@pytest.fixture(autouse=False, scope="function")
def db2_session(db2_sync_config: Db2Config) -> "Generator[Db2Driver, None, None]":
    """Function-scoped Db2Driver session providing driver access."""
    with db2_sync_config.provide_session() as driver:
        yield driver
