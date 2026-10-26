"""IBM Db2 integration test fixtures."""

import os
import shlex
from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING, Any

import pytest
from pytest_databases.docker.db2 import (
    DB2_PORT,
    Db2Service,
    create_worker_database,
    exec_db2_sql,
    validate_db2_database_name,
)
from pytest_databases.helpers import exec_output_to_bytes, get_xdist_worker_num

from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2ConnectionParams, Db2SyncConfig

if TYPE_CHECKING:
    from pytest_databases import ContainerService
    from pytest_databases.types import ServiceContainer, XdistIsolationLevel

    from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver

__all__ = (
    "db2_async_config",
    "db2_async_session",
    "db2_connection_config",
    "db2_image",
    "db2_service",
    "db2_session",
    "db2_sync_config",
)


@pytest.fixture(scope="session")
def db2_image() -> str:
    """Return the pinned IBM Db2 Community Edition container image."""
    return os.environ.get("DB2_IMAGE", "icr.io/db2_community/db2:11.5.9.0")


@pytest.fixture(scope="session")
def db2_service(
    container_service: "ContainerService",
    db2_image: str,
    db2_dbname: str,
    db2_username: str,
    db2_password: str,
    db2_startup_timeout: int,
    xdist_db2_isolation_level: "XdistIsolationLevel",
) -> Generator[Db2Service, None, None]:
    """Start and yield a ready IBM Db2 service container with automatic 4Kn Direct-IO recovery."""
    worker_num = get_xdist_worker_num()
    name = "db2"
    initial_db = (
        validate_db2_database_name(os.getenv("DB2_DBNAME", "pytestdb"))
        if worker_num is not None and xdist_db2_isolation_level == "database"
        else db2_dbname
    )
    if worker_num is not None and xdist_db2_isolation_level == "server":
        name = f"db2_{worker_num + 1}"

    fallback_attempted = False

    def check(service: "ServiceContainer") -> bool:
        nonlocal fallback_attempted
        needs_4k_fallback = False
        output = ""
        try:
            output = exec_db2_sql(
                service.container, "SELECT 1 FROM SYSIBM.SYSDUMMY1", db_name=initial_db, instance_user=db2_username
            )
        except RuntimeError as exc:
            error_text = str(exc)
            if not fallback_attempted and ("SQL1013N" in error_text or "42705" in error_text):
                needs_4k_fallback = True
            else:
                return False

        if needs_4k_fallback:
            fallback_attempted = True
            _ensure_db2_4k_database(service.container, initial_db, instance_user=db2_username)
            return False

        return "1" in output

    with container_service.run(
        image=db2_image,
        container_port=DB2_PORT,
        name=name,
        entrypoint=["/bin/bash", "-c"],
        command=[
            (
                "sed -i 's/db2set DB2COMM=TCPIP/"
                "db2set DB2COMM=TCPIP \\&\\& db2set DB2_DIRECT_IO=NO \\&\\& db2set DB2_4K_DEVICE_SUPPORT=ON/' "
                "/var/db2_setup/include/db2_common_functions && "
                "exec /var/db2_setup/lib/setup_db2_instance.sh"
            )
        ],
        env={
            "LICENSE": "accept",
            "DB2INST1_PASSWORD": db2_password,
            "DBNAME": initial_db,
            "SAMPLEDB": "false",
            "AUTOCONFIG": "false",
            "ARCHIVE_LOGS": "false",
            "TO_CREATE_SAMPLEDB": "false",
            "REPODB": "false",
        },
        wait_for_log="Setup has completed",
        check=check,
        timeout=db2_startup_timeout,
        pause=2.0,
        transient=xdist_db2_isolation_level == "server",
        privileged=True,
        ipc_mode="host",
    ) as service:
        if db2_dbname != initial_db:
            create_worker_database(service.container, db2_dbname, instance_user=db2_username)
        yield Db2Service(
            container=service.container,
            host=service.host,
            port=service.port,
            db_name=db2_dbname,
            username=db2_username,
            password=db2_password,
        )


@pytest.fixture(scope="session")
def db2_connection_config(db2_service: Db2Service) -> Db2ConnectionParams:
    """Build SQLSpec Db2 connection parameters from the Db2 service container."""
    return {
        "database": db2_service.db_name,
        "hostname": db2_service.host,
        "port": db2_service.port,
        "protocol": "TCPIP",
        "user": db2_service.username,
        "password": db2_service.password,
    }


@pytest.fixture(scope="session")
def db2_sync_config(db2_connection_config: Db2ConnectionParams) -> Generator[Db2SyncConfig, None, None]:
    """Yield a session-scoped Db2SyncConfig and close its connection pool on teardown."""
    config = Db2SyncConfig(connection_config=dict(db2_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
async def db2_async_config(db2_connection_config: Db2ConnectionParams) -> AsyncGenerator[Db2AsyncConfig, None]:
    """Yield a session-scoped Db2AsyncConfig and close its connection pool on teardown."""
    config = Db2AsyncConfig(connection_config=dict(db2_connection_config))
    try:
        yield config
    finally:
        await config.close_pool()


@pytest.fixture
def db2_session(db2_sync_config: Db2SyncConfig) -> "Generator[Db2SyncDriver, None, None]":
    """Yield a function-scoped Db2 synchronous driver session."""
    with db2_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def db2_async_session(db2_async_config: Db2AsyncConfig) -> "AsyncGenerator[Db2AsyncDriver, None]":
    """Yield a function-scoped Db2 asynchronous driver session."""
    async with db2_async_config.provide_session() as driver:
        yield driver


def _ensure_db2_4k_database(container: Any, db_name: str, *, instance_user: str) -> None:
    """Configure 4Kn Direct-IO registry settings and create the initial Db2 database if missing."""
    validate_db2_database_name(db_name)
    quoted_db = shlex.quote(db_name)
    inner_script = (
        f"db2 list db directory | grep -iq 'Database name.*= {db_name}' || "
        f"(db2set DB2_DIRECT_IO=NO && db2set DB2_4K_DEVICE_SUPPORT=ON && "
        f"db2stop force && db2start && db2 create database {quoted_db})"
    )
    script = f"flock /tmp/sqlspec_db2_init.lock -c {shlex.quote(inner_script)}"
    result = container.exec_run(["su", "-", instance_user, "-c", script])
    if result.exit_code != 0:
        decoded = exec_output_to_bytes(result.output).decode("utf-8", errors="replace")
        msg = f"Failed to initialize 4Kn Db2 database {db_name!r} (exit {result.exit_code}): {decoded.strip()}"
        raise RuntimeError(msg)
