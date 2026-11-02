"""IBM Db2 integration test fixtures."""

import json
import os
import platform
import shlex
import sys
import sysconfig
from collections.abc import AsyncGenerator, Generator
from importlib.metadata import version
from importlib.util import find_spec
from pathlib import Path
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

import sqlspec
from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2ConnectionParams, Db2SyncConfig

if TYPE_CHECKING:
    from pytest_databases import ContainerService
    from pytest_databases.types import ServiceContainer, XdistIsolationLevel

    from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver

__all__ = (
    "db2_arrow_odbc_runtime",
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
    request: pytest.FixtureRequest,
    container_service: "ContainerService",
    db2_image: str,
    db2_dbname: str,
    db2_username: str,
    db2_password: str,
    db2_startup_timeout: int,
    xdist_db2_isolation_level: "XdistIsolationLevel",
) -> Generator[Db2Service, None, None]:
    """Start and yield a ready IBM Db2 service container with automatic 4Kn Direct-IO recovery."""
    client_needed = (
        sys.platform == "linux"
        and platform.machine().lower() in {"x86_64", "amd64"}
        and any(
            isinstance(item, pytest.Function) and "db2_arrow_odbc_runtime" in item.fixturenames
            for item in request.session.items
        )
    )
    client_mounts = _db2_odbc_client_mounts() if client_needed else {}
    if client_needed and not client_mounts:
        msg = (
            "Arrow-ODBC Db2 acceptance requires an isolated or managed Linux x86-64 Python installation "
            "and an available glibc dynamic loader; broad system Python prefixes cannot be mounted safely."
        )
        raise RuntimeError(msg)
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
        volumes=client_mounts or None,
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
def db2_arrow_odbc_runtime(db2_service: Db2Service) -> tuple[list[str], dict[str, str], str]:
    """Run the host client against the container's installed 64-bit IBM ODBC runtime.

    Linux x86-64 clients use their own Python and system loader from read-only
    mounts. IBM's installation registry and instance profile remain inside the
    existing Db2 service container; no host driver registration is changed.
    """
    python_base = Path(sys.base_prefix).resolve()
    executable = Path(sys.executable).resolve()
    package_paths = tuple(
        dict.fromkeys(str(Path(sysconfig.get_path(kind)).resolve()) for kind in ("purelib", "platlib"))
    )
    library_path = ":".join((
        "/opt/sqlspec-host-libraries",
        str(python_base / "lib"),
        "/opt/ibm/db2/V11.5/lib64",
        "/opt/ibm/db2/V11.5/lib64/gskit",
    ))
    driver = "/opt/ibm/db2/V11.5/lib64/libdb2o.so.1"
    client_mounts = _db2_odbc_client_mounts()
    if not client_mounts:
        msg = "Arrow-ODBC Db2 acceptance requires a Linux x86-64 glibc host with its dynamic loader available."
        raise RuntimeError(msg)
    db2_service.container.reload()
    actual_mounts = {item["Destination"]: item for item in db2_service.container.attrs.get("Mounts", ())}
    missing_mounts = sorted(
        item["bind"]
        for item in client_mounts.values()
        if item["bind"] not in actual_mounts or actual_mounts[item["bind"]].get("RW", True)
    )
    if missing_mounts:
        msg = (
            "The existing Db2 service lacks required read-only client mounts; cached containers cannot "
            f"acquire new mounts in place. Missing or writable destinations: {missing_mounts!r}"
        )
        raise RuntimeError(msg)
    result = db2_service.container.exec_run(["test", "-r", str(executable)])
    if result.exit_code:
        msg = f"The mounted canonical Python executable is not readable inside Db2: {str(executable)!r}"
        raise RuntimeError(msg)
    result = db2_service.container.exec_run(["test", "-r", driver])
    if result.exit_code:
        msg = f"The pinned Db2 service image does not provide the required IBM 64-bit ODBC driver: {driver}"
        raise RuntimeError(msg)
    command = ["/opt/sqlspec-host-libraries/ld-linux-x86-64.so.2", "--library-path", library_path, str(executable)]
    environment = {
        "LD_LIBRARY_PATH": "/opt/ibm/db2/V11.5/lib64:/opt/ibm/db2/V11.5/lib64/gskit",
        "DB2INSTANCE": db2_service.username,
        "PYTHONHOME": str(python_base),
        "PYTHONPATH": ":".join(dict.fromkeys((str(Path(sqlspec.__file__).resolve().parent.parent), *package_paths))),
    }
    probe = (
        "import json,sys,sqlspec,pyarrow,arrow_odbc; from importlib.metadata import version; "
        "from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig; "
        "from pathlib import Path; "
        "print(json.dumps({'python':list(sys.version_info[:3]), 'sqlspec':str(Path(sqlspec.__file__).resolve()), "
        "'pyarrow':str(Path(pyarrow.__file__).resolve()), 'arrow_odbc':str(Path(arrow_odbc.__file__).resolve()), "
        "'versions':{name:version(name) for name in ('sqlspec','pyarrow','arrow-odbc')}}))"
    )
    result = db2_service.container.exec_run([*command, "-c", probe], environment=environment)
    output = exec_output_to_bytes(result.output).decode("utf-8", errors="replace")
    if result.exit_code:
        msg = f"The mounted Db2 client Python environment failed initialization (exit {result.exit_code}):\n{output}"
        raise RuntimeError(msg)
    expected_origins: dict[str, str] = {}
    for module_name in ("pyarrow", "arrow_odbc"):
        spec = find_spec(module_name)
        if spec is None or spec.origin is None:
            msg = f"The host environment lacks the required {module_name!r} package origin."
            raise RuntimeError(msg)
        expected_origins[module_name] = str(Path(spec.origin).resolve())
    expected = {
        "python": list(sys.version_info[:3]),
        "sqlspec": str(Path(sqlspec.__file__).resolve()),
        **expected_origins,
        "versions": {name: version(name) for name in ("sqlspec", "pyarrow", "arrow-odbc")},
    }
    if json.loads(output) != expected:
        msg = f"The mounted Db2 client imported a different Python/package environment: {output}"
        raise RuntimeError(msg)
    return command, environment, driver


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


def _db2_odbc_client_mounts() -> dict[str, dict[str, str]]:
    """Expose only the Python client, SQLSpec package, and host loader read-only."""
    if sys.platform != "linux" or platform.machine().lower() not in {"x86_64", "amd64"}:
        return {}
    python_base = Path(sys.base_prefix).resolve()
    executable_base = Path(sys.executable).resolve().parent.parent
    if any(
        prefix in {Path("/"), Path("/usr"), Path("/usr/local")}
        for prefix in (python_base, executable_base, Path(sys.prefix).resolve())
    ):
        return {}
    system_libraries = next(
        (path for path in (Path("/lib/x86_64-linux-gnu"), Path("/lib64")) if (path / "ld-linux-x86-64.so.2").is_file()),
        None,
    )
    if system_libraries is None:
        return {}
    paths = {
        python_base,
        executable_base,
        *(Path(sysconfig.get_path(kind)).resolve() for kind in ("purelib", "platlib")),
        Path(sqlspec.__file__).resolve().parent,
    }
    mounts = {str(path): {"bind": str(path), "mode": "ro"} for path in paths}
    mounts[str(system_libraries.resolve())] = {"bind": "/opt/sqlspec-host-libraries", "mode": "ro"}
    return mounts


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
