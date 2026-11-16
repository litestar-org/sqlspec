"""IBM Db2 integration test fixtures."""

import json
import os
import platform
import sys
import sysconfig
from collections.abc import AsyncGenerator, Generator
from importlib.metadata import version
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pytest_databases.docker.db2 import Db2Service
from pytest_databases.helpers import exec_output_to_bytes

import sqlspec
from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2ConnectionParams, Db2SyncConfig

if TYPE_CHECKING:
    from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver

__all__ = (
    "db2_arrow_odbc_runtime",
    "db2_async_config",
    "db2_async_session",
    "db2_connection_config",
    "db2_image",
    "db2_session",
    "db2_sync_config",
    "db2_volumes",
)


@pytest.fixture(scope="session")
def db2_image() -> str:
    """Return the pinned IBM Db2 Community Edition container image."""
    return os.environ.get("DB2_IMAGE", "icr.io/db2_community/db2:11.5.9.0")


@pytest.fixture(scope="session")
def db2_volumes() -> dict[str, dict[str, str]] | None:
    """Keep read-only client mounts stable when the same Db2 service spans test shards."""
    return _db2_odbc_client_mounts() or None


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
