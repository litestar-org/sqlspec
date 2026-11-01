"""SAP SQL Anywhere integration test fixtures."""

import ctypes
import io
import os
import tarfile
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.adapters.sqlanywhere import SqlanywhereConfig

if TYPE_CHECKING:
    from pytest_databases.docker.sqlanywhere import SQLAnywhereService

    from sqlspec.adapters.sqlanywhere import SqlanywhereDriver

__all__ = (
    "sqlanywhere_client_runtime",
    "sqlanywhere_config",
    "sqlanywhere_connection_config",
    "sqlanywhere_session",
    "xdist_sqlanywhere_isolation_level",
)

_CLIENT_LIBRARY_DEPENDENCIES = ("libdbtasks16_r.so", "libdblib16_r.so")
_CLIENT_DIRECTORIES = ("lib64", "res")


@pytest.fixture(scope="session")
def xdist_sqlanywhere_isolation_level() -> str:
    """Run one SQL Anywhere server per worker because the demo image cannot create databases on demand."""
    return "server"


@pytest.fixture(scope="session")
def sqlanywhere_client_runtime(
    sqlanywhere_service: "SQLAnywhereService", tmp_path_factory: pytest.TempPathFactory
) -> Generator[Path, None, None]:
    """Provide the SQL Anywhere ``dbcapi`` client runtime to ``sqlanydb``.

    ``sqlanydb`` is a ctypes wrapper over SAP's native ``dbcapi`` library, which is not on
    PyPI. When ``SQLANY_API_DLL`` is not already configured, the runtime is copied out of the
    server container, its dependent libraries are preloaded, and ``SQLANY16`` /
    ``SQLANY_API_DLL`` are set before the first connection initializes ``sqlanydb``.
    """
    configured = os.environ.get("SQLANY_API_DLL")
    if configured:
        yield Path(configured).parent
        return
    root = tmp_path_factory.mktemp("sqlanywhere-client")
    install_root = os.environ.get("SQLANYWHERE_CLIENT_ROOT", "/opt/sqlanywhere16")
    for directory in _CLIENT_DIRECTORIES:
        chunks, _ = sqlanywhere_service.container.get_archive(f"{install_root}/{directory}")
        with tarfile.open(fileobj=io.BytesIO(b"".join(chunks))) as archive:
            archive.extractall(root, filter="data")
    library_dir = root / "lib64"
    for name in _CLIENT_LIBRARY_DEPENDENCIES:
        ctypes.CDLL(str(library_dir / name), mode=ctypes.RTLD_GLOBAL)
    previous = {key: os.environ.get(key) for key in ("SQLANY16", "SQLANY_API_DLL")}
    os.environ["SQLANY16"] = str(root)
    os.environ["SQLANY_API_DLL"] = str(library_dir / "libdbcapi_r.so")
    try:
        yield library_dir
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@pytest.fixture(scope="session")
def sqlanywhere_connection_config(
    sqlanywhere_service: "SQLAnywhereService", sqlanywhere_client_runtime: Path
) -> dict[str, Any]:
    """Return ``sqlanydb`` connection parameters for the SQL Anywhere service."""
    return {
        "uid": sqlanywhere_service.user,
        "pwd": sqlanywhere_service.password,
        "dbn": sqlanywhere_service.database,
        "host": f"{sqlanywhere_service.host}:{sqlanywhere_service.port}",
    }


@pytest.fixture
def sqlanywhere_config(sqlanywhere_connection_config: dict[str, Any]) -> Generator[SqlanywhereConfig, None, None]:
    """Yield a SQL Anywhere config whose pool is closed after the test."""
    config = SqlanywhereConfig(connection_config=dict(sqlanywhere_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def sqlanywhere_session(sqlanywhere_config: SqlanywhereConfig) -> Generator["SqlanywhereDriver", None, None]:
    """Yield a SQL Anywhere driver session."""
    with sqlanywhere_config.provide_session() as session:
        yield session
