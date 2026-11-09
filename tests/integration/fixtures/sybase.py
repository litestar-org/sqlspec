"""SAP Sybase ASE integration test fixtures."""

import os
from collections.abc import Generator
from typing import TYPE_CHECKING, Any

import pytest
from pytest_databases.docker.sybase import SybaseService

from sqlspec.adapters.sybase import SybaseConfig

if TYPE_CHECKING:
    from sqlspec.adapters.sybase import SybaseDriver

__all__ = ("sybase_config", "sybase_connection_config", "sybase_session")


@pytest.fixture(scope="session")
def sybase_connection_config(
    sybase_service: "SybaseService", tmp_path_factory: pytest.TempPathFactory
) -> Generator[dict[str, Any], None, None]:
    """Return ``pymssql`` FreeTDS 5.0 connection parameters for the Sybase ASE service.

    Uses a dedicated ``FREETDSCONF`` section alias so FreeTDS negotiates TDS 5.0
    for Sybase ASE without mutating ``TDSVER`` for SQL Server tests in the same
    process.
    """
    section_name = "SQLSPEC_SYBASE_PYTEST"
    conf_dir = tmp_path_factory.mktemp("freetds")
    conf_path = conf_dir / "freetds.conf"
    conf_path.write_text(
        f"[{section_name}]\nhost = {sybase_service.host}\nport = {sybase_service.port}\ntds version = 5.0\n",
        encoding="utf-8",
    )
    previous_conf = os.environ.get("FREETDSCONF")
    os.environ["FREETDSCONF"] = str(conf_path)
    try:
        yield {
            "server_alias": section_name,
            "port": sybase_service.port,
            "user": sybase_service.user,
            "password": sybase_service.password,
            "database": sybase_service.database,
            "autocommit": True,
        }
    finally:
        if previous_conf is None:
            os.environ.pop("FREETDSCONF", None)
        else:
            os.environ["FREETDSCONF"] = previous_conf


@pytest.fixture
def sybase_config(sybase_connection_config: dict[str, Any]) -> Generator[SybaseConfig, None, None]:
    """Yield a Sybase ASE config whose pool is closed after the test."""
    config = SybaseConfig(connection_config=dict(sybase_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def sybase_session(sybase_config: SybaseConfig) -> Generator["SybaseDriver", None, None]:
    """Yield a Sybase ASE driver session."""
    with sybase_config.provide_session() as session:
        yield session
