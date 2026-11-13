"""Shared Oracle integration fixtures."""

import zipfile
from typing import TYPE_CHECKING, cast

import pytest
from pytest_databases.docker.oracle import OracleADBService, OracleService

from sqlspec.utils.module_loader import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator
    from pathlib import Path

    from sqlspec.adapters.oracledb import (
        OracleAsyncConfig,
        OracleAsyncDriver,
        OraclePoolParams,
        OracleSyncConfig,
        OracleSyncDriver,
    )
else:
    OracleAsyncConfig = import_optional_attr("sqlspec.adapters.oracledb", "OracleAsyncConfig")
    OracleAsyncDriver = import_optional_attr("sqlspec.adapters.oracledb", "OracleAsyncDriver")
    OraclePoolParams = import_optional_attr("sqlspec.adapters.oracledb", "OraclePoolParams")
    OracleSyncConfig = import_optional_attr("sqlspec.adapters.oracledb", "OracleSyncConfig")
    OracleSyncDriver = import_optional_attr("sqlspec.adapters.oracledb", "OracleSyncDriver")

__all__ = (
    "oracle_18c_async_config",
    "oracle_18c_async_session",
    "oracle_18c_connection_config",
    "oracle_adb_async_config",
    "oracle_adb_p12_only_wallet_zip",
    "oracle_adb_sync_config",
    "oracle_aq_privileges",
    "oracle_async_config",
    "oracle_async_session",
    "oracle_connection_config",
    "oracle_sync_config",
    "oracle_sync_session",
)


@pytest.fixture(scope="session")
def oracle_18c_connection_config(oracle_18c_service: OracleService) -> "OraclePoolParams":
    """Provide Oracle 18c pool parameters for non-native JSON storage coverage."""
    return _oracle_pool_params(oracle_18c_service)


@pytest.fixture(scope="session")
async def oracle_18c_async_config(
    oracle_18c_connection_config: "OraclePoolParams",
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Provide a session-scoped Oracle 18c async configuration."""
    pytest.importorskip("oracledb")
    config = OracleAsyncConfig(connection_config=OraclePoolParams(**oracle_18c_connection_config))
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()
            config.connection_instance = None


@pytest.fixture
async def oracle_18c_async_session(
    oracle_18c_async_config: "OracleAsyncConfig",
) -> "AsyncGenerator[OracleAsyncDriver, None]":
    """Create an Oracle 18c async driver session."""
    pytest.importorskip("oracledb")
    async with oracle_18c_async_config.provide_session() as driver:
        yield driver


@pytest.fixture(scope="session")
def oracle_connection_config(oracle_26ai_service: OracleService) -> "OraclePoolParams":
    """Provide shared Oracle pool parameters."""
    return _oracle_pool_params(oracle_26ai_service)


@pytest.fixture(scope="session")
def oracle_aq_privileges(oracle_26ai_service: OracleService) -> None:
    """Grant the application user privileges required by Oracle AQ tests."""
    app_user = oracle_26ai_service.user
    exit_code, output = oracle_26ai_service.exec_sql(
        f"GRANT aq_administrator_role, aq_user_role TO {app_user};\n"
        f"GRANT EXECUTE ON dbms_aq TO {app_user};",
        user="system",
    )
    if exit_code != 0:
        msg = f"Failed to grant Oracle AQ privileges to {app_user!r}: {output.decode('utf-8', errors='replace')}"
        raise RuntimeError(msg)


@pytest.fixture(scope="session")
def oracle_sync_config(oracle_connection_config: "OraclePoolParams") -> "Generator[OracleSyncConfig, None, None]":
    """Provide a session-scoped Oracle sync configuration."""
    pytest.importorskip("oracledb")
    config = OracleSyncConfig(connection_config=OraclePoolParams(**oracle_connection_config))
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()


@pytest.fixture(scope="session")
async def oracle_async_config(
    oracle_connection_config: "OraclePoolParams",
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Provide a session-scoped Oracle async configuration."""
    pytest.importorskip("oracledb")
    config = OracleAsyncConfig(connection_config=OraclePoolParams(**oracle_connection_config))
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()
            config.connection_instance = None


@pytest.fixture
def oracle_sync_session(oracle_sync_config: "OracleSyncConfig") -> "Generator[OracleSyncDriver, None, None]":
    """Create an Oracle sync driver session."""
    pytest.importorskip("oracledb")
    with oracle_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def oracle_async_session(oracle_async_config: "OracleAsyncConfig") -> "AsyncGenerator[OracleAsyncDriver, None]":
    """Create an Oracle async driver session."""
    pytest.importorskip("oracledb")
    async with oracle_async_config.provide_session() as driver:
        yield driver


@pytest.fixture(scope="session")
def oracle_adb_p12_only_wallet_zip(
    oracle_adb_service: OracleADBService, tmp_path_factory: pytest.TempPathFactory
) -> "Path":
    """Build a PKCS#12-only wallet archive using the upstream wallet builder."""
    archive_dir = tmp_path_factory.mktemp("oracle_adb_wallet_zip")
    source_path = oracle_adb_service.build_wallet_zip(archive_dir / "source.zip", include_sso=False)
    archive_path = archive_dir / "wallet.zip"
    with (
        zipfile.ZipFile(source_path) as source,
        zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive,
    ):
        for name in ("ewallet.p12", "tnsnames.ora", "sqlnet.ora"):
            archive.writestr(name, source.read(name))
    return archive_path


@pytest.fixture(scope="session")
def oracle_adb_sync_config(
    oracle_adb_service: OracleADBService, oracle_adb_p12_only_wallet_zip: "Path"
) -> "Generator[OracleSyncConfig, None, None]":
    """Provide a session-scoped Oracle Autonomous Database sync config using wallet_zip."""
    pytest.importorskip("oracledb")
    params = _oracle_adb_pool_params(oracle_adb_service, oracle_adb_p12_only_wallet_zip)
    config = OracleSyncConfig(connection_config=OraclePoolParams(**params))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
async def oracle_adb_async_config(
    oracle_adb_service: OracleADBService, oracle_adb_p12_only_wallet_zip: "Path"
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Provide a session-scoped Oracle Autonomous Database async config using wallet_zip."""
    pytest.importorskip("oracledb")
    params = _oracle_adb_pool_params(oracle_adb_service, oracle_adb_p12_only_wallet_zip)
    config = OracleAsyncConfig(connection_config=OraclePoolParams(**params))
    try:
        yield config
    finally:
        await config.close_pool()


def _oracle_pool_params(oracle_service: OracleService) -> "OraclePoolParams":
    return cast(
        "OraclePoolParams",
        {
            "host": oracle_service.host,
            "port": oracle_service.port,
            "service_name": oracle_service.service_name,
            "user": oracle_service.user,
            "password": oracle_service.password,
            "min": 1,
            "max": 5,
            "retry_count": 5,
            "retry_delay": 1,
        },
    )


def _oracle_adb_pool_params(
    oracle_adb_service: OracleADBService, wallet_zip_path: "Path"
) -> "OraclePoolParams":
    return cast(
        "OraclePoolParams",
        {
            "dsn": oracle_adb_service.mtls_alias,
            "user": oracle_adb_service.user,
            "password": oracle_adb_service.password,
            "wallet_zip": wallet_zip_path,
            "wallet_password": oracle_adb_service.wallet_password,
            "min": 1,
            "max": 3,
            "retry_count": 5,
            "retry_delay": 1,
        },
    )
