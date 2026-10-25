"""Shared Oracle integration fixtures."""

import shlex
import time
import zipfile
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest
from pytest_databases._service import get_docker_client, should_keep_containers
from pytest_databases.docker.oracle import OracleADBService, OracleService
from pytest_databases.helpers import exec_output_to_bytes

from sqlspec.utils.module_loader import import_optional, import_optional_attr

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
    from sqlspec.adapters.oracledb.core import parse_tnsnames_ora
else:
    OracleAsyncConfig = import_optional_attr("sqlspec.adapters.oracledb", "OracleAsyncConfig")
    OracleAsyncDriver = import_optional_attr("sqlspec.adapters.oracledb", "OracleAsyncDriver")
    OraclePoolParams = import_optional_attr("sqlspec.adapters.oracledb", "OraclePoolParams")
    OracleSyncConfig = import_optional_attr("sqlspec.adapters.oracledb", "OracleSyncConfig")
    OracleSyncDriver = import_optional_attr("sqlspec.adapters.oracledb", "OracleSyncDriver")
    parse_tnsnames_ora = import_optional_attr("sqlspec.adapters.oracledb.core", "parse_tnsnames_ora")

__all__ = (
    "oracle_18c_async_config",
    "oracle_18c_async_session",
    "oracle_18c_connection_config",
    "oracle_adb_async_config",
    "oracle_adb_p12_only_wallet_zip",
    "oracle_adb_sync_config",
    "oracle_adb_volumes",
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
def oracle_connection_config(oracle_23ai_service: OracleService) -> "OraclePoolParams":
    """Provide shared Oracle pool parameters."""
    return _oracle_pool_params(oracle_23ai_service)


@pytest.fixture(scope="session")
def oracle_aq_privileges(oracle_23ai_service: OracleService) -> None:
    """Grant the application user privileges required by Oracle AQ tests via in-container sqlplus."""
    app_user = oracle_23ai_service.user
    script = "\n".join((
        "SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF ECHO OFF",
        "WHENEVER OSERROR EXIT 9",
        "WHENEVER SQLERROR EXIT SQL.SQLCODE",
        f"GRANT aq_administrator_role, aq_user_role TO {app_user};",
        f"GRANT EXECUTE ON dbms_aq TO {app_user};",
        "EXIT",
        "",
    ))
    connect_string = f"system/{oracle_23ai_service.system_password}@//localhost:1521/{oracle_23ai_service.service_name}"
    command = f"printf '%s' {shlex.quote(script)} | sqlplus -L -S {shlex.quote(connect_string)}"
    result = oracle_23ai_service.container.exec_run(["bash", "-lc", command])
    exit_code = result.exit_code if result.exit_code is not None else -1
    if exit_code != 0:
        decoded_output = exec_output_to_bytes(result.output).decode("utf-8", errors="replace")
        msg = f"Failed to grant Oracle AQ privileges to {app_user!r}: {decoded_output}"
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
def oracle_adb_volumes(oracle_adb_image: str, request: pytest.FixtureRequest) -> dict[str, dict[str, str]]:
    """Patch Oracle ADB Free spfilePOD1.ora filesystemio_options=NONE for overlayfs and 4Kn block devices."""
    dbs_dir = "/u01/app/oracle/product/23.0.0.0/dbhome_1/dbs"
    volume_name = f"sqlspec_adb_dbs_{uuid4().hex[:8]}"
    client = get_docker_client()
    volume = client.volumes.create(name=volume_name)
    mounts = {volume_name: {"bind": dbs_dir, "mode": "rw"}}
    patch_script = (
        "sqlplus -S / as sysdba <<'EOF'\n"
        "CREATE PFILE='/tmp/initPOD1.ora' FROM SPFILE;\n"
        "HOST sed -i \"s/filesystemio_options=.*/filesystemio_options='NONE'/\" /tmp/initPOD1.ora\n"
        "CREATE SPFILE FROM PFILE='/tmp/initPOD1.ora';\n"
        "EXIT;\n"
        "EOF"
    )
    try:
        client.containers.run(
            oracle_adb_image, command=["bash", "-lc", patch_script], entrypoint=[], remove=True, volumes=mounts
        )
    except Exception:
        _remove_adb_volumes([volume])
        client.close()
        raise

    def cleanup() -> None:
        try:
            if not should_keep_containers(request.config):
                _remove_adb_volumes([volume])
        finally:
            client.close()

    request.config.add_cleanup(cleanup)
    return mounts


@pytest.fixture(scope="session")
def oracle_adb_p12_only_wallet_zip(
    oracle_adb_wallet_dir: "Path", oracle_adb_service: OracleADBService, tmp_path_factory: pytest.TempPathFactory
) -> "Path":
    """Build a PKCS#12-only Oracle Autonomous Database wallet archive omitting ewallet.pem and cwallet.sso."""
    archive_dir = tmp_path_factory.mktemp("oracle_adb_wallet_zip")
    archive_path = archive_dir / "wallet.zip"
    p12_bytes = _load_or_synthesize_adb_p12_bytes(oracle_adb_wallet_dir, oracle_adb_service.wallet_password)
    tnsnames_bytes = (oracle_adb_wallet_dir / "tnsnames.ora").read_bytes()
    sqlnet_bytes = (oracle_adb_wallet_dir / "sqlnet.ora").read_bytes()
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("ewallet.p12", p12_bytes)
        archive.writestr("tnsnames.ora", tnsnames_bytes)
        archive.writestr("sqlnet.ora", sqlnet_bytes)
    return archive_path


@pytest.fixture(scope="session")
def oracle_adb_sync_config(
    oracle_adb_service: OracleADBService, oracle_adb_wallet_dir: "Path", oracle_adb_p12_only_wallet_zip: "Path"
) -> "Generator[OracleSyncConfig, None, None]":
    """Provide a session-scoped Oracle Autonomous Database sync config using wallet_zip."""
    pytest.importorskip("oracledb")
    params = _oracle_adb_pool_params(oracle_adb_service, oracle_adb_wallet_dir, oracle_adb_p12_only_wallet_zip)
    config = OracleSyncConfig(connection_config=OraclePoolParams(**params))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
async def oracle_adb_async_config(
    oracle_adb_service: OracleADBService, oracle_adb_wallet_dir: "Path", oracle_adb_p12_only_wallet_zip: "Path"
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Provide a session-scoped Oracle Autonomous Database async config using wallet_zip."""
    pytest.importorskip("oracledb")
    params = _oracle_adb_pool_params(oracle_adb_service, oracle_adb_wallet_dir, oracle_adb_p12_only_wallet_zip)
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
    oracle_adb_service: OracleADBService, wallet_dir: "Path", wallet_zip_path: "Path"
) -> "OraclePoolParams":
    tns_alias = _resolve_oracle_adb_mtls_alias(oracle_adb_service, wallet_dir)
    return cast(
        "OraclePoolParams",
        {
            "dsn": tns_alias,
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


def _resolve_oracle_adb_mtls_alias(oracle_adb_service: OracleADBService, wallet_dir: "Path") -> str:
    entries = parse_tnsnames_ora(wallet_dir / "tnsnames.ora")
    candidates = (
        oracle_adb_service.service_name.lower(),
        oracle_adb_service.service_name.lower().split(".", 1)[0],
        *(alias.lower() for alias in oracle_adb_service.service_names),
    )
    for candidate in candidates:
        descriptor = entries.get(candidate, "")
        if "tcps" in descriptor.lower():
            return candidate
    for alias, descriptor in entries.items():
        if "tcps" in descriptor.lower():
            return alias
    return oracle_adb_service.service_names[0]


def _load_or_synthesize_adb_p12_bytes(wallet_dir: "Path", wallet_password: str) -> bytes:
    p12_path = wallet_dir / "ewallet.p12"
    if p12_path.is_file():
        return p12_path.read_bytes()
    pem_path = wallet_dir / "ewallet.pem"
    if not pem_path.is_file():
        msg = f"Expected ewallet.p12 or ewallet.pem in {wallet_dir}"
        raise RuntimeError(msg)
    serialization_mod: Any = import_optional("cryptography.hazmat.primitives.serialization")
    pkcs12_mod: Any = import_optional("cryptography.hazmat.primitives.serialization.pkcs12")
    x509_mod: Any = import_optional("cryptography.x509")
    if serialization_mod is None or pkcs12_mod is None or x509_mod is None:
        msg = "cryptography is required to synthesize ewallet.p12 from ewallet.pem"
        raise RuntimeError(msg)
    pem_bytes = pem_path.read_bytes()
    password_bytes = wallet_password.encode("utf-8")
    try:
        private_key = serialization_mod.load_pem_private_key(pem_bytes, password=None)
    except TypeError:
        private_key = serialization_mod.load_pem_private_key(pem_bytes, password=password_bytes)
    certs = x509_mod.load_pem_x509_certificates(pem_bytes)
    leaf_cert = certs[0] if certs else None
    ca_certs = certs[1:] if len(certs) > 1 else None
    synthesized: bytes = pkcs12_mod.serialize_key_and_certificates(
        name=b"oracle-adb-wallet",
        key=private_key,
        cert=leaf_cert,
        cas=ca_certs,
        encryption_algorithm=serialization_mod.BestAvailableEncryption(password_bytes),
    )
    return synthesized


def _remove_adb_volumes(volumes: list[Any]) -> None:
    """Remove temporary Docker volumes created for Oracle ADB Free O_DIRECT directories."""
    for volume in volumes:
        for _ in range(10):
            try:
                volume.remove(force=True)
                break
            except Exception:
                time.sleep(0.5)
