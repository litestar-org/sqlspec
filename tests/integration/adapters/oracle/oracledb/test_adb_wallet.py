"""Live Oracle Autonomous Database Free 23ai mTLS wallet_zip and DBMS_CLOUD integration tests."""

import zipfile
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from sqlspec.adapters.oracledb import OracleAsyncConfig, OraclePoolParams, OracleSyncConfig
from sqlspec.exceptions import SQLSpecError
from tests.integration.fixtures.oracle import resolve_oracle_adb_mtls_alias

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_databases.docker.oracle import OracleADBService

pytestmark = [
    pytest.mark.oracle,
    pytest.mark.integration,
    pytest.mark.xdist_group("oracle_adb"),
    pytest.mark.timeout(600),
]


def test_oracle_adb_sync_wallet_zip_p12_to_pem_mtls(
    oracle_adb_sync_config: OracleSyncConfig, oracle_adb_p12_only_wallet_zip: "Path"
) -> None:
    """Verify Thin-mode mTLS connection over tcps via PKCS#12-only wallet_zip conversion."""
    with zipfile.ZipFile(oracle_adb_p12_only_wallet_zip, "r") as archive:
        member_names = set(archive.namelist())
    assert "ewallet.p12" in member_names
    assert "tnsnames.ora" in member_names
    assert "sqlnet.ora" in member_names
    assert "ewallet.pem" not in member_names
    assert "cwallet.sso" not in member_names

    wallet_location = oracle_adb_sync_config.connection_config.get("wallet_location")
    assert isinstance(wallet_location, str)

    with oracle_adb_sync_config.provide_session() as driver:
        protocol = driver.select_value("SELECT SYS_CONTEXT('USERENV', 'NETWORK_PROTOCOL') AS proto FROM DUAL")
        assert str(protocol).lower() == "tcps"
        echoed = driver.select_value("SELECT :val AS v FROM DUAL", {"val": "mtls-sync"})
        assert echoed == "mtls-sync"


async def test_oracle_adb_async_wallet_zip_p12_to_pem_mtls(oracle_adb_async_config: OracleAsyncConfig) -> None:
    """Verify async Thin-mode mTLS connection and parameter binding via wallet_zip."""
    async with oracle_adb_async_config.provide_session() as driver:
        protocol = await driver.select_value("SELECT SYS_CONTEXT('USERENV', 'NETWORK_PROTOCOL') AS proto FROM DUAL")
        assert str(protocol).lower() == "tcps"
        echoed = await driver.select_value("SELECT :val AS v FROM DUAL", {"val": "mtls-async"})
        assert echoed == "mtls-async"


def test_oracle_adb_extracted_wallet_dir_service_name(
    oracle_adb_service: "OracleADBService", oracle_adb_wallet_dir: "Path"
) -> None:
    """Verify direct extracted wallet directory connection using TNS alias resolution."""
    tns_alias = resolve_oracle_adb_mtls_alias(oracle_adb_service, oracle_adb_wallet_dir)
    config = OracleSyncConfig(
        connection_config=OraclePoolParams(
            dsn=tns_alias,
            user=oracle_adb_service.user,
            password=oracle_adb_service.password,
            config_dir=str(oracle_adb_wallet_dir),
            wallet_location=str(oracle_adb_wallet_dir),
            wallet_password=oracle_adb_service.wallet_password,
            min=1,
            max=2,
        )
    )
    try:
        with config.provide_session() as driver:
            protocol = driver.select_value("SELECT SYS_CONTEXT('USERENV', 'NETWORK_PROTOCOL') AS proto FROM DUAL")
            assert str(protocol).lower() == "tcps"
    finally:
        config.close_pool()


def test_oracle_adb_dbms_cloud_capability_probe(oracle_adb_sync_config: OracleSyncConfig) -> None:
    """Verify DBMS_CLOUD catalog discovery and SQLSpecError mapping on COPY_DATA invocation."""
    table_name = f"adb_cloud_probe_{uuid4().hex[:8]}"
    with oracle_adb_sync_config.provide_session() as driver:
        package_count = int(
            driver.select_value(
                "SELECT COUNT(*) AS cnt FROM ALL_OBJECTS WHERE OBJECT_NAME = 'DBMS_CLOUD' AND OBJECT_TYPE = 'PACKAGE'"
            )
            or 0
        )
        assert package_count >= 0
        driver.execute(f"CREATE TABLE {table_name} (id NUMBER, label VARCHAR2(64))")
        try:
            with pytest.raises(SQLSpecError) as exc_info:
                driver.load_from_storage(
                    table_name,
                    "https://objectstorage.us-ashburn-1.oraclecloud.com/n/testns/b/testbkt/o/probe.parquet",
                    file_format="parquet",
                    credential_name="SQLSPEC_NONEXISTENT_CRED",
                )
            error_text = str(exc_info.value).upper()
            if package_count > 0:
                assert "ORA-" in error_text
            else:
                assert "DBMS_CLOUD" in error_text or "ORA-" in error_text or "PLS-" in error_text
        finally:
            driver.execute(f"DROP TABLE {table_name} PURGE")
