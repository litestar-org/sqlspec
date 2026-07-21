"""Oracle direct path load default integration coverage (Thin mode)."""

from collections.abc import AsyncGenerator, Generator

import pyarrow as pa
import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleSyncConfig

pytestmark = pytest.mark.xdist_group("oracle")


@pytest.fixture
def oracle_sync_dpl_config(oracle_23ai_service: "OracleService") -> "Generator[OracleSyncConfig, None, None]":
    config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )
    yield config
    config.close_pool()


@pytest.fixture
async def oracle_async_dpl_config(oracle_23ai_service: "OracleService") -> "AsyncGenerator[OracleAsyncConfig, None]":
    config = OracleAsyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
        }
    )
    yield config
    await config.close_pool()


def test_sync_direct_path_load_round_trip(oracle_sync_dpl_config: "OracleSyncConfig") -> None:
    with oracle_sync_dpl_config.provide_session() as driver:
        if not getattr(driver.connection, "thin", False):
            pytest.skip("direct path load requires Thin mode")
        driver.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE DPL_SYNC_TEST'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        driver.execute_script("CREATE TABLE DPL_SYNC_TEST (ID NUMBER, NAME VARCHAR2(50))")

        arrow_table = pa.table({"ID": list(range(100)), "NAME": [f"row{i}" for i in range(100)]})
        job = driver.load_from_arrow("DPL_SYNC_TEST", arrow_table)
        driver.commit()

        assert job.telemetry["rows_processed"] == 100
        count = driver.execute("SELECT COUNT(*) AS C FROM DPL_SYNC_TEST").get_data()[0]["c"]
        assert count == 100

        overwrite_table = pa.table({"ID": [1, 2, 3], "NAME": ["a", "b", "c"]})
        driver.load_from_arrow("DPL_SYNC_TEST", overwrite_table, overwrite=True)
        driver.commit()
        count = driver.execute("SELECT COUNT(*) AS C FROM DPL_SYNC_TEST").get_data()[0]["c"]
        assert count == 3

        driver.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE DPL_SYNC_TEST'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )


async def test_async_direct_path_load_round_trip(oracle_async_dpl_config: "OracleAsyncConfig") -> None:
    async with oracle_async_dpl_config.provide_session() as driver:
        if not getattr(driver.connection, "thin", False):
            pytest.skip("direct path load requires Thin mode")
        await driver.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE DPL_ASYNC_TEST'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        await driver.execute_script("CREATE TABLE DPL_ASYNC_TEST (ID NUMBER, NAME VARCHAR2(50))")

        arrow_table = pa.table({"ID": list(range(100)), "NAME": [f"row{i}" for i in range(100)]})
        job = await driver.load_from_arrow("DPL_ASYNC_TEST", arrow_table)
        await driver.commit()

        assert job.telemetry["rows_processed"] == 100
        result = await driver.execute("SELECT COUNT(*) AS C FROM DPL_ASYNC_TEST")
        assert result.get_data()[0]["c"] == 100

        await driver.execute_script(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE DPL_ASYNC_TEST'; "
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
