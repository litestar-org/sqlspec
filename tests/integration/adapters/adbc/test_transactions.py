"""Integration tests for ADBC transaction semantics."""

from collections.abc import Generator

import pytest

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from tests.integration.adapters.adbc.conftest import xfail_if_driver_missing

_BACKENDS = (
    pytest.param({"uri": ":memory:", "driver_name": "adbc_driver_sqlite"}, id="sqlite"),
    pytest.param({"driver_name": "adbc_driver_duckdb.dbapi.connect"}, id="duckdb"),
)


@pytest.fixture(params=_BACKENDS)
def adbc_tx_driver(request: pytest.FixtureRequest) -> Generator[AdbcDriver, None, None]:
    """Provide a fresh ADBC driver per backend with a clean table."""
    config = AdbcConfig(connection_config=dict(request.param))
    try:
        with config.provide_session() as driver:
            driver.execute_script("DROP TABLE IF EXISTS adbc_tx_test")
            driver.execute_script("CREATE TABLE adbc_tx_test (value INTEGER)")
            driver.commit()
            yield driver
    finally:
        config.close_pool()


@pytest.mark.adbc
@xfail_if_driver_missing
def test_adbc_repeated_commit_cycles(adbc_tx_driver: AdbcDriver) -> None:
    """execute -> commit repeated across statements does not raise."""
    adbc_tx_driver.execute("INSERT INTO adbc_tx_test (value) VALUES (1)")
    adbc_tx_driver.commit()
    adbc_tx_driver.execute("INSERT INTO adbc_tx_test (value) VALUES (2)")
    adbc_tx_driver.commit()

    result = adbc_tx_driver.execute("SELECT COUNT(*) AS count FROM adbc_tx_test")
    assert result.get_data()[0]["count"] == 2


@pytest.mark.adbc
@xfail_if_driver_missing
def test_adbc_rollback_undoes_insert(adbc_tx_driver: AdbcDriver) -> None:
    """rollback() undoes an uncommitted insert."""
    adbc_tx_driver.begin()
    adbc_tx_driver.execute("INSERT INTO adbc_tx_test (value) VALUES (99)")
    adbc_tx_driver.rollback()

    result = adbc_tx_driver.execute("SELECT COUNT(*) AS count FROM adbc_tx_test")
    assert result.get_data()[0]["count"] == 0
