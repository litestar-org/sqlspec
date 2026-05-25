"""Unit tests for mssql_python driver wiring."""

from typing import Any, cast

from sqlspec.adapters.mssql_python.data_dictionary import MssqlPythonAsyncDataDictionary, MssqlPythonSyncDataDictionary
from sqlspec.adapters.mssql_python.driver import MssqlPythonAsyncDriver, MssqlPythonDriver


class DummyConnection:
    """Minimal connection for driver construction."""

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_sync_driver_lazily_initializes_data_dictionary() -> None:
    """The sync driver should expose the MSSQL data dictionary."""
    driver = MssqlPythonDriver(cast("Any", DummyConnection()))

    assert isinstance(driver.data_dictionary, MssqlPythonSyncDataDictionary)
    assert driver.data_dictionary is driver.data_dictionary


def test_async_driver_lazily_initializes_data_dictionary() -> None:
    """The async driver should expose the MSSQL async data dictionary."""
    driver = MssqlPythonAsyncDriver(cast("Any", DummyConnection()))

    assert isinstance(driver.data_dictionary, MssqlPythonAsyncDataDictionary)
    assert driver.data_dictionary is driver.data_dictionary
