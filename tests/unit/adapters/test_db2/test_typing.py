"""Tests for the Db2 optional-dependency facade and vendor type aliases."""

import sys

import ibm_db_dbi
import pytest

import sqlspec.adapters.db2.pool as pool_module
import sqlspec.typing
from sqlspec.adapters.db2._typing import Db2Error, Db2SyncSessionContext
from sqlspec.adapters.db2.core import default_statement_config
from sqlspec.adapters.db2.data_dictionary import Db2SyncDataDictionary
from sqlspec.adapters.db2.driver import Db2SyncDriver
from sqlspec.adapters.db2.pool import Db2SyncConnectionPool
from sqlspec.utils.module_loader import reset_dependency_cache
from tests.unit.adapters.test_db2._fakes import FakeDb2Connection, FakeIbmDbDbiModule


def test_ibm_db_installed_flag_is_lazy(monkeypatch: pytest.MonkeyPatch) -> None:
    """The flag re-evaluates availability after the dependency cache is reset."""
    with monkeypatch.context() as patch:
        patch.setitem(sys.modules, "ibm_db", None)
        reset_dependency_cache("ibm_db")
        assert bool(sqlspec.typing.IBM_DB_INSTALLED) is False

    reset_dependency_cache("ibm_db")
    assert bool(sqlspec.typing.IBM_DB_INSTALLED) is True


def test_db2_error_catches_vendor_errors() -> None:
    """``Db2Error`` is the vendor base class: driver errors are caught, other errors are not."""
    with pytest.raises(Db2Error):
        raise ibm_db_dbi.ProgrammingError("SQL0204N  undefined name")

    with pytest.raises(ValueError):
        try:
            raise ValueError("not a driver error")
        except Db2Error:
            pass


def test_session_context_builds_sync_driver() -> None:
    """Entering the session context yields a sync driver bound to the acquired connection."""
    connection = FakeDb2Connection()
    released: list[object] = []
    context = Db2SyncSessionContext(
        acquire_connection=lambda: connection,
        release_connection=lambda conn, **_: released.append(conn),
        statement_config=default_statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with context as driver:
        assert isinstance(driver, Db2SyncDriver)
        assert driver.connection is connection

    assert released == [connection]


def test_pool_imports_ibm_db_dbi_on_first_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """The pool resolves ``ibm_db_dbi`` through the module loader when it first connects."""
    fake_module = FakeIbmDbDbiModule()
    requested: list[str] = []

    def fake_import_optional(name: str) -> FakeIbmDbDbiModule:
        requested.append(name)
        return fake_module

    monkeypatch.setattr(pool_module, "_IBM_DB_DBI", None)
    monkeypatch.setattr(pool_module, "import_optional", fake_import_optional)
    pool = Db2SyncConnectionPool({"database": "TESTDB"})

    pool.new_connection()
    pool.new_connection()

    assert requested == ["ibm_db_dbi"]
    assert len(fake_module.connect_calls) == 2
    assert all("DATABASE=TESTDB;" in call[0] for call in fake_module.connect_calls)


def test_driver_data_dictionary_is_reused() -> None:
    """The driver builds one Db2 data dictionary and returns it on every access."""
    driver = Db2SyncDriver(FakeDb2Connection())

    dictionary = driver.data_dictionary

    assert isinstance(dictionary, Db2SyncDataDictionary)
    assert driver.data_dictionary is dictionary
