"""Fixtures wiring the shared ibm_db fakes into the Db2 adapter."""

import pytest

import sqlspec.adapters.db2._typing as typing_module
import sqlspec.adapters.db2.pool as pool_module
from tests.unit.adapters.test_db2._fakes import FakeDb2Connection, FakeIbmDbDbiModule, FakeIbmDbModule


@pytest.fixture
def fake_ibm_db(monkeypatch: pytest.MonkeyPatch) -> "tuple[FakeIbmDbModule, FakeIbmDbDbiModule]":
    """Patch the adapter's ``ibm_db`` and ``ibm_db_dbi`` references with paired fakes.

    Returns:
        The fake ``ibm_db`` module and the fake ``ibm_db_dbi`` module.
    """
    ibm_db = FakeIbmDbModule()
    ibm_db_dbi = FakeIbmDbDbiModule(ibm_db)
    monkeypatch.setattr(typing_module, "ibm_db", ibm_db)
    monkeypatch.setattr(typing_module, "ibm_db_dbi", ibm_db_dbi)
    monkeypatch.setattr(pool_module, "_IBM_DB_DBI", ibm_db_dbi)
    return ibm_db, ibm_db_dbi


@pytest.fixture
def fake_connection() -> FakeDb2Connection:
    """Return a fresh fake connection with autocommit off.

    Returns:
        The fake connection.
    """
    return FakeDb2Connection()
