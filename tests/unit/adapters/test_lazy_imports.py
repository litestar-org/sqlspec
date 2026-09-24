"""Tests for lazy loading of adapter subpackages."""

import importlib
import sys

import pytest

import sqlspec.adapters


def test_adapters_module_has_db2_in_all_and_dir() -> None:
    """Verify db2 is advertised in __all__ and __dir__."""
    assert "db2" in sqlspec.adapters.__all__
    assert "db2" in dir(sqlspec.adapters)


def test_adapters_dynamic_getattr_resolves_db2() -> None:
    """Verify getattr on sqlspec.adapters resolves db2 package."""
    db2_module = getattr(sqlspec.adapters, "db2")
    assert db2_module.__name__ == "sqlspec.adapters.db2"
    assert hasattr(db2_module, "Db2SyncConfig")
    assert hasattr(db2_module, "Db2SyncDriver")
    assert hasattr(db2_module, "Db2SyncConnectionPool")
    assert hasattr(db2_module, "Db2SyncExceptionHandler")


def test_adapters_getattr_raises_attribute_error_for_unknown() -> None:
    """Verify AttributeError is raised for nonexistent adapters."""
    with pytest.raises(AttributeError, match="has no attribute 'nonexistent_adapter'"):
        getattr(sqlspec.adapters, "nonexistent_adapter")


def test_db2_adapter_public_exports_are_accessible() -> None:
    """Verify direct import of public symbols from sqlspec.adapters.db2."""
    from sqlspec.adapters.db2 import (
        Db2ConnectionParams,
        Db2DriverFeatures,
        Db2PoolParams,
        Db2SyncConfig,
        Db2SyncConnection,
        Db2SyncConnectionContext,
        Db2SyncConnectionPool,
        Db2SyncDriver,
        Db2SyncExceptionHandler,
    )

    assert Db2SyncConfig is not None
    assert Db2SyncDriver is not None
    assert Db2SyncConnectionPool is not None
    assert Db2SyncExceptionHandler is not None
    assert Db2SyncConnectionContext is not None
    assert Db2ConnectionParams is not None
    assert Db2PoolParams is not None
    assert Db2DriverFeatures is not None
    assert Db2SyncConnection is not None


def test_adapters_import_without_ibm_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify importing sqlspec.adapters succeeds when ibm_db is not installed."""
    monkeypatch.setitem(sys.modules, "ibm_db", None)
    monkeypatch.setitem(sys.modules, "ibm_db_dbi", None)

    adapters_mod = importlib.import_module("sqlspec.adapters")
    assert adapters_mod is not None
