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
    assert hasattr(db2_module, "Db2Config")
    assert hasattr(db2_module, "Db2Driver")
    assert hasattr(db2_module, "Db2ConnectionPool")
    assert hasattr(db2_module, "Db2ExceptionHandler")


def test_adapters_getattr_raises_attribute_error_for_unknown() -> None:
    """Verify AttributeError is raised for nonexistent adapters."""
    with pytest.raises(AttributeError, match="has no attribute 'nonexistent_adapter'"):
        getattr(sqlspec.adapters, "nonexistent_adapter")


def test_db2_adapter_public_exports_are_accessible() -> None:
    """Verify direct import of public symbols from sqlspec.adapters.db2."""
    from sqlspec.adapters.db2 import (
        Db2Config,
        Db2Connection,
        Db2ConnectionContext,
        Db2ConnectionParams,
        Db2ConnectionPool,
        Db2Driver,
        Db2DriverFeatures,
        Db2ExceptionHandler,
        Db2PoolParams,
    )

    assert Db2Config is not None
    assert Db2Driver is not None
    assert Db2ConnectionPool is not None
    assert Db2ExceptionHandler is not None
    assert Db2ConnectionContext is not None
    assert Db2ConnectionParams is not None
    assert Db2PoolParams is not None
    assert Db2DriverFeatures is not None
    assert Db2Connection is not None


def test_adapters_import_without_ibm_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify importing sqlspec.adapters succeeds when ibm_db is not installed."""
    monkeypatch.setitem(sys.modules, "ibm_db", None)
    monkeypatch.setitem(sys.modules, "ibm_db_dbi", None)

    adapters_mod = importlib.import_module("sqlspec.adapters")
    assert adapters_mod is not None
