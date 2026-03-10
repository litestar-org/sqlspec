"""Tests for the Spanner adapter package boundary."""

import importlib
import sys

import pytest


def test_importing_spanner_adapter_does_not_auto_import_dialects(monkeypatch: pytest.MonkeyPatch) -> None:
    """Importing the adapter package should not register dialects as a side effect."""
    monkeypatch.delitem(sys.modules, "sqlspec.dialects", raising=False)
    monkeypatch.delitem(sys.modules, "sqlspec.adapters.spanner", raising=False)

    importlib.import_module("sqlspec.adapters.spanner")

    assert "sqlspec.dialects" not in sys.modules


def test_legacy_spanner_dialect_module_is_not_available() -> None:
    """The old adapter-local dialect module path was intentionally removed."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("sqlspec.adapters.spanner.dialect")
