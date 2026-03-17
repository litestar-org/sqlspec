"""Tests for the Spanner adapter package boundary."""

import importlib
import sys

import pytest
from sqlglot.dialects.dialect import Dialect


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


def test_importing_sqlspec_dialects_registers_postgres_and_spanner_dialects(monkeypatch: pytest.MonkeyPatch) -> None:
    """Top-level dialect import should register all documented sqlspec dialects."""
    for module_name in (
        "sqlspec.dialects",
        "sqlspec.dialects.postgres",
        "sqlspec.dialects.spanner",
        "sqlspec.dialects.postgres._pgvector",
        "sqlspec.dialects.postgres._paradedb",
    ):
        monkeypatch.delitem(sys.modules, module_name, raising=False)

    Dialect.classes.pop("pgvector", None)
    Dialect.classes.pop("paradedb", None)
    Dialect.classes.pop("spanner", None)
    Dialect.classes.pop("spangres", None)

    importlib.import_module("sqlspec.dialects")

    assert "pgvector" in Dialect.classes
    assert "paradedb" in Dialect.classes
    assert "spanner" in Dialect.classes
    assert "spangres" in Dialect.classes
