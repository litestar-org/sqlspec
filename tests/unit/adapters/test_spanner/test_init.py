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


def test_importing_dialect_subpackages_registers_dialects() -> None:
    """Importing the dialect subpackages registers them via sqlglot's metaclass."""
    importlib.import_module("sqlspec.dialects.postgres")
    importlib.import_module("sqlspec.dialects.spanner")

    assert "pgvector" in Dialect.classes
    assert "paradedb" in Dialect.classes
    assert "spanner" in Dialect.classes
    assert "spangres" in Dialect.classes


def test_dialect_names_resolve_through_entry_points(monkeypatch: pytest.MonkeyPatch) -> None:
    """sqlglot resolves sqlspec dialect names lazily via the entry-point group."""
    for key in ("pgvector", "paradedb", "spanner", "spangres"):
        Dialect.classes.pop(key, None)
    for module_name in list(sys.modules):
        if module_name.startswith("sqlspec.dialects"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    assert Dialect.get("spanner") is not None
    assert Dialect.get("pgvector") is not None
    assert "spanner" in Dialect.classes
    assert "pgvector" in Dialect.classes


def test_spanner_adapter_exports_sync_and_async_symbols() -> None:
    """Verify sqlspec.adapters.spanner exports all sync and async adapter symbols."""
    spanner_pkg = importlib.import_module("sqlspec.adapters.spanner")
    expected = {
        "SpannerAsyncConfig",
        "SpannerAsyncConnection",
        "SpannerAsyncConnectionContext",
        "SpannerAsyncCursor",
        "SpannerAsyncDataDictionary",
        "SpannerAsyncDriver",
        "SpannerAsyncExceptionHandler",
        "SpannerAsyncSessionContext",
        "SpannerConnection",
        "SpannerConnectionContext",
        "SpannerConnectionParams",
        "SpannerDataDictionary",
        "SpannerDriverFeatures",
        "SpannerExceptionHandler",
        "SpannerPoolParams",
        "SpannerSessionContext",
        "SpannerSyncConfig",
        "SpannerSyncCursor",
        "SpannerSyncDataDictionary",
        "SpannerSyncDriver",
        "default_statement_config",
    }
    assert expected.issubset(set(spanner_pkg.__all__))
    for name in spanner_pkg.__all__:
        assert getattr(spanner_pkg, name, None) is not None

