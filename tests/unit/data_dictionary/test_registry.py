"""Regression tests for data dictionary registry loading state."""

from types import SimpleNamespace

import sqlspec.data_dictionary._registry as registry


def test_dialects_loaded_annotation_is_bool() -> None:
    """_DIALECTS_LOADED should be annotated to avoid Literal[False] narrowing."""
    assert registry.__annotations__["_DIALECTS_LOADED"] is bool


def test_load_default_dialects_sets_loaded_flag(monkeypatch) -> None:
    """_load_default_dialects sets the loaded flag after importing dialects."""
    calls: list[str] = []
    monkeypatch.setattr(registry, "_DIALECTS_LOADED", False)
    monkeypatch.setattr(registry.importlib, "import_module", lambda name: calls.append(name))

    registry._load_default_dialects()

    assert calls == ["sqlspec.data_dictionary.dialects"]
    assert registry._DIALECTS_LOADED is True


def test_load_default_dialects_is_idempotent(monkeypatch) -> None:
    """_load_default_dialects should not import again after the flag is set."""
    monkeypatch.setattr(registry, "_DIALECTS_LOADED", True)

    def fail_import(name: str) -> None:
        raise AssertionError(name)

    monkeypatch.setattr(registry.importlib, "import_module", fail_import)

    registry._load_default_dialects()


def test_get_dialect_config_triggers_load(monkeypatch) -> None:
    """get_dialect_config calls the default dialect loader before lookup."""
    calls = 0
    config = SimpleNamespace(name="example")
    monkeypatch.setitem(registry._DIALECT_CONFIGS, "example", config)

    def fake_load() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(registry, "_load_default_dialects", fake_load)

    assert registry.get_dialect_config("example") is config
    assert calls == 1
