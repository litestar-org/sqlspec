"""Regression tests for the lazy typing hubs."""

import pickle
from typing import get_args

import pytest

import sqlspec.typing as public_typing
from sqlspec import _typing as private_typing


def test_typing_hubs_expose_all_public_names_in_dir() -> None:
    """dir() should cover the full public surface of both typing modules."""
    assert set(public_typing.__all__).issubset(set(dir(public_typing)))
    assert set(private_typing.__all__).issubset(set(dir(private_typing)))


def test_typing_hubs_resolve_all_public_exports() -> None:
    """Every public name should be resolvable through getattr()."""
    for name in public_typing.__all__:
        getattr(public_typing, name)

    for name in private_typing.__all__:
        getattr(private_typing, name)


def test_lazy_exports_cache_the_first_resolved_object(monkeypatch) -> None:
    """First access should resolve once and subsequent accesses should reuse the cached object."""
    sentinel = object()
    calls: list[tuple[str, str, object]] = []

    def fake_resolve(module_name: str, attr_name: str, fallback: object) -> object:
        calls.append((module_name, attr_name, fallback))
        return sentinel

    monkeypatch.delitem(public_typing.__dict__, "BaseModel", raising=False)
    monkeypatch.delitem(private_typing.__dict__, "BaseModel", raising=False)
    monkeypatch.setattr(private_typing, "resolve_optional_attr", fake_resolve)

    assert getattr(private_typing, "BaseModel") is sentinel
    assert getattr(private_typing, "BaseModel") is sentinel
    assert getattr(public_typing, "BaseModel") is sentinel
    assert getattr(public_typing, "BaseModel") is sentinel
    assert calls == [("pydantic", "BaseModel", private_typing.BaseModelStub)]


def test_lazy_exports_preserve_missing_dependency_shim_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing optional dependencies should expose one stable shim through both hubs."""

    def missing_dependency(_module_name: str, _attr_name: str, fallback: object) -> object:
        return fallback

    monkeypatch.delitem(public_typing.__dict__, "ArrowTable", raising=False)
    monkeypatch.delitem(private_typing.__dict__, "ArrowTable", raising=False)
    monkeypatch.setattr(private_typing, "resolve_optional_attr", missing_dependency)

    private_arrow_table = getattr(private_typing, "ArrowTable")
    public_arrow_table = getattr(public_typing, "ArrowTable")

    assert private_arrow_table is private_typing.ArrowTableResult
    assert public_arrow_table is private_arrow_table
    assert getattr(private_typing, "ArrowTable") is private_arrow_table
    assert getattr(public_typing, "ArrowTable") is public_arrow_table


@pytest.mark.parametrize("export_name", ("Span", "Tracer", "Counter", "Gauge", "Histogram"))
def test_observability_fallbacks_preserve_public_class_names(monkeypatch: pytest.MonkeyPatch, export_name: str) -> None:
    """Laziness should not change the observable identity of fallback classes."""

    def missing_dependency(_module_name: str, _attr_name: str, fallback: object) -> object:
        return fallback

    monkeypatch.delitem(public_typing.__dict__, export_name, raising=False)
    monkeypatch.delitem(private_typing.__dict__, export_name, raising=False)
    monkeypatch.setattr(private_typing, "resolve_optional_attr", missing_dependency)

    fallback = getattr(public_typing, export_name)

    assert fallback is getattr(private_typing, export_name)
    assert fallback.__name__ == export_name
    assert fallback.__qualname__ == export_name
    assert repr(fallback) == f"<class 'sqlspec._typing.{export_name}'>"
    assert pickle.loads(pickle.dumps(fallback)) is fallback


@pytest.mark.parametrize(
    ("export_name", "stub_name"),
    (("cattrs_structure", "cattrs_structure_stub"), ("cattrs_unstructure", "cattrs_unstructure_stub")),
)
def test_cattrs_fallbacks_preserve_public_function_identity(
    monkeypatch: pytest.MonkeyPatch, export_name: str, stub_name: str
) -> None:
    """Missing cattrs exports should retain their advertised function identity."""

    def missing_dependency(_module_name: str, _attr_name: str, fallback: object) -> object:
        return fallback

    monkeypatch.delitem(public_typing.__dict__, export_name, raising=False)
    monkeypatch.delitem(private_typing.__dict__, export_name, raising=False)
    monkeypatch.delattr(private_typing, stub_name)
    monkeypatch.setattr(private_typing, "resolve_optional_attr", missing_dependency)

    fallback = getattr(public_typing, export_name)

    assert fallback is getattr(private_typing, export_name)
    assert fallback.__name__ == export_name
    assert fallback.__qualname__ == export_name
    assert repr(fallback).startswith(f"<function {export_name} at ")
    assert pickle.loads(pickle.dumps(fallback)) is fallback


def test_get_type_adapter_resolves_lazy_pydantic_exports(monkeypatch: pytest.MonkeyPatch) -> None:
    """The helper should resolve lazy globals explicitly for normal and fail-fast adapters."""
    created: list[object] = []

    class FakeTypeAdapter:
        def __init__(self, annotation: object) -> None:
            created.append(annotation)

    class FakeFailFast:
        pass

    monkeypatch.setattr(private_typing, "TypeAdapter", FakeTypeAdapter)
    monkeypatch.setattr(private_typing, "FailFast", FakeFailFast)
    public_typing.get_type_adapter.cache_clear()
    monkeypatch.setattr(public_typing, "PYDANTIC_USE_FAILFAST", False)

    assert isinstance(public_typing.get_type_adapter(str), FakeTypeAdapter)
    assert created == [str]

    public_typing.get_type_adapter.cache_clear()
    monkeypatch.setattr(public_typing, "PYDANTIC_USE_FAILFAST", True)
    assert isinstance(public_typing.get_type_adapter(int), FakeTypeAdapter)
    assert get_args(created[-1])[0] is int
    assert isinstance(get_args(created[-1])[1], FakeFailFast)
