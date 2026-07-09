"""Regression tests for the lazy typing hubs."""

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
