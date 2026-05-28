"""Round-trip tests for copy.deepcopy and pickle on mypyc-compiled FastAPI filter providers."""

# Safe: pickle round-trips here serialize objects produced inside the test and
# immediately deserialize them in the same process. No untrusted input crosses
# the pickle boundary.
import copy
import inspect
import pickle
from typing import Any

import pytest

from sqlspec.extensions.fastapi import providers as fastapi_providers
from sqlspec.extensions.fastapi.providers import FieldNameType


_FACTORIES: "dict[str, Any]" = {
    "_LimitOffsetFilterProvider": lambda cls: cls(20),
    "_IdFilterProvider": lambda cls: cls("id", int),
    "_BeforeAfterFilterProvider": lambda cls: cls("created_at", "createdBefore", "createdAfter"),
    "_SearchFilterProvider": lambda cls: cls({"name", "email"}, False),
    "_OrderByProvider": lambda cls: cls("id", {"sort_field": "id"}),
    "_CollectionFilterProvider": lambda cls: cls(FieldNameType("status", str), negated=False),
    "_NullFilterProvider": lambda cls: cls("deleted_at", negated=False),
}


def _discover_provider_classes() -> "list[type[Any]]":
    classes: list[type[Any]] = []
    for name, cls in inspect.getmembers(fastapi_providers, inspect.isclass):
        if not name.startswith("_") or name.startswith("__"):
            continue
        if not hasattr(cls, "__mypyc_attrs__"):
            continue
        classes.append(cls)
    return classes


def _build(cls: "type[Any]") -> Any:
    factory = _FACTORIES.get(cls.__name__)
    if factory is None:
        msg = (
            f"No factory registered for {cls.__name__}. Add an entry to "
            "_FACTORIES at the top of this test module."
        )
        raise AssertionError(msg)
    return factory(cls)


def _assert_mypyc_attrs_equal(left: Any, right: Any) -> None:
    for attr in type(left).__mypyc_attrs__:
        left_value = getattr(left, attr)
        right_value = getattr(right, attr)
        assert left_value == right_value, (
            f"{type(left).__name__}.{attr} diverged after round-trip: "
            f"{left_value!r} != {right_value!r}"
        )


@pytest.mark.parametrize("cls", _discover_provider_classes(), ids=lambda c: c.__name__)
def test_provider_deepcopy_roundtrip(cls: "type[Any]") -> None:
    inst = _build(cls)
    out = copy.deepcopy(inst)
    assert out is not inst
    assert type(out) is type(inst)
    _assert_mypyc_attrs_equal(inst, out)


@pytest.mark.parametrize("cls", _discover_provider_classes(), ids=lambda c: c.__name__)
def test_provider_pickle_roundtrip(cls: "type[Any]") -> None:
    inst = _build(cls)
    out = pickle.loads(pickle.dumps(inst))
    assert out is not inst
    assert type(out) is type(inst)
    _assert_mypyc_attrs_equal(inst, out)


def test_factory_registry_covers_all_discovered_classes() -> None:
    discovered = {cls.__name__ for cls in _discover_provider_classes()}
    registered = set(_FACTORIES)
    missing = discovered - registered
    extra = registered - discovered
    assert not missing, f"_FACTORIES missing entries for newly added provider classes: {missing}"
    assert not extra, f"_FACTORIES has stale entries for removed/renamed classes: {extra}"
