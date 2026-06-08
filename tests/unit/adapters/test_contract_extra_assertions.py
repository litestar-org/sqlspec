"""Unit tests for the additive extra_assertions hook on the contract harness."""

import contextlib
from collections.abc import Iterator

import pytest

from tests.integration.adapters.contracts._cases import DriverCase
from tests.integration.adapters.contracts.behaviors import (
    dispatch_async_extra_assertions,
    dispatch_sync_extra_assertions,
    known_extra_assertion_keys,
    register_async_extra_assertion,
    register_sync_extra_assertion,
    validate_extra_assertions,
)


def _make_case(*extra_assertions: str) -> DriverCase:
    return DriverCase(
        id="unit-extra",
        fixture_name="unit_extra_fixture",
        adapter="unit",
        dialect="sqlite",
        mode="sync",
        extra_assertions=extra_assertions,
    )


@contextlib.contextmanager
def _registered_sync(key: str, scope: str) -> "Iterator[list[tuple[object, DriverCase]]]":
    calls: list[tuple[object, DriverCase]] = []
    register_sync_extra_assertion(key, scope, lambda driver, case: calls.append((driver, case)))
    try:
        yield calls
    finally:
        from tests.integration.adapters.contracts import behaviors

        behaviors._SYNC_EXTRA_ASSERTIONS.pop(key, None)


@contextlib.contextmanager
def _registered_async(key: str, scope: str) -> "Iterator[list[tuple[object, DriverCase]]]":
    calls: list[tuple[object, DriverCase]] = []

    async def _fn(driver: object, case: DriverCase) -> None:
        calls.append((driver, case))

    register_async_extra_assertion(key, scope, _fn)
    try:
        yield calls
    finally:
        from tests.integration.adapters.contracts import behaviors

        behaviors._ASYNC_EXTRA_ASSERTIONS.pop(key, None)


def test_driver_case_extra_assertions_defaults_empty() -> None:
    """extra_assertions is an additive opt-in field defaulting to an empty tuple."""
    case = DriverCase(id="x", fixture_name="f", adapter="a", dialect="sqlite", mode="sync")
    assert case.extra_assertions == ()


def test_dispatch_sync_runs_registered_assertion_for_matching_scope() -> None:
    """A registered sync proof runs when its scope matches the dispatching behavior."""
    with _registered_sync("unit:proof", "unit_scope") as calls:
        case = _make_case("unit:proof")
        sentinel = object()
        dispatch_sync_extra_assertions(sentinel, case, "unit_scope")
        assert calls == [(sentinel, case)]


def test_dispatch_sync_skips_other_scopes() -> None:
    """A proof registered under a different scope is not run by this behavior."""
    with _registered_sync("unit:proof", "unit_scope") as calls:
        case = _make_case("unit:proof")
        dispatch_sync_extra_assertions(object(), case, "other_scope")
        assert calls == []


def test_dispatch_sync_ignores_cases_without_keys() -> None:
    """A case that opts into nothing triggers no dispatch work."""
    with _registered_sync("unit:proof", "unit_scope") as calls:
        dispatch_sync_extra_assertions(object(), _make_case(), "unit_scope")
        assert calls == []


async def test_dispatch_async_runs_registered_assertion_for_matching_scope() -> None:
    """A registered async proof is awaited when its scope matches."""
    with _registered_async("unit:aproof", "unit_scope") as calls:
        case = _make_case("unit:aproof")
        sentinel = object()
        await dispatch_async_extra_assertions(sentinel, case, "unit_scope")
        assert calls == [(sentinel, case)]


def test_validate_extra_assertions_rejects_unknown_key() -> None:
    """An extra_assertions key registered in neither registry fails loud (no silent coverage loss)."""
    case = _make_case("definitely-not-registered")
    with pytest.raises(KeyError):
        validate_extra_assertions(case)


def test_validate_extra_assertions_accepts_known_key() -> None:
    """Validation passes once the key is present in a registry."""
    with _registered_sync("unit:known", "unit_scope"):
        validate_extra_assertions(_make_case("unit:known"))
        assert "unit:known" in known_extra_assertion_keys()


def test_register_sync_rejects_duplicate_key() -> None:
    """Duplicate registration of the same key is an error, not a silent overwrite."""
    with _registered_sync("unit:dup", "unit_scope"):
        with pytest.raises(ValueError):
            register_sync_extra_assertion("unit:dup", "unit_scope", lambda driver, case: None)
