"""Tests for names promoted to the public package surfaces."""

from typing import NoReturn

import pytest

import sqlspec
from sqlspec.utils import serializers, uuids


def test_top_level_uuid_exports() -> None:
    """The top-level package re-exports the identifier generators from ``sqlspec.utils.uuids``."""
    from sqlspec import nanoid, uuid4, uuid6, uuid7

    assert uuid4 is uuids.uuid4
    assert uuid6 is uuids.uuid6
    assert uuid7 is uuids.uuid7
    assert nanoid is uuids.nanoid
    assert {"uuid4", "uuid6", "uuid7", "nanoid"}.issubset(sqlspec.__all__)


@pytest.mark.parametrize("name", ("nanoid", "uuid4", "uuid6", "uuid7"))
def test_top_level_uuid_exports_are_cached_after_first_access(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """The first lookup stores the helper on the module so later lookups skip ``__getattr__``."""
    monkeypatch.delitem(vars(sqlspec), name, raising=False)

    first = getattr(sqlspec, name)

    assert first is getattr(uuids, name)
    assert vars(sqlspec)[name] is first

    def fail_lookup(attr: str) -> NoReturn:
        raise AssertionError(attr)

    monkeypatch.setattr(sqlspec, "__getattr__", fail_lookup)
    assert getattr(sqlspec, name) is first


def test_top_level_dir_lists_public_exports() -> None:
    """``dir(sqlspec)`` includes every name in ``sqlspec.__all__``."""
    assert set(sqlspec.__all__) <= set(dir(sqlspec))


def test_litestar_correlation_exports() -> None:
    """The Litestar package exports the correlation middleware and trace header defaults."""
    from sqlspec.extensions import litestar
    from sqlspec.extensions.litestar import TRACE_CONTEXT_FALLBACK_HEADERS, CorrelationMiddleware, plugin

    assert CorrelationMiddleware is plugin.CorrelationMiddleware
    assert TRACE_CONTEXT_FALLBACK_HEADERS is plugin.TRACE_CONTEXT_FALLBACK_HEADERS
    assert {"CorrelationMiddleware", "TRACE_CONTEXT_FALLBACK_HEADERS"}.issubset(litestar.__all__)


def test_top_level_exports_resolve() -> None:
    """Every name in ``sqlspec.__all__`` resolves, including lazily loaded names."""
    for name in sqlspec.__all__:
        getattr(sqlspec, name)


def test_serializer_exports_resolve() -> None:
    """Every name in ``sqlspec.utils.serializers.__all__`` resolves."""
    for name in serializers.__all__:
        getattr(serializers, name)
