"""Tests for names promoted to the public package surfaces."""

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
