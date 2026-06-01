"""Tests for Sanic correlation extractor lifecycle."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sqlspec.core import CorrelationExtractor
from sqlspec.extensions.sanic import SQLSpecPlugin
from sqlspec.utils.correlation import CorrelationContext

pytestmark = pytest.mark.anyio


def _make_config(*, enable_correlation: bool = False) -> MagicMock:
    config = MagicMock()
    config.supports_connection_pooling = True
    config.extension_config = {
        "sanic": {
            "disable_di": True,
            "enable_correlation_middleware": enable_correlation,
            "enable_sqlcommenter_middleware": False,
        }
    }
    return config


def _make_request(correlation_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        app=SimpleNamespace(ctx=SimpleNamespace()),
        ctx=SimpleNamespace(),
        endpoint=None,
        headers={"x-request-id": correlation_id},
        path="/test",
        uri_template=None,
    )


def test_sanic_plugin_extractor_is_none_when_correlation_disabled() -> None:
    plugin = SQLSpecPlugin(MagicMock(configs={"default": _make_config(enable_correlation=False)}))

    assert plugin._extractor is None  # pyright: ignore[reportPrivateUsage]


def test_sanic_plugin_builds_extractor_when_correlation_enabled() -> None:
    plugin = SQLSpecPlugin(MagicMock(configs={"default": _make_config(enable_correlation=True)}))

    assert isinstance(plugin._extractor, CorrelationExtractor)  # pyright: ignore[reportPrivateUsage]


async def test_sanic_plugin_reuses_same_extractor_across_requests() -> None:
    plugin = SQLSpecPlugin(MagicMock(configs={"default": _make_config(enable_correlation=True)}))
    extractor_id = id(plugin._extractor)  # pyright: ignore[reportPrivateUsage]

    try:
        for index in range(5):
            request = _make_request(f"request-{index}")
            plugin._set_correlation_context(request)  # pyright: ignore[reportPrivateUsage]
            assert CorrelationContext.get() == f"request-{index}"
            assert id(plugin._extractor) == extractor_id  # pyright: ignore[reportPrivateUsage]
            plugin._restore_correlation_context(request, SimpleNamespace(headers={}))  # pyright: ignore[reportPrivateUsage]
    finally:
        CorrelationContext.clear()
