"""Native Spanner snapshot bounds compose with read routing."""

import inspect
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import pytest

from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import ImproperConfigurationError, ReadOnlyTransactionError
from sqlspec.utils.routing import RoutingContext

pytestmark = [pytest.mark.integration, pytest.mark.spanner, pytest.mark.xdist_group("spanner")]


async def _resolve(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


@asynccontextmanager
async def _enter(context: Any, *, asynchronous: bool) -> AsyncGenerator[Any]:
    if asynchronous:
        async with context as value:
            yield value
    else:
        with context as value:
            yield value


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_snapshot_bounds_repeatable_reads_and_dml_refusal(
    spanner_config: SpannerSyncConfig, spanner_async_config: SpannerAsyncConfig, asynchronous: bool
) -> None:
    config = spanner_async_config if asynchronous else spanner_config
    registry = SQLSpec()
    registry.add_config(config)
    async with _enter(
        registry.provide_read_session(config, exact_staleness=timedelta(0)), asynchronous=asynchronous
    ) as reader:
        assert await _resolve(reader.select_value("SELECT 1")) == 1
        assert await _resolve(reader.select_value("SELECT 2")) == 2
        with pytest.raises(ReadOnlyTransactionError):
            await _resolve(reader.execute("INSERT INTO should_never_be_dispatched (id) VALUES (1)"))
    async with _enter(
        config.provide_read_session(max_staleness=timedelta(seconds=5)), asynchronous=asynchronous
    ) as reader:
        assert await _resolve(reader.select_value("SELECT 3")) == 3
    with pytest.raises(ImproperConfigurationError, match="multi_use"):
        config.provide_read_session(max_staleness=timedelta(seconds=5), multi_use=True)
    with RoutingContext.primary(config.bind_key):
        async with _enter(config.provide_read_session(), asynchronous=asynchronous) as reader:
            assert await _resolve(reader.select_value("SELECT 4")) == 4
    with RoutingContext.replica(config.bind_key, max_staleness_seconds=5):
        async with _enter(config.provide_read_session(), asynchronous=asynchronous) as reader:
            assert await _resolve(reader.select_value("SELECT 5")) == 5
