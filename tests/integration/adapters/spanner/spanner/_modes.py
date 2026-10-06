"""Helpers that run one Spanner test body against the sync and async adapters."""

from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from inspect import isawaitable
from typing import Any, Literal, TypeVar

from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerAsyncDriver, SpannerSyncConfig, SpannerSyncDriver

__all__ = ("SpannerModeConfig", "SpannerModeDriver", "invoke", "mode_session")

T = TypeVar("T")

SpannerModeConfig = SpannerSyncConfig | SpannerAsyncConfig
SpannerModeDriver = SpannerSyncDriver | SpannerAsyncDriver

_PROVIDERS = {"session": "provide_session", "read": "provide_read_session", "write": "provide_write_session"}


async def invoke(result: "T | Awaitable[T]") -> T:
    """Return a driver call result, awaiting it for the async adapter."""
    if isawaitable(result):
        return await result
    return result


@asynccontextmanager
async def mode_session(
    config: SpannerModeConfig, kind: Literal["session", "read", "write"] = "session", **kwargs: Any
) -> "AsyncIterator[Any]":
    """Open a default, read, or write session on either adapter."""
    provider: Any = getattr(config, _PROVIDERS[kind])
    if isinstance(config, SpannerAsyncConfig):
        async with provider(**kwargs) as session:
            yield session
    else:
        with provider(**kwargs) as session:
            yield session
