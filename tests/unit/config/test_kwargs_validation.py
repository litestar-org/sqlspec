"""Regression tests for pooled-config unexpected-keyword-argument validation.

The pooled base configs historically accepted an unbounded ``**kwargs`` that was
captured in the signature and silently discarded, so a typo'd keyword argument
produced no error. These tests pin the validate-and-raise behavior: any keyword
argument the pooled constructors do not recognize must raise ``TypeError``.
"""

from typing import Any

import pytest

from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig


class _MiniSyncPooledConfig(SyncDatabaseConfig[Any, Any, Any]):
    """Minimal concrete sync pooled config for construction tests."""

    def _create_pool(self) -> Any:
        return None

    def _close_pool(self) -> None:
        return None


class _MiniAsyncPooledConfig(AsyncDatabaseConfig[Any, Any, Any]):
    """Minimal concrete async pooled config for construction tests."""

    async def _create_pool(self) -> Any:
        return None

    async def _close_pool(self) -> None:
        return None


def test_sync_pooled_config_rejects_unexpected_kwarg() -> None:
    """Sync pooled config raises TypeError on an unexpected keyword argument."""
    with pytest.raises(TypeError, match="unexpected_option"):
        _MiniSyncPooledConfig(unexpected_option=123)


def test_async_pooled_config_rejects_unexpected_kwarg() -> None:
    """Async pooled config raises TypeError on an unexpected keyword argument."""
    with pytest.raises(TypeError, match="unexpected_option"):
        _MiniAsyncPooledConfig(unexpected_option=123)


def test_sync_pooled_config_accepts_known_kwargs() -> None:
    """Sync pooled config still constructs when only known keyword arguments are passed."""
    config = _MiniSyncPooledConfig(bind_key="primary", connection_config={"database": ":memory:"})
    assert config.bind_key == "primary"
    assert config.connection_config == {"database": ":memory:"}


def test_async_pooled_config_accepts_known_kwargs() -> None:
    """Async pooled config still constructs when only known keyword arguments are passed."""
    config = _MiniAsyncPooledConfig(bind_key="primary", connection_config={"database": ":memory:"})
    assert config.bind_key == "primary"
    assert config.connection_config == {"database": ":memory:"}
