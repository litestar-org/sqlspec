# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Tests for sqlspec.utils.sync_tools module.

Tests synchronization tools including async/sync conversion utilities,
capacity limiting, and async context management.
"""

import asyncio
from typing import Any

import pytest

from sqlspec.exceptions import MissingDependencyError
from sqlspec.utils.portal import PortalManager
from sqlspec.utils.sync_tools import (
    CapacityLimiter,
    NoValue,
    async_,
    await_,
    ensure_async_,
    get_next,
    run_,
    with_ensure_async_,
)

pytestmark = pytest.mark.xdist_group("utils")


def test_capacity_limiter_basic() -> None:
    """Test CapacityLimiter basic functionality."""
    limiter = CapacityLimiter(5)
    assert limiter.total_tokens == 5


def test_capacity_limiter_property_setter() -> None:
    """Test CapacityLimiter total_tokens property setter."""
    limiter = CapacityLimiter(5)
    limiter.total_tokens = 10
    assert limiter.total_tokens == 10


async def test_capacity_limiter_async_context() -> None:
    """Test CapacityLimiter as async context manager."""
    limiter = CapacityLimiter(1)

    async with limiter:
        assert limiter._semaphore._value == 0

    assert limiter._semaphore._value == 1


async def test_capacity_limiter_acquire_release() -> None:
    """Test CapacityLimiter manual acquire/release."""
    limiter = CapacityLimiter(1)

    await limiter.acquire()
    assert limiter._semaphore._value == 0

    limiter.release()
    assert limiter._semaphore._value == 1


async def test_capacity_limiter_concurrent_access_edge_cases() -> None:
    """Test CapacityLimiter with edge case concurrent scenarios."""
    limiter = CapacityLimiter(1)
    results = []

    async def worker(worker_id: int) -> None:
        try:
            async with limiter:
                results.append(f"worker_{worker_id}_started")
                await asyncio.sleep(0.001)
                results.append(f"worker_{worker_id}_finished")
        except Exception as e:
            results.append(f"worker_{worker_id}_error_{e}")

    tasks = [worker(i) for i in range(5)]
    await asyncio.gather(*tasks)

    assert len(results) == 10
    assert all("error" not in result for result in results)


def test_run_basic() -> None:
    """Test run_ decorator basic functionality."""

    @run_
    async def async_function(x: int) -> int:
        return x * 2

    result = async_function(5)
    assert result == 10


def test_run_with_exception() -> None:
    """Test run_ decorator with exception."""

    @run_
    async def async_failing_function() -> None:
        raise ValueError("Async error")

    with pytest.raises(ValueError, match="Async error"):
        async_failing_function()


def test_run_exception_propagation_detailed() -> None:
    """Test that run_ properly propagates various exception types."""

    async def async_func_value_error() -> None:
        raise ValueError("Async ValueError")

    async def async_func_runtime_error() -> None:
        raise RuntimeError("Async RuntimeError")

    async def async_func_custom_error() -> None:
        raise MissingDependencyError("Custom error")

    sync_func_ve = run_(async_func_value_error)
    with pytest.raises(ValueError, match="Async ValueError"):
        sync_func_ve()

    sync_func_re = run_(async_func_runtime_error)
    with pytest.raises(RuntimeError, match="Async RuntimeError"):
        sync_func_re()

    sync_func_ce = run_(async_func_custom_error)
    with pytest.raises(MissingDependencyError, match="Custom error"):
        sync_func_ce()


def test_await_basic() -> None:
    """Test await_ decorator basic functionality with automatic portal."""

    async def async_function(x: int) -> int:
        return x * 3

    sync_version = await_(async_function)
    result = sync_version(4)
    assert result == 12


def test_await_sync_error() -> None:
    """Test await_ decorator raises error when opt-in with raise_sync_error=True."""

    async def async_function() -> int:
        return 42

    sync_version = await_(async_function, raise_sync_error=True)

    try:
        asyncio.get_running_loop()

        with pytest.raises(RuntimeError):
            sync_version()
    except RuntimeError:
        with pytest.raises(RuntimeError):
            sync_version()


def test_await_raise_sync_error_configurations() -> None:
    """Test await_ with different raise_sync_error configurations."""

    async def simple_async_func(x: int) -> int:
        await asyncio.sleep(0.001)
        return x * 2

    sync_func_default = await_(simple_async_func)
    result = sync_func_default(21)
    assert result == 42

    sync_func_strict = await_(simple_async_func, raise_sync_error=True)
    with pytest.raises(RuntimeError, match="Cannot run async function"):
        sync_func_strict(21)


async def test_async_basic() -> None:
    """Test async_ decorator basic functionality."""

    def sync_function(x: int) -> int:
        return x * 4

    async_version = async_(sync_function)
    result = await async_version(3)
    assert result == 12


async def test_async_with_limiter() -> None:
    """Test async_ decorator with custom limiter."""
    limiter = CapacityLimiter(1)

    def sync_function(x: int) -> int:
        return x * 5

    async_version = async_(sync_function, limiter=limiter)
    result = await async_version(2)
    assert result == 10


async def test_ensure_async_with_async_function() -> None:
    """Test ensure_async_ with already async function."""

    async def already_async(x: int) -> int:
        return x * 6

    ensured: Any = ensure_async_(already_async)
    result = await ensured(2)
    assert result == 12


async def test_ensure_async_with_sync_function() -> None:
    """Test ensure_async_ with sync function."""

    def sync_function(x: int) -> int:
        return x * 7

    ensured = ensure_async_(sync_function)
    result = await ensured(3)
    assert result == 21


async def test_ensure_async_exception_propagation() -> None:
    """Test ensure_async_ properly propagates exceptions."""

    @ensure_async_
    def sync_func_that_raises() -> None:
        raise ValueError("Sync function error")

    with pytest.raises(ValueError, match="Sync function error"):
        await sync_func_that_raises()


async def test_with_ensure_async_context_manager() -> None:
    """Test with_ensure_async_ with sync context manager."""

    class SyncContextManager:
        def __init__(self) -> None:
            self.entered = False
            self.exited = False

        def __enter__(self) -> "SyncContextManager":
            self.entered = True
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.exited = True

    sync_cm = SyncContextManager()
    async_cm = with_ensure_async_(sync_cm)

    async with async_cm as result:
        assert result.entered is True
        assert result.exited is False

    assert result.exited is True


async def test_with_ensure_async_async_context_manager() -> None:
    """Test with_ensure_async_ with already async context manager."""

    class AsyncContextManager:
        def __init__(self) -> None:
            self.entered = False
            self.exited = False

        async def __aenter__(self) -> "AsyncContextManager":
            self.entered = True
            return self

        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.exited = True

    async_cm = AsyncContextManager()
    ensured = with_ensure_async_(async_cm)

    async with ensured as result:
        assert result.entered is True
        assert result.exited is False

    assert result.exited is True


async def test_get_next_basic() -> None:
    """Test get_next with async iterator."""

    class AsyncIterator:
        def __init__(self, items: list[int]) -> None:
            self.items = items
            self.index = 0

        def __aiter__(self) -> "AsyncIterator":
            return self

        async def __anext__(self) -> int:
            if self.index >= len(self.items):
                raise StopAsyncIteration
            value = self.items[self.index]
            self.index += 1
            return value

    iterator = AsyncIterator([1, 2, 3])

    result1 = await get_next(iterator)
    assert result1 == 1

    result2 = await get_next(iterator)
    assert result2 == 2


async def test_get_next_with_default() -> None:
    """Test get_next with default value when iterator is exhausted."""

    class EmptyAsyncIterator:
        def __aiter__(self) -> "EmptyAsyncIterator":
            return self

        async def __anext__(self) -> int:
            raise StopAsyncIteration

    iterator = EmptyAsyncIterator()

    result = await get_next(iterator, "default_value")
    assert result == "default_value"


async def test_get_next_no_default_behavior() -> None:
    """Test get_next behavior when iterator is exhausted without default."""

    class EmptyAsyncIterator:
        def __aiter__(self) -> "EmptyAsyncIterator":
            return self

        async def __anext__(self) -> int:
            raise StopAsyncIteration

    iterator = EmptyAsyncIterator()

    # Should raise StopAsyncIteration when no default is provided
    with pytest.raises(StopAsyncIteration):
        await get_next(iterator)


def test_no_value_class() -> None:
    """Test NoValue class basic functionality."""
    no_val = NoValue()
    assert isinstance(no_val, NoValue)

    assert no_val is not None
    assert no_val != "some_value"  # type: ignore[comparison-overlap]  # pyright: ignore[reportUnnecessaryComparison]


def test_sync_tools_error_handling() -> None:
    """Test sync tools handle errors appropriately."""

    @run_
    async def async_function_with_error() -> None:
        raise RuntimeError("Async runtime error")

    with pytest.raises(RuntimeError, match="Async runtime error"):
        async_function_with_error()


async def test_async_tools_integration() -> None:
    """Test async tools work together."""

    def blocking_operation(x: int) -> int:
        return x**2

    async_op = async_(blocking_operation)

    CapacityLimiter(2)

    tasks = [async_op(i) for i in range(5)]
    results = await asyncio.gather(*tasks)

    expected = [i**2 for i in range(5)]
    assert results == expected


def test_await_portal_integration() -> None:
    """Test await_ uses portal automatically with default settings."""

    async def async_add(x: int, y: int) -> int:
        await asyncio.sleep(0.01)
        return x + y

    sync_add = await_(async_add)
    result = sync_add(5, 3)

    assert result == 8


def test_await_portal_integration_multiple_calls() -> None:
    """Test await_ portal integration with multiple calls reuses same portal."""

    async def async_multiply(x: int) -> int:
        await asyncio.sleep(0.01)
        return x * 2

    sync_multiply = await_(async_multiply)

    result1 = sync_multiply(5)
    result2 = sync_multiply(10)
    result3 = sync_multiply(20)

    assert result1 == 10
    assert result2 == 20
    assert result3 == 40


def test_await_portal_exception_propagation() -> None:
    """Test await_ portal integration propagates exceptions correctly."""

    async def async_error() -> int:
        await asyncio.sleep(0.01)
        raise ValueError("Portal error test")

    sync_error = await_(async_error)

    with pytest.raises(ValueError, match="Portal error test"):
        sync_error()


def test_await_portal_with_complex_types() -> None:
    """Test await_ portal integration with complex return types."""

    async def fetch_data(user_id: int) -> dict[str, Any]:
        await asyncio.sleep(0.01)
        return {"user_id": user_id, "status": "active", "data": [1, 2, 3]}

    sync_fetch = await_(fetch_data)
    result = sync_fetch(42)

    assert result == {"user_id": 42, "status": "active", "data": [1, 2, 3]}


def test_await_portal_cleanup() -> None:
    """Test await_ portal integration - verify portal is running and can be stopped."""

    async def async_func() -> int:
        await asyncio.sleep(0.01)
        return 100

    sync_func = await_(async_func)
    result = sync_func()

    assert result == 100

    manager = PortalManager()
    assert manager.is_running

    manager.stop()
    assert not manager.is_running
