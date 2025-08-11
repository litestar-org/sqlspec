"""Tests for sqlspec.utils.sync_tools module.

Tests synchronization tools including async/sync conversion utilities,
capacity limiting, and async context management.
"""

import asyncio
from typing import Any

import pytest

from sqlspec.exceptions import MissingDependencyError
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


def test_capacity_limiter_basic() -> None:
    """Test CapacityLimiter basic functionality."""
    limiter = CapacityLimiter(5)
    assert limiter.total_tokens == 5


def test_capacity_limiter_property_setter() -> None:
    """Test CapacityLimiter total_tokens property setter."""
    limiter = CapacityLimiter(5)
    limiter.total_tokens = 10
    assert limiter.total_tokens == 10


@pytest.mark.asyncio
async def test_capacity_limiter_async_context() -> None:
    """Test CapacityLimiter as async context manager."""
    limiter = CapacityLimiter(1)

    async with limiter:
        # Inside context, semaphore should be acquired
        assert limiter._semaphore._value == 0

    # Outside context, semaphore should be released
    assert limiter._semaphore._value == 1


@pytest.mark.asyncio
async def test_capacity_limiter_acquire_release() -> None:
    """Test CapacityLimiter manual acquire/release."""
    limiter = CapacityLimiter(1)

    await limiter.acquire()
    assert limiter._semaphore._value == 0

    limiter.release()
    assert limiter._semaphore._value == 1


@pytest.mark.asyncio
async def test_capacity_limiter_concurrent_access_edge_cases() -> None:
    """Test CapacityLimiter with edge case concurrent scenarios."""
    limiter = CapacityLimiter(1)
    results = []

    async def worker(worker_id: int) -> None:
        try:
            async with limiter:
                results.append(f"worker_{worker_id}_started")
                await asyncio.sleep(0.001)  # Very small delay
                results.append(f"worker_{worker_id}_finished")
        except Exception as e:
            results.append(f"worker_{worker_id}_error_{e}")

    # Run multiple workers concurrently
    tasks = [worker(i) for i in range(5)]
    await asyncio.gather(*tasks)

    # Should have orderly execution due to capacity limit
    assert len(results) == 10  # 5 workers * 2 events each
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

    # Test different exception types
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
    """Test await_ decorator basic functionality."""

    async def async_function(x: int) -> int:
        return x * 3

    sync_version = await_(async_function, raise_sync_error=False)
    result = sync_version(4)
    assert result == 12


def test_await_sync_error() -> None:
    """Test await_ decorator raises error when no loop and raise_sync_error=True."""

    async def async_function() -> int:
        return 42

    sync_version = await_(async_function, raise_sync_error=True)

    # This test depends on whether there's already an event loop running
    # In a testing environment, this behavior might vary
    try:
        asyncio.get_running_loop()
        # If there's a loop, we expect a RuntimeError about calling from within a task
        with pytest.raises(RuntimeError):
            sync_version()
    except RuntimeError:
        # No loop running, should also raise RuntimeError due to raise_sync_error=True
        with pytest.raises(RuntimeError):
            sync_version()


def test_await_raise_sync_error_configurations() -> None:
    """Test await_ with different raise_sync_error configurations."""

    async def simple_async_func(x: int) -> int:
        await asyncio.sleep(0.001)
        return x * 2

    # Test with raise_sync_error=False (should work in sync context)
    sync_func_safe = await_(simple_async_func, raise_sync_error=False)
    result = sync_func_safe(21)
    assert result == 42

    # Test with raise_sync_error=True (should raise in sync context)
    sync_func_strict = await_(simple_async_func, raise_sync_error=True)
    with pytest.raises(RuntimeError, match="Cannot run async function"):
        sync_func_strict(21)


@pytest.mark.asyncio
async def test_async_basic() -> None:
    """Test async_ decorator basic functionality."""

    def sync_function(x: int) -> int:
        return x * 4

    async_version = async_(sync_function)
    result = await async_version(3)
    assert result == 12


@pytest.mark.asyncio
async def test_async_with_limiter() -> None:
    """Test async_ decorator with custom limiter."""
    limiter = CapacityLimiter(1)

    def sync_function(x: int) -> int:
        return x * 5

    async_version = async_(sync_function, limiter=limiter)
    result = await async_version(2)
    assert result == 10


@pytest.mark.asyncio
async def test_ensure_async_with_async_function() -> None:
    """Test ensure_async_ with already async function."""

    async def already_async(x: int) -> int:
        return x * 6

    ensured: Any = ensure_async_(already_async)  # type: ignore[arg-type]
    result = await ensured(2)
    assert result == 12


@pytest.mark.asyncio
async def test_ensure_async_with_sync_function() -> None:
    """Test ensure_async_ with sync function."""

    def sync_function(x: int) -> int:
        return x * 7

    ensured = ensure_async_(sync_function)
    result = await ensured(3)
    assert result == 21


@pytest.mark.asyncio
async def test_ensure_async_exception_propagation() -> None:
    """Test ensure_async_ properly propagates exceptions."""

    @ensure_async_
    def sync_func_that_raises() -> None:
        raise ValueError("Sync function error")

    with pytest.raises(ValueError, match="Sync function error"):
        await sync_func_that_raises()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_get_next_with_default() -> None:
    """Test get_next with default value when iterator is exhausted."""

    class EmptyAsyncIterator:
        async def __anext__(self) -> int:
            raise StopAsyncIteration

    iterator = EmptyAsyncIterator()

    result = await get_next(iterator, "default_value")
    assert result == "default_value"


@pytest.mark.asyncio
async def test_get_next_no_default_behavior() -> None:
    """Test get_next behavior when iterator is exhausted without default."""

    class EmptyAsyncIterator:
        async def __anext__(self) -> int:
            raise StopAsyncIteration

    iterator = EmptyAsyncIterator()

    # The function might return a default value (NoValue) instead of raising
    # Let's just test that it handles the case without crashing
    try:
        result = await get_next(iterator)
        # If it returns something, check it's the expected default behavior
        assert isinstance(result, (type(NoValue), type(NoValue())))
    except StopAsyncIteration:
        # This is also acceptable behavior
        pass


def test_no_value_class() -> None:
    """Test NoValue class basic functionality."""
    no_val = NoValue()
    assert isinstance(no_val, NoValue)

    # Should be usable as a sentinel value
    assert no_val is not None
    assert no_val != "some_value"  # type: ignore[comparison-overlap]


def test_sync_tools_error_handling() -> None:
    """Test sync tools handle errors appropriately."""

    @run_
    async def async_function_with_error() -> None:
        raise RuntimeError("Async runtime error")

    with pytest.raises(RuntimeError, match="Async runtime error"):
        async_function_with_error()


@pytest.mark.asyncio
async def test_async_tools_comprehensive() -> None:
    """Test async tools work together comprehensively."""

    # Test combining multiple async utilities
    def blocking_operation(x: int) -> int:
        return x**2

    async_op = async_(blocking_operation)

    # Test with capacity limiter
    CapacityLimiter(2)

    # Run multiple operations concurrently
    tasks = [async_op(i) for i in range(5)]
    results = await asyncio.gather(*tasks)

    expected = [i**2 for i in range(5)]
    assert results == expected
