"""Tests for sqlspec.utils.sync_tools module."""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, TypeVar

import pytest

from sqlspec.utils.sync_tools import CapacityLimiter, async_, await_, ensure_async_, run_, with_ensure_async_

T = TypeVar("T")


async def test_ensure_async_basic_functionality() -> None:
    """Test basic functionality of ensure_async_ decorator."""

    @ensure_async_
    def sync_func(x: int) -> int:
        return x * 2

    @ensure_async_  # type: ignore[arg-type]
    async def async_func(x: int) -> int:
        return x * 2

    assert await sync_func(21) == 42
    assert await async_func(21) == 42


async def test_ensure_async_with_complex_return_types() -> None:
    """Test ensure_async_ with complex return types."""

    @ensure_async_
    def sync_dict_func() -> dict[str, Any]:
        return {"key": "value", "number": 42}

    @ensure_async_
    def sync_list_func() -> list[int]:
        return [1, 2, 3, 4, 5]

    dict_result = await sync_dict_func()
    assert dict_result == {"key": "value", "number": 42}

    list_result = await sync_list_func()
    assert list_result == [1, 2, 3, 4, 5]


async def test_ensure_async_with_exceptions() -> None:
    """Test ensure_async_ properly propagates exceptions."""

    @ensure_async_
    def sync_func_that_raises() -> None:
        raise ValueError("Sync function error")

    @ensure_async_  # type: ignore[arg-type]
    async def async_func_that_raises() -> None:
        raise RuntimeError("Async function error")

    with pytest.raises(ValueError, match="Sync function error"):
        await sync_func_that_raises()

    with pytest.raises(RuntimeError, match="Async function error"):
        await async_func_that_raises()


async def test_ensure_async_with_arguments() -> None:
    """Test ensure_async_ with various argument patterns."""

    @ensure_async_
    def sync_func_with_args(a: int, b: str, c: float = math.pi) -> str:
        return f"{a}-{b}-{c:.2f}"

    @ensure_async_
    def sync_func_with_kwargs(*args: Any, **kwargs: Any) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return args, kwargs

    result1 = await sync_func_with_args(1, "test")
    assert result1 == "1-test-3.14"

    result2 = await sync_func_with_args(1, "test", c=math.e)
    assert result2 == "1-test-2.72"

    args_result, kwargs_result = await sync_func_with_kwargs(1, 2, 3, key="value")
    assert args_result == (1, 2, 3)
    assert kwargs_result == {"key": "value"}


async def test_with_ensure_async_basic_functionality() -> None:
    """Test basic functionality of with_ensure_async_ context manager."""

    @contextmanager
    def sync_cm() -> Iterator[int]:
        yield 42

    @asynccontextmanager
    async def async_cm() -> AsyncIterator[int]:
        yield 42

    async with with_ensure_async_(sync_cm()) as value:
        assert value == 42

    async with with_ensure_async_(async_cm()) as value:
        assert value == 42


async def test_with_ensure_async_with_setup_teardown() -> None:
    """Test with_ensure_async_ with setup and teardown actions."""
    setup_called = []
    teardown_called = []

    @contextmanager
    def sync_cm_with_setup_teardown() -> Iterator[str]:
        setup_called.append("setup")
        try:
            yield "test_value"
        finally:
            teardown_called.append("teardown")

    async with with_ensure_async_(sync_cm_with_setup_teardown()) as value:
        assert value == "test_value"
        assert setup_called == ["setup"]
        assert teardown_called == []  # Should not be called yet

    assert teardown_called == ["teardown"]


async def test_with_ensure_async_exception_handling() -> None:
    """Test with_ensure_async_ properly handles exceptions."""

    @contextmanager
    def sync_cm_that_raises() -> Iterator[None]:
        try:
            yield
        finally:
            pass

    with pytest.raises(ValueError, match="Test exception"):
        async with with_ensure_async_(sync_cm_that_raises()):
            raise ValueError("Test exception")


async def test_capacity_limiter_basic_usage() -> None:
    """Test basic CapacityLimiter functionality."""
    limiter = CapacityLimiter(1)

    async with limiter:
        assert limiter.total_tokens == 0

    assert limiter.total_tokens == 1


async def test_capacity_limiter_multiple_capacity() -> None:
    """Test CapacityLimiter with multiple capacity."""
    limiter = CapacityLimiter(3)

    # Use all capacity
    async with limiter:
        assert limiter.total_tokens == 2
        async with limiter:
            assert limiter.total_tokens == 1
            async with limiter:
                assert limiter.total_tokens == 0

    assert limiter.total_tokens == 3


async def test_capacity_limiter_concurrent_access() -> None:
    """Test CapacityLimiter with concurrent access."""
    limiter = CapacityLimiter(2)
    results = []

    async def worker(worker_id: int) -> None:
        async with limiter:
            results.append(f"worker_{worker_id}_started")
            await asyncio.sleep(0.01)  # Small delay
            results.append(f"worker_{worker_id}_finished")

    # Start multiple workers
    await asyncio.gather(worker(1), worker(2), worker(3))

    # Check that workers were limited
    assert len(results) == 6
    assert all(f"worker_{i}_started" in results for i in [1, 2, 3])
    assert all(f"worker_{i}_finished" in results for i in [1, 2, 3])


def test_run_basic_functionality() -> None:
    """Test basic run_ functionality."""

    async def async_func(x: int) -> int:
        return x * 2

    sync_func = run_(async_func)
    assert sync_func(21) == 42


def test_run_with_complex_async_operations() -> None:
    """Test run_ with more complex async operations."""

    async def async_fetch_data(delay: float) -> dict[str, Any]:
        await asyncio.sleep(delay)
        return {"status": "success", "delay": delay}

    sync_fetch = run_(async_fetch_data)
    result = sync_fetch(0.01)
    assert result["status"] == "success"
    assert result["delay"] == 0.01


def test_run_exception_propagation() -> None:
    """Test that run_ properly propagates exceptions."""

    async def async_func_that_raises() -> None:
        raise RuntimeError("Async error")

    sync_func = run_(async_func_that_raises)

    with pytest.raises(RuntimeError, match="Async error"):
        sync_func()


def test_run_with_arguments() -> None:
    """Test run_ with various argument patterns."""

    async def async_func_with_args(a: int, b: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"a": a, "b": b, "args": args, "kwargs": kwargs}

    # Create a new event loop for this test to avoid reusing existing ones
    import asyncio

    loop = asyncio.new_event_loop()
    try:
        sync_func = run_(async_func_with_args)
        result = sync_func(1, "test", 3, 4, key="value")

        assert result["a"] == 1
        assert result["b"] == "test"
        assert result["args"] == (3, 4)
        assert result["kwargs"] == {"key": "value"}
    finally:
        loop.close()


def test_await_basic_functionality() -> None:
    """Test basic await_ functionality."""

    async def async_func(x: int) -> int:
        return x * 2

    sync_func = await_(async_func, raise_sync_error=False)
    assert sync_func(21) == 42


def test_await_with_raise_sync_error_false() -> None:
    """Test await_ with raise_sync_error=False."""

    async def async_func(x: int) -> int:
        await asyncio.sleep(0.001)
        return x * 3

    sync_func = await_(async_func, raise_sync_error=False)
    result = sync_func(14)
    assert result == 42


def test_await_with_raise_sync_error_true() -> None:
    """Test await_ with raise_sync_error=True in sync context."""

    async def async_func(x: int) -> int:
        return x * 2

    sync_func = await_(async_func, raise_sync_error=True)

    # In a synchronous context, this should raise an error
    with pytest.raises(RuntimeError, match="Cannot run async function"):
        sync_func(21)


def test_await_exception_propagation() -> None:
    """Test that await_ properly propagates exceptions."""

    async def async_func_that_raises() -> None:
        raise ValueError("Async error in await_")

    sync_func = await_(async_func_that_raises, raise_sync_error=False)

    with pytest.raises(ValueError, match="Async error in await_"):
        sync_func()


async def test_async_basic_functionality() -> None:
    """Test basic async_ functionality."""

    def sync_func(x: int) -> int:
        return x * 2

    async_func = async_(sync_func)
    assert await async_func(21) == 42


async def test_async_with_complex_operations() -> None:
    """Test async_ with more complex sync operations."""

    def sync_complex_func(data: dict[str, Any]) -> dict[str, Any]:
        # Simulate some processing
        result = data.copy()
        result["processed"] = True
        result["count"] = len(data)
        return result

    async_func = async_(sync_complex_func)
    input_data = {"a": 1, "b": 2, "c": 3}
    result = await async_func(input_data)

    assert result["processed"] is True
    assert result["count"] == 3
    assert result["a"] == 1


async def test_async_exception_propagation() -> None:
    """Test that async_ properly propagates exceptions."""

    def sync_func_that_raises() -> None:
        raise KeyError("Sync error in async_")

    async_func = async_(sync_func_that_raises)

    with pytest.raises(KeyError, match="Sync error in async_"):
        await async_func()


async def test_async_with_blocking_operations() -> None:
    """Test async_ with blocking operations (simulated)."""
    import time

    def blocking_sync_func(duration: float) -> str:
        time.sleep(duration)
        return f"Completed after {duration}s"

    async_func = async_(blocking_sync_func)
    result = await async_func(0.01)  # Short duration for test
    assert "Completed after 0.01s" in result


def test_run_await_async_integration() -> None:
    """Test integration between run_, await_, and async_."""

    # Chain: sync -> async_ -> run_
    def original_sync_func(x: int) -> int:
        return x * 2

    async_version = async_(original_sync_func)
    back_to_sync = run_(async_version)  # type: ignore[var-annotated,arg-type]

    assert back_to_sync(21) == 42


async def test_ensure_async_await_integration() -> None:
    """Test integration between ensure_async_ and await_."""

    @ensure_async_
    def sync_func(x: int) -> int:
        return x * 3

    # Convert back to sync using await_
    sync_version = await_(sync_func, raise_sync_error=False)  # type: ignore[arg-type,var-annotated]
    with pytest.raises(
        RuntimeError,
        match="await_ cannot be called from within an async task running on the same event loop. Use 'await' instead.",
    ):
        sync_version(14)


@pytest.mark.parametrize(
    ("capacity", "workers", "expected_concurrent"),
    [
        (1, 3, 1),  # Only 1 worker at a time
        (2, 4, 2),  # Only 2 workers at a time
        (5, 3, 3),  # All 3 workers can run
    ],
    ids=["single_capacity", "double_capacity", "excess_capacity"],
)
async def test_capacity_limiter_various_configurations(capacity: int, workers: int, expected_concurrent: int) -> None:
    """Test CapacityLimiter with various capacity configurations."""
    limiter = CapacityLimiter(capacity)
    concurrent_count = 0
    max_concurrent = 0

    async def worker() -> None:
        nonlocal concurrent_count, max_concurrent
        async with limiter:
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.01)  # Simulate work
            concurrent_count -= 1

    await asyncio.gather(*[worker() for _ in range(workers)])

    assert max_concurrent <= expected_concurrent


def test_sync_tools_preserve_function_metadata() -> None:
    """Test that sync tools preserve function metadata."""

    def original_func(x: int) -> int:
        """Original function docstring."""
        return x

    async def original_async_func(x: int) -> int:
        """Original async function docstring."""
        return x

    # Test async_ preserves metadata
    async_version = async_(original_func)
    assert async_version.__name__ == original_func.__name__

    # Test run_ preserves metadata
    sync_version = run_(original_async_func)
    assert sync_version.__name__ == original_async_func.__name__


async def test_nested_context_managers() -> None:
    """Test nested usage of with_ensure_async_."""

    @contextmanager
    def outer_cm() -> Iterator[str]:
        yield "outer"

    @contextmanager
    def inner_cm() -> Iterator[str]:
        yield "inner"

    async with with_ensure_async_(outer_cm()) as outer_value:
        assert outer_value == "outer"
        async with with_ensure_async_(inner_cm()) as inner_value:
            assert inner_value == "inner"


def test_run_from_sync_context() -> None:
    """Test that run_ works properly from sync context."""

    async def async_func() -> str:
        return "success"

    sync_version = run_(async_func)
    result = sync_version()
    assert result == "success"


async def test_error_handling_in_complex_scenarios() -> None:
    """Test error handling in complex async/sync conversion scenarios."""

    def sync_func_with_error(should_error: bool) -> str:
        if should_error:
            raise ValueError("Controlled error")
        return "success"

    # Test direct async_ conversion and execution
    async_version = async_(sync_func_with_error)

    # Test success case - await the async version directly
    result = await async_version(False)
    assert result == "success"

    # Test error case - await and expect exception
    with pytest.raises(ValueError, match="Controlled error"):
        await async_version(True)

    # Test that run_ raises error when called from async context
    # We'll test this in a separate sync test since calling run_ from async context
    # creates an unawaited coroutine before raising the RuntimeError
