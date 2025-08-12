"""Tests for portal provider functionality."""

import asyncio
import threading

import pytest

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.portal import Portal, PortalProvider, PortalProviderSingleton

# Portal Provider Singleton Tests


def test_singleton_same_loop() -> None:
    """Test that singleton returns same instance for same loop."""
    provider1 = PortalProvider()
    provider2 = PortalProvider()

    assert provider1 is provider2


def test_singleton_different_loops() -> None:
    """Test that singleton returns different instances for different loops."""
    loop1 = asyncio.new_event_loop()
    loop2 = asyncio.new_event_loop()

    try:
        provider1 = PortalProvider(loop=loop1)
        provider2 = PortalProvider(loop=loop2)

        assert provider1 is not provider2
    finally:
        loop1.close()
        loop2.close()


def test_singleton_clear_instances() -> None:
    """Test clearing singleton instances."""
    # Clear instances for testing
    PortalProviderSingleton._instances.clear()

    provider1 = PortalProvider()
    provider2 = PortalProvider()

    assert provider1 is provider2
    assert len(PortalProviderSingleton._instances) == 1


# Portal Provider Tests


def test_portal_provider_initialization() -> None:
    """Test portal provider initializes correctly."""
    provider = PortalProvider()

    assert provider._loop is None
    assert provider._thread is None
    assert not provider.is_running
    assert not provider.is_ready


def test_portal_provider_with_loop() -> None:
    """Test portal provider initialization with provided loop."""
    loop = asyncio.new_event_loop()
    try:
        provider = PortalProvider(loop=loop)
        assert provider._loop is loop
    finally:
        loop.close()


def test_portal_property() -> None:
    """Test portal property returns Portal instance."""
    provider = PortalProvider()
    portal = provider.portal

    assert isinstance(portal, Portal)
    assert portal._provider is provider


def test_loop_property_not_started() -> None:
    """Test loop property raises error when not started."""
    provider = PortalProvider()

    with pytest.raises(ImproperConfigurationError, match="The PortalProvider is not started"):
        _ = provider.loop


def test_start_portal_success() -> None:
    """Test successful portal start."""
    provider = PortalProvider()
    provider.start()

    try:
        assert provider.is_running
        assert provider.is_ready
        assert provider._thread is not None
        assert provider._loop is not None
        assert provider._thread.is_alive()
    finally:
        provider.stop()


def test_start_portal_already_started() -> None:
    """Test starting already started portal shows warning."""
    provider = PortalProvider()
    provider.start()

    try:
        with pytest.warns(UserWarning, match="PortalProvider already started"):
            provider.start()
    finally:
        provider.stop()


def test_stop_portal_success() -> None:
    """Test successful portal stop."""
    provider = PortalProvider()
    provider.start()

    assert provider.is_running

    provider.stop()

    assert not provider.is_running
    assert not provider.is_ready
    assert provider._loop is None
    assert provider._thread is None


def test_stop_portal_not_started() -> None:
    """Test stopping a portal that's not started."""
    provider = PortalProvider()

    # Should not raise exception
    provider.stop()

    assert not provider.is_running
    assert not provider.is_ready


def test_call_success() -> None:
    """Test successful async function call through portal."""
    provider = PortalProvider()
    provider.start()

    try:

        async def test_func(arg1: str, arg2: int = 42) -> str:
            await asyncio.sleep(0.01)  # Simulate async work
            return f"{arg1}_{arg2}"

        result = provider.call(test_func, "hello", arg2=100)

        assert result == "hello_100"
    finally:
        provider.stop()


def test_call_portal_not_started() -> None:
    """Test calling function when portal is not started."""
    provider = PortalProvider()

    async def test_func() -> str:
        return "test"

    with pytest.raises(ImproperConfigurationError, match="The PortalProvider is not started"):
        provider.call(test_func)


def test_call_with_exception() -> None:
    """Test calling function that raises exception."""
    provider = PortalProvider()
    provider.start()

    try:

        async def test_func() -> None:
            await asyncio.sleep(0.01)
            raise ValueError("Test exception")

        with pytest.raises(ValueError, match="Test exception"):
            provider.call(test_func)
    finally:
        provider.stop()


def test_concurrent_calls() -> None:
    """Test multiple concurrent calls through portal."""
    provider = PortalProvider()
    provider.start()

    try:

        async def test_func(value: int) -> int:
            await asyncio.sleep(0.01)
            return value * 2

        # Test concurrent calls from multiple threads
        results = []
        threads = []

        def call_func(val: int) -> None:
            result = provider.call(test_func, val)
            results.append(result)

        for i in range(5):
            thread = threading.Thread(target=call_func, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert sorted(results) == [0, 2, 4, 6, 8]
    finally:
        provider.stop()


# Portal Tests


def test_portal_initialization() -> None:
    """Test portal initializes correctly."""
    provider = PortalProvider()
    portal = Portal(provider)

    assert portal._provider is provider


def test_portal_call_delegates_to_provider() -> None:
    """Test that portal.call delegates to provider.call."""
    provider = PortalProvider()
    provider.start()

    try:
        portal = Portal(provider)

        async def test_func(value: str) -> str:
            await asyncio.sleep(0.01)
            return f"result_{value}"

        result = portal.call(test_func, "test")

        assert result == "result_test"
    finally:
        provider.stop()


def test_portal_call_not_started() -> None:
    """Test portal call when provider not started."""
    provider = PortalProvider()
    portal = Portal(provider)

    async def test_func() -> str:
        return "test"

    with pytest.raises(ImproperConfigurationError):
        portal.call(test_func)


# Integration Tests


def test_complex_async_function() -> None:
    """Test calling complex async function with multiple operations."""
    provider = PortalProvider()
    provider.start()

    try:

        async def complex_func(items: list[int]) -> dict[str, int]:
            result = {}
            for item in items:
                await asyncio.sleep(0.001)  # Simulate async work
                result[f"item_{item}"] = item * item
            return result

        result = provider.call(complex_func, [1, 2, 3, 4])
        expected = {"item_1": 1, "item_2": 4, "item_3": 9, "item_4": 16}

        assert result == expected
    finally:
        provider.stop()


def test_async_context_manager_simulation() -> None:
    """Test async function that simulates context manager behavior."""
    provider = PortalProvider()
    provider.start()

    try:

        async def simulate_context_manager() -> str:
            # Simulate acquiring resource
            await asyncio.sleep(0.01)
            try:
                # Simulate work with resource
                await asyncio.sleep(0.01)
                return "resource_result"
            finally:
                # Simulate cleanup
                await asyncio.sleep(0.01)

        result = provider.call(simulate_context_manager)
        assert result == "resource_result"
    finally:
        provider.stop()


def test_portal_lifecycle_management() -> None:
    """Test proper lifecycle management of portal."""
    provider = PortalProvider()

    # Initially not running
    assert not provider.is_running
    assert not provider.is_ready

    # Start portal
    provider.start()
    assert provider.is_running
    assert provider.is_ready

    # Use portal
    async def test_func() -> str:
        return "success"

    result = provider.call(test_func)
    assert result == "success"

    # Stop portal
    provider.stop()
    assert not provider.is_running
    assert not provider.is_ready

    # Cannot use after stop
    with pytest.raises(ImproperConfigurationError):
        provider.call(test_func)


def test_error_propagation() -> None:
    """Test that errors are properly propagated from async functions."""
    provider = PortalProvider()
    provider.start()

    try:

        async def error_func(error_type: type[Exception], message: str) -> None:
            await asyncio.sleep(0.01)
            raise error_type(message)

        # Test different exception types
        with pytest.raises(ValueError, match="Value error"):
            provider.call(error_func, ValueError, "Value error")

        with pytest.raises(RuntimeError, match="Runtime error"):
            provider.call(error_func, RuntimeError, "Runtime error")

        with pytest.raises(KeyError, match="Key error"):
            provider.call(error_func, KeyError, "Key error")
    finally:
        provider.stop()


# Edge Cases


def test_stop_before_start() -> None:
    """Test stopping portal before starting."""
    provider = PortalProvider()
    provider.stop()  # Should not raise exception

    assert not provider.is_running
    assert not provider.is_ready


def test_multiple_stops() -> None:
    """Test stopping portal multiple times."""
    provider = PortalProvider()
    provider.start()

    provider.stop()
    provider.stop()  # Should not raise exception

    assert not provider.is_running


def test_rapid_start_stop_cycles() -> None:
    """Test rapid start/stop cycles."""
    provider = PortalProvider()

    for _ in range(3):
        provider.start()
        assert provider.is_running

        async def test_func() -> str:
            return "test"

        result = provider.call(test_func)
        assert result == "test"

        provider.stop()
        assert not provider.is_running


def test_call_with_keyword_only_args() -> None:
    """Test calling function with keyword-only arguments."""
    provider = PortalProvider()
    provider.start()

    try:

        async def test_func(*, name: str, value: int) -> str:
            await asyncio.sleep(0.01)
            return f"{name}={value}"

        result = provider.call(test_func, name="test", value=42)
        assert result == "test=42"
    finally:
        provider.stop()


def test_call_returns_none() -> None:
    """Test calling function that returns None."""
    provider = PortalProvider()
    provider.start()

    try:

        async def test_func() -> None:
            await asyncio.sleep(0.01)
            return

        result = provider.call(test_func)
        assert result is None
    finally:
        provider.stop()


def test_global_portal_provider_reset() -> None:
    """Test resetting the global portal provider for testing."""
    import sqlspec.utils.portal as portal_module

    # Store original
    original = portal_module.PortalProviderSingleton._instances.copy()

    try:
        # Reset instances
        portal_module.PortalProviderSingleton._instances.clear()

        # Get new instance
        provider1 = PortalProvider()
        provider2 = PortalProvider()

        assert provider1 is provider2
        assert len(portal_module.PortalProviderSingleton._instances) == 1

    finally:
        # Restore original
        portal_module.PortalProviderSingleton._instances.clear()
        portal_module.PortalProviderSingleton._instances.update(original)
