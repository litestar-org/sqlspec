"""Unit tests for portal provider and portal manager."""

import asyncio
from collections.abc import Callable, Coroutine, Generator
from typing import Any

import pytest

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.portal import Portal, PortalManager, PortalProvider, get_global_portal

pytestmark = pytest.mark.xdist_group("portal")


@pytest.fixture(autouse=True)
def _cleanup_portal_manager() -> Generator[None, None, None]:
    """Clean up the portal manager after each test."""
    yield
    manager = PortalManager()
    if manager.is_running:
        manager.stop()

    # Reset singleton instance
    PortalManager._instance = None  # pyright: ignore[reportPrivateUsage]


@pytest.fixture()
def async_add() -> Callable[[int, int], Coroutine[Any, Any, int]]:
    """Fixture providing simple async addition function."""

    async def sample_async_function(x: int, y: int) -> int:
        await asyncio.sleep(0.01)
        return x + y

    return sample_async_function


@pytest.fixture()
def async_multiply() -> Callable[[int], Coroutine[Any, Any, int]]:
    """Fixture providing async multiplication function."""

    async def sample_async_function(x: int) -> int:
        await asyncio.sleep(0.01)
        return x * 2

    return sample_async_function


def test_portal_provider_not_singleton() -> None:
    """PortalProvider instances are independent, not singleton."""
    provider1 = PortalProvider()
    provider2 = PortalProvider()
    assert provider1 is not provider2


def test_portal_provider_start_stop() -> None:
    """PortalProvider can be started and stopped."""
    provider = PortalProvider()
    assert not provider.is_running
    assert not provider.is_ready

    provider.start()
    assert provider.is_running
    assert provider.is_ready

    provider.stop()
    assert not provider.is_running


def test_portal_provider_start_already_started() -> None:
    """Starting already-started provider is idempotent."""
    provider = PortalProvider()
    provider.start()
    assert provider.is_running

    provider.start()
    assert provider.is_running

    provider.stop()


def test_portal_provider_stop_not_started() -> None:
    """Stopping non-started provider is safe."""
    provider = PortalProvider()
    provider.stop()
    assert not provider.is_running


def test_portal_provider_call(async_multiply: Callable[[int], Coroutine[Any, Any, int]]) -> None:
    """PortalProvider.call executes async function from sync context."""
    provider = PortalProvider()
    provider.start()

    result = provider.call(async_multiply, 5)
    assert result == 10

    provider.stop()


def test_portal_provider_call_after_stop(async_add: Callable[[int, int], Coroutine[Any, Any, int]]) -> None:
    """PortalProvider.call raises once the provider has been stopped."""
    provider = PortalProvider()
    provider.start()
    provider.stop()

    with pytest.raises(ImproperConfigurationError, match="Portal provider not running"):
        provider.call(async_add, 1, 2)


def test_portal_provider_call_with_kwargs(async_add: Callable[[int, int], Coroutine[Any, Any, int]]) -> None:
    """PortalProvider.call supports keyword arguments."""
    provider = PortalProvider()
    provider.start()

    result = provider.call(async_add, x=5, y=3)
    assert result == 8

    result = provider.call(async_add, 10, y=20)
    assert result == 30

    provider.stop()


def test_portal_provider_call_not_started() -> None:
    """PortalProvider.call raises error when not started."""
    provider = PortalProvider()

    async def dummy() -> int:
        return 42

    with pytest.raises(ImproperConfigurationError, match="Portal provider not running"):
        provider.call(dummy)


def test_portal_provider_call_exception() -> None:
    """PortalProvider.call propagates exceptions from async function."""

    async def faulty_async_function() -> None:
        raise ValueError("Intentional error")

    provider = PortalProvider()
    provider.start()

    with pytest.raises(ValueError, match="Intentional error"):
        provider.call(faulty_async_function)

    provider.stop()


def test_portal_provider_multiple_calls(async_multiply: Callable[[int], Coroutine[Any, Any, int]]) -> None:
    """PortalProvider can handle multiple sequential calls."""
    provider = PortalProvider()
    provider.start()

    result1 = provider.call(async_multiply, 3)
    result2 = provider.call(async_multiply, 7)
    result3 = provider.call(async_multiply, 10)

    assert result1 == 6
    assert result2 == 14
    assert result3 == 20

    provider.stop()


def test_portal_call(async_multiply: Callable[[int], Coroutine[Any, Any, int]]) -> None:
    """Portal.call delegates to provider."""
    provider = PortalProvider()
    portal = Portal(provider)
    provider.start()

    result = portal.call(async_multiply, 3)
    assert result == 6

    provider.stop()


def test_portal_provider_property() -> None:
    """PortalProvider.portal property returns Portal instance."""
    provider = PortalProvider()
    portal = provider.portal

    assert isinstance(portal, Portal)


def test_portal_manager_singleton() -> None:
    """PortalManager is a singleton."""
    manager1 = PortalManager()
    manager2 = PortalManager()

    assert manager1 is manager2


def test_portal_manager_get_or_create_portal() -> None:
    """PortalManager.get_or_create_portal returns global portal."""
    manager = PortalManager()
    portal1 = manager.get_or_create_portal()
    portal2 = manager.get_or_create_portal()

    assert portal1 is portal2
    assert manager.is_running

    manager.stop()


def test_portal_manager_lazy_initialization() -> None:
    """PortalManager only starts portal on first get_or_create_portal call."""
    manager = PortalManager()

    assert not manager.is_running

    portal = manager.get_or_create_portal()

    assert manager.is_running
    assert isinstance(portal, Portal)

    manager.stop()


def test_portal_manager_restarts_after_pid_change(monkeypatch: Any) -> None:
    """PortalManager rebuilds the portal when it detects a PID change."""
    manager = PortalManager()
    portal1 = manager.get_or_create_portal()
    provider1 = manager._provider  # type: ignore[attr-defined]

    assert manager.is_running
    assert provider1 is not None

    monkeypatch.setattr(manager, "_pid", -1)

    portal2 = manager.get_or_create_portal()
    provider2 = manager._provider  # type: ignore[attr-defined]

    assert portal2 is not portal1
    assert provider2 is not provider1

    manager.stop()


def test_portal_manager_stop() -> None:
    """PortalManager.stop cleans up portal provider."""
    manager = PortalManager()
    manager.get_or_create_portal()

    assert manager.is_running

    manager.stop()

    assert not manager.is_running


def test_get_global_portal() -> None:
    """get_global_portal returns singleton portal."""
    portal1 = get_global_portal()
    portal2 = get_global_portal()

    assert portal1 is portal2

    manager = PortalManager()
    assert manager.is_running

    manager.stop()


def test_get_global_portal_functional(async_add: Callable[[int, int], Coroutine[Any, Any, int]]) -> None:
    """get_global_portal provides working portal."""
    portal = get_global_portal()
    result = portal.call(async_add, 5, 3)

    assert result == 8

    manager = PortalManager()
    manager.stop()


def test_portal_loop_property() -> None:
    """PortalProvider.loop property returns event loop."""
    provider = PortalProvider()

    with pytest.raises(ImproperConfigurationError, match="Portal provider not started"):
        _ = provider.loop

    provider.start()
    loop = provider.loop

    assert isinstance(loop, asyncio.AbstractEventLoop)
    assert loop.is_running()

    provider.stop()


def test_portal_concurrent_calls(async_add: Callable[[int, int], Coroutine[Any, Any, int]]) -> None:
    """PortalProvider handles multiple calls correctly (sequential in practice)."""
    provider = PortalProvider()
    provider.start()

    results = []
    for i in range(10):
        result = provider.call(async_add, i, i)
        results.append(result)

    assert results == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    provider.stop()


def test_portal_with_complex_return_type() -> None:
    """PortalProvider handles complex return types."""

    async def fetch_data() -> dict[str, Any]:
        await asyncio.sleep(0.01)
        return {"status": "success", "data": [1, 2, 3], "count": 3}

    provider = PortalProvider()
    provider.start()

    result = provider.call(fetch_data)

    assert result == {"status": "success", "data": [1, 2, 3], "count": 3}

    provider.stop()


def test_portal_thread_safety() -> None:
    """PortalManager is thread-safe (basic check)."""
    import threading

    manager = PortalManager()
    portals = []

    def get_portal() -> None:
        portal = manager.get_or_create_portal()
        portals.append(portal)

    threads = [threading.Thread(target=get_portal) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(portals) == 10
    assert all(p is portals[0] for p in portals)

    manager.stop()


def test_portal_manager_atexit_registration() -> None:
    """PortalManager registers atexit handler on portal creation."""
    manager = PortalManager()

    assert not manager._atexit_registered  # pyright: ignore[reportPrivateUsage]

    manager.get_or_create_portal()

    assert manager._atexit_registered  # pyright: ignore[reportPrivateUsage]
    assert manager.is_running

    manager.stop()


def test_portal_manager_atexit_cleanup() -> None:
    """PortalManager._atexit_cleanup stops running provider."""
    manager = PortalManager()
    manager.get_or_create_portal()

    assert manager.is_running

    manager._atexit_cleanup()  # pyright: ignore[reportPrivateUsage]

    assert not manager.is_running


def test_portal_manager_atexit_cleanup_noop_when_stopped() -> None:
    """PortalManager._atexit_cleanup is no-op when already stopped."""
    manager = PortalManager()
    manager.get_or_create_portal()
    manager.stop()

    assert not manager.is_running

    manager._atexit_cleanup()  # pyright: ignore[reportPrivateUsage]

    assert not manager.is_running


def test_portal_call_timeout() -> None:
    """PortalProvider.call raises error on timeout."""

    async def slow_function() -> int:
        await asyncio.sleep(10)
        return 42

    provider = PortalProvider()
    provider.start()

    with pytest.raises(ImproperConfigurationError, match="timed out after"):
        provider.call(slow_function, timeout=0.1)

    provider.stop()
