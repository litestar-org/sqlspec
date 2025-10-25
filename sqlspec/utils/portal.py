"""Portal provider for calling async functions from synchronous contexts.

Provides a background thread with an event loop to execute async database operations
from sync frameworks like Flask. Based on the portal pattern from Advanced Alchemy.
"""

import asyncio
import functools
import queue
import threading
from typing import TYPE_CHECKING, Any, TypeVar

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.singleton import SingletonMeta

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

__all__ = ("Portal", "PortalManager", "PortalProvider", "get_global_portal")

logger = get_logger("utils.portal")

_R = TypeVar("_R")


class PortalProvider:
    """Manages a background thread with event loop for async operations.

    Creates a daemon thread running an event loop to execute async functions
    from synchronous contexts (Flask routes, etc.).
    """

    def __init__(self) -> None:
        """Initialize the PortalProvider."""
        self._request_queue: queue.Queue[
            tuple[
                Callable[..., Coroutine[Any, Any, Any]],
                tuple[Any, ...],
                dict[str, Any],
                queue.Queue[tuple[Any | None, Exception | None]],
            ]
        ] = queue.Queue()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready_event: threading.Event = threading.Event()

    @property
    def portal(self) -> "Portal":
        """The portal instance for calling async functions.

        Returns:
            Portal instance.
        """
        return Portal(self)

    @property
    def is_running(self) -> bool:
        """Check if portal provider is running.

        Returns:
            True if thread is alive, False otherwise.
        """
        return self._thread is not None and self._thread.is_alive()

    @property
    def is_ready(self) -> bool:
        """Check if portal provider is ready.

        Returns:
            True if ready event is set, False otherwise.
        """
        return self._ready_event.is_set()

    @property
    def loop(self) -> "asyncio.AbstractEventLoop":
        """Get the event loop.

        Returns:
            The event loop.

        Raises:
            ImproperConfigurationError: If portal provider not started.
        """
        if self._loop is None:
            msg = "Portal provider not started. Call start() first."
            raise ImproperConfigurationError(msg)
        return self._loop

    def start(self) -> None:
        """Start the background thread and event loop.

        Creates a daemon thread running an event loop for async operations.
        """
        if self._thread is not None:
            logger.debug("Portal provider already started")
            return

        self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._thread.start()
        self._ready_event.wait()
        logger.debug("Portal provider started")

    def stop(self) -> None:
        """Stop the background thread and event loop.

        Gracefully shuts down the event loop and waits for thread to finish.
        """
        if self._loop is None or self._thread is None:
            logger.debug("Portal provider not running")
            return

        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)

        if self._thread.is_alive():
            logger.warning("Portal thread did not stop within 5 seconds")

        self._loop.close()
        self._loop = None
        self._thread = None
        self._ready_event.clear()
        logger.debug("Portal provider stopped")

    def _run_event_loop(self) -> None:
        """Main function of the background thread.

        Creates event loop and runs forever until stopped.
        """
        if self._loop is None:
            self._loop = asyncio.new_event_loop()

        asyncio.set_event_loop(self._loop)
        self._ready_event.set()
        self._loop.run_forever()

    @staticmethod
    async def _async_caller(
        func: "Callable[..., Coroutine[Any, Any, _R]]", args: "tuple[Any, ...]", kwargs: "dict[str, Any]"
    ) -> _R:
        """Wrapper to run async function.

        Args:
            func: The async function to call.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            Result of the async function.
        """
        result: _R = await func(*args, **kwargs)
        return result

    def call(self, func: "Callable[..., Coroutine[Any, Any, _R]]", *args: Any, **kwargs: Any) -> _R:
        """Call an async function from synchronous context.

        Executes the async function in the background event loop and blocks
        until the result is available.

        Args:
            func: The async function to call.
            *args: Positional arguments to the function.
            **kwargs: Keyword arguments to the function.

        Returns:
            Result of the async function.

        Raises:
            ImproperConfigurationError: If portal provider not started.
        """
        if self._loop is None:
            msg = "Portal provider not started. Call start() first."
            raise ImproperConfigurationError(msg)

        local_result_queue: queue.Queue[tuple[_R | None, Exception | None]] = queue.Queue()

        self._request_queue.put((func, args, kwargs, local_result_queue))

        self._loop.call_soon_threadsafe(self._process_request)

        result, exception = local_result_queue.get()

        if exception:
            raise exception
        return result  # type: ignore[return-value]

    def _process_request(self) -> None:
        """Process a request from the request queue in the event loop."""
        if self._loop is None:
            return

        if not self._request_queue.empty():
            func, args, kwargs, local_result_queue = self._request_queue.get()
            future = asyncio.run_coroutine_threadsafe(self._async_caller(func, args, kwargs), self._loop)

            future.add_done_callback(
                functools.partial(self._handle_future_result, local_result_queue=local_result_queue)  # pyright: ignore[reportArgumentType]
            )

    @staticmethod
    def _handle_future_result(
        future: "asyncio.Future[Any]", local_result_queue: "queue.Queue[tuple[Any | None, Exception | None]]"
    ) -> None:
        """Handle result or exception from completed future.

        Args:
            future: The completed future.
            local_result_queue: Queue to put result in.
        """
        try:
            result = future.result()
            local_result_queue.put((result, None))
        except Exception as exc:
            local_result_queue.put((None, exc))


class Portal:
    """Portal for calling async functions using PortalProvider."""

    def __init__(self, provider: "PortalProvider") -> None:
        """Initialize Portal with provider.

        Args:
            provider: The portal provider instance.
        """
        self._provider = provider

    def call(self, func: "Callable[..., Coroutine[Any, Any, _R]]", *args: Any, **kwargs: Any) -> _R:
        """Call an async function using the portal provider.

        Args:
            func: The async function to call.
            *args: Positional arguments to the function.
            **kwargs: Keyword arguments to the function.

        Returns:
            Result of the async function.
        """
        return self._provider.call(func, *args, **kwargs)


class PortalManager(metaclass=SingletonMeta):
    """Singleton manager for global portal instance.

    Provides a global portal for use by sync_tools and other utilities
    that need to call async functions from synchronous contexts without
    an existing event loop.

    Example:
        manager = PortalManager()
        portal = manager.get_or_create_portal()
        result = portal.call(some_async_function, arg1, arg2)
    """

    def __init__(self) -> None:
        """Initialize the PortalManager singleton."""
        self._provider: PortalProvider | None = None
        self._portal: Portal | None = None
        self._lock = threading.Lock()

    def get_or_create_portal(self) -> Portal:
        """Get or create the global portal instance.

        Lazily creates and starts the portal provider on first access.
        Thread-safe via locking.

        Returns:
            Global portal instance.
        """
        if self._portal is None:
            with self._lock:
                if self._portal is None:
                    self._provider = PortalProvider()
                    self._provider.start()
                    self._portal = Portal(self._provider)
                    logger.debug("Global portal provider created and started")

        return self._portal

    @property
    def is_running(self) -> bool:
        """Check if global portal is running.

        Returns:
            True if portal provider exists and is running, False otherwise.
        """
        return self._provider is not None and self._provider.is_running

    def stop(self) -> None:
        """Stop the global portal provider.

        Should typically only be called during application shutdown.
        """
        if self._provider is not None:
            self._provider.stop()
            self._provider = None
            self._portal = None
            logger.debug("Global portal provider stopped")


def get_global_portal() -> Portal:
    """Get the global portal instance for async-to-sync bridging.

    Convenience function that creates and returns the singleton portal.
    Used by sync_tools and other utilities.

    Returns:
        Global portal instance.
    """
    manager = PortalManager()
    return manager.get_or_create_portal()
