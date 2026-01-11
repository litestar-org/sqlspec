"""Portal provider for calling async functions from synchronous contexts.

Provides a background thread with an event loop to execute async database operations
from sync frameworks like Flask. Based on the portal pattern from Advanced Alchemy.
"""

import asyncio
import atexit
import functools
import os
import queue
import threading
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

__all__ = ("Portal", "PortalManager", "PortalProvider", "get_global_portal")

logger = get_logger("sqlspec.utils.portal")

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
        self._loop_thread: threading.Thread | None = None
        self._ready_event: threading.Event = threading.Event()
        self._pid: int | None = None

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
        return self._loop_thread is not None and self._loop_thread.is_alive()

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
        if self._loop_thread is not None:
            logger.debug("Portal provider already started")
            return

        self._loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._loop_thread.start()
        self._ready_event.wait()
        self._pid = os.getpid()
        logger.debug("Portal provider started")

    def stop(self) -> None:
        """Stop the background thread and event loop.

        Gracefully shuts down the event loop and waits for thread to finish.
        Only closes the loop after the thread has terminated to avoid
        undefined behavior from closing a running loop.
        """
        if self._loop is None or self._loop_thread is None:
            logger.debug("Portal provider not running")
            return

        self._loop.call_soon_threadsafe(self._loop.stop)
        self._loop_thread.join(timeout=5)

        if self._loop_thread.is_alive():
            logger.warning("Portal thread did not stop within 5 seconds, skipping loop.close()")
        else:
            self._loop.close()

        self._loop = None
        self._loop_thread = None
        self._ready_event.clear()
        self._pid = None
        logger.debug("Portal provider stopped")

    def _run_event_loop(self) -> None:
        if self._loop is None:
            self._loop = asyncio.new_event_loop()

        asyncio.set_event_loop(self._loop)
        self._ready_event.set()
        self._loop.run_forever()

    @staticmethod
    async def _async_caller(
        func: "Callable[..., Coroutine[Any, Any, _R]]", args: "tuple[Any, ...]", kwargs: "dict[str, Any]"
    ) -> _R:
        result: _R = await func(*args, **kwargs)
        return result

    def call(self, func: "Callable[..., Coroutine[Any, Any, _R]]", *args: Any, **kwargs: Any) -> _R:
        """Call an async function from synchronous context.

        Executes the async function in the background event loop and blocks
        until the result is available or timeout is reached.

        Args:
            func: The async function to call.
            *args: Positional arguments to the function.
            **kwargs: Keyword arguments. Supports 'timeout' (float, default 300.0).

        Returns:
            Result of the async function.

        Raises:
            ImproperConfigurationError: If portal provider not started or timeout reached.

        """
        timeout: float = float(kwargs.pop("timeout", 300.0))
        if self._loop is None or not self.is_running:
            msg = "Portal provider not running. Call start() first."
            raise ImproperConfigurationError(msg)

        local_result_queue: queue.Queue[tuple[_R | None, Exception | None]] = queue.Queue()

        self._request_queue.put((func, args, kwargs, local_result_queue))

        self._loop.call_soon_threadsafe(self._process_request)

        try:
            result, exception = local_result_queue.get(timeout=timeout)
        except queue.Empty:
            msg = f"Portal call timed out after {timeout} seconds"
            raise ImproperConfigurationError(msg) from None

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
            **kwargs: Keyword arguments. Supports 'timeout' (float, default 300.0).

        Returns:
            Result of the async function.

        """
        return self._provider.call(func, *args, **kwargs)


class PortalManager:
    """Singleton manager for global portal instance.

    Provides a global portal for use by sync_tools and other utilities
    that need to call async functions from synchronous contexts without
    an existing event loop.

    Example:
        manager = PortalManager()
        portal = manager.get_or_create_portal()
        result = portal.call(some_async_function, arg1, arg2)

    """

    _instance: "ClassVar[PortalManager | None]" = None
    _singleton_lock: "ClassVar[threading.Lock]" = threading.Lock()
    _initialized: bool = False

    def __new__(cls) -> "PortalManager":
        """Get the singleton instance of PortalManager."""
        if cls._instance is None:
            with cls._singleton_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """Initialize the PortalManager singleton."""
        if hasattr(self, "_initialized") and self._initialized:
            return

        self._provider: PortalProvider | None = None
        self._portal: Portal | None = None
        self._lock = threading.Lock()
        self._pid: int | None = None
        self._atexit_registered: bool = False
        self._initialized = True

    def get_or_create_portal(self) -> Portal:
        """Get or create the global portal instance.

        Lazily creates and starts the portal provider on first access.
        Thread-safe via locking. Registers an atexit handler for cleanup.

        Returns:
            Global portal instance.

        """
        current_pid = os.getpid()
        if self._needs_restart(current_pid):
            with self._lock:
                if self._needs_restart(current_pid):
                    if self._provider is not None:
                        self._provider.stop()
                    self._provider = PortalProvider()
                    self._provider.start()
                    self._portal = Portal(self._provider)
                    self._pid = current_pid
                    self._register_atexit()
                    logger.debug("Global portal provider created and started")

        return cast("Portal", self._portal)

    def _register_atexit(self) -> None:
        """Register atexit handler for graceful shutdown.

        Only registers once per process to avoid duplicate cleanup.
        """
        if not self._atexit_registered:
            atexit.register(self._atexit_cleanup)
            self._atexit_registered = True
            logger.debug("Portal atexit handler registered")

    def _atexit_cleanup(self) -> None:
        """Cleanup handler called at interpreter shutdown.

        Gracefully stops the portal provider to ensure pending
        async operations complete before the process exits.
        """
        if self._provider is not None and self._provider.is_running:
            logger.debug("Portal atexit cleanup: stopping provider")
            self.stop()

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
            self._pid = None
            logger.debug("Global portal provider stopped")

    def _needs_restart(self, current_pid: int) -> bool:
        provider_missing = self._provider is None or not self._provider.is_running
        portal_missing = self._portal is None
        pid_changed = self._pid is not None and self._pid != current_pid
        return portal_missing or provider_missing or pid_changed

    @classmethod
    def get_instance(cls) -> "PortalManager":
        """Get the singleton instance."""
        return cls()


def get_global_portal() -> Portal:
    """Get the global portal instance for async-to-sync bridging.

    Convenience function that creates and returns the singleton portal.
    Used by sync_tools and other utilities.

    Returns:
        Global portal instance.

    """
    return PortalManager().get_or_create_portal()
