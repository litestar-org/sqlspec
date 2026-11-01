"""Exclusive PostgreSQL ADBC connection leases."""

import contextlib
import math
import os
import threading
import time
from typing import TYPE_CHECKING

from sqlspec.exceptions import ConnectionTimeoutError, DatabaseConnectionError, ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.adbc._typing import AdbcConnection

__all__ = ("AdbcConnectionPool",)


class AdbcConnectionPool:
    """Own one original seed and bounded exclusive native clone leases.

    Native creation, cloning and cleanup run outside the bookkeeping condition.
    Failed cleanup retains ownership until a subsequent close retries it.
    """

    __slots__ = (
        "_clones",
        "_condition",
        "_create_connection",
        "_max_size",
        "_pid",
        "_reservations",
        "_seed",
        "_seed_lock",
        "_state",
        "_timeout",
    )

    def __init__(
        self, create_connection: "Callable[[], AdbcConnection]", *, max_size: int = 4, timeout: float = 30.0
    ) -> None:
        """Configure a lazy pool without opening any native connections."""
        if isinstance(max_size, bool) or not isinstance(max_size, int) or max_size <= 0:
            msg = "ADBC pool max_size must be a positive integer."
            raise ImproperConfigurationError(msg)
        if not math.isfinite(timeout) or timeout < 0:
            msg = "ADBC pool timeout must be finite and nonnegative."
            raise ImproperConfigurationError(msg)
        self._create_connection = create_connection
        self._max_size = max_size
        self._timeout = timeout
        self._pid = os.getpid()
        self._condition = threading.Condition(threading.Lock())
        self._seed_lock = threading.Lock()
        self._seed: AdbcConnection | None = None
        self._clones: dict[int, tuple[AdbcConnection, str]] = {}
        self._reservations = 0
        self._state = "OPEN"

    def acquire(self) -> "AdbcConnection":
        """Acquire an exclusive clone or wait up to the configured timeout."""
        self.check_pid()
        deadline = time.monotonic() + self._timeout
        with self._condition:
            while True:
                if self._state != "OPEN":
                    msg = "ADBC pool is closed to new acquisitions."
                    raise DatabaseConnectionError(msg)
                if len(self._clones) + self._reservations < self._max_size:
                    self._reservations += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    msg = "Timed out waiting for an exclusive ADBC connection."
                    raise ConnectionTimeoutError(msg)
                self._condition.wait(min(remaining, threading.TIMEOUT_MAX))
        try:
            with self._seed_lock:
                if self._seed is None:
                    self._seed = self._create_connection()
                connection = self._seed.adbc_clone()
        except BaseException:
            with self._condition:
                self._reservations -= 1
                self._condition.notify_all()
            with contextlib.suppress(BaseException):
                self._finish_close()
            raise
        with self._condition:
            self._reservations -= 1
            accepted = self._state == "OPEN"
            self._clones[id(connection)] = (connection, "active" if accepted else "closing")
            self._condition.notify_all()
        if not accepted:
            self._close_clone(connection)
            self._finish_close()
            msg = "ADBC pool closed during connection acquisition."
            raise DatabaseConnectionError(msg)
        return connection

    def release(self, connection: "AdbcConnection", *, discard: bool = False) -> None:
        """Release exactly one active lease, retaining any failed native close."""
        self.check_pid()
        with self._condition:
            entry = self._clones.get(id(connection))
            if entry is None or entry[0] is not connection or entry[1] != "active":
                msg = "Connection is not an active lease of this ADBC pool."
                raise DatabaseConnectionError(msg)
            self._clones[id(connection)] = (connection, "closing")
        self._close_clone(connection)
        self._finish_close()

    def close(self) -> bool:
        """Begin terminal drain and retry failed cleanup without closing active leases.

        Returns:
            True only when every owned clone and the original seed are closed.
        """
        self.check_pid()
        with self._condition:
            if self._state == "CLOSED":
                return True
            self._state = "DRAINING"
            closing = []
            for key, (connection, state) in self._clones.items():
                if state in {"idle", "quarantined"}:
                    self._clones[key] = (connection, "closing")
                    closing.append(connection)
            self._condition.notify_all()
        first_error: BaseException | None = None
        for connection in closing:
            try:
                self._close_clone(connection)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        try:
            self._finish_close()
        except BaseException as exc:
            if first_error is None:
                first_error = exc
        if first_error is not None:
            raise first_error
        with self._condition:
            return self._state == "CLOSED"

    @property
    def is_closed(self) -> bool:
        """Return whether every owned native resource has been closed."""
        self.check_pid()
        with self._condition:
            return self._state == "CLOSED"

    def check_pid(self) -> None:
        """Reject inherited pool use before acquiring any lock or native handle."""
        if os.getpid() != self._pid:
            msg = "ADBC pools cannot be used after fork; create a new configuration in the child."
            raise DatabaseConnectionError(msg)

    def _close_clone(self, connection: "AdbcConnection") -> None:
        try:
            connection.close()
        except BaseException:
            with self._condition:
                self._clones[id(connection)] = (connection, "quarantined")
                self._condition.notify_all()
            raise
        with self._condition:
            del self._clones[id(connection)]
            self._condition.notify_all()

    def _finish_close(self) -> None:
        with self._condition:
            if self._state != "DRAINING" or self._clones or self._reservations:
                return
        with self._seed_lock:
            with self._condition:
                if self._state != "DRAINING" or self._clones or self._reservations:
                    return
            if self._seed is not None:
                self._seed.close()
                self._seed = None
            with self._condition:
                self._state = "CLOSED"
                self._condition.notify_all()
