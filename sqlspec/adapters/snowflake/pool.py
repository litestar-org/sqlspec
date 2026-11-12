"""Bounded exclusive Snowflake connection pooling."""

import contextlib
import math
import os
import threading
import time
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.snowflake.core import normalize_connection_params
from sqlspec.exceptions import DatabaseConnectionError, ImproperConfigurationError, MissingDependencyError
from sqlspec.typing import import_optional

if TYPE_CHECKING:
    from sqlspec.adapters.snowflake._typing import SnowflakeConnection
    from sqlspec.observability import ObservabilityRuntime

__all__ = ("SnowflakeConnectionPool",)


class SnowflakeConnectionPool:
    """Own bounded exclusive sessions and reuse healthy handles between leases."""

    __slots__ = (
        "_checked_out",
        "_closed",
        "_condition",
        "_connection_parameters",
        "_created_pid",
        "_health_check_interval",
        "_idle",
        "_max_size",
        "_min_size",
        "_observability_runtime",
        "_recycle_seconds",
        "_thread_local",
        "_timeout",
        "_total_count",
    )

    def __init__(
        self,
        connection_parameters: Mapping[str, Any],
        *,
        min_size: object = 0,
        max_size: object = 5,
        timeout: object = 30.0,
        recycle_seconds: object = 3600,
        health_check_interval: object = 60.0,
        observability_runtime: "ObservabilityRuntime | None" = None,
    ) -> None:
        """Configure capacity, idle health checks, recycling, and connection observability."""
        if (
            isinstance(min_size, bool)
            or not isinstance(min_size, int)
            or min_size < 0
            or isinstance(max_size, bool)
            or not isinstance(max_size, int)
            or max_size < 1
            or min_size > max_size
        ):
            msg = "Snowflake pool requires 0 <= min_size <= max_size and max_size >= 1"
            raise ImproperConfigurationError(msg)
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            msg = "Snowflake pool timeout must be finite and positive"
            raise ImproperConfigurationError(msg)
        if isinstance(recycle_seconds, bool) or not isinstance(recycle_seconds, int) or recycle_seconds < 0:
            msg = "Snowflake pool recycle_seconds must be a nonnegative integer"
            raise ImproperConfigurationError(msg)
        if (
            isinstance(health_check_interval, bool)
            or not isinstance(health_check_interval, (int, float))
            or not math.isfinite(health_check_interval)
            or health_check_interval < 0
        ):
            msg = "Snowflake pool health_check_interval must be finite and nonnegative"
            raise ImproperConfigurationError(msg)
        self._connection_parameters = dict(connection_parameters)
        self._min_size, self._max_size = min_size, max_size
        self._timeout, self._recycle_seconds = float(timeout), recycle_seconds
        self._health_check_interval = float(health_check_interval)
        self._observability_runtime = observability_runtime
        self._created_pid = os.getpid()
        self._condition = threading.Condition(threading.Lock())
        self._thread_local = threading.local()
        self._idle: list[tuple[SnowflakeConnection, float, float]] = []
        self._checked_out: dict[int, tuple[SnowflakeConnection, float]] = {}
        self._total_count = 0
        self._closed = False
        try:
            for _ in range(min_size):
                self._total_count += 1
                connection = self._create_physical_connection()
                now = time.monotonic()
                self._idle.append((connection, now, now))
        except BaseException:
            self.close()
            raise

    def check_pid(self) -> None:
        """Refuse use after a process fork before accessing inherited locks."""
        if os.getpid() != self._created_pid:
            msg = "Snowflake connection pool cannot be reused after a process fork"
            raise DatabaseConnectionError(msg)

    def _open_connection(self) -> "SnowflakeConnection":
        connector = import_optional("snowflake.connector")
        if connector is None:
            raise MissingDependencyError(package="snowflake-connector-python", install_package="snowflake")
        return cast(
            "SnowflakeConnection", connector.connect(**normalize_connection_params(self._connection_parameters))
        )

    def _create_physical_connection(self) -> "SnowflakeConnection":
        connection: SnowflakeConnection | None = None
        try:
            connection = self._open_connection()
            if self._observability_runtime is not None:
                self._observability_runtime.emit_connection_create_sync(connection)
        except BaseException:
            try:
                if connection is not None:
                    self._destroy_connection(connection)
            finally:
                with self._condition:
                    self._total_count -= 1
                    self._condition.notify_all()
            raise
        else:
            return connection

    def new_connection(self) -> "SnowflakeConnection":
        """Open a standalone connection configured like a pooled one.

        The connection is owned by the caller: it does not count against pool capacity and is not
        closed by pool shutdown.

        Returns:
            A newly opened physical connection.
        """
        self.check_pid()
        connection = self._open_connection()
        try:
            if self._observability_runtime is not None:
                self._observability_runtime.emit_connection_create_sync(connection)
        except BaseException:
            with contextlib.suppress(Exception):
                connection.close()
            raise
        return connection

    def _destroy_connection(self, connection: "SnowflakeConnection") -> None:
        try:
            if self._observability_runtime is not None:
                self._observability_runtime.emit_connection_destroy_sync(connection)
        finally:
            with contextlib.suppress(Exception):
                connection.close()

    def acquire(self) -> "SnowflakeConnection":
        """Acquire one exclusive healthy handle within the configured timeout."""
        self.check_pid()
        deadline = time.monotonic() + self._timeout
        while True:
            idle: tuple[SnowflakeConnection, float, float] | None = None
            waited = False
            with self._condition:
                if self._closed:
                    msg = "Snowflake connection pool is closed"
                    raise DatabaseConnectionError(msg)
                if self._idle:
                    idle = self._idle.pop()
                elif self._total_count < self._max_size:
                    self._total_count += 1
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        msg = "Timed out waiting for a Snowflake connection"
                        raise DatabaseConnectionError(msg)
                    self._condition.wait(min(remaining, threading.TIMEOUT_MAX))
                    waited = True
            if waited:
                continue
            if idle is None:
                connection = self._create_physical_connection()
                created_at = time.monotonic()
            else:
                connection, created_at, released_at = idle
                expired = self._recycle_seconds > 0 and time.monotonic() - created_at >= self._recycle_seconds
                healthy = not expired
                if healthy and time.monotonic() - released_at >= self._health_check_interval:
                    try:
                        healthy = not connection.is_closed()
                    except Exception:
                        healthy = False
                if not healthy:
                    try:
                        self._destroy_connection(connection)
                    finally:
                        with self._condition:
                            self._total_count -= 1
                            self._condition.notify_all()
                    continue
            with self._condition:
                if not self._closed:
                    self._checked_out[id(connection)] = (connection, created_at)
                    return connection
                self._total_count -= 1
                self._condition.notify_all()
            self._destroy_connection(connection)
            msg = "Snowflake connection pool closed during acquisition"
            raise DatabaseConnectionError(msg)

    def release(self, connection: "SnowflakeConnection", *, discard: bool = False) -> None:
        """Roll back a completed lease and retain only healthy reusable sessions."""
        self.check_pid()
        with self._condition:
            entry = self._checked_out.pop(id(connection), None)
            if entry is None or entry[0] is not connection:
                msg = "Connection is not an active lease of this Snowflake pool"
                raise DatabaseConnectionError(msg)
        created_at = entry[1]
        try:
            if not discard:
                connection.rollback()
                discard = connection.is_closed()
        except Exception:
            discard = True
        expired = self._recycle_seconds > 0 and time.monotonic() - created_at >= self._recycle_seconds
        with self._condition:
            if not (discard or expired or self._closed):
                self._idle.append((connection, created_at, time.monotonic()))
                self._condition.notify()
                return
            self._total_count -= 1
            self._condition.notify_all()
        self._destroy_connection(connection)

    @contextmanager
    def get_connection(self) -> "Generator[SnowflakeConnection, None, None]":
        """Reuse an outer thread lease when connection contexts are nested.

        Yields:
            An exclusively leased connection shared by nested contexts on this thread.
        """
        self.check_pid()
        connection = getattr(self._thread_local, "connection", None)
        if connection is None:
            connection = self.acquire()
            self._thread_local.connection = connection
            self._thread_local.depth = 0
            self._thread_local.discard = False
        self._thread_local.depth += 1
        try:
            yield connection
        except BaseException:
            self._thread_local.discard = True
            raise
        finally:
            self._thread_local.depth -= 1
            if self._thread_local.depth == 0:
                discard = self._thread_local.discard
                self._thread_local.connection = None
                self.release(connection, discard=discard)

    def close(self) -> None:
        """Stop acquisitions and close idle handles, draining active leases on return."""
        self.check_pid()
        with self._condition:
            self._closed = True
            idle, self._idle = self._idle, []
            self._total_count -= len(idle)
            self._condition.notify_all()
        first_error: BaseException | None = None
        for connection, _, _ in idle:
            try:
                self._destroy_connection(connection)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def size(self) -> int:
        """Return the number of reserved or physically owned connections."""
        self.check_pid()
        with self._condition:
            return self._total_count

    def checked_out(self) -> int:
        """Return the number of active connection leases."""
        self.check_pid()
        with self._condition:
            return len(self._checked_out)
