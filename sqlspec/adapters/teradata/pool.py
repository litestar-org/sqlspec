"""Exclusive, bounded Teradata DB-API connection pooling."""

import contextlib
import math
import os
import threading
import time
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from typing import Any

from mypy_extensions import mypyc_attr

from sqlspec.adapters.teradata.core import (
    build_connection_config,
    create_mapped_exception,
    format_query_band,
    render_query_band_sql,
)
from sqlspec.exceptions import (
    ConnectionTimeoutError,
    DatabaseConnectionError,
    ImproperConfigurationError,
    MissingDependencyError,
)
from sqlspec.utils.module_loader import import_optional

__all__ = ("TeradataConnectionPool",)


@mypyc_attr(allow_interpreted_subclasses=False)
class TeradataConnectionPool:
    """Lazily allocate exclusive leases with bounded capacity and fork refusal."""

    __slots__ = (
        "_autocommit_baseline",
        "_closed",
        "_condition",
        "_connection_config",
        "_creating",
        "_generation",
        "_health_check_interval",
        "_in_use",
        "_max_size",
        "_min_size",
        "_on_connection_create",
        "_pid",
        "_pool",
        "_recycle_seconds",
        "_session_query_band",
        "_thread_local",
        "_timeout",
    )

    def __init__(
        self,
        connection_config: Mapping[str, Any],
        *,
        min_size: object = 1,
        max_size: object = 4,
        timeout: object = 30.0,
        recycle_seconds: object = 1800,
        health_check_interval: object = 60.0,
        on_connection_create: Callable[[Any], None] | None = None,
        session_query_band: Mapping[str, str] | str | None = None,
    ) -> None:
        """Configure a lazy bounded pool before compiled integer argument coercion."""
        if (
            isinstance(min_size, bool)
            or isinstance(max_size, bool)
            or isinstance(timeout, bool)
            or isinstance(recycle_seconds, bool)
            or isinstance(health_check_interval, bool)
            or not isinstance(min_size, int)
            or not isinstance(max_size, int)
            or not isinstance(timeout, (int, float))
            or not isinstance(recycle_seconds, int)
            or not isinstance(health_check_interval, (int, float))
            or min_size < 0
            or max_size < 1
            or min_size > max_size
            or not math.isfinite(timeout)
            or timeout < 0
            or recycle_seconds < 0
            or not math.isfinite(health_check_interval)
            or health_check_interval < 0
        ):
            msg = "Invalid Teradata pool bounds or timeout"
            raise ImproperConfigurationError(msg)
        timeout_seconds = float(timeout)
        health_interval_seconds = float(health_check_interval)
        self._connection_config = build_connection_config(connection_config)
        self._autocommit_baseline = self._connection_config.pop("autocommit", True)
        self._closed = False
        self._condition = threading.Condition()
        self._creating = 0
        self._generation = 0
        self._health_check_interval = health_interval_seconds
        self._in_use: dict[int, tuple[Any, float, float]] = {}
        self._pool: list[tuple[Any, float, float]] = []
        self._max_size = max_size
        self._min_size = min_size
        self._on_connection_create = on_connection_create
        self._pid = os.getpid()
        self._recycle_seconds = recycle_seconds
        self._session_query_band = format_query_band(session_query_band) if session_query_band is not None else None
        self._thread_local = threading.local()
        self._timeout = timeout_seconds

    @property
    def is_closed(self) -> bool:
        self.check_pid()
        return self._closed

    def check_pid(self) -> None:
        """Refuse inherited driver handles before acquiring inherited locks."""
        if os.getpid() != self._pid:
            msg = "Teradata pools cannot be shared across forked processes"
            raise DatabaseConnectionError(msg)

    def new_connection(self) -> Any:
        """Open and initialize a caller-owned connection."""
        self.check_pid()
        module = import_optional("teradatasql")
        if module is None:
            raise MissingDependencyError(package="teradatasql", install_package="teradata")
        try:
            connection = module.connect(**self._connection_config)
        except Exception as exc:
            error_base = getattr(module, "Error", None)
            if (isinstance(error_base, type) and isinstance(exc, error_base)) or type(exc).__name__ in {
                "OperationalError",
                "DatabaseError",
                "InterfaceError",
            }:
                raise create_mapped_exception(exc) from exc
            raise
        try:
            connection.autocommit = self._autocommit_baseline
            if self._session_query_band is not None:
                cursor = connection.cursor()
                try:
                    cursor.execute(render_query_band_sql(self._session_query_band))
                    if connection.autocommit is False:
                        connection.commit()
                finally:
                    cursor.close()
            if self._on_connection_create:
                self._on_connection_create(connection)
        except BaseException:
            connection.close()
            raise
        return connection

    def acquire(self) -> Any:
        """Acquire an exclusive lease, waiting at most the configured timeout."""
        self.check_pid()
        deadline = time.monotonic() + self._timeout
        while True:
            entry = None
            should_wait = False
            with self._condition:
                if self._closed:
                    msg = "Teradata pool is closed"
                    raise DatabaseConnectionError(msg)
                if self._pool:
                    entry = self._pool.pop()
                    self._creating += 1
                elif len(self._in_use) + self._creating < self._max_size:
                    self._creating += 1
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        msg = "Timed out acquiring Teradata connection"
                        raise ConnectionTimeoutError(msg)
                    self._condition.wait(remaining)
                    should_wait = True
            if should_wait:
                continue
            now = time.monotonic()
            created = now
            connection = None
            try:
                if entry is not None:
                    connection, created, last_used = entry
                    stale = self._recycle_seconds > 0 and now - created >= self._recycle_seconds
                    if not stale and self._health_check_interval > 0 and now - last_used >= self._health_check_interval:
                        try:
                            cursor = connection.cursor()
                            try:
                                cursor.execute("SELECT 1")
                                cursor.fetchone()
                            finally:
                                cursor.close()
                        except Exception:
                            stale = True
                    if stale:
                        with contextlib.suppress(Exception):
                            connection.close()
                        connection = None
                if connection is None:
                    connection = self.new_connection()
                    created = now
                with self._condition:
                    if self._closed:
                        connection.close()
                        msg = "Teradata pool closed during checkout"
                        raise DatabaseConnectionError(msg)
                    self._in_use[id(connection)] = (connection, created, now)
                return connection
            finally:
                with self._condition:
                    self._creating -= 1
                    self._condition.notify_all()

    def release(self, connection: Any, *, discard: bool = False) -> None:
        """Sanitize a lease and return it to the idle pool."""
        self.check_pid()
        with self._condition:
            entry = self._in_use.get(id(connection))
        if entry is None:
            return
        if not discard:
            try:
                if connection.autocommit is False:
                    connection.rollback()
                connection.autocommit = self._autocommit_baseline
                self._reset_session_query_band(connection)
            except Exception:
                discard = True
        with self._condition:
            self._in_use.pop(id(connection), None)
            discard = discard or self._closed
            if not discard:
                self._pool.append((connection, entry[1], time.monotonic()))
            self._condition.notify_all()
        if discard:
            with contextlib.suppress(Exception):
                connection.close()

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """Provide a reentrant thread-local lease.

        Yields:
            Exclusively leased DB-API connection.
        """
        self.check_pid()
        depth = getattr(self._thread_local, "depth", 0)
        if depth:
            if self._closed or getattr(self._thread_local, "generation", -1) != self._generation:
                msg = "Teradata pool lease has been retired"
                raise DatabaseConnectionError(msg)
            self._thread_local.depth += 1
            try:
                yield self._thread_local.connection
            finally:
                self._thread_local.depth -= 1
            return
        connection = self.acquire()
        self._thread_local.connection = connection
        self._thread_local.depth = 1
        self._thread_local.generation = self._generation
        discard = False
        try:
            yield connection
        except BaseException:
            discard = True
            raise
        finally:
            self._thread_local.depth = 0
            self._thread_local.connection = None
            self.release(connection, discard=discard)

    def close(self) -> None:
        """Close idle handles and retire checked-out leases when returned."""
        self.check_pid()
        with self._condition:
            self._closed = True
            self._generation += 1
            entries, self._pool = self._pool, []
            self._condition.notify_all()
        for connection, _, _ in entries:
            with contextlib.suppress(Exception):
                connection.close()

    def size(self) -> int:
        """Return the allocated plus pending connection count."""
        self.check_pid()
        with self._condition:
            return len(self._pool) + len(self._in_use) + self._creating

    def checked_out(self) -> int:
        """Return the number of active leases."""
        self.check_pid()
        with self._condition:
            return len(self._in_use)

    def _reset_session_query_band(self, connection: Any) -> None:
        """Reset the session query band to the configured baseline or NONE."""
        cursor = connection.cursor()
        try:
            cursor.execute(render_query_band_sql(self._session_query_band or ""))
        finally:
            cursor.close()
        if connection.autocommit is False:
            connection.commit()
