"""PyMySQL database configuration with thread-local connections."""

import contextlib
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypedDict, cast

import pymysql
from typing_extensions import NotRequired

from sqlspec.adapters.pymysql._typing import PyMysqlConnection
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Generator


class PyMysqlConnectionParams(TypedDict):
    """PyMySQL connection parameters."""

    host: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    port: NotRequired[int]
    unix_socket: NotRequired[str]
    charset: NotRequired[str]
    connect_timeout: NotRequired[int]
    read_timeout: NotRequired[int]
    write_timeout: NotRequired[int]
    autocommit: NotRequired[bool]
    ssl: NotRequired["dict[str, Any]"]
    client_flag: NotRequired[int]
    cursorclass: NotRequired[type]
    init_command: NotRequired[str]
    sql_mode: NotRequired[str]
    extra: NotRequired["dict[str, Any]"]


__all__ = ("PyMysqlConnectionPool",)

logger = get_logger(__name__)


class PyMysqlConnectionPool:
    """Thread-local connection manager for PyMySQL."""

    __slots__ = ("_connection_parameters", "_health_check_interval", "_recycle_seconds", "_thread_local")

    def __init__(
        self, connection_parameters: "dict[str, Any]", recycle_seconds: int = 86400, health_check_interval: float = 30.0
    ) -> None:
        self._connection_parameters = connection_parameters
        self._thread_local = threading.local()
        self._recycle_seconds = recycle_seconds
        self._health_check_interval = health_check_interval

    def _create_connection(self) -> PyMysqlConnection:
        connection = pymysql.connect(**self._connection_parameters)
        return cast("PyMysqlConnection", connection)

    def _is_connection_alive(self, connection: PyMysqlConnection) -> bool:
        try:
            connection.ping(reconnect=False)
        except Exception:
            return False
        return True

    def _get_thread_connection(self) -> PyMysqlConnection:
        thread_state = self._thread_local.__dict__
        if "connection" not in thread_state:
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("PyMysqlConnection", self._thread_local.connection)

        if self._recycle_seconds > 0 and time.time() - self._thread_local.created_at > self._recycle_seconds:
            logger.debug("PyMySQL connection exceeded recycle time, recreating")
            with contextlib.suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("PyMysqlConnection", self._thread_local.connection)

        idle_time = time.time() - thread_state.get("last_used", 0)
        if idle_time > self._health_check_interval and not self._is_connection_alive(self._thread_local.connection):
            logger.debug("PyMySQL connection failed health check after %.1fs idle, recreating", idle_time)
            with contextlib.suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        self._thread_local.last_used = time.time()
        return cast("PyMysqlConnection", self._thread_local.connection)

    def _close_thread_connection(self) -> None:
        thread_state = self._thread_local.__dict__
        if "connection" in thread_state:
            with contextlib.suppress(Exception):
                self._thread_local.connection.close()
            del self._thread_local.connection
            if "created_at" in thread_state:
                del self._thread_local.created_at
            if "last_used" in thread_state:
                del self._thread_local.last_used

    @contextmanager
    def get_connection(self) -> "Generator[PyMysqlConnection, None, None]":
        connection = self._get_thread_connection()
        try:
            yield connection
        finally:
            with contextlib.suppress(Exception):
                if connection.open and connection.get_autocommit() is False:
                    connection.commit()

    def close(self) -> None:
        self._close_thread_connection()

    def acquire(self) -> PyMysqlConnection:
        return self._get_thread_connection()

    def release(self, connection: PyMysqlConnection) -> None:
        _ = connection

    def size(self) -> int:
        try:
            _ = self._thread_local.connection
        except AttributeError:
            return 0
        else:
            return 1

    def checked_out(self) -> int:
        return 0
