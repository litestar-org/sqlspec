"""mssql-python pool facade."""

import contextlib
import threading
import warnings
import weakref
from typing import TYPE_CHECKING, Any, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.mssql_python._typing import MSSQL_PYTHON_MODULE, MssqlPythonConnection, MssqlPythonPoolingManager
from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ("MssqlPythonConnectionPool",)

_POOLING_PARAMS: "tuple[int, int, bool] | None" = None
_POOL_LOCK = threading.Lock()
_ACTIVE_ENABLED_POOLS: "weakref.WeakSet[MssqlPythonConnectionPool]" = weakref.WeakSet()
_ACTIVE_DISABLED_POOLS: "weakref.WeakSet[MssqlPythonConnectionPool]" = weakref.WeakSet()


@mypyc_attr(native_class=False)
class MssqlPythonConnectionPool:
    """Small SQLSpec pool facade over mssql-python's driver-level pooling."""

    __slots__ = (
        "__weakref__",
        "_closed",
        "connect_kwargs",
        "connection_string",
        "enabled",
        "idle_timeout",
        "max_size",
        "on_connection_create",
        "require_unpooled",
    )

    def __init__(
        self,
        *,
        connection_string: str,
        connect_kwargs: "dict[str, Any] | None" = None,
        max_size: int = 100,
        idle_timeout: int = 600,
        enabled: bool = True,
        require_unpooled: bool = False,
        on_connection_create: "Callable[[MssqlPythonConnection], None] | None" = None,
    ) -> None:
        """Initialize the pool facade and synchronize process-global mssql-python pooling state.

        ``mssql-python`` auto-enables native DDBC connection pooling on the first
        ``connect()`` call unless ``mssql_python.pooling(enabled=False)`` has been
        called explicitly, so disabled pools must still initialize the driver state.
        A pool created with ``require_unpooled=True`` refuses to start while native pooling is
        enabled, and blocks enabled pools from starting while it remains open.

        Raises:
            ImproperConfigurationError: Native pooling conflicts with an unpooled tenant pool.
        """
        self.connection_string = connection_string
        self.connect_kwargs = connect_kwargs or {}
        self.max_size = max_size
        self.idle_timeout = idle_timeout
        self.enabled = enabled
        self.require_unpooled = require_unpooled
        self.on_connection_create = on_connection_create
        self._closed = False
        global _POOLING_PARAMS
        new_params = (max_size, idle_timeout, True) if enabled else (0, 0, False)
        with _POOL_LOCK:
            if require_unpooled and (enabled or _ACTIVE_ENABLED_POOLS or MssqlPythonPoolingManager.is_enabled()):
                msg = "Tenant mode requires pool_enabled=False on every configuration; disable SDK pooling before the first connection"
                raise ImproperConfigurationError(msg)
            if enabled and any(pool.require_unpooled for pool in _ACTIVE_DISABLED_POOLS):
                msg = "Tenant mode requires pool_enabled=False on every configuration"
                raise ImproperConfigurationError(msg)
            active_pools_exist = bool(_ACTIVE_ENABLED_POOLS or _ACTIVE_DISABLED_POOLS)
            params_changed = _POOLING_PARAMS is None or new_params != _POOLING_PARAMS
            if active_pools_exist and _POOLING_PARAMS is not None and new_params != _POOLING_PARAMS:
                warnings.warn(
                    f"mssql-python pooling config already set to {_POOLING_PARAMS}; "
                    f"overwriting with {new_params}. Only one pool config per process is supported.",
                    stacklevel=2,
                )
            if params_changed:
                if enabled:
                    MSSQL_PYTHON_MODULE.pooling(max_size=max_size, idle_timeout=idle_timeout, enabled=True)
                    _ACTIVE_DISABLED_POOLS.clear()
                else:
                    MSSQL_PYTHON_MODULE.pooling(enabled=False)
                    _ACTIVE_ENABLED_POOLS.clear()
                _POOLING_PARAMS = new_params
            if enabled:
                _ACTIVE_ENABLED_POOLS.add(self)
            else:
                _ACTIVE_DISABLED_POOLS.add(self)

    def acquire(self) -> "MssqlPythonConnection":
        if self._closed:
            msg = "Cannot acquire a connection from a closed mssql-python pool."
            raise RuntimeError(msg)
        connection = cast(
            "MssqlPythonConnection", MSSQL_PYTHON_MODULE.connect(self.connection_string, **self.connect_kwargs)
        )
        if self.on_connection_create is not None and not getattr(connection, "_sqlspec_initialized", False):
            try:
                self.on_connection_create(connection)
            except BaseException:
                with contextlib.suppress(Exception):
                    connection.close()
                raise
            with contextlib.suppress(AttributeError):
                setattr(connection, "_sqlspec_initialized", True)
        return connection

    def release(self, connection: "MssqlPythonConnection") -> None:
        connection.close()

    def close(self) -> None:
        """Mark the pool closed and shut down native DDBC pools once no enabled pool remains."""
        global _POOLING_PARAMS
        with _POOL_LOCK:
            if self._closed:
                return
            self._closed = True
            was_enabled_owner = self in _ACTIVE_ENABLED_POOLS
            _ACTIVE_ENABLED_POOLS.discard(self)
            _ACTIVE_DISABLED_POOLS.discard(self)
            if was_enabled_owner and not _ACTIVE_ENABLED_POOLS:
                MSSQL_PYTHON_MODULE.pooling(enabled=False)
                _POOLING_PARAMS = None
            elif not _ACTIVE_ENABLED_POOLS and not _ACTIVE_DISABLED_POOLS:
                _POOLING_PARAMS = None
