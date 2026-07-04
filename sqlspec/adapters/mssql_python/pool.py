"""mssql-python pool facade."""

import warnings
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.mssql_python._typing import MSSQL_PYTHON_MODULE, MssqlPythonConnection

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ("MssqlPythonConnectionPool",)

_POOLING_PARAMS: "tuple[int, int, bool] | None" = None


class MssqlPythonConnectionPool:
    """Small SQLSpec pool facade over mssql-python's driver-level pooling."""

    __slots__ = (
        "_closed",
        "connect_kwargs",
        "connection_string",
        "enabled",
        "idle_timeout",
        "max_size",
        "on_connection_create",
    )

    def __init__(
        self,
        *,
        connection_string: str,
        connect_kwargs: "dict[str, Any] | None" = None,
        max_size: int = 100,
        idle_timeout: int = 600,
        enabled: bool = True,
        on_connection_create: "Callable[[MssqlPythonConnection], None] | None" = None,
    ) -> None:
        self.connection_string = connection_string
        self.connect_kwargs = connect_kwargs or {}
        self.max_size = max_size
        self.idle_timeout = idle_timeout
        self.enabled = enabled
        self.on_connection_create = on_connection_create
        self._closed = False
        global _POOLING_PARAMS
        new_params = (max_size, idle_timeout, enabled)
        if _POOLING_PARAMS is not None and new_params != _POOLING_PARAMS:
            warnings.warn(
                f"mssql-python pooling config already set to {_POOLING_PARAMS}; "
                f"overwriting with {new_params}. Only one pool config per process is supported.",
                stacklevel=2,
            )
        MSSQL_PYTHON_MODULE.pooling(max_size=max_size, idle_timeout=idle_timeout, enabled=enabled)
        _POOLING_PARAMS = new_params

    def acquire(self) -> "MssqlPythonConnection":
        if self._closed:
            msg = "Cannot acquire a connection from a closed mssql-python pool."
            raise RuntimeError(msg)
        connection = cast(
            "MssqlPythonConnection", MSSQL_PYTHON_MODULE.connect(self.connection_string, **self.connect_kwargs)
        )
        if self.on_connection_create is not None:
            self.on_connection_create(connection)
        return connection

    def release(self, connection: "MssqlPythonConnection") -> None:
        connection.close()

    def close(self) -> None:
        self._closed = True
