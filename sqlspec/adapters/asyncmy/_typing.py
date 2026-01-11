"""AsyncMy adapter type definitions.

This module contains type aliases and classes that are excluded from mypyc
compilation to avoid ABI boundary issues.
"""

from typing import TYPE_CHECKING, Any, TypeAlias

from asyncmy import Connection  # pyright: ignore

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Protocol

    from sqlspec.adapters.asyncmy.driver import AsyncmyDriver
    from sqlspec.core import StatementConfig

    class AsyncmyConnectionProtocol(Protocol):
        def cursor(self) -> Any: ...

        async def commit(self) -> Any: ...

        async def rollback(self) -> Any: ...

        async def close(self) -> Any: ...

    AsyncmyConnection: TypeAlias = AsyncmyConnectionProtocol
else:
    AsyncmyConnection = Connection


class AsyncmySessionContext:
    """Async context manager for AsyncMy sessions.

    This class is intentionally excluded from mypyc compilation to avoid ABI
    boundary issues. It receives callables from uncompiled config classes and
    instantiates compiled Driver objects, acting as a bridge between compiled
    and uncompiled code.

    Uses callable-based connection management to decouple from config implementation.
    """

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[[Any], Any]",
        statement_config: "StatementConfig",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[AsyncmyDriver], AsyncmyDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: AsyncmyDriver | None = None

    async def __aenter__(self) -> "AsyncmyDriver":
        from sqlspec.adapters.asyncmy.driver import AsyncmyDriver

        self._connection = await self._acquire_connection()
        self._driver = AsyncmyDriver(
            connection=self._connection, statement_config=self._statement_config, driver_features=self._driver_features
        )
        return self._prepare_driver(self._driver)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            await self._release_connection(self._connection)
            self._connection = None
        return None


__all__ = ("AsyncmyConnection", "AsyncmySessionContext")
