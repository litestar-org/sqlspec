"""CockroachDB AsyncPG adapter type definitions."""

from typing import TYPE_CHECKING, Any

from asyncpg.pool import PoolConnectionProxy

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    from asyncpg import Connection, Pool, Record

    from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver
    from sqlspec.core import StatementConfig

    CockroachAsyncpgConnection: TypeAlias = Connection[Record] | PoolConnectionProxy[Record]
    CockroachAsyncpgPool: TypeAlias = Pool[Record]
else:
    from asyncpg import Pool

    CockroachAsyncpgConnection = PoolConnectionProxy
    CockroachAsyncpgPool = Pool


class CockroachAsyncpgSessionContext:
    """Async context manager for CockroachDB AsyncPG sessions."""

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
        prepare_driver: "Callable[[CockroachAsyncpgDriver], CockroachAsyncpgDriver]",
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._connection: Any = None
        self._driver: CockroachAsyncpgDriver | None = None

    async def __aenter__(self) -> "CockroachAsyncpgDriver":
        from sqlspec.adapters.cockroach_asyncpg.driver import CockroachAsyncpgDriver

        self._connection = await self._acquire_connection()
        self._driver = CockroachAsyncpgDriver(
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


__all__ = ("CockroachAsyncpgConnection", "CockroachAsyncpgPool", "CockroachAsyncpgSessionContext")
