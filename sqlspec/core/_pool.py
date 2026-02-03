"""Thread-local object pool primitives for performance-sensitive hot paths."""

import threading
from typing import TYPE_CHECKING, Generic, TypeVar

from mypy_extensions import mypyc_attr

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core.statement import SQL, ProcessedState

__all__ = ("ObjectPool", "get_processed_state_pool", "get_sql_pool", )


T = TypeVar("T")
_thread_local = threading.local()


@mypyc_attr(allow_interpreted_subclasses=False)
class ObjectPool(Generic[T]):
    """Reusable object pool with reset-instead-of-recreate semantics."""

    __slots__ = ("_factory", "_max_size", "_pool", "_resetter")

    def __init__(self, factory: "Callable[[], T]", resetter: "Callable[[T], None]", max_size: int = 100) -> None:
        self._pool: list[T] = []
        self._max_size = max_size
        self._factory = factory
        self._resetter = resetter

    def acquire(self) -> T:
        if self._pool:
            return self._pool.pop()
        return self._factory()

    def release(self, obj: T) -> None:
        self._resetter(obj)
        if len(self._pool) < self._max_size:
            self._pool.append(obj)


def _reset_noop(_: object) -> None:
    return None


def _create_sql() -> "SQL":
    from sqlspec.core.statement import SQL

    return SQL.__new__(SQL)


def _create_processed_state() -> "ProcessedState":
    from sqlspec.core.statement import ProcessedState

    return ProcessedState("", [], None, "COMMAND")


def get_sql_pool() -> "ObjectPool[SQL]":
    pool = getattr(_thread_local, "sql_pool", None)
    if pool is None:
        from sqlspec.core.statement import SQL

        pool = ObjectPool(factory=_create_sql, resetter=SQL.reset)
        _thread_local.sql_pool = pool
    return pool


def get_processed_state_pool() -> "ObjectPool[ProcessedState]":
    pool = getattr(_thread_local, "processed_state_pool", None)
    if pool is None:
        from sqlspec.core.statement import ProcessedState

        pool = ObjectPool(factory=_create_processed_state, resetter=ProcessedState.reset)
        _thread_local.processed_state_pool = pool
    return pool
