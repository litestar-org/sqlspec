from __future__ import annotations

from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    NamedTuple,
    Protocol,
)

if TYPE_CHECKING:
    import inspect
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager
    from pathlib import Path


class SQLOperationType(Enum):
    """Enumeration of aiosql operation types."""

    INSERT_RETURNING = 0
    INSERT_UPDATE_DELETE = 1
    INSERT_UPDATE_DELETE_MANY = 2
    SCRIPT = 3
    SELECT = 4
    SELECT_ONE = 5
    SELECT_VALUE = 6
    BULK_SELECT = 7


class QueryDatum(NamedTuple):
    query_name: str
    doc_comments: str
    operation_type: SQLOperationType
    sql: str
    record_class: Any = None
    signature: inspect.Signature | None = None
    floc: tuple[Path | str, int] | None = None


class QueryFn(Protocol):
    __name__: str
    __signature__: inspect.Signature | None
    sql: str
    operation: SQLOperationType

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...  # pragma: no cover


QueryDataTree = dict[str, QueryDatum | dict]


class SyncDriverAdapterProtocol(Protocol):
    def process_sql(self, query_name: str, op_type: SQLOperationType, sql: str) -> str: ...  # pragma: no cover

    def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> list: ...  # pragma: no cover

    def select_one(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None: ...  # pragma: no cover

    def select_value(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    def select_cursor(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> AbstractContextManager[Any]: ...  # pragma: no cover

    # TODO: Next major version introduce a return? Optional return?
    def insert_update_delete(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> int: ...  # pragma: no cover

    # TODO: Next major version introduce a return? Optional return?
    def insert_update_delete_many(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> int: ...  # pragma: no cover

    def insert_returning(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    def execute_script(self, conn: Any, sql: str) -> str: ...  # pragma: no cover


class AsyncDriverAdapterProtocol(Protocol):
    def process_sql(self, query_name: str, op_type: SQLOperationType, sql: str) -> str: ...  # pragma: no cover

    async def select(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> list: ...  # pragma: no cover

    async def select_one(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None: ...  # pragma: no cover

    async def select_value(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    async def select_cursor(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> AbstractAsyncContextManager[Any]: ...  # pragma: no cover

    # TODO: Next major version introduce a return? Optional return?
    async def insert_update_delete(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> None: ...  # pragma: no cover

    # TODO: Next major version introduce a return? Optional return?
    async def insert_update_delete_many(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> None: ...  # pragma: no cover

    async def insert_returning(
        self,
        conn: Any,
        query_name: str,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    async def execute_script(self, conn: Any, sql: str) -> str: ...  # pragma: no cover


DriverAdapterProtocol = SyncDriverAdapterProtocol | AsyncDriverAdapterProtocol
