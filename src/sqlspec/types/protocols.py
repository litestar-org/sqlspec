# SPDX-FileCopyrightText: 2023-present Cody Fincher <codyfincher@google.com>
#
# SPDX-License-Identifier: MIT
from __future__ import annotations

from collections.abc import Collection, Iterable
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Dict, NamedTuple, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    import inspect
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager
    from pathlib import Path


@runtime_checkable
class DataclassProtocol(Protocol):
    """Protocol for instance checking dataclasses"""

    __dataclass_fields__: ClassVar[dict[str, Any]]


T_co = TypeVar("T_co", covariant=True)


@runtime_checkable
class InstantiableCollection(Collection[T_co], Protocol[T_co]):  # pyright: ignore
    """A protocol for instantiable collection types."""

    def __init__(self, iterable: Iterable[T_co], /) -> None: ...


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


QueryDataTree = Dict[str, QueryDatum | Dict]


class SyncDriverAdapterProtocol(Protocol):
    is_asyncio: bool = False

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
    is_asyncio: bool = True

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
