# SPDX-FileCopyrightText: 2023-present Cody Fincher <codyfincher@google.com>
#
# SPDX-License-Identifier: MIT
from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol, Union

if TYPE_CHECKING:
    import inspect
    from collections.abc import AsyncGenerator, Callable, Iterable
    from contextlib import AbstractContextManager
    from pathlib import Path

__all__ = (
    "AsyncDriverAdapterProtocol",
    "StatementDetails",
    "StatementFn",
    "StatementType",
    "SyncDriverAdapterProtocol",
)


class StatementType(Enum):
    """Enumeration of SQL operation types."""

    INSERT_UPDATE_DELETE = 0
    INSERT_UPDATE_DELETE_MANY = 1
    INSERT_UPDATE_DELETE_RETURNING = 2
    INSERT_UPDATE_DELETE_MANY_RETURNING = 3
    SCRIPT = 4
    SELECT = 5
    SELECT_ONE = 6
    SELECT_SCALAR = 7
    BULK_SELECT = 8


class StatementDetails(NamedTuple):
    statement_name: str
    doc_comments: str
    operation_type: StatementType
    sql: str
    record_class: Any = None
    signature: inspect.Signature | None = None
    floc: tuple[Path | str, int] | None = None
    attributes: dict[str, dict[str, str]] | None = None


class StatementFn(Protocol):
    __name__: str
    __signature__: inspect.Signature | None
    sql: str
    operation: StatementType
    attributes: dict[str, dict[str, str]] | None = None

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...  # pragma: no cover


SQLStatements = dict[str, Union[StatementDetails, dict]]


class SyncDriverAdapterProtocol(Protocol):
    is_async: bool = False

    def process_sql(self, op_type: StatementType, sql: str) -> str: ...  # pragma: no cover

    def select(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]: ...  # pragma: no cover

    def select_one(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None: ...  # pragma: no cover

    def select_scalar(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    def with_cursor(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> AbstractContextManager[Any]: ...  # pragma: no cover

    def insert_update_delete(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> int: ...  # pragma: no cover

    def insert_update_delete_returning(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> Any: ...  # pragma: no cover

    def insert_update_delete_many(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> int: ...  # pragma: no cover

    def insert_update_delete_many_returning(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> Iterable[Any]: ...  # pragma: no cover

    def execute_script(
        self, connection: Any, sql: str, parameters: list | dict | None = None
    ) -> str: ...  # pragma: no cover


class AsyncDriverAdapterProtocol(Protocol):
    is_async: bool = True

    def process_sql(self, op_type: StatementType, sql: str) -> str: ...  # pragma: no cover

    async def select(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]: ...  # pragma: no cover

    async def select_one(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None: ...  # pragma: no cover

    async def select_scalar(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    async def with_cursor(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> AsyncGenerator[Any, None]: ...  # pragma: no cover

    async def insert_update_delete(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> int: ...  # pragma: no cover

    async def insert_update_delete_returning(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any: ...  # pragma: no cover

    async def insert_update_delete_many(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> int: ...  # pragma: no cover

    async def insert_update_delete_many_returning(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Iterable[Any]: ...  # pragma: no cover

    async def execute_script(
        self, connection: Any, sql: str, parameters: list | dict | None = None, record_class: Callable | None = None
    ) -> Any: ...  # pragma: no cover


DriverAdapterProtocol = Union[SyncDriverAdapterProtocol, AsyncDriverAdapterProtocol]
