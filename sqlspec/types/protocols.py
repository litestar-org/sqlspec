# SPDX-FileCopyrightText: 2023-present Cody Fincher <codyfincher@google.com>
#
# SPDX-License-Identifier: MIT
from __future__ import annotations

from collections.abc import AsyncGenerator, Iterable
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, Union

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "AsyncDriverAdapterProtocol",
    "InstantiableCollection",
)


T_co = TypeVar("T_co", covariant=True)


class SyncDriverProtocol(Protocol):
    is_async: bool = False

    async def execute(
        self,
        sql: str,
        *,
        parameters: list | dict,
        connection: Any,
        schema_type: Callable | None,
    ) -> Iterable[Any]: ...  # pragma: no cover


class AsyncDriverProtocol(Protocol):
    is_async: bool = True

    def process_sql(self, sql: str) -> str: ...  # pragma: no cover

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
