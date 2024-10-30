# SPDX-FileCopyrightText: 2023-present Cody Fincher <codyfincher@google.com>
#
# SPDX-License-Identifier: MIT
from __future__ import annotations

from collections.abc import Collection, Iterable
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, Protocol, TypeVar, Union, runtime_checkable

if TYPE_CHECKING:
    import inspect
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager
    from pathlib import Path

__all__ = (
    "DataclassProtocol",
    "InstantiableCollection",
    "Logger",
)


class Logger(Protocol):
    """Logger protocol."""

    def debug(self, event: str, *args: Any, **kwargs: Any) -> Any:
        """Output a log message at 'DEBUG' level.

        Args:
             event: Log message.
             *args: Any args.
             **kwargs: Any kwargs.
        """

    def info(self, event: str, *args: Any, **kwargs: Any) -> Any:
        """Output a log message at 'INFO' level.

        Args:
             event: Log message.
             *args: Any args.
             **kwargs: Any kwargs.
        """

    def warning(self, event: str, *args: Any, **kwargs: Any) -> Any:
        """Output a log message at 'WARNING' level.

        Args:
             event: Log message.
             *args: Any args.
             **kwargs: Any kwargs.
        """

    def warn(self, event: str, *args: Any, **kwargs: Any) -> Any:
        """Output a log message at 'WARN' level.

        Args:
             event: Log message.
             *args: Any args.
             **kwargs: Any kwargs.
        """

    def error(self, event: str, *args: Any, **kwargs: Any) -> Any:
        """Output a log message at 'ERROR' level.

        Args:
             event: Log message.
             *args: Any args.
             **kwargs: Any kwargs.
        """

    def fatal(self, event: str, *args: Any, **kwargs: Any) -> Any:
        """Output a log message at 'FATAL' level.

        Args:
             event: Log message.
             *args: Any args.
             **kwargs: Any kwargs.
        """

    def exception(self, event: str, *args: Any, **kwargs: Any) -> Any:
        """Log a message with level 'ERROR' on this logger. The arguments are interpreted as for debug(). Exception info
        is added to the logging message.

        Args:
             event: Log message.
             *args: Any args.
             **kwargs: Any kwargs.
        """

    def critical(self, event: str, *args: Any, **kwargs: Any) -> Any:
        """Output a log message at 'INFO' level.

        Args:
             event: Log message.
             *args: Any args.
             **kwargs: Any kwargs.
        """

    def setLevel(self, level: int) -> None:  # noqa: N802
        """Set the log level

        Args:
            level: Log level to set as an integer

        Returns:
            None
        """


@runtime_checkable
class DataclassProtocol(Protocol):
    """Protocol for instance checking dataclasses"""

    __dataclass_fields__: ClassVar[dict[str, Any]]


T_co = TypeVar("T_co", covariant=True)


@runtime_checkable
class InstantiableCollection(Collection[T_co], Protocol[T_co]):
    """A protocol for instantiable collection types."""

    def __init__(self, iterable: Iterable[T_co], /) -> None: ...


class StatementType(Enum):
    """Enumeration of SQL operation types."""

    INSERT_UPDATE_DELETE_RETURNING = 0
    INSERT_UPDATE_DELETE = 1
    INSERT_UPDATE_DELETE_MANY = 2
    SCRIPT = 3
    SELECT = 4
    SELECT_ONE = 5
    SELECT_VALUE = 6
    BULK_SELECT = 7


class StatementDetails(NamedTuple):
    statement_name: str
    doc_comments: str
    operation_type: StatementType
    sql: str
    record_class: Any = None
    signature: inspect.Signature | None = None
    floc: tuple[Path | str, int] | None = None


class StatementFn(Protocol):
    __name__: str
    __signature__: inspect.Signature | None
    sql: str
    operation: StatementType

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...  # pragma: no cover


SQLStatements = dict[str, Union[StatementDetails, dict]]


class SyncDriverAdapterProtocol(Protocol):
    is_asyncio: bool = False

    def process_sql(self, op_type: StatementType, sql: str) -> str: ...  # pragma: no cover

    def select(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> list: ...  # pragma: no cover

    def select_one(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None: ...  # pragma: no cover

    def select_value(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    def select_cursor(
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
    ) -> int: ...  # pragma: no cover

    # TODO: Next major version introduce a return? Optional return?
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
    ) -> int: ...  # pragma: no cover

    def insert_returning(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    def execute_script(self, connection: Any, sql: str) -> str: ...  # pragma: no cover


class AsyncDriverAdapterProtocol(Protocol):
    is_asyncio: bool = True

    def process_sql(self, op_type: StatementType, sql: str) -> str: ...  # pragma: no cover

    async def select(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> list: ...  # pragma: no cover

    async def select_one(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
        record_class: Callable | None,
    ) -> Any | None: ...  # pragma: no cover

    async def select_value(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    async def select_cursor(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> AbstractAsyncContextManager[Any]: ...  # pragma: no cover

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
    ) -> int: ...  # pragma: no cover

    async def insert_update_delete_many(
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
    ) -> int: ...  # pragma: no cover

    def insert_returning(
        self,
        connection: Any,
        sql: str,
        parameters: list | dict,
    ) -> Any | None: ...  # pragma: no cover

    async def execute_script(self, connection: Any, sql: str) -> str: ...  # pragma: no cover


DriverAdapterProtocol = Union[SyncDriverAdapterProtocol, AsyncDriverAdapterProtocol]
