"""Compiled base session context managers coordinating driver lifecycle."""

import contextlib
from typing import TYPE_CHECKING, Any

from mypy_extensions import mypyc_attr

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from sqlspec.core.statement import StatementConfig

__all__ = ("AsyncDriverSessionContext", "SyncDriverSessionContext")


@mypyc_attr(allow_interpreted_subclasses=True)
class SyncDriverSessionContext:
    """Base sync context manager coordinating connection acquisition, driver creation, and cleanup."""

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_class",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig | Callable[[], StatementConfig]",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[Any], Any]",
        driver_class: "type[Any] | None" = None,
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_class = driver_class
        self._connection: Any = None
        self._driver: Any = None

    def _resolve_driver_class(self) -> "type[Any]":
        msg = f"{type(self).__name__} must define _resolve_driver_class() or pass driver_class"
        raise NotImplementedError(msg)

    def _cleanup_driver(self, driver: Any, *, error: bool) -> None:
        driver._close_open_streams(error=error)

    def __enter__(self) -> Any:
        driver_cls = self._driver_class
        if driver_cls is None:
            driver_cls = self._resolve_driver_class()
        self._connection = self._acquire_connection()
        try:
            statement_config = self._statement_config() if callable(self._statement_config) else self._statement_config
            self._driver = driver_cls(
                connection=self._connection, statement_config=statement_config, driver_features=self._driver_features
            )
            return self._prepare_driver(self._driver)
        except BaseException as exc:
            with contextlib.suppress(Exception):
                self.__exit__(type(exc), exc, exc.__traceback__)
            raise

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        driver = self._driver
        try:
            if driver is not None:
                self._cleanup_driver(driver, error=exc_type is not None)
        except BaseException as close_exc:
            if exc_type is None:
                exc_type, exc_val, exc_tb = type(close_exc), close_exc, close_exc.__traceback__
            raise
        finally:
            try:
                if self._connection is not None:
                    self._release_connection(self._connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
                    self._connection = None
            finally:
                self._driver = None
                if driver is not None:
                    driver._mark_closed()
        return None


@mypyc_attr(allow_interpreted_subclasses=True)
class AsyncDriverSessionContext:
    """Base async context manager coordinating connection acquisition, driver creation, and cleanup."""

    __slots__ = (
        "_acquire_connection",
        "_connection",
        "_driver",
        "_driver_class",
        "_driver_features",
        "_prepare_driver",
        "_release_connection",
        "_statement_config",
    )

    def __init__(
        self,
        acquire_connection: "Callable[[], Any]",
        release_connection: "Callable[..., Any]",
        statement_config: "StatementConfig | Callable[[], StatementConfig]",
        driver_features: "dict[str, Any]",
        prepare_driver: "Callable[[Any], Any]",
        driver_class: "type[Any] | None" = None,
    ) -> None:
        self._acquire_connection = acquire_connection
        self._release_connection = release_connection
        self._statement_config = statement_config
        self._driver_features = driver_features
        self._prepare_driver = prepare_driver
        self._driver_class = driver_class
        self._connection: Any = None
        self._driver: Any = None

    def _resolve_driver_class(self) -> "type[Any]":
        msg = f"{type(self).__name__} must define _resolve_driver_class() or pass driver_class"
        raise NotImplementedError(msg)

    async def __aenter__(self) -> Any:
        driver_cls = self._driver_class
        if driver_cls is None:
            driver_cls = self._resolve_driver_class()
        self._connection = await self._acquire_connection()
        enter_error: BaseException | None = None
        try:
            statement_config = self._statement_config() if callable(self._statement_config) else self._statement_config
            self._driver = driver_cls(
                connection=self._connection, statement_config=statement_config, driver_features=self._driver_features
            )
            return self._prepare_driver(self._driver)
        except BaseException as exc:
            enter_error = exc
        with contextlib.suppress(Exception):
            await self.__aexit__(type(enter_error), enter_error, enter_error.__traceback__)
        raise enter_error

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        driver = self._driver
        close_error: BaseException | None = None
        release_error: BaseException | None = None
        if driver is not None:
            try:
                await driver._close_open_streams(error=exc_type is not None)
            except BaseException as close_exc:
                close_error = close_exc
                if exc_type is None:
                    exc_type = type(close_exc)
                    exc_val = close_exc
                    exc_tb = close_exc.__traceback__
        if self._connection is not None:
            connection = self._connection
            self._connection = None
            try:
                await self._release_connection(connection, exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            except BaseException as rel_exc:
                release_error = rel_exc
        self._driver = None
        if driver is not None:
            driver._mark_closed()
        if close_error is not None:
            raise close_error
        if release_error is not None:
            raise release_error
        return None
