"""MySQL ADK stores preserve native missing-table and unrelated error behavior."""

import inspect
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from aiomysql import ProgrammingError as AiomysqlProgrammingError
from asyncmy.errors import ProgrammingError as AsyncmyProgrammingError
from mysql.connector import Error as MysqlConnectorError
from pymysql.err import ProgrammingError as PyMysqlProgrammingError

from sqlspec.adapters.aiomysql.adk import AiomysqlADKStore
from sqlspec.adapters.aiomysql.litestar import AiomysqlStore
from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore
from sqlspec.adapters.asyncmy.litestar import AsyncmyStore
from sqlspec.adapters.mysqlconnector.adk import MysqlConnectorAsyncADKStore, MysqlConnectorSyncADKStore
from sqlspec.adapters.mysqlconnector.litestar import MysqlConnectorAsyncStore, MysqlConnectorSyncStore
from sqlspec.adapters.pymysql.adk import PyMysqlADKStore
from sqlspec.adapters.pymysql.litestar import PyMysqlStore


@pytest.mark.parametrize(
    ("store_factory", "error_factory"),
    [
        (AiomysqlADKStore, AiomysqlProgrammingError),
        (AsyncmyADKStore, AsyncmyProgrammingError),
        (PyMysqlADKStore, PyMysqlProgrammingError),
        (MysqlConnectorAsyncADKStore, lambda code, message: MysqlConnectorError(msg=message, errno=code)),
        (MysqlConnectorSyncADKStore, lambda code, message: MysqlConnectorError(msg=message, errno=code)),
    ],
)
@pytest.mark.parametrize(
    ("method", "args", "empty_result"),
    [
        ("get_session", ("app", "user", "session"), None),
        ("list_sessions", ("app", "user"), []),
        ("get_events", ("app", "user", "session"), []),
        ("delete_expired_events", (datetime(2026, 1, 1, tzinfo=timezone.utc),), 0),
        ("delete_idle_sessions", (datetime(2026, 1, 1, tzinfo=timezone.utc),), 0),
        ("delete_idle_user_states", (datetime(2026, 1, 1, tzinfo=timezone.utc),), 0),
        ("get_app_state", ("app",), None),
        ("get_user_state", ("app", "user"), None),
        ("get_metadata", ("key",), None),
    ],
)
@pytest.mark.parametrize("missing_table", [True, False])
async def test_mysql_adk_native_errors(
    store_factory: Callable[[Any], Any],
    error_factory: Callable[[int, str], Exception],
    method: str,
    args: tuple[Any, ...],
    empty_result: Any,
    missing_table: bool,
) -> None:
    config = MagicMock()
    config.extension_config = {"adk": {}}
    error = error_factory(1146 if missing_table else 1064, "native database error")
    config.provide_connection.side_effect = error
    store = store_factory(config)

    async def invoke() -> Any:
        result = getattr(store, method)(*args)
        return await result if inspect.isawaitable(result) else result

    if missing_table:
        assert await invoke() == empty_result
    else:
        with pytest.raises(type(error)) as caught:
            await invoke()
        assert caught.value is error


@pytest.mark.parametrize(
    ("store_factory", "error_factory"),
    [
        (AiomysqlStore, AiomysqlProgrammingError),
        (AsyncmyStore, AsyncmyProgrammingError),
        (PyMysqlStore, PyMysqlProgrammingError),
        (MysqlConnectorAsyncStore, lambda code, message: MysqlConnectorError(msg=message, errno=code)),
        (MysqlConnectorSyncStore, lambda code, message: MysqlConnectorError(msg=message, errno=code)),
    ],
)
@pytest.mark.parametrize(
    ("method", "args", "empty_result"), [("get", ("key",), None), ("exists", ("key",), False), ("delete_all", (), None)]
)
@pytest.mark.parametrize("missing_table", [True, False])
async def test_mysql_litestar_native_errors(
    store_factory: Callable[[Any], Any],
    error_factory: Callable[[int, str], Exception],
    method: str,
    args: tuple[Any, ...],
    empty_result: Any,
    missing_table: bool,
) -> None:
    config = MagicMock()
    config.extension_config = {"litestar": {}}
    error = error_factory(1146 if missing_table else 1064, "native database error")
    config.provide_connection.side_effect = error
    store = store_factory(config)
    if missing_table:
        assert await getattr(store, method)(*args) == empty_result
    else:
        with pytest.raises(type(error)) as caught:
            await getattr(store, method)(*args)
        assert caught.value is error
