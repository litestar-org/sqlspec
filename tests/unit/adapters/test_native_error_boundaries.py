"""Driver exception handlers leave application exceptions untouched."""

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from mssql_python import Error as MssqlPythonError
from pymssql import Error as PymssqlError

from sqlspec.adapters.arrow_odbc.driver import ArrowOdbcExceptionHandler
from sqlspec.adapters.mssql_python.adk import MssqlPythonADKStore
from sqlspec.adapters.mssql_python.driver import MssqlPythonExceptionHandler
from sqlspec.adapters.pymssql.adk import PymssqlADKStore
from sqlspec.adapters.pymssql.driver import PymssqlExceptionHandler
from sqlspec.driver import BaseSyncExceptionHandler


@pytest.mark.parametrize(
    "handler_type", [ArrowOdbcExceptionHandler, MssqlPythonExceptionHandler, PymssqlExceptionHandler]
)
def test_native_error_handler_preserves_application_error(handler_type: type[BaseSyncExceptionHandler]) -> None:
    error = ValueError("invalid application value")
    with pytest.raises(ValueError) as caught, handler_type():
        raise error
    assert caught.value is error


@pytest.mark.parametrize(
    ("store_type", "error_factory"),
    [(PymssqlADKStore, PymssqlError), (MssqlPythonADKStore, lambda message: MssqlPythonError(message, ""))],
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
@pytest.mark.parametrize("error_kind", ["missing_table", "other_database", "application"])
def test_sql_server_store_error_boundary(
    store_type: Callable[[Any], Any],
    error_factory: Callable[[str], Exception],
    method: str,
    args: tuple[Any, ...],
    empty_result: Any,
    error_kind: str,
) -> None:
    config = MagicMock()
    config.extension_config = {"adk": {}}
    error: Exception
    if error_kind == "application":
        error = ValueError("invalid object name in application input")
    else:
        error = error_factory("invalid object name (208)" if error_kind == "missing_table" else "syntax error (102)")
    config.provide_connection.side_effect = error
    store = store_type(config)
    if error_kind == "missing_table":
        assert getattr(store, method)(*args) == empty_result
    else:
        with pytest.raises(type(error)) as caught:
            getattr(store, method)(*args)
        assert caught.value is error
