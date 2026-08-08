"""Tests for shared SQLSpec exception contracts."""

import sqlite3

import asyncpg
from google.api_core import exceptions as api_exceptions
from psycopg import errors as pg_errors

from sqlspec.adapters.asyncpg.core import create_mapped_exception as map_asyncpg_error
from sqlspec.adapters.bigquery.core import create_mapped_exception as map_bigquery_error
from sqlspec.adapters.psqlpy.core import create_mapped_exception as map_psqlpy_error
from sqlspec.adapters.psycopg.core import create_mapped_exception as map_psycopg_error
from sqlspec.adapters.spanner.core import create_mapped_exception as map_spanner_error
from sqlspec.adapters.sqlite.core import create_mapped_exception as map_sqlite_error
from sqlspec.exceptions import OperationalError, OperationCancelledError, QueryTimeoutError, map_sqlstate_to_exception


def test_cancellation_and_timeout_are_sibling_operational_errors() -> None:
    assert issubclass(OperationCancelledError, OperationalError)
    assert issubclass(QueryTimeoutError, OperationalError)
    assert not issubclass(OperationCancelledError, QueryTimeoutError)
    assert not issubclass(QueryTimeoutError, OperationCancelledError)


def test_ambiguous_query_cancelled_sqlstate_is_operational_error() -> None:
    assert map_sqlstate_to_exception("57014") is OperationalError


def test_postgres_native_errors_distinguish_cancellation_timeout_and_ambiguity() -> None:
    for mapper, error_type in (
        (map_asyncpg_error, asyncpg.exceptions.QueryCanceledError),
        (map_psycopg_error, pg_errors.QueryCanceled),
    ):
        assert isinstance(mapper(error_type("canceling statement due to user request")), OperationCancelledError)
        assert isinstance(mapper(error_type("canceling statement due to statement timeout")), QueryTimeoutError)
        assert type(mapper(error_type("query terminated"))) is OperationalError


def test_postgres_sqlstate_fallback_distinguishes_timeout() -> None:
    class _PostgresError(Exception):
        sqlstate = "57014"

    for mapper in (map_asyncpg_error, map_psycopg_error):
        assert isinstance(mapper(_PostgresError("canceling statement due to statement timeout")), QueryTimeoutError)


def test_message_only_postgres_errors_prefer_timeout_to_cancellation() -> None:
    assert isinstance(map_psqlpy_error(Exception("query cancelled by user")), OperationCancelledError)
    assert isinstance(map_psqlpy_error(Exception("canceling statement due to statement timeout")), QueryTimeoutError)


def test_sqlite_interrupt_maps_to_operation_cancelled() -> None:
    assert isinstance(map_sqlite_error(sqlite3.OperationalError("query interrupted")), OperationCancelledError)

    error = sqlite3.OperationalError("interrupted")
    error.sqlite_errorcode = 9  # type: ignore[attr-defined]
    error.sqlite_errorname = "SQLITE_INTERRUPT"  # type: ignore[attr-defined]
    assert isinstance(map_sqlite_error(error), OperationCancelledError)


def test_bigquery_errors_distinguish_native_cancellation_and_timeout() -> None:
    assert isinstance(map_bigquery_error(api_exceptions.Cancelled("cancelled")), OperationCancelledError)  # type: ignore[no-untyped-call]
    assert isinstance(map_bigquery_error(Exception("deadline exceeded")), QueryTimeoutError)


def test_spanner_errors_distinguish_native_cancellation_and_deadline() -> None:
    assert isinstance(map_spanner_error(api_exceptions.Cancelled("cancelled")), OperationCancelledError)  # type: ignore[no-untyped-call]
    assert isinstance(map_spanner_error(api_exceptions.DeadlineExceeded("deadline exceeded")), QueryTimeoutError)  # type: ignore[no-untyped-call]
