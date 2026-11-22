"""Integration tests for the Oracle cache-hit executors."""

from typing import Any

import pytest

from sqlspec.adapters.oracledb import OracleAsyncDriver, OracleSyncDriver
from sqlspec.adapters.oracledb.core import execute_cache_hit, execute_cache_hit_async
from sqlspec.exceptions import UniqueViolationError

pytestmark = pytest.mark.xdist_group("oracle")

_SYNC_TABLE = "test_cache_hit_oracledb_sync"
_ASYNC_TABLE = "test_cache_hit_oracledb_async"
_SYNC_ROWID_TABLE = "test_rowid_oracledb_sync"
_ASYNC_ROWID_TABLE = "test_rowid_oracledb_async"


def _drop_sql(table: str) -> str:
    return (
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}';"
        " EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def _create_sql(table: str) -> str:
    return f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, body CLOB)"


def _insert_sql(table: str) -> str:
    return f"INSERT INTO {table} (id, body) VALUES (1, 'one')"


def _select_sql(table: str) -> str:
    return f"SELECT id, body FROM {table} ORDER BY id"


def _update_sql(table: str) -> str:
    return f"UPDATE {table} SET body = 'two'"


def _insert_row_sql(table: str) -> str:
    return f"INSERT INTO {table} (id, body) VALUES (:1, :2)"


def _select_id_by_rowid_sql(table: str) -> str:
    return f"SELECT id FROM {table} WHERE ROWID = :1"


def _update_row_sql(table: str) -> str:
    return f"UPDATE {table} SET body = :1 WHERE id = :2"


def _delete_row_sql(table: str) -> str:
    return f"DELETE FROM {table} WHERE id = :1"


def _assert_insert_rowids(results: "list[Any]") -> None:
    rowids = [result.last_inserted_id for result in results]
    assert all(isinstance(rowid, str) and rowid for rowid in rowids)
    assert len(set(rowids)) == len(rowids)


def _assert_cache_hit_results(general: Any, cached: Any, updated: Any, outcomes: "list[Any]") -> None:
    assert outcomes == [cached, updated]
    assert general.get_data() == [{"id": 1, "body": "one"}]
    assert cached.get_data() == general.get_data()
    assert cached.column_names == general.column_names
    assert cached.rows_affected == 1
    assert updated.rows_affected == 1


def test_sync_cache_hit_executor_matches_general_path(
    oracle_sync_session: OracleSyncDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    session = oracle_sync_session
    session.execute_script(_drop_sql(_SYNC_TABLE))
    session.execute_script(_create_sql(_SYNC_TABLE))
    calls: list[tuple[Any, ...]] = []
    outcomes: list[Any] = []

    def recording_executor(*args: Any) -> Any:
        calls.append(args)
        outcome = execute_cache_hit(*args)
        outcomes.append(outcome)
        return outcome

    monkeypatch.setattr(session, "_cache_hit_executor", recording_executor)
    try:
        session.execute(_insert_sql(_SYNC_TABLE))
        general = session.execute(_select_sql(_SYNC_TABLE))
        cached = session.execute(_select_sql(_SYNC_TABLE))
        session.execute(_update_sql(_SYNC_TABLE))
        updated = session.execute(_update_sql(_SYNC_TABLE))

        _assert_cache_hit_results(general, cached, updated, outcomes)

        with pytest.raises(UniqueViolationError):
            session.execute(_insert_sql(_SYNC_TABLE))

        assert len(calls) == 3
    finally:
        session.rollback()
        session.execute_script(_drop_sql(_SYNC_TABLE))


async def test_async_cache_hit_executor_matches_general_path(
    oracle_async_session: OracleAsyncDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    session = oracle_async_session
    await session.execute_script(_drop_sql(_ASYNC_TABLE))
    await session.execute_script(_create_sql(_ASYNC_TABLE))
    calls: list[tuple[Any, ...]] = []
    outcomes: list[Any] = []

    async def recording_executor(*args: Any) -> Any:
        calls.append(args)
        outcome = await execute_cache_hit_async(*args)
        outcomes.append(outcome)
        return outcome

    monkeypatch.setattr(session, "_cache_hit_executor", recording_executor)
    try:
        await session.execute(_insert_sql(_ASYNC_TABLE))
        general = await session.execute(_select_sql(_ASYNC_TABLE))
        cached = await session.execute(_select_sql(_ASYNC_TABLE))
        await session.execute(_update_sql(_ASYNC_TABLE))
        updated = await session.execute(_update_sql(_ASYNC_TABLE))

        _assert_cache_hit_results(general, cached, updated, outcomes)

        with pytest.raises(UniqueViolationError):
            await session.execute(_insert_sql(_ASYNC_TABLE))

        assert len(calls) == 3
    finally:
        await session.rollback()
        await session.execute_script(_drop_sql(_ASYNC_TABLE))


def test_sync_insert_reports_rowid_on_every_execution_path(oracle_sync_session: OracleSyncDriver) -> None:
    session = oracle_sync_session
    session.execute_script(_drop_sql(_SYNC_ROWID_TABLE))
    session.execute_script(_create_sql(_SYNC_ROWID_TABLE))
    try:
        first = session.execute(_insert_row_sql(_SYNC_ROWID_TABLE), 1, "one")
        cached = session.execute(_insert_row_sql(_SYNC_ROWID_TABLE), 2, "two")
        assert session.set_cached_statement_executor(None) is True
        without_executor = session.execute(_insert_row_sql(_SYNC_ROWID_TABLE), 3, "three")

        _assert_insert_rowids([first, cached, without_executor])
        located = [
            session.select_value(_select_id_by_rowid_sql(_SYNC_ROWID_TABLE), result.last_inserted_id)
            for result in (first, cached, without_executor)
        ]
        assert located == [1, 2, 3]

        updated = session.execute(_update_row_sql(_SYNC_ROWID_TABLE), "changed", 1)
        deleted = session.execute(_delete_row_sql(_SYNC_ROWID_TABLE), 1)
        assert updated.rows_affected == 1
        assert updated.last_inserted_id is None
        assert deleted.rows_affected == 1
        assert deleted.last_inserted_id is None
    finally:
        session.rollback()
        session.execute_script(_drop_sql(_SYNC_ROWID_TABLE))


async def test_async_insert_reports_rowid_on_every_execution_path(oracle_async_session: OracleAsyncDriver) -> None:
    session = oracle_async_session
    await session.execute_script(_drop_sql(_ASYNC_ROWID_TABLE))
    await session.execute_script(_create_sql(_ASYNC_ROWID_TABLE))
    try:
        first = await session.execute(_insert_row_sql(_ASYNC_ROWID_TABLE), 1, "one")
        cached = await session.execute(_insert_row_sql(_ASYNC_ROWID_TABLE), 2, "two")
        assert session.set_cached_statement_executor(None) is True
        without_executor = await session.execute(_insert_row_sql(_ASYNC_ROWID_TABLE), 3, "three")

        _assert_insert_rowids([first, cached, without_executor])
        located = [
            await session.select_value(_select_id_by_rowid_sql(_ASYNC_ROWID_TABLE), result.last_inserted_id)
            for result in (first, cached, without_executor)
        ]
        assert located == [1, 2, 3]

        updated = await session.execute(_update_row_sql(_ASYNC_ROWID_TABLE), "changed", 1)
        deleted = await session.execute(_delete_row_sql(_ASYNC_ROWID_TABLE), 1)
        assert updated.rows_affected == 1
        assert updated.last_inserted_id is None
        assert deleted.rows_affected == 1
        assert deleted.last_inserted_id is None
    finally:
        await session.rollback()
        await session.execute_script(_drop_sql(_ASYNC_ROWID_TABLE))
