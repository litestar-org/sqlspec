"""Integration tests for the mssql-python cache-hit executor against SQL Server."""

from typing import Any

import pytest

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.core import execute_cache_hit
from sqlspec.exceptions import UniqueViolationError

pytestmark = [pytest.mark.mssql_python, pytest.mark.xdist_group("mssql")]

_DROP_TABLE_SQL = "DROP TABLE IF EXISTS sqlspec_mssql_python_cache_hit"
_CREATE_TABLE_SQL = "CREATE TABLE sqlspec_mssql_python_cache_hit (code VARCHAR(20) PRIMARY KEY, label VARCHAR(50))"
_INSERT_SQL = "INSERT INTO sqlspec_mssql_python_cache_hit (code, label) VALUES (?, ?)"
_SELECT_SQL = "SELECT code, label FROM sqlspec_mssql_python_cache_hit WHERE code = ?"
_SELECT_ALL_SQL = "SELECT code, label FROM sqlspec_mssql_python_cache_hit ORDER BY code"


def test_cache_hit_executor_matches_general_path(
    mssql_python_config: MssqlPythonConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    with mssql_python_config.provide_session() as session:
        session.execute_script(_DROP_TABLE_SQL)
        session.execute_script(_CREATE_TABLE_SQL)
        session.commit()
        calls: list[tuple[Any, ...]] = []
        outcomes: list[Any] = []

        def recording_executor(*args: Any) -> Any:
            calls.append(args)
            outcome = execute_cache_hit(*args)
            outcomes.append(outcome)
            return outcome

        monkeypatch.setattr(session, "_cache_hit_executor", recording_executor)
        try:
            session.execute(_INSERT_SQL, ("a", "one"))
            inserted = session.execute(_INSERT_SQL, ("b", "two"))
            general = session.execute(_SELECT_SQL, ("a",))
            cached = session.execute(_SELECT_SQL, ("b",))
            session.execute(_SELECT_ALL_SQL)
            everything = session.execute(_SELECT_ALL_SQL)

            assert outcomes == [inserted, cached, everything]
            assert inserted.rows_affected == 1
            assert general.get_data() == [{"code": "a", "label": "one"}]
            assert cached.get_data() == [{"code": "b", "label": "two"}]
            assert cached.column_names == general.column_names
            assert cached.rows_affected == 1
            assert everything.get_data() == [{"code": "a", "label": "one"}, {"code": "b", "label": "two"}]

            with pytest.raises(UniqueViolationError):
                session.execute(_INSERT_SQL, ("a", "duplicate"))

            assert len(calls) == 4
        finally:
            session.rollback()
            session.execute_script(_DROP_TABLE_SQL)
            session.commit()
