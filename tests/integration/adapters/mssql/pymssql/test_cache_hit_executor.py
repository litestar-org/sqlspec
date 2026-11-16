"""Integration tests for the pymssql cache-hit executor against SQL Server."""

from typing import Any

import pytest

from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.adapters.pymssql.core import execute_cache_hit
from sqlspec.exceptions import UniqueViolationError

pytestmark = [pytest.mark.pymssql, pytest.mark.xdist_group("mssql")]

_DROP_TABLE_SQL = "DROP TABLE IF EXISTS sqlspec_pymssql_cache_hit"
_CREATE_TABLE_SQL = "CREATE TABLE sqlspec_pymssql_cache_hit (id INT PRIMARY KEY, label VARCHAR(50))"
_INSERT_SQL = "INSERT INTO sqlspec_pymssql_cache_hit (id, label) VALUES (?, ?)"
_SELECT_SQL = "SELECT id, label FROM sqlspec_pymssql_cache_hit WHERE id = ?"
_SELECT_LIKE_SQL = "SELECT id, label FROM sqlspec_pymssql_cache_hit WHERE label LIKE 't%' AND id = ?"
_SELECT_ALL_SQL = "SELECT id, label FROM sqlspec_pymssql_cache_hit ORDER BY id"


def test_cache_hit_executor_matches_general_path(
    pymssql_config: PymssqlConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pymssql_config.provide_session() as session:
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
            session.execute(_INSERT_SQL, (1, "one"))
            inserted = session.execute(_INSERT_SQL, (2, "two"))
            general = session.execute(_SELECT_SQL, (1,))
            cached = session.execute(_SELECT_SQL, (2,))
            session.execute(_SELECT_LIKE_SQL, (2,))
            literal_percent = session.execute(_SELECT_LIKE_SQL, (2,))
            session.execute(_SELECT_ALL_SQL)
            everything = session.execute(_SELECT_ALL_SQL)

            assert outcomes == [inserted, cached, None, None]
            assert inserted.rows_affected == 1
            assert general.get_data() == [{"id": 1, "label": "one"}]
            assert cached.get_data() == [{"id": 2, "label": "two"}]
            assert cached.column_names == general.column_names
            assert cached.rows_affected == 1
            assert literal_percent.get_data() == [{"id": 2, "label": "two"}]
            assert everything.get_data() == [{"id": 1, "label": "one"}, {"id": 2, "label": "two"}]

            with pytest.raises(UniqueViolationError):
                session.execute(_INSERT_SQL, (1, "duplicate"))

            assert len(calls) == 5
        finally:
            session.rollback()
            session.execute_script(_DROP_TABLE_SQL)
            session.commit()
