"""GizmoSQL integration tests for the ADBC adapter."""

from typing import TypeAlias

import pytest

from sqlspec import SQLResult
from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.adapters.adbc.core import detect_dialect
from sqlspec.exceptions import SQLParsingError, UniqueViolationError
from tests.integration.adapters.adbc.conftest import xfail_if_driver_missing

pytestmark = [pytest.mark.adbc, pytest.mark.xdist_group("gizmosql")]

SQLITE_FLIGHTSQL_RESULT_XFAIL = (
    "Tracked in sqlspec-z5q3.1: GizmoSQL SQLite FlightSQL result streams return inconsistent schemas with ADBC 1.11"
)
DUCKDB_DUPLICATE_KEY_XFAIL = (
    "Tracked in sqlspec-z5q3.2: GizmoSQL DuckDB suppresses duplicate-key errors over FlightSQL ADBC 1.11"
)

GizmoSQLSessionCase: TypeAlias = tuple[AdbcDriver, str]
GizmoSQLConfigCase: TypeAlias = tuple[AdbcConfig, str]


@pytest.fixture(params=[pytest.param(("adbc_gizmosql_session", "duckdb"), id="duckdb")])
def gizmosql_session_case(request: pytest.FixtureRequest) -> GizmoSQLSessionCase:
    """Return a GizmoSQL session and expected backend dialect."""

    fixture_name, expected_dialect = request.param
    return request.getfixturevalue(fixture_name), expected_dialect


@pytest.fixture(
    params=[
        pytest.param(("adbc_gizmosql_config", "duckdb"), id="duckdb"),
        pytest.param(("adbc_gizmosql_sqlite_config", "sqlite"), id="sqlite"),
    ]
)
def gizmosql_config_case(request: pytest.FixtureRequest) -> GizmoSQLConfigCase:
    """Return a GizmoSQL config and expected backend dialect."""

    fixture_name, expected_dialect = request.param
    return request.getfixturevalue(fixture_name), expected_dialect


def _count_rows(session: AdbcDriver, name: str) -> int:
    result = session.execute("SELECT COUNT(*) AS count FROM test_table_adbc WHERE name = ?", (name,))
    assert result.data is not None
    return int(result.get_data()[0]["count"])


@xfail_if_driver_missing
def test_gizmosql_provides_connection_and_session(gizmosql_config_case: GizmoSQLConfigCase) -> None:
    """GizmoSQL configs should provide raw ADBC connections and sqlspec sessions."""
    config, expected_dialect = gizmosql_config_case

    with config.provide_connection() as connection:
        assert connection is not None
        assert detect_dialect(connection) == expected_dialect

    with config.provide_session() as session:
        assert isinstance(session, AdbcDriver)
        assert session.dialect == expected_dialect


@xfail_if_driver_missing
@pytest.mark.xfail(reason=SQLITE_FLIGHTSQL_RESULT_XFAIL, strict=False)
def test_gizmosql_sqlite_result_streams_fetch(adbc_gizmosql_sqlite_config: AdbcConfig) -> None:
    """GizmoSQL SQLite should return stable FlightSQL result schemas."""
    with adbc_gizmosql_sqlite_config.provide_session() as session:
        result = session.execute("SELECT 1 AS value")

    assert result.data is not None
    assert result.get_data() == [{"value": 1}]


@xfail_if_driver_missing
def test_gizmosql_basic_crud(gizmosql_session_case: GizmoSQLSessionCase) -> None:
    """GizmoSQL should support basic CRUD through the ADBC driver."""
    session, _expected_dialect = gizmosql_session_case

    insert_result = session.execute("INSERT INTO test_table_adbc (id, name, value) VALUES (?, ?, ?)", (1, "one", 10))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (-1, 0, 1)

    select_result = session.execute("SELECT name, value FROM test_table_adbc WHERE id = ?", (1,))
    assert select_result.data is not None
    assert select_result.get_data() == [{"name": "one", "value": 10}]

    update_result = session.execute("UPDATE test_table_adbc SET value = ? WHERE id = ?", (20, 1))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected in (-1, 0, 1)

    verify_result = session.execute("SELECT value FROM test_table_adbc WHERE id = ?", (1,))
    assert verify_result.data is not None
    assert verify_result.get_data()[0]["value"] == 20

    delete_result = session.execute("DELETE FROM test_table_adbc WHERE id = ?", (1,))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected in (-1, 0, 1)
    assert _count_rows(session, "one") == 0


@xfail_if_driver_missing
def test_gizmosql_execute_script_consumes_count_streams(gizmosql_session_case: GizmoSQLSessionCase) -> None:
    """GizmoSQL script execution should consume FlightSQL DDL/DML count streams."""
    session, _expected_dialect = gizmosql_session_case

    result = session.execute_script(
        """
            DROP TABLE IF EXISTS gizmosql_script_test_adbc;
            CREATE TABLE gizmosql_script_test_adbc (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            );
            INSERT INTO gizmosql_script_test_adbc (id, name, value) VALUES (1, 'script-one', 1);
            UPDATE gizmosql_script_test_adbc SET value = 2 WHERE id = 1;
        """
    )

    assert isinstance(result, SQLResult)
    select_result = session.execute("SELECT name, value FROM gizmosql_script_test_adbc WHERE id = ?", (1,))
    assert select_result.data is not None
    assert select_result.get_data() == [{"name": "script-one", "value": 2}]


@xfail_if_driver_missing
def test_gizmosql_qmark_parameters_accept_positional_lists(gizmosql_session_case: GizmoSQLSessionCase) -> None:
    """GizmoSQL should execute qmark parameters with positional list values."""
    session, _expected_dialect = gizmosql_session_case

    session.execute("INSERT INTO test_table_adbc (id, name, value) VALUES (?, ?, ?)", [2, "list-bind", 30])

    result = session.execute("SELECT value FROM test_table_adbc WHERE name = ?", ["list-bind"])
    assert result.data is not None
    assert result.get_data()[0]["value"] == 30


@xfail_if_driver_missing
def test_gizmosql_numeric_parameters_execute(gizmosql_session_case: GizmoSQLSessionCase) -> None:
    """GizmoSQL should execute numeric parameters through FlightSQL."""
    session, _expected_dialect = gizmosql_session_case

    session.execute("INSERT INTO test_table_adbc (id, name, value) VALUES ($1, $2, $3)", (3, "numeric-bind", 40))

    result = session.execute("SELECT value FROM test_table_adbc WHERE name = $1", ("numeric-bind",))
    assert result.data is not None
    assert result.get_data()[0]["value"] == 40


@xfail_if_driver_missing
def test_gizmosql_execute_many_reports_bulk_count(gizmosql_session_case: GizmoSQLSessionCase) -> None:
    """GizmoSQL executemany should report the submitted parameter-set count."""
    session, _expected_dialect = gizmosql_session_case
    parameters = [(10, "bulk-one", 1), (11, "bulk-two", 2), (12, "bulk-three", 3)]

    result = session.execute_many("INSERT INTO test_table_adbc (id, name, value) VALUES (?, ?, ?)", parameters)

    assert result.rows_affected == len(parameters)
    count_result = session.execute("SELECT COUNT(*) AS count FROM test_table_adbc WHERE name LIKE ?", ("bulk-%",))
    assert count_result.data is not None
    assert count_result.get_data()[0]["count"] == len(parameters)


@xfail_if_driver_missing
def test_gizmosql_transactions_commit_and_rollback(gizmosql_session_case: GizmoSQLSessionCase) -> None:
    """GizmoSQL should honor explicit begin, commit, and rollback calls."""
    session, _expected_dialect = gizmosql_session_case

    session.begin()
    session.execute("INSERT INTO test_table_adbc (id, name, value) VALUES (?, ?, ?)", (20, "rolled-back", 1))
    session.rollback()
    assert _count_rows(session, "rolled-back") == 0

    session.begin()
    session.execute("INSERT INTO test_table_adbc (id, name, value) VALUES (?, ?, ?)", (21, "committed", 2))
    session.commit()
    assert _count_rows(session, "committed") == 1


@xfail_if_driver_missing
@pytest.mark.xfail(reason=DUCKDB_DUPLICATE_KEY_XFAIL, strict=False)
def test_gizmosql_unique_violation_maps_to_sqlspec_exception(gizmosql_session_case: GizmoSQLSessionCase) -> None:
    """GizmoSQL duplicate primary keys should map to UniqueViolationError."""
    session, _expected_dialect = gizmosql_session_case

    session.execute("INSERT INTO test_table_adbc (id, name, value) VALUES (?, ?, ?)", (30, "unique-one", 1))

    with pytest.raises(UniqueViolationError):
        session.execute("INSERT INTO test_table_adbc (id, name, value) VALUES (?, ?, ?)", (30, "unique-two", 2))


@xfail_if_driver_missing
def test_gizmosql_syntax_error_maps_to_sql_parsing_error(gizmosql_session_case: GizmoSQLSessionCase) -> None:
    """GizmoSQL parser errors should map to SQLParsingError."""
    session, _expected_dialect = gizmosql_session_case

    with pytest.raises(SQLParsingError):
        session.execute("SELCT * FROM test_table_adbc")


@xfail_if_driver_missing
def test_gizmosql_explain_select(gizmosql_session_case: GizmoSQLSessionCase) -> None:
    """GizmoSQL should execute EXPLAIN SELECT end to end."""
    session, _expected_dialect = gizmosql_session_case

    result = session.execute("EXPLAIN SELECT * FROM test_table_adbc")

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 1
