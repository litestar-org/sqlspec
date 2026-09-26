"""Integration tests for IBM Db2 behavior outside the shared adapter contracts."""

from collections.abc import Generator
from decimal import Decimal
from typing import Any

import pytest

pytest.importorskip("ibm_db")
pytest.importorskip("ibm_db_dbi")

from sqlspec.adapters.db2.driver import Db2SyncDriver

pytestmark = [pytest.mark.db2, pytest.mark.xdist_group("db2")]


_USERS_TABLE_EXISTS_SQL = (
    "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = CURRENT SCHEMA AND TABNAME = 'TEST_INTEGRATION_USERS'"
)


def _drop_users_table(session: Db2SyncDriver) -> None:
    if session.select_value(_USERS_TABLE_EXISTS_SQL):
        session.execute_script("DROP TABLE TEST_INTEGRATION_USERS")
        session.commit()


@pytest.fixture
def clean_users_table(db2_session: Db2SyncDriver) -> Generator[None, None, None]:
    """Create ``TEST_INTEGRATION_USERS`` fresh and drop it after the test."""
    _drop_users_table(db2_session)
    db2_session.execute_script(
        "CREATE TABLE TEST_INTEGRATION_USERS ("
        "id INT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL, balance DECFLOAT(34), label VARGRAPHIC(32)"
        ")"
    )
    db2_session.commit()
    yield
    db2_session.rollback()
    _drop_users_table(db2_session)


def test_db2_integration_connection_ping(db2_session: Db2SyncDriver) -> None:
    """A live session answers a query against ``SYSIBM.SYSDUMMY1``."""
    assert db2_session.select_value("SELECT 1 FROM SYSIBM.SYSDUMMY1") == 1


def test_db2_special_registers(db2_session: Db2SyncDriver, db2_connection_config: "dict[str, Any]") -> None:
    """Special registers report the connected database and the user's default schema."""
    row = db2_session.select_one(
        "SELECT CURRENT SERVER AS server, CURRENT SCHEMA AS current_schema FROM SYSIBM.SYSDUMMY1"
    )
    assert row["server"].strip() == db2_connection_config["database"].upper()
    assert row["current_schema"].strip() == db2_connection_config["user"].upper()


def test_db2_uncommitted_read_isolation_clause(db2_session: Db2SyncDriver, clean_users_table: None) -> None:
    """A query carrying ``WITH UR`` binds its parameters and returns committed rows."""
    db2_session.execute("INSERT INTO TEST_INTEGRATION_USERS (id, name) VALUES (?, ?)", (1, "reader"))
    db2_session.commit()

    rows = db2_session.select("SELECT id, name FROM TEST_INTEGRATION_USERS WHERE id = ? WITH UR", (1,))

    assert rows == [{"id": 1, "name": "reader"}]


def test_db2_decfloat_and_graphic_round_trip(db2_session: Db2SyncDriver, clean_users_table: None) -> None:
    """DECFLOAT keeps decimal precision and VARGRAPHIC keeps non-ASCII text."""
    balance = Decimal("12345678901234567890.123456789")
    label = "Grüße 東京"
    db2_session.execute(
        "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance, label) VALUES (?, ?, ?, ?)",
        (7, "decimal", balance, label),
    )
    db2_session.commit()

    row = db2_session.select_one("SELECT balance, label FROM TEST_INTEGRATION_USERS WHERE id = ?", (7,))

    assert Decimal(str(row["balance"])) == balance
    assert row["label"] == label


def test_db2_savepoint_rollback_retains_open_cursor(db2_session: Db2SyncDriver, clean_users_table: None) -> None:
    """Rolling back to a savepoint discards later work and keeps a cursor opened before it readable."""
    db2_session.execute_many(
        "INSERT INTO TEST_INTEGRATION_USERS (id, name) VALUES (?, ?)", [(30, "first"), (31, "second")]
    )
    db2_session.commit()

    db2_session.begin()
    with db2_session.with_cursor(db2_session.connection) as cursor:
        cursor.execute("SELECT id FROM TEST_INTEGRATION_USERS ORDER BY id")
        first = cursor.fetchone()
        db2_session.create_savepoint("sp_partial")
        db2_session.execute("INSERT INTO TEST_INTEGRATION_USERS (id, name) VALUES (?, ?)", (32, "discarded"))
        db2_session.rollback_to_savepoint("sp_partial")
        second = cursor.fetchone()
    db2_session.commit()

    assert [first[0], second[0]] == [30, 31]
    assert db2_session.select_value("SELECT COUNT(*) FROM TEST_INTEGRATION_USERS WHERE id = ?", (32,)) == 0


def test_db2_integration_arrow_conversion(db2_session: Db2SyncDriver, clean_users_table: None) -> None:
    """``select_to_arrow`` converts Db2 rows into a pyarrow table."""
    pa = pytest.importorskip("pyarrow")

    db2_session.execute("INSERT INTO TEST_INTEGRATION_USERS (id, name) VALUES (?, ?)", (42, "ArrowUser"))
    db2_session.commit()

    result = db2_session.select_to_arrow("SELECT id, name FROM TEST_INTEGRATION_USERS WHERE id = ?", (42,))
    table = result.get_data()

    assert isinstance(table, pa.Table)
    assert table.to_pydict() == {"id": [42], "name": ["ArrowUser"]}
