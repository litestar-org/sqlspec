"""Integration tests for the IBM Db2 adapter against live or mock container."""

import os
from collections.abc import Generator
from decimal import Decimal

import pytest

pytest.importorskip("ibm_db")
pytest.importorskip("ibm_db_dbi")

from sqlspec.adapters.db2.driver import Db2SyncDriver

DB2_INTEGRATION_ENABLED = bool(
    os.environ.get("DB2_HOST") or os.environ.get("SQLSPEC_ENABLE_DB2_INTEGRATION_TESTS") == "1"
)

pytestmark = [
    pytest.mark.db2,
    pytest.mark.xdist_group("db2"),
    pytest.mark.skipif(
        not DB2_INTEGRATION_ENABLED,
        reason="Db2 integration tests require a live database; set DB2_HOST or SQLSPEC_ENABLE_DB2_INTEGRATION_TESTS=1",
    ),
]


@pytest.fixture
def clean_users_table(db2_session: Db2SyncDriver) -> Generator[None, None, None]:
    """Ensure test table is created fresh and dropped after test completion."""
    try:
        tables = db2_session.data_dictionary.get_tables(db2_session)
        table_names = {t["table_name"].upper() for t in tables}
        if "TEST_INTEGRATION_USERS" in table_names:
            db2_session.execute_script("DROP TABLE TEST_INTEGRATION_USERS")
            db2_session.commit()
    except Exception:
        pass

    db2_session.execute_script(
        "CREATE TABLE TEST_INTEGRATION_USERS (id INT NOT NULL PRIMARY KEY, name VARCHAR(64) NOT NULL, balance DECFLOAT)"
    )
    db2_session.commit()

    yield

    try:
        db2_session.execute_script("DROP TABLE TEST_INTEGRATION_USERS")
        db2_session.commit()
    except Exception:
        pass


def test_db2_integration_connection_ping(db2_session: Db2SyncDriver) -> None:
    """Verify live Db2 connection lifecycle and dummy table ping execution."""
    result = db2_session.select_value("SELECT 1 FROM SYSIBM.SYSDUMMY1")
    assert result == 1


def test_db2_integration_crud_and_parameters(db2_session: Db2SyncDriver, clean_users_table: None) -> None:
    """Verify CRUD execution and positional parameter binding against live Db2."""
    db2_session.execute(
        "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance) VALUES (?, ?, ?)", (1, "Alice", Decimal("100.50"))
    )
    db2_session.commit()

    user = db2_session.select_one("SELECT id, name, balance FROM TEST_INTEGRATION_USERS WHERE id = ?", (1,))
    assert user is not None
    assert user["id"] == 1
    assert user["name"] == "Alice"
    assert Decimal(str(user["balance"])) == Decimal("100.50")

    db2_session.execute("UPDATE TEST_INTEGRATION_USERS SET balance = ? WHERE id = ?", (Decimal("150.00"), 1))
    db2_session.commit()

    updated_balance = db2_session.select_value("SELECT balance FROM TEST_INTEGRATION_USERS WHERE id = ?", (1,))
    assert Decimal(str(updated_balance)) == Decimal("150.00")

    db2_session.execute("DELETE FROM TEST_INTEGRATION_USERS WHERE id = ?", (1,))
    db2_session.commit()

    count = db2_session.select_value("SELECT COUNT(*) FROM TEST_INTEGRATION_USERS")
    assert count == 0


def test_db2_integration_transactions(db2_session: Db2SyncDriver, clean_users_table: None) -> None:
    """Verify transaction commit and rollback boundaries."""
    db2_session.begin()
    db2_session.execute(
        "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance) VALUES (?, ?, ?)",
        (10, "CommittedUser", Decimal("50.00")),
    )
    db2_session.commit()

    count_after_commit = db2_session.select_value("SELECT COUNT(*) FROM TEST_INTEGRATION_USERS WHERE id = ?", (10,))
    assert count_after_commit == 1

    db2_session.begin()
    try:
        db2_session.execute(
            "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance) VALUES (?, ?, ?)",
            (20, "AbortedUser", Decimal("75.00")),
        )
        raise RuntimeError("Forced abort")
    except RuntimeError:
        db2_session.rollback()

    count_after_abort = db2_session.select_value("SELECT COUNT(*) FROM TEST_INTEGRATION_USERS WHERE id = ?", (20,))
    assert count_after_abort == 0


def test_db2_integration_savepoints(db2_session: Db2SyncDriver, clean_users_table: None) -> None:
    """Verify nested savepoint creation and partial transaction rollback."""
    db2_session.begin()
    try:
        db2_session.execute(
            "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance) VALUES (?, ?, ?)",
            (30, "PreservedUser", Decimal("25.00")),
        )
        db2_session.create_savepoint("sp_partial")
        db2_session.execute(
            "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance) VALUES (?, ?, ?)",
            (31, "DiscardedUser", Decimal("10.00")),
        )
        db2_session.rollback_to_savepoint("sp_partial")
        db2_session.commit()
    except Exception:
        db2_session.rollback()
        raise

    preserved = db2_session.select_value("SELECT COUNT(*) FROM TEST_INTEGRATION_USERS WHERE id = ?", (30,))
    discarded = db2_session.select_value("SELECT COUNT(*) FROM TEST_INTEGRATION_USERS WHERE id = ?", (31,))
    assert preserved == 1
    assert discarded == 0


def test_db2_integration_data_dictionary(db2_session: Db2SyncDriver, clean_users_table: None) -> None:
    """Verify schema catalog reflection on live database."""
    tables = db2_session.data_dictionary.get_tables(db2_session)
    table_names = [t["table_name"].upper() for t in tables]
    assert "TEST_INTEGRATION_USERS" in table_names

    columns = db2_session.data_dictionary.get_columns(db2_session, table="TEST_INTEGRATION_USERS")
    col_names = {c["column_name"].upper() for c in columns}
    assert {"ID", "NAME", "BALANCE"}.issubset(col_names)

    pk_cols = [c for c in columns if c.get("is_primary")]
    assert len(pk_cols) == 1
    assert pk_cols[0]["column_name"].upper() == "ID"


def test_db2_integration_arrow_conversion(db2_session: Db2SyncDriver, clean_users_table: None) -> None:
    """Verify Apache Arrow export from query results against live Db2."""
    pa = pytest.importorskip("pyarrow")

    db2_session.execute(
        "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance) VALUES (?, ?, ?)", (42, "ArrowUser", Decimal("99.99"))
    )
    db2_session.commit()

    result = db2_session.select_to_arrow("SELECT id, name FROM TEST_INTEGRATION_USERS WHERE id = ?", (42,))
    table = result.get_data()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.column_names == ["id", "name"]
    assert table.to_pydict() == {"id": [42], "name": ["ArrowUser"]}
