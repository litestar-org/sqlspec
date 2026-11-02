"""Integration tests for IBM Db2 behavior outside the shared adapter contracts."""

import asyncio
import json
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("ibm_db")
pytest.importorskip("ibm_db_dbi")

from sqlspec import sql
from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
from sqlspec.adapters.db2.core import extract_sqlstate
from sqlspec.adapters.db2.driver import Db2SyncDriver
from sqlspec.adapters.db2.events import Db2SyncEventQueueStore
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    NotNullViolationError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.extensions.events import EventMessage, SyncTableEventQueue

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


def test_db2_semicolon_schema_survives_native_dsn_roundtrip(db2_connection_config: dict[str, Any]) -> None:
    """The CLI driver receives one semicolon-bearing schema value after DSN parsing and rendering."""
    schema = "SQLSPEC;SEMICOLON"
    source = Db2SyncConfig(connection_config={**db2_connection_config, "current_schema": schema})
    config = Db2SyncConfig(connection_config={"dsn": source.get_connection_string()})
    try:
        with config.provide_session() as session:
            assert session.select_value("SELECT CURRENT SCHEMA FROM SYSIBM.SYSDUMMY1").strip() == schema
    finally:
        config.close_pool()


def test_db2_skip_locked_dequeue_excludes_another_sessions_locked_event(
    db2_session: Db2SyncDriver, db2_connection_config: dict[str, Any], db2_contract_table: str
) -> None:
    """A consumer claims the second event while a different connection holds the first row locked."""
    config = Db2SyncConfig(
        connection_config=dict(db2_connection_config), extension_config={"events": {"queue_table": db2_contract_table}}
    )
    store = Db2SyncEventQueueStore(config)
    for statement in store.create_statements():
        db2_session.execute_script(statement)
    db2_session.commit()
    queue = SyncTableEventQueue(config, queue_table=db2_contract_table, select_for_update=True, skip_locked=True)
    try:
        first_id = queue.publish("native_locking", {"position": 1})
        second_id = queue.publish("native_locking", {"position": 2})
        db2_session.begin()
        locked = db2_session.select_one(
            sql.select("event_id").from_(db2_contract_table).where_eq("event_id", first_id).for_update()
        )
        assert locked["event_id"] == first_id
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(queue.dequeue, "native_locking", 0.0)
            try:
                message = pending.result(timeout=10)
                assert isinstance(message, EventMessage)
                assert message.event_id == second_id
                assert message.payload == {"position": 2}
            finally:
                db2_session.rollback()
        first = queue.dequeue("native_locking", poll_interval=0.0)
        assert isinstance(first, EventMessage)
        assert first.event_id == first_id
        assert queue.dequeue("native_locking", poll_interval=0.0) is None
    finally:
        db2_session.rollback()
        config.close_pool()


async def test_db2_async_native_pool_bounds_contending_sessions_and_recovers(
    db2_connection_config: dict[str, Any],
) -> None:
    """Concurrent native borrowers fill the configured capacity and a third times out without leaking it."""
    config = Db2AsyncConfig(connection_config={**db2_connection_config, "max_size": 2, "acquire_timeout": 0.1})
    entered: asyncio.Queue[int] = asyncio.Queue()
    release = asyncio.Event()

    async def hold(value: int) -> None:
        async with config.provide_session() as session:
            observed = await session.select_value("SELECT CAST(:value AS INTEGER) FROM SYSIBM.SYSDUMMY1", value=value)
            await entered.put(observed)
            await release.wait()

    workers = [asyncio.create_task(hold(value)) for value in (1, 2)]
    try:
        values = await asyncio.wait_for(asyncio.gather(entered.get(), entered.get()), timeout=10)
        assert set(values) == {1, 2}
        pool = await config.provide_pool()
        assert (pool.size(), pool.checked_out()) == (2, 2)
        with pytest.raises(ConnectionTimeoutError):
            async with config.provide_session():
                pytest.fail("A third session exceeded max_size while both native borrowers were held.")
        release.set()
        await asyncio.wait_for(asyncio.gather(*workers), timeout=10)
        assert (pool.size(), pool.checked_out()) == (2, 0)
        async with config.provide_session() as session:
            assert await session.select_value("SELECT 42 FROM SYSIBM.SYSDUMMY1") == 42
    finally:
        release.set()
        try:
            await asyncio.gather(*workers)
        finally:
            await config.close_pool()


@pytest.mark.parametrize(
    ("statement", "parameters", "error_type", "sqlstate"),
    [
        (
            "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance) VALUES (:id, :name, :balance)",
            {"id": 1, "name": "duplicate", "balance": 1},
            UniqueViolationError,
            "23505",
        ),
        (
            "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance) VALUES (:id, :name, :balance)",
            {"id": 2, "name": None, "balance": 1},
            NotNullViolationError,
            "23502",
        ),
        (
            "INSERT INTO TEST_INTEGRATION_USERS (id, name, balance) VALUES (:id, :name, :balance)",
            {"id": 2, "name": "negative", "balance": -1},
            CheckViolationError,
            "23513",
        ),
        ("SELECT * FROM SQLSPEC_MISSING_NATIVE_TABLE", {}, SQLParsingError, "42704"),
    ],
    ids=["unique", "not-null", "check", "missing-table"],
)
def test_db2_native_sqlstate_maps_exact_public_exception(
    db2_session: Db2SyncDriver,
    clean_users_table: None,
    statement: str,
    parameters: dict[str, Any],
    error_type: type[SQLSpecError],
    sqlstate: str,
) -> None:
    """Real CLI diagnostics preserve representative SQLSTATEs and their specific mapped exception types."""
    db2_session.execute_script(
        "ALTER TABLE TEST_INTEGRATION_USERS ADD CONSTRAINT SQLSPEC_BALANCE_CHECK CHECK (balance >= 0)"
    )
    db2_session.execute("INSERT INTO TEST_INTEGRATION_USERS (id, name) VALUES (:id, :name)", id=1, name="retained")
    db2_session.commit()
    with pytest.raises(error_type) as caught:
        db2_session.execute(statement, **parameters)
    assert extract_sqlstate(caught.value) == sqlstate
    db2_session.rollback()


@pytest.mark.parametrize("multiple_rows", [False, True], ids=["mapping", "two-rows"])
def test_db2_merge_extended_typed_source_roundtrip(
    db2_session: Db2SyncDriver, db2_contract_table: str, multiple_rows: bool
) -> None:
    """The native engine executes the builder's bool, temporal, UUID, CLOB, binary, float, JSON and NULL casts."""
    db2_session.execute_script(
        f"CREATE TABLE {db2_contract_table} ("
        "record_id INTEGER NOT NULL PRIMARY KEY, label VARCHAR(40), enabled BOOLEAN, amount DECFLOAT(34), "
        "captured_at TIMESTAMP, uuid_value VARCHAR(36), body CLOB, day_value DATE, time_value TIME, "
        "binary_value VARBINARY(64), ratio DOUBLE, payload CLOB, absent CLOB)"
    )
    db2_session.commit()
    identifier = UUID("12345678-1234-5678-1234-567812345678")
    values = {
        "record_id": 1,
        "label": "typed",
        "enabled": True,
        "amount": Decimal("12345678901234567890.123456789"),
        "captured_at": datetime(2026, 6, 7, 8, 9, 10, 345678),
        "uuid_value": identifier,
        "body": "x" * 32673,
        "day_value": date(2026, 6, 7),
        "time_value": time(12, 34, 56),
        "binary_value": b"\x00\xff",
        "ratio": 0.5,
        "payload": {"nested": "value"},
        "absent": None,
    }
    query = (
        sql
        .merge(dialect="db2")
        .into(db2_contract_table, alias="target")
        .using([values, {**values, "record_id": 2, "payload": None}] if multiple_rows else values, alias="src")
        .on("target.record_id = src.record_id")
        .when_not_matched_then_insert({column: f"src.{column}" for column in values})
    )
    db2_session.execute(query)
    db2_session.commit()
    row = db2_session.select_one(f"SELECT * FROM {db2_contract_table} WHERE record_id = :record_id", record_id=1)
    assert (row["record_id"], row["label"], row["enabled"]) == (1, "typed", 1)
    assert Decimal(str(row["amount"])) == Decimal("12345678901234567890.123456789")
    assert row["captured_at"] == datetime(2026, 6, 7, 8, 9, 10, 345678)
    assert str(row["uuid_value"]) == "12345678-1234-5678-1234-567812345678"
    assert row["body"] == "x" * 32673
    assert (row["day_value"], row["time_value"]) == (date(2026, 6, 7), time(12, 34, 56))
    assert bytes(row["binary_value"]) == b"\x00\xff"
    assert row["ratio"] == 0.5
    payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(row["payload"])
    assert payload == {"nested": "value"}
    assert row["absent"] is None
    if multiple_rows:
        assert db2_session.select_value(f"SELECT COUNT(*) FROM {db2_contract_table}") == 2
        assert (
            db2_session.select_value(
                f"SELECT payload FROM {db2_contract_table} WHERE record_id = :record_id", record_id=2
            )
            is None
        )


def test_db2_catalog_uniqueness_and_distinct_objects(db2_session: Db2SyncDriver, db2_contract_table: str) -> None:
    """Catalog flags distinguish single-column uniqueness and composite keys without duplicated objects/indexes."""
    db2_session.execute_script(
        f"CREATE TABLE {db2_contract_table} ("
        "id INTEGER NOT NULL PRIMARY KEY, email VARCHAR(64) NOT NULL UNIQUE, "
        "left_id INTEGER NOT NULL, right_id INTEGER NOT NULL, UNIQUE (left_id, right_id))"
    )
    db2_session.commit()
    dictionary = db2_session.data_dictionary
    columns = {
        column["column_name"]: column for column in dictionary.get_columns(db2_session, table=db2_contract_table)
    }
    assert columns["ID"]["is_primary"] is True
    assert columns["EMAIL"]["is_primary"] is False
    assert columns["EMAIL"]["is_unique"] is True
    assert columns["LEFT_ID"]["is_unique"] is False
    assert columns["RIGHT_ID"]["is_unique"] is False
    indexes = dictionary.get_indexes(db2_session, table=db2_contract_table)
    identities = [(index["schema_name"], index["table_name"], index["index_name"]) for index in indexes]
    assert len(identities) == len(set(identities)) == 3
    assert sorted(tuple(index["columns"] or ()) for index in indexes) == [("EMAIL",), ("ID",), ("LEFT_ID", "RIGHT_ID")]
    objects = dictionary.get_objects(db2_session).items
    matching = [item for item in objects if isinstance(item, dict) and item.get("object_name") == db2_contract_table]
    assert [(item["object_name"], item["object_type"]) for item in matching] == [(db2_contract_table, "TABLE")]
    tables = dictionary.get_tables(db2_session)
    assert [table["table_name"] for table in tables if table["table_name"] == db2_contract_table] == [
        db2_contract_table
    ]


@pytest.fixture
def db2_contract_table(db2_session: Db2SyncDriver) -> Generator[str, None, None]:
    """Reserve a fixed scratch table and clean it before and after every native residual case."""
    table = "SQLSPEC_DB2_NATIVE_CONTRACT"
    _drop_contract_table(db2_session, table)
    try:
        yield table
    finally:
        db2_session.rollback()
        _drop_contract_table(db2_session, table)


def _drop_contract_table(session: Db2SyncDriver, table: str) -> None:
    exists = session.select_value(
        "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = CURRENT SCHEMA AND TABNAME = :table_name",
        table_name=table,
    )
    if exists:
        session.execute_script(f"DROP TABLE {table}")
        session.commit()
