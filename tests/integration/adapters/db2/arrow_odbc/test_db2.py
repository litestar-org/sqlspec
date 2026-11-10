"""Native IBM Db2 acceptance through the 64-bit Arrow-ODBC transport."""

import json
import platform
import sys
from typing import TYPE_CHECKING

import pytest
from pytest_databases.docker.db2 import DB2_PORT
from pytest_databases.helpers import exec_output_to_bytes

if TYPE_CHECKING:
    from pytest_databases.docker.db2 import Db2Service

    from sqlspec.adapters.db2 import Db2ConnectionParams

pytestmark = [
    pytest.mark.db2,
    pytest.mark.integration,
    pytest.mark.xdist_group("db2"),
    pytest.mark.skipif(
        sys.platform != "linux" or platform.machine().lower() not in {"x86_64", "amd64"},
        reason="The IBM 64-bit ODBC runtime acceptance uses a Linux x86-64 client",
    ),
]

_ACCEPTANCE_SCRIPT = """
import json
import os
from decimal import Decimal
from uuid import uuid4

import pyarrow as pa
from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig

payload = json.loads(os.environ.pop("SQLSPEC_DB2_ODBC_CONFIG"))
config = ArrowOdbcConfig(
    connection_config=payload,
    driver_features={"enable_pooling": False, "query_timeout_sec": 15},
)
table_name = "SQLSPEC_ARROW_ODBC_" + uuid4().hex[:12].upper()
index_name = table_name + "_UQ"
created = False
try:
    with config.provide_session() as session:
        assert session.dialect == "db2"
        try:
            session.execute(
                f"CREATE TABLE {table_name} ("
                "id INTEGER NOT NULL PRIMARY KEY, big_value BIGINT, label VARCHAR(64), amount DECIMAL(14, 3))"
            )
            session.commit()
            created = True
            session.execute(f"CREATE UNIQUE INDEX {index_name} ON {table_name} (label)")
            session.commit()
            insert = f"INSERT INTO {table_name} (id, big_value, label, amount) VALUES (?, ?, ?, ?)"
            session.execute(insert, 1, 2**40, "Grüße 東京", Decimal("123.450"))
            session.execute(insert, 2, None, None, None)
            session.commit()
            session.begin()
            session.execute(insert, 3, 2**42, "rolled back", Decimal("456.789"))
            session.rollback()
            assert session.select_value(f"SELECT COUNT(*) FROM {table_name} WHERE id = ?", 3) == 0
            rows = session.select(f"SELECT id, big_value, label, amount FROM {table_name} ORDER BY id")
            assert rows[0]["id"] == 1
            assert rows[0]["big_value"] == 2**40
            assert rows[0]["label"] == "Grüße 東京"
            assert Decimal(str(rows[0]["amount"])) == Decimal("123.450")
            assert rows[1] == {"id": 2, "big_value": None, "label": None, "amount": None}
            result = session.select_to_arrow(
                f"SELECT id, big_value, label, amount FROM {table_name} WHERE id >= ? ORDER BY id", 1
            )
            table = result.get_data()
            assert table.num_rows == 2
            assert table.column_names == ["id", "big_value", "label", "amount"]
            assert table.schema.field("big_value").type == pa.int64()
            assert table.schema.field("amount").type == pa.decimal128(14, 3)
            assert table.column("big_value").to_pylist() == [2**40, None]
            assert table.column("label").to_pylist() == ["Grüße 東京", None]
            assert table.column("amount").to_pylist() == [Decimal("123.450"), None]
            schema_name = str(session.select_value("SELECT CURRENT SCHEMA FROM SYSIBM.SYSDUMMY1")).strip()
            dictionary = session.data_dictionary
            columns = dictionary.get_columns(session, table=table_name, schema=schema_name)
            assert [column["column_name"] for column in columns] == ["ID", "BIG_VALUE", "LABEL", "AMOUNT"]
            assert [column["ordinal_position"] for column in columns] == [1, 2, 3, 4]
            assert all(
                column["table_name"] == table_name and column["schema_name"] == schema_name
                for column in columns
            )
            assert columns[0]["is_primary"] == 1
            assert columns[2]["is_unique"] == 1
            assert columns[3]["numeric_scale"] == 3
            indexes = dictionary.get_indexes(session, table=table_name, schema=schema_name)
            assert len(indexes) == 2
            indexes_by_name = {index["index_name"]: index for index in indexes}
            assert len(indexes_by_name) == 2
            assert indexes_by_name[index_name]["is_unique"] == 1
            assert sum(bool(index["is_primary"]) for index in indexes) == 1
            tables = [
                item for item in dictionary.get_tables(session, schema=schema_name)
                if item["table_name"] == table_name
            ]
            assert len(tables) == 1
            assert tables[0]["schema_name"] == schema_name
            assert tables[0]["table_type"] == "BASE TABLE"
        finally:
            session.rollback()
    with config.provide_session() as reopened:
        try:
            persisted = reopened.select(f"SELECT id, big_value FROM {table_name} ORDER BY id")
            assert persisted == [{"id": 1, "big_value": 2**40}, {"id": 2, "big_value": None}]
        finally:
            reopened.rollback()
finally:
    try:
        if created:
            with config.provide_session() as cleanup:
                cleanup.rollback()
                cleanup.execute(f"DROP TABLE {table_name}")
                cleanup.commit()
    finally:
        config.close_pool()
print(json.dumps({"dialect": "db2", "rows": 2, "transport": "arrow_odbc"}))
"""


def test_arrow_odbc_db2_parameter_binding_and_arrow_null_indicators(
    db2_service: "Db2Service",
    db2_connection_config: "Db2ConnectionParams",
    db2_arrow_odbc_runtime: tuple[list[str], dict[str, str], str],
) -> None:
    """Transactions, catalog metadata, and Arrow buffers preserve typed Db2 values and NULL indicators."""
    command, environment, driver = db2_arrow_odbc_runtime
    password = str(db2_connection_config["password"])
    parameters = {
        "driver": driver,
        "host": "127.0.0.1",
        "database": db2_connection_config["database"],
        "uid": db2_connection_config["user"],
        "pwd": password,
        "extra": {"Port": DB2_PORT, "Protocol": "TCPIP"},
        "login_timeout": 15,
        "autocommit": False,
    }
    result = db2_service.container.exec_run(
        [*command, "-c", _ACCEPTANCE_SCRIPT],
        environment={**environment, "SQLSPEC_DB2_ODBC_CONFIG": json.dumps(parameters)},
    )
    output = exec_output_to_bytes(result.output).decode("utf-8", errors="replace")
    if result.exit_code:
        diagnostic = output.replace(password, "<redacted>")
        pytest.fail(f"Native Arrow-ODBC Db2 acceptance failed (exit {result.exit_code}):\n{diagnostic}")
    assert json.loads(output) == {"dialect": "db2", "rows": 2, "transport": "arrow_odbc"}


_QUALIFIED_LITESTAR_SCRIPT = """
import asyncio
import json
import os
from uuid import uuid4

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.arrow_odbc.litestar import ArrowOdbcStore

payload = json.loads(os.environ.pop("SQLSPEC_DB2_ODBC_CONFIG"))
admin_config = ArrowOdbcConfig(
    connection_config=payload,
    driver_features={"enable_pooling": False, "query_timeout_sec": 15},
)
with admin_config.provide_session() as session:
    schema_name = str(session.select_value("SELECT CURRENT SCHEMA FROM SYSIBM.SYSDUMMY1")).strip()
    session.rollback()
admin_config.close_pool()

qualified_table = f"{schema_name}.SQLSPEC_SESS_{uuid4().hex[:8].upper()}"
chunk_table = f"{qualified_table}_chunks"
store_config = ArrowOdbcConfig(
    connection_config=payload,
    driver_features={"enable_pooling": False, "query_timeout_sec": 15},
    extension_config={"litestar": {"session_table": qualified_table}},
)
store = ArrowOdbcStore(store_config)
large_payload = b"db2-qualified-chunk-" * 300


async def _exercise() -> int:
    await store.create_table()
    await store.set("qual-db2-1", large_payload, expires_in=300)
    with store_config.provide_session() as check_session:
        session_rows = int(check_session.select_value(f"SELECT COUNT(*) FROM {qualified_table}") or 0)
        chunk_rows = int(check_session.select_value(f"SELECT COUNT(*) FROM {chunk_table}") or 0)
        check_session.rollback()
    assert session_rows == 1
    assert chunk_rows >= 2
    assert await store.get("qual-db2-1") == large_payload
    await store.delete("qual-db2-1")
    assert await store.get("qual-db2-1") is None
    with store_config.provide_session() as check_session:
        after_sessions = int(check_session.select_value(f"SELECT COUNT(*) FROM {qualified_table}") or 0)
        after_chunks = int(check_session.select_value(f"SELECT COUNT(*) FROM {chunk_table}") or 0)
        check_session.rollback()
    assert after_sessions == 0
    assert after_chunks == 0
    return chunk_rows


created = False
try:
    chunk_count = asyncio.run(_exercise())
    created = True
finally:
    try:
        with store_config.provide_session() as cleanup:
            cleanup.rollback()
            for stmt in store._drop_table_sql():
                try:
                    cleanup.execute(stmt)
                    cleanup.commit()
                except Exception:
                    cleanup.rollback()
    finally:
        store_config.close_pool()

assert created is True
print(json.dumps({"qualified_table": qualified_table, "chunks": chunk_count, "round_trip": True}))
"""


def test_arrow_odbc_db2_qualified_litestar_session_chunked_round_trip(
    db2_service: "Db2Service",
    db2_connection_config: "Db2ConnectionParams",
    db2_arrow_odbc_runtime: tuple[list[str], dict[str, str], str],
) -> None:
    """Qualified Db2 session_table round-trips chunked session payloads and cascades deletes."""
    command, environment, driver = db2_arrow_odbc_runtime
    password = str(db2_connection_config["password"])
    parameters = {
        "driver": driver,
        "host": "127.0.0.1",
        "database": db2_connection_config["database"],
        "uid": db2_connection_config["user"],
        "pwd": password,
        "extra": {"Port": DB2_PORT, "Protocol": "TCPIP"},
        "login_timeout": 15,
        "autocommit": False,
    }
    result = db2_service.container.exec_run(
        [*command, "-c", _QUALIFIED_LITESTAR_SCRIPT],
        environment={**environment, "SQLSPEC_DB2_ODBC_CONFIG": json.dumps(parameters)},
    )
    output = exec_output_to_bytes(result.output).decode("utf-8", errors="replace")
    if result.exit_code:
        diagnostic = output.replace(password, "<redacted>")
        pytest.fail(
            f"Native Arrow-ODBC Db2 qualified Litestar round-trip failed (exit {result.exit_code}):\n{diagnostic}"
        )
    parsed = json.loads(output)
    assert parsed["round_trip"] is True
    assert parsed["chunks"] >= 2


_RECONCILE_SCRIPT = """
import json
import os
from uuid import uuid4

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.migrations.schema import metadata_has_index

payload = json.loads(os.environ.pop("SQLSPEC_DB2_ODBC_CONFIG"))
reconcile_payload = {**payload, "autocommit": True}
prefix = "AO_REC_" + uuid4().hex[:6].upper()
sess_table = f"{prefix}_SESS"
chunk_table = f"{sess_table}_CHUNKS"
events_table = f"{prefix}_EVT"
adk_session = f"{prefix}_ADK_S"
adk_event = f"{prefix}_ADK_E"
adk_app = f"{prefix}_ADK_A"
adk_user = f"{prefix}_ADK_U"
adk_meta = f"{prefix}_ADK_M"
adk_memory = f"{prefix}_ADK_MEM"
mig_table = f"{prefix}_MIG"
dropped_adk_index = f"IDX_{adk_session}_UPDATE_TIME"

config = ArrowOdbcConfig(
    connection_config=reconcile_payload,
    driver_features={"enable_pooling": False, "query_timeout_sec": 15},
    migration_config={"version_table_name": mig_table.lower()},
    extension_config={
        "litestar": {"migration_mode": "reconcile", "session_table": sess_table.lower()},
        "events": {"migration_mode": "reconcile", "queue_table": events_table.lower()},
        "adk": {
            "migration_mode": "reconcile",
            "enable_memory": True,
            "session_table": adk_session.lower(),
            "events_table": adk_event.lower(),
            "app_state_table": adk_app.lower(),
            "user_state_table": adk_user.lower(),
            "metadata_table": adk_meta.lower(),
            "memory_table": adk_memory.lower(),
        },
    },
)
all_tables = (chunk_table, sess_table, events_table, adk_memory, adk_meta, adk_user, adk_app, adk_event, adk_session)
try:
    config.migrate_up(echo=False)
    with config.provide_session() as driver:
        schema_name = str(driver.select_value("SELECT CURRENT SCHEMA FROM SYSIBM.SYSDUMMY1")).strip()
        existing = {row["table_name"].upper() for row in driver.data_dictionary.get_tables(driver, schema=schema_name)}
        assert set(all_tables).issubset(existing)
        assert int(driver.select_value(f"SELECT COUNT(*) FROM {mig_table}")) == 0
        driver.execute(f"DROP TABLE {chunk_table}")
        driver.execute(f"DROP INDEX {dropped_adk_index}")

    config.migrate_up(echo=False)
    with config.provide_session() as driver:
        schema_name = str(driver.select_value("SELECT CURRENT SCHEMA FROM SYSIBM.SYSDUMMY1")).strip()
        restored_tables = {
            row["table_name"].upper() for row in driver.data_dictionary.get_tables(driver, schema=schema_name)
        }
        assert chunk_table in restored_tables
        assert int(driver.select_value(f"SELECT COUNT(*) FROM {mig_table}")) == 0
        indexes = driver.data_dictionary.get_indexes(driver, table=adk_session, schema=schema_name)
        assert metadata_has_index(indexes, adk_session, dropped_adk_index, schema_name=schema_name)
finally:
    try:
        with config.provide_session() as cleanup:
            for table in (*all_tables, mig_table):
                try:
                    cleanup.execute(f"DROP TABLE {table}")
                except Exception:
                    pass
    finally:
        config.close_pool()

print(json.dumps({"reconciled": True, "chunk_table": chunk_table}))
"""


def test_arrow_odbc_db2_extension_reconcile_restores_chunk_table_and_indexes(request: pytest.FixtureRequest) -> None:
    """Db2 ArrowOdbcConfig migrate_up() reconciles litestar, events, and adk and repairs dropped objects."""
    try:
        db2_service: Db2Service = request.getfixturevalue("db2_service")
        db2_connection_config: Db2ConnectionParams = request.getfixturevalue("db2_connection_config")
        command, environment, driver = request.getfixturevalue("db2_arrow_odbc_runtime")
    except pytest.skip.Exception:
        raise
    except Exception as exc:
        pytest.skip(f"Db2 service container unavailable on host: {exc}")

    password = str(db2_connection_config["password"])
    parameters = {
        "driver": driver,
        "host": "127.0.0.1",
        "database": db2_connection_config["database"],
        "uid": db2_connection_config["user"],
        "pwd": password,
        "extra": {"Port": DB2_PORT, "Protocol": "TCPIP"},
        "login_timeout": 15,
        "autocommit": True,
    }
    result = db2_service.container.exec_run(
        [*command, "-c", _RECONCILE_SCRIPT],
        environment={**environment, "SQLSPEC_DB2_ODBC_CONFIG": json.dumps(parameters)},
    )
    output = exec_output_to_bytes(result.output).decode("utf-8", errors="replace")
    if result.exit_code:
        diagnostic = output.replace(password, "<redacted>")
        pytest.fail(f"Native Arrow-ODBC Db2 extension reconcile failed (exit {result.exit_code}):\n{diagnostic}")
    parsed = json.loads(output)
    assert parsed["reconciled"] is True
