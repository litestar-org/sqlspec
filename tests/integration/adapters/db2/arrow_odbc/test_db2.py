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
