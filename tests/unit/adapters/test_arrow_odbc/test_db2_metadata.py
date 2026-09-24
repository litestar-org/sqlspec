"""Unit tests for arrow-odbc Db2 reflection, error classification, types, and result names."""

from typing import Any

import pyarrow as pa
import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig, ArrowOdbcDriver
from sqlspec.adapters.arrow_odbc.core import create_mapped_exception
from sqlspec.adapters.arrow_odbc.type_converter import odbc_type_to_arrow
from sqlspec.exceptions import CheckViolationError, DeadlockError, QueryTimeoutError
from tests.unit.adapters.test_arrow_odbc._db2_fakes import (
    DB2_CONNECTION_STRING,
    FakeArrowOdbcConnection,
    as_connection,
    db2_error_message,
)

_UPPERCASE_RESULT = pa.table({"ID": [1, 2], "CUSTOMER_NAME": ["Ada", "Grace"], "MixedCase": [True, False]})


def _db2_driver(connection: FakeArrowOdbcConnection, **driver_features: Any) -> ArrowOdbcDriver:
    config = ArrowOdbcConfig(
        connection_config={"connection_string": DB2_CONNECTION_STRING}, driver_features=driver_features
    )
    return ArrowOdbcDriver(
        as_connection(connection), statement_config=config.statement_config, driver_features=config.driver_features
    )


@pytest.mark.parametrize("domain", ["tables", "columns", "indexes", "foreign_keys"])
def test_db2_get_tables_binds_table_name(domain: str) -> None:
    """By-schema catalog queries bind every named parameter, leaving table_name NULL."""
    connection = FakeArrowOdbcConnection(result=pa.table({"SCHEMA_NAME": pa.array([], pa.string())}))
    driver = _db2_driver(connection)

    getattr(driver.data_dictionary, f"get_{domain}")(driver, schema="app")

    sql, parameters = connection.calls[0]
    assert "SYSCAT." in sql
    assert parameters is not None
    assert len(parameters) == sql.count("?")
    assert set(parameters) == {"APP", None}


@pytest.mark.parametrize(
    ("sqlstate", "native_error", "detail", "expected"),
    [
        ("23513", -545, "SQL0545N The requested operation is not allowed.", CheckViolationError),
        (
            "57033",
            -913,
            'SQL0913N Unsuccessful execution caused by deadlock or timeout. Reason code "2".',
            DeadlockError,
        ),
        (
            "57033",
            -913,
            'SQL0913N Unsuccessful execution caused by deadlock or timeout. Reason code "68".',
            QueryTimeoutError,
        ),
        ("40001", -911, 'SQL0911N The current transaction has been rolled back. Reason code "68".', QueryTimeoutError),
        ("40001", -911, 'SQL0911N The current transaction has been rolled back. Reason code "2".', DeadlockError),
    ],
)
def test_sqlstate_check_violation_and_statement_rollback(
    sqlstate: str, native_error: int, detail: str, expected: type[Exception]
) -> None:
    """Db2 check violations, statement rollbacks and lock timeouts map to specific errors."""
    mapped = create_mapped_exception(Exception(db2_error_message(sqlstate, native_error, detail)))

    assert type(mapped) is expected


@pytest.mark.parametrize(
    ("type_name", "expected"),
    [
        ("DECFLOAT(34)", pa.string()),
        ("GRAPHIC(10)", pa.string()),
        ("VARGRAPHIC(100)", pa.string()),
        ("DBCLOB(1M)", pa.string()),
        ("LONG VARCHAR", pa.string()),
        ("LONG VARGRAPHIC", pa.string()),
        ("CHARACTER(8)", pa.string()),
        ("CHARACTER VARYING(64)", pa.string()),
        ("DOUBLE PRECISION", pa.float64()),
    ],
)
def test_type_db2_names_map_to_arrow(type_name: str, expected: pa.DataType) -> None:
    """Db2 type names resolve to Arrow types."""
    assert odbc_type_to_arrow(type_name) == expected


def _result_names(driver: ArrowOdbcDriver) -> "dict[str, list[str]]":
    execute_row = driver.execute("SELECT * FROM customers").get_data()[0]
    stream_row = next(iter(driver.select_stream("SELECT * FROM customers")))
    table = driver.select_to_arrow("SELECT * FROM customers").get_data()
    reader = driver.select_to_arrow("SELECT * FROM customers", return_format="reader").get_data()
    return {
        "execute": list(execute_row),
        "stream": list(stream_row),
        "table": table.column_names,
        "reader": reader.schema.names,
        "reader_batches": next(batch.schema.names for batch in reader),
    }


def test_db2_columns_lowercased() -> None:
    """Db2 implicit-uppercase result names are lowercased on every result path; quoted names are kept."""
    names = _result_names(_db2_driver(FakeArrowOdbcConnection(result=_UPPERCASE_RESULT)))

    assert names == {
        key: ["id", "customer_name", "MixedCase"] for key in ("execute", "stream", "table", "reader", "reader_batches")
    }


@pytest.mark.parametrize(
    "driver_factory",
    [
        lambda connection: _db2_driver(connection, enable_lowercase_column_names=False),
        lambda connection: ArrowOdbcDriver(
            as_connection(connection), driver_features={"dbms_name": "Microsoft SQL Server"}
        ),
    ],
    ids=["db2-disabled", "mssql-default"],
)
def test_result_names_kept_when_lowercasing_is_off(driver_factory: Any) -> None:
    """Disabling the feature, or any non-Db2 dialect, keeps result names as reported."""
    names = _result_names(driver_factory(FakeArrowOdbcConnection(result=_UPPERCASE_RESULT)))

    assert names == {
        key: ["ID", "CUSTOMER_NAME", "MixedCase"] for key in ("execute", "stream", "table", "reader", "reader_batches")
    }
