"""Unit tests for arrow-odbc dialect resolution and Db2 exception mapping."""

import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig, resolve_dialect_from_dbms_name
from sqlspec.adapters.arrow_odbc.core import create_mapped_exception
from sqlspec.core import SQL, LimitOffsetFilter
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DeadlockError,
    ForeignKeyViolationError,
    NotNullViolationError,
    QueryTimeoutError,
    UniqueViolationError,
)


@pytest.mark.parametrize(
    "dbms_name",
    [
        "DB2",
        "IBM DB2",
        "IBM DB2 ODBC DRIVER",
        "DB2/LINUXX8664",
        "clidriver",
        "libdb2o.so",
        "Driver={IBM DB2 ODBC DRIVER};Database=SAMPLE;",
    ],
)
def test_resolve_dialect_db2_dbms_names(dbms_name: str) -> None:
    """Verify various Db2 driver and DBMS names resolve to the db2 dialect."""
    assert resolve_dialect_from_dbms_name(dbms_name) == "db2"


def test_arrow_odbc_config_resolves_db2_dialect() -> None:
    """Verify ArrowOdbcConfig infers db2 dialect from connection string."""
    config = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver={IBM DB2 ODBC DRIVER};Database=TESTDB;Hostname=db2;"}
    )

    assert config.statement_config.dialect == "db2"
    assert config.statement_config.parameter_config.default_parameter_style == "qmark"
    statement = SQL("SELECT * FROM users", statement_config=config.statement_config)
    filtered = LimitOffsetFilter(10, 0).append_to_statement(statement)
    compiled_sql, params = filtered.compile()
    assert "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY" in compiled_sql
    assert params == (0, 10)


@pytest.mark.parametrize(
    ("sqlstate", "expected_type"),
    [
        ("23505", UniqueViolationError),
        ("23503", ForeignKeyViolationError),
        ("23502", NotNullViolationError),
        ("40001", DeadlockError),
        ("57014", QueryTimeoutError),
        ("08001", DatabaseConnectionError),
    ],
)
def test_arrow_odbc_db2_specific_sqlstate_mapping(sqlstate: str, expected_type: type[Exception]) -> None:
    """Verify Db2 SQLSTATE error codes map to granular SQLSpec exceptions."""
    err = Exception(f"State: {sqlstate}, Native error: -803, Message: [IBM][CLI Driver][DB2/LINUXX8664] Error occurred")
    mapped = create_mapped_exception(err)

    assert isinstance(mapped, expected_type)
