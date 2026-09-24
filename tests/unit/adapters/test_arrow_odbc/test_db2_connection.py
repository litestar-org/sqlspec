"""Unit tests for arrow-odbc Db2 connection strings and parameter binding."""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

pytest.importorskip("arrow_odbc")

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig, ArrowOdbcDriver
from sqlspec.adapters.arrow_odbc.core import build_connection_config
from sqlspec.exceptions import ImproperConfigurationError
from tests.unit.adapters.test_arrow_odbc._db2_fakes import FakeArrowOdbcConnection, as_connection

_DB2_FIELDS = {"driver": "{IBM DB2 ODBC DRIVER}", "host": "h", "port": 50000, "database": "d", "uid": "u", "pwd": "p"}


def _captured_connection_string(monkeypatch: pytest.MonkeyPatch, connection_config: "dict[str, Any]") -> str:
    captured: list[str] = []

    def fake_connect(connection_string: str, **_: Any) -> FakeArrowOdbcConnection:
        captured.append(connection_string)
        return FakeArrowOdbcConnection()

    monkeypatch.setattr("sqlspec.adapters.arrow_odbc.config.arrow_odbc_connect", fake_connect)
    ArrowOdbcConfig(connection_config=connection_config).create_connection()
    return captured[0]


def test_db2_discrete_fields_use_cli_keywords(monkeypatch: pytest.MonkeyPatch) -> None:
    """Discrete fields for a Db2 driver render the IBM CLI Hostname/Port/Protocol keywords."""
    connection_string = _captured_connection_string(monkeypatch, dict(_DB2_FIELDS))

    assert (
        connection_string == "Driver={IBM DB2 ODBC DRIVER};Hostname=h;Port=50000;Protocol=TCPIP;Database=d;UID=u;PWD=p;"
    )


def test_db2_connection_string_merge_uses_cli_keywords(monkeypatch: pytest.MonkeyPatch) -> None:
    """Host and port overrides merged into a Db2 connection string replace Hostname and add Port."""
    connection_string = _captured_connection_string(
        monkeypatch,
        {
            "connection_string": "Driver={IBM DB2 ODBC DRIVER};Hostname=old;Database=d;Protocol=TCPIP;",
            "host": "db2.internal",
            "port": 50001,
        },
    )

    assert (
        connection_string == "Driver={IBM DB2 ODBC DRIVER};Hostname=db2.internal;Port=50001;Protocol=TCPIP;Database=d;"
    )


def test_db2_explicit_protocol_is_kept() -> None:
    """A configured protocol replaces the TCPIP default."""
    connection_string, _ = build_connection_config({**_DB2_FIELDS, "protocol": "TCPIP6"}, dialect="db2")

    assert "Protocol=TCPIP6;" in connection_string
    assert connection_string.count("Protocol=") == 1


@pytest.mark.parametrize("option", ["trusted_connection", "trust_server_certificate", "encrypt"])
def test_db2_rejects_sql_server_options(option: str) -> None:
    """SQL Server-only connection options are refused for Db2."""
    with pytest.raises(ImproperConfigurationError, match="SQL Server option"):
        build_connection_config({**_DB2_FIELDS, option: True}, dialect="db2")


def test_db2_requires_a_connection_option() -> None:
    """A Db2 configuration without any connection option is refused."""
    with pytest.raises(ImproperConfigurationError, match="requires 'connection_string'"):
        build_connection_config({"database": None}, dialect="db2")


def test_mssql_connection_string_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """SQL Server discrete fields keep the Server=host,port form."""
    connection_string = _captured_connection_string(
        monkeypatch,
        {
            "driver": "{ODBC Driver 18 for SQL Server}",
            "host": "h",
            "port": 1433,
            "database": "d",
            "uid": "u",
            "pwd": "p",
            "trust_server_certificate": True,
        },
    )

    assert connection_string == (
        "Driver={ODBC Driver 18 for SQL Server};Server=h,1433;Database=d;UID=u;PWD=p;TrustServerCertificate=yes;"
    )


@pytest.mark.parametrize(
    ("dbms_name", "expected"), [("DB2", "2026-01-01 10:00:00"), ("Microsoft SQL Server", "2026-01-01 12:00:00+02:00")]
)
def test_db2_aware_datetime_bound_as_naive_utc(dbms_name: str, expected: str) -> None:
    """Db2 binds aware datetimes as naive UTC text; other dialects keep the offset."""
    connection = FakeArrowOdbcConnection()
    driver = ArrowOdbcDriver(as_connection(connection), driver_features={"dbms_name": dbms_name})
    moment = datetime(2026, 1, 1, 12, tzinfo=timezone(timedelta(hours=2)))

    driver.execute("INSERT INTO events (created_at, closed_at) VALUES (?, ?)", moment, None)
    driver.select_to_arrow("SELECT id FROM events WHERE created_at < ?", moment)
    list(driver.select_stream("SELECT id FROM events WHERE created_at < ?", moment))

    assert [parameters for _, parameters in connection.calls] == [[expected, None], [expected], [expected]]
