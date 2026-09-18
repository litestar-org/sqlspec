"""arrow-odbc configuration surface: connection string merging, encoding, pooling, SQLSTATE."""

from typing import Any, cast

import pytest

pytest.importorskip("arrow_odbc")

from arrow_odbc import TextEncoding

import sqlspec.adapters.arrow_odbc.config as arrow_odbc_config
from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig, ArrowOdbcDriver, build_connection_config
from sqlspec.adapters.arrow_odbc.core import create_mapped_exception
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DataError,
    ImproperConfigurationError,
    IntegrityError,
    SQLParsingError,
)


class _RecordingConnection:
    def __init__(self) -> None:
        self.read_kwargs: list[dict[str, Any]] = []

    def read_arrow_batches(self, **kwargs: Any) -> None:
        self.read_kwargs.append(kwargs)

    @property
    def dbms_name(self) -> str:
        return "SQLite"


def test_connection_string_keeps_extra_options() -> None:
    """An explicit connection string must not discard the other declared options."""
    connection_string, _ = build_connection_config({
        "connection_string": "DSN=analytics;",
        "extra": {"ApplicationIntent": "ReadOnly"},
    })

    assert "DSN=analytics;" in connection_string
    assert "ApplicationIntent=ReadOnly" in connection_string


def test_connection_string_keeps_explicit_odbc_fields() -> None:
    """Individual ODBC fields supplied alongside a connection string must survive."""
    connection_string, _ = build_connection_config({"connection_string": "DSN=analytics", "database": "reporting"})

    assert "DSN=analytics" in connection_string
    assert "Database=reporting" in connection_string


def test_explicit_field_outranks_the_same_option_inside_the_connection_string() -> None:
    """ODBC honours a repeated keyword's first occurrence, so the explicit field must lead."""
    connection_string, _ = build_connection_config({"connection_string": "Database=prod", "database": "staging"})

    assert connection_string.index("Database=staging") < connection_string.index("Database=prod")


def test_an_already_braced_value_is_not_quoted_again() -> None:
    """The braced driver spelling is the usual one and must reach the driver manager intact."""
    connection_string, _ = build_connection_config({"driver": "{ODBC Driver 18 for SQL Server}", "server": "db"})

    assert "Driver={ODBC Driver 18 for SQL Server};" in connection_string


def test_a_value_needing_quotes_is_braced() -> None:
    """A value carrying ODBC punctuation cannot be read as the start of another keyword."""
    connection_string, _ = build_connection_config({"driver": "PostgreSQL Unicode(x64)", "server": "db"})

    assert "Driver={PostgreSQL Unicode(x64)};" in connection_string


def test_a_value_carrying_a_closing_brace_is_rejected() -> None:
    """ODBC defines no escape for it, so quoting would silently truncate the value."""
    with pytest.raises(ImproperConfigurationError, match="closing brace"):
        build_connection_config({"driver": "d", "pwd": "p{a}}ss}"})


def test_connection_string_alone_is_unchanged() -> None:
    """With no other fields the connection string is passed through as given."""
    connection_string, _ = build_connection_config({"connection_string": "DSN=analytics;"})

    assert connection_string == "DSN=analytics;"


def test_connect_kwargs_still_survive_a_connection_string() -> None:
    """Connect keyword arguments are separate from the connection string."""
    _, connect_kwargs = build_connection_config({"connection_string": "DSN=analytics;", "user": "svc"})

    assert connect_kwargs["user"] == "svc"


def test_payload_text_encoding_reaches_read_arrow_batches() -> None:
    """The encoding is a read parameter, so it must be threaded through the read call."""
    connection = _RecordingConnection()
    driver = ArrowOdbcDriver(
        connection=cast("Any", connection), driver_features={"payload_text_encoding": TextEncoding.UTF16}
    )

    driver._read_arrow_batches("SELECT 1", None, 100)  # pyright: ignore[reportPrivateUsage]

    assert connection.read_kwargs[0]["payload_text_encoding"] is TextEncoding.UTF16


def test_payload_text_encoding_is_omitted_when_unset() -> None:
    """Leaving the feature unset must not override the driver's own default."""
    connection = _RecordingConnection()
    driver = ArrowOdbcDriver(connection=cast("Any", connection))

    driver._read_arrow_batches("SELECT 1", None, 100)  # pyright: ignore[reportPrivateUsage]

    assert "payload_text_encoding" not in connection.read_kwargs[0]


def test_driver_pooling_is_enabled_once_per_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """The upstream switch is process-global and must be called exactly once."""
    calls: list[str] = []
    monkeypatch.setattr(arrow_odbc_config, "_DRIVER_POOLING_ENABLED", False)
    monkeypatch.setattr(arrow_odbc_config, "enable_odbc_connection_pooling", lambda: calls.append("enabled"))

    arrow_odbc_config._apply_driver_pooling(True)  # pyright: ignore[reportPrivateUsage]
    arrow_odbc_config._apply_driver_pooling(True)  # pyright: ignore[reportPrivateUsage]

    assert calls == ["enabled"]


def test_a_later_config_that_declines_pooling_does_not_disturb_it(monkeypatch: pytest.MonkeyPatch) -> None:
    """Declining is the default, so it must not fail or undo another config's opt-in."""
    calls: list[str] = []
    monkeypatch.setattr(arrow_odbc_config, "_DRIVER_POOLING_ENABLED", False)
    monkeypatch.setattr(arrow_odbc_config, "enable_odbc_connection_pooling", lambda: calls.append("enabled"))

    arrow_odbc_config._apply_driver_pooling(True)  # pyright: ignore[reportPrivateUsage]
    arrow_odbc_config._apply_driver_pooling(False)  # pyright: ignore[reportPrivateUsage]

    assert calls == ["enabled"]


def test_driver_pooling_defaults_to_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without the feature the upstream switch is never touched."""
    calls: list[str] = []
    monkeypatch.setattr(arrow_odbc_config, "_DRIVER_POOLING_ENABLED", False)
    monkeypatch.setattr(arrow_odbc_config, "enable_odbc_connection_pooling", lambda: calls.append("enabled"))

    arrow_odbc_config._apply_driver_pooling(False)  # pyright: ignore[reportPrivateUsage]

    assert calls == []


def test_enable_driver_pooling_is_a_declared_feature() -> None:
    assert (
        "enable_driver_pooling"
        in ArrowOdbcConfig(
            connection_config={"dsn": "analytics"}, driver_features={"enable_driver_pooling": False}
        ).driver_features
    )


@pytest.mark.parametrize(
    ("sqlstate", "expected"),
    [("23000", IntegrityError), ("22001", DataError), ("08001", DatabaseConnectionError), ("42S02", SQLParsingError)],
)
def test_sqlstate_classes_map_to_sqlspec_exceptions(sqlstate: str, expected: type[Exception]) -> None:
    """Non-SQL-Server ODBC drivers report SQLSTATE, not native error numbers."""
    mapped = create_mapped_exception(Exception(f"State: {sqlstate}, Message: driver reported a failure"))

    assert isinstance(mapped, expected)
