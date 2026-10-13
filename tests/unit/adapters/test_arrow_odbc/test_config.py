"""arrow-odbc configuration surface: connection string merging, encoding, pooling, SQLSTATE."""

from typing import Any, cast

import pytest

pytest.importorskip("arrow_odbc")

from arrow_odbc import TextEncoding

import sqlspec.adapters.arrow_odbc.config as arrow_odbc_config
from sqlspec.adapters.arrow_odbc import (
    ArrowOdbcConfig,
    ArrowOdbcDriver,
    apply_driver_features,
    build_connection_config,
    build_statement_config,
    resolve_dialect_from_dbms_name,
)
from sqlspec.adapters.arrow_odbc.core import _extract_sqlstate, create_mapped_exception
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
)


class _RecordingConnection:
    def __init__(self) -> None:
        self.read_kwargs: list[dict[str, Any]] = []

    def read_arrow_batches(self, **kwargs: Any) -> None:
        self.read_kwargs.append(kwargs)


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
    """Explicit field replaces matching option inside connection string with key deduplication."""
    connection_string, _ = build_connection_config({"connection_string": "Database=prod", "database": "staging"})

    assert "Database=staging" in connection_string
    assert "Database=prod" not in connection_string
    assert connection_string.lower().count("database=") == 1


def test_build_connection_config_merges_port_with_connection_string_server() -> None:
    """Discrete port must be attached to the server defined in connection_string for SQL Server."""
    connection_string, _ = build_connection_config(
        {"connection_string": "Server=host;UID=u;PWD=p;", "port": 1433}, dialect="mssql"
    )

    assert "Server=host,1433" in connection_string


def test_build_connection_config_discrete_server_overrides_connection_string() -> None:
    """Discrete server must override server in connection_string for SQL Server."""
    connection_string, _ = build_connection_config(
        {"connection_string": "Server=oldhost;UID=u;PWD=p;", "server": "newhost", "port": 14333}, dialect="mssql"
    )

    assert "Server=newhost,14333" in connection_string
    assert "oldhost" not in connection_string


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


@pytest.mark.parametrize(
    "message", ["Constraint failed: duplicate key value is (40001).", "Column 'TOTAL' has a size of 53000 bytes"]
)
def test_a_five_character_token_in_prose_is_not_read_as_a_sqlstate(message: str) -> None:
    """Only the driver's own State field carries a SQLSTATE, so prose must not be mined for one."""
    assert _extract_sqlstate(message) is None


def test_non_mssql_connection_config_keeps_separate_port_keyword() -> None:
    """Non-SQL-Server dialects emit separate Server and Port keywords instead of Server=host,port."""
    connection_string, _ = build_connection_config({
        "driver": "PostgreSQL Unicode",
        "server": "pg.internal",
        "port": 5432,
        "database": "app",
    })

    assert "Server=pg.internal;" in connection_string
    assert "Port=5432;" in connection_string
    assert "Server=pg.internal,5432" not in connection_string


def test_mssql_inferred_connection_config_uses_comma_port_syntax() -> None:
    """SQL Server drivers combine server and port with a comma when dialect is inferred."""
    connection_string, _ = build_connection_config({"driver": "FreeTDS", "server": "sql.internal", "port": 1433})

    assert "Server=sql.internal,1433" in connection_string
    assert "Port=" not in connection_string


@pytest.mark.parametrize(
    ("dbms_name", "expected_dialect"), [("FreeTDS", "mssql"), ("libtdsodbc.so", "mssql"), ("psqlodbcw.so", "postgres")]
)
def test_resolve_dialect_from_dbms_name_recognizes_common_odbc_drivers(dbms_name: str, expected_dialect: str) -> None:
    assert resolve_dialect_from_dbms_name(dbms_name) == expected_dialect


def test_config_reads_extra_options_and_preserves_driver_over_dsn() -> None:
    """ArrowOdbcConfig inspects extra for connection_string/autocommit and prioritizes driver over dsn."""
    config = ArrowOdbcConfig(
        connection_config={
            "extra": {"connection_string": "Driver={PostgreSQL Unicode};Server=localhost;", "autocommit": False}
        }
    )
    assert config.statement_config.dialect == "postgres"
    assert config.driver_features["connection_autocommit"] is False

    driver_and_dsn = ArrowOdbcConfig(connection_config={"driver": "ODBC Driver 18 for SQL Server", "dsn": "analytics"})
    assert driver_and_dsn.driver_features["dbms_name"] == "ODBC Driver 18 for SQL Server"
    assert driver_and_dsn.statement_config.dialect == "tsql"


@pytest.mark.parametrize(
    ("error_text", "expected_type"),
    [
        ("State: 23514, Native error: 0, Message: violates check constraint", CheckViolationError),
        ("State: 42501, Native error: 0, Message: permission denied for table items", PermissionDeniedError),
        ("State: HYT00, Native error: 0, Message: timeout expired", QueryTimeoutError),
        ("State: HYT01, Native error: 0, Message: connection timeout expired", DatabaseConnectionError),
        ("State: 42000, Native error: 229, Message: [SQL Server]SELECT permission was denied", PermissionDeniedError),
        (
            "State: 42000, Native error: 230, Message: [SQL Server]SELECT permission was denied on column",
            PermissionDeniedError,
        ),
        (
            "State: 23000, Native error: 547, Message: [SQL Server]The INSERT statement conflicted with the CHECK constraint",
            CheckViolationError,
        ),
        (
            "State: 23000, Native error: 547, Message: [SQL Server]The INSERT statement conflicted with the FOREIGN KEY constraint",
            ForeignKeyViolationError,
        ),
    ],
)
def test_create_mapped_exception_specific_sqlstates_and_native_codes(
    error_text: str, expected_type: type[Exception]
) -> None:
    mapped = create_mapped_exception(Exception(error_text))
    assert isinstance(mapped, expected_type)


def test_apply_driver_features_applies_json_serializers_to_explicit_statement_config() -> None:
    """Custom JSON serializers in driver_features apply even when statement_config is explicitly provided."""

    def custom_serializer(value: Any) -> str:
        return f"custom:{value}"

    def custom_deserializer(raw: str) -> dict[str, str]:
        return {"decoded": raw}

    explicit_statement_config = build_statement_config(dialect="postgres")

    updated_config, _ = apply_driver_features(
        explicit_statement_config, {"json_serializer": custom_serializer, "json_deserializer": custom_deserializer}
    )
    assert updated_config.parameter_config.json_serializer is custom_serializer
    assert updated_config.parameter_config.json_deserializer is custom_deserializer

    config = ArrowOdbcConfig(
        connection_config={"dsn": "pg"},
        statement_config=explicit_statement_config,
        driver_features={"json_serializer": custom_serializer, "json_deserializer": custom_deserializer},
    )
    assert config.statement_config.parameter_config.json_serializer is custom_serializer
    assert config.statement_config.parameter_config.json_deserializer is custom_deserializer


def test_create_connection_wraps_driver_pooling_failure_in_improper_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failures inside _apply_driver_pooling during create_connection raise ImproperConfigurationError."""
    monkeypatch.setattr(arrow_odbc_config, "_DRIVER_POOLING_ENABLED", False)

    def broken_pooling() -> None:
        msg = "ODBC environment already initialized"
        raise RuntimeError(msg)

    monkeypatch.setattr(arrow_odbc_config, "enable_odbc_connection_pooling", broken_pooling)
    config = ArrowOdbcConfig(connection_config={"dsn": "analytics"}, driver_features={"enable_driver_pooling": True})

    with pytest.raises(ImproperConfigurationError, match="Could not configure arrow-odbc connection"):
        config.create_connection()
