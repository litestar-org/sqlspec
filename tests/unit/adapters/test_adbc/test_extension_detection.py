"""Unit tests for ADBC postgres extension detection logic."""

from pytest import MonkeyPatch

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.adbc.core import (
    apply_driver_features,
    build_postgres_extension_probe_names,
    detect_postgres_extensions,
    get_statement_config,
    resolve_postgres_extension_state,
)
from sqlspec.core import StatementConfig


class _Cursor:
    def __init__(self, rows: list[tuple[str]], *, error: Exception | None = None) -> None:
        self.rows = rows
        self.error = error
        self.closed = False

    def execute(self, _sql: str, _parameters: object) -> None:
        if self.error is not None:
            raise self.error

    def fetchall(self) -> list[tuple[str]]:
        return self.rows

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.cursor_requested = False

    def cursor(self) -> _Cursor:
        self.cursor_requested = True
        return self._cursor


def test_apply_driver_features_sets_pgvector_default() -> None:
    """enable_pgvector defaults based on PGVECTOR_INSTALLED flag."""
    statement_config = get_statement_config("postgres")
    _, features = apply_driver_features(statement_config, None)
    assert "enable_pgvector" in features
    # PGVECTOR_INSTALLED is an OptionalDependencyFlag (truthy/falsy)
    assert features["enable_pgvector"] is not None


def test_apply_driver_features_sets_paradedb_default() -> None:
    """enable_paradedb defaults to True."""
    statement_config = get_statement_config("postgres")
    _, features = apply_driver_features(statement_config, None)
    assert features["enable_paradedb"] is True


def test_apply_driver_features_respects_user_overrides() -> None:
    """User can explicitly disable extension detection."""
    statement_config = get_statement_config("postgres")
    _, features = apply_driver_features(statement_config, {"enable_pgvector": False, "enable_paradedb": False})
    assert features["enable_pgvector"] is False
    assert features["enable_paradedb"] is False


def test_build_postgres_extension_probe_names_filters_disabled_features() -> None:
    """Only enabled extension probes should be returned."""
    assert build_postgres_extension_probe_names({"enable_pgvector": True, "enable_paradedb": False}) == ["vector"]


def test_detect_postgres_extensions_returns_tuple() -> None:
    """detect_postgres_extensions returns (pgvector_available, paradedb_available)."""
    cursor = _Cursor([("vector",)])
    connection = _Connection(cursor)

    pgvector, paradedb = detect_postgres_extensions(connection, enable_pgvector=True, enable_paradedb=True)
    assert pgvector is True
    assert paradedb is False
    assert cursor.closed is True


def test_detect_postgres_extensions_both_available() -> None:
    """Both extensions detected when both present."""
    connection = _Connection(_Cursor([("vector",), ("pg_search",)]))

    pgvector, paradedb = detect_postgres_extensions(connection, enable_pgvector=True, enable_paradedb=True)
    assert pgvector is True
    assert paradedb is True


def test_detect_postgres_extensions_none_enabled() -> None:
    """Returns (False, False) when both flags disabled."""
    connection = _Connection(_Cursor([]))

    pgvector, paradedb = detect_postgres_extensions(connection, enable_pgvector=False, enable_paradedb=False)
    assert pgvector is False
    assert paradedb is False
    assert connection.cursor_requested is False


def test_detect_postgres_extensions_handles_error() -> None:
    """Returns (False, False) on query failure."""
    cursor = _Cursor([], error=Exception("connection error"))
    connection = _Connection(cursor)

    pgvector, paradedb = detect_postgres_extensions(connection, enable_pgvector=True, enable_paradedb=True)
    assert pgvector is False
    assert paradedb is False
    assert cursor.closed is True


def test_adbc_config_initializes_extension_flags_to_none() -> None:
    """AdbcConfig starts with _pgvector_available and _paradedb_available as None."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "sqlite"})
    assert config._pgvector_available is None  # pyright: ignore[reportPrivateUsage]
    assert config._paradedb_available is None  # pyright: ignore[reportPrivateUsage]


def test_resolve_postgres_extension_state_promotes_paradedb() -> None:
    """Detected extensions should promote the runtime dialect."""
    statement_config, pgvector_available, paradedb_available = resolve_postgres_extension_state(
        get_statement_config("postgres"), {"enable_pgvector": True, "enable_paradedb": True}, {"vector", "pg_search"}
    )

    assert statement_config.dialect == "paradedb"
    assert pgvector_available is True
    assert paradedb_available is True


def test_adbc_config_update_dialect_for_extensions_pgvector() -> None:
    """Dialect switches to pgvector when pgvector is available."""
    config = AdbcConfig(connection_config={"uri": "postgresql://localhost/test"})
    config._pgvector_available = True  # pyright: ignore[reportPrivateUsage]
    config._paradedb_available = False  # pyright: ignore[reportPrivateUsage]
    config._update_dialect_for_extensions()  # pyright: ignore[reportPrivateUsage]
    assert config.statement_config.dialect == "pgvector"


def test_adbc_config_update_dialect_for_extensions_paradedb() -> None:
    """Dialect switches to paradedb when both extensions available (paradedb > pgvector)."""
    config = AdbcConfig(connection_config={"uri": "postgresql://localhost/test"})
    config._pgvector_available = True  # pyright: ignore[reportPrivateUsage]
    config._paradedb_available = True  # pyright: ignore[reportPrivateUsage]
    config._update_dialect_for_extensions()  # pyright: ignore[reportPrivateUsage]
    assert config.statement_config.dialect == "paradedb"


def test_adbc_config_update_dialect_skips_non_postgres() -> None:
    """Dialect is not changed for non-postgres backends."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "sqlite"})
    original_dialect = config.statement_config.dialect
    config._pgvector_available = True  # pyright: ignore[reportPrivateUsage]
    config._paradedb_available = True  # pyright: ignore[reportPrivateUsage]
    config._update_dialect_for_extensions()  # pyright: ignore[reportPrivateUsage]
    assert config.statement_config.dialect == original_dialect


def test_adbc_config_update_dialect_preserves_custom_dialect() -> None:
    """If user explicitly set a non-postgres dialect, don't override it."""
    config = AdbcConfig(
        connection_config={"uri": "postgresql://localhost/test"}, statement_config=StatementConfig(dialect="custom")
    )
    config._pgvector_available = True  # pyright: ignore[reportPrivateUsage]
    config._paradedb_available = True  # pyright: ignore[reportPrivateUsage]
    config._update_dialect_for_extensions()  # pyright: ignore[reportPrivateUsage]
    assert config.statement_config.dialect == "custom"


def test_adbc_config_provide_session_skips_extension_probe_for_non_postgres(monkeypatch: MonkeyPatch) -> None:
    """Non-postgres sessions should not create a connection for extension detection."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": ":memory:"})

    def fail_create_connection(_self: AdbcConfig) -> None:
        raise AssertionError("non-postgres startup path should not probe extensions")

    monkeypatch.setattr(AdbcConfig, "create_connection", fail_create_connection)

    session = config.provide_session()

    assert session is not None
    assert config._pgvector_available is False  # pyright: ignore[reportPrivateUsage]
    assert config._paradedb_available is False  # pyright: ignore[reportPrivateUsage]
    assert config.statement_config.dialect == "sqlite"
