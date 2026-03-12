"""Unit tests for ADBC postgres extension detection logic."""

from unittest.mock import MagicMock

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.adapters.adbc.core import apply_driver_features, detect_postgres_extensions, get_statement_config


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


def test_detect_postgres_extensions_returns_tuple() -> None:
    """detect_postgres_extensions returns (pgvector_available, paradedb_available)."""
    # Mock a connection with a cursor that returns pgvector extension
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("vector",)]
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    pgvector, paradedb = detect_postgres_extensions(mock_conn, enable_pgvector=True, enable_paradedb=True)
    assert pgvector is True
    assert paradedb is False


def test_detect_postgres_extensions_both_available() -> None:
    """Both extensions detected when both present."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("vector",), ("pg_search",)]
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    pgvector, paradedb = detect_postgres_extensions(mock_conn, enable_pgvector=True, enable_paradedb=True)
    assert pgvector is True
    assert paradedb is True


def test_detect_postgres_extensions_none_enabled() -> None:
    """Returns (False, False) when both flags disabled."""
    mock_conn = MagicMock()

    pgvector, paradedb = detect_postgres_extensions(mock_conn, enable_pgvector=False, enable_paradedb=False)
    assert pgvector is False
    assert paradedb is False
    # Should not have queried the database
    mock_conn.cursor.assert_not_called()


def test_detect_postgres_extensions_handles_error() -> None:
    """Returns (False, False) on query failure."""
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("connection error")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    pgvector, paradedb = detect_postgres_extensions(mock_conn, enable_pgvector=True, enable_paradedb=True)
    assert pgvector is False
    assert paradedb is False


def test_adbc_config_initializes_extension_flags_to_none() -> None:
    """AdbcConfig starts with _pgvector_available and _paradedb_available as None."""
    config = AdbcConfig(connection_config={"uri": ":memory:", "driver_name": "sqlite"})
    assert config._pgvector_available is None  # pyright: ignore[reportPrivateUsage]
    assert config._paradedb_available is None  # pyright: ignore[reportPrivateUsage]


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
    from sqlspec.core import StatementConfig

    config = AdbcConfig(
        connection_config={"uri": "postgresql://localhost/test"}, statement_config=StatementConfig(dialect="custom")
    )
    config._pgvector_available = True  # pyright: ignore[reportPrivateUsage]
    config._paradedb_available = True  # pyright: ignore[reportPrivateUsage]
    config._update_dialect_for_extensions()  # pyright: ignore[reportPrivateUsage]
    assert config.statement_config.dialect == "custom"
