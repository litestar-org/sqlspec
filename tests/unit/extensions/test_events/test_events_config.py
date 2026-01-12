"""Configuration-related tests for extension auto-migration inclusion."""

from sqlspec.adapters.sqlite import SqliteConfig


def test_events_extension_auto_includes_migrations(tmp_path) -> None:
    """Configs with events settings auto-include extension migrations."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "events.db")},
        migration_config={"script_location": "migrations"},
        extension_config={"events": {"queue_table": "app_events"}},
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is not None
    assert "events" in include_extensions


def test_events_extension_preserves_existing_includes(tmp_path) -> None:
    """Existing include_extensions lists are preserved and extended."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "events_existing.db")},
        migration_config={"include_extensions": ["custom"]},
        extension_config={"events": {}},
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions == ["custom", "events"]


def test_exclude_extensions_prevents_auto_inclusion(tmp_path) -> None:
    """exclude_extensions prevents auto-inclusion of events migrations."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "events_skip.db")},
        migration_config={"script_location": "migrations", "exclude_extensions": ["events"]},
        extension_config={"events": {"backend": "listen_notify"}},
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is None or "events" not in include_extensions


def test_litestar_with_session_table_true_auto_includes_migrations(tmp_path) -> None:
    """Litestar with session_table=True auto-includes migrations."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "litestar.db")},
        migration_config={"script_location": "migrations"},
        extension_config={"litestar": {"session_table": True}},
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is not None
    assert "litestar" in include_extensions


def test_litestar_with_session_table_string_auto_includes_migrations(tmp_path) -> None:
    """Litestar with session_table as string auto-includes migrations."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "litestar_custom.db")},
        migration_config={"script_location": "migrations"},
        extension_config={"litestar": {"session_table": "my_sessions"}},
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is not None
    assert "litestar" in include_extensions


def test_litestar_without_session_table_no_migrations(tmp_path) -> None:
    """Litestar without session_table does NOT auto-include migrations."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "litestar_di.db")},
        migration_config={"script_location": "migrations"},
        extension_config={"litestar": {"session_key": "db"}},  # Just DI config, no session storage
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is None or "litestar" not in include_extensions


def test_adk_extension_auto_includes_migrations(tmp_path) -> None:
    """Configs with adk settings auto-include extension migrations."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "adk.db")},
        migration_config={"script_location": "migrations"},
        extension_config={"adk": {"session_table": "adk_sessions"}},
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is not None
    assert "adk" in include_extensions


def test_multiple_extensions_auto_include_migrations(tmp_path) -> None:
    """Multiple extensions with settings all get auto-included."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "multi.db")},
        migration_config={"script_location": "migrations"},
        extension_config={
            "litestar": {"session_table": True},  # Needs session_table for migrations
            "adk": {},
            "events": {"backend": "table_queue"},
        },
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is not None
    assert "litestar" in include_extensions
    assert "adk" in include_extensions
    assert "events" in include_extensions


def test_exclude_extensions_partial(tmp_path) -> None:
    """exclude_extensions only excludes specified extensions."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "partial.db")},
        migration_config={"script_location": "migrations", "exclude_extensions": ["events"]},
        extension_config={"litestar": {"session_table": True}, "events": {"backend": "listen_notify"}},
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is not None
    assert "litestar" in include_extensions
    assert "events" not in include_extensions


def test_no_auto_include_without_extension_config(tmp_path) -> None:
    """Extensions not in extension_config are not auto-included."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "empty.db")}, migration_config={"script_location": "migrations"}
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is None


def test_observability_extensions_no_migrations(tmp_path) -> None:
    """Observability extensions (otel, prometheus) don't have migrations."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "otel.db")},
        migration_config={"script_location": "migrations"},
        extension_config={"otel": {"enabled": True}, "prometheus": {"enabled": True}},
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions is None
