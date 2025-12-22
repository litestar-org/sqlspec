"""Configuration-related tests for the events extension."""

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
        migration_config={"include_extensions": ["litestar"]},
        extension_config={"events": {}},
    )

    include_extensions = config.migration_config.get("include_extensions")
    assert include_extensions == ["litestar", "events"]
