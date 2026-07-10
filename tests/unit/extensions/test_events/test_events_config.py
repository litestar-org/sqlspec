"""Configuration-related tests for extension auto-migration inclusion."""

import importlib
from pathlib import Path
from typing import get_args, get_type_hints

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import EventsConfig


def test_events_backend_literal_uses_canonical_transport_names() -> None:
    """The SQLSpec selector excludes the Litestar Queues-only polling mode."""

    backend_hint = get_type_hints(EventsConfig)["backend"]
    backend_args = get_args(backend_hint)
    backend_values = get_args(backend_args[0]) if len(backend_args) == 1 and get_args(backend_args[0]) else backend_args

    assert set(backend_values) == {"notify", "notify_queue", "poll_queue", "aq", "txeventq"}


_POSTGRES_DRIVER_FEATURES = (
    ("sqlspec.adapters.asyncpg.config", "AsyncpgDriverFeatures"),
    ("sqlspec.adapters.psycopg.config", "PsycopgDriverFeatures"),
    ("sqlspec.adapters.psqlpy.config", "PsqlpyDriverFeatures"),
)
_POSTGRES_EVENT_BACKENDS = {"notify", "notify_queue", "poll_queue"}
_RETIRED_EVENT_BACKENDS = ("listen_notify", "listen_notify_durable", "table_queue")


@pytest.mark.parametrize(("module_name", "features_name"), _POSTGRES_DRIVER_FEATURES)
def test_postgres_driver_feature_event_backends_are_canonical_literals(module_name: str, features_name: str) -> None:
    """PostgreSQL adapter feature typing exposes only canonical event transports."""
    module = importlib.import_module(module_name)
    features = getattr(module, features_name)
    annotation = features.__annotations__["events_backend"]
    literal = get_args(annotation)[0]

    assert set(get_args(literal)) == _POSTGRES_EVENT_BACKENDS


def test_active_event_config_prose_uses_canonical_transport_names() -> None:
    """Active adapter config and event reference docs do not advertise retired names."""
    project_root = Path(__file__).parents[4]
    paths = [*project_root.glob("sqlspec/adapters/*/config.py"), project_root / "docs/reference/extensions/events.rst"]

    violations = {
        str(path.relative_to(project_root)): retired
        for path in paths
        for retired in _RETIRED_EVENT_BACKENDS
        if retired in path.read_text(encoding="utf-8")
    }

    assert violations == {}


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
        extension_config={"events": {"backend": "notify"}},
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
            "events": {"backend": "poll_queue"},
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
        extension_config={"litestar": {"session_table": True}, "events": {"backend": "notify"}},
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
