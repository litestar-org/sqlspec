"""Unit tests for typed extension migration modes, reconciler discovery, and adapter guards."""

import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

from sqlspec.adapters.adbc.core import is_sqlite_reconcile_target
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk.store import SqliteADKStore
from sqlspec.adapters.sqlite.events.store import SqliteEventQueueStore
from sqlspec.adapters.sqlite.litestar.store import SqliteStore
from sqlspec.config import (
    ADKConfig,
    EventsConfig,
    ExtensionConfigs,
    ExtensionMigrationMode,
    LitestarConfig,
    resolve_extension_migration_mode,
)
from sqlspec.core import StatementConfig
from sqlspec.exceptions import ImproperConfigurationError, MigrationError
from sqlspec.migrations.base import ExtensionReconcilerDescriptor
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.migrations.utils import UNCONDITIONAL_BUILTIN_RECONCILE_ADAPTERS, validate_builtin_reconcile_adapter


def _write_extension_dir(base_dir: Path, *, with_reconcile: bool = True, poison_reconcile: bool = False) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / "0001_init.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE demo_ext (id INTEGER PRIMARY KEY);\n"
        "-- name: migrate-0001-down\nDROP TABLE demo_ext;\n",
        encoding="utf-8",
    )
    if with_reconcile:
        body = (
            'raise RuntimeError("reconcile module must not be imported during discovery")\n'
            if poison_reconcile
            else "def reconcile(context):\n    return None\n"
        )
        (base_dir / "reconcile.py").write_text(body, encoding="utf-8")
    return base_dir


def test_add_extension_migrations_default_and_explicit_reconcile_mode(tmp_path: Path) -> None:
    ext_dir = _write_extension_dir(tmp_path / "ext_demo")
    config = SqliteConfig(connection_config={"database": ":memory:"})

    config.add_extension_migrations("ext_demo", ext_dir)
    assert config.migration_config["include_extensions"] == ["ext_demo"]
    assert config.extension_config["ext_demo"] == {"migrations_path": ext_dir}

    config2 = SqliteConfig(connection_config={"database": ":memory:"})
    config2.add_extension_migrations(
        "ext_demo", ext_dir, settings={"queue_table": "custom_queue"}, migration_mode="reconcile"
    )
    assert config2.migration_config["include_extensions"] == ["ext_demo"]
    assert config2.extension_config["ext_demo"] == {
        "queue_table": "custom_queue",
        "migrations_path": ext_dir,
        "migration_mode": "reconcile",
    }


def test_add_extension_migrations_explicit_mode_overrides_existing_settings(tmp_path: Path) -> None:
    ext_dir = _write_extension_dir(tmp_path / "ext_demo")
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"ext_demo": {"session_table": "s", "migration_mode": "versioned"}},
    )

    config.add_extension_migrations("ext_demo", ext_dir, migration_mode="reconcile")

    assert config.migration_config["include_extensions"] == ["ext_demo"]
    assert config.extension_config["ext_demo"] == {
        "session_table": "s",
        "migrations_path": ext_dir,
        "migration_mode": "reconcile",
    }


def test_auto_inclusion_preserves_mode_and_litestar_session_table_gate() -> None:
    adk_cfg: ADKConfig = {"migration_mode": "reconcile"}
    events_cfg: EventsConfig = {"migration_mode": "reconcile"}
    litestar_no_session: LitestarConfig = {"disable_di": True, "migration_mode": "reconcile"}
    ext_without_session: ExtensionConfigs = {"adk": adk_cfg, "events": events_cfg, "litestar": litestar_no_session}
    config_without_session = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config=ext_without_session
    )
    assert config_without_session.migration_config["include_extensions"] == ["adk", "events"]
    assert cast("dict[str, Any]", config_without_session.extension_config["litestar"])["migration_mode"] == "reconcile"

    litestar_with_session: LitestarConfig = {"session_table": "ls", "migration_mode": "reconcile"}
    ext_with_session: ExtensionConfigs = {"adk": adk_cfg, "events": events_cfg, "litestar": litestar_with_session}
    config_with_session = SqliteConfig(connection_config={"database": ":memory:"}, extension_config=ext_with_session)
    assert config_with_session.migration_config["include_extensions"] == ["litestar", "adk", "events"]
    assert cast("dict[str, Any]", config_with_session.extension_config["litestar"])["migration_mode"] == "reconcile"


def test_add_extension_migrations_cache_invalidation_on_mode_change(tmp_path: Path) -> None:
    ext_dir = _write_extension_dir(tmp_path / "ext_demo")
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path / "app_migrations")},
    )
    config.add_extension_migrations("ext_demo", ext_dir)

    cmds1 = config.get_migration_commands()
    assert cmds1.extension_reconcilers == ()

    config.add_extension_migrations("ext_demo", ext_dir, migration_mode="reconcile")
    assert config._migration_commands is None

    cmds2 = config.get_migration_commands()
    assert cmds2 is not cmds1
    assert cmds2.extension_reconcilers == (ExtensionReconcilerDescriptor("ext_demo", ext_dir / "reconcile.py"),)

    config.add_extension_migrations("ext_demo", ext_dir, migration_mode="reconcile")
    assert config._migration_commands is cmds2

    config.add_extension_migrations("ext_demo", ext_dir, migration_mode="versioned")
    assert config._migration_commands is None


@pytest.mark.parametrize("bad_mode", ["auto", "", "RECONCILE", True, False, 123, ["reconcile"]])
def test_invalid_migration_mode_rejected_atomically(tmp_path: Path, bad_mode: Any) -> None:
    ext_dir = _write_extension_dir(tmp_path / "ext_demo")
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path / "app_migrations")},
    )
    cmds = config.get_migration_commands()
    prior_ext = dict(config.extension_config)
    prior_mig = dict(config.migration_config)

    with pytest.raises(MigrationError, match="ext_demo"):
        config.add_extension_migrations("ext_demo", ext_dir, migration_mode=bad_mode)

    assert config.extension_config == prior_ext
    assert config.migration_config == prior_mig
    assert config._migration_commands is cmds

    with pytest.raises(MigrationError, match="ext_demo"):
        config.add_extension_migrations("ext_demo", ext_dir, settings={"migration_mode": bad_mode})

    assert config.extension_config == prior_ext
    assert config.migration_config == prior_mig
    assert config._migration_commands is cmds

    with pytest.raises(MigrationError, match="litestar"):
        SqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"litestar": {"migration_mode": bad_mode}}
        )

    with pytest.raises(MigrationError, match="events"):
        SqliteConfig(
            connection_config={"database": ":memory:"},
            migration_config={"exclude_extensions": ["events"]},
            extension_config={"events": {"migration_mode": bad_mode}},
        )


def test_fresh_import_order_has_no_cycle() -> None:
    proc_utils = subprocess.run(
        [sys.executable, "-c", "import sqlspec.migrations.utils"], capture_output=True, text=True, check=True
    )
    assert proc_utils.returncode == 0

    proc_cfg = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sqlspec.config; print(sqlspec.config.resolve_extension_migration_mode(None, None, extension_name='adk'))",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc_cfg.stdout.strip() == "versioned"


def test_resolve_extension_migration_mode_precedence_and_validation() -> None:
    default_mode: ExtensionMigrationMode = resolve_extension_migration_mode(None, None, extension_name="adk")
    assert default_mode == "versioned"
    assert (
        resolve_extension_migration_mode("reconcile", {"migration_mode": "versioned"}, extension_name="adk")
        == "reconcile"
    )
    with pytest.raises(MigrationError, match="adk"):
        resolve_extension_migration_mode("reconcile", {"migration_mode": "bad"}, extension_name="adk")


def test_versioned_mode_ignores_reconcile_module_and_keeps_numbered_files(tmp_path: Path) -> None:
    ext_dir = _write_extension_dir(tmp_path / "ext_ver", with_reconcile=True, poison_reconcile=True)
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path / "app_migrations")},
    )
    config.add_extension_migrations("ext_ver", ext_dir)

    commands = SyncMigrationCommands(config)
    assert commands.extension_reconcilers == ()
    versions = [version for version, _ in commands.runner.get_migration_files()]
    assert "ext_ext_ver_0001" in versions


async def test_reconcile_mode_discovers_lazily_and_omits_numbered_files_sync_and_async(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ext_dir = _write_extension_dir(tmp_path / "ext_rec", with_reconcile=True, poison_reconcile=True)
    sync_cfg = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path / "sync_migrations")},
    )
    sync_cfg.add_extension_migrations("ext_rec", ext_dir, migration_mode="reconcile")

    sync_cmds = SyncMigrationCommands(sync_cfg)
    expected = (ExtensionReconcilerDescriptor("ext_rec", ext_dir / "reconcile.py"),)
    assert sync_cmds.extension_reconcilers == expected
    assert [v for v, _ in sync_cmds.runner.get_migration_files()] == []

    pkg_root = tmp_path / "pkg_root"
    ns_ext_dir = _write_extension_dir(
        pkg_root / "sqlspec" / "extensions" / "custom_ns_ext" / "migrations", with_reconcile=True, poison_reconcile=True
    )
    (pkg_root / "sqlspec" / "extensions" / "custom_ns_ext" / "__init__.py").write_text("", encoding="utf-8")
    import sqlspec.extensions

    monkeypatch.setattr(
        sqlspec.extensions, "__path__", [*list(sqlspec.extensions.__path__), str(pkg_root / "sqlspec" / "extensions")]
    )

    async_cfg = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={
            "script_location": str(tmp_path / "async_migrations"),
            "include_extensions": ["custom_ns_ext"],
        },
        extension_config={"custom_ns_ext": {"migration_mode": "reconcile"}},
    )
    async_cmds = AsyncMigrationCommands(async_cfg)
    assert async_cmds.extension_reconcilers == (
        ExtensionReconcilerDescriptor("custom_ns_ext", ns_ext_dir / "reconcile.py"),
    )
    assert [v for v, _ in await async_cmds.runner.get_migration_files()] == []


def test_multi_extension_order_and_mixed_modes(tmp_path: Path) -> None:
    dir_a = _write_extension_dir(tmp_path / "ext_a")
    dir_b = _write_extension_dir(tmp_path / "ext_b")
    dir_c = _write_extension_dir(tmp_path / "ext_c")

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={
            "script_location": str(tmp_path / "app_migrations"),
            "include_extensions": ["ext_b", "ext_c", "ext_a"],
        },
        extension_config={
            "ext_a": {"migrations_path": dir_a, "migration_mode": "reconcile"},
            "ext_b": {"migrations_path": dir_b, "migration_mode": "reconcile"},
            "ext_c": {"migrations_path": dir_c, "migration_mode": "versioned"},
        },
    )
    commands = SyncMigrationCommands(config)
    assert [d.name for d in commands.extension_reconcilers] == ["ext_b", "ext_a"]
    assert [v for v, _ in commands.runner.get_migration_files()] == ["ext_ext_c_0001"]


def test_missing_reconcile_module_or_unresolvable_extension_raises(tmp_path: Path) -> None:
    no_rec_dir = _write_extension_dir(tmp_path / "no_rec", with_reconcile=False)
    cfg_missing_file = SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(tmp_path / "m1")}
    )
    cfg_missing_file.add_extension_migrations("no_rec", no_rec_dir, migration_mode="reconcile")
    with pytest.raises(MigrationError, match="no_rec"):
        SyncMigrationCommands(cfg_missing_file)

    cfg_missing_dir = SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(tmp_path / "m2")}
    )
    cfg_missing_dir.add_extension_migrations("bad_dir", tmp_path / "does_not_exist", migration_mode="reconcile")
    with pytest.raises(MigrationError, match="bad_dir"):
        SyncMigrationCommands(cfg_missing_dir)

    cfg_ghost_pkg = AiosqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path / "m3"), "include_extensions": ["ghost_ext"]},
        extension_config={"ghost_ext": {"migration_mode": "reconcile"}},
    )
    with pytest.raises(MigrationError, match="ghost_ext"):
        AsyncMigrationCommands(cfg_ghost_pkg)


class _AdapterStub:
    def __init__(
        self, adapter_name: str | None, *, dialect: str = "sqlite", connection_config: dict[str, Any] | None = None
    ) -> None:
        if adapter_name is not None:
            type(self)
        self.statement_config = StatementConfig(dialect=dialect)
        self.connection_config = connection_config or {}


def _make_adapter_stub(
    adapter_name: str | None, *, dialect: str = "sqlite", connection_config: dict[str, Any] | None = None
) -> Any:
    attrs: dict[str, Any] = {}
    if adapter_name is not None:
        attrs["adapter_name"] = adapter_name
    cls = type(f"Stub_{adapter_name or 'none'}", (_AdapterStub,), attrs)
    return cls(adapter_name, dialect=dialect, connection_config=connection_config)


def test_validate_builtin_reconcile_adapter_supported_matrix() -> None:
    expected_unconditional = {
        "sqlite",
        "aiosqlite",
        "asyncpg",
        "psycopg",
        "psqlpy",
        "cockroach_asyncpg",
        "cockroach_psycopg",
        "aiomysql",
        "asyncmy",
        "pymysql",
        "mysqlconnector",
        "duckdb",
        "mssql_python",
        "pymssql",
        "oracledb",
        "spanner",
        "bigquery",
        "sqlanywhere",
    }
    assert UNCONDITIONAL_BUILTIN_RECONCILE_ADAPTERS == expected_unconditional

    for name in sorted(expected_unconditional):
        assert validate_builtin_reconcile_adapter(_make_adapter_stub(name)) == name

    assert validate_builtin_reconcile_adapter(_make_adapter_stub("arrow_odbc", dialect="mssql")) == "arrow_odbc"
    assert validate_builtin_reconcile_adapter(_make_adapter_stub("arrow_odbc", dialect="tsql")) == "arrow_odbc"

    for adbc_conn in (
        {"driver_name": "adbc_driver_sqlite"},
        {"uri": "sqlite:///tmp.db"},
        {"driver_name": "/opt/libadbc_driver_sqlite.so"},
        {"driver_name": "/opt/custom_sqlite.so"},
    ):
        stub = _make_adapter_stub("adbc", dialect="sqlite", connection_config=adbc_conn)
        assert validate_builtin_reconcile_adapter(stub) == "adbc"


def test_validate_builtin_reconcile_adapter_refusals_name_remedies() -> None:
    flightsql_driver = _make_adapter_stub(
        "adbc", dialect="sqlite", connection_config={"driver_name": "adbc_driver_flightsql"}
    )
    with pytest.raises(MigrationError) as exc_flight:
        validate_builtin_reconcile_adapter(flightsql_driver)
    msg_flight = str(exc_flight.value)
    assert "flightsql" in msg_flight
    assert 'migration_mode="versioned"' in msg_flight
    assert "reconcile.py" in msg_flight
    assert "gizmosql_backend" in msg_flight

    flightsql_uri = _make_adapter_stub("adbc", dialect="duckdb", connection_config={"uri": "grpc+tls://localhost:8815"})
    with pytest.raises(MigrationError) as exc_uri:
        validate_builtin_reconcile_adapter(flightsql_uri)
    assert 'migration_mode="versioned"' in str(exc_uri.value)
    assert "reconcile.py" in str(exc_uri.value)

    arrow_pg = _make_adapter_stub("arrow_odbc", dialect="postgres")
    with pytest.raises(MigrationError) as exc_arrow:
        validate_builtin_reconcile_adapter(arrow_pg)
    assert "arrow_odbc" in str(exc_arrow.value)
    assert 'migration_mode="versioned"' in str(exc_arrow.value)
    assert "reconcile.py" in str(exc_arrow.value)

    unresolvable = _make_adapter_stub(None)
    with pytest.raises(MigrationError) as exc_none:
        validate_builtin_reconcile_adapter(unresolvable)
    assert 'migration_mode="versioned"' in str(exc_none.value)
    assert "reconcile.py" in str(exc_none.value)

    custom = _make_adapter_stub("my_custom_adapter")
    with pytest.raises(MigrationError) as exc_custom:
        validate_builtin_reconcile_adapter(custom)
    assert "my_custom_adapter" in str(exc_custom.value)
    assert 'migration_mode="versioned"' in str(exc_custom.value)
    assert "reconcile.py" in str(exc_custom.value)


@pytest.mark.parametrize(
    ("conn_cfg", "expected"),
    [
        ({"driver_name": "adbc_driver_sqlite"}, True),
        ({"uri": "sqlite:///x.db"}, True),
        ({"driver_name": "/opt/libadbc_driver_sqlite.so"}, True),
        ({"driver_name": "/opt/libvendor.so"}, False),
        ({"driver_name": "adbc_driver_flightsql"}, False),
        ({"driver_name": "adbc_driver_sqlite", "uri": "grpc://h:1"}, False),
    ],
)
def test_is_sqlite_reconcile_target(conn_cfg: dict[str, Any], expected: bool) -> None:
    assert is_sqlite_reconcile_target(conn_cfg) is expected


@pytest.mark.parametrize("mode", ["versioned", "reconcile"])
def test_store_allowlists_accept_migration_mode(mode: ExtensionMigrationMode) -> None:
    litestar_cfg: LitestarConfig = {"session_table": "sessions", "migration_mode": mode}
    events_cfg: EventsConfig = {"migration_mode": mode}
    adk_cfg: ADKConfig = {"migration_mode": mode}
    ext_cfg: ExtensionConfigs = {"litestar": litestar_cfg, "events": events_cfg, "adk": adk_cfg}
    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config=ext_cfg)
    litestar_store = SqliteStore(config)
    events_store = SqliteEventQueueStore(config)
    adk_store = SqliteADKStore(config)
    assert litestar_store.table_name == "sessions"
    assert events_store.table_name == "sqlspec_event_queue"
    assert adk_store.session_table == "adk_session"


def test_store_allowlist_still_rejects_unknown_option() -> None:
    config = SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"litestar": {"unknown_option": 1}}
    )
    with pytest.raises(ImproperConfigurationError, match="unknown_option"):
        SqliteStore(config)
