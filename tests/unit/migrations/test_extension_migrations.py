"""Unit tests for typed extension migration modes, reconciler discovery, and adapter guards."""

import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.adapters.adbc.core import (
    RECONCILE_ADBC_DIALECTS,
    is_sqlite_reconcile_target,
    resolve_reconcile_adbc_dialect,
)
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
    assert cmds1.extension_reconcilers == (ExtensionReconcilerDescriptor("ext_demo", ext_dir / "reconcile.py"),)

    config.add_extension_migrations("ext_demo", ext_dir, migration_mode="versioned")
    assert config._migration_commands is None

    cmds2 = config.get_migration_commands()
    assert cmds2 is not cmds1
    assert cmds2.extension_reconcilers == ()

    config.add_extension_migrations("ext_demo", ext_dir, migration_mode="versioned")
    assert config._migration_commands is cmds2

    config.add_extension_migrations("ext_demo", ext_dir, migration_mode="reconcile")
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
    assert proc_cfg.stdout.strip() == "reconcile"


def test_resolve_extension_migration_mode_precedence_and_validation() -> None:
    default_mode: ExtensionMigrationMode = resolve_extension_migration_mode(None, None, extension_name="adk")
    assert default_mode == "reconcile"
    assert (
        resolve_extension_migration_mode("reconcile", {"migration_mode": "versioned"}, extension_name="adk")
        == "reconcile"
    )
    assert (
        resolve_extension_migration_mode("versioned", {"migration_mode": "reconcile"}, extension_name="adk")
        == "versioned"
    )
    with pytest.raises(MigrationError, match="adk"):
        resolve_extension_migration_mode("reconcile", {"migration_mode": "bad"}, extension_name="adk")


def test_versioned_mode_ignores_reconcile_module_and_keeps_numbered_files(tmp_path: Path) -> None:
    ext_dir = _write_extension_dir(tmp_path / "ext_ver", with_reconcile=True, poison_reconcile=True)
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path / "app_migrations")},
    )
    config.add_extension_migrations("ext_ver", ext_dir, migration_mode="versioned")

    commands = SyncMigrationCommands(config)
    assert commands.extension_reconcilers == ()
    versions = [version for version, _ in commands.runner.get_migration_files()]
    assert "ext_ext_ver_0001" in versions

    fallback_dir = _write_extension_dir(tmp_path / "ext_fallback", with_reconcile=False)
    fallback_cfg = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path / "fallback_migrations")},
    )
    fallback_cfg.add_extension_migrations("ext_fallback", fallback_dir)
    fallback_cmds = SyncMigrationCommands(fallback_cfg)
    assert fallback_cmds.extension_reconcilers == ()
    assert "ext_ext_fallback_0001" in [v for v, _ in fallback_cmds.runner.get_migration_files()]


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
        "snowflake",
        "bigquery",
        "sqlanywhere",
        "sybase",
        "db2",
        "teradata",
    }
    assert UNCONDITIONAL_BUILTIN_RECONCILE_ADAPTERS == expected_unconditional

    for name in sorted(expected_unconditional):
        assert validate_builtin_reconcile_adapter(_make_adapter_stub(name)) == name

    assert validate_builtin_reconcile_adapter(_make_adapter_stub("arrow_odbc", dialect="mssql")) == "arrow_odbc"
    assert validate_builtin_reconcile_adapter(_make_adapter_stub("arrow_odbc", dialect="tsql")) == "arrow_odbc"
    assert validate_builtin_reconcile_adapter(_make_adapter_stub("arrow_odbc", dialect="db2")) == "arrow_odbc"

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


def test_adbc_explicit_statement_dialect_and_derive_pool() -> None:
    assert RECONCILE_ADBC_DIALECTS == frozenset({
        "bigquery",
        "db2",
        "duckdb",
        "mssql",
        "oracle",
        "postgres",
        "snowflake",
        "sqlite",
        "teradata",
    })

    auto_cfg = AdbcConfig(connection_config={"uri": "grpc+tls://h:8815"})
    assert auto_cfg.statement_config.dialect == "duckdb"
    assert auto_cfg.explicit_statement_dialect is None
    derived_auto = auto_cfg.derive_pool()
    assert derived_auto.explicit_statement_dialect is None

    explicit_cfg = AdbcConfig(
        connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://h:8815"},
        statement_config=StatementConfig(dialect="postgres"),
    )
    assert explicit_cfg.explicit_statement_dialect == "postgres"
    derived_explicit = explicit_cfg.derive_pool()
    assert derived_explicit.explicit_statement_dialect == "postgres"


def test_resolve_reconcile_adbc_dialect_known_backends() -> None:
    cases: list[tuple[AdbcConfig, str]] = [
        (AdbcConfig(connection_config={"driver_name": "adbc_driver_postgresql"}), "postgres"),
        (AdbcConfig(connection_config={"uri": "duckdb:///:memory:"}), "duckdb"),
        (AdbcConfig(connection_config={"driver_name": "adbc_driver_snowflake"}), "snowflake"),
        (AdbcConfig(connection_config={"driver_name": "adbc_driver_bigquery"}), "bigquery"),
        (AdbcConfig(connection_config={"uri": "oracle://h/s"}), "oracle"),
        (AdbcConfig(connection_config={"uri": "db2://h/d"}), "db2"),
        (AdbcConfig(connection_config={"uri": "mssql://h/d"}), "mssql"),
        (AdbcConfig(connection_config={"driver_name": "adbc_driver_sqlite"}), "sqlite"),
        (AdbcConfig(connection_config={"driver_name": "gizmosql"}), "duckdb"),
        (AdbcConfig(connection_config={"driver_name": "gizmosql", "gizmosql_backend": "sqlite"}), "sqlite"),
        (
            AdbcConfig(
                connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://h:8815"},
                statement_config=StatementConfig(dialect="postgres"),
            ),
            "postgres",
        ),
        (
            AdbcConfig(
                connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://h:8815"},
                statement_config=StatementConfig(dialect="tsql"),
            ),
            "mssql",
        ),
        (
            AdbcConfig(
                connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://h:8815"},
                statement_config=StatementConfig(dialect="postgresql"),
            ),
            "postgres",
        ),
    ]
    for cfg, expected_dialect in cases:
        resolved = resolve_reconcile_adbc_dialect(cfg.connection_config, cfg.explicit_statement_dialect)
        assert resolved == expected_dialect
        assert validate_builtin_reconcile_adapter(cfg) == "adbc"


def test_adbc_flightsql_refusals_require_explicit_non_sqlite_dialect() -> None:
    refused_configs = [
        AdbcConfig(connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://h:8815"}),
        AdbcConfig(connection_config={"uri": "grpc+tls://h:8815"}),
        AdbcConfig(
            connection_config={"driver_name": "adbc_driver_flightsql", "uri": "grpc://h:8815"},
            statement_config=StatementConfig(dialect="sqlite"),
        ),
        AdbcConfig(connection_config={"driver_name": "/opt/postgresql/libvendor.so"}),
    ]
    for cfg in refused_configs:
        assert resolve_reconcile_adbc_dialect(cfg.connection_config, cfg.explicit_statement_dialect) is None
        with pytest.raises(MigrationError) as exc_info:
            validate_builtin_reconcile_adapter(cfg)
        err_text = str(exc_info.value)
        assert "statement_config" in err_text
        assert "gizmosql_backend" in err_text
        assert 'migration_mode="versioned"' in err_text


def test_adbc_nontransactional_migrate_up_commits_reconciled_ddl(tmp_path: Path) -> None:
    pytest.importorskip("adbc_driver_sqlite")
    db_file = tmp_path / "adbc_reconcile_persist.db"
    config = AdbcConfig(
        connection_config={"driver_name": "adbc_driver_sqlite", "uri": str(db_file)},
        migration_config={"script_location": str(tmp_path / "migrations"), "include_extensions": ["litestar"]},
        extension_config={"litestar": {"session_table": "ls_persisted", "migration_mode": "reconcile"}},
    )
    config.migrate_up()
    with config.provide_session() as session:
        tables = {str(t.get("table_name") or "").lower() for t in session.data_dictionary.get_tables(session)}
        assert "ls_persisted" in tables
    config.close_pool()


def test_oracle_tracking_ddl_orders_default_before_not_null_and_normalizes_uppercase_rows() -> None:
    from unittest.mock import MagicMock

    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker(version_table_name="ddl_migrations")
    oracle_ddl = tracker._tracking_table_ddl().build(dialect="oracle").sql
    assert oracle_ddl.index("DEFAULT CURRENT_TIMESTAMP") < oracle_ddl.index("NOT NULL")

    driver = MagicMock()
    result = MagicMock()
    result.data = [{"VERSION_NUM": "0001", "EXECUTION_SEQUENCE": 1}]
    result.get_data.return_value = [{"VERSION_NUM": "0001", "EXECUTION_SEQUENCE": 1}]
    driver.execute.return_value = result

    assert tracker.get_current_version(driver) == "0001"
    applied = tracker.get_applied_migrations(driver)
    assert applied[0]["version_num"] == "0001"


def test_adbc_driver_and_runner_implicit_transaction_detection() -> None:
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from sqlspec.adapters.adbc.driver import AdbcDriver
    from sqlspec.migrations.runner import BaseMigrationRunner

    fake_adbc_conn = SimpleNamespace(
        _commit_supported=True, _autocommit=False, adbc_connection=SimpleNamespace(get_option=lambda _k: "true")
    )
    drv = AdbcDriver(connection=MagicMock())
    drv._transaction_active = True
    assert drv._has_implicit_transaction() is True

    drv._transaction_active = False
    drv.driver_features = {"autocommit": True}
    assert drv._has_implicit_transaction() is False

    drv.driver_features = {}
    drv.connection = cast("Any", SimpleNamespace(_commit_supported=False, _autocommit=False))
    assert drv._has_implicit_transaction() is False

    drv.connection = cast("Any", fake_adbc_conn)
    assert drv._has_implicit_transaction() is False

    drv.connection = cast("Any", SimpleNamespace(autocommit=False))
    assert drv._has_implicit_transaction() is True

    drv.connection = cast("Any", SimpleNamespace(autocommit=None))
    assert drv._has_implicit_transaction() is False

    async def _coro() -> bool:
        return True

    assert BaseMigrationRunner._has_active_implicit_transaction(cast("Any", SimpleNamespace())) is False
    assert (
        BaseMigrationRunner._has_active_implicit_transaction(
            cast("Any", SimpleNamespace(_connection_in_transaction=lambda: True))
        )
        is True
    )
    assert (
        BaseMigrationRunner._has_active_implicit_transaction(
            cast(
                "Any",
                SimpleNamespace(
                    _connection_in_transaction=lambda: (_ for _ in ()).throw(NotImplementedError()),
                    _has_implicit_transaction="not-callable",
                ),
            )
        )
        is False
    )
    assert (
        BaseMigrationRunner._has_active_implicit_transaction(
            cast(
                "Any",
                SimpleNamespace(
                    _connection_in_transaction=_coro,
                    _has_implicit_transaction=lambda: (_ for _ in ()).throw(AttributeError()),
                ),
            )
        )
        is False
    )
    assert (
        BaseMigrationRunner._has_active_implicit_transaction(
            cast("Any", SimpleNamespace(_connection_in_transaction=lambda: False, _has_implicit_transaction=_coro))
        )
        is False
    )


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_reconciler_downgrade_runs_only_at_base_not_step_back(tmp_path: Path, is_async: bool) -> None:
    """downgrade('-1') preserves reconciled extension tables while downgrade('base') drops them."""
    core_dir = tmp_path / "migrations"
    core_dir.mkdir()
    (core_dir / "0001_core.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE core_t (id INTEGER PRIMARY KEY);\n\n"
        "-- name: migrate-0001-down\nDROP TABLE core_t;\n",
        encoding="utf-8",
    )
    ext_dir = tmp_path / "ext_demo"
    ext_dir.mkdir()
    (ext_dir / "reconcile.py").write_text(
        """
async def _up_async(driver):
    await driver.execute("CREATE TABLE IF NOT EXISTS ext_demo_t (id INTEGER PRIMARY KEY)")

async def _down_async(driver):
    await driver.execute("DROP TABLE IF EXISTS ext_demo_t")

def reconcile(context):
    if context.is_async_driver:
        return _up_async(context.driver)
    context.driver.execute("CREATE TABLE IF NOT EXISTS ext_demo_t (id INTEGER PRIMARY KEY)")
    return None

def downgrade(context):
    if context.is_async_driver:
        return _down_async(context.driver)
    context.driver.execute("DROP TABLE IF EXISTS ext_demo_t")
    return None
""",
        encoding="utf-8",
    )

    cfg: SqliteConfig | AiosqliteConfig
    if is_async:
        cfg = AiosqliteConfig(
            connection_config={"database": str(tmp_path / "reconcile_base.db")},
            migration_config={
                "enabled": True,
                "script_location": str(core_dir),
                "include_extensions": ["demo"],
                "use_logger": False,
            },
            extension_config={"demo": {"migrations_path": ext_dir}},
        )
        await AsyncMigrationCommands(cfg).upgrade(echo=False)
        await AsyncMigrationCommands(cfg).downgrade(revision="-1", echo=False)
        async with cfg.provide_session() as driver:
            tables_after_step = {
                row["name"] for row in await driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
        assert "core_t" not in tables_after_step
        assert "ext_demo_t" in tables_after_step

        await AsyncMigrationCommands(cfg).downgrade(revision="-1", echo=False)
        async with cfg.provide_session() as driver:
            tables_after_second_step = {
                row["name"] for row in await driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
        assert "ext_demo_t" in tables_after_second_step

        await AsyncMigrationCommands(cfg).downgrade(revision="base", echo=False)
        async with cfg.provide_session() as driver:
            tables_after_base = {
                row["name"] for row in await driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
        await cfg.close_pool()
        assert "ext_demo_t" not in tables_after_base
        return

    cfg = SqliteConfig(
        connection_config={"database": str(tmp_path / "reconcile_base.db")},
        migration_config={
            "enabled": True,
            "script_location": str(core_dir),
            "include_extensions": ["demo"],
            "use_logger": False,
        },
        extension_config={"demo": {"migrations_path": ext_dir}},
    )
    SyncMigrationCommands(cfg).upgrade(echo=False)
    SyncMigrationCommands(cfg).downgrade(revision="-1", echo=False)
    with cfg.provide_session() as driver:
        tables_after_step = {
            row["name"] for row in driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    assert "core_t" not in tables_after_step
    assert "ext_demo_t" in tables_after_step

    SyncMigrationCommands(cfg).downgrade(revision="-1", echo=False)
    with cfg.provide_session() as driver:
        tables_after_second_step = {
            row["name"] for row in driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    assert "ext_demo_t" in tables_after_second_step

    SyncMigrationCommands(cfg).downgrade(revision="base", echo=False)
    with cfg.provide_session() as driver:
        tables_after_base = {
            row["name"] for row in driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    cfg.close_pool()
    assert "ext_demo_t" not in tables_after_base
