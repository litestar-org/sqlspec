"""DuckDB configuration tests covering statement config builders."""

import asyncio
import tempfile
from pathlib import Path
from typing import Any, NotRequired, get_type_hints
from unittest.mock import patch
from uuid import UUID, uuid4

import duckdb
import pytest

from sqlspec.adapters.adbc.core import build_connection_config as build_adbc_connection_config
from sqlspec.adapters.duckdb.config import (
    DuckDBConfig,
    DuckDBConnectionParams,
    DuckDBExtensionConfig,
    DuckDBPoolParams,
    DuckDBSecretConfig,
)
from sqlspec.adapters.duckdb.core import (
    _parse_memory_size_bytes,
    apply_driver_features,
    build_connection_config,
    build_statement_config,
    default_statement_config,
    resolve_rowcount,
)
from sqlspec.adapters.duckdb.driver import DuckDBDriver
from sqlspec.adapters.duckdb.pool import _SQL_IDENTIFIER_RE, DuckDBConnectionPool, _validate_sql_identifier
from sqlspec.driver._common import _CLOSED_CONNECTION
from sqlspec.exceptions import ImproperConfigurationError


def test_build_default_statement_config_custom_serializer() -> None:
    """Custom serializer should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    statement_config = build_statement_config(json_serializer=serializer)
    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_duckdb_config_applies_driver_feature_serializer() -> None:
    """Driver features should mutate the DuckDB statement configuration."""

    def serializer(_: object) -> str:
        return "feature"

    config = DuckDBConfig(driver_features={"json_serializer": serializer})
    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_duckdb_config_honors_uuid_conversion_flag() -> None:
    """Driver features should control native UUID parameter coercion without string sniffing."""

    uuid_value = UUID("550e8400-e29b-41d4-a716-446655440000")

    enabled_config = DuckDBConfig(driver_features={"enable_uuid_conversion": True})
    disabled_config = DuckDBConfig(driver_features={"enable_uuid_conversion": False})

    enabled_map = enabled_config.statement_config.parameter_config.type_coercion_map
    disabled_map = disabled_config.statement_config.parameter_config.type_coercion_map

    assert str not in enabled_map
    assert str not in disabled_map
    assert enabled_map[UUID](uuid_value) == str(uuid_value)
    assert UUID not in disabled_map


def test_connection_param_types_match_current_duckdb_settings() -> None:
    """Typed config keys should match DuckDB 1.5 setting types."""

    hints = get_type_hints(DuckDBConnectionParams, include_extras=True)

    assert hints["parquet_metadata_cache"] == NotRequired[bool]
    assert hints["progress_bar_time"] == NotRequired[int]


def test_pool_params_do_not_type_noop_pool_sizing_keys() -> None:
    """Only pool settings consumed by DuckDBConnectionPool should remain typed."""

    hints = get_type_hints(DuckDBPoolParams, include_extras=True)

    assert "pool_min_size" not in hints
    assert "pool_max_size" not in hints
    assert "pool_timeout" not in hints
    assert hints["pool_recycle_seconds"] == NotRequired[int]
    assert hints["health_check_interval"] == NotRequired[float]


def test_extension_config_types_repository_url() -> None:
    """DuckDB install_extension supports repository_url as a distinct kwarg."""

    hints = get_type_hints(DuckDBExtensionConfig, include_extras=True)

    assert hints["repository_url"] == NotRequired[str]


def test_secret_config_types_scope_provider_persistence_and_optional_values() -> None:
    """Provider-based DuckDB secrets do not always need inline value pairs."""

    hints = get_type_hints(DuckDBSecretConfig, include_extras=True)

    assert hints["value"] == NotRequired[dict[str, Any]]
    assert hints["provider"] == NotRequired[str]
    assert hints["scope"] == NotRequired[str]
    assert hints["persistent"] == NotRequired[bool]
    assert hints["replace"] == NotRequired[bool]


def test_build_connection_config_preserves_nested_config_and_extra() -> None:
    """DuckDB connect should receive database/read_only plus one merged config dict."""

    connection_config = {
        "database": "analytics.duckdb",
        "read_only": True,
        "config": {"threads": 1, "parquet_metadata_cache": True},
        "extra": {"custom_user_agent": "sqlspec-tests"},
        "progress_bar_time": 250,
        "pool_min_size": 2,
        "pool_max_size": 4,
        "pool_timeout": 10.0,
        "pool_recycle_seconds": 60,
        "health_check_interval": 5.0,
    }

    assert build_connection_config(connection_config) == {
        "database": "analytics.duckdb",
        "read_only": True,
        "config": {
            "threads": 1,
            "parquet_metadata_cache": True,
            "custom_user_agent": "sqlspec-tests",
            "progress_bar_time": 250,
        },
    }


def test_build_connection_config_resolves_database_aliases() -> None:
    """DuckDB connect should map db, path, and file aliases to database without leaking to config."""
    assert build_connection_config({"path": "my.duckdb"}) == {"database": "my.duckdb"}
    assert build_connection_config({"db": "my.duckdb"}) == {"database": "my.duckdb"}
    assert build_connection_config({"file": "my.duckdb"}) == {"database": "my.duckdb"}


def test_duckdb_config_resolves_database_aliases() -> None:
    """DuckDBConfig should resolve path/db/file aliases and not fall back to shared memory."""
    config = DuckDBConfig(connection_config={"path": "target.duckdb"})
    assert config.connection_config["database"] == "target.duckdb"
    assert "path" not in config.connection_config

    config_db = DuckDBConfig(connection_config={"db": "target2.duckdb"})
    assert config_db.connection_config["database"] == "target2.duckdb"
    assert "db" not in config_db.connection_config


def test_driver_init_does_not_apply_driver_features_when_statement_config_is_none() -> None:
    connection = duckdb.connect(database=":memory:")
    driver_features: dict[str, Any] = {"json_serializer": lambda value: "[]"}
    driver = DuckDBDriver(connection=connection, statement_config=None, driver_features=driver_features)

    assert driver.driver_features is not driver_features
    assert driver.driver_features == driver_features


def test_driver_init_apply_driver_features_not_called_when_statement_config_provided() -> None:
    connection = duckdb.connect(database=":memory:")
    driver_features: dict[str, Any] = {"json_serializer": lambda value: "[]"}
    driver = DuckDBDriver(
        connection=connection, statement_config=default_statement_config, driver_features=driver_features
    )

    assert driver.statement_config is default_statement_config
    assert driver.driver_features is not driver_features
    assert driver.driver_features == driver_features


def test_driver_init_custom_json_serializer_identity_preserved_through_session() -> None:

    def custom_serializer(_: object) -> str:
        return "custom"

    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, driver_features={"json_serializer": custom_serializer}
    )
    with config.provide_session() as driver:
        serializer_in_driver = driver.statement_config.parameter_config.json_serializer
    assert serializer_in_driver is custom_serializer


def test_driver_init_apply_driver_features_not_called_per_session_via_config() -> None:

    def custom_serializer(_: object) -> str:
        return "custom"

    with patch("sqlspec.adapters.duckdb.config.apply_driver_features", wraps=apply_driver_features) as mock_apply:
        config = DuckDBConfig(
            connection_config={"database": ":memory:"}, driver_features={"json_serializer": custom_serializer}
        )
        with config.provide_session():
            pass
    assert mock_apply.call_count == 1


def test_bool_type_coercion_preserves_native_bool() -> None:
    """Boolean parameters must remain native Python bools rather than coercing to int."""
    coercion_map = default_statement_config.parameter_config.type_coercion_map
    assert coercion_map[bool](True) is True
    assert coercion_map[bool](False) is False

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    with config.provide_session() as driver:
        row = driver.select_one("SELECT typeof(?) AS t, ? AS v", True, True)
        assert row["t"] == "BOOLEAN"
        assert row["v"] is True


def test_apply_driver_features_sets_events_defaults() -> None:
    """apply_driver_features should default enable_events=True and events_backend='poll_queue'."""
    _, features = apply_driver_features(default_statement_config, None)
    assert features["enable_events"] is True
    assert features["events_backend"] == "poll_queue"


def test_resolve_rowcount_falls_back_to_cursor_rowcount_when_fetchone_returns_none() -> None:
    """resolve_rowcount must consult cursor.rowcount when cursor.fetchone() returns None without raising."""

    class _NoneFetchCursor:
        rowcount = 4

        def fetchone(self) -> None:
            return None

    assert resolve_rowcount(_NoneFetchCursor()) == 4


def test_provide_session_storage_settings_caching() -> None:
    """DuckDBConnectionPool caches _storage_settings on thread-local state."""
    config = DuckDBConfig(connection_config={"database": ":memory:"})
    pool = config.create_pool()
    with pool.get_connection() as conn:
        settings1 = pool._storage_settings(conn)
        settings2 = pool._storage_settings(conn)
        assert settings1 is settings2


def test_version_tuple_parsing() -> None:
    """_parse_duckdb_version parses release and pre-release version strings correctly."""
    from sqlspec.adapters.duckdb.core import _parse_duckdb_version, duckdb_version_tuple

    assert _parse_duckdb_version("1.2.0") == (1, 2, 0)
    assert _parse_duckdb_version("2.0.0rc1") == (2, 0, 0)
    assert _parse_duckdb_version("v0.9.2") == (0, 9, 2)
    assert duckdb_version_tuple() >= (0, 0, 0)


def test_duckdb_replica_paths_reject_writer_file_and_memory(tmp_path) -> None:
    from sqlspec.exceptions import ImproperConfigurationError

    database = str(tmp_path / "writer.duckdb")
    for replica in (True, database, ":memory:"):
        with pytest.raises(ImproperConfigurationError):
            DuckDBConfig(connection_config={"database": database}, replicas=replica)
    primary = DuckDBConfig(connection_config={"database": database}, replicas=str(tmp_path / "snapshot.duckdb"))
    reader = primary.replica_groups["default"].configs[0]
    assert reader.connection_config["read_only"] is True
    assert primary.connection_config.get("read_only", False) is False
    shared_reader = DuckDBConfig(connection_config={"database": database, "read_only": True}, replicas=database)
    assert shared_reader.replica_groups["default"].configs[0].connection_config["read_only"] is True


def test_duckdb_snapshot_reader_enforces_native_read_only(tmp_path) -> None:
    from sqlspec.exceptions import ReadOnlyTransactionError

    snapshot_path = str(tmp_path / "snapshot.duckdb")
    snapshot = DuckDBConfig(connection_config={"database": snapshot_path})
    with snapshot.provide_session() as session:
        session.execute("CREATE TABLE entries (id INTEGER)")
        session.execute("INSERT INTO entries VALUES (1)")
    snapshot.close_pool()
    primary = DuckDBConfig(connection_config={"database": str(tmp_path / "writer.duckdb")}, replicas=snapshot_path)
    with primary.provide_read_session() as reader:
        assert reader.select_value("SELECT COUNT(*) FROM entries") == 1
        with pytest.raises(ReadOnlyTransactionError):
            reader.execute("INSERT INTO entries VALUES (2)")
    primary.close_pool()


@pytest.mark.parametrize("body_raises", [False, True], ids=["clean-exit", "error-exit"])
def test_provide_session_rolls_back_open_transaction_on_exit(body_raises: bool) -> None:
    """DuckDB provide_session rolls back uncommitted work and detaches the driver on clean and error exit."""
    config = DuckDBConfig(connection_config={"database": ":memory:session_rollback_test"})
    try:
        with config.provide_session() as setup_session:
            setup_session.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER); DELETE FROM items;")
        if body_raises:
            with pytest.raises(ValueError, match="abort"), config.provide_session() as session:
                session.begin()
                session.execute("INSERT INTO items VALUES (1)")
                raise ValueError("abort")
        else:
            with config.provide_session() as session:
                session.begin()
                session.execute("INSERT INTO items VALUES (1)")
        assert session.connection is _CLOSED_CONNECTION
        with pytest.raises(ImproperConfigurationError, match="already closed"):
            session.execute("SELECT 1")
        with config.provide_session() as check_session:
            assert check_session.select_value("SELECT COUNT(*) FROM items") == 0
    finally:
        config.close_pool()


def test_provide_session_failed_rollback_forwards_exception_to_pool_and_avoids_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Clean-exit rollback failure forwards error metadata to pool release, detaches the driver, and avoids commit."""
    db_file = tmp_path / "failed_rollback.duckdb"
    config = DuckDBConfig(connection_config={"database": str(db_file)})
    try:
        with config.provide_session() as setup_session:
            setup_session.execute("CREATE TABLE items (id INTEGER)")

        def fail_rollback(_self: DuckDBDriver) -> None:
            raise RuntimeError("duckdb rollback failed")

        monkeypatch.setattr(DuckDBDriver, "rollback", fail_rollback)
        with pytest.raises(RuntimeError, match="duckdb rollback failed"), config.provide_session() as session:
            session.begin()
            session.execute("INSERT INTO items VALUES (42)")

        assert session.connection is _CLOSED_CONNECTION
        assert config.provide_pool().size() == 0
        monkeypatch.undo()
        with config.provide_session() as check_session:
            assert check_session.select_value("SELECT COUNT(*) FROM items") == 0
    finally:
        config.close_pool()


def test_duckdb_patterns_reject_trailing_newline() -> None:
    """DuckDB secret identifier and memory limit patterns reject trailing newlines."""
    assert _SQL_IDENTIFIER_RE.match("secret_name\n") is None
    assert _parse_memory_size_bytes("10MB\n") is None
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _validate_sql_identifier("bad_secret\n", "secret_name")
    cfg = DuckDBConfig(
        connection_config={"database": ":memory:bad_secret_nl"},
        driver_features={
            "secrets": [{"name": "bad_secret\n", "secret_type": "s3", "value": {"key_id": "a"}, "required": True}]
        },
    )
    try:
        with pytest.raises(ValueError, match="Invalid SQL identifier"), cfg.provide_session():
            pass
    finally:
        cfg.close_pool()


@pytest.mark.parametrize("db_value", [None, "", ":memory:"])
def test_duckdb_default_in_memory_configs_are_isolated(db_value: str | None) -> None:
    """Separate DuckDBConfig instances with omitted, empty, or ':memory:' database stay isolated."""
    conn_cfg_a: DuckDBPoolParams = {} if db_value is None else {"database": db_value}
    conn_cfg_b: DuckDBPoolParams = {} if db_value is None else {"database": db_value}
    cfg_a = DuckDBConfig(connection_config=conn_cfg_a)
    cfg_b = DuckDBConfig(connection_config=conn_cfg_b)
    try:
        db_a = cfg_a.connection_config["database"]
        db_b = cfg_b.connection_config["database"]
        assert isinstance(db_a, str)
        assert isinstance(db_b, str)
        assert db_a.startswith(":memory:sqlspec_")
        assert db_b.startswith(":memory:sqlspec_")
        assert db_a != db_b

        with cfg_a.provide_session() as s_a:
            s_a.execute("CREATE TABLE iso_items (id INTEGER)")
            s_a.execute("INSERT INTO iso_items VALUES (1)")
        with cfg_b.provide_session() as s_b:
            rows = s_b.select_value("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'iso_items'")
            assert rows == 0
    finally:
        cfg_a.close_pool()
        cfg_b.close_pool()


def test_duckdb_explicit_named_memory_database_is_shared() -> None:
    """Explicit ':memory:<name>' databases remain shared across independent DuckDBConfig instances."""
    shared_name = f":memory:shared_{uuid4().hex}"
    cfg_a = DuckDBConfig(connection_config={"database": shared_name})
    cfg_b = DuckDBConfig(connection_config={"database": shared_name})
    try:
        assert cfg_a.connection_config["database"] == shared_name
        assert cfg_b.connection_config["database"] == shared_name
        with cfg_a.provide_session() as s_a:
            s_a.execute("CREATE TABLE shared_items (id INTEGER)")
            s_a.execute("INSERT INTO shared_items VALUES (42)")
        with cfg_b.provide_session() as s_b:
            assert s_b.select_value("SELECT id FROM shared_items") == 42
    finally:
        cfg_a.close_pool()
        cfg_b.close_pool()


def test_duckdb_in_memory_creates_and_cleans_private_temp_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """In-memory DuckDB pools allocate an isolated temp_directory and remove it on close_pool."""
    cwd_dir = tmp_path / "cwd"
    temp_root = tmp_path / "sys_tmp"
    cwd_dir.mkdir()
    temp_root.mkdir()
    monkeypatch.chdir(cwd_dir)
    monkeypatch.setenv("TMPDIR", str(temp_root))
    monkeypatch.setattr(tempfile, "tempdir", str(temp_root))

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    managed_dir: Path | None = None
    try:
        with config.provide_session() as session:
            configured = session.select_value("SELECT current_setting('temp_directory')")
            assert isinstance(configured, str)
            managed_dir = Path(configured)
            assert managed_dir.exists()
            assert managed_dir.name.startswith("sqlspec_duckdb_")
            assert managed_dir.parent == temp_root
            assert not (cwd_dir / ".tmp").exists()
    finally:
        config.close_pool()

    assert managed_dir is not None
    assert not managed_dir.exists()
    assert list(cwd_dir.iterdir()) == []


def test_duckdb_explicit_temp_directory_is_preserved_on_close(tmp_path: Path) -> None:
    """An explicit temp_directory on an in-memory DuckDBConfig is respected and not deleted on close."""
    explicit_dir = tmp_path / "user_spill"
    explicit_dir.mkdir()
    marker = explicit_dir / "keep.txt"
    marker.write_text("ok", encoding="utf-8")

    config = DuckDBConfig(connection_config={"database": ":memory:", "temp_directory": str(explicit_dir)})
    try:
        with config.provide_session() as session:
            configured = session.select_value("SELECT current_setting('temp_directory')")
            assert isinstance(configured, str)
            assert Path(configured) == explicit_dir
    finally:
        config.close_pool()

    assert explicit_dir.exists()
    assert marker.exists()


@pytest.mark.parametrize(
    ("conn_cfg"),
    [
        {"driver_name": "duckdb", "path": ":memory:"},
        {"driver_name": "duckdb", "uri": ":memory:"},
        {"driver_name": "duckdb", "uri": "duckdb://:memory:"},
        {"driver_name": "duckdb", "uri": "duckdb:///:memory:"},
        {"driver_name": "adbc_driver_duckdb", "entrypoint": "duckdb_adbc_init", "uri": ":memory:"},
    ],
)
def test_adbc_duckdb_memory_uri_normalizes_to_path(conn_cfg: dict[str, Any]) -> None:
    """DuckDB ADBC configurations normalize ':memory:' URIs to path=':memory:' without uri."""
    normalized = build_adbc_connection_config(conn_cfg)
    assert normalized["path"] == ":memory:"
    assert "uri" not in normalized


@pytest.mark.parametrize(
    ("conn_cfg"),
    [
        {"driver_name": "duckdb", "path": ":memory:shared_db"},
        {"driver_name": "duckdb", "uri": ":memory:shared_db"},
        {"driver_name": "duckdb", "uri": "duckdb://:memory:shared_db"},
        {"driver_name": "duckdb", "uri": "duckdb:///:memory:shared_db"},
    ],
)
def test_adbc_duckdb_rejects_named_memory_database(conn_cfg: dict[str, Any]) -> None:
    """Named ':memory:<name>' identifiers are rejected for DuckDB ADBC to avoid local file creation."""
    with pytest.raises(ImproperConfigurationError, match="DuckDB ADBC"):
        build_adbc_connection_config(conn_cfg)


@pytest.mark.parametrize("use_memory", [False, True])
def test_duckdb_config_reuses_idle_task_connections_sequentially_and_isolates_concurrent(
    tmp_path: Path, use_memory: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sequential asyncio tasks reuse idle DuckDB connections while concurrent tasks isolate them."""
    db_target = ":memory:" if use_memory else str(tmp_path / "seq_tasks.duckdb")
    config = DuckDBConfig(connection_config={"database": db_target})
    pool = config.provide_pool()
    original_new_connection = DuckDBConnectionPool.new_connection
    created_connections: list[Any] = []

    def counting_new_connection(pool_self: DuckDBConnectionPool, record_thread_state: bool = False) -> Any:
        conn = original_new_connection(pool_self, record_thread_state=record_thread_state)
        if pool_self is pool:
            created_connections.append(conn)
        return conn

    monkeypatch.setattr(DuckDBConnectionPool, "new_connection", counting_new_connection)

    async def _run() -> None:
        seq_ids: list[int] = []

        async def sequential_worker(idx: int) -> None:
            with config.provide_session() as session:
                seq_ids.append(id(session.connection))
                assert session.select_value("SELECT ?", idx) == idx

        for idx in range(12):
            await asyncio.create_task(sequential_worker(idx))
        await asyncio.sleep(0)

        assert len(created_connections) == 1
        assert len(set(seq_ids)) == 1
        assert len(pool._idle_task_states) == 1

        all_entered = asyncio.Event()
        entered = 0
        conc_ids: list[int] = []

        async def concurrent_worker() -> None:
            nonlocal entered
            with config.provide_session() as session:
                conc_ids.append(id(session.connection))
                entered += 1
                if entered == 10:
                    all_entered.set()
                await all_entered.wait()

        tasks = [asyncio.create_task(concurrent_worker()) for _ in range(10)]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0)
        assert len(set(conc_ids)) == 10
        assert len(created_connections) == 10
        assert len(pool._idle_task_states) == 8

    try:
        asyncio.run(_run())
    finally:
        config.close_pool()

    assert pool._idle_task_states == []
    assert pool.size() == 0
