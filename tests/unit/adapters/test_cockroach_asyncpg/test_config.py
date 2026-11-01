"""Unit tests for CockroachDB AsyncPG configuration.

Tests cover:
- CockroachAsyncpgConfig initialization and defaults
- Driver feature propagation (retry, follower reads, JSON serializers)
- Connection config normalization
"""

from typing import Any, NotRequired, cast, get_args, get_origin
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.cockroach_asyncpg import (
    CockroachAsyncpgConfig,
    CockroachAsyncpgConnectionConfig,
    CockroachAsyncpgDriver,
    CockroachAsyncpgDriverFeatures,
    CockroachAsyncpgPoolConfig,
    CockroachAsyncpgRetryConfig,
    build_connection_config,
)
from sqlspec.adapters.cockroach_asyncpg.adk.store import CockroachAsyncpgADKMemoryStore, CockroachAsyncpgADKStore
from sqlspec.adapters.cockroach_asyncpg.litestar.store import CockroachAsyncpgStore
from sqlspec.core import SQL, StatementConfig
from sqlspec.exceptions import ImproperConfigurationError


def test_cockroach_asyncpg_native_storage_defaults_off() -> None:
    config = CockroachAsyncpgConfig()
    assert config.driver_features["enable_native_storage"] is False


def test_cockroach_asyncpg_native_storage_options_reach_driver() -> None:
    options = {"nullas": "NULL", "nullif": "NULL", "skip": 0}
    config = CockroachAsyncpgConfig(
        driver_features={"enable_native_storage": True, "native_storage_csv_options": options}
    )
    driver = config.driver_type(connection=cast("Any", object()), driver_features=config.driver_features)
    assert driver.driver_features["enable_native_storage"] is True
    assert driver.driver_features["native_storage_csv_options"] == options


@pytest.mark.parametrize(
    "options",
    [None, [], {"invalid": "fragment"}, {"nullas": 1}, {"nullif": None}, {"skip": -1}, {"skip": True}, {"skip": "1"}],
)
def test_cockroach_asyncpg_native_storage_rejects_invalid_csv_options(options: Any) -> None:
    with pytest.raises(ImproperConfigurationError, match="native_storage_csv_options"):
        CockroachAsyncpgConfig(driver_features={"native_storage_csv_options": options})


@pytest.mark.parametrize("options", [{}, {"skip": 1}, {"nullas": ""}, {"nullif": ""}])
def test_cockroach_asyncpg_native_storage_preserves_explicit_csv_options(options: dict[str, Any]) -> None:
    config = CockroachAsyncpgConfig(driver_features={"native_storage_csv_options": options})
    assert config.driver_features["native_storage_csv_options"] == options


def test_cockroach_asyncpg_config_default_initialization() -> None:
    """Config should initialize with sensible defaults."""
    config = CockroachAsyncpgConfig()
    assert config.connection_config is not None
    assert config.statement_config is not None
    assert config.driver_features is not None


def test_cockroach_asyncpg_config_auto_retry_enabled_by_default() -> None:
    """Auto retry should be enabled by default."""
    config = CockroachAsyncpgConfig()
    assert config.driver_features.get("enable_auto_retry") is True


def test_cockroach_asyncpg_config_retry_config_extraction() -> None:
    """Retry config should be extractable from driver features."""
    config = CockroachAsyncpgConfig(driver_features={"max_retries": 5, "retry_delay_base_ms": 100.0})
    retry_config = CockroachAsyncpgRetryConfig.from_features(config.driver_features)
    assert retry_config.max_retries == 5
    assert retry_config.base_delay_ms == 100.0


def test_cockroach_asyncpg_config_disable_auto_retry() -> None:
    """Auto retry can be explicitly disabled."""
    config = CockroachAsyncpgConfig(driver_features={"enable_auto_retry": False})
    assert config.driver_features.get("enable_auto_retry") is False


def test_cockroach_asyncpg_config_follower_reads_configuration() -> None:
    """Follower reads settings should be stored in driver features."""
    config = CockroachAsyncpgConfig(driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"})
    assert config.driver_features.get("enable_follower_reads") is True
    assert config.driver_features.get("default_staleness") == "'-10s'"


def test_cockroach_asyncpg_config_json_serializer_propagation() -> None:
    """JSON serializers should propagate to statement config."""

    def custom_serializer(obj: object) -> str:
        return f"custom:{obj}"

    def custom_deserializer(s: str) -> object:
        return {"parsed": s}

    config = CockroachAsyncpgConfig(
        driver_features={"json_serializer": custom_serializer, "json_deserializer": custom_deserializer}
    )
    param_config = config.statement_config.parameter_config
    assert param_config.json_serializer is custom_serializer
    assert param_config.json_deserializer is custom_deserializer


def test_cockroach_asyncpg_config_connection_config_dict_normalization() -> None:
    """Connection config dict should be normalized."""
    config = CockroachAsyncpgConfig(connection_config={"host": "localhost", "port": 26257, "database": "testdb"})
    assert config.connection_config["host"] == "localhost"
    assert config.connection_config["port"] == 26257


def test_cockroach_asyncpg_config_dsn_in_connection_config() -> None:
    """DSN string should be accepted in connection config."""
    config = CockroachAsyncpgConfig(connection_config={"dsn": "postgresql://user:pass@localhost:26257/testdb"})
    assert "dsn" in config.connection_config


def test_cockroach_asyncpg_config_bind_key_configuration() -> None:
    """Bind key should be stored for multi-database setups."""
    config = CockroachAsyncpgConfig(bind_key="cockroach_primary")
    assert config.bind_key == "cockroach_primary"


def test_cockroach_asyncpg_connection_config_describes_current_asyncpg_connect_keys() -> None:
    """Typed config should include current asyncpg connection keywords."""
    annotations = CockroachAsyncpgConnectionConfig.__annotations__

    assert annotations["service"] is not None
    assert annotations["servicefile"] is not None
    assert annotations["timeout"] is not None
    assert annotations["target_session_attrs"] is not None
    assert annotations["krbsrvname"] is not None
    assert annotations["gsslib"] is not None

    target_session_attrs_annotation = cast("Any", annotations["target_session_attrs"])
    assert get_origin(target_session_attrs_annotation) is NotRequired
    assert set(get_args(get_args(target_session_attrs_annotation)[0])) == {
        "any",
        "primary",
        "standby",
        "read-write",
        "read-only",
        "prefer-standby",
    }

    gsslib_annotation = cast("Any", annotations["gsslib"])
    assert get_origin(gsslib_annotation) is NotRequired
    assert set(get_args(get_args(gsslib_annotation)[0])) == {"gssapi", "sspi"}


def test_cockroach_asyncpg_pool_config_describes_current_asyncpg_pool_keys() -> None:
    """Typed pool config should include asyncpg pool-only keywords."""
    annotations = CockroachAsyncpgPoolConfig.__annotations__

    assert annotations["connect"] is not None
    assert annotations["reset"] is not None
    assert annotations["loop"] is not None
    assert annotations["connection_class"] is not None
    assert annotations["record_class"] is not None


@pytest.mark.anyio
async def test_cockroach_asyncpg_create_pool_forwards_current_asyncpg_config_keys() -> None:
    """Pool creation should pass through current asyncpg connection and pool keywords."""

    async def reset(connection: object) -> None:
        _ = connection

    async def connect_factory(**_: Any) -> object:
        return object()

    loop = object()
    connection_class = object()
    record_class = object()
    pool = object()
    config = CockroachAsyncpgConfig(
        connection_config={
            "connect": connect_factory,
            "host": "localhost",
            "port": 26257,
            "database": "defaultdb",
            "service": "cockroach",
            "servicefile": "/tmp/pg_service.conf",
            "timeout": 3.5,
            "target_session_attrs": "read-write",
            "krbsrvname": "postgres",
            "gsslib": "gssapi",
            "reset": reset,
            "loop": loop,
            "connection_class": connection_class,
            "record_class": record_class,
        }
    )

    with patch(
        "sqlspec.adapters.cockroach_asyncpg.config.asyncpg_create_pool", new=AsyncMock(return_value=pool)
    ) as mock:
        result = await config._create_pool()

    assert result is pool
    await_args = mock.await_args
    assert await_args is not None
    kwargs = await_args.kwargs
    assert kwargs["service"] == "cockroach"
    assert kwargs["servicefile"] == "/tmp/pg_service.conf"
    assert kwargs["timeout"] == 3.5
    assert kwargs["target_session_attrs"] == "read-write"
    assert kwargs["krbsrvname"] == "postgres"
    assert kwargs["gsslib"] == "gssapi"
    assert kwargs["connect"] is connect_factory
    assert kwargs["reset"] is reset
    assert kwargs["loop"] is loop
    assert kwargs["connection_class"] is connection_class
    assert kwargs["record_class"] is record_class
    assert kwargs["init"] == config._init_connection


async def test_cockroach_asyncpg_config_init_connection_registers_json_codecs_before_user_hook() -> None:
    """Connection init should install JSON codecs before user callbacks."""
    events: list[str] = []

    async def user_hook(connection: object) -> None:
        events.append("user")

    async def register_json(connection: object, **_: object) -> None:
        events.append("json")

    config = CockroachAsyncpgConfig(driver_features={"on_connection_create": user_hook})
    connection = AsyncMock()
    with patch(
        "sqlspec.adapters.cockroach_asyncpg.config.register_json_codecs",
        new=AsyncMock(side_effect=register_json),
        create=True,
    ) as register_mock:
        await config._init_connection(connection)
    register_mock.assert_awaited_once()
    assert events == ["json", "user"]


async def test_cockroach_asyncpg_config_init_connection_skips_json_codecs_when_disabled() -> None:
    """Disabling JSON codecs should preserve the user callback."""
    events: list[str] = []

    async def user_hook(connection: object) -> None:
        events.append("user")

    config = CockroachAsyncpgConfig(driver_features={"enable_json_codecs": False, "on_connection_create": user_hook})
    connection = AsyncMock()
    with patch(
        "sqlspec.adapters.cockroach_asyncpg.config.register_json_codecs", new_callable=AsyncMock, create=True
    ) as register_mock:
        await config._init_connection(connection)
    register_mock.assert_not_awaited()
    assert events == ["user"]


@pytest.mark.anyio
async def test_cockroach_asyncpg_init_connection_registers_pgvector_when_enabled() -> None:
    config = CockroachAsyncpgConfig(driver_features={"enable_pgvector": True})
    connection = AsyncMock()
    with (
        patch("sqlspec.adapters.cockroach_asyncpg.config.register_json_codecs", new_callable=AsyncMock, create=True),
        patch(
            "sqlspec.adapters.cockroach_asyncpg.config.register_pgvector_support", new_callable=AsyncMock, create=True
        ) as register_pgvector,
    ):
        await config._init_connection(connection)
    register_pgvector.assert_awaited_once_with(connection)


@pytest.mark.anyio
async def test_cockroach_asyncpg_init_connection_skips_pgvector_by_default() -> None:
    config = CockroachAsyncpgConfig()
    connection = AsyncMock()
    with (
        patch("sqlspec.adapters.cockroach_asyncpg.config.register_json_codecs", new_callable=AsyncMock, create=True),
        patch(
            "sqlspec.adapters.cockroach_asyncpg.config.register_pgvector_support", new_callable=AsyncMock, create=True
        ) as register_pgvector,
    ):
        await config._init_connection(connection)
    register_pgvector.assert_not_awaited()


def test_cockroach_asyncpg_provide_session_uses_lazy_lambda() -> None:
    config = CockroachAsyncpgConfig()
    session_config = config.provide_session()._statement_config
    assert callable(session_config)


def test_cockroach_asyncpg_provide_session_reflects_post_construction_dialect_change() -> None:
    config = CockroachAsyncpgConfig()
    context = config.provide_session()
    config.statement_config = StatementConfig(dialect="pgvector")
    session_config = context._statement_config
    assert callable(session_config)
    assert session_config().dialect == "pgvector"


def test_cockroach_asyncpg_driver_features_typed_dict_accepts_retry_features() -> None:
    """TypedDict should accept all retry-related features."""
    features: CockroachAsyncpgDriverFeatures = {
        "enable_auto_retry": True,
        "max_retries": 5,
        "retry_delay_base_ms": 50.0,
        "retry_delay_max_ms": 3000.0,
        "enable_retry_logging": True,
    }
    assert features["enable_auto_retry"] is True
    assert features["max_retries"] == 5


def test_cockroach_asyncpg_driver_features_typed_dict_accepts_follower_read_features() -> None:
    """TypedDict should accept follower read features."""
    features: CockroachAsyncpgDriverFeatures = {"enable_follower_reads": True, "default_staleness": "'-5s'"}
    assert features["enable_follower_reads"] is True
    assert features["default_staleness"] == "'-5s'"


def test_cockroach_asyncpg_driver_features_typed_dict_accepts_json_features() -> None:
    """TypedDict should accept JSON codec features."""

    def serializer_fn(x: object) -> str:
        return str(x)

    def deserializer_fn(x: str) -> object:
        return x

    features: CockroachAsyncpgDriverFeatures = {
        "json_serializer": serializer_fn,
        "json_deserializer": deserializer_fn,
        "enable_json_codecs": True,
    }
    assert features["enable_json_codecs"] is True


def test_cockroach_asyncpg_driver_features_typed_dict_accepts_event_features() -> None:
    """TypedDict should accept event backend features."""
    features: CockroachAsyncpgDriverFeatures = {"enable_events": True, "events_backend": "poll_queue"}
    assert features["enable_events"] is True
    assert features["events_backend"] == "poll_queue"


async def test_create_connection_consumes_no_pool_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    """A standalone connection must not be taken out of the pool."""
    sentinel = MagicMock()
    connect = AsyncMock(return_value=sentinel)
    monkeypatch.setattr("sqlspec.adapters.cockroach_asyncpg.config.asyncpg_connect", connect)
    monkeypatch.setattr(CockroachAsyncpgConfig, "_init_connection", AsyncMock())
    config = CockroachAsyncpgConfig(connection_config={"host": "localhost", "min_size": 1, "max_size": 1})

    connection = await config.create_connection()

    assert connection is sentinel
    assert config.connection_instance is None
    assert "min_size" not in connect.call_args.kwargs
    assert "max_size" not in connect.call_args.kwargs


def test_build_connection_config_resolves_aliases() -> None:
    """build_connection_config should map conninfo/url to dsn, dbname/db to database, and username to user."""
    cfg = build_connection_config({
        "url": "postgresql://usr:pwd@host.internal:26257/main",
        "dbname": "override_db",
        "username": "override_usr",
    })
    assert cfg["dsn"] == "postgresql://usr:pwd@host.internal:26257/main"
    assert cfg["database"] == "override_db"
    assert cfg["user"] == "override_usr"
    assert "url" not in cfg
    assert "dbname" not in cfg
    assert "username" not in cfg


def test_cockroach_asyncpg_config_normalizes_aliases() -> None:
    """CockroachAsyncpgConfig should normalize aliases in connection_config."""
    config = CockroachAsyncpgConfig(
        connection_config={
            "conninfo": "postgresql://localhost:26257/test",
            "dbname": "target_db",
            "username": "target_user",
        }
    )
    assert config.connection_config["dsn"] == "postgresql://localhost:26257/test"
    assert config.connection_config["database"] == "target_db"
    assert config.connection_config["user"] == "target_user"
    assert "conninfo" not in config.connection_config
    assert "dbname" not in config.connection_config
    assert "username" not in config.connection_config


def test_cockroach_asyncpg_build_connection_config_cluster_and_extra() -> None:
    """build_connection_config should route cluster to server_settings.options and flatten extra."""
    cfg = build_connection_config({
        "host": "localhost",
        "cluster": "brave-hippo-42",
        "server_settings": {"application_name": "sqlspec"},
        "extra": {"statement_cache_size": 0},
    })
    assert cfg["server_settings"] == {"application_name": "sqlspec", "options": "--cluster=brave-hippo-42"}
    assert cfg["statement_cache_size"] == 0
    assert "cluster" not in cfg
    assert "extra" not in cfg


def test_cockroach_asyncpg_preserves_on_connection_create_in_driver_features() -> None:
    """CockroachAsyncpgConfig should preserve on_connection_create in driver_features."""

    async def _hook(_conn: Any) -> None:
        return None

    config = CockroachAsyncpgConfig(driver_features={"on_connection_create": _hook})
    assert config.driver_features["on_connection_create"] is _hook


def test_cockroach_asyncpg_retry_config_rejects_negative_values() -> None:
    """CockroachAsyncpgRetryConfig.from_features should reject negative values."""
    with pytest.raises(ImproperConfigurationError, match="max_retries"):
        CockroachAsyncpgRetryConfig.from_features({"max_retries": -1})
    with pytest.raises(ImproperConfigurationError, match="base_delay_ms"):
        CockroachAsyncpgRetryConfig.from_features({"retry_delay_base_ms": -5.0})
    with pytest.raises(ImproperConfigurationError, match="max_delay_ms"):
        CockroachAsyncpgRetryConfig.from_features({"retry_delay_max_ms": -10.0})


async def test_cockroach_asyncpg_follower_read_auto_transaction_commits_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auto-started follower-read transactions must commit after read and rollback on error."""
    in_tx = False

    def _in_tx(_self: Any) -> bool:
        return in_tx

    async def _begin(_self: Any) -> None:
        nonlocal in_tx
        in_tx = True

    async def _commit(_self: Any) -> None:
        nonlocal in_tx
        in_tx = False

    async def _rollback(_self: Any) -> None:
        nonlocal in_tx
        in_tx = False

    monkeypatch.setattr(CockroachAsyncpgDriver, "_connection_in_transaction", _in_tx)
    monkeypatch.setattr(CockroachAsyncpgDriver, "begin", _begin)
    monkeypatch.setattr(CockroachAsyncpgDriver, "commit", _commit)
    monkeypatch.setattr(CockroachAsyncpgDriver, "rollback", _rollback)

    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=[])
    driver = CockroachAsyncpgDriver(
        connection=conn, driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
    )
    stmt = SQL("SELECT 1")
    await driver.dispatch_execute(conn, stmt)
    assert in_tx is False

    conn.fetch = AsyncMock(side_effect=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        await driver.dispatch_execute(conn, stmt)
    assert in_tx is False


async def test_cockroach_asyncpg_adk_memory_insert_12_and_13_placeholders() -> None:
    """CockroachAsyncpgADKMemoryStore.insert_memory_entries must bind 12 or 13 placeholders inside a transaction."""
    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _FakeTx:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *_args: Any) -> None:
            return None

    class _FakeConn:
        def transaction(self) -> _FakeTx:
            return _FakeTx()

        async def execute(self, sql: str, *args: Any) -> str:
            executed.append((sql, args))
            return "INSERT 0 1"

    class _FakeCtx:
        async def __aenter__(self) -> _FakeConn:
            return _FakeConn()

        async def __aexit__(self, *_args: Any) -> None:
            return None

    config = CockroachAsyncpgConfig(extension_config={"adk": {"enable_memory": True}})
    monkeypatch_conn = MagicMock(return_value=_FakeCtx())
    object.__setattr__(config, "provide_connection", monkeypatch_conn)
    store = CockroachAsyncpgADKMemoryStore(config)
    entry: Any = {
        "id": "m1",
        "session_id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "scope": "user",
        "event_id": "e1",
        "author": "user",
        "timestamp": "2026-01-01T00:00:00Z",
        "content_json": {},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": "2026-01-01T00:00:00Z",
    }
    count = await store.insert_memory_entries([entry])
    assert count == 1
    assert "$12" in executed[0][0]
    assert "$13" not in executed[0][0]
    assert len(executed[0][1]) == 12

    executed.clear()
    config_with_owner = CockroachAsyncpgConfig(
        extension_config={"adk": {"enable_memory": True, "owner_id_column": "tenant_id UUID"}}
    )
    object.__setattr__(config_with_owner, "provide_connection", monkeypatch_conn)
    store_with_owner = CockroachAsyncpgADKMemoryStore(config_with_owner)
    count_owner = await store_with_owner.insert_memory_entries([entry], owner_id="t1")
    assert count_owner == 1
    assert "$13" in executed[0][0]
    assert len(executed[0][1]) == 13


async def test_cockroach_asyncpg_adk_locality_validation_and_schema_qualified_ddl() -> None:
    """ADK and Litestar stores must validate LOCALITY clauses and sanitize schema-qualified index names."""
    bad_config = CockroachAsyncpgConfig(extension_config={"adk": {"table_locality": "GLOBAL; DROP TABLE users"}})
    bad_store = CockroachAsyncpgADKStore(bad_config)
    with pytest.raises(ImproperConfigurationError, match="LOCALITY"):
        await bad_store._sessions_table_ddl()

    good_config = CockroachAsyncpgConfig(extension_config={"adk": {"table_locality": "REGIONAL BY ROW AS region"}})
    adk_store = CockroachAsyncpgADKStore(good_config)
    adk_store._session_table = "tenant_a.adk_sessions"
    ddl = await adk_store._sessions_table_ddl()
    assert "idx_tenant_a_adk_sessions_app_user" in ddl
    assert "LOCALITY REGIONAL BY ROW AS region" in ddl

    litestar_store = CockroachAsyncpgStore(good_config)
    litestar_store._table_name = "tenant_a.litestar_sessions"
    assert "idx_tenant_a_litestar_sessions_expires_at" in litestar_store._table_ddl()


async def test_cockroach_asyncpg_follower_read_only_for_readonly_statements() -> None:
    """CockroachAsyncpgDriver should not begin follower-read transactions for modifying operations."""
    in_tx = False

    async def _in_tx() -> bool:
        return in_tx

    async def _begin(self: Any) -> None:
        nonlocal in_tx
        in_tx = True

    with (
        patch.object(CockroachAsyncpgDriver, "_connection_in_transaction", _in_tx),
        patch.object(CockroachAsyncpgDriver, "begin", _begin),
    ):
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        driver = CockroachAsyncpgDriver(
            connection=conn, driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
        )
        modifying_stmt = SQL("INSERT INTO users (name) VALUES ('alice') RETURNING id")
        await driver.dispatch_execute(conn, modifying_stmt)
        assert in_tx is False


async def test_cockroach_asyncpg_begin_follower_read_failure_handling() -> None:
    """Failing SET TRANSACTION in begin() must rollback and raise mapped exception."""
    conn = MagicMock()
    conn.is_in_transaction.return_value = False
    tx = AsyncMock()
    conn.transaction.return_value = tx
    conn.execute = AsyncMock(side_effect=RuntimeError("syntax error in staleness"))

    driver = CockroachAsyncpgDriver(
        connection=conn, driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
    )
    with pytest.raises(Exception):
        await driver.begin()
    assert driver._transaction is None
    tx.rollback.assert_awaited_once()


async def test_cockroach_asyncpg_select_stream_follower_reads() -> None:
    """select_stream on a SELECT query with follower reads emits the staleness clause and closes."""
    executed_statements: list[str] = []
    conn = MagicMock()
    conn.is_in_transaction.return_value = False
    tx = AsyncMock()
    conn.transaction.return_value = tx

    async def fake_execute(query: str, *args: Any, **kwargs: Any) -> str:
        executed_statements.append(query)
        return "OK"

    conn.execute = AsyncMock(side_effect=fake_execute)

    cursor_mock = AsyncMock()
    cursor_mock.fetch = AsyncMock(side_effect=[[{"id": 1}], []])
    conn.cursor = AsyncMock(return_value=cursor_mock)

    driver = CockroachAsyncpgDriver(
        connection=conn, driver_features={"enable_follower_reads": True, "default_staleness": "'-10s'"}
    )
    stream = driver.dispatch_select_stream(SQL("SELECT id FROM users"), chunk_size=10)
    assert stream is not None
    rows = [row async for row in stream]
    assert rows == [{"id": 1}]
    assert any("AS OF SYSTEM TIME '-10s'" in s for s in executed_statements)
    tx.commit.assert_awaited()
