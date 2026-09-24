"""Unit tests for the Db2 Litestar session stores.

Store behaviors run against ``Db2SyncStore`` and ``Db2AsyncStore`` through ``db2_mode``.
"""

import importlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
import sqlglot

import sqlspec.dialects.db2  # noqa: F401  # pyright: ignore[reportUnusedImport]
from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
from sqlspec.adapters.db2.core import TABLE_EXISTS_SQL, to_db_timestamp, utc_now
from sqlspec.adapters.db2.litestar import Db2AsyncStore, Db2SyncStore
from tests.unit.adapters.test_db2._fakes import DriverMode, FakeDb2Cursor, db2_description

pytestmark = pytest.mark.anyio


def _store(
    db2_mode: DriverMode, *cursors: FakeDb2Cursor, extension_config: "dict[str, Any] | None" = None
) -> "tuple[Any, Any]":
    config = db2_mode.session_config(cursors, extension_config=extension_config)
    store_class = Db2AsyncStore if db2_mode.is_async else Db2SyncStore
    return store_class(config), config


def _utc_naive_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _assert_naive_utc_near(value: Any, expected: datetime) -> None:
    assert isinstance(value, datetime)
    assert value.tzinfo is None
    assert abs((value - expected).total_seconds()) < 1


def _session_row(data: bytes, expires_at: "datetime | None") -> FakeDb2Cursor:
    return FakeDb2Cursor(rows=[(data, expires_at)], description=db2_description("data", "expires_at"))


async def test_get_compares_expiry_with_bound_utc_now(db2_mode: DriverMode) -> None:
    """Reads compare expiry with a bound naive-UTC time instead of the server clock."""
    store, config = _store(db2_mode, _session_row(b"payload", None))

    assert await store.get("session-1") == b"payload"

    [(sql, params)] = config.executed
    assert "CURRENT TIMESTAMP" not in sql.upper()
    assert params[0] == "session-1"
    _assert_naive_utc_near(params[1], _utc_naive_now())


async def test_get_renews_with_bound_utc_times(db2_mode: DriverMode) -> None:
    """Renewing a session binds naive-UTC expiry and update times."""
    stored_expiry = _utc_naive_now() + timedelta(minutes=5)
    store, config = _store(db2_mode, _session_row(b"payload", stored_expiry))

    assert await store.get("session-1", renew_for=120) == b"payload"

    (_, _), (renew_sql, renew_params) = config.executed
    assert renew_sql.lstrip().upper().startswith("UPDATE")
    assert "CURRENT TIMESTAMP" not in renew_sql.upper()
    _assert_naive_utc_near(renew_params[0], _utc_naive_now() + timedelta(seconds=120))
    _assert_naive_utc_near(renew_params[1], _utc_naive_now())
    assert renew_params[2] == "session-1"
    assert config.connection.commits == 1


async def test_get_returns_none_for_missing_session(db2_mode: DriverMode) -> None:
    """A missing or expired session reads as None without a renewal write."""
    store, config = _store(db2_mode)

    assert await store.get("missing", renew_for=60) is None
    assert len(config.executed) == 1


async def test_set_binds_naive_utc_expiry_and_now(db2_mode: DriverMode) -> None:
    """Upserts bind the aware expiry as naive UTC and the write time as naive UTC."""
    store, config = _store(db2_mode)

    await store.set("session-1", "value", expires_in=timedelta(seconds=60))

    [(sql, params)] = config.executed
    assert sql.lstrip().upper().startswith("MERGE INTO")
    assert "CURRENT TIMESTAMP" not in sql.upper()
    assert params[0] == "session-1"
    assert params[1] == b"value"
    _assert_naive_utc_near(params[2], _utc_naive_now() + timedelta(seconds=60))
    _assert_naive_utc_near(params[3], _utc_naive_now())
    assert config.connection.commits == 1


async def test_set_without_expiry_binds_null_expiry(db2_mode: DriverMode) -> None:
    """Sessions without an expiry bind NULL for expires_at."""
    store, config = _store(db2_mode)

    await store.set("session-1", b"value")

    [(_, params)] = config.executed
    assert params[2] is None


async def test_create_table_runs_ddl_once_when_probe_misses(db2_mode: DriverMode) -> None:
    """A catalog miss creates the table and its expiry index, probing each with bound names."""
    store, config = _store(db2_mode)

    await db2_mode.call(store._create_table)

    statements = config.executed
    assert [sql.split()[0].upper() for sql, _ in statements] == ["SELECT", "CREATE", "SELECT", "CREATE"]
    assert statements[0] == (TABLE_EXISTS_SQL, (None, "LITESTAR_SESSION"))
    assert statements[1][0].lstrip().upper().startswith("CREATE TABLE LITESTAR_SESSION")
    assert statements[2][1] == (None, "IX_LITESTAR_SESSION_EXP")
    assert statements[2][0].strip().upper().startswith("SELECT 1 FROM SYSCAT.INDEXES")
    assert statements[3][0] == "CREATE INDEX IX_LITESTAR_SESSION_EXP ON litestar_session(expires_at)"


@pytest.mark.parametrize(
    ("settings", "created", "reconcile_kwargs"),
    [({}, True, {"assume_existing": True}), ({"create_schema": False}, False, {})],
    ids=["create", "reconcile-only"],
)
async def test_create_table_creates_then_reconciles(
    db2_mode: DriverMode,
    monkeypatch: pytest.MonkeyPatch,
    settings: "dict[str, Any]",
    created: bool,
    reconcile_kwargs: "dict[str, Any]",
) -> None:
    """``create_table`` runs the catalog-probed DDL only when creation is enabled, then reconciles."""
    store, config = _store(db2_mode, extension_config={"litestar": settings})
    reconciled: list[dict[str, Any]] = []

    async def record_reconcile(self: Any, **kwargs: Any) -> None:
        reconciled.append(kwargs)

    monkeypatch.setattr(type(store), "reconcile_schema", record_reconcile)

    await store.create_table()

    assert [sql.split()[0].upper() for sql, _ in config.executed] == (
        ["SELECT", "CREATE", "SELECT", "CREATE"] if created else []
    )
    assert reconciled == [reconcile_kwargs]


async def test_create_table_skips_when_table_exists(db2_mode: DriverMode) -> None:
    """An existing table and index are left untouched."""
    present = FakeDb2Cursor(rows=[(1,)], description=db2_description("1"))
    store, config = _store(db2_mode, present, FakeDb2Cursor(rows=[(1,)], description=db2_description("1")))

    await db2_mode.call(store._create_table)

    assert [sql.split()[0].upper() for sql, _ in config.executed] == ["SELECT", "SELECT"]


async def test_create_table_probes_mixed_case_names_as_folded(db2_mode: DriverMode) -> None:
    """Unquoted mixed-case table names are probed by their upper-folded catalog name."""
    store, config = _store(
        db2_mode,
        FakeDb2Cursor(rows=[(1,)], description=db2_description("1")),
        extension_config={"litestar": {"session_table": "AppSessions"}},
    )

    await db2_mode.call(store._create_table)

    assert config.executed[0][1] == (None, "APPSESSIONS")
    assert config.executed[1][1] == (None, "IX_APPSESSIONS_EXP")


async def test_delete_expired_binds_utc_cutoff(db2_mode: DriverMode) -> None:
    """Expired-session cleanup compares with a bound naive-UTC cutoff and returns the row count."""
    store, config = _store(db2_mode, FakeDb2Cursor(rowcount=3))

    assert await store.delete_expired() == 3

    [(sql, params)] = config.executed
    assert "CURRENT TIMESTAMP" not in sql.upper()
    assert "EXPIRES_AT <= ?" in sql.upper()
    _assert_naive_utc_near(params[0], _utc_naive_now())


async def test_exists_binds_utc_now(db2_mode: DriverMode) -> None:
    """Existence checks compare expiry with a bound naive-UTC time."""
    store, config = _store(db2_mode, FakeDb2Cursor(rows=[(1,)], description=db2_description("present")))

    assert await store.exists("session-1") is True

    [(sql, params)] = config.executed
    assert "CURRENT TIMESTAMP" not in sql.upper()
    assert params[0] == "session-1"
    _assert_naive_utc_near(params[1], _utc_naive_now())


async def test_expires_in_reads_stored_utc_expiry(db2_mode: DriverMode) -> None:
    """Remaining lifetime is computed from the stored naive-UTC expiry."""
    stored_expiry = _utc_naive_now() + timedelta(seconds=90)
    store, _ = _store(db2_mode, FakeDb2Cursor(rows=[(stored_expiry,)], description=db2_description("expires_at")))

    remaining = await store.expires_in("session-1")

    assert remaining is not None
    assert 88 <= remaining <= 90


async def test_expires_in_is_none_without_expiry(db2_mode: DriverMode) -> None:
    """Sessions without a stored expiry report no remaining lifetime."""
    store, _ = _store(db2_mode, FakeDb2Cursor(rows=[(None,)], description=db2_description("expires_at")))

    assert await store.expires_in("session-1") is None


async def test_delete_and_delete_all_commit(db2_mode: DriverMode) -> None:
    """Deletes bind the session key and commit their unit of work."""
    store, config = _store(db2_mode)

    await store.delete("session-1")
    await store.delete_all()

    (delete_sql, delete_params), (delete_all_sql, delete_all_params) = config.executed
    assert delete_sql == "DELETE FROM litestar_session WHERE session_id = ?"
    assert delete_params == ("session-1",)
    assert delete_all_sql == "DELETE FROM litestar_session"
    assert not delete_all_params
    assert config.connection.commits == 2


async def test_table_ddl_round_trips_through_db2_dialect(db2_mode: DriverMode) -> None:
    """The session DDL survives a parse and render through the Db2 dialect."""
    store, _ = _store(db2_mode)

    rendered = sqlglot.parse_one(store._table_ddl(), read="db2").sql(dialect="db2")

    assert "BLOB(10M)" in rendered
    assert "DEFAULT CURRENT TIMESTAMP" in rendered
    assert store._drop_table_sql() == ["DROP TABLE litestar_session"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (datetime(2026, 1, 1, 12, tzinfo=timezone(timedelta(hours=2))), datetime(2026, 1, 1, 10)),
        (datetime(2026, 1, 1, 12), datetime(2026, 1, 1, 12)),
        (None, None),
    ],
    ids=["aware", "naive", "none"],
)
def test_to_db_timestamp_binds_naive_utc(value: "datetime | None", expected: "datetime | None") -> None:
    """Aware values convert to naive UTC; naive values are taken as UTC."""
    assert to_db_timestamp(value) == expected


def test_utc_now_is_naive_utc() -> None:
    """The bind clock is naive and tracks UTC."""
    _assert_naive_utc_near(utc_now(), _utc_naive_now())


@pytest.mark.parametrize(
    ("config_class", "store_class"), [(Db2SyncConfig, Db2SyncStore), (Db2AsyncConfig, Db2AsyncStore)]
)
def test_store_resolves_by_config_name(config_class: Any, store_class: Any) -> None:
    """The Litestar session migration resolves the store matching each Db2 config."""
    migration = importlib.import_module("sqlspec.extensions.litestar.migrations.0001_create_session_table")
    context = SimpleNamespace(config=config_class(connection_config={"database": "d"}))

    assert migration._get_store_class(context) is store_class
