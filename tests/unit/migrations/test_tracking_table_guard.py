"""PostgreSQL tracking-table guard: schema pinning, ownership, and the recheck under the advisory lock."""

from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.core import SQLResult
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker

pytestmark = pytest.mark.anyio

OWNED_ROW = {"relkind": "r", "has_user_triggers": False, "owned_by_current_role": True}


class _Harness:
    """Records statements issued against a canned PostgreSQL driver."""

    def __init__(
        self,
        *,
        is_async: bool,
        current_schema: str | None = "public",
        relation_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.current_schema = current_schema
        self.relation_rows = relation_rows if relation_rows is not None else []
        self.driver = MagicMock()
        self.driver.dialect = "postgres"
        type(self.driver).dialect = "postgres"
        self.driver.driver_features = {"autocommit": True}
        self.driver._connection_in_transaction = MagicMock(return_value=False)
        tracker = AsyncMigrationTracker() if is_async else SyncMigrationTracker()
        columns = [{"column_name": col.name} for col in tracker._tracking_table_ddl().columns]  # pyright: ignore[reportPrivateUsage]
        if is_async:
            self.driver.data_dictionary.get_columns = AsyncMock(return_value=columns)
            self.driver.execute = AsyncMock(side_effect=self._execute)
            self.driver.commit = AsyncMock()
            self.driver.rollback = AsyncMock()
        else:
            self.driver.data_dictionary.get_columns.return_value = columns
            self.driver.execute = MagicMock(side_effect=self._record)
            self.driver.commit = MagicMock()
            self.driver.rollback = MagicMock()

    def _record(self, stmt: Any, params: Any = None) -> SQLResult:
        text = str(stmt)
        self.executed.append((text, params))
        if "pg_try_advisory_lock" in text:
            return SQLResult(data=[{"acquired": True}])
        if "current_schema()" in text:
            return SQLResult(data=[{"schema_name": self.current_schema}])
        if "pg_catalog.pg_class" in text:
            return SQLResult(data=self.relation_rows)
        return SQLResult()

    async def _execute(self, stmt: Any, params: Any = None) -> SQLResult:
        return self._record(stmt, params)

    def guard_calls(self) -> list[tuple[str, Any]]:
        return [call for call in self.executed if "pg_catalog.pg_class" in call[0]]


async def _ensure(tracker: Any, harness: _Harness, is_async: bool) -> None:
    if is_async:
        await tracker.ensure_tracking_table(harness.driver)
    else:
        tracker.ensure_tracking_table(harness.driver)


def _tracker(is_async: bool, **kwargs: Any) -> Any:
    return AsyncMigrationTracker(**kwargs) if is_async else SyncMigrationTracker(**kwargs)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_guard_rejects_table_owned_by_foreign_role(is_async: bool) -> None:
    harness = _Harness(
        is_async=is_async, relation_rows=[{"relkind": "r", "has_user_triggers": False, "owned_by_current_role": False}]
    )

    with pytest.raises(MigrationError, match=r"public\.ddl_migrations.*owned by the current role"):
        await _ensure(_tracker(is_async), harness, is_async)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_guard_accepts_owned_table(is_async: bool) -> None:
    harness = _Harness(is_async=is_async, relation_rows=[OWNED_ROW])

    await _ensure(_tracker(is_async), harness, is_async)

    assert len(harness.guard_calls()) == 1


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_guard_queries_by_schema_and_name(is_async: bool) -> None:
    harness = _Harness(is_async=is_async, relation_rows=[OWNED_ROW])

    await _ensure(_tracker(is_async), harness, is_async)

    sql_text, params = harness.guard_calls()[0]
    assert params == {"schema_name": "public", "table_name": "ddl_migrations"}
    assert "to_regclass" not in sql_text


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_unconfigured_schema_pins_to_current_schema(is_async: bool) -> None:
    harness = _Harness(is_async=is_async, current_schema="app", relation_rows=[OWNED_ROW])
    tracker = _tracker(is_async)

    await _ensure(tracker, harness, is_async)

    assert tracker.version_table_schema == "app"
    assert tracker.version_table == "app.ddl_migrations"
    assert "app" in str(tracker._tracking_table_ddl().build().sql)  # pyright: ignore[reportPrivateUsage]
    assert harness.guard_calls()[0][1] == {"schema_name": "app", "table_name": "ddl_migrations"}


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_mixed_case_current_schema_is_quoted(is_async: bool) -> None:
    harness = _Harness(is_async=is_async, current_schema="AppSchema", relation_rows=[OWNED_ROW])
    tracker = _tracker(is_async)

    await _ensure(tracker, harness, is_async)

    assert tracker.version_table == '"AppSchema".ddl_migrations'


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_configured_schema_skips_current_schema_query(is_async: bool) -> None:
    harness = _Harness(is_async=is_async, relation_rows=[OWNED_ROW])

    await _ensure(_tracker(is_async, version_table_schema="history"), harness, is_async)

    assert not any("current_schema()" in text for text, _ in harness.executed)
    assert harness.guard_calls()[0][1] == {"schema_name": "history", "table_name": "ddl_migrations"}


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_null_current_schema_raises(is_async: bool) -> None:
    harness = _Harness(is_async=is_async, current_schema=None)

    with pytest.raises(MigrationError, match="current_schema"):
        await _ensure(_tracker(is_async), harness, is_async)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_lock_rechecks_guard_after_acquire(is_async: bool) -> None:
    harness = _Harness(
        is_async=is_async, relation_rows=[{"relkind": "r", "has_user_triggers": True, "owned_by_current_role": True}]
    )
    tracker = _tracker(is_async)

    with pytest.raises(MigrationError, match="triggers"):
        if is_async:
            async with tracker.acquire_lock(harness.driver):
                pass
        else:
            with tracker.acquire_lock(harness.driver):
                pass

    statements = [text for text, _ in harness.executed]
    assert any("pg_try_advisory_lock" in text for text in statements)
    assert any("pg_advisory_unlock" in text for text in statements)
    assert statements.index(harness.guard_calls()[0][0]) < max(
        i for i, text in enumerate(statements) if "pg_advisory_unlock" in text
    )


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_positional_catalog_rows_are_paired_with_column_names(is_async: bool) -> None:
    harness = _Harness(is_async=is_async, relation_rows=[])
    original = harness._record

    def _positional(stmt: Any, params: Any = None) -> SQLResult:
        text = str(stmt)
        if "pg_catalog.pg_class" in text:
            harness.executed.append((text, params))
            return SQLResult(
                data=[("r", False, False)], column_names=["relkind", "has_user_triggers", "owned_by_current_role"]
            )
        return original(stmt, params)

    harness._record = _positional
    if is_async:
        harness.driver.execute = AsyncMock(side_effect=harness._execute)
    else:
        harness.driver.execute = MagicMock(side_effect=_positional)

    with pytest.raises(MigrationError, match="owned by the current role"):
        await _ensure(_tracker(is_async), harness, is_async)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        (
            {"version_table_name": "DDL_Migrations", "version_table_schema": "App"},
            {"schema_name": "app", "table_name": "ddl_migrations"},
        ),
        (
            {"version_table_name": '"DDL_Migrations"', "version_table_schema": '"App"'},
            {"schema_name": "App", "table_name": "DDL_Migrations"},
        ),
    ],
    ids=["unquoted-folded", "quoted-preserved"],
)
async def test_guard_folds_unquoted_names_like_postgres(
    is_async: bool, kwargs: dict[str, Any], expected: dict[str, str]
) -> None:
    harness = _Harness(is_async=is_async, relation_rows=[OWNED_ROW])

    await _ensure(_tracker(is_async, **kwargs), harness, is_async)

    assert harness.guard_calls()[0][1] == expected


async def _locked_key(tracker: Any, harness: _Harness, is_async: bool) -> int:
    harness.executed.clear()
    if is_async:
        async with tracker.acquire_lock(harness.driver):
            pass
    else:
        with tracker.acquire_lock(harness.driver):
            pass
    lock_calls = [params for text, params in harness.executed if "pg_try_advisory_lock" in text]
    return cast("int", lock_calls[0]["lock_id"])


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_advisory_lock_key_is_stable_across_locked_commands(is_async: bool) -> None:
    harness = _Harness(is_async=is_async, relation_rows=[OWNED_ROW])
    fresh_key = await _locked_key(_tracker(is_async), _Harness(is_async=is_async, relation_rows=[OWNED_ROW]), is_async)
    tracker = _tracker(is_async)

    first_key = await _locked_key(tracker, harness, is_async)
    second_key = await _locked_key(tracker, harness, is_async)

    assert first_key == fresh_key
    assert second_key == fresh_key


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_schema_pin_is_re_resolved_per_locked_command(is_async: bool) -> None:
    harness = _Harness(is_async=is_async, current_schema="first", relation_rows=[OWNED_ROW])
    tracker = _tracker(is_async)

    await _locked_key(tracker, harness, is_async)
    assert tracker.version_table == "first.ddl_migrations"

    harness.current_schema = "second"
    await _locked_key(tracker, harness, is_async)

    assert tracker.version_table == "second.ddl_migrations"
    assert harness.guard_calls()[-1][1] == {"schema_name": "second", "table_name": "ddl_migrations"}


@pytest.mark.parametrize(
    ("extra_row", "pattern"),
    [({"has_rules": True}, "rewrite rules"), ({"has_row_security": True}, "row-level security")],
    ids=["rules", "row-level-security"],
)
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_guard_rejects_rules_and_row_security(is_async: bool, extra_row: dict[str, Any], pattern: str) -> None:
    """Tracking table guard rejects pre-existing PostgreSQL relations with rewrite rules or RLS."""
    row = {**OWNED_ROW, **extra_row}
    harness = _Harness(is_async=is_async, relation_rows=[row])

    with pytest.raises(MigrationError, match=pattern):
        await _ensure(_tracker(is_async), harness, is_async)


async def test_pg_relation_guard_and_omissions_sql_contract() -> None:
    """PostgreSQL relation guard avoids inverted superuser logic and omission check covers matviews and attacl."""
    from sqlspec.migrations.base import _PG_RELATION_GUARD_SQL, _PG_SCHEMA_OMISSIONS_SQL

    assert "WHERE r.oid = c.relowner AND r.rolsuper" in _PG_RELATION_GUARD_SQL
    assert "pg_catalog.pg_has_role(current_user, c.relowner, 'MEMBER')" in _PG_RELATION_GUARD_SQL
    assert "pg_catalog.pg_rewrite" in _PG_RELATION_GUARD_SQL
    assert "pg_catalog.row_security_active(c.oid)" in _PG_RELATION_GUARD_SQL
    assert "'materialized_view'" in _PG_SCHEMA_OMISSIONS_SQL
    assert "attacl IS NOT NULL" in _PG_SCHEMA_OMISSIONS_SQL
