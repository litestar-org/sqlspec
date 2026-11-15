"""Schema extraction refusals for row-security policies and warnings for dropped objects."""

import logging
import re
from pathlib import Path
from typing import Any, Literal, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.core import SQLResult
from sqlspec.data_dictionary import MetadataResult
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import MigrationError, SQLSpecError, SquashValidationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.runner import SyncMigrationRunner
from sqlspec.migrations.squash import MigrationSquasher

PG_OMISSIONS = [
    {"object_kind": "grant", "schema_name": "public", "object_name": "orders"},
    {"object_kind": "function", "schema_name": "public", "object_name": "touch_updated"},
    {"object_kind": "trigger", "schema_name": "public", "object_name": "orders.audit"},
    {"object_kind": "domain", "schema_name": "public", "object_name": "email"},
]


class _SyncSession:
    def __init__(self, driver: Any) -> None:
        self._driver = driver

    def __enter__(self) -> Any:
        return self._driver

    def __exit__(self, *_args: Any) -> Literal[False]:
        return False


class _AsyncSession:
    def __init__(self, driver: Any) -> None:
        self._driver = driver

    async def __aenter__(self) -> Any:
        return self._driver

    async def __aexit__(self, *_args: Any) -> Literal[False]:
        return False


class _StubConfig:
    supports_transactional_ddl = True
    supports_migration_schemas = True
    driver_features: dict[str, Any] = {}
    extension_config: dict[str, Any] = {}
    bind_key = None
    is_replica = False
    migration_tracker_type = MagicMock()

    def __init__(self, driver: Any, migrations_dir: Path, *, is_async: bool) -> None:
        self.migration_config = {"script_location": str(migrations_dir), "include_extensions": []}
        self._driver = driver
        self._is_async = is_async

    def get_observability_runtime(self) -> None:
        return None

    def provide_session(self) -> Any:
        return _AsyncSession(self._driver) if self._is_async else _SyncSession(self._driver)


def _stub_driver(dialect: str, rows_by_marker: dict[str, list[dict[str, Any]]], *, is_async: bool) -> Any:
    driver = MagicMock()
    driver.dialect = dialect

    def _rows(stmt: Any) -> SQLResult:
        text = str(stmt)
        for marker, rows in rows_by_marker.items():
            if marker in text:
                return SQLResult(data=rows)
        return SQLResult()

    if is_async:
        driver.data_dictionary.get_schema_ddl = AsyncMock(return_value=MetadataResult("ddl"))

        async def _execute_async(stmt: Any, _params: Any = None) -> SQLResult:
            return _rows(stmt)

        driver.execute = AsyncMock(side_effect=_execute_async)
    else:
        driver.data_dictionary.get_schema_ddl = MagicMock(return_value=MetadataResult("ddl"))
        driver.execute = MagicMock(side_effect=lambda stmt, _params=None: _rows(stmt))
    return driver


def _commands(tmp_path: Path, driver: Any, *, is_async: bool) -> Any:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir(exist_ok=True)
    config = cast("Any", _StubConfig(driver, migrations_dir, is_async=is_async))
    commands: Any = AsyncMigrationCommands(config) if is_async else SyncMigrationCommands(config)
    commands.tracker.version_table_name = "ddl_migrations"
    commands.tracker.version_table_schema = None
    return commands


async def _dump(commands: Any, *, is_async: bool) -> str:
    if is_async:
        return cast("str", await commands.dump_schema(echo=False))
    return cast("str", commands.dump_schema(echo=False))


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("dialect", "marker", "rows", "expected"),
    [
        (
            "oracle",
            "all_policies",
            [{"schema_name": "APP", "table_name": "ORDERS", "policy_name": "TENANT_VPD"}],
            "APP.ORDERS",
        ),
        (
            "mssql",
            "sys.security_policies",
            [{"schema_name": "dbo", "table_name": "orders", "policy_name": "tenant_filter"}],
            "dbo.orders",
        ),
    ],
    ids=["oracle-vpd", "mssql-security-policy"],
)
async def test_schema_extraction_refuses_row_security_policies(
    tmp_path: Path, is_async: bool, dialect: str, marker: str, rows: list[dict[str, Any]], expected: str
) -> None:
    driver = _stub_driver(dialect, {marker: rows}, is_async=is_async)
    commands = _commands(tmp_path, driver, is_async=is_async)

    with pytest.raises(MigrationError, match=re.escape(expected)):
        await _dump(commands, is_async=is_async)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_schema_extraction_warns_on_pg_omissions(
    tmp_path: Path, is_async: bool, caplog: pytest.LogCaptureFixture
) -> None:
    driver = _stub_driver("postgres", {"UNION ALL": PG_OMISSIONS}, is_async=is_async)
    commands = _commands(tmp_path, driver, is_async=is_async)

    with caplog.at_level(logging.WARNING):
        output = await _dump(commands, is_async=is_async)

    records = [record for record in caplog.records if "migration.schema_extraction.omitted" in record.getMessage()]
    assert "-- SQLSpec Schema Dump" in output
    assert len(records) == 1
    fields = records[0].__dict__["extra_fields"]
    assert fields["object_kinds"] == ["grant", "function", "trigger", "domain"]
    assert fields["names"] == [
        "grant:public.orders",
        "function:public.touch_updated",
        "trigger:public.orders.audit",
        "domain:public.email",
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_schema_extraction_excludes_tracking_tables(tmp_path: Path, is_async: bool) -> None:
    rows = [
        {"schema_name": "public", "table_name": "ddl_migrations"},
        {"schema_name": "public", "table_name": "ddl_migrations_lock"},
    ]
    driver = _stub_driver("postgres", {"pg_policy": rows}, is_async=is_async)
    commands = _commands(tmp_path, driver, is_async=is_async)

    output = await _dump(commands, is_async=is_async)

    assert "-- SQLSpec Schema Dump" in output


def test_schema_extraction_reports_unreadable_catalog(tmp_path: Path) -> None:

    driver = _stub_driver("oracle", {}, is_async=False)
    driver.execute = MagicMock(side_effect=SQLSpecError("ORA-00942: table or view does not exist"))
    commands = _commands(tmp_path, driver, is_async=False)

    with pytest.raises(MigrationError, match="cannot prove"):
        commands.dump_schema(echo=False)


def test_schema_squash_refuses_vpd(tmp_path: Path) -> None:
    rows = [{"schema_name": "APP", "table_name": "ORDERS", "policy_name": "TENANT_VPD"}]
    driver = MagicMock(spec=SyncDriverAdapterBase)
    driver.dialect = "oracle"
    driver.execute = MagicMock(return_value=SQLResult(data=rows))
    runner = SyncMigrationRunner(tmp_path, context=MigrationContext(dialect="oracle", driver=driver))
    squasher = MigrationSquasher(tmp_path, runner, strategy="schema")

    with pytest.raises(SquashValidationError, match=r"APP\.ORDERS"):
        squasher._extract_schema_ddl_from_context()  # pyright: ignore[reportPrivateUsage]
