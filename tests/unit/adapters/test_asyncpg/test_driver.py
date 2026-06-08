"""AsyncPG driver regression tests."""

# pyright: reportArgumentType=false, reportOptionalMemberAccess=false

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.asyncpg.core import default_statement_config
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.core import SQL
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg._typing import AsyncpgConnection


def _connection() -> "AsyncpgConnection":
    return cast("AsyncpgConnection", object())


class _CopyCursor:
    def __init__(self) -> None:
        self.copy_to_table = AsyncMock()
        self.copy_from_query = AsyncMock()
        self.execute = AsyncMock()


class _CompiledCopyDriver(AsyncpgDriver):
    def _get_compiled_sql(self, *_args: object, **_kwargs: object) -> tuple[str, object]:
        return "COPY (SELECT 1) FROM STDIN", None


@pytest.mark.anyio
async def test_asyncpg_copy_from_stdin_uses_copy_to_table() -> None:
    config = default_statement_config.replace(execution_args={"postgres_copy_data": "1\tAlice"})
    statement = SQL("COPY users FROM STDIN", statement_config=config)
    cursor = _CopyCursor()
    driver = AsyncpgDriver(connection=_connection())

    await driver._handle_copy_operation(cast("Any", cursor), statement)  # pyright: ignore[reportPrivateUsage]

    cursor.copy_to_table.assert_awaited_once()
    await_args = cursor.copy_to_table.await_args
    assert await_args is not None
    assert await_args.args == ("users",)
    assert await_args.kwargs["source"].read() == b"1\tAlice"
    cursor.copy_from_query.assert_not_awaited()
    cursor.execute.assert_not_awaited()


@pytest.mark.anyio
async def test_asyncpg_copy_from_stdin_splits_schema_table() -> None:
    config = default_statement_config.replace(execution_args={"postgres_copy_data": "1"})
    statement = SQL("COPY public.users FROM STDIN", statement_config=config)
    cursor = _CopyCursor()
    driver = AsyncpgDriver(connection=_connection())

    await driver._handle_copy_operation(cast("Any", cursor), statement)  # pyright: ignore[reportPrivateUsage]

    await_args = cursor.copy_to_table.await_args
    assert await_args is not None
    assert await_args.args == ("users",)
    assert await_args.kwargs["schema_name"] == "public"


@pytest.mark.anyio
async def test_asyncpg_copy_from_stdin_forwards_copy_options() -> None:
    config = default_statement_config.replace(
        execution_args={
            "postgres_copy_data": "1,Alice",
            "postgres_copy_columns": ("id", "name"),
            "postgres_copy_format": "csv",
            "postgres_copy_delimiter": ",",
        }
    )
    statement = SQL("COPY users (id, name) FROM STDIN", statement_config=config)
    cursor = _CopyCursor()
    driver = AsyncpgDriver(connection=_connection())

    await driver._handle_copy_operation(cast("Any", cursor), statement)  # pyright: ignore[reportPrivateUsage]

    await_args = cursor.copy_to_table.await_args
    assert await_args is not None
    assert await_args.kwargs["columns"] == ("id", "name")
    assert await_args.kwargs["format"] == "csv"
    assert await_args.kwargs["delimiter"] == ","


@pytest.mark.anyio
async def test_asyncpg_copy_from_stdin_uses_metadata_table_fallback() -> None:
    config = default_statement_config.replace(
        execution_args={"postgres_copy_data": "1", "postgres_copy_table": "public.users"}
    )
    statement = SimpleNamespace(operation_type="COPY_FROM", statement_config=config)
    cursor = _CopyCursor()
    driver = _CompiledCopyDriver(connection=_connection())

    await driver._handle_copy_operation(cast("Any", cursor), cast("SQL", statement))  # pyright: ignore[reportPrivateUsage]

    await_args = cursor.copy_to_table.await_args
    assert await_args is not None
    assert await_args.args == ("users",)
    assert await_args.kwargs["schema_name"] == "public"


@pytest.mark.anyio
async def test_handle_copy_operation_uses_processed_state_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    config = default_statement_config.replace(execution_args={"postgres_copy_data": "1"})
    statement = SQL("COPY users FROM STDIN", statement_config=config)
    statement.compile()
    cursor = _CopyCursor()
    driver = AsyncpgDriver(connection=_connection())

    monkeypatch.setattr(
        AsyncpgDriver,
        "_get_compiled_sql",
        lambda *_args, **_kwargs: pytest.fail("processed COPY statements should not recompile"),
    )

    await driver._handle_copy_operation(cast("Any", cursor), statement)  # pyright: ignore[reportPrivateUsage]

    cursor.copy_to_table.assert_awaited_once()


@pytest.mark.anyio
async def test_handle_copy_operation_falls_back_to_get_compiled_sql_when_not_processed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = default_statement_config.replace(
        execution_args={"postgres_copy_data": "1", "postgres_copy_table": "users"}
    )
    statement = SimpleNamespace(operation_type="COPY_FROM", statement_config=config, is_processed=False)
    cursor = _CopyCursor()
    driver = AsyncpgDriver(connection=_connection())
    calls = 0

    def get_compiled_sql(*_args: object, **_kwargs: object) -> tuple[str, object]:
        nonlocal calls
        calls += 1
        return "COPY users FROM STDIN", None

    monkeypatch.setattr(AsyncpgDriver, "_get_compiled_sql", get_compiled_sql)

    await driver._handle_copy_operation(cast("Any", cursor), cast("SQL", statement))  # pyright: ignore[reportPrivateUsage]

    assert calls == 1
    cursor.copy_to_table.assert_awaited_once()


@pytest.mark.anyio
async def test_asyncpg_copy_from_stdin_requires_table_name() -> None:
    config = default_statement_config.replace(execution_args={"postgres_copy_data": "1"})
    statement = SimpleNamespace(operation_type="COPY_FROM", statement_config=config)
    cursor = _CopyCursor()
    driver = _CompiledCopyDriver(connection=_connection())

    with pytest.raises(SQLSpecError, match="postgres_copy_table"):
        await driver._handle_copy_operation(cast("Any", cursor), cast("SQL", statement))  # pyright: ignore[reportPrivateUsage]

    cursor.copy_to_table.assert_not_awaited()
