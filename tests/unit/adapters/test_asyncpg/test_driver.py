"""AsyncPG driver regression tests."""

# pyright: reportArgumentType=false, reportOptionalMemberAccess=false

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
    statement = SQL("COPY users FROM STDIN", statement_config=config)
    cursor = _CopyCursor()
    driver = AsyncpgDriver(connection=_connection())

    await driver._handle_copy_operation(cast("Any", cursor), statement)  # pyright: ignore[reportPrivateUsage]

    await_args = cursor.copy_to_table.await_args
    assert await_args is not None
    assert await_args.args == ("users",)
    assert await_args.kwargs["schema_name"] == "public"


@pytest.mark.anyio
async def test_handle_copy_operation_uses_processed_state_when_available() -> None:
    config = default_statement_config.replace(execution_args={"postgres_copy_data": "1"})
    statement = SQL("COPY users FROM STDIN", statement_config=config)
    statement.compile()
    cursor = _CopyCursor()
    driver = AsyncpgDriver(connection=_connection())

    await driver._handle_copy_operation(cast("Any", cursor), statement)  # pyright: ignore[reportPrivateUsage]

    cursor.copy_to_table.assert_awaited_once()


@pytest.mark.anyio
async def test_asyncpg_copy_from_stdin_requires_table_name() -> None:
    config = default_statement_config.replace(execution_args={"postgres_copy_data": "1"})
    statement = SQL("COPY (SELECT 1) FROM STDIN", statement_config=config)
    cursor = _CopyCursor()
    driver = AsyncpgDriver(connection=_connection())

    with pytest.raises(SQLSpecError, match="postgres_copy_table"):
        await driver._handle_copy_operation(cast("Any", cursor), statement)  # pyright: ignore[reportPrivateUsage]

    cursor.copy_to_table.assert_not_awaited()
