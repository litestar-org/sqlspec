# pyright: reportPrivateUsage=false
"""Tests for the fetch method compatibility aliases."""

import inspect
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from tests.conftest import requires_interpreted

_ALIAS_PAIRS = (
    ("fetch", "select"),
    ("fetch_one", "select_one"),
    ("fetch_one_or_none", "select_one_or_none"),
    ("fetch_value", "select_value"),
    ("fetch_value_or_none", "select_value_or_none"),
    ("fetch_to_arrow", "select_to_arrow"),
    ("fetch_stream", "select_stream"),
    ("fetch_with_total", "select_with_total"),
)

_DELEGATION_CASES = (
    (
        "fetch",
        "select",
        ("SELECT * FROM users", {"id": 1}),
        {"schema_type": None, "statement_config": None},
        {"schema_type": None, "statement_config": None},
        [{"id": 1}],
    ),
    (
        "fetch_one",
        "select_one",
        ("SELECT * FROM users WHERE id = ?", {"id": 1}),
        {"schema_type": None, "statement_config": None},
        {"schema_type": None, "statement_config": None},
        {"id": 1},
    ),
    (
        "fetch_one_or_none",
        "select_one_or_none",
        ("SELECT * FROM users WHERE id = ?", {"id": 999}),
        {"schema_type": None, "statement_config": None},
        {"schema_type": None, "statement_config": None},
        None,
    ),
    (
        "fetch_value",
        "select_value",
        ("SELECT COUNT(*) FROM users",),
        {"statement_config": None},
        {"value_type": None, "statement_config": None},
        42,
    ),
    (
        "fetch_value_or_none",
        "select_value_or_none",
        ("SELECT MAX(id) FROM empty_table",),
        {"statement_config": None},
        {"value_type": None, "statement_config": None},
        None,
    ),
    (
        "fetch_to_arrow",
        "select_to_arrow",
        ("SELECT * FROM users",),
        {
            "statement_config": None,
            "return_format": "table",
            "native_only": False,
            "batch_size": None,
            "arrow_schema": None,
        },
        {
            "statement_config": None,
            "return_format": "table",
            "native_only": False,
            "batch_size": None,
            "arrow_schema": None,
        },
        object(),
    ),
    (
        "fetch_stream",
        "select_stream",
        ("SELECT * FROM users",),
        {"schema_type": None, "statement_config": None, "chunk_size": 25, "native_only": False},
        {"schema_type": None, "statement_config": None, "chunk_size": 25, "native_only": False},
        object(),
    ),
    (
        "fetch_with_total",
        "select_with_total",
        ("SELECT * FROM users LIMIT 2",),
        {"schema_type": None, "statement_config": None},
        {"schema_type": None, "statement_config": None, "count_with_window": False},
        ([{"id": 1}, {"id": 2}], 100),
    ),
)


@pytest.mark.parametrize("base", (SyncDriverAdapterBase, AsyncDriverAdapterBase), ids=("sync", "async"))
@pytest.mark.parametrize(("alias_name", "target_name"), _ALIAS_PAIRS)
def test_fetch_alias_matches_select_signature(base: type[Any], alias_name: str, target_name: str) -> None:
    alias = getattr(base, alias_name)
    target = getattr(base, target_name)

    assert callable(alias)
    assert inspect.signature(alias).parameters == inspect.signature(target).parameters


@requires_interpreted
@pytest.mark.parametrize(
    ("alias_name", "target_name", "args", "call_kwargs", "expected_kwargs", "expected"), _DELEGATION_CASES
)
def test_sync_fetch_alias_delegates(
    alias_name: str,
    target_name: str,
    args: tuple[Any, ...],
    call_kwargs: dict[str, Any],
    expected_kwargs: dict[str, Any],
    expected: Any,
) -> None:
    driver = Mock(spec=SyncDriverAdapterBase)
    target = Mock(return_value=expected)
    setattr(driver, target_name, target)

    result = getattr(SyncDriverAdapterBase, alias_name)(driver, *args, **call_kwargs)

    target.assert_called_once_with(*args, **expected_kwargs)
    assert result is expected


@requires_interpreted
@pytest.mark.parametrize(
    ("alias_name", "target_name", "args", "call_kwargs", "expected_kwargs", "expected"),
    tuple(case for case in _DELEGATION_CASES if case[0] != "fetch_stream"),
)
async def test_async_fetch_alias_delegates(
    alias_name: str,
    target_name: str,
    args: tuple[Any, ...],
    call_kwargs: dict[str, Any],
    expected_kwargs: dict[str, Any],
    expected: Any,
) -> None:
    driver = AsyncMock(spec=AsyncDriverAdapterBase)
    target = AsyncMock(return_value=expected)
    setattr(driver, target_name, target)

    result = await getattr(AsyncDriverAdapterBase, alias_name)(driver, *args, **call_kwargs)

    target.assert_awaited_once_with(*args, **expected_kwargs)
    assert result is expected


@requires_interpreted
def test_async_fetch_stream_delegates_without_awaiting() -> None:
    driver = Mock(spec=AsyncDriverAdapterBase)
    stream = object()
    driver.select_stream = Mock(return_value=stream)

    result = AsyncDriverAdapterBase.fetch_stream(
        driver, "SELECT * FROM users", schema_type=None, statement_config=None, chunk_size=25, native_only=False
    )

    driver.select_stream.assert_called_once_with(
        "SELECT * FROM users", schema_type=None, statement_config=None, chunk_size=25, native_only=False
    )
    assert result is stream


@requires_interpreted
def test_sync_fetch_preserves_schema_type() -> None:
    class UserSchema:
        pass

    driver = Mock(spec=SyncDriverAdapterBase)
    expected = [UserSchema()]
    driver.select = Mock(return_value=expected)

    result = SyncDriverAdapterBase.fetch(driver, "SELECT * FROM users", schema_type=UserSchema, statement_config=None)

    driver.select.assert_called_once_with("SELECT * FROM users", schema_type=UserSchema, statement_config=None)
    assert result is expected


@requires_interpreted
async def test_async_fetch_one_preserves_schema_type() -> None:
    class UserSchema:
        pass

    driver = AsyncMock(spec=AsyncDriverAdapterBase)
    expected = UserSchema()
    driver.select_one = AsyncMock(return_value=expected)

    result = await AsyncDriverAdapterBase.fetch_one(
        driver, "SELECT * FROM users WHERE id = 1", schema_type=UserSchema, statement_config=None
    )

    driver.select_one.assert_awaited_once_with(
        "SELECT * FROM users WHERE id = 1", schema_type=UserSchema, statement_config=None
    )
    assert result is expected
