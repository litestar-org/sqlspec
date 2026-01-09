# pyright: reportPrivateUsage=false
"""Tests for fetch* method aliases in SyncDriverAdapterBase and AsyncDriverAdapterBase.

Tests that all 14 fetch* methods (7 sync + 7 async) exist, have matching signatures,
and delegate correctly to their corresponding select* methods.

Uses function-based pytest approach as per AGENTS.md requirements.
"""

import inspect
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("driver")


# Test method existence and signature equivalence


def test_sync_fetch_method_exists() -> None:
    """Test that fetch() method exists on SyncDriverAdapterBase."""
    assert hasattr(SyncDriverAdapterBase, "fetch")
    assert callable(getattr(SyncDriverAdapterBase, "fetch"))


def test_sync_fetch_one_method_exists() -> None:
    """Test that fetch_one() method exists on SyncDriverAdapterBase."""
    assert hasattr(SyncDriverAdapterBase, "fetch_one")
    assert callable(getattr(SyncDriverAdapterBase, "fetch_one"))


def test_sync_fetch_one_or_none_method_exists() -> None:
    """Test that fetch_one_or_none() method exists on SyncDriverAdapterBase."""
    assert hasattr(SyncDriverAdapterBase, "fetch_one_or_none")
    assert callable(getattr(SyncDriverAdapterBase, "fetch_one_or_none"))


def test_sync_fetch_value_method_exists() -> None:
    """Test that fetch_value() method exists on SyncDriverAdapterBase."""
    assert hasattr(SyncDriverAdapterBase, "fetch_value")
    assert callable(getattr(SyncDriverAdapterBase, "fetch_value"))


def test_sync_fetch_value_or_none_method_exists() -> None:
    """Test that fetch_value_or_none() method exists on SyncDriverAdapterBase."""
    assert hasattr(SyncDriverAdapterBase, "fetch_value_or_none")
    assert callable(getattr(SyncDriverAdapterBase, "fetch_value_or_none"))


def test_sync_fetch_to_arrow_method_exists() -> None:
    """Test that fetch_to_arrow() method exists on SyncDriverAdapterBase."""
    assert hasattr(SyncDriverAdapterBase, "fetch_to_arrow")
    assert callable(getattr(SyncDriverAdapterBase, "fetch_to_arrow"))


def test_sync_fetch_with_total_method_exists() -> None:
    """Test that fetch_with_total() method exists on SyncDriverAdapterBase."""
    assert hasattr(SyncDriverAdapterBase, "fetch_with_total")
    assert callable(getattr(SyncDriverAdapterBase, "fetch_with_total"))


def test_async_fetch_method_exists() -> None:
    """Test that fetch() method exists on AsyncDriverAdapterBase."""
    assert hasattr(AsyncDriverAdapterBase, "fetch")
    assert callable(getattr(AsyncDriverAdapterBase, "fetch"))


def test_async_fetch_one_method_exists() -> None:
    """Test that fetch_one() method exists on AsyncDriverAdapterBase."""
    assert hasattr(AsyncDriverAdapterBase, "fetch_one")
    assert callable(getattr(AsyncDriverAdapterBase, "fetch_one"))


def test_async_fetch_one_or_none_method_exists() -> None:
    """Test that fetch_one_or_none() method exists on AsyncDriverAdapterBase."""
    assert hasattr(AsyncDriverAdapterBase, "fetch_one_or_none")
    assert callable(getattr(AsyncDriverAdapterBase, "fetch_one_or_none"))


def test_async_fetch_value_method_exists() -> None:
    """Test that fetch_value() method exists on AsyncDriverAdapterBase."""
    assert hasattr(AsyncDriverAdapterBase, "fetch_value")
    assert callable(getattr(AsyncDriverAdapterBase, "fetch_value"))


def test_async_fetch_value_or_none_method_exists() -> None:
    """Test that fetch_value_or_none() method exists on AsyncDriverAdapterBase."""
    assert hasattr(AsyncDriverAdapterBase, "fetch_value_or_none")
    assert callable(getattr(AsyncDriverAdapterBase, "fetch_value_or_none"))


def test_async_fetch_to_arrow_method_exists() -> None:
    """Test that fetch_to_arrow() method exists on AsyncDriverAdapterBase."""
    assert hasattr(AsyncDriverAdapterBase, "fetch_to_arrow")
    assert callable(getattr(AsyncDriverAdapterBase, "fetch_to_arrow"))


def test_async_fetch_with_total_method_exists() -> None:
    """Test that fetch_with_total() method exists on AsyncDriverAdapterBase."""
    assert hasattr(AsyncDriverAdapterBase, "fetch_with_total")
    assert callable(getattr(AsyncDriverAdapterBase, "fetch_with_total"))


# Test signature equivalence between fetch* and select* methods


def test_sync_fetch_signature_matches_select() -> None:
    """Test that fetch() signature matches select() signature."""
    fetch_sig = inspect.signature(SyncDriverAdapterBase.fetch)
    select_sig = inspect.signature(SyncDriverAdapterBase.select)

    # Parameters should be identical
    assert fetch_sig.parameters == select_sig.parameters


def test_sync_fetch_one_signature_matches_select_one() -> None:
    """Test that fetch_one() signature matches select_one() signature."""
    fetch_sig = inspect.signature(SyncDriverAdapterBase.fetch_one)
    select_sig = inspect.signature(SyncDriverAdapterBase.select_one)

    assert fetch_sig.parameters == select_sig.parameters


def test_sync_fetch_one_or_none_signature_matches_select_one_or_none() -> None:
    """Test that fetch_one_or_none() signature matches select_one_or_none() signature."""
    fetch_sig = inspect.signature(SyncDriverAdapterBase.fetch_one_or_none)
    select_sig = inspect.signature(SyncDriverAdapterBase.select_one_or_none)

    assert fetch_sig.parameters == select_sig.parameters


def test_sync_fetch_value_signature_matches_select_value() -> None:
    """Test that fetch_value() signature matches select_value() signature."""
    fetch_sig = inspect.signature(SyncDriverAdapterBase.fetch_value)
    select_sig = inspect.signature(SyncDriverAdapterBase.select_value)

    assert fetch_sig.parameters == select_sig.parameters


def test_sync_fetch_value_or_none_signature_matches_select_value_or_none() -> None:
    """Test that fetch_value_or_none() signature matches select_value_or_none() signature."""
    fetch_sig = inspect.signature(SyncDriverAdapterBase.fetch_value_or_none)
    select_sig = inspect.signature(SyncDriverAdapterBase.select_value_or_none)

    assert fetch_sig.parameters == select_sig.parameters


def test_sync_fetch_to_arrow_signature_matches_select_to_arrow() -> None:
    """Test that fetch_to_arrow() signature matches select_to_arrow() signature."""
    fetch_sig = inspect.signature(SyncDriverAdapterBase.fetch_to_arrow)
    select_sig = inspect.signature(SyncDriverAdapterBase.select_to_arrow)

    assert fetch_sig.parameters == select_sig.parameters


def test_sync_fetch_with_total_signature_matches_select_with_total() -> None:
    """Test that fetch_with_total() signature matches select_with_total() signature."""
    fetch_sig = inspect.signature(SyncDriverAdapterBase.fetch_with_total)
    select_sig = inspect.signature(SyncDriverAdapterBase.select_with_total)

    assert fetch_sig.parameters == select_sig.parameters


def test_async_fetch_signature_matches_select() -> None:
    """Test that async fetch() signature matches async select() signature."""
    fetch_sig = inspect.signature(AsyncDriverAdapterBase.fetch)
    select_sig = inspect.signature(AsyncDriverAdapterBase.select)

    assert fetch_sig.parameters == select_sig.parameters


def test_async_fetch_one_signature_matches_select_one() -> None:
    """Test that async fetch_one() signature matches async select_one() signature."""
    fetch_sig = inspect.signature(AsyncDriverAdapterBase.fetch_one)
    select_sig = inspect.signature(AsyncDriverAdapterBase.select_one)

    assert fetch_sig.parameters == select_sig.parameters


def test_async_fetch_one_or_none_signature_matches_select_one_or_none() -> None:
    """Test that async fetch_one_or_none() signature matches async select_one_or_none() signature."""
    fetch_sig = inspect.signature(AsyncDriverAdapterBase.fetch_one_or_none)
    select_sig = inspect.signature(AsyncDriverAdapterBase.select_one_or_none)

    assert fetch_sig.parameters == select_sig.parameters


def test_async_fetch_value_signature_matches_select_value() -> None:
    """Test that async fetch_value() signature matches async select_value() signature."""
    fetch_sig = inspect.signature(AsyncDriverAdapterBase.fetch_value)
    select_sig = inspect.signature(AsyncDriverAdapterBase.select_value)

    assert fetch_sig.parameters == select_sig.parameters


def test_async_fetch_value_or_none_signature_matches_select_value_or_none() -> None:
    """Test that async fetch_value_or_none() signature matches async select_value_or_none() signature."""
    fetch_sig = inspect.signature(AsyncDriverAdapterBase.fetch_value_or_none)
    select_sig = inspect.signature(AsyncDriverAdapterBase.select_value_or_none)

    assert fetch_sig.parameters == select_sig.parameters


def test_async_fetch_to_arrow_signature_matches_select_to_arrow() -> None:
    """Test that async fetch_to_arrow() signature matches async select_to_arrow() signature."""
    fetch_sig = inspect.signature(AsyncDriverAdapterBase.fetch_to_arrow)
    select_sig = inspect.signature(AsyncDriverAdapterBase.select_to_arrow)

    assert fetch_sig.parameters == select_sig.parameters


def test_async_fetch_with_total_signature_matches_select_with_total() -> None:
    """Test that async fetch_with_total() signature matches async select_with_total() signature."""
    fetch_sig = inspect.signature(AsyncDriverAdapterBase.fetch_with_total)
    select_sig = inspect.signature(AsyncDriverAdapterBase.select_with_total)

    assert fetch_sig.parameters == select_sig.parameters


# Test delegation behavior using mocks


@requires_interpreted
def test_sync_fetch_delegates_to_select() -> None:
    """Test that fetch() delegates to select() with identical arguments."""
    # Create mock driver with mocked select method
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select = Mock(return_value=[{"id": 1}])

    # Call the real fetch method implementation
    result = SyncDriverAdapterBase.fetch(
        mock_driver, "SELECT * FROM users", {"id": 1}, schema_type=None, statement_config=None
    )

    # Verify select was called with same arguments
    mock_driver.select.assert_called_once_with(
        "SELECT * FROM users", {"id": 1}, schema_type=None, statement_config=None
    )
    assert result == [{"id": 1}]


@requires_interpreted
def test_sync_fetch_one_delegates_to_select_one() -> None:
    """Test that fetch_one() delegates to select_one() with identical arguments."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_one = Mock(return_value={"id": 1})

    result = SyncDriverAdapterBase.fetch_one(
        mock_driver, "SELECT * FROM users WHERE id = ?", {"id": 1}, schema_type=None, statement_config=None
    )

    mock_driver.select_one.assert_called_once_with(
        "SELECT * FROM users WHERE id = ?", {"id": 1}, schema_type=None, statement_config=None
    )
    assert result == {"id": 1}


@requires_interpreted
def test_sync_fetch_one_or_none_delegates_to_select_one_or_none() -> None:
    """Test that fetch_one_or_none() delegates to select_one_or_none() with identical arguments."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_one_or_none = Mock(return_value=None)

    result = SyncDriverAdapterBase.fetch_one_or_none(
        mock_driver, "SELECT * FROM users WHERE id = ?", {"id": 999}, schema_type=None, statement_config=None
    )

    mock_driver.select_one_or_none.assert_called_once_with(
        "SELECT * FROM users WHERE id = ?", {"id": 999}, schema_type=None, statement_config=None
    )
    assert result is None


@requires_interpreted
def test_sync_fetch_value_delegates_to_select_value() -> None:
    """Test that fetch_value() delegates to select_value() with identical arguments."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value = Mock(return_value=42)

    result = SyncDriverAdapterBase.fetch_value(mock_driver, "SELECT COUNT(*) FROM users", statement_config=None)

    mock_driver.select_value.assert_called_once_with("SELECT COUNT(*) FROM users", statement_config=None)
    assert result == 42


@requires_interpreted
def test_sync_fetch_value_or_none_delegates_to_select_value_or_none() -> None:
    """Test that fetch_value_or_none() delegates to select_value_or_none() with identical arguments."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_value_or_none = Mock(return_value=None)

    result = SyncDriverAdapterBase.fetch_value_or_none(
        mock_driver, "SELECT MAX(id) FROM empty_table", statement_config=None
    )

    mock_driver.select_value_or_none.assert_called_once_with("SELECT MAX(id) FROM empty_table", statement_config=None)
    assert result is None


@requires_interpreted
def test_sync_fetch_to_arrow_delegates_to_select_to_arrow() -> None:
    """Test that fetch_to_arrow() delegates to select_to_arrow() with identical arguments."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_arrow_result = Mock()
    mock_driver.select_to_arrow = Mock(return_value=mock_arrow_result)

    result = SyncDriverAdapterBase.fetch_to_arrow(
        mock_driver,
        "SELECT * FROM users",
        statement_config=None,
        return_format="table",
        native_only=False,
        batch_size=None,
        arrow_schema=None,
    )

    mock_driver.select_to_arrow.assert_called_once_with(
        "SELECT * FROM users",
        statement_config=None,
        return_format="table",
        native_only=False,
        batch_size=None,
        arrow_schema=None,
    )
    assert result == mock_arrow_result


@requires_interpreted
def test_sync_fetch_with_total_delegates_to_select_with_total() -> None:
    """Test that fetch_with_total() delegates to select_with_total() with identical arguments."""
    mock_driver = Mock(spec=SyncDriverAdapterBase)
    mock_driver.select_with_total = Mock(return_value=([{"id": 1}, {"id": 2}], 100))

    result = SyncDriverAdapterBase.fetch_with_total(
        mock_driver, "SELECT * FROM users LIMIT 2", schema_type=None, statement_config=None
    )

    mock_driver.select_with_total.assert_called_once_with(
        "SELECT * FROM users LIMIT 2", schema_type=None, statement_config=None
    )
    assert result == ([{"id": 1}, {"id": 2}], 100)


@requires_interpreted
@pytest.mark.asyncio
async def test_async_fetch_delegates_to_select() -> None:
    """Test that async fetch() delegates to async select() with identical arguments."""
    mock_driver = AsyncMock(spec=AsyncDriverAdapterBase)
    mock_driver.select = AsyncMock(return_value=[{"id": 1}])

    result = await AsyncDriverAdapterBase.fetch(
        mock_driver, "SELECT * FROM users", {"id": 1}, schema_type=None, statement_config=None
    )

    mock_driver.select.assert_called_once_with(
        "SELECT * FROM users", {"id": 1}, schema_type=None, statement_config=None
    )
    assert result == [{"id": 1}]


@requires_interpreted
@pytest.mark.asyncio
async def test_async_fetch_one_delegates_to_select_one() -> None:
    """Test that async fetch_one() delegates to async select_one() with identical arguments."""
    mock_driver = AsyncMock(spec=AsyncDriverAdapterBase)
    mock_driver.select_one = AsyncMock(return_value={"id": 1})

    result = await AsyncDriverAdapterBase.fetch_one(
        mock_driver, "SELECT * FROM users WHERE id = ?", {"id": 1}, schema_type=None, statement_config=None
    )

    mock_driver.select_one.assert_called_once_with(
        "SELECT * FROM users WHERE id = ?", {"id": 1}, schema_type=None, statement_config=None
    )
    assert result == {"id": 1}


@requires_interpreted
@pytest.mark.asyncio
async def test_async_fetch_one_or_none_delegates_to_select_one_or_none() -> None:
    """Test that async fetch_one_or_none() delegates to async select_one_or_none() with identical arguments."""
    mock_driver = AsyncMock(spec=AsyncDriverAdapterBase)
    mock_driver.select_one_or_none = AsyncMock(return_value=None)

    result = await AsyncDriverAdapterBase.fetch_one_or_none(
        mock_driver, "SELECT * FROM users WHERE id = ?", {"id": 999}, schema_type=None, statement_config=None
    )

    mock_driver.select_one_or_none.assert_called_once_with(
        "SELECT * FROM users WHERE id = ?", {"id": 999}, schema_type=None, statement_config=None
    )
    assert result is None


@requires_interpreted
@pytest.mark.asyncio
async def test_async_fetch_value_delegates_to_select_value() -> None:
    """Test that async fetch_value() delegates to async select_value() with identical arguments."""
    mock_driver = AsyncMock(spec=AsyncDriverAdapterBase)
    mock_driver.select_value = AsyncMock(return_value=42)

    result = await AsyncDriverAdapterBase.fetch_value(mock_driver, "SELECT COUNT(*) FROM users", statement_config=None)

    mock_driver.select_value.assert_called_once_with("SELECT COUNT(*) FROM users", statement_config=None)
    assert result == 42


@requires_interpreted
@pytest.mark.asyncio
async def test_async_fetch_value_or_none_delegates_to_select_value_or_none() -> None:
    """Test that async fetch_value_or_none() delegates to async select_value_or_none() with identical arguments."""
    mock_driver = AsyncMock(spec=AsyncDriverAdapterBase)
    mock_driver.select_value_or_none = AsyncMock(return_value=None)

    result = await AsyncDriverAdapterBase.fetch_value_or_none(
        mock_driver, "SELECT MAX(id) FROM empty_table", statement_config=None
    )

    mock_driver.select_value_or_none.assert_called_once_with("SELECT MAX(id) FROM empty_table", statement_config=None)
    assert result is None


@requires_interpreted
@pytest.mark.asyncio
async def test_async_fetch_to_arrow_delegates_to_select_to_arrow() -> None:
    """Test that async fetch_to_arrow() delegates to async select_to_arrow() with identical arguments."""
    mock_driver = AsyncMock(spec=AsyncDriverAdapterBase)
    mock_arrow_result = Mock()
    mock_driver.select_to_arrow = AsyncMock(return_value=mock_arrow_result)

    result = await AsyncDriverAdapterBase.fetch_to_arrow(
        mock_driver,
        "SELECT * FROM users",
        statement_config=None,
        return_format="table",
        native_only=False,
        batch_size=None,
        arrow_schema=None,
    )

    mock_driver.select_to_arrow.assert_called_once_with(
        "SELECT * FROM users",
        statement_config=None,
        return_format="table",
        native_only=False,
        batch_size=None,
        arrow_schema=None,
    )
    assert result == mock_arrow_result


@requires_interpreted
@pytest.mark.asyncio
async def test_async_fetch_with_total_delegates_to_select_with_total() -> None:
    """Test that async fetch_with_total() delegates to async select_with_total() with identical arguments."""
    mock_driver = AsyncMock(spec=AsyncDriverAdapterBase)
    mock_driver.select_with_total = AsyncMock(return_value=([{"id": 1}, {"id": 2}], 100))

    result = await AsyncDriverAdapterBase.fetch_with_total(
        mock_driver, "SELECT * FROM users LIMIT 2", schema_type=None, statement_config=None
    )

    mock_driver.select_with_total.assert_called_once_with(
        "SELECT * FROM users LIMIT 2", schema_type=None, statement_config=None
    )
    assert result == ([{"id": 1}, {"id": 2}], 100)


# Test that fetch methods preserve schema_type argument handling


@requires_interpreted
def test_sync_fetch_with_schema_type_argument() -> None:
    """Test that fetch() correctly passes schema_type to select()."""

    class UserSchema:
        """Sample schema class."""

        def __init__(self, **kwargs: Any) -> None:
            self.id = kwargs.get("id")
            self.name = kwargs.get("name")

    mock_driver = Mock(spec=SyncDriverAdapterBase)
    expected_result = [UserSchema(id=1, name="Alice")]
    mock_driver.select = Mock(return_value=expected_result)

    result = SyncDriverAdapterBase.fetch(
        mock_driver, "SELECT * FROM users", schema_type=UserSchema, statement_config=None
    )

    mock_driver.select.assert_called_once_with("SELECT * FROM users", schema_type=UserSchema, statement_config=None)
    assert result == expected_result


@requires_interpreted
@pytest.mark.asyncio
async def test_async_fetch_one_with_schema_type_argument() -> None:
    """Test that async fetch_one() correctly passes schema_type to select_one()."""

    class UserSchema:
        """Sample schema class."""

        def __init__(self, **kwargs: Any) -> None:
            self.id = kwargs.get("id")
            self.name = kwargs.get("name")

    mock_driver = AsyncMock(spec=AsyncDriverAdapterBase)
    expected_result = UserSchema(id=1, name="Alice")
    mock_driver.select_one = AsyncMock(return_value=expected_result)

    result = await AsyncDriverAdapterBase.fetch_one(
        mock_driver, "SELECT * FROM users WHERE id = 1", schema_type=UserSchema, statement_config=None
    )

    mock_driver.select_one.assert_called_once_with(
        "SELECT * FROM users WHERE id = 1", schema_type=UserSchema, statement_config=None
    )
    assert result == expected_result
