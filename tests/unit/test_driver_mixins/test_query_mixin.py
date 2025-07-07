"""Unit tests for query mixin functionality."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.driver.mixins import AsyncQueryMixin, SyncQueryMixin
from sqlspec.exceptions import NotFoundError
from sqlspec.statement.filters import OffsetPagination
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL


class MockSyncDriver(SyncQueryMixin):
    """Mock driver for testing SyncQueryMixin."""

    def __init__(self) -> None:
        self._connection = MagicMock()
        self.config = MagicMock()
        self.dialect = "sqlite"
        self.execute = MagicMock()

    @property
    def connection(self) -> MagicMock:
        return self._connection  # type: ignore[no-any-return]

    def _transform_to_sql(self, statement: Any, params: Any = None, config: Any = None) -> SQL:
        """Mock implementation of _normalize_statement."""
        return SQL(statement if isinstance(statement, str) else str(statement))


class MockAsyncDriver(AsyncQueryMixin):
    """Mock driver for testing AsyncQueryMixin."""

    def __init__(self) -> None:
        self._connection = MagicMock()
        self.config = MagicMock()
        self.dialect = "sqlite"
        self.execute = AsyncMock()

    @property
    def connection(self) -> MagicMock:
        return self._connection  # type: ignore[no-any-return]

    def _transform_to_sql(self, statement: Any, params: Any = None, config: Any = None) -> SQL:
        """Mock implementation of _normalize_statement."""
        return SQL(statement if isinstance(statement, str) else str(statement))


class TestSyncQueryMixin:
    """Test synchronous query mixin methods."""

    def test_select_one_success(self) -> None:
        """Test select_one returns exactly one row."""
        driver = MockSyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users WHERE id = 1"),
            data=[{"id": 1, "name": "John"}],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        result = driver.select_one("SELECT * FROM users WHERE id = 1")
        assert result == {"id": 1, "name": "John"}
        driver.execute.assert_called_once()

    def test_select_one_no_rows(self) -> None:
        """Test select_one raises when no rows found."""
        driver = MockSyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users WHERE id = 1"),
            data=[],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        with pytest.raises(NotFoundError):
            driver.select_one("SELECT * FROM users WHERE id = 1")

    def test_select_one_multiple_rows(self) -> None:
        """Test select_one raises when multiple rows found."""
        driver = MockSyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users"),
            data=[{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        with pytest.raises(ValueError, match="Expected exactly one row"):
            driver.select_one("SELECT * FROM users")

    def test_select_one_or_none_success(self) -> None:
        """Test select_one_or_none returns one row when found."""
        driver = MockSyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users WHERE id = 1"),
            data=[{"id": 1, "name": "John"}],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        result = driver.select_one_or_none("SELECT * FROM users WHERE id = 1")
        assert result == {"id": 1, "name": "John"}

    def test_select_one_or_none_no_rows(self) -> None:
        """Test select_one_or_none returns None when no rows found."""
        driver = MockSyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users WHERE id = 999"),
            data=[],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        result = driver.select_one_or_none("SELECT * FROM users WHERE id = 999")
        assert result is None

    def test_select_value_success(self) -> None:
        """Test select_value returns single scalar value."""
        driver = MockSyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT COUNT(*) FROM users"),
            data=[{"count": 42}],
            column_names=["count"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        result = driver.select_value("SELECT COUNT(*) FROM users")
        assert result == 42

    def test_select_value_tuple_row(self) -> None:
        """Test select_value works with tuple rows."""
        driver = MockSyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT COUNT(*) FROM users"),
            data=[(42,)],
            column_names=["count"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        result = driver.select_value("SELECT COUNT(*) FROM users")
        assert result == 42

    def test_select_value_or_none_no_rows(self) -> None:
        """Test select_value_or_none returns None when no rows."""
        driver = MockSyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT name FROM users WHERE id = 999"),
            data=[],
            column_names=["name"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        result = driver.select_value_or_none("SELECT name FROM users WHERE id = 999")
        assert result is None

    def test_select_returns_all_rows(self) -> None:
        """Test select returns all rows."""
        driver = MockSyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users"),
            data=[{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}, {"id": 3, "name": "Bob"}],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        result = driver.select("SELECT * FROM users")
        assert len(result) == 3
        assert result[0]["name"] == "John"
        assert result[2]["name"] == "Bob"

    def test_paginate_with_kwargs(self) -> None:
        """Test paginate with limit/offset in kwargs."""
        driver = MockSyncDriver()

        # Mock count query result
        count_result = SQLResult(
            statement=SQL("SELECT COUNT(*) FROM users"),
            data=[{"count": 100}],
            column_names=["count"],
            rows_affected=0,
            operation_type="SELECT",
        )

        # Mock data query result
        data_result = SQLResult(
            statement=SQL("SELECT * FROM users LIMIT 10 OFFSET 20"),
            data=[{"id": i, "name": f"User{i}"} for i in range(21, 31)],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )

        driver.execute.side_effect = [count_result, data_result]

        result = driver.paginate("SELECT * FROM users", limit=10, offset=20)

        assert isinstance(result, OffsetPagination)
        assert result.limit == 10
        assert result.offset == 20
        assert result.total == 100
        assert len(result.items) == 10
        assert result.items[0]["id"] == 21

    def test_paginate_with_limit_offset_filter(self) -> None:
        """Test paginate with LimitOffsetFilter."""
        from sqlspec.statement.filters import LimitOffsetFilter

        driver = MockSyncDriver()

        # Mock count query result
        count_result = SQLResult(
            statement=SQL("SELECT COUNT(*) FROM users"),
            data=[{"count": 50}],
            column_names=["count"],
            rows_affected=0,
            operation_type="SELECT",
        )

        # Mock data query result
        data_result = SQLResult(
            statement=SQL("SELECT * FROM users LIMIT 5 OFFSET 10"),
            data=[{"id": i, "name": f"User{i}"} for i in range(11, 16)],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )

        driver.execute.side_effect = [count_result, data_result]

        filter_obj = LimitOffsetFilter(limit=5, offset=10)
        result = driver.paginate("SELECT * FROM users", filter_obj)

        assert isinstance(result, OffsetPagination)
        assert result.limit == 5
        assert result.offset == 10
        assert result.total == 50
        assert len(result.items) == 5

    def test_paginate_no_limit_offset(self) -> None:
        """Test paginate raises when no limit/offset provided."""
        driver = MockSyncDriver()

        with pytest.raises(ValueError, match="Pagination requires"):
            driver.paginate("SELECT * FROM users")


class TestAsyncQueryMixin:
    """Test asynchronous query mixin methods."""

    @pytest.mark.asyncio
    async def test_select_one_async(self) -> None:
        """Test async select_one returns exactly one row."""
        driver = MockAsyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT * FROM users WHERE id = 1"),
            data=[{"id": 1, "name": "John"}],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        result = await driver.select_one("SELECT * FROM users WHERE id = 1")
        assert result == {"id": 1, "name": "John"}
        driver.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_select_value_async(self) -> None:
        """Test async select_value returns single scalar value."""
        driver = MockAsyncDriver()
        mock_result = SQLResult(
            statement=SQL("SELECT COUNT(*) FROM users"),
            data=[{"count": 42}],
            column_names=["count"],
            rows_affected=0,
            operation_type="SELECT",
        )
        driver.execute.return_value = mock_result

        result = await driver.select_value("SELECT COUNT(*) FROM users")
        assert result == 42

    @pytest.mark.asyncio
    async def test_paginate_async(self) -> None:
        """Test async paginate with limit/offset."""
        driver = MockAsyncDriver()

        # Mock count query result
        count_result = SQLResult(
            statement=SQL("SELECT COUNT(*) FROM users"),
            data=[{"count": 100}],
            column_names=["count"],
            rows_affected=0,
            operation_type="SELECT",
        )

        # Mock data query result
        data_result = SQLResult(
            statement=SQL("SELECT * FROM users LIMIT 10 OFFSET 0"),
            data=[{"id": i, "name": f"User{i}"} for i in range(1, 11)],
            column_names=["id", "name"],
            rows_affected=0,
            operation_type="SELECT",
        )

        driver.execute.side_effect = [count_result, data_result]

        result = await driver.paginate("SELECT * FROM users", limit=10, offset=0)

        # Check all attributes of the result
        assert result.limit == 10
        assert result.offset == 0
        assert result.total == 100
        assert len(result.items) == 10
        assert result.items[0]["id"] == 1
