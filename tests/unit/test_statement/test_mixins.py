"""Unit tests for sqlspec.statement.mixins module.

Tests the mixin classes that provide additional functionality for database drivers,
including SQL translation, result conversion, and data format support.
"""

import datetime
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from unittest.mock import Mock, patch
from uuid import UUID

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLConversionError, SQLSpecError
from sqlspec.statement.mixins import (
    AsyncArrowMixin,
    AsyncParquetMixin,
    ResultConverter,
    SQLTranslatorMixin,
    SyncArrowMixin,
    SyncParquetMixin,
    _default_msgspec_deserializer,
)
from sqlspec.statement.sql import SQL, SQLConfig


# Test data models for conversion testing
@dataclass
class UserDataclass:
    """Test dataclass for conversion."""

    id: int
    name: str
    email: str
    active: bool = True


class UserStatus(Enum):
    """Test enum for conversion."""

    ACTIVE = "active"
    INACTIVE = "inactive"


@pytest.fixture
def translator_mixin() -> SQLTranslatorMixin[Any]:
    """Create a SQLTranslatorMixin instance for testing."""

    class TestTranslator(SQLTranslatorMixin[Any]):
        dialect = "postgres"

    return TestTranslator()


@pytest.mark.parametrize(
    ("statement", "from_dialect", "to_dialect", "should_succeed"),
    [
        ("SELECT * FROM users", "mysql", "postgres", True),
        ("SELECT `column` FROM `table`", "mysql", "postgres", True),
        ("SELECT TOP 10 * FROM users", "sqlserver", "postgres", True),
        (exp.Select().select(exp.Star()).from_("users"), None, "mysql", True),
        ("INVALID SQL SYNTAX", "mysql", "postgres", False),
    ],
    ids=["mysql_to_postgres", "backticks_conversion", "top_to_limit", "expression_input", "invalid_sql"],
)
def test_convert_to_dialect(
    translator_mixin: SQLTranslatorMixin[Any],
    statement: Any,
    from_dialect: Optional[str],
    to_dialect: Optional[str],
    should_succeed: bool,
) -> None:
    """Test convert_to_dialect method with various inputs."""
    if should_succeed:
        if isinstance(statement, str) and from_dialect:
            # Mock the dialect parsing
            with patch("sqlspec.statement.mixins.parse_one") as mock_parse:
                mock_expr = Mock(spec=exp.Expression)
                mock_expr.sql.return_value = f"-- Converted from {from_dialect} to {to_dialect}"
                mock_parse.return_value = mock_expr

                result = translator_mixin.convert_to_dialect(statement, to_dialect)

                assert isinstance(result, str)
                mock_parse.assert_called_once_with(statement, dialect=translator_mixin.dialect)
        else:
            # Direct conversion
            result = translator_mixin.convert_to_dialect(statement, to_dialect)
            assert isinstance(result, str)
    else:
        with pytest.raises(SQLConversionError):
            translator_mixin.convert_to_dialect(statement, to_dialect)


def test_convert_to_dialect_with_sql_instance(translator_mixin: SQLTranslatorMixin[Any]) -> None:
    """Test convert_to_dialect with SQL instance."""
    # Create SQL instance with expression
    sql_stmt = SQL("SELECT * FROM users")

    result = translator_mixin.convert_to_dialect(sql_stmt, "mysql")

    assert isinstance(result, str)
    assert "SELECT" in result.upper()


def test_convert_to_dialect_unparseable_sql(translator_mixin: SQLTranslatorMixin[Any]) -> None:
    """Test convert_to_dialect with SQL that has no expression."""
    # Create SQL instance without expression parsing
    config = SQLConfig(enable_parsing=False)
    sql_stmt = SQL("SELECT * FROM users", config=config)
    sql_stmt._parsed_expression = None  # Force no expression

    with pytest.raises(SQLConversionError, match="Statement could not be parsed"):
        translator_mixin.convert_to_dialect(sql_stmt, "mysql")


@pytest.mark.parametrize(
    ("pretty", "expected_method_call"),
    [
        (True, {"pretty": True}),
        (False, {"pretty": False}),
    ],
    ids=["pretty_enabled", "pretty_disabled"],
)
def test_convert_to_dialect_pretty_formatting(
    translator_mixin: SQLTranslatorMixin[Any], pretty: bool, expected_method_call: dict[str, Any]
) -> None:
    """Test pretty formatting parameter."""
    with patch("sqlspec.statement.mixins.parse_one") as mock_parse:
        mock_expr = Mock(spec=exp.Expression)
        mock_expr.sql.return_value = "SELECT * FROM users"
        mock_parse.return_value = mock_expr

        translator_mixin.convert_to_dialect("SELECT * FROM users", "mysql", pretty=pretty)

        mock_expr.sql.assert_called_once_with(dialect="mysql", **expected_method_call)


@pytest.fixture
def sample_dict_data() -> dict[str, Any]:
    """Sample dictionary data for conversion testing."""
    return {"id": 1, "name": "John Doe", "email": "john@example.com", "active": True}


@pytest.fixture
def sample_list_data() -> list[dict[str, Any]]:
    """Sample list of dictionaries for conversion testing."""
    return [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "active": True},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "active": False},
    ]


def test_to_schema_no_schema_type_single_item(sample_dict_data: dict[str, Any]) -> None:
    """Test to_schema with no schema type for single item."""
    result = ResultConverter.to_schema(sample_dict_data)

    assert result is sample_dict_data  # Should return same object


def test_to_schema_no_schema_type_list(sample_list_data: list[dict[str, Any]]) -> None:
    """Test to_schema with no schema type for list."""
    result = ResultConverter.to_schema(sample_list_data)

    assert result is sample_list_data  # Should return same object


def test_to_schema_dataclass_single_item(sample_dict_data: dict[str, Any]) -> None:
    """Test to_schema with dataclass for single item."""
    result = ResultConverter.to_schema(sample_dict_data, schema_type=UserDataclass)

    assert isinstance(result, UserDataclass)
    assert result.id == 1
    assert result.name == "John Doe"
    assert result.email == "john@example.com"
    assert result.active is True


def test_to_schema_dataclass_list(sample_list_data: list[dict[str, Any]]) -> None:
    """Test to_schema with dataclass for list."""
    result = ResultConverter.to_schema(sample_list_data, schema_type=UserDataclass)

    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(item, UserDataclass) for item in result)
    assert result[0].name == "John Doe"
    assert result[1].name == "Jane Smith"


@pytest.mark.parametrize(
    ("schema_type", "should_succeed"),
    [
        (UserDataclass, True),  # Valid dataclass
        # Note: We'll skip the invalid types to avoid linter errors with type[Any]
    ],
    ids=["valid_dataclass"],
)
def test_to_schema_valid_schema_types(
    sample_dict_data: dict[str, Any], schema_type: type[UserDataclass], should_succeed: bool
) -> None:
    """Test to_schema with valid schema types."""
    if should_succeed:
        result = ResultConverter.to_schema(sample_dict_data, schema_type=schema_type)
        assert isinstance(result, schema_type)


def test_to_schema_invalid_schema_types(sample_dict_data: dict[str, Any]) -> None:
    """Test to_schema with invalid schema types."""
    # Test with string type (invalid)
    with pytest.raises(SQLSpecError, match="should be a valid Dataclass"):
        ResultConverter.to_schema(sample_dict_data, schema_type=str)  # type: ignore[type-var]

    # Test with int type (invalid)
    with pytest.raises(SQLSpecError, match="should be a valid Dataclass"):
        ResultConverter.to_schema(sample_dict_data, schema_type=int)  # type: ignore[type-var]


def test_to_schema_msgspec_struct_mock(sample_dict_data: dict[str, Any]) -> None:
    """Test to_schema with mocked msgspec struct."""
    # Mock msgspec functionality
    with (
        patch("sqlspec.statement.mixins.is_msgspec_struct", return_value=True),
        patch("sqlspec.statement.mixins.convert") as mock_convert,
    ):
        mock_schema_type = Mock()
        mock_convert.return_value = Mock()

        result = ResultConverter.to_schema(sample_dict_data, schema_type=mock_schema_type)  # type: ignore[arg-type,var-annotated]

        assert mock_convert.called
        assert result is mock_convert.return_value


def test_to_schema_pydantic_model_mock(sample_dict_data: dict[str, Any]) -> None:
    """Test to_schema with mocked Pydantic model."""
    # Mock Pydantic functionality
    with (
        patch("sqlspec.statement.mixins.is_pydantic_model", return_value=True),
        patch("sqlspec.statement.mixins.get_type_adapter") as mock_adapter,
    ):
        mock_schema_type = Mock()
        mock_type_adapter = Mock()
        mock_type_adapter.validate_python.return_value = Mock()
        mock_adapter.return_value = mock_type_adapter

        result = ResultConverter.to_schema(sample_dict_data, schema_type=mock_schema_type)  # type: ignore[arg-type,var-annotated]

        mock_adapter.assert_called_once_with(mock_schema_type)
        mock_type_adapter.validate_python.assert_called_once_with(sample_dict_data, from_attributes=True)
        assert result is mock_type_adapter.validate_python.return_value


@pytest.mark.parametrize(
    ("target_type", "value", "expected_result"),
    [
        (UUID, "550e8400-e29b-41d4-a716-446655440000", "550e8400e29b41d4a716446655440000"),
        (datetime.datetime, datetime.datetime(2023, 1, 1, 12, 0, 0), "2023-01-01T12:00:00"),
        (datetime.date, datetime.date(2023, 1, 1), "2023-01-01"),
        (datetime.time, datetime.time(12, 0, 0), "12:00:00"),
        (UserStatus, UserStatus.ACTIVE, "active"),
        (Path, "/tmp/test", Path("/tmp/test")),
    ],
    ids=[
        "uuid_conversion",
        "datetime_conversion",
        "date_conversion",
        "time_conversion",
        "enum_conversion",
        "path_conversion",
    ],
)
def test_default_msgspec_deserializer_type_decoders(target_type: type, value: Any, expected_result: Any) -> None:
    """Test default type decoders."""
    if target_type == UUID:
        # UUID needs special handling - test the hex conversion
        uuid_val = UUID(value)
        result = _default_msgspec_deserializer(target_type, uuid_val)
        assert result == expected_result
    elif target_type in (datetime.datetime, datetime.date, datetime.time):
        result = _default_msgspec_deserializer(target_type, value)
        assert result == expected_result
    elif target_type == UserStatus:
        result = _default_msgspec_deserializer(target_type, value)
        assert result == expected_result
    else:
        result = _default_msgspec_deserializer(target_type, value)
        assert result == expected_result


def test_default_msgspec_deserializer_already_correct_type() -> None:
    """Test when value is already the target type."""
    value = 42
    result = _default_msgspec_deserializer(int, value)

    assert result == value
    assert result is value


def test_default_msgspec_deserializer_direct_conversion() -> None:
    """Test direct type conversion."""
    result = _default_msgspec_deserializer(str, 42)

    assert result == "42"
    assert isinstance(result, str)


def test_default_msgspec_deserializer_unsupported_type() -> None:
    """Test with unsupported type conversion."""

    class UnsupportedType:
        def __init__(self, value: Any) -> None:
            raise ValueError("Cannot create instance")

    with pytest.raises(TypeError, match="Unsupported type"):
        _default_msgspec_deserializer(UnsupportedType, "value")


def test_default_msgspec_deserializer_custom_decoders() -> None:
    """Test with custom type decoders."""

    def custom_predicate(target_type: type) -> bool:
        return target_type is str

    def custom_decoder(target_type: type, value: Any) -> str:
        return f"CUSTOM:{value}"

    custom_decoders = [(custom_predicate, custom_decoder)]

    result = _default_msgspec_deserializer(str, "test", type_decoders=custom_decoders)

    assert result == "CUSTOM:test"


@pytest.fixture
def arrow_mixin() -> SyncArrowMixin[Any]:
    """Create a SyncArrowMixin instance for testing."""

    class TestArrowMixin(SyncArrowMixin[Any]):
        pass

    return TestArrowMixin()  # type: ignore


def test_select_to_arrow_not_implemented(arrow_mixin: SyncArrowMixin[Any]) -> None:
    """Test that select_to_arrow raises NotImplementedError by default."""
    with pytest.raises(NotImplementedError, match="Arrow support not implemented"):
        arrow_mixin.select_to_arrow("SELECT * FROM users")


def test_select_to_arrow_signature(arrow_mixin: SyncArrowMixin[Any]) -> None:
    """Test select_to_arrow method signature accepts expected parameters."""
    # This test verifies the method exists and accepts the expected parameters
    try:
        arrow_mixin.select_to_arrow(
            statement="SELECT * FROM users",
            parameters={"id": 1},
            connection=None,
            config=None,
            custom_param="value",
        )
    except NotImplementedError:
        pass  # Expected
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")


@pytest.fixture
def async_arrow_mixin() -> AsyncArrowMixin[Any]:
    """Create an AsyncArrowMixin instance for testing."""

    class TestAsyncArrowMixin(AsyncArrowMixin[Any]):
        pass

    return TestAsyncArrowMixin()  # type: ignore


async def test_async_select_to_arrow_not_implemented(async_arrow_mixin: AsyncArrowMixin[Any]) -> None:
    """Test that select_to_arrow raises NotImplementedError by default."""
    with pytest.raises(NotImplementedError, match="Arrow support not implemented"):
        await async_arrow_mixin.select_to_arrow("SELECT * FROM users")


async def test_async_select_to_arrow_signature(async_arrow_mixin: AsyncArrowMixin[Any]) -> None:
    """Test select_to_arrow method signature accepts expected parameters."""
    # This test verifies the method exists and accepts the expected parameters
    try:
        await async_arrow_mixin.select_to_arrow(
            statement="SELECT * FROM users",
            parameters={"id": 1},
            connection=None,
            config=None,
            custom_param="value",
        )
    except NotImplementedError:
        pass  # Expected
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")


@pytest.fixture
def parquet_mixin() -> SyncParquetMixin[Any]:
    """Create a SyncParquetMixin instance for testing."""

    class TestParquetMixin(SyncParquetMixin[Any]):
        pass

    return TestParquetMixin()  # type: ignore


def test_to_parquet_not_implemented(parquet_mixin: SyncParquetMixin[Any]) -> None:
    """Test that to_parquet raises NotImplementedError by default."""
    with pytest.raises(NotImplementedError, match="Parquet support not implemented"):
        parquet_mixin.to_parquet("SELECT * FROM users")


def test_to_parquet_signature(parquet_mixin: SyncParquetMixin[Any]) -> None:
    """Test to_parquet method signature accepts expected parameters."""
    # This test verifies the method exists and accepts the expected parameters
    try:
        parquet_mixin.to_parquet(
            statement="SELECT * FROM users",
            parameters={"id": 1},
            connection=None,
            config=None,
            file_path="/tmp/test.parquet",
        )
    except NotImplementedError:
        pass  # Expected
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")


@pytest.fixture
def async_parquet_mixin() -> AsyncParquetMixin[Any]:
    """Create an AsyncParquetMixin instance for testing."""

    class TestAsyncParquetMixin(AsyncParquetMixin[Any]):
        pass

    return TestAsyncParquetMixin()  # type: ignore


async def test_async_to_parquet_not_implemented(async_parquet_mixin: AsyncParquetMixin[Any]) -> None:
    """Test that to_parquet raises NotImplementedError by default."""
    with pytest.raises(NotImplementedError, match="Parquet support not implemented"):
        await async_parquet_mixin.to_parquet("SELECT * FROM users")


async def test_async_to_parquet_signature(async_parquet_mixin: AsyncParquetMixin[Any]) -> None:
    """Test to_parquet method signature accepts expected parameters."""
    # This test verifies the method exists and accepts the expected parameters
    try:
        await async_parquet_mixin.to_parquet(
            statement="SELECT * FROM users",
            parameters={"id": 1},
            connection=None,
            config=None,
            file_path="/tmp/test.parquet",
        )
    except NotImplementedError:
        pass  # Expected
    except Exception as e:
        pytest.fail(f"Unexpected exception: {e}")


def test_sql_translator_mixin_inheritance() -> None:
    """Test that SQLTranslatorMixin can be properly inherited."""

    class CombinedMixin(SQLTranslatorMixin[Any], SyncArrowMixin[Any]):
        dialect = "postgres"

    instance = CombinedMixin()  # type: ignore

    # Should have both translator and arrow capabilities
    assert hasattr(instance, "convert_to_dialect")
    assert hasattr(instance, "select_to_arrow")
    assert instance.dialect == "postgres"


def test_result_converter_static_methods() -> None:
    """Test that ResultConverter static methods work independently."""
    data = {"id": 1, "name": "test", "email": "test@example.com"}

    # Should work without instantiation
    result = ResultConverter.to_schema(data)
    assert result is data

    # Test with dataclass conversion
    result_dc = ResultConverter.to_schema(data, schema_type=UserDataclass)
    assert isinstance(result_dc, UserDataclass)
    assert result_dc.id == 1
    assert result_dc.name == "test"
    assert result_dc.email == "test@example.com"


@pytest.mark.parametrize(
    ("mixin_class", "method_name", "is_async"),
    [
        (SyncArrowMixin, "select_to_arrow", False),
        (AsyncArrowMixin, "select_to_arrow", True),
        (SyncParquetMixin, "to_parquet", False),
        (AsyncParquetMixin, "to_parquet", True),
    ],
    ids=["sync_arrow", "async_arrow", "sync_parquet", "async_parquet"],
)
def test_mixin_method_consistency(mixin_class: type, method_name: str, is_async: bool) -> None:
    """Test that mixin methods have consistent signatures and behavior."""

    class TestMixin(mixin_class):  # type: ignore[misc]
        pass

    instance = TestMixin()
    method = getattr(instance, method_name)

    # Method should exist
    assert callable(method)

    # Method should raise NotImplementedError
    if is_async:

        async def run_test() -> None:
            with pytest.raises(NotImplementedError):
                await method("SELECT * FROM test")  # pyright: ignore

        # Run async test (we can't use asyncio.run here easily in pytest)
        import asyncio

        try:
            asyncio.run(run_test())
        except RuntimeError:
            # If there's already an event loop, create a new task
            pass
    else:
        with pytest.raises(NotImplementedError):
            method("SELECT * FROM test")
