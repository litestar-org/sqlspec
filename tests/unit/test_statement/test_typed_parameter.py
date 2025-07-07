"""Tests for TypedParameter functionality."""

import math
from datetime import date, datetime
from decimal import Decimal

from sqlspec.statement.parameters import ParameterConverter, ParameterInfo, ParameterStyle, TypedParameter
from sqlspec.statement.sql import SQL, SQLConfig


def test_typed_parameter_hashable() -> None:
    """Test that TypedParameter is hashable."""
    from sqlglot import exp

    tp1 = TypedParameter(
        value="test", sqlglot_type=exp.DataType.build("VARCHAR"), type_hint="string", semantic_name="username"
    )

    tp2 = TypedParameter(
        value=[1, 2, 3], sqlglot_type=exp.DataType.build("ARRAY"), type_hint="array", semantic_name="ids"
    )

    # Should be hashable
    hash(tp1)
    hash(tp2)

    # Test in set
    param_set = {tp1, tp2}
    assert len(param_set) == 2


def test_wrap_parameters_with_types_dict() -> None:
    """Test wrapping dict parameters with types."""
    params = {
        "name": "John",
        "age": 30,
        "created": datetime(2024, 1, 1, 12, 0, 0),
        "balance": Decimal("100.50"),
        "tags": ["python", "sql"],
        "is_active": True,
        "metadata": {"key": "value"},
    }

    param_info: list[ParameterInfo] = []  # Not used for dict params
    wrapped = ParameterConverter.wrap_parameters_with_types(params, param_info)
    assert isinstance(wrapped, dict)
    # Simple types should not be wrapped
    assert wrapped["name"] == "John"
    assert wrapped["age"] == 30

    # Boolean should be wrapped
    assert isinstance(wrapped["is_active"], TypedParameter)
    assert wrapped["is_active"].value is True
    assert wrapped["is_active"].type_hint == "boolean"

    # Complex types should be wrapped
    assert isinstance(wrapped["created"], TypedParameter)
    assert wrapped["created"].value == datetime(2024, 1, 1, 12, 0, 0)
    assert wrapped["created"].type_hint == "timestamp"
    assert wrapped["created"].semantic_name == "created"

    assert isinstance(wrapped["balance"], TypedParameter)
    assert wrapped["balance"].value == Decimal("100.50")
    assert wrapped["balance"].type_hint == "decimal"

    assert isinstance(wrapped["tags"], TypedParameter)
    assert wrapped["tags"].value == ["python", "sql"]
    assert wrapped["tags"].type_hint == "array"

    assert isinstance(wrapped["metadata"], TypedParameter)
    assert wrapped["metadata"].value == {"key": "value"}
    assert wrapped["metadata"].type_hint == "json"


def test_wrap_parameters_with_types_list() -> None:
    """Test wrapping list parameters with types."""
    params = [
        "John",
        30,
        datetime(2024, 1, 1),
        None,
        [1, 2, 3],
        9999999999,  # Bigint
    ]

    param_info = [
        ParameterInfo(name="name", position=0, style=ParameterStyle.QMARK, ordinal=0, placeholder_text="?"),
        ParameterInfo(name="age", position=10, style=ParameterStyle.QMARK, ordinal=1, placeholder_text="?"),
        ParameterInfo(name=None, position=20, style=ParameterStyle.QMARK, ordinal=2, placeholder_text="?"),
        ParameterInfo(name=None, position=30, style=ParameterStyle.QMARK, ordinal=3, placeholder_text="?"),
        ParameterInfo(name="ids", position=40, style=ParameterStyle.QMARK, ordinal=4, placeholder_text="?"),
        ParameterInfo(name=None, position=50, style=ParameterStyle.QMARK, ordinal=5, placeholder_text="?"),
    ]

    wrapped = ParameterConverter.wrap_parameters_with_types(params, param_info)
    assert isinstance(wrapped, list)

    # Simple types should not be wrapped
    assert wrapped[0] == "John"
    assert wrapped[1] == 30

    # Complex types should be wrapped
    assert isinstance(wrapped[2], TypedParameter)
    assert wrapped[2].value == datetime(2024, 1, 1)
    assert wrapped[2].type_hint == "timestamp"

    assert isinstance(wrapped[3], TypedParameter)
    assert wrapped[3].value is None
    assert wrapped[3].type_hint == "null"

    assert isinstance(wrapped[4], TypedParameter)
    assert wrapped[4].value == [1, 2, 3]
    assert wrapped[4].type_hint == "array"
    assert wrapped[4].semantic_name == "ids"

    # Bigint should be wrapped
    assert isinstance(wrapped[5], TypedParameter)
    assert wrapped[5].value == 9999999999
    assert wrapped[5].type_hint == "bigint"


def test_wrap_parameters_with_types_already_wrapped() -> None:
    """Test that already wrapped parameters are not re-wrapped."""
    from sqlglot import exp

    tp = TypedParameter(
        value="test", sqlglot_type=exp.DataType.build("VARCHAR"), type_hint="string", semantic_name="test_param"
    )

    params = {"param": tp}
    wrapped = ParameterConverter.wrap_parameters_with_types(params, [])
    assert isinstance(wrapped, dict)

    # Should be the same object
    assert wrapped["param"] is tp


def test_sql_with_typed_parameters() -> None:
    """Test SQL execution with TypedParameter wrapping."""
    config = SQLConfig(enable_parameter_type_wrapping=True)

    # Test with datetime parameter
    sql = SQL("SELECT * FROM users WHERE created > ? AND active = ?", datetime(2024, 1, 1), True, _config=config)

    # Process the SQL (this will wrap parameters internally)
    compiled_sql, params = sql.compile()

    # The compile method unwraps TypedParameter for final output
    # So check the internal processed state instead
    assert sql._processed_state is not None
    internal_params = sql._processed_state.merged_parameters

    # First param should be wrapped as TypedParameter internally
    assert isinstance(internal_params[0], TypedParameter)
    assert internal_params[0].value == datetime(2024, 1, 1)
    assert internal_params[0].type_hint == "timestamp"

    # Second param (boolean) should be wrapped since it's a special type
    assert isinstance(internal_params[1], TypedParameter)
    assert internal_params[1].value is True
    assert internal_params[1].type_hint == "boolean"

    # The final output params should be unwrapped
    assert params[0] == datetime(2024, 1, 1)
    assert params[1] is True


def test_typed_parameter_type_inference() -> None:
    """Test type inference for various Python types."""

    test_cases = [
        # (value, expected_type_hint, expected_sqlglot_type)
        (None, "null", "NULL"),
        (True, "boolean", "BOOLEAN"),
        (False, "boolean", "BOOLEAN"),
        (42, "integer", "INT"),
        (9999999999, "bigint", "BIGINT"),
        (math.pi, "float", "FLOAT"),
        (Decimal("100.50"), "decimal", "DECIMAL"),
        (date(2024, 1, 1), "date", "DATE"),
        (datetime(2024, 1, 1, 12, 0), "timestamp", "TIMESTAMP"),
        ("hello", "string", "VARCHAR"),
        (b"binary", "binary", "BINARY"),
        ([1, 2, 3], "array", "ARRAY"),
        ({"key": "value"}, "json", "JSON"),
    ]

    for value, expected_hint, expected_type in test_cases:
        wrapped = ParameterConverter.wrap_parameters_with_types({"param": value}, [])
        assert isinstance(wrapped, dict)

        if (
            isinstance(value, (str, int, float))
            and not isinstance(value, bool)
            and (not isinstance(value, int) or abs(value) <= 2147483647)
        ):
            # Simple types are not wrapped
            assert wrapped["param"] == value
        else:
            # Complex types are wrapped
            assert isinstance(wrapped["param"], TypedParameter)
            assert wrapped["param"].value == value
            assert wrapped["param"].type_hint == expected_hint
            assert str(wrapped["param"].sqlglot_type) == expected_type


def test_typed_parameter_performance_optimization() -> None:
    """Test that simple scalar types are not wrapped for performance."""
    params = {"string": "hello", "small_int": 100, "float": math.pi, "big_int": 9999999999, "bool": True}

    wrapped = ParameterConverter.wrap_parameters_with_types(params, [])
    assert isinstance(wrapped, dict)

    # Simple scalars should not be wrapped (except bigint and bool)
    assert wrapped["string"] == "hello"
    assert wrapped["small_int"] == 100
    assert wrapped["float"] == math.pi

    # Bigint should be wrapped
    assert isinstance(wrapped["big_int"], TypedParameter)
    assert wrapped["big_int"].type_hint == "bigint"

    # Boolean is wrapped as it needs special handling
    assert isinstance(wrapped["bool"], TypedParameter)
    assert wrapped["bool"].value is True
    assert wrapped["bool"].type_hint == "boolean"
