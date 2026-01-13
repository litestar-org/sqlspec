"""Unit tests for mock adapter core utilities."""

import sqlite3
from datetime import date, datetime
from decimal import Decimal

import pytest

from sqlspec.adapters.mock.core import (
    apply_driver_features,
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    resolve_rowcount,
)
from sqlspec.core import ParameterStyle, StatementConfig
from sqlspec.exceptions import (
    CheckViolationError,
    ForeignKeyViolationError,
    NotNullViolationError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json


def test_driver_profile_defaults() -> None:
    """Test driver profile has correct default values."""
    assert driver_profile.name == "Mock"
    assert driver_profile.default_style == ParameterStyle.QMARK
    assert ParameterStyle.QMARK in driver_profile.supported_styles
    assert ParameterStyle.NAMED_COLON in driver_profile.supported_styles
    assert driver_profile.has_native_list_expansion is False
    assert driver_profile.json_serializer_strategy == "helper"
    assert driver_profile.default_dialect == "sqlite"


def test_default_statement_config() -> None:
    """Test default statement config is properly initialized."""
    assert default_statement_config.dialect == "sqlite"
    assert default_statement_config.parameter_config.default_parameter_style == ParameterStyle.QMARK


def test_format_identifier_simple() -> None:
    """Test formatting simple identifiers."""
    assert format_identifier("users") == '"users"'
    assert format_identifier("table_name") == '"table_name"'


def test_format_identifier_with_schema() -> None:
    """Test formatting identifiers with schema."""
    result = format_identifier("public.users")
    assert result == '"public"."users"'


def test_format_identifier_with_quotes() -> None:
    """Test formatting identifiers containing quotes."""
    result = format_identifier('table"name')
    assert result == '"table""name"'


def test_format_identifier_empty_raises() -> None:
    """Test formatting empty identifier raises error."""
    with pytest.raises(SQLSpecError, match="Table name must not be empty"):
        format_identifier("")

    with pytest.raises(SQLSpecError, match="Table name must not be empty"):
        format_identifier("   ")


def test_format_identifier_with_dots_edge_case() -> None:
    """Test formatting with multiple dots."""
    result = format_identifier("db.schema.table")
    assert result == '"db"."schema"."table"'


def test_build_insert_statement_basic() -> None:
    """Test building basic INSERT statement."""
    result = build_insert_statement("users", ["id", "name"])
    assert result == 'INSERT INTO "users" ("id", "name") VALUES (?, ?)'


def test_build_insert_statement_single_column() -> None:
    """Test INSERT statement with single column."""
    result = build_insert_statement("counters", ["count"])
    assert result == 'INSERT INTO "counters" ("count") VALUES (?)'


def test_build_insert_statement_many_columns() -> None:
    """Test INSERT statement with many columns."""
    columns = ["col1", "col2", "col3", "col4", "col5"]
    result = build_insert_statement("data", columns)
    assert 'INSERT INTO "data"' in result
    assert '"col1", "col2", "col3", "col4", "col5"' in result
    assert "VALUES (?, ?, ?, ?, ?)" in result


def test_build_insert_statement_with_schema() -> None:
    """Test INSERT statement with schema-qualified table."""
    result = build_insert_statement("public.users", ["id", "name"])
    assert result == 'INSERT INTO "public"."users" ("id", "name") VALUES (?, ?)'


def test_collect_rows_empty() -> None:
    """Test collecting empty result set."""
    data, columns, count = collect_rows([], None)
    assert data == []
    assert columns == []
    assert count == 0


def test_collect_rows_with_data() -> None:
    """Test collecting rows with data."""
    description = [("id",), ("name",)]
    rows = [(1, "Alice"), (2, "Bob")]
    data, columns, count = collect_rows(rows, description)

    assert count == 2
    assert columns == ["id", "name"]
    assert len(data) == 2
    assert data[0] == {"id": 1, "name": "Alice"}
    assert data[1] == {"id": 2, "name": "Bob"}


def test_collect_rows_with_none_values() -> None:
    """Test collecting rows containing None values."""
    description = [("id",), ("value",)]
    rows = [(1, None), (2, "text")]
    data, _columns, count = collect_rows(rows, description)

    assert count == 2
    assert data[0] == {"id": 1, "value": None}
    assert data[1] == {"id": 2, "value": "text"}


def test_resolve_rowcount_with_valid_cursor() -> None:
    """Test resolving rowcount from cursor with rowcount attribute."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE test (id INTEGER)")
    cursor.execute("INSERT INTO test VALUES (1)")

    rowcount = resolve_rowcount(cursor)
    assert rowcount == 1

    conn.close()


def test_resolve_rowcount_negative_value() -> None:
    """Test resolving rowcount when value is negative."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("SELECT 1")

    rowcount = resolve_rowcount(cursor)
    assert rowcount == 0

    conn.close()


def test_resolve_rowcount_no_attribute() -> None:
    """Test resolving rowcount from object without rowcount."""

    class FakeCursor:
        pass

    rowcount = resolve_rowcount(FakeCursor())
    assert rowcount == 0


def test_normalize_execute_parameters_with_tuple() -> None:
    """Test normalizing tuple parameters."""
    result = normalize_execute_parameters((1, "test"))
    assert result == (1, "test")


def test_normalize_execute_parameters_with_list() -> None:
    """Test normalizing list parameters."""
    result = normalize_execute_parameters([1, 2, 3])
    assert result == [1, 2, 3]


def test_normalize_execute_parameters_empty() -> None:
    """Test normalizing empty parameters."""
    result = normalize_execute_parameters(None)
    assert result == ()


def test_normalize_execute_many_parameters_valid() -> None:
    """Test normalizing execute_many parameters."""
    params = [(1, "a"), (2, "b")]
    result = normalize_execute_many_parameters(params)
    assert result == params


def test_normalize_execute_many_parameters_empty_raises() -> None:
    """Test normalizing empty execute_many parameters raises error."""
    with pytest.raises(ValueError, match="execute_many requires parameters"):
        normalize_execute_many_parameters(None)

    with pytest.raises(ValueError, match="execute_many requires parameters"):
        normalize_execute_many_parameters([])


def test_create_mapped_exception_unique_constraint_code() -> None:
    """Test creating exception for unique constraint violation with error code."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE test (id INTEGER UNIQUE)")
        conn.execute("INSERT INTO test VALUES (1)")
        conn.execute("INSERT INTO test VALUES (1)")
    except sqlite3.Error as e:
        result = create_mapped_exception(e)
        assert isinstance(result, UniqueViolationError)
    finally:
        conn.close()


def test_create_mapped_exception_foreign_key_constraint() -> None:
    """Test creating exception for foreign key constraint violation."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE child (id INTEGER, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id))")
        conn.execute("INSERT INTO child VALUES (1, 999)")
    except sqlite3.Error as e:
        result = create_mapped_exception(e)
        assert isinstance(result, ForeignKeyViolationError)
    finally:
        conn.close()


def test_create_mapped_exception_not_null_constraint() -> None:
    """Test creating exception for not null constraint violation."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE test (id INTEGER NOT NULL)")
        conn.execute("INSERT INTO test VALUES (NULL)")
    except sqlite3.Error as e:
        result = create_mapped_exception(e)
        assert isinstance(result, NotNullViolationError)
    finally:
        conn.close()


def test_create_mapped_exception_check_constraint() -> None:
    """Test creating exception for check constraint violation."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE test (id INTEGER CHECK(id > 0))")
        conn.execute("INSERT INTO test VALUES (-1)")
    except sqlite3.Error as e:
        result = create_mapped_exception(e)
        assert isinstance(result, CheckViolationError)
    finally:
        conn.close()


def test_create_mapped_exception_syntax_error() -> None:
    """Test creating exception for SQL syntax error."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("INVALID SQL SYNTAX")
    except sqlite3.Error as e:
        result = create_mapped_exception(e)
        assert isinstance(result, SQLParsingError)
    finally:
        conn.close()


def test_create_mapped_exception_generic_error() -> None:
    """Test creating exception for generic database error."""

    class CustomSQLiteError(sqlite3.Error):
        pass

    error = CustomSQLiteError("Generic error")
    result = create_mapped_exception(error)
    assert isinstance(result, SQLSpecError)
    assert "Generic error" in str(result)


def test_apply_driver_features_defaults() -> None:
    """Test applying driver features with defaults."""
    config = StatementConfig()
    result_config, features = apply_driver_features(config, None)

    assert features["json_serializer"] is to_json
    assert features["json_deserializer"] is from_json
    assert result_config is not None


def test_apply_driver_features_custom_serializers() -> None:
    """Test applying driver features with custom JSON serializers."""

    def custom_serializer(obj: object) -> str:
        return "custom"

    def custom_deserializer(s: str) -> object:
        return {"custom": True}

    config = StatementConfig()
    _result_config, features = apply_driver_features(
        config, {"json_serializer": custom_serializer, "json_deserializer": custom_deserializer}
    )

    assert features["json_serializer"] is custom_serializer
    assert features["json_deserializer"] is custom_deserializer


def test_apply_driver_features_preserves_other_features() -> None:
    """Test that apply_driver_features preserves non-JSON features."""
    config = StatementConfig()
    _result_config, features = apply_driver_features(config, {"custom_feature": "value", "another_feature": 123})

    assert features["custom_feature"] == "value"
    assert features["another_feature"] == 123
    assert "json_serializer" in features
    assert "json_deserializer" in features


def test_driver_profile_type_coercions() -> None:
    """Test that driver profile has correct type coercions."""
    coercions = driver_profile.custom_type_coercions

    assert bool in coercions
    assert datetime in coercions
    assert date in coercions
    assert Decimal in coercions

    bool_converter = coercions[bool]
    assert bool_converter(True) == 1
    assert bool_converter(False) == 0


def test_driver_profile_decimal_conversion() -> None:
    """Test Decimal to string conversion in driver profile."""
    coercions = driver_profile.custom_type_coercions
    decimal_converter = coercions[Decimal]

    result = decimal_converter(Decimal("123.45"))
    assert result == "123.45"
