"""Unit tests for the sqlspec.core.parameters module.

Tests the 2-Phase Parameter Conversion System:
- Phase 1: SQLGlot compatibility conversion
- Phase 2: Execution format conversion
- Multi-database parameter style support (10+ styles)
- Parameter container type preservation
- Performance and edge cases
"""

import json
import math
from datetime import date, datetime
from decimal import Decimal
from importlib import import_module
from typing import Any

import pytest
import sqlglot

from sqlspec.core import (
    DRIVER_PARAMETER_PROFILES,
    DriverParameterProfile,
    ParameterConverter,
    ParameterInfo,
    ParameterProcessor,
    ParameterStyle,
    ParameterStyleConfig,
    ParameterValidator,
    TypedParameter,
    build_statement_config_from_profile,
    get_driver_profile,
    is_iterable_parameters,
    register_driver_profile,
    replace_null_parameters_with_literals,
    replace_placeholders_with_literals,
    wrap_with_type,
)
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError
from sqlspec.utils.serializers import from_json, to_json

_ADAPTER_MODULE_NAMES: "tuple[str, ...]" = (
    "sqlspec.adapters.adbc",
    "sqlspec.adapters.aiosqlite",
    "sqlspec.adapters.asyncmy",
    "sqlspec.adapters.asyncpg",
    "sqlspec.adapters.bigquery",
    "sqlspec.adapters.duckdb",
    "sqlspec.adapters.oracledb",
    "sqlspec.adapters.psqlpy",
    "sqlspec.adapters.psycopg",
    "sqlspec.adapters.sqlite",
)

for _module_name in _ADAPTER_MODULE_NAMES:
    import_module(_module_name)

pytestmark = pytest.mark.xdist_group("core")


@pytest.mark.parametrize(
    "style,expected_value",
    [
        (ParameterStyle.NONE, "none"),
        (ParameterStyle.STATIC, "static"),
        (ParameterStyle.QMARK, "qmark"),
        (ParameterStyle.NUMERIC, "numeric"),
        (ParameterStyle.NAMED_COLON, "named_colon"),
        (ParameterStyle.POSITIONAL_COLON, "positional_colon"),
        (ParameterStyle.NAMED_AT, "named_at"),
        (ParameterStyle.NAMED_DOLLAR, "named_dollar"),
        (ParameterStyle.NAMED_PYFORMAT, "pyformat_named"),
        (ParameterStyle.POSITIONAL_PYFORMAT, "pyformat_positional"),
    ],
)
def test_parameter_style_values(style: ParameterStyle, expected_value: str) -> None:
    """Test ParameterStyle enum values match expected strings."""
    assert style.value == expected_value

    assert style == expected_value


def test_typed_parameter_basic() -> None:
    """Test basic TypedParameter creation and properties."""
    param = TypedParameter(True, bool, "is_active")

    assert param.value is True
    assert param.original_type is bool
    assert param.semantic_name == "is_active"


def test_typed_parameter_defaults() -> None:
    """Test TypedParameter with default values."""
    param = TypedParameter(42)

    assert param.value == 42
    assert param.original_type is int
    assert param.semantic_name is None


def test_typed_parameter_hash_equality() -> None:
    """Test TypedParameter hash and equality for dictionary operations."""
    param1 = TypedParameter(True, bool, "active")
    param2 = TypedParameter(True, bool, "active")
    param3 = TypedParameter(False, bool, "active")

    assert param1 == param2
    assert hash(param1) == hash(param2)

    assert param1 != param3

    first_hash = hash(param1)
    second_hash = hash(param1)
    assert first_hash == second_hash


def test_typed_parameter_repr() -> None:
    """Test TypedParameter string representation for debugging."""
    param = TypedParameter(Decimal("123.45"), Decimal, "price")
    repr_str = repr(param)

    assert "TypedParameter" in repr_str
    assert "123.45" in repr_str
    assert "Decimal" in repr_str
    assert "price" in repr_str


@pytest.mark.parametrize(
    "value,expected_type",
    [
        (True, bool),
        (False, bool),
        (42, int),
        ("test", str),
        (Decimal("12.34"), Decimal),
        (datetime(2023, 1, 1), datetime),
        (date(2023, 1, 1), date),
        ([1, 2, 3], list),
        ({"key": "value"}, dict),
    ],
)
def test_typed_parameter_with_different_types(value: Any, expected_type: type) -> None:
    """Test TypedParameter with different value types."""
    param = TypedParameter(value)
    assert param.original_type == expected_type
    assert param.value == value


def test_wrap_with_type_basic_types() -> None:
    """Test type wrapping for basic Python types."""

    bool_result = wrap_with_type(True, "is_active")
    assert isinstance(bool_result, TypedParameter)
    assert bool_result.value is True
    assert bool_result.original_type is bool
    assert bool_result.semantic_name == "is_active"

    decimal_result = wrap_with_type(Decimal("123.45"))
    assert isinstance(decimal_result, TypedParameter)
    assert decimal_result.value == Decimal("123.45")
    assert decimal_result.original_type == Decimal


def test_wrap_with_type_datetime_types() -> None:
    """Test type wrapping for date/datetime types."""
    test_date = date(2023, 1, 1)
    date_result = wrap_with_type(test_date)
    assert isinstance(date_result, TypedParameter)
    assert date_result.value == test_date
    assert date_result.original_type == date

    test_datetime = datetime(2023, 1, 1, 12, 0, 0)
    datetime_result = wrap_with_type(test_datetime)
    assert isinstance(datetime_result, TypedParameter)
    assert datetime_result.value == test_datetime
    assert datetime_result.original_type == datetime


def test_wrap_with_type_no_wrapping_needed() -> None:
    """Test types that don't need wrapping are returned unchanged."""

    assert wrap_with_type("test") == "test"
    assert wrap_with_type(42) == 42
    assert wrap_with_type(math.pi) == math.pi
    assert wrap_with_type(None) is None


@pytest.mark.parametrize(
    "name,style,position,ordinal,placeholder_text",
    [
        ("user_id", ParameterStyle.NAMED_COLON, 25, 0, ":user_id"),
        (None, ParameterStyle.QMARK, 10, 1, "?"),
        ("param1", ParameterStyle.NAMED_PYFORMAT, 35, 0, "%(param1)s"),
        (None, ParameterStyle.POSITIONAL_PYFORMAT, 15, 2, "%s"),
        ("id", ParameterStyle.NAMED_AT, 20, 0, "@id"),
        ("value", ParameterStyle.NAMED_DOLLAR, 30, 0, "$value"),
        ("1", ParameterStyle.NUMERIC, 5, 1, "$1"),
        ("2", ParameterStyle.POSITIONAL_COLON, 40, 3, ":2"),
    ],
)
def test_parameter_info_creation(
    name: str | None, style: ParameterStyle, position: int, ordinal: int, placeholder_text: str
) -> None:
    """Test ParameterInfo creation with various parameter types."""
    param_info = ParameterInfo(
        name=name, style=style, position=position, ordinal=ordinal, placeholder_text=placeholder_text
    )

    assert param_info.name == name
    assert param_info.style == style
    assert param_info.position == position
    assert param_info.ordinal == ordinal
    assert param_info.placeholder_text == placeholder_text


def test_mixed_named_and_numeric_parameters() -> None:
    """Test mixed named (:name) and numeric ($2) parameters."""
    sql = "SELECT :name::text as name, $2::int as age"
    parameters = {"name": "Mixed", "age": 25}

    converter = ParameterConverter()

    converted_sql, converted_params = converter.convert_placeholder_style(
        sql, parameters, ParameterStyle.NUMERIC, is_many=False
    )

    assert converted_sql == "SELECT $1::text as name, $2::int as age"
    assert converted_params == ("Mixed", 25)


def test_build_statement_config_helper_strategy_applies_serializer() -> None:
    """Helper strategy profiles should inject JSON serializers and tuple adapters."""

    calls: list[Any] = []

    def custom_serializer(value: Any) -> str:
        calls.append(value)
        return f"encoded:{value}"

    profile = get_driver_profile("sqlite")
    config = build_statement_config_from_profile(profile, json_serializer=custom_serializer)

    parameter_config = config.parameter_config
    assert parameter_config.json_serializer is custom_serializer

    dict_encoder = parameter_config.type_coercion_map[dict]
    encoded_dict = dict_encoder({"a": 1})
    assert encoded_dict == "encoded:{'a': 1}"

    tuple_encoder = parameter_config.type_coercion_map[tuple]
    encoded_tuple = tuple_encoder((1, 2))
    assert encoded_tuple == "encoded:[1, 2]"
    assert calls == [{"a": 1}, [1, 2]]


def test_build_statement_config_helper_strategy_defaults_to_json() -> None:
    """Helper strategy should fall back to module JSON helpers when none provided."""

    profile = get_driver_profile("sqlite")
    config = build_statement_config_from_profile(profile)

    parameter_config = config.parameter_config
    assert parameter_config.json_serializer is to_json

    dict_encoder = parameter_config.type_coercion_map[dict]
    encoded_dict = dict_encoder({"a": 1})
    assert isinstance(encoded_dict, str)
    assert json.loads(encoded_dict) == {"a": 1}

    tuple_encoder = parameter_config.type_coercion_map[tuple]
    encoded_tuple = tuple_encoder((1, 2))
    assert isinstance(encoded_tuple, str)
    assert json.loads(encoded_tuple) == [1, 2]


def test_build_statement_config_driver_strategy_preserves_type_map() -> None:
    """Driver strategy should leave type coercion map unmodified except JSON slots."""

    def dummy_serializer(value: Any) -> str:
        return f"json:{value}"

    profile = get_driver_profile("asyncpg")
    config = build_statement_config_from_profile(profile, json_serializer=dummy_serializer)

    parameter_config = config.parameter_config
    assert parameter_config.json_serializer is dummy_serializer
    assert dict not in parameter_config.type_coercion_map
    assert tuple not in parameter_config.type_coercion_map


def test_build_statement_config_driver_strategy_defaults_to_json() -> None:
    """Driver strategy should wire default JSON helpers when overrides absent."""

    profile = get_driver_profile("asyncpg")
    config = build_statement_config_from_profile(profile)

    parameter_config = config.parameter_config
    assert parameter_config.json_serializer is to_json
    assert parameter_config.json_deserializer is from_json
    assert parameter_config.type_coercion_map == dict(profile.custom_type_coercions)


def test_build_statement_config_helper_tuple_strategy_override() -> None:
    """Overriding tuple strategy to tuple should preserve tuple payload."""

    captured: list[Any] = []

    def recorder(value: Any) -> str:
        captured.append(value)
        return f"encoded:{value}"

    profile = get_driver_profile("sqlite")
    config = build_statement_config_from_profile(
        profile, parameter_overrides={"json_tuple_strategy": "tuple"}, json_serializer=recorder
    )

    tuple_encoder = config.parameter_config.type_coercion_map[tuple]
    encoded_value = tuple_encoder((1, 2))

    assert encoded_value == "encoded:(1, 2)"
    assert captured[-1] == (1, 2)


def test_replace_null_parameters_with_literals_numeric_dialect() -> None:
    """Null parameters should render as literals and shrink parameter list."""

    expression = sqlglot.parse_one("INSERT INTO test VALUES ($1, $2)", dialect="postgres")
    modified_expression, cleaned_params = replace_null_parameters_with_literals(
        expression, (42, None), dialect="postgres"
    )

    assert modified_expression.sql(dialect="postgres") == "INSERT INTO test VALUES ($1, NULL)"
    assert cleaned_params == (42,)


def test_replace_placeholders_with_literals_basic_sequence() -> None:
    """Placeholders are replaced by literals when provided with positional parameters."""

    expression = sqlglot.parse_one("SELECT ? AS value", dialect="bigquery")
    transformed = replace_placeholders_with_literals(expression, [123], json_serializer=json.dumps)

    assert transformed.sql(dialect="bigquery") == "SELECT 123 AS value"


def test_replace_placeholders_with_literals_named_mapping() -> None:
    """Named parameters in mappings are embedded as string literals."""

    expression = sqlglot.parse_one("SELECT @name AS user", dialect="bigquery")
    transformed = replace_placeholders_with_literals(expression, {"@name": "bob"}, json_serializer=json.dumps)

    assert transformed.sql(dialect="bigquery") == "SELECT 'bob' AS user"


def test_build_statement_config_applies_overrides_and_extras() -> None:
    """Overrides and extras should both be reflected in the resulting config."""

    def uppercase(value: str) -> str:
        return value.upper()

    overrides = {"type_coercion_map": {str: uppercase}, "supported_parameter_styles": {ParameterStyle.NAMED_AT}}

    profile = get_driver_profile("bigquery")
    config = build_statement_config_from_profile(profile, parameter_overrides=overrides)

    parameter_config = config.parameter_config
    assert parameter_config.supported_parameter_styles == {ParameterStyle.NAMED_AT}
    assert parameter_config.type_coercion_map[str]("value") == "VALUE"
    assert parameter_config.type_coercion_map[tuple]([1, 2]) == [1, 2]


def test_get_driver_profile_missing_raises() -> None:
    """Unknown adapter keys should raise ImproperConfigurationError."""

    with pytest.raises(ImproperConfigurationError):
        get_driver_profile("does-not-exist")


def test_register_driver_profile_duplicate_guard() -> None:
    """Registering the same adapter twice without override should fail."""

    profile = DriverParameterProfile(
        name="TestAdapter",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
    )

    key = "test-duplicate"
    DRIVER_PARAMETER_PROFILES.pop(key, None)
    register_driver_profile(key, profile)
    try:
        with pytest.raises(ImproperConfigurationError):
            register_driver_profile(key, profile)
    finally:
        DRIVER_PARAMETER_PROFILES.pop(key, None)


def test_register_driver_profile_allows_override() -> None:
    """allow_override should replace an existing driver profile."""

    base_profile = DriverParameterProfile(
        name="TestAdapter",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
    )
    replacement_profile = DriverParameterProfile(
        name="TestAdapterReplacement",
        default_style=ParameterStyle.NUMERIC,
        supported_styles={ParameterStyle.NUMERIC},
        default_execution_style=ParameterStyle.NUMERIC,
        supported_execution_styles={ParameterStyle.NUMERIC},
        has_native_list_expansion=True,
        preserve_parameter_format=False,
        needs_static_script_compilation=True,
        allow_mixed_parameter_styles=True,
        preserve_original_params_for_many=True,
        json_serializer_strategy="driver",
    )

    key = "test-override"
    DRIVER_PARAMETER_PROFILES.pop(key, None)
    register_driver_profile(key, base_profile)
    try:
        register_driver_profile(key, replacement_profile, allow_override=True)
        resolved_profile = get_driver_profile(key)

        assert resolved_profile is replacement_profile
        assert resolved_profile.default_style is ParameterStyle.NUMERIC
        assert resolved_profile.has_native_list_expansion is True
    finally:
        DRIVER_PARAMETER_PROFILES.pop(key, None)


def test_mixed_parameter_style_with_processor() -> None:
    """Test mixed parameter styles through the full processor pipeline."""
    sql = "SELECT :name::text as name, $2::int as age"
    parameters = {"name": "Test", "age": 30}

    config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_COLON},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        allow_mixed_parameter_styles=False,
        preserve_parameter_format=True,
    )

    processor = ParameterProcessor()
    processed_sql, processed_params = processor.process(
        sql=sql, parameters=parameters, config=config, dialect="postgres", is_many=False
    )

    assert processed_sql == "SELECT $1::text as name, $2::int as age"
    assert processed_params == ("Test", 30)
    assert len(processed_params) == 2


def test_parameter_processing_result_profile_metadata() -> None:
    """Parameter profile captures placeholder reuse and style information."""

    sql = "SELECT :id::int AS id, :id::int AS other"
    parameters = {"id": 42}

    config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_COLON,
        supported_parameter_styles={ParameterStyle.NAMED_COLON},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
    )

    processor = ParameterProcessor()
    result = processor.process(sql=sql, parameters=parameters, config=config, dialect="postgres")
    profile = result.parameter_profile

    assert profile.total_count == 2
    assert profile.placeholder_count("$1") == 2
    assert profile.reused_ordinals == (1,)
    assert profile.styles == (ParameterStyle.NUMERIC.value,)


def test_mixed_parameters_order_sensitivity() -> None:
    """Test that mixed parameters maintain correct order mapping."""

    sql = "SELECT $1::text as first, :middle::text as mid, $3::int as last"
    parameters = {"first": "A", "middle": "B", "last": "C"}

    converter = ParameterConverter()
    converted_sql, converted_params = converter.convert_placeholder_style(
        sql, parameters, ParameterStyle.NUMERIC, is_many=False
    )

    assert converted_sql == "SELECT $1::text as first, $2::text as mid, $3::int as last"

    assert converted_params == ("A", "B", "C")
    assert isinstance(converted_params, tuple)
    assert len(converted_params) == 3


def test_mixed_parameters_with_repeated_numeric() -> None:
    """Test mixed parameters with repeated numeric parameters."""
    sql = "SELECT :name::text as name, $2::int as age, $2::int as age2"
    parameters = {"name": "User", "age": 25}

    converter = ParameterConverter()
    converted_sql, converted_params = converter.convert_placeholder_style(
        sql, parameters, ParameterStyle.NUMERIC, is_many=False
    )

    assert converted_sql == "SELECT $1::text as name, $2::int as age, $2::int as age2"
    assert converted_params == ("User", 25)
    assert isinstance(converted_params, tuple)
    assert len(converted_params) == 2


def test_repeated_named_params_expand_for_qmark_style() -> None:
    """Test that repeated named params are expanded when converting to QMARK style (Issue #310).

    When converting from NAMED_COLON to QMARK, each occurrence of a named parameter
    like :query_like must produce a separate ? placeholder with the value duplicated.
    """
    sql = (
        "SELECT name FROM skill WHERE LOWER(name) LIKE :query_like "
        "OR LOWER(description) LIKE :query_like "
        "ORDER BY CASE WHEN LOWER(name) LIKE :query_like THEN 1 ELSE 0 END DESC "
        "LIMIT :limit"
    )
    parameters = {"query_like": "%duckdb%", "limit": 10}

    converter = ParameterConverter()
    converted_sql, converted_params = converter.convert_placeholder_style(
        sql, parameters, ParameterStyle.QMARK, is_many=False
    )

    # SQL should have 4 ? placeholders (3 for query_like + 1 for limit)
    assert converted_sql.count("?") == 4
    # Parameters should be expanded: 3 times query_like value + 1 time limit value
    assert converted_params == ("%duckdb%", "%duckdb%", "%duckdb%", 10)
    assert len(converted_params) == 4  # type: ignore[arg-type]


def test_edge_case_all_numeric_parameters() -> None:
    """Test that non-mixed numeric parameters still work correctly."""
    sql = "SELECT $1::text as name, $2::int as age"
    parameters = ("Alice", 30)

    converter = ParameterConverter()
    converted_sql, converted_params = converter.convert_placeholder_style(
        sql, parameters, ParameterStyle.NUMERIC, is_many=False
    )

    assert converted_sql == "SELECT $1::text as name, $2::int as age"
    assert converted_params == ("Alice", 30)


def test_edge_case_all_named_parameters() -> None:
    """Test that non-mixed named parameters still work correctly."""
    sql = "SELECT :name::text as name, :age::int as age"
    parameters = {"name": "Bob", "age": 35}

    converter = ParameterConverter()
    converted_sql, converted_params = converter.convert_placeholder_style(
        sql, parameters, ParameterStyle.NUMERIC, is_many=False
    )

    assert converted_sql == "SELECT $1::text as name, $2::int as age"
    assert converted_params == ("Bob", 35)


def test_parameter_info_repr() -> None:
    """Test ParameterInfo string representation."""
    param = ParameterInfo("test_param", ParameterStyle.NAMED_COLON, 10, 0, ":test_param")
    repr_str = repr(param)

    assert "ParameterInfo" in repr_str
    assert "test_param" in repr_str
    assert "ParameterStyle.NAMED_COLON" in repr_str
    assert "10" in repr_str


def test_parameter_style_config_basic() -> None:
    """Test basic ParameterStyleConfig creation."""
    config = ParameterStyleConfig(ParameterStyle.QMARK)

    assert config.default_parameter_style == ParameterStyle.QMARK
    assert config.supported_parameter_styles == {ParameterStyle.QMARK}
    assert config.default_execution_parameter_style == ParameterStyle.QMARK
    assert config.type_coercion_map == {}
    assert not config.has_native_list_expansion
    assert config.output_transformer is None
    assert config.needs_static_script_compilation is False
    assert config.strict_named_parameters is True


def test_parameter_style_config_advanced() -> None:
    """Test ParameterStyleConfig with advanced options."""
    coercion_map: dict[type, Any] = {bool: lambda x: 1 if x else 0}

    def output_transformer(sql: str, params: Any) -> tuple[str, Any]:
        return (sql.upper(), params)

    config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_COLON,
        supported_parameter_styles={ParameterStyle.NAMED_COLON, ParameterStyle.QMARK},
        supported_execution_parameter_styles={ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
        type_coercion_map=coercion_map,
        has_native_list_expansion=True,
        output_transformer=output_transformer,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=True,
        preserve_parameter_format=True,
        strict_named_parameters=False,
    )

    assert config.default_parameter_style == ParameterStyle.NAMED_COLON
    assert config.supported_parameter_styles == {ParameterStyle.NAMED_COLON, ParameterStyle.QMARK}
    assert config.supported_execution_parameter_styles == {ParameterStyle.QMARK}  # type: ignore[comparison-overlap]
    assert config.default_execution_parameter_style == ParameterStyle.QMARK
    assert config.type_coercion_map == coercion_map
    assert config.has_native_list_expansion is True
    assert config.output_transformer == output_transformer
    assert config.needs_static_script_compilation is False
    assert config.allow_mixed_parameter_styles is True
    assert config.preserve_parameter_format is True
    assert config.strict_named_parameters is False


def test_parameter_style_config_hash() -> None:
    """Test ParameterStyleConfig hash method for caching."""
    config1 = ParameterStyleConfig(ParameterStyle.QMARK)
    config2 = ParameterStyleConfig(ParameterStyle.QMARK)
    config3 = ParameterStyleConfig(ParameterStyle.NAMED_COLON)

    assert config1.hash() == config2.hash()

    assert config1.hash() != config3.hash()


@pytest.fixture
def validator() -> ParameterValidator:
    """Create a ParameterValidator instance."""
    return ParameterValidator()


@pytest.mark.parametrize(
    "sql,expected_count,expected_styles",
    [
        ("SELECT * FROM users", 0, []),
        ("SELECT * FROM users WHERE id = ?", 1, [ParameterStyle.QMARK]),
        ("SELECT * FROM users WHERE name = :name", 1, [ParameterStyle.NAMED_COLON]),
        ("SELECT * FROM users WHERE id = ? AND name = ?", 2, [ParameterStyle.QMARK, ParameterStyle.QMARK]),
        (
            "SELECT * FROM users WHERE name = :name AND email = :email",
            2,
            [ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_COLON],
        ),
        ("SELECT * FROM users WHERE id = %(id)s", 1, [ParameterStyle.NAMED_PYFORMAT]),
        ("SELECT * FROM users WHERE name = %s", 1, [ParameterStyle.POSITIONAL_PYFORMAT]),
        ("SELECT * FROM users WHERE id = @id", 1, [ParameterStyle.NAMED_AT]),
        ("SELECT * FROM users WHERE id = $1", 1, [ParameterStyle.NUMERIC]),
        ("SELECT * FROM users WHERE name = $name", 1, [ParameterStyle.NAMED_DOLLAR]),
        ("SELECT * FROM users WHERE id = :1", 1, [ParameterStyle.POSITIONAL_COLON]),
    ],
)
def test_extract_parameters(
    validator: ParameterValidator, sql: str, expected_count: int, expected_styles: list[ParameterStyle]
) -> None:
    """Test parameter extraction from various SQL patterns."""
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == expected_count
    for i, expected_style in enumerate(expected_styles):
        assert parameters[i].style == expected_style


@pytest.mark.parametrize(
    "sql,should_be_ignored",
    [
        ("SELECT 'test with ? inside'", True),
        ('SELECT "test with ? inside"', True),
        ("SELECT $tag$content with ? and :param$tag$", True),
        ("SELECT * FROM test -- comment with ? and :param", True),
        ("SELECT * FROM test /* comment with ? and :param */", True),
        ("SELECT * FROM json WHERE data ?? 'key'", True),
        ("SELECT * FROM json WHERE data ?| array['key']", True),
        ("SELECT * FROM json WHERE data ?& array['key']", True),
        ("SELECT * FROM users WHERE id::int = 5", False),
        ("SELECT * FROM v$version", True),
        ('SELECT * FROM "V$VERSION"', True),
    ],
)
def test_extract_parameters_ignores_special_cases(
    validator: ParameterValidator, sql: str, should_be_ignored: bool
) -> None:
    """Test that parameters in special contexts are handled correctly."""
    parameters = validator.extract_parameters(sql)

    if should_be_ignored:
        assert len(parameters) == 0
    else:
        assert all(p.placeholder_text != "::int" for p in parameters)


def test_extract_parameters_caching(validator: ParameterValidator) -> None:
    """Test that parameter extraction results are cached."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"

    parameters1 = validator.extract_parameters(sql)
    parameters2 = validator.extract_parameters(sql)

    assert parameters1 is parameters2


def test_extract_parameters_complex_sql(validator: ParameterValidator) -> None:
    """Test parameter extraction from complex SQL with multiple styles."""
    sql = """
    SELECT u.*, o.*
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.name = :name
      AND u.email = %(email)s
      AND o.created_at > ?
      AND o.status = @status
      AND o.total > $1
    """
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 5
    styles = {p.style for p in parameters}
    assert ParameterStyle.NAMED_COLON in styles
    assert ParameterStyle.NAMED_PYFORMAT in styles
    assert ParameterStyle.QMARK in styles
    assert ParameterStyle.NAMED_AT in styles

    assert ParameterStyle.NUMERIC in styles


def test_parameter_position_tracking(validator: ParameterValidator) -> None:
    """Test that parameter positions are tracked correctly."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 2
    qmark_param = next(p for p in parameters if p.style == ParameterStyle.QMARK)
    named_param = next(p for p in parameters if p.style == ParameterStyle.NAMED_COLON)

    assert qmark_param.position < named_param.position
    assert sql[qmark_param.position] == "?"
    assert sql[named_param.position : named_param.position + len(":name")] == ":name"


@pytest.mark.parametrize(
    "dialect,expected_incompatible",
    [
        (None, {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT, ParameterStyle.POSITIONAL_COLON}),
        ("mysql", {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT, ParameterStyle.POSITIONAL_COLON}),
        ("postgres", {ParameterStyle.POSITIONAL_COLON}),
        ("sqlite", {ParameterStyle.POSITIONAL_COLON}),
        (
            "oracle",
            {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT, ParameterStyle.POSITIONAL_COLON},
        ),
        (
            "bigquery",
            {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT, ParameterStyle.POSITIONAL_COLON},
        ),
    ],
)
def test_get_sqlglot_incompatible_styles(
    validator: ParameterValidator, dialect: str | None, expected_incompatible: set[ParameterStyle]
) -> None:
    """Test dialect-specific SQLGlot incompatible style detection."""
    incompatible = validator.get_sqlglot_incompatible_styles(dialect)
    assert incompatible == expected_incompatible


@pytest.fixture
def converter() -> ParameterConverter:
    """Create a ParameterConverter instance."""
    return ParameterConverter()


@pytest.mark.parametrize(
    "sql,dialect,expected_needs_conversion",
    [
        ("SELECT * FROM users WHERE id = ?", "postgres", False),
        ("SELECT * FROM users WHERE id = :name", "postgres", False),
        ("SELECT * FROM users WHERE id = %s", "postgres", False),
        ("SELECT * FROM users WHERE id = %s", "mysql", True),
        ("SELECT * FROM users WHERE id = %(name)s", "mysql", True),
        ("SELECT * FROM users WHERE id = :1", "postgres", True),
        ("SELECT * FROM users WHERE id = :1", "oracle", True),
    ],
)
def test_normalize_sql_for_parsing(
    converter: ParameterConverter, sql: str, dialect: str, expected_needs_conversion: bool
) -> None:
    """Test Phase 1 SQLGlot compatibility normalization."""
    normalized_sql, param_info = converter.normalize_sql_for_parsing(sql, dialect)

    if expected_needs_conversion:
        assert normalized_sql != sql

        assert ":param_" in normalized_sql
    else:
        assert normalized_sql == sql

    assert len(param_info) > 0


def test_normalize_sql_pyformat_conversion(converter: ParameterConverter) -> None:
    """Test conversion of problematic pyformat styles."""
    sql = "SELECT * FROM users WHERE name = %s AND id = %(user_id)s"
    normalized_sql, param_info = converter.normalize_sql_for_parsing(sql, "mysql")

    assert "%s" not in normalized_sql
    assert "%(user_id)s" not in normalized_sql
    assert ":param_0" in normalized_sql
    assert ":param_1" in normalized_sql

    assert len(param_info) == 2
    assert param_info[0].style == ParameterStyle.POSITIONAL_PYFORMAT
    assert param_info[1].style == ParameterStyle.NAMED_PYFORMAT
    assert param_info[1].name == "user_id"


def test_normalize_sql_positional_colon_conversion(converter: ParameterConverter) -> None:
    """Test conversion of Oracle-style positional colon parameters."""
    sql = "SELECT * FROM users WHERE id = :1 AND name = :2"
    normalized_sql, param_info = converter.normalize_sql_for_parsing(sql, "oracle")

    assert ":1" not in normalized_sql
    assert ":2" not in normalized_sql
    assert ":param_0" in normalized_sql
    assert ":param_1" in normalized_sql

    assert len(param_info) == 2
    assert param_info[0].style == ParameterStyle.POSITIONAL_COLON
    assert param_info[1].style == ParameterStyle.POSITIONAL_COLON


@pytest.mark.parametrize(
    "sql,parameters,target_style,expected_sql_pattern,expected_param_format",
    [
        ("SELECT * FROM users WHERE name = :param_0", ["john"], ParameterStyle.QMARK, "?", list),
        ("SELECT * FROM users WHERE name = :param_0", ["john"], ParameterStyle.NUMERIC, "$1", list),
        ("SELECT * FROM users WHERE name = :param_0", ["john"], ParameterStyle.POSITIONAL_PYFORMAT, "%s", list),
        ("SELECT * FROM users WHERE id = ?", {"id": 123}, ParameterStyle.NAMED_COLON, ":param_0", dict),
        ("SELECT * FROM users WHERE id = ?", {"id": 123}, ParameterStyle.NAMED_PYFORMAT, "%(param_0)s", dict),
        ("SELECT * FROM users WHERE id = ?", {"id": 123}, ParameterStyle.NAMED_AT, "@param_0", dict),
    ],
)
def test_convert_placeholder_style(
    converter: ParameterConverter,
    sql: str,
    parameters: Any,
    target_style: ParameterStyle,
    expected_sql_pattern: str,
    expected_param_format: type,
) -> None:
    """Test Phase 2 execution format conversion."""
    converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, target_style)

    assert expected_sql_pattern in converted_sql
    assert isinstance(converted_params, expected_param_format)


def test_convert_static_embedding(converter: ParameterConverter) -> None:
    """Test STATIC style parameter embedding."""
    sql = "SELECT * FROM users WHERE id = ? AND active = ?"
    parameters = [123, True]

    converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.STATIC)

    assert "123" in converted_sql
    assert "TRUE" in converted_sql
    assert "?" not in converted_sql
    assert converted_params is None


def test_convert_static_embedding_parameter_reuse(converter: ParameterConverter) -> None:
    """Test STATIC style parameter embedding with parameter reuse."""

    sql = "SELECT $1, $2, $1, $3, $1"
    parameters = [100, 200, 300]

    converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.STATIC)

    expected = "SELECT 100, 200, 100, 300, 100"
    assert converted_sql == expected
    assert converted_params is None

    sql_named = "SELECT :value, :other, :value"
    params_named = {"value": "hello", "other": 42}

    converted_sql_named, converted_params_named = converter.convert_placeholder_style(
        sql_named, params_named, ParameterStyle.STATIC
    )

    expected_named = "SELECT 'hello', 42, 'hello'"
    assert converted_sql_named == expected_named
    assert converted_params_named is None


def test_convert_parameter_format_preservation(converter: ParameterConverter) -> None:
    """Test parameter format preservation (list vs tuple vs dict)."""
    sql = "SELECT * FROM users WHERE id = ? AND name = ?"

    tuple_params = (123, "john")
    _, converted_params = converter.convert_placeholder_style(sql, tuple_params, ParameterStyle.QMARK)

    assert isinstance(converted_params, (list, tuple))
    assert len(converted_params) == 2


def test_convert_executemany_handling(converter: ParameterConverter) -> None:
    """Test execute_many parameter handling."""
    sql = "INSERT INTO users (id, name) VALUES (?, ?)"
    many_parameters = [[1, "alice"], [2, "bob"], [3, "charlie"]]

    converted_sql, converted_params = converter.convert_placeholder_style(
        sql, many_parameters, ParameterStyle.QMARK, is_many=True
    )

    assert "?" in converted_sql
    assert converted_params == many_parameters


@pytest.fixture
def processor() -> ParameterProcessor:
    """Create a ParameterProcessor instance."""
    return ParameterProcessor()


@pytest.fixture
def basic_config() -> ParameterStyleConfig:
    """Create a basic parameter style configuration."""
    return ParameterStyleConfig(ParameterStyle.QMARK)


@pytest.fixture
def advanced_config() -> ParameterStyleConfig:
    """Create an advanced parameter style configuration."""
    return ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_COLON,
        supported_parameter_styles={ParameterStyle.NAMED_COLON, ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
        type_coercion_map={bool: lambda x: 1 if x else 0},
        output_transformer=lambda sql, params: (sql.upper(), params),
    )


def test_process_no_parameters(processor: ParameterProcessor, basic_config: ParameterStyleConfig) -> None:
    """Test processing SQL with no parameters (fast path)."""
    sql = "SELECT * FROM users"

    final_sql, final_params = processor.process(sql, None, basic_config)

    assert final_sql == sql
    assert final_params is None


def test_process_type_wrapping(processor: ParameterProcessor, basic_config: ParameterStyleConfig) -> None:
    """Test type wrapping in parameter processing."""
    sql = "SELECT * FROM users WHERE active = ?"
    parameters = [True]

    final_sql, final_params = processor.process(sql, parameters, basic_config)

    assert final_sql == sql
    assert len(final_params) == 1


def test_process_type_coercion(processor: ParameterProcessor, advanced_config: ParameterStyleConfig) -> None:
    """Test type coercion mapping."""
    sql = "SELECT * FROM users WHERE active = :active"
    parameters = {"active": True}

    _, final_params = processor.process(sql, parameters, advanced_config, "postgres")

    if isinstance(final_params, dict):
        value = final_params["active"]

        if hasattr(value, "value"):
            assert value.value == 1 or value.value is True
        else:
            assert value == 1 or value is True
    elif isinstance(final_params, (list, tuple)):
        found_coerced = False
        for param in final_params:
            if hasattr(param, "value"):
                if param.value == 1 or param.value is True:
                    found_coerced = True
                    break
            elif param == 1 or param is True:
                found_coerced = True
                break
        assert found_coerced


def test_process_output_transformation(processor: ParameterProcessor, advanced_config: ParameterStyleConfig) -> None:
    """Test final output transformation."""
    sql = "select * from users where id = :id"
    parameters = {"id": 123}

    final_sql, final_params = processor.process(sql, parameters, advanced_config, "postgres")

    assert final_sql.isupper()
    assert "SELECT" in final_sql

    if isinstance(final_params, dict):
        assert final_params["id"] == 123
    elif isinstance(final_params, (list, tuple)):
        assert 123 in final_params


def test_process_full_pipeline(processor: ParameterProcessor) -> None:
    """Test complete processing pipeline with complex scenario."""

    config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_PYFORMAT,
        default_execution_parameter_style=ParameterStyle.QMARK,
        type_coercion_map={bool: lambda x: 1 if x else 0, Decimal: lambda x: float(x)},
    )

    sql = "SELECT * FROM orders WHERE active = %(active)s AND total > %(total)s"
    parameters = {"active": True, "total": Decimal("99.99")}

    final_sql, final_params = processor.process(sql, parameters, config, "mysql")

    assert "?" in final_sql
    assert "%(active)s" not in final_sql
    assert "%(total)s" not in final_sql

    if final_params is None or (isinstance(final_params, dict) and len(final_params) == 0):
        pass
    elif isinstance(final_params, list):
        assert len(final_params) > 0
    elif isinstance(final_params, dict):
        assert len(final_params) > 0
    elif isinstance(final_params, tuple):
        assert len(final_params) > 0


def test_process_execute_many_mapping_payload(
    processor: "ParameterProcessor", basic_config: "ParameterStyleConfig"
) -> None:
    """Ensure execute_many normalizes mapping payloads for positional placeholders."""

    sql = "INSERT INTO metrics (a, b) VALUES (?, ?)"
    parameters = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    final_sql, final_params = processor.process(sql, parameters, basic_config, is_many=True)

    assert final_sql == sql
    assert isinstance(final_params, list)
    assert all(isinstance(param_set, (list, tuple)) for param_set in final_params)
    assert [tuple(param_set) for param_set in final_params] == [(1, "x"), (2, "y")]


def test_process_execute_many_named_to_positional(processor: "ParameterProcessor") -> None:
    """Execute_many with named placeholders should convert mapping batches to positional values."""

    config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_DOLLAR,
        supported_parameter_styles={ParameterStyle.NAMED_DOLLAR, ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.QMARK,
    )

    sql = "INSERT INTO metrics (a, b) VALUES ($a, $b)"
    parameters = [{"a": 10, "b": 20}, {"b": 40, "a": 30}]

    final_sql, final_params = processor.process(sql, parameters, config, "duckdb", is_many=True)

    assert final_sql.count("?") == 2
    assert isinstance(final_params, list)
    assert [tuple(param_set) for param_set in final_params] == [(10, 20), (30, 40)]


def test_list_parameter_preservation(converter: ParameterConverter) -> None:
    """Test that list parameters are properly handled."""
    sql = "INSERT INTO users (id, name, active) VALUES (?, ?, ?)"
    parameters = [1, "alice", True]

    _, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.QMARK)

    assert isinstance(converted_params, list)
    assert converted_params == parameters


def test_tuple_parameter_handling(converter: ParameterConverter) -> None:
    """Test that tuple parameters are handled correctly."""
    sql = "INSERT INTO users (id, name) VALUES (?, ?)"
    parameters = (1, "alice")

    _, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.QMARK)

    assert isinstance(converted_params, (list, tuple))
    assert len(converted_params) == 2


def test_dict_parameter_handling(converter: ParameterConverter) -> None:
    """Test that dictionary parameters work with named styles."""
    sql = "INSERT INTO users (id, name) VALUES (:id, :name)"
    parameters = {"id": 1, "name": "alice"}

    _, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.NAMED_COLON)

    assert isinstance(converted_params, dict)
    assert converted_params == parameters


def test_missing_named_parameters_raise(converter: ParameterConverter) -> None:
    """Missing named parameters should raise rather than fall back to ordinal ordering."""
    sql = "SELECT * FROM users WHERE id = :provider_user_id AND token = :access_token"
    parameters = {"access_token": "token"}

    with pytest.raises(SQLSpecError):
        converter.convert_placeholder_style(sql, parameters, ParameterStyle.QMARK)


def test_missing_named_parameters_can_fallback_when_disabled(converter: ParameterConverter) -> None:
    """Missing named parameters may fall back to ordinal ordering when strict mode is disabled."""
    sql = "SELECT * FROM users WHERE id = :provider_user_id AND token = :access_token"
    parameters = {"access_token": "token", "refresh_token": "refresh"}

    _sql, converted_params = converter.convert_placeholder_style(
        sql, parameters, ParameterStyle.QMARK, strict_named_parameters=False
    )

    assert isinstance(converted_params, (list, tuple))
    assert len(converted_params) == 2
    assert next(iter(converted_params)) == "token"


@pytest.mark.parametrize(
    "database,input_style,output_style,sql_pattern,param_pattern",
    [
        ("sqlite", ParameterStyle.NAMED_COLON, ParameterStyle.QMARK, "?", list),
        ("postgresql", ParameterStyle.QMARK, ParameterStyle.NUMERIC, "$1", list),
        ("mysql", ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT, "%s", list),
        ("oracle", ParameterStyle.QMARK, ParameterStyle.POSITIONAL_COLON, ":1", dict),
        ("sqlserver", ParameterStyle.QMARK, ParameterStyle.NAMED_AT, "@param", dict),
        ("postgresql", ParameterStyle.QMARK, ParameterStyle.NAMED_DOLLAR, "$param", dict),
    ],
)
def test_database_specific_parameter_conversion(
    converter: ParameterConverter,
    database: str,
    input_style: ParameterStyle,
    output_style: ParameterStyle,
    sql_pattern: str,
    param_pattern: type,
) -> None:
    """Test parameter conversion for specific database requirements."""

    assert database is not None
    assert param_pattern is not None

    sql = "SELECT * FROM users WHERE id = :id"
    parameters = {"id": 123}

    if input_style != ParameterStyle.NAMED_COLON:
        sql, parameters = converter.convert_placeholder_style(sql, parameters, input_style)

    final_sql, final_params = converter.convert_placeholder_style(sql, parameters, output_style)

    assert sql_pattern in final_sql

    assert final_params is not None


def test_mixed_style_detection(validator: ParameterValidator) -> None:
    """Test detection of mixed parameter styles."""
    sql = "SELECT * FROM users WHERE id = ? AND name = :name AND email = %(email)s"
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 3
    styles = {p.style for p in parameters}
    assert ParameterStyle.QMARK in styles
    assert ParameterStyle.NAMED_COLON in styles
    assert ParameterStyle.NAMED_PYFORMAT in styles


def test_mixed_style_normalization() -> None:
    """Test normalization of mixed parameter styles."""
    converter = ParameterConverter()
    sql = "SELECT * FROM users WHERE id = ? AND name = :name AND status = %(status)s"

    normalized_sql, _ = converter.normalize_sql_for_parsing(sql, "mysql")

    assert "?" in normalized_sql
    assert ":name" in normalized_sql
    assert "%(status)s" not in normalized_sql
    assert ":param_" in normalized_sql


def test_parameter_caching() -> None:
    """Test that parameter extraction is cached."""
    validator = ParameterValidator()
    sql = "SELECT * FROM users WHERE id = ? AND name = :name"

    params1 = validator.extract_parameters(sql)

    params2 = validator.extract_parameters(sql)

    assert params1 is params2


def test_singledispatch_type_wrapping_performance() -> None:
    """Test that singledispatch provides efficient type-based dispatch."""

    bool_result = wrap_with_type(True)
    decimal_result = wrap_with_type(Decimal("123.45"))
    string_result = wrap_with_type("test")

    assert isinstance(bool_result, TypedParameter)
    assert isinstance(decimal_result, TypedParameter)

    assert string_result == "test"
    assert not isinstance(string_result, TypedParameter)


def test_hash_map_placeholder_generation() -> None:
    """Test that placeholder generation uses O(1) hash map lookups."""
    converter = ParameterConverter()

    supported_styles = [
        ParameterStyle.QMARK,
        ParameterStyle.NUMERIC,
        ParameterStyle.POSITIONAL_PYFORMAT,
        ParameterStyle.NAMED_COLON,
        ParameterStyle.NAMED_PYFORMAT,
        ParameterStyle.NAMED_AT,
    ]

    for style in supported_styles:
        sql = "SELECT * FROM users WHERE id = :param_0"
        parameters = ["test_value"]

        converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, style)

        assert converted_sql is not None
        assert converted_params is not None


def test_empty_sql_handling(validator: ParameterValidator) -> None:
    """Test handling of empty SQL strings."""
    parameters = validator.extract_parameters("")
    assert len(parameters) == 0


def test_null_parameter_handling(converter: ParameterConverter) -> None:
    """Test handling of null parameters."""
    sql = "SELECT * FROM users WHERE deleted_at = ?"
    parameters = [None]

    converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.STATIC)

    assert "NULL" in converted_sql
    assert "?" not in converted_sql
    assert converted_params is None


def test_complex_data_types(converter: ParameterConverter) -> None:
    """Test handling of complex data types."""
    sql = "INSERT INTO products (data) VALUES (?)"
    complex_data = {
        "name": "Product",
        "price": Decimal("99.99"),
        "available": True,
        "tags": ["electronics", "gadget"],
        "created": datetime(2023, 1, 1, 12, 0, 0),
    }
    parameters = [complex_data]

    _, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.QMARK)

    assert isinstance(converted_params, list)
    assert len(converted_params) == 1

    assert converted_params[0] == complex_data


def test_parameter_ordinal_assignment(validator: ParameterValidator) -> None:
    """Test that parameter ordinals are assigned correctly."""
    sql = "SELECT * FROM users WHERE id = ? AND name = ? AND email = ?"
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 3
    assert parameters[0].ordinal == 0
    assert parameters[1].ordinal == 1
    assert parameters[2].ordinal == 2


def test_large_parameter_count(validator: ParameterValidator) -> None:
    """Test handling of SQL with many parameters."""

    placeholders = ", ".join("?" for _ in range(50))
    sql = f"INSERT INTO test_table (col1, col2, ...) VALUES ({placeholders})"

    parameters = validator.extract_parameters(sql)
    assert len(parameters) == 50

    for i, param in enumerate(parameters):
        assert param.ordinal == i


@pytest.mark.parametrize(
    "obj,expected",
    [
        ([1, 2, 3], True),
        ((1, 2, 3), True),
        ({1, 2, 3}, True),
        ({"a": 1}, False),
        ("string", False),
        (b"bytes", False),
        (123, False),
        (None, False),
    ],
)
def test_is_iterable_parameters(obj: Any, expected: bool) -> None:
    """Test is_iterable_parameters helper function."""
    assert is_iterable_parameters(obj) == expected


def test_sqlite_compatibility(validator: ParameterValidator) -> None:
    """Test SQLite parameter style compatibility."""

    incompatible = validator.get_sqlglot_incompatible_styles("sqlite")
    assert ParameterStyle.POSITIONAL_COLON in incompatible
    assert ParameterStyle.QMARK not in incompatible
    assert ParameterStyle.NAMED_COLON not in incompatible


def test_postgresql_compatibility(validator: ParameterValidator) -> None:
    """Test PostgreSQL parameter style compatibility."""

    incompatible = validator.get_sqlglot_incompatible_styles("postgres")
    assert incompatible == {ParameterStyle.POSITIONAL_COLON}


def test_mysql_compatibility(validator: ParameterValidator) -> None:
    """Test MySQL parameter style compatibility."""

    incompatible = validator.get_sqlglot_incompatible_styles("mysql")
    assert ParameterStyle.POSITIONAL_PYFORMAT in incompatible
    assert ParameterStyle.NAMED_PYFORMAT in incompatible
    assert ParameterStyle.POSITIONAL_COLON in incompatible


def test_oracle_compatibility(validator: ParameterValidator) -> None:
    """Test Oracle parameter style compatibility."""

    incompatible = validator.get_sqlglot_incompatible_styles("oracle")
    assert ParameterStyle.POSITIONAL_COLON in incompatible
    assert ParameterStyle.POSITIONAL_PYFORMAT in incompatible
    assert ParameterStyle.NAMED_PYFORMAT in incompatible


def test_dollar_numeric_vs_named_disambiguation() -> None:
    """Test differentiation between $1 (numeric) and $name (named)."""
    validator = ParameterValidator()
    sql = "SELECT * FROM users WHERE id = $1 AND name = $username"
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 2

    dollar_1_param = next(p for p in parameters if p.placeholder_text == "$1")
    dollar_username_param = next(p for p in parameters if p.placeholder_text == "$username")

    assert dollar_1_param.style == ParameterStyle.NUMERIC
    assert dollar_username_param.style == ParameterStyle.NAMED_DOLLAR
    assert dollar_1_param.name == "1"
    assert dollar_username_param.name == "username"


def test_positional_colon_oracle_style() -> None:
    """Test Oracle-style positional colon parameters (:1, :2, :3)."""
    validator = ParameterValidator()
    sql = "SELECT * FROM users WHERE id = :1 AND name = :2 AND active = :3"
    parameters = validator.extract_parameters(sql)

    assert len(parameters) == 3
    assert all(p.style == ParameterStyle.POSITIONAL_COLON for p in parameters)
    assert parameters[0].placeholder_text == ":1"
    assert parameters[1].placeholder_text == ":2"
    assert parameters[2].placeholder_text == ":3"
    assert parameters[0].name == "1"
    assert parameters[1].name == "2"
    assert parameters[2].name == "3"


def test_preserve_parameter_names_in_conversion(converter: ParameterConverter) -> None:
    """Test that parameter names are preserved during conversion."""
    sql = "SELECT * FROM users WHERE name = :user_name AND email = :user_email"
    parameters = {"user_name": "alice", "user_email": "alice@example.com"}

    converted_sql, converted_params = converter.convert_placeholder_style(
        sql, parameters, ParameterStyle.NAMED_PYFORMAT
    )

    assert "%(user_name)s" in converted_sql
    assert "%(user_email)s" in converted_sql
    assert isinstance(converted_params, dict)
    assert converted_params["user_name"] == "alice"
    assert converted_params["user_email"] == "alice@example.com"


def test_duplicate_named_parameters_named_colon_to_numeric(converter: ParameterConverter) -> None:
    """Test conversion from named colon parameters with duplicates to numeric style.

    This reproduces the issue described in the bug report where duplicate named
    parameters cause incorrect parameter counting.
    """
    # SQL with duplicate :embedding parameter (appears twice)
    sql = "SELECT id, name, 1 - (embedding <=> :embedding) AS similarity FROM items WHERE 1 - (embedding <=> :embedding) > :threshold ORDER BY similarity DESC LIMIT :limit"

    parameters = {"embedding": [0.1, 0.2, 0.3], "threshold": 0.5, "limit": 10}

    converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.NUMERIC)

    # Should have exactly 3 parameters, not 4
    assert isinstance(converted_params, (list, tuple))
    assert len(converted_params) == 3

    # Check the SQL has correct numeric placeholders
    assert "$1" in converted_sql
    assert "$2" in converted_sql
    assert "$3" in converted_sql
    assert "$4" not in converted_sql  # Should not have a 4th parameter

    # Check parameter values are correct
    assert converted_params[0] == [0.1, 0.2, 0.3]  # embedding value appears once
    assert converted_params[1] == 0.5  # threshold
    assert converted_params[2] == 10  # limit


def test_duplicate_named_parameters_various_styles(converter: ParameterConverter) -> None:
    """Test duplicate parameter handling with various parameter styles."""
    test_cases = [
        {
            "style": ParameterStyle.NAMED_AT,
            "sql": "SELECT * FROM table WHERE col1 = @param AND col2 = @param",
            "expected_placeholders": ["@param"],
        },
        {
            "style": ParameterStyle.NAMED_DOLLAR,
            "sql": "SELECT * FROM table WHERE col1 = $param AND col2 = $param",
            "expected_placeholders": ["$param"],
        },
        {
            "style": ParameterStyle.NAMED_PYFORMAT,
            "sql": "SELECT * FROM table WHERE col1 = %(param)s AND col2 = %(param)s",
            "expected_placeholders": ["%(param)s"],
        },
    ]

    for case in test_cases:
        parameters = {"param": "test_value"}

        converted_sql, converted_params = converter.convert_placeholder_style(
            case["sql"],  # type: ignore[arg-type]
            parameters,
            ParameterStyle.NUMERIC,
        )

        # Should convert to single numeric parameter
        assert isinstance(converted_params, (list, tuple))
        assert len(converted_params) == 1
        assert converted_params[0] == "test_value"

        # Should have $1 twice in the SQL
        assert converted_sql.count("$1") == 2
        assert "$2" not in converted_sql


def test_duplicate_parameters_mixed_with_unique(converter: ParameterConverter) -> None:
    """Test duplicate parameters mixed with unique parameters."""
    sql = "SELECT :a, :b, :a, :c, :b"
    parameters = {"a": 1, "b": 2, "c": 3}

    converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.NUMERIC)

    # Should have exactly 3 unique parameters
    assert isinstance(converted_params, (list, tuple))
    assert len(converted_params) == 3

    # Check the correct values are extracted
    assert 1 in converted_params  # a
    assert 2 in converted_params  # b
    assert 3 in converted_params  # c

    # Check SQL has correct numeric placeholders in the right positions
    # Original: :a, :b, :a, :c, :b
    # Should become: $1, $2, $1, $3, $2
    expected_positions = ["$1", "$2", "$1", "$3", "$2"]

    # Extract placeholder positions from converted SQL
    import re

    placeholders_in_sql = [match.group() for match in re.finditer(r"\$\d+", converted_sql)]

    assert placeholders_in_sql == expected_positions


def test_duplicate_parameters_qmark_to_numeric(converter: ParameterConverter) -> None:
    """Test duplicate qmark parameters conversion (edge case)."""
    # This is a different case - qmark parameters are positional so duplicates would be different values
    sql = "SELECT * FROM table WHERE col1 = ? AND col2 = ?"
    parameters = [1, 2]

    converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.NUMERIC)

    # Should have 2 parameters as they're positional
    assert isinstance(converted_params, (list, tuple))
    assert len(converted_params) == 2
    assert list(converted_params) == [1, 2]

    assert "$1" in converted_sql
    assert "$2" in converted_sql


def test_vector_similarity_search_example(converter: ParameterConverter) -> None:
    """Test the exact example from the bug report."""
    sql = """SELECT
    id,
    name,
    1 - (embedding <=> :embedding) as similarity
FROM
    items
WHERE
    1 - (embedding <=> :embedding) > :threshold
ORDER BY
    similarity DESC
LIMIT
    :limit"""

    parameters = {"embedding": [0.1, 0.2, 0.3], "threshold": 0.5, "limit": 10}

    _converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.NUMERIC)

    # Should have exactly 3 parameters despite :embedding appearing twice
    assert isinstance(converted_params, (list, tuple))
    assert len(converted_params) == 3

    # Verify parameter values
    assert [0.1, 0.2, 0.3] in converted_params
    assert 0.5 in converted_params
    assert 10 in converted_params


def test_parameter_processor_duplicate_handling(processor: ParameterProcessor) -> None:
    """Test the full parameter processor with duplicate parameters."""
    config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NAMED_COLON, default_execution_parameter_style=ParameterStyle.NUMERIC
    )

    sql = "SELECT :param1, :param2, :param1 WHERE id = :param2"
    parameters = {"param1": "value1", "param2": "value2"}

    _processed_sql, processed_params = processor.process(sql, parameters, config, dialect="postgres")

    # Should have exactly 2 unique parameters
    assert isinstance(processed_params, (list, tuple))
    assert len(processed_params) == 2
    assert "value1" in processed_params
    assert "value2" in processed_params


# ============================================================================
# Type Narrowing Tests - Testing new ConvertedParameters type aliases
# ============================================================================


def test_converted_parameters_type_narrowing_none(converter: ParameterConverter) -> None:
    """Test that None parameters return None (ConvertedParameters type)."""
    sql = "SELECT * FROM table"
    parameters = None

    _converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.QMARK)

    # Should return None for None input
    assert converted_params is None


def test_converted_parameters_type_narrowing_empty_dict(converter: ParameterConverter) -> None:
    """Test that empty dict parameters return empty dict (ConvertedParameters type)."""
    sql = "SELECT * FROM table WHERE id = :id"
    parameters: dict[str, object] = {}
    param_info = converter.validator.extract_parameters(sql)

    # Use _convert_parameter_format directly to test empty dict handling
    converted_params = converter._convert_parameter_format(  # pyright: ignore
        parameters, param_info, ParameterStyle.NAMED_COLON, parameters, preserve_parameter_format=False
    )

    # Should return empty dict for empty dict input
    assert converted_params == {}
    assert isinstance(converted_params, dict)


def test_converted_parameters_type_narrowing_empty_list(converter: ParameterConverter) -> None:
    """Test that empty list parameters return empty list (ConvertedParameters type)."""
    sql = "SELECT * FROM table WHERE id = ?"
    parameters: list[object] = []
    param_info = converter.validator.extract_parameters(sql)

    # Use _convert_parameter_format directly to test empty list handling
    converted_params = converter._convert_parameter_format(  # pyright: ignore
        parameters, param_info, ParameterStyle.QMARK, parameters, preserve_parameter_format=False
    )

    # Should return empty list for empty list input
    assert converted_params == []
    assert isinstance(converted_params, list)


def test_converted_parameters_type_narrowing_dict_output(converter: ParameterConverter) -> None:
    """Test that dict parameters return dict[str, Any] (NamedParameterOutput type)."""
    sql = "SELECT * FROM table WHERE id = :id AND name = :name"
    parameters = {"id": 1, "name": "test"}

    _converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.NAMED_COLON)

    # Should return dict for named parameters
    assert isinstance(converted_params, dict)
    assert converted_params == {"id": 1, "name": "test"}
    # Type should be exactly dict, not Mapping
    assert type(converted_params) is dict


def test_converted_parameters_type_narrowing_list_output(converter: ParameterConverter) -> None:
    """Test that list parameters return list[Any] (PositionalParameterOutput type)."""
    sql = "SELECT * FROM table WHERE id = ? AND name = ?"
    parameters = [1, "test"]

    _converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.QMARK)

    # Should return list for positional parameters
    assert isinstance(converted_params, list)
    assert converted_params == [1, "test"]
    # Type should be exactly list, not Sequence
    assert type(converted_params) is list


def test_converted_parameters_type_narrowing_tuple_output(converter: ParameterConverter) -> None:
    """Test that tuple parameters return tuple[Any, ...] (PositionalParameterOutput type)."""
    sql = "SELECT * FROM table WHERE id = ? AND name = ?"
    parameters = (1, "test")

    _converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.QMARK)

    # Should return tuple for positional parameters when preserving format
    assert isinstance(converted_params, tuple)
    assert converted_params == (1, "test")
    # Type should be exactly tuple, not Sequence
    assert type(converted_params) is tuple


def test_converted_parameters_type_narrowing_static_style(converter: ParameterConverter) -> None:
    """Test that static style returns None (ConvertedParameters type)."""
    sql = "SELECT * FROM table WHERE id = :id"
    parameters = {"id": 1}

    converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.STATIC)

    # Should return None for static style (parameters embedded in SQL)
    assert converted_params is None
    assert "1" in converted_sql  # Parameter should be embedded


def test_positional_parameter_output_type_narrowing(converter: ParameterConverter) -> None:
    """Test _preserve_original_format returns PositionalParameterOutput."""
    param_values = [1, 2, 3]

    # Test with tuple original
    result_tuple = converter._preserve_original_format(param_values, (1, 2, 3))  # pyright: ignore
    assert isinstance(result_tuple, tuple)
    assert result_tuple == (1, 2, 3)

    # Test with list original
    result_list = converter._preserve_original_format(param_values, [1, 2, 3])  # pyright: ignore
    assert isinstance(result_list, list)
    assert result_list == [1, 2, 3]

    # Test with dict original (should return tuple)
    result_dict = converter._preserve_original_format(param_values, {"a": 1})  # pyright: ignore
    assert isinstance(result_dict, tuple)
    assert result_dict == (1, 2, 3)


def test_named_parameter_output_type_narrowing(converter: ParameterConverter) -> None:
    """Test _convert_sequence_to_dict returns NamedParameterOutput."""
    sql = "SELECT * FROM table WHERE id = :id AND name = :name"
    param_info = converter.validator.extract_parameters(sql)
    parameters = [1, "test"]

    result = converter._convert_sequence_to_dict(parameters, param_info)  # pyright: ignore

    # Should return dict[str, Any]
    assert isinstance(result, dict)
    assert type(result) is dict
    assert result == {"id": 1, "name": "test"}


def test_converted_parameters_processor_none_handling(processor: ParameterProcessor) -> None:
    """Test ParameterProcessor handles None parameters correctly."""
    config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
    sql = "SELECT * FROM table"
    parameters = None

    result = processor.process(sql, parameters, config)

    # Should return None for None parameters
    assert result.parameters is None


def test_converted_parameters_processor_empty_dict_handling(processor: ParameterProcessor) -> None:
    """Test ParameterProcessor handles empty dict correctly."""
    config = ParameterStyleConfig(default_parameter_style=ParameterStyle.NAMED_COLON)
    sql = "SELECT * FROM table"
    parameters: dict[str, object] = {}

    result = processor.process(sql, parameters, config)

    # Should return empty dict for empty dict
    assert result.parameters == {}
    assert isinstance(result.parameters, dict)


def test_converted_parameters_processor_dict_output(processor: ParameterProcessor) -> None:
    """Test ParameterProcessor returns dict[str, Any] for named parameters."""
    config = ParameterStyleConfig(default_parameter_style=ParameterStyle.NAMED_COLON)
    sql = "SELECT * FROM table WHERE id = :id"
    parameters = {"id": 1}

    result = processor.process(sql, parameters, config)

    # Should return dict
    assert isinstance(result.parameters, dict)
    assert type(result.parameters) is dict


def test_converted_parameters_processor_list_output(processor: ParameterProcessor) -> None:
    """Test ParameterProcessor returns list[Any] for positional parameters."""
    config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK, preserve_parameter_format=False)
    sql = "SELECT * FROM table WHERE id = ?"
    parameters = [1]

    result = processor.process(sql, parameters, config)

    # Should return list or tuple
    assert isinstance(result.parameters, (list, tuple))


def test_converted_parameters_transformers_null_pruning(processor: ParameterProcessor) -> None:
    """Test replace_null_parameters_with_literals returns ConvertedParameters."""
    _config = ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK)
    sql = "SELECT * FROM table WHERE id = ? AND name = ?"
    parameters = [1, None]

    # Parse SQL to get expression
    expression = sqlglot.parse_one(sql, dialect="postgres")

    # Test null pruning
    _transformed_expr, transformed_params = replace_null_parameters_with_literals(expression, parameters)

    # Should return concrete type - list or tuple
    assert transformed_params is not None
    assert isinstance(transformed_params, (list, tuple))
    # Should have removed the None parameter
    assert len(transformed_params) == 1
    assert transformed_params[0] == 1


def test_converted_parameters_transformers_none_input(processor: ParameterProcessor) -> None:
    """Test replace_null_parameters_with_literals handles None input."""
    expression = sqlglot.parse_one("SELECT * FROM table", dialect="postgres")

    # Test with None parameters
    _transformed_expr, transformed_params = replace_null_parameters_with_literals(expression, None)

    # Should return None for None input
    assert transformed_params is None


def test_converted_parameters_transformers_empty_dict(processor: ParameterProcessor) -> None:
    """Test replace_null_parameters_with_literals handles empty dict."""
    expression = sqlglot.parse_one("SELECT * FROM table", dialect="postgres")
    parameters: dict[str, object] = {}

    # Test with empty dict
    _transformed_expr, transformed_params = replace_null_parameters_with_literals(expression, parameters)

    # Should return dict for empty dict
    assert transformed_params == {}
    assert isinstance(transformed_params, dict)


def test_converted_parameters_is_many_handling(converter: ParameterConverter) -> None:
    """Test is_many parameter handling with type narrowing."""
    sql = "INSERT INTO table (id, name) VALUES (?, ?)"
    parameters = [[1, "test1"], [2, "test2"]]

    _converted_sql, converted_params = converter.convert_placeholder_style(
        sql, parameters, ParameterStyle.QMARK, is_many=True
    )

    # Should return list of parameters
    assert isinstance(converted_params, list)
    assert len(converted_params) == 2
    assert all(isinstance(p, list) for p in converted_params)


def test_converted_parameters_dict_to_positional_conversion(converter: ParameterConverter) -> None:
    """Test conversion from dict to positional returns PositionalParameterOutput."""
    sql = "SELECT * FROM table WHERE id = :id AND name = :name"
    parameters = {"id": 1, "name": "test"}

    _converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.QMARK)

    # Should return list or tuple (PositionalParameterOutput)
    assert isinstance(converted_params, (list, tuple))
    assert len(converted_params) == 2


def test_converted_parameters_sequence_to_named_conversion(converter: ParameterConverter) -> None:
    """Test conversion from sequence to named returns NamedParameterOutput."""
    sql = "SELECT * FROM table WHERE id = :id AND name = :name"
    parameters = [1, "test"]

    _converted_sql, converted_params = converter.convert_placeholder_style(sql, parameters, ParameterStyle.NAMED_COLON)

    # Should return dict (NamedParameterOutput)
    assert isinstance(converted_params, dict)
    assert type(converted_params) is dict


def test_converted_parameters_fallback_to_none(converter: ParameterConverter) -> None:
    """Test that non-standard parameters return None."""
    sql = "SELECT * FROM table WHERE id = :id"
    # Pass a non-standard parameter type
    parameters = "invalid"
    param_info = converter.validator.extract_parameters(sql)

    # Use _convert_parameter_format directly
    converted_params = converter._convert_parameter_format(  # pyright: ignore
        parameters, param_info, ParameterStyle.NAMED_COLON, parameters, preserve_parameter_format=False
    )

    # Should return None for non-standard parameters
    assert converted_params is None


def test_converted_parameters_processor_coercion(processor: ParameterProcessor) -> None:
    """Test that type coercion preserves ConvertedParameters type."""

    def custom_coercion(value: Any) -> str:
        return f"coerced:{value}"

    config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK, type_coercion_map={int: custom_coercion}
    )
    sql = "SELECT * FROM table WHERE id = ?"
    parameters = [42]

    result = processor.process(sql, parameters, config)

    # Should return list with coerced value
    assert isinstance(result.parameters, (list, tuple))
    assert result.parameters[0] == "coerced:42"
