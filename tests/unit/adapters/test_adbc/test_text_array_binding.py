"""Unit tests for ADBC PostgreSQL text array None element preservation."""

from sqlspec.adapters.adbc.core import (
    _convert_array_for_postgres_adbc,
    get_statement_config,
    prepare_parameters_with_casts,
    prepare_postgres_parameters,
)
from sqlspec.adapters.adbc.type_converter import get_adbc_type_converter


def test_convert_array_for_postgres_adbc_preserves_none() -> None:
    """Test that _convert_array_for_postgres_adbc preserves None elements in lists and tuples."""
    input_list = ["alpha", None, "beta"]
    result_list = _convert_array_for_postgres_adbc(input_list)
    assert result_list == ["alpha", None, "beta"]
    assert result_list[1] is None

    input_tuple = ("foo", None, "bar")
    result_tuple = _convert_array_for_postgres_adbc(input_tuple)
    assert result_tuple == ["foo", None, "bar"]
    assert result_tuple[1] is None


def test_adbc_output_converter_convert_sequence_preserves_none() -> None:
    """Test that ADBCOutputConverter.convert_sequence preserves None elements for postgres dialects."""
    converter = get_adbc_type_converter("postgres")
    assert hasattr(converter, "convert_sequence")

    res_list = converter.convert_sequence(["a", None, "b"])
    assert res_list == ["a", None, "b"]
    assert res_list[1] is None

    res_tuple = converter.convert_sequence(("x", None, "y"))
    assert res_tuple == ["x", None, "y"]
    assert res_tuple[1] is None


def test_prepare_parameters_with_casts_preserves_none_in_text_array() -> None:
    """Test that parameter preparation with cast mapping preserves None elements in text arrays."""
    statement_config = get_statement_config("postgres")
    params = [["hello", None, "world"]]
    casts: dict[int, str] = {1: "TEXT[]"}

    prepared = prepare_parameters_with_casts(
        params, casts, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared == [["hello", None, "world"]]
    assert prepared[0][1] is None


def test_prepare_postgres_parameters_preserves_none_without_casts() -> None:
    """Test that prepare_postgres_parameters preserves None in array parameters when no casts are present."""
    statement_config = get_statement_config("postgres")
    params = [["first", None, "second"]]

    prepared = prepare_postgres_parameters(
        params, {}, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared == [["first", None, "second"]]
    assert prepared[0][1] is None
