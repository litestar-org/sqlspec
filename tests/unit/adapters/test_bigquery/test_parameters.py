"""Unit tests for BigQuery parameter handling utilities."""

from enum import Enum
from typing import Any, cast

import pytest
from google.cloud import bigquery

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.bigquery.core import create_parameters, default_statement_config
from sqlspec.core import SQL, TypedParameter
from sqlspec.exceptions import SQLSpecError


def test_create_parameters_requires_named_parameters() -> None:
    """Positional parameters should raise to avoid silent no-op behaviour."""

    with pytest.raises(SQLSpecError, match="requires named parameters"):
        create_parameters([1, 2, 3])


def test_create_parameters_builds_array_query_parameter() -> None:
    """Python sequences should become native BigQuery ARRAY query parameters."""
    parameters = create_parameters({"values": [1, 2, 3]})

    api_repr = cast("dict[str, Any]", parameters[0].to_api_repr())

    assert api_repr["name"] == "values"
    assert api_repr["parameterType"] == {"type": "ARRAY", "arrayType": {"type": "INT64"}}
    assert api_repr["parameterValue"] == {"arrayValues": [{"value": "1"}, {"value": "2"}, {"value": "3"}]}


def test_create_parameters_rejects_compiled_empty_array_parameter() -> None:
    """Compiled empty arrays should fail locally before BigQuery sees a JSON string."""
    config = BigQueryConfig()
    _sql, parameters = SQL(
        "SELECT ARRAY_LENGTH(@values)", {"values": []}, statement_config=config.statement_config
    ).compile()

    with pytest.raises(SQLSpecError, match="Cannot determine BigQuery ARRAY type"):
        create_parameters(parameters)


def test_sql_compile_accepts_native_at_parameter_keys() -> None:
    """Common parameter alignment accepts exact BigQuery placeholder-token keys."""
    statement = SQL("SELECT @name AS name", {"@name": "alpha"}, statement_config=default_statement_config)

    assert statement.named_parameters == {"@name": "alpha"}
    assert statement.compile()[1] == {"@name": "alpha"}


def test_create_parameters_strips_native_at_parameter_keys_for_client() -> None:
    """BigQuery client query parameters use names without the SQL @ marker."""
    (sql, parameters) = SQL(
        "SELECT @name AS name", {"@name": "alpha"}, statement_config=default_statement_config
    ).compile()
    bq_parameters = create_parameters(parameters)
    api_repr = cast("dict[str, Any]", bq_parameters[0].to_api_repr())

    assert sql == "SELECT @name AS name"
    assert api_repr["name"] == "name"
    assert api_repr["parameterValue"] == {"value": "alpha"}


def test_sql_compile_rejects_duplicate_native_parameter_key_aliases() -> None:
    """Both @name and name remain ambiguous because they provide two values for one placeholder."""
    statement = SQL(
        "SELECT @name AS name", {"name": "alpha", "@name": "beta"}, statement_config=default_statement_config
    )
    with pytest.raises(SQLSpecError, match="2 parameters provided but 1 placeholders detected"):
        statement.compile()


def test_json_parameter_uses_the_json_type_with_the_raw_value() -> None:
    """The client serializes JSON itself, so serializing first double-encodes."""
    parameter = cast("Any", create_parameters({"payload": {"name": "alpha"}})[0])

    assert parameter.type_ == "JSON"
    assert parameter.value == {"name": "alpha"}


def test_enum_members_bind_by_value() -> None:
    """Enum members are bound by value through an explicit check, not a duck-typed probe."""

    class _Status(Enum):
        ACTIVE = "active"

    parameter = cast("Any", create_parameters({"status": _Status.ACTIVE})[0])

    assert parameter.type_ == "STRING"
    assert parameter.value == "active"


def test_prebuilt_query_parameters_pass_through_untouched() -> None:
    """A caller who built the parameter keeps their declared type."""
    prebuilt = bigquery.ScalarQueryParameter("value", "NUMERIC", "1.25")

    (parameter,) = create_parameters({"value": prebuilt})

    assert parameter is prebuilt


def test_typed_null_is_not_typed_string() -> None:
    """A NULL bound to an integer column must not be sent as a STRING."""
    parameter = cast("Any", create_parameters({"value": TypedParameter(None, int)})[0])

    assert parameter.type_ == "INT64"
    assert parameter.value is None


def test_untyped_null_still_falls_back_to_string() -> None:
    """Without a declared type the existing behaviour is preserved."""
    parameter = cast("Any", create_parameters({"value": None})[0])

    assert parameter.type_ == "STRING"


def test_typed_parameter_with_a_value_is_unwrapped() -> None:
    """The wrapper is still unwrapped; only the over-broad probe is gone."""
    parameter = cast("Any", create_parameters({"value": TypedParameter(7, int)})[0])

    assert parameter.type_ == "INT64"
    assert parameter.value == 7
