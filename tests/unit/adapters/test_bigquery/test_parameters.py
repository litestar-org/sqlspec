"""Unit tests for BigQuery parameter handling utilities."""

from typing import Any, cast

import pytest

from sqlspec.adapters.bigquery.core import create_parameters, default_statement_config
from sqlspec.core import SQL
from sqlspec.exceptions import SQLSpecError


def test_create_parameters_requires_named_parameters() -> None:
    """Positional parameters should raise to avoid silent no-op behaviour."""

    with pytest.raises(SQLSpecError, match="requires named parameters"):
        create_parameters([1, 2, 3], json_serializer=lambda value: value)


def test_create_parameters_builds_array_query_parameter() -> None:
    """Python sequences should become native BigQuery ARRAY query parameters."""
    parameters = create_parameters({"values": [1, 2, 3]}, json_serializer=str)

    api_repr = cast("dict[str, Any]", parameters[0].to_api_repr())

    assert api_repr["name"] == "values"
    assert api_repr["parameterType"] == {"type": "ARRAY", "arrayType": {"type": "INT64"}}
    assert api_repr["parameterValue"] == {"arrayValues": [{"value": "1"}, {"value": "2"}, {"value": "3"}]}


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
    bq_parameters = create_parameters(parameters, json_serializer=str)
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
