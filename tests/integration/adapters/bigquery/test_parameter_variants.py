"""BigQuery-specific parameter variant coverage.

These tests intentionally stay adapter-local because BigQuery integration
tests are optional-gated and the shared C5 contract only runs active default
adapters. Generic qmark/named binding cases belong in the shared contract.
"""

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.core import SQL, SQLResult
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService

pytestmark = [pytest.mark.bigquery, pytest.mark.xdist_group("bigquery")]


@pytest.fixture(scope="session")
def bigquery_parameter_variants_session(
    bigquery_session: BigQueryDriver, bigquery_service: "BigQueryService"
) -> Generator[tuple[BigQueryDriver, str], None, None]:
    """Create a project-qualified BigQuery table for native parameter variants."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_parameter_variants`"
    bigquery_session.execute_script(
        f"""
        CREATE OR REPLACE TABLE {table_name} (
            id INT64,
            name STRING NOT NULL,
            value INT64,
            description STRING
        )
        """
    )
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value, description) VALUES (@id, @name, @value, @description)",
        {"id": 1, "name": "alpha", "value": 100, "description": "First test"},
    )
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value, description) VALUES (@id, @name, @value, @description)",
        {"id": 2, "name": "beta", "value": 200, "description": "Second test"},
    )
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value, description) VALUES (@id, @name, @value, @description)",
        {"id": 3, "name": "gamma", "value": 300, "description": None},
    )
    yield (bigquery_session, table_name)


def test_bigquery_native_named_at_parameters_with_project_dataset_table(
    bigquery_parameter_variants_session: tuple[BigQueryDriver, str],
) -> None:
    """BigQuery binds native @name parameters against fully-qualified tables."""
    session, table_name = bigquery_parameter_variants_session

    result = session.execute(
        f"SELECT name, value FROM {table_name} WHERE value BETWEEN @minimum AND @maximum ORDER BY value",
        {"minimum": 100, "maximum": 200},
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "alpha", "value": 100}, {"name": "beta", "value": 200}]


def test_bigquery_native_named_at_parameters_with_sql_object(
    bigquery_parameter_variants_session: tuple[BigQueryDriver, str],
) -> None:
    """BigQuery preserves native @name placeholders in SQL objects."""
    session, table_name = bigquery_parameter_variants_session

    result = session.execute(SQL(f"SELECT name FROM {table_name} WHERE name = @name", name="gamma"))

    assert result.get_data() == [{"name": "gamma"}]


def test_bigquery_native_parameter_keys_may_include_at_prefix(
    bigquery_parameter_variants_session: tuple[BigQueryDriver, str],
) -> None:
    """BigQuery strips @ from parameter dictionary keys before creating query parameters."""
    session, table_name = bigquery_parameter_variants_session

    result = session.execute(f"SELECT name FROM {table_name} WHERE name = @name", {"@name": "alpha"})

    assert result.get_data() == [{"name": "alpha"}]


def test_bigquery_array_parameter_with_unnest(bigquery_session: BigQueryDriver) -> None:
    """BigQuery creates native ARRAY query parameters for non-empty Python sequences."""
    result = bigquery_session.execute(
        "SELECT value FROM UNNEST(@values) AS value ORDER BY value", {"values": [1, 2, 3]}
    )

    assert result.get_data() == [{"value": 1}, {"value": 2}, {"value": 3}]


def test_bigquery_empty_array_parameter_requires_inferable_element_type(bigquery_session: BigQueryDriver) -> None:
    """BigQuery cannot infer an ARRAY query parameter type from an empty sequence."""
    with pytest.raises(SQLSpecError, match="Cannot determine BigQuery ARRAY type"):
        bigquery_session.execute("SELECT ARRAY_LENGTH(@values)", {"values": []})


def test_bigquery_parameters_inside_struct_expression(bigquery_session: BigQueryDriver) -> None:
    """BigQuery native parameters work inside STRUCT expressions."""
    result = bigquery_session.execute(
        """
        SELECT
            STRUCT(@name AS name, @age AS age) AS person,
            STRUCT(@name AS name, @age AS age).name AS person_name
        """,
        {"name": "Ada", "age": 37},
    )

    row = result.get_data()[0]
    assert row["person"]["name"] == "Ada"
    assert row["person"]["age"] == 37
    assert row["person_name"] == "Ada"
