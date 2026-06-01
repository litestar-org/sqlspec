"""Test parameter conversion and validation for BigQuery driver.

This test suite validates that the SQLTransformer properly converts different
input parameter styles to the target BigQuery NAMED_AT style (@param).

BigQuery Parameter Conversion Requirements:
- Input: QMARK (?) -> Output: NAMED_AT (@p1, @p2, ...)
- Input: NAMED_COLON (:name) -> Output: NAMED_AT (@name)
- Input: NAMED_PYFORMAT (%(name)s) -> Output: NAMED_AT (@name)
- Input: NAMED_AT (@name) -> Output: NAMED_AT (@name) (no conversion)

BigQuery uses @param style for named parameters.
"""

from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.core import SQL, SQLResult

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService
pytestmark = pytest.mark.xdist_group("bigquery")


@pytest.fixture(scope="session")
def bigquery_parameter_session(
    bigquery_session: BigQueryDriver, bigquery_service: "BigQueryService"
) -> Generator[tuple[BigQueryDriver, str], None, None]:
    """Session-scoped BigQuery fixture for parameter conversion testing.

    Uses CREATE OR REPLACE TABLE to create the table once and lets the
    emulator container handle cleanup at session end. Test cases are
    read-only against this dataset (test_special_characters_in_parameters
    appends an extra "special" row but other tests filter on test1/2/3 or
    tolerate extra rows).
    """
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_parameter_conversion`"
    bigquery_session.execute_script(
        f"\n        CREATE OR REPLACE TABLE {table_name} (\n            id INT64,\n            name STRING NOT NULL,\n            value INT64,\n            description STRING\n        )\n    "
    )
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value, description) VALUES (@id, @name, @value, @desc)",
        {"id": 1, "name": "test1", "value": 100, "desc": "First test"},
    )
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value, description) VALUES (@id, @name, @value, @desc)",
        {"id": 2, "name": "test2", "value": 200, "desc": "Second test"},
    )
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value, description) VALUES (@id, @name, @value, @desc)",
        {"id": 3, "name": "test3", "value": 300, "desc": None},
    )
    yield (bigquery_session, table_name)


def test_named_at_parameter_style_named_at_single_parameter(
    bigquery_parameter_session: tuple[BigQueryDriver, str],
) -> None:
    """Test single @param placeholder works natively."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(f"SELECT * FROM {table_name} WHERE name = @name", {"name": "test1"})
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test1"


def test_named_at_parameter_style_named_at_multiple_parameters(
    bigquery_parameter_session: tuple[BigQueryDriver, str],
) -> None:
    """Test multiple @param placeholders work natively."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(
        f"SELECT * FROM {table_name} WHERE value >= @min_val AND value <= @max_val ORDER BY value",
        {"min_val": 100, "max_val": 200},
    )
    assert isinstance(result, SQLResult)
    assert len(result.data) == 2
    assert result.get_data()[0]["value"] == 100
    assert result.get_data()[1]["value"] == 200


def test_qmark_conversion_qmark_single_parameter(bigquery_parameter_session: tuple[BigQueryDriver, str]) -> None:
    """Test single ? placeholder gets converted to @p1."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(f"SELECT * FROM {table_name} WHERE name = ?", ("test1",))
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test1"


def test_qmark_conversion_qmark_multiple_parameters(bigquery_parameter_session: tuple[BigQueryDriver, str]) -> None:
    """Test multiple ? placeholders get converted to @p1, @p2, etc."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(f"SELECT * FROM {table_name} WHERE name = ? AND value > ?", ("test2", 100))
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test2"


def test_named_colon_conversion_named_colon_single_parameter(
    bigquery_parameter_session: tuple[BigQueryDriver, str],
) -> None:
    """Test single :name placeholder gets converted to @name."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(f"SELECT * FROM {table_name} WHERE name = :name", {"name": "test1"})
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test1"


def test_named_colon_conversion_named_colon_multiple_parameters(
    bigquery_parameter_session: tuple[BigQueryDriver, str],
) -> None:
    """Test multiple :name placeholders get converted."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(
        f"SELECT * FROM {table_name} WHERE name = :name AND value > :min_val", {"name": "test2", "min_val": 100}
    )
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test2"


def test_named_pyformat_conversion_named_pyformat_single_parameter(
    bigquery_parameter_session: tuple[BigQueryDriver, str],
) -> None:
    """Test single %(name)s placeholder gets converted to @name."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(f"SELECT * FROM {table_name} WHERE name = %(name)s", {"name": "test1"})
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test1"


def test_named_pyformat_conversion_named_pyformat_multiple_parameters(
    bigquery_parameter_session: tuple[BigQueryDriver, str],
) -> None:
    """Test multiple %(name)s placeholders get converted."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(
        f"SELECT * FROM {table_name} WHERE name = %(test_name)s AND value < %(max_val)s",
        {"test_name": "test3", "max_val": 350},
    )
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test3"


def test_sql_object_conversion_sql_object_with_named_at(bigquery_parameter_session: tuple[BigQueryDriver, str]) -> None:
    """Test SQL object with @param placeholders."""
    (session, table_name) = bigquery_parameter_session
    sql_named_at = SQL(
        f"SELECT * FROM {table_name} WHERE value BETWEEN @min_val AND @max_val", min_val=150, max_val=250
    )
    result = session.execute(sql_named_at)
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test2"


def test_sql_object_conversion_sql_object_with_qmark(bigquery_parameter_session: tuple[BigQueryDriver, str]) -> None:
    """Test SQL object with ? placeholders."""
    (session, table_name) = bigquery_parameter_session
    sql_qmark = SQL(f"SELECT * FROM {table_name} WHERE name = ? OR name = ?", "test1", "test3")
    result = session.execute(sql_qmark)
    assert isinstance(result, SQLResult)
    assert len(result.data) == 2
    names = [row["name"] for row in result.get_data()]
    assert "test1" in names
    assert "test3" in names


def test_edge_cases_null_parameters(bigquery_parameter_session: tuple[BigQueryDriver, str]) -> None:
    """Test NULL parameter handling."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(f"SELECT * FROM {table_name} WHERE description IS NULL")
    assert isinstance(result, SQLResult)
    assert len(result.data) == 1
    assert result.get_data()[0]["name"] == "test3"


def test_edge_cases_sql_injection_prevention(bigquery_parameter_session: tuple[BigQueryDriver, str]) -> None:
    """Test that parameter escaping prevents SQL injection."""
    (session, table_name) = bigquery_parameter_session
    malicious_input = "'; DROP TABLE test_parameter_conversion; --"
    result = session.execute(f"SELECT * FROM {table_name} WHERE name = @name", {"name": malicious_input})
    assert len(result.data) == 0
    count_result = session.execute(f"SELECT COUNT(*) as count FROM {table_name}")
    assert count_result.get_data()[0]["count"] >= 3


def test_edge_cases_special_characters_in_parameters(bigquery_parameter_session: tuple[BigQueryDriver, str]) -> None:
    """Test special characters in parameter values."""
    (session, table_name) = bigquery_parameter_session
    special_value = 'O\'Reilly & Sons "Test" <script>'
    session.execute(
        f"INSERT INTO {table_name} (id, name, value, description) VALUES (@id, @name, @value, @desc)",
        {"id": 99, "name": "special", "value": 999, "desc": special_value},
    )
    result = session.execute(f"SELECT * FROM {table_name} WHERE name = @name", {"name": "special"})
    assert len(result.data) == 1
    assert result.get_data()[0]["description"] == special_value


def test_edge_cases_like_with_wildcards(bigquery_parameter_session: tuple[BigQueryDriver, str]) -> None:
    """Test LIKE queries with wildcard parameters."""
    (session, table_name) = bigquery_parameter_session
    result = session.execute(f"SELECT * FROM {table_name} WHERE name LIKE @pattern", {"pattern": "test%"})
    assert len(result.data) >= 3
    for row in result.get_data():
        assert row["name"].startswith("test")
