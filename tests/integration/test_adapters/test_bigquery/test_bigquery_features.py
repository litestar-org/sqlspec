"""BigQuery-specific feature tests."""

import pytest

from sqlspec.adapters.bigquery import BigQueryDriver
from sqlspec.core import SQLResult

pytestmark = pytest.mark.xdist_group("bigquery")


def test_bigquery_standard_sql_functions(bigquery_session: BigQueryDriver) -> None:
    """Test BigQuery standard SQL functions."""

    result = bigquery_session.execute("""
        SELECT
            ABS(-42) as abs_value,
            ROUND(3.15159234, 2) as rounded,
            MOD(17, 5) as mod_result,
            POWER(2, 3) as power_result
    """)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["abs_value"] == 42
    assert result.data[0]["rounded"] == 3.15
    assert result.data[0]["mod_result"] == 2
    assert result.data[0]["power_result"] == 8

    string_result = bigquery_session.execute("""
        SELECT
            UPPER('hello') as upper_str,
            LOWER('WORLD') as lower_str,
            LENGTH('BigQuery') as str_length,
            CONCAT('Hello', ' ', 'World') as concatenated
    """)
    assert isinstance(string_result, SQLResult)
    assert string_result.data is not None
    assert string_result.data[0]["upper_str"] == "HELLO"
    assert string_result.data[0]["lower_str"] == "world"
    assert string_result.data[0]["str_length"] == 8
    assert string_result.data[0]["concatenated"] == "Hello World"


def test_bigquery_conditional_functions(bigquery_session: BigQueryDriver) -> None:
    """Test BigQuery conditional functions."""

    result = bigquery_session.execute("""
        SELECT
            CASE
                WHEN 1 > 0 THEN 'positive'
                WHEN 1 = 0 THEN 'zero'
                ELSE 'negative'
            END as case_result,
            IF(10 > 5, 'greater', 'lesser') as if_result,
            IFNULL(NULL, 'default_value') as ifnull_result,
            NULLIF(5, 5) as nullif_result,
            COALESCE(NULL, NULL, 'first_non_null') as coalesce_result
    """)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["case_result"] == "positive"
    assert result.data[0]["if_result"] == "greater"
    assert result.data[0]["ifnull_result"] == "default_value"
    assert result.data[0]["nullif_result"] is None
    assert result.data[0]["coalesce_result"] == "first_non_null"
