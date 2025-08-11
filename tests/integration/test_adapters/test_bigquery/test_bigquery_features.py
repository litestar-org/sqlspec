"""BigQuery-specific feature tests with CORE_ROUND_3 architecture."""

import pytest
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.bigquery import BigQueryDriver
from sqlspec.core.result import SQLResult


@pytest.mark.xdist_group("bigquery")
def test_bigquery_standard_sql_functions(bigquery_session: BigQueryDriver) -> None:
    """Test BigQuery standard SQL functions."""

    # Test basic math functions
    result = bigquery_session.execute("""
        SELECT
            ABS(-42) as abs_value,
            ROUND(3.14159, 2) as rounded,
            MOD(17, 5) as mod_result,
            POWER(2, 3) as power_result
    """)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["abs_value"] == 42
    assert result.data[0]["rounded"] == 3.14
    assert result.data[0]["mod_result"] == 2
    assert result.data[0]["power_result"] == 8

    # Test string functions
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


@pytest.mark.xdist_group("bigquery")
@pytest.mark.xfail(reason="BigQuery emulator has issues with DATETIME function - requires DATE or TIMESTAMP type")
def test_bigquery_date_time_functions(bigquery_session: BigQueryDriver) -> None:
    """Test BigQuery date and time functions."""

    # Test date functions (avoid CURRENT_DATE/TIMESTAMP due to emulator issues)
    result = bigquery_session.execute("""
        SELECT
            DATE('2024-01-15') as test_date,
            DATETIME('2024-01-15 10:30:00') as test_datetime,
            DATE_DIFF(DATE('2024-01-20'), DATE('2024-01-15'), DAY) as day_diff,
            EXTRACT(YEAR FROM DATE('2024-01-15')) as year_part,
            EXTRACT(MONTH FROM DATE('2024-01-15')) as month_part
    """)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["day_diff"] == 5
    assert result.data[0]["year_part"] == 2024
    assert result.data[0]["month_part"] == 1


@pytest.mark.xdist_group("bigquery")
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


@pytest.mark.xdist_group("bigquery")
def test_bigquery_aggregate_functions(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery aggregate functions."""
    table_name = bigquery_test_table

    # Insert test data for aggregation
    test_data = [(1, "Group A", 10), (2, "Group A", 20), (3, "Group B", 15), (4, "Group B", 25), (5, "Group C", 30)]
    bigquery_session.execute_many(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", test_data)

    # Test basic aggregates
    result = bigquery_session.execute(f"""
        SELECT
            COUNT(*) as total_count,
            COUNT(DISTINCT name) as distinct_groups,
            SUM(value) as total_value,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value,
            STDDEV(value) as stddev_value
        FROM {table_name}
    """)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["total_count"] == 5
    assert result.data[0]["distinct_groups"] == 3
    assert result.data[0]["total_value"] == 100  # 10+20+15+25+30
    assert result.data[0]["avg_value"] == 20.0
    assert result.data[0]["min_value"] == 10
    assert result.data[0]["max_value"] == 30

    # Test GROUP BY aggregates
    group_result = bigquery_session.execute(f"""
        SELECT
            name,
            COUNT(*) as count,
            SUM(value) as sum_value,
            AVG(value) as avg_value
        FROM {table_name}
        GROUP BY name
        ORDER BY name
    """)
    assert isinstance(group_result, SQLResult)
    assert group_result.data is not None
    assert len(group_result.data) == 3
    assert group_result.data[0]["name"] == "Group A"
    assert group_result.data[0]["count"] == 2
    assert group_result.data[0]["sum_value"] == 30  # 10+20


@pytest.mark.xdist_group("bigquery")
def test_bigquery_join_operations(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test BigQuery JOIN operations."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_table`"

    # Drop table if exists and recreate
    try:
        bigquery_session.execute_script(f"DROP TABLE {table_name}")
    except Exception:
        pass  # Table doesn't exist, ignore

    bigquery_session.execute_script(f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING,
            value INT64
        )
    """)

    # Create second table for joins
    bigquery_session.execute_script(f"""
        CREATE TABLE IF NOT EXISTS `{bigquery_service.project}.{bigquery_service.dataset}.join_table` (
            id INT64,
            category STRING,
            multiplier INT64
        )
    """)

    # Insert data into both tables
    bigquery_session.execute_many(
        f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)",
        [(1, "Item A", 100), (2, "Item B", 200), (3, "Item C", 150)],
    )

    bigquery_session.execute_many(
        f"INSERT INTO `{bigquery_service.project}.{bigquery_service.dataset}.join_table` (id, category, multiplier) VALUES (?, ?, ?)",
        [(1, "Premium", 2), (2, "Standard", 1), (4, "Discount", 0)],
    )

    # Test INNER JOIN
    inner_result = bigquery_session.execute(f"""
        SELECT
            t1.name,
            t1.value,
            t2.category,
            t1.value * t2.multiplier as calculated_value
        FROM {table_name} t1
        INNER JOIN `{bigquery_service.project}.{bigquery_service.dataset}.join_table` t2
        ON t1.id = t2.id
        ORDER BY t1.id
    """)
    assert isinstance(inner_result, SQLResult)
    assert inner_result.data is not None
    assert len(inner_result.data) == 2  # Only matching records
    assert inner_result.data[0]["name"] == "Item A"
    assert inner_result.data[0]["calculated_value"] == 200  # 100 * 2

    # Test LEFT JOIN
    left_result = bigquery_session.execute(f"""
        SELECT
            t1.name,
            t2.category
        FROM {table_name} t1
        LEFT JOIN `{bigquery_service.project}.{bigquery_service.dataset}.join_table` t2
        ON t1.id = t2.id
        ORDER BY t1.id
    """)
    assert isinstance(left_result, SQLResult)
    assert left_result.data is not None
    assert len(left_result.data) == 3  # All records from left table
    assert left_result.data[2]["name"] == "Item C"
    assert left_result.data[2]["category"] is None  # No match in right table

    # Cleanup
    bigquery_session.execute_script(f"DROP TABLE `{bigquery_service.project}.{bigquery_service.dataset}.join_table`")


@pytest.mark.xdist_group("bigquery")
def test_bigquery_subqueries(bigquery_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test BigQuery subquery operations."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_table`"

    # Drop table if exists and recreate
    try:
        bigquery_session.execute_script(f"DROP TABLE {table_name}")
    except Exception:
        pass  # Table doesn't exist, ignore

    bigquery_session.execute_script(f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING,
            value INT64
        )
    """)

    # Insert test data
    test_data = [(1, "High", 80), (2, "Medium", 60), (3, "Low", 40), (4, "High", 90)]
    bigquery_session.execute_many(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", test_data)

    # Test scalar subquery
    scalar_result = bigquery_session.execute(f"""
        SELECT name, value
        FROM {table_name}
        WHERE value > (SELECT AVG(value) FROM {table_name})
        ORDER BY value DESC
    """)
    assert isinstance(scalar_result, SQLResult)
    assert scalar_result.data is not None
    assert len(scalar_result.data) == 2  # 80 and 90 are above average (67.5)
    assert scalar_result.data[0]["value"] == 90

    # Test EXISTS subquery
    exists_result = bigquery_session.execute(f"""
        SELECT DISTINCT name
        FROM {table_name} t1
        WHERE EXISTS (
            SELECT 1 FROM {table_name} t2
            WHERE t2.name = t1.name AND t2.value > 70
        )
        ORDER BY name
    """)
    assert isinstance(exists_result, SQLResult)
    assert exists_result.data is not None
    assert len(exists_result.data) == 1
    assert exists_result.data[0]["name"] == "High"

    # Test IN subquery
    in_result = bigquery_session.execute(f"""
        SELECT name, value
        FROM {table_name}
        WHERE value IN (
            SELECT MAX(value)
            FROM {table_name}
            GROUP BY name
        )
        ORDER BY value
    """)
    assert isinstance(in_result, SQLResult)
    assert in_result.data is not None
    assert len(in_result.data) == 3  # Max value for each group


@pytest.mark.xdist_group("bigquery")
def test_bigquery_cte_common_table_expressions(
    bigquery_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test BigQuery Common Table Expressions (CTEs)."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_table`"

    # Drop table if exists and recreate
    try:
        bigquery_session.execute_script(f"DROP TABLE {table_name}")
    except Exception:
        pass  # Table doesn't exist, ignore

    bigquery_session.execute_script(f"""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING,
            value INT64
        )
    """)

    # Insert test data
    test_data = [(1, "Dept A", 50), (2, "Dept A", 60), (3, "Dept B", 70), (4, "Dept B", 80)]
    bigquery_session.execute_many(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", test_data)

    # Test single CTE
    cte_result = bigquery_session.execute(f"""
        WITH dept_stats AS (
            SELECT
                name as department,
                AVG(value) as avg_value,
                COUNT(*) as employee_count
            FROM {table_name}
            GROUP BY name
        )
        SELECT
            department,
            avg_value,
            employee_count,
            CASE
                WHEN avg_value > 60 THEN 'High Performance'
                ELSE 'Standard Performance'
            END as performance_category
        FROM dept_stats
        ORDER BY department
    """)
    assert isinstance(cte_result, SQLResult)
    assert cte_result.data is not None
    assert len(cte_result.data) == 2
    assert cte_result.data[0]["department"] == "Dept A"
    assert cte_result.data[0]["avg_value"] == 55.0
    assert cte_result.data[1]["performance_category"] == "High Performance"

    # Test multiple CTEs
    multiple_cte_result = bigquery_session.execute(f"""
        WITH
        dept_totals AS (
            SELECT name, SUM(value) as total_value
            FROM {table_name}
            GROUP BY name
        ),
        overall_stats AS (
            SELECT
                SUM(total_value) as grand_total,
                AVG(total_value) as avg_dept_total
            FROM dept_totals
        )
        SELECT
            dt.name as department,
            dt.total_value,
            ROUND(100.0 * dt.total_value / os.grand_total, 1) as percentage_of_total
        FROM dept_totals dt
        CROSS JOIN overall_stats os
        ORDER BY dt.total_value DESC
    """)
    assert isinstance(multiple_cte_result, SQLResult)
    assert multiple_cte_result.data is not None
    assert len(multiple_cte_result.data) == 2
    # Dept B (150) should have higher percentage than Dept A (110)
    assert multiple_cte_result.data[0]["department"] == "Dept B"
    assert multiple_cte_result.data[0]["percentage_of_total"] > 50.0
