"""Comprehensive unit tests for the enhanced SelectBuilder with all new features.

This module tests the SelectBuilder enhancements including:
- DISTINCT support
- UNION, INTERSECT, EXCEPT operations
- Subquery helpers (EXISTS, IN, etc.)
- Condition helpers (LIKE, BETWEEN, NULL, etc.)
- Aggregate function helpers (COUNT, SUM, AVG, MAX, MIN)
- Window functions with OVER clauses
- CASE expressions via CaseBuilder
- SQL injection prevention
- Edge cases and error handling
"""

import pytest
from sqlglot import exp

from sqlspec.exceptions import SQLBuilderError
from sqlspec.sql.builder import SelectBuilder
from sqlspec.sql.builder._select import CaseBuilder


class TestSelectBuilderBasicEnhancements:
    """Test basic enhancements to SelectBuilder."""

    def test_distinct_columns(self) -> None:
        """Test DISTINCT with specific columns."""
        builder = SelectBuilder().select("id", "name").distinct("id").from_("users")
        query = builder.build()

        assert "DISTINCT" in query.sql
        assert "id" in query.sql
        assert "name" in query.sql
        assert "users" in query.sql

    def test_distinct_no_columns(self) -> None:
        """Test DISTINCT without specific columns (affects all selected columns)."""
        builder = SelectBuilder().select("id", "name").distinct().from_("users")
        query = builder.build()

        assert "DISTINCT" in query.sql
        assert "id" in query.sql
        assert "name" in query.sql

    def test_distinct_multiple_columns(self) -> None:
        """Test DISTINCT with multiple columns."""
        builder = SelectBuilder().select("id", "name", "email").distinct("id", "name").from_("users")
        query = builder.build()

        assert "DISTINCT" in query.sql
        # Should contain both distinct columns
        assert "id" in query.sql
        assert "name" in query.sql


class TestSelectBuilderSetOperations:
    """Test set operations (UNION, INTERSECT, EXCEPT)."""

    def test_union_basic(self) -> None:
        """Test basic UNION operation."""
        builder1 = SelectBuilder().select("id").from_("users")
        builder2 = SelectBuilder().select("id").from_("customers")

        union_builder = builder1.union(builder2)
        query = union_builder.build()

        assert "UNION" in query.sql
        assert "users" in query.sql
        assert "customers" in query.sql

    def test_union_all(self) -> None:
        """Test UNION ALL operation."""
        builder1 = SelectBuilder().select("id").from_("users")
        builder2 = SelectBuilder().select("id").from_("customers")

        union_builder = builder1.union(builder2, all_=True)
        query = union_builder.build()

        assert "UNION ALL" in query.sql

    def test_intersect_operation(self) -> None:
        """Test INTERSECT operation."""
        builder1 = SelectBuilder().select("id").from_("users")
        builder2 = SelectBuilder().select("id").from_("customers")

        intersect_builder = builder1.intersect(builder2)
        query = intersect_builder.build()

        assert "INTERSECT" in query.sql
        assert "users" in query.sql
        assert "customers" in query.sql

    def test_except_operation(self) -> None:
        """Test EXCEPT operation."""
        builder1 = SelectBuilder().select("id").from_("users")
        builder2 = SelectBuilder().select("id").from_("customers")

        except_builder = builder1.except_(builder2)
        query = except_builder.build()

        assert "EXCEPT" in query.sql
        assert "users" in query.sql
        assert "customers" in query.sql

    def test_set_operations_with_parameters(self) -> None:
        """Test set operations preserve parameters from both queries."""
        builder1 = SelectBuilder().select("id").from_("users").where(("name", "John"))
        builder2 = SelectBuilder().select("id").from_("customers").where(("name", "Jane"))

        union_builder = builder1.union(builder2)
        query = union_builder.build()

        # Both parameter values should be present
        assert "John" in query.parameters.values()
        assert "Jane" in query.parameters.values()


class TestSelectBuilderSubqueryHelpers:
    """Test subquery helper methods."""

    def test_where_exists_with_builder(self) -> None:
        """Test WHERE EXISTS with SelectBuilder subquery."""
        subquery = SelectBuilder().select("1").from_("orders").where(("user_id", "users.id"))
        builder = SelectBuilder().select("*").from_("users").where_exists(subquery)

        query = builder.build()

        assert "EXISTS" in query.sql
        assert "orders" in query.sql
        assert "user_id" in query.sql

    def test_where_exists_with_string(self) -> None:
        """Test WHERE EXISTS with string subquery."""
        builder = (
            SelectBuilder().select("*").from_("users").where_exists("SELECT 1 FROM orders WHERE user_id = users.id")
        )

        query = builder.build()

        assert "EXISTS" in query.sql
        assert "orders" in query.sql

    def test_where_not_exists(self) -> None:
        """Test WHERE NOT EXISTS clause."""
        subquery = SelectBuilder().select("1").from_("orders").where(("user_id", "users.id"))
        builder = SelectBuilder().select("*").from_("users").where_not_exists(subquery)

        query = builder.build()

        assert "NOT EXISTS" in query.sql or ("NOT" in query.sql and "EXISTS" in query.sql)

    def test_where_in_with_list(self) -> None:
        """Test WHERE IN with list of values."""
        builder = SelectBuilder().select("*").from_("users").where_in("id", [1, 2, 3])

        query = builder.build()

        assert "IN" in query.sql
        # Parameters should contain all values
        assert 1 in query.parameters.values()
        assert 2 in query.parameters.values()
        assert 3 in query.parameters.values()

    def test_where_in_with_tuple(self) -> None:
        """Test WHERE IN with tuple of values."""
        builder = SelectBuilder().select("*").from_("users").where_in("status", ("active", "pending"))

        query = builder.build()

        assert "IN" in query.sql
        assert "active" in query.parameters.values()
        assert "pending" in query.parameters.values()

    def test_where_in_with_subquery(self) -> None:
        """Test WHERE IN with subquery."""
        subquery = SelectBuilder().select("user_id").from_("orders")
        builder = SelectBuilder().select("*").from_("users").where_in("id", subquery)

        query = builder.build()

        assert "IN" in query.sql
        assert "orders" in query.sql

    def test_where_not_in(self) -> None:
        """Test WHERE NOT IN clause."""
        builder = SelectBuilder().select("*").from_("users").where_not_in("status", ["banned", "deleted"])

        query = builder.build()

        assert "NOT IN" in query.sql or ("NOT" in query.sql and "IN" in query.sql)
        assert "banned" in query.parameters.values()
        assert "deleted" in query.parameters.values()


class TestSelectBuilderConditionHelpers:
    """Test condition helper methods."""

    def test_where_like_basic(self) -> None:
        """Test basic WHERE LIKE clause."""
        builder = SelectBuilder().select("*").from_("users").where_like("name", "John%")

        query = builder.build()

        assert "LIKE" in query.sql
        assert "John%" in query.parameters.values()

    def test_where_like_with_escape(self) -> None:
        """Test WHERE LIKE with escape character."""
        builder = SelectBuilder().select("*").from_("users").where_like("name", "John\\_%", escape="\\")

        query = builder.build()

        assert "LIKE" in query.sql
        assert "ESCAPE" in query.sql
        assert "John\\_%" in query.parameters.values()
        assert "\\" in query.parameters.values()

    def test_where_between(self) -> None:
        """Test WHERE BETWEEN clause."""
        builder = SelectBuilder().select("*").from_("users").where_between("age", 18, 65)

        query = builder.build()

        assert "BETWEEN" in query.sql
        assert 18 in query.parameters.values()
        assert 65 in query.parameters.values()

    def test_where_null(self) -> None:
        """Test WHERE IS NULL clause."""
        builder = SelectBuilder().select("*").from_("users").where_null("deleted_at")

        query = builder.build()

        assert "IS NULL" in query.sql

    def test_where_not_null(self) -> None:
        """Test WHERE IS NOT NULL clause."""
        builder = SelectBuilder().select("*").from_("users").where_not_null("email")

        query = builder.build()

        assert "IS NOT NULL" in query.sql or ("IS" in query.sql and "NOT NULL" in query.sql)


class TestSelectBuilderAggregateHelpers:
    """Test aggregate function helpers."""

    def test_select_count_star(self) -> None:
        """Test COUNT(*) function."""
        builder = SelectBuilder().count_().from_("users")

        query = builder.build()

        assert "COUNT(*)" in query.sql or ("COUNT" in query.sql and "*" in query.sql)

    def test_select_count_column(self) -> None:
        """Test COUNT(column) function."""
        builder = SelectBuilder().count_("id").from_("users")

        query = builder.build()

        assert "COUNT" in query.sql
        assert "id" in query.sql

    def test_select_count_with_alias(self) -> None:
        """Test COUNT with alias."""
        builder = SelectBuilder().count_("id", "total_users").from_("users")

        query = builder.build()

        assert "COUNT" in query.sql
        assert "total_users" in query.sql

    def test_select_sum(self) -> None:
        """Test SUM function."""
        builder = SelectBuilder().sum_("amount", "total_amount").from_("orders")

        query = builder.build()

        assert "SUM" in query.sql
        assert "amount" in query.sql
        assert "total_amount" in query.sql

    def test_select_avg(self) -> None:
        """Test AVG function."""
        builder = SelectBuilder().avg_("price", "avg_price").from_("products")

        query = builder.build()

        assert "AVG" in query.sql
        assert "price" in query.sql
        assert "avg_price" in query.sql

    def test_select_max(self) -> None:
        """Test MAX function."""
        builder = SelectBuilder().max_("created_at", "latest").from_("posts")

        query = builder.build()

        assert "MAX" in query.sql
        assert "created_at" in query.sql
        assert "latest" in query.sql

    def test_select_min(self) -> None:
        """Test MIN function."""
        builder = SelectBuilder().min_("price", "lowest_price").from_("products")

        query = builder.build()

        assert "MIN" in query.sql
        assert "price" in query.sql
        assert "lowest_price" in query.sql


class TestSelectBuilderWindowFunctions:
    """Test window function support."""

    def test_window_function_basic(self) -> None:
        """Test basic window function."""
        builder = SelectBuilder().window("ROW_NUMBER()", alias="row_num").from_("users")

        query = builder.build()

        assert "ROW_NUMBER()" in query.sql
        assert "OVER" in query.sql
        assert "row_num" in query.sql

    def test_window_function_with_partition(self) -> None:
        """Test window function with PARTITION BY."""
        builder = (
            SelectBuilder().window("ROW_NUMBER()", partition_by="department", alias="dept_row_num").from_("employees")
        )

        query = builder.build()

        assert "ROW_NUMBER()" in query.sql
        assert "PARTITION BY" in query.sql
        assert "department" in query.sql

    def test_window_function_with_order(self) -> None:
        """Test window function with ORDER BY."""
        builder = SelectBuilder().window("ROW_NUMBER()", order_by="salary", alias="salary_rank").from_("employees")

        query = builder.build()

        assert "ROW_NUMBER()" in query.sql
        assert "ORDER BY" in query.sql
        assert "salary" in query.sql

    def test_window_function_with_partition_and_order(self) -> None:
        """Test window function with both PARTITION BY and ORDER BY."""
        builder = (
            SelectBuilder()
            .window("RANK()", partition_by="department", order_by="salary", alias="dept_salary_rank")
            .from_("employees")
        )

        query = builder.build()

        assert "RANK()" in query.sql
        assert "PARTITION BY" in query.sql
        assert "ORDER BY" in query.sql
        assert "department" in query.sql
        assert "salary" in query.sql

    def test_window_function_multiple_partitions(self) -> None:
        """Test window function with multiple partition columns."""
        builder = (
            SelectBuilder()
            .window("COUNT(*)", partition_by=["department", "location"], alias="dept_location_count")
            .from_("employees")
        )

        query = builder.build()

        assert "COUNT(*)" in query.sql or ("COUNT" in query.sql and "*" in query.sql)
        assert "PARTITION BY" in query.sql
        assert "department" in query.sql
        assert "location" in query.sql

    def test_window_function_with_frame(self) -> None:
        """Test window function with frame specification."""
        builder = (
            SelectBuilder()
            .window(
                "SUM(amount)",
                order_by="date",
                frame="ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                alias="running_total",
            )
            .from_("transactions")
        )

        query = builder.build()

        assert "SUM" in query.sql
        assert "amount" in query.sql
        assert "ROWS BETWEEN" in query.sql or "UNBOUNDED PRECEDING" in query.sql


class TestCaseBuilder:
    """Test CASE expression builder."""

    def test_case_when_else_basic(self) -> None:
        """Test basic CASE WHEN ELSE expression."""
        builder = (
            SelectBuilder()
            .case_("status_text")
            .when("status = 1", "Active")
            .when("status = 2", "Inactive")
            .else_("Unknown")
            .end()
            .from_("users")
        )

        query = builder.build()

        assert "CASE" in query.sql
        assert "WHEN" in query.sql
        assert "ELSE" in query.sql
        assert "END" in query.sql
        assert "status_text" in query.sql
        # Parameters should contain the values
        assert "Active" in query.parameters.values()
        assert "Inactive" in query.parameters.values()
        assert "Unknown" in query.parameters.values()

    def test_case_without_else(self) -> None:
        """Test CASE expression without ELSE clause."""
        builder = (
            SelectBuilder()
            .case_("priority_text")
            .when("priority = 1", "High")
            .when("priority = 2", "Medium")
            .end()
            .from_("tasks")
        )

        query = builder.build()

        assert "CASE" in query.sql
        assert "WHEN" in query.sql
        assert "END" in query.sql
        # Should not contain ELSE
        assert "ELSE" not in query.sql
        assert "High" in query.parameters.values()
        assert "Medium" in query.parameters.values()

    def test_case_multiple_conditions(self) -> None:
        """Test CASE with multiple WHEN conditions."""
        builder = (
            SelectBuilder()
            .case_("grade")
            .when("score >= 90", "A")
            .when("score >= 80", "B")
            .when("score >= 70", "C")
            .when("score >= 60", "D")
            .else_("F")
            .end()
            .from_("students")
        )

        query = builder.build()

        assert "CASE" in query.sql
        # Should have multiple WHEN clauses
        when_count = query.sql.count("WHEN")
        assert when_count == 4
        assert "A" in query.parameters.values()
        assert "B" in query.parameters.values()
        assert "C" in query.parameters.values()
        assert "D" in query.parameters.values()
        assert "F" in query.parameters.values()

    def test_case_with_expression_conditions(self) -> None:
        """Test CASE with sqlglot expression conditions."""
        condition_expr = exp.GT(this=exp.column("age"), expression=exp.Literal.number(18))

        builder = SelectBuilder().case_("age_group").when(condition_expr, "Adult").else_("Minor").end().from_("people")

        query = builder.build()

        assert "CASE" in query.sql
        assert "WHEN" in query.sql
        assert "Adult" in query.parameters.values()
        assert "Minor" in query.parameters.values()

    def test_case_fluent_chaining(self) -> None:
        """Test that CASE builder returns to SelectBuilder for continued chaining."""
        builder = (
            SelectBuilder()
            .select("id", "name")
            .case_("status_desc")
            .when("status = 1", "Active")
            .else_("Inactive")
            .end()
            .from_("users")
            .where(("active", True))
            .order_by("name")
        )

        query = builder.build()

        assert "SELECT" in query.sql
        assert "id" in query.sql
        assert "name" in query.sql
        assert "CASE" in query.sql
        assert "FROM users" in query.sql
        assert "WHERE" in query.sql
        assert "ORDER BY" in query.sql
        assert True in query.parameters.values()


class TestSelectBuilderEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_values_in_where_in(self) -> None:
        """Test WHERE IN with empty list should handle gracefully."""
        builder = SelectBuilder().select("*").from_("users").where_in("id", [])
        query = builder.build()

        # Should still generate valid SQL
        assert "SELECT" in query.sql
        assert "FROM users" in query.sql

    def test_single_value_in_where_in(self) -> None:
        """Test WHERE IN with single value."""
        builder = SelectBuilder().select("*").from_("users").where_in("id", [42])
        query = builder.build()

        assert "IN" in query.sql
        assert 42 in query.parameters.values()

    def test_none_values_in_parameters(self) -> None:
        """Test handling of None values in parameters."""
        builder = SelectBuilder().select("*").from_("users").where(("deleted_at", None))
        query = builder.build()

        assert None in query.parameters.values()

    def test_complex_nested_conditions(self) -> None:
        """Test complex nested conditions with multiple helpers."""
        subquery = SelectBuilder().select("user_id").from_("orders").where_between("total", 100, 1000)

        builder = (
            SelectBuilder()
            .select("id", "name", "email")
            .from_("users")
            .where_in("id", subquery)
            .where_not_null("email")
            .where_like("name", "%John%")
            .where_between("age", 18, 65)
        )

        query = builder.build()

        assert "SELECT" in query.sql
        assert "IN" in query.sql
        assert "IS NOT NULL" in query.sql or ("IS" in query.sql and "NOT NULL" in query.sql)
        assert "LIKE" in query.sql
        assert "BETWEEN" in query.sql
        # Parameters from main query and subquery
        assert "%John%" in query.parameters.values()
        assert 18 in query.parameters.values()
        assert 65 in query.parameters.values()
        assert 100 in query.parameters.values()
        assert 1000 in query.parameters.values()

    def test_unsupported_values_type_where_in(self) -> None:
        """Test WHERE IN with unsupported values type raises error."""
        with pytest.raises(SQLBuilderError) as exc_info:
            SelectBuilder().select("*").from_("users").where_in("id", 42)  # pyright: ignore[reportArgumentType]

        assert "Unsupported values type for IN clause" in str(exc_info.value)

    def test_invalid_subquery_parsing(self) -> None:
        """Test invalid subquery string parsing raises error."""
        with pytest.raises(SQLBuilderError) as exc_info:
            SelectBuilder().select("*").from_("users").where_exists("INVALID SQL SYNTAX")

        assert "Could not parse subquery" in str(exc_info.value)


class TestSelectBuilderSQLInjectionPrevention:
    """Test SQL injection prevention and parameter safety."""

    def test_malicious_string_in_where_clause(self) -> None:
        """Test that malicious strings are properly parameterized."""
        malicious_input = "'; DROP TABLE users; --"
        builder = SelectBuilder().select("*").from_("users").where(("name", malicious_input))

        query = builder.build()

        # The malicious input should be in parameters, not directly in SQL
        assert malicious_input in query.parameters.values()
        assert "DROP TABLE" not in query.sql
        assert "';" not in query.sql or query.sql.count("';") == 0  # No SQL injection

    def test_malicious_string_in_like_pattern(self) -> None:
        """Test that malicious LIKE patterns are properly parameterized."""
        malicious_pattern = "%'; DELETE FROM users WHERE '1'='1"
        builder = SelectBuilder().select("*").from_("users").where_like("name", malicious_pattern)

        query = builder.build()

        assert malicious_pattern in query.parameters.values()
        assert "DELETE FROM" not in query.sql

    def test_malicious_values_in_where_in(self) -> None:
        """Test that malicious values in IN clause are properly parameterized."""
        malicious_values = ["1'; DROP TABLE users; --", "2'; DELETE FROM users; --"]
        builder = SelectBuilder().select("*").from_("users").where_in("id", malicious_values)

        query = builder.build()

        # All malicious values should be in parameters
        for value in malicious_values:
            assert value in query.parameters.values()
        assert "DROP TABLE" not in query.sql
        assert "DELETE FROM" not in query.sql

    def test_malicious_case_values(self) -> None:
        """Test that malicious values in CASE expressions are properly parameterized."""
        malicious_value = "'; DROP TABLE users; SELECT '"

        builder = (
            SelectBuilder().case_("result").when("status = 1", malicious_value).else_("Safe").end().from_("test_table")
        )

        query = builder.build()

        assert malicious_value in query.parameters.values()
        assert "DROP TABLE" not in query.sql

    def test_safe_column_references(self) -> None:
        """Test that column references in conditions are not parameterized."""
        # This should create a condition comparing two columns, not parameterizing the second column name
        builder = SelectBuilder().select("*").from_("users").where("users.id = orders.user_id")

        query = builder.build()

        # Column references should be in the SQL directly
        assert "users.id" in query.sql
        assert "orders.user_id" in query.sql


class TestSelectBuilderComplexScenarios:
    """Test complex real-world scenarios combining multiple features."""

    def test_complex_analytics_query(self) -> None:
        """Test complex analytics query with multiple features."""
        subquery = (
            SelectBuilder()
            .select("department")
            .avg_("salary", "dept_avg_salary")
            .from_("employees")
            .group_by("department")
        )

        builder = (
            SelectBuilder()
            .select("e.id", "e.name", "e.department", "e.salary")
            .case_("performance_tier")
            .when("e.salary > dept_avg.dept_avg_salary * 1.2", "High Performer")
            .when("e.salary > dept_avg.dept_avg_salary * 0.8", "Average Performer")
            .else_("Needs Improvement")
            .end()
            .window("RANK()", partition_by="e.department", order_by="e.salary", alias="salary_rank")
            .from_("employees e")
            .left_join(f"({subquery.build().sql}) dept_avg", "e.department = dept_avg.department")
            .where_not_null("e.salary")
            .where_in("e.status", ["active", "on_leave"])
            .order_by("e.department", "salary_rank")
        )

        query = builder.build()

        # Check that all components are present
        assert "SELECT" in query.sql
        assert "CASE" in query.sql
        assert "RANK()" in query.sql
        assert "PARTITION BY" in query.sql
        assert "LEFT JOIN" in query.sql
        assert "IS NOT NULL" in query.sql or ("IS" in query.sql and "NOT NULL" in query.sql)
        assert "IN" in query.sql
        assert "ORDER BY" in query.sql

        # Check parameterized values
        assert "High Performer" in query.parameters.values()
        assert "Average Performer" in query.parameters.values()
        assert "Needs Improvement" in query.parameters.values()
        assert "active" in query.parameters.values()
        assert "on_leave" in query.parameters.values()

    def test_complex_reporting_query_with_unions(self) -> None:
        """Test complex reporting query with multiple UNION operations."""
        current_orders = (
            SelectBuilder()
            .select("'current'", "customer_id", "order_date", "total")
            .from_("orders")
            .where_between("order_date", "2024-01-01", "2024-12-31")
        )

        archived_orders = (
            SelectBuilder()
            .select("'archived'", "customer_id", "order_date", "total")
            .from_("archived_orders")
            .where_between("order_date", "2024-01-01", "2024-12-31")
        )

        pending_orders = (
            SelectBuilder()
            .select("'pending'", "customer_id", "estimated_date", "estimated_total")
            .from_("pending_orders")
            .where_not_null("estimated_date")
        )

        # Combine all three queries
        combined = current_orders.union(archived_orders).union(pending_orders, all_=True)

        query = combined.build()

        assert "UNION" in query.sql
        assert "orders" in query.sql
        assert "archived_orders" in query.sql
        assert "pending_orders" in query.sql
        # Date parameters should be present
        assert "2024-01-01" in query.parameters.values()
        assert "2024-12-31" in query.parameters.values()

    def test_data_quality_check_query(self) -> None:
        """Test data quality check query using multiple condition helpers."""
        builder = (
            SelectBuilder()
            .select("table_name", "issue_type", "record_count")
            .from_("""
                    (
                      SELECT 'users' as table_name, 'missing_email' as issue_type, COUNT(*) as record_count
                      FROM users WHERE email IS NULL
                      UNION ALL
                      SELECT 'users', 'invalid_email', COUNT(*)
                      FROM users WHERE email NOT LIKE '%@%'
                      UNION ALL
                      SELECT 'orders', 'negative_amount', COUNT(*)
                      FROM orders WHERE amount < 0
                    ) quality_issues
                  """)
            .where("record_count > 0")
            .order_by("table_name", "issue_type")
        )

        query = builder.build()

        assert "SELECT" in query.sql
        assert "table_name" in query.sql
        assert "issue_type" in query.sql
        assert "record_count" in query.sql
        assert "WHERE" in query.sql
        assert "ORDER BY" in query.sql


class TestSelectBuilderDialectSupport:
    """Test dialect-specific features and compatibility."""

    def test_postgresql_dialect(self) -> None:
        """Test PostgreSQL-specific features."""
        builder = SelectBuilder(dialect="postgres")
        builder.select("id", "name").from_("users").where(("active", True))

        query = builder.build()

        assert "SELECT" in query.sql
        assert True in query.parameters.values()

    def test_mysql_dialect(self) -> None:
        """Test MySQL-specific features."""
        builder = SelectBuilder(dialect="mysql")
        builder.select("id", "name").from_("users").limit(10).offset(20)

        query = builder.build()

        assert "SELECT" in query.sql
        assert "LIMIT" in query.sql
        assert "OFFSET" in query.sql or "LIMIT 20, 10" in query.sql  # MySQL can use LIMIT offset, count

    def test_sqlite_dialect(self) -> None:
        """Test SQLite-specific features."""
        builder = SelectBuilder(dialect="sqlite")
        builder.select("id", "name").from_("users").where_like("name", "%test%")

        query = builder.build()

        assert "SELECT" in query.sql
        assert "LIKE" in query.sql
        assert "%test%" in query.parameters.values()


class TestSelectBuilderErrorHandling:
    """Test error handling and validation."""

    def test_invalid_window_function_expression(self) -> None:
        """Test invalid window function expression raises error."""
        with pytest.raises(SQLBuilderError) as exc_info:
            SelectBuilder().window("INVALID_FUNCTION_SYNTAX()").from_("test")

        assert "Could not parse function expression" in str(exc_info.value)

    def test_invalid_case_builder_usage(self) -> None:
        """Test that CaseBuilder requires proper usage."""
        # This should work fine - just testing the basic flow
        case_builder = CaseBuilder(SelectBuilder(), "test_alias")
        assert case_builder._alias == "test_alias"
        assert isinstance(case_builder._case_expr, exp.Case)

    def test_parameter_naming_consistency(self) -> None:
        """Test that parameter naming is consistent and doesn't conflict."""
        builder = (
            SelectBuilder()
            .select("*")
            .from_("users")
            .where_like("name", "%test%")
            .where_between("age", 18, 65)
            .where_in("status", ["active", "pending"])
        )

        query = builder.build()

        # All parameter names should be unique
        param_names = list(query.parameters.keys())
        assert len(param_names) == len(set(param_names))  # No duplicates

        # All values should be preserved
        assert "%test%" in query.parameters.values()
        assert 18 in query.parameters.values()
        assert 65 in query.parameters.values()
        assert "active" in query.parameters.values()
        assert "pending" in query.parameters.values()


class TestSelectBuilderPerformance:
    """Test performance-related aspects and optimization."""

    def test_large_in_clause_parameters(self) -> None:
        """Test handling of large IN clause with many parameters."""
        large_list = list(range(1000))  # 1000 parameters
        builder = SelectBuilder().select("*").from_("users").where_in("id", large_list)

        query = builder.build()

        assert "IN" in query.sql
        assert len(query.parameters) == 1000
        # Check that all values are present
        for value in large_list:
            assert value in query.parameters.values()

    def test_complex_nested_query_parameters(self) -> None:
        """Test parameter handling in deeply nested queries."""
        level3 = SelectBuilder().select("user_id").from_("transactions").where_between("amount", 1000, 5000)

        level2 = (
            SelectBuilder()
            .select("user_id")
            .from_("orders")
            .where_in("user_id", level3)
            .where_like("status", "completed%")
        )

        level1 = (
            SelectBuilder()
            .select("id", "name", "email")
            .from_("users")
            .where_in("id", level2)
            .where_not_null("email")
            .where_between("created_at", "2024-01-01", "2024-12-31")
        )

        query = level1.build()

        # Should contain parameters from all levels
        assert 1000 in query.parameters.values()  # level3
        assert 5000 in query.parameters.values()  # level3
        assert "completed%" in query.parameters.values()  # level2
        assert "2024-01-01" in query.parameters.values()  # level1
        assert "2024-12-31" in query.parameters.values()  # level1

        # Should have reasonable SQL structure
        assert "SELECT" in query.sql
        assert "FROM users" in query.sql
        assert "IN" in query.sql
        assert "LIKE" in query.sql
        assert "BETWEEN" in query.sql
        assert "IS NOT NULL" in query.sql or ("IS" in query.sql and "NOT NULL" in query.sql)
