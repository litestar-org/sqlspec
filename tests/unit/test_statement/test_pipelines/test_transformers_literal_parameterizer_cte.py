"""Test literal parameterization behavior with CTEs."""

from sqlglot import parse_one

from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.transformers._literal_parameterizer import ParameterizeLiterals
from sqlspec.statement.sql import SQLConfig


class TestCTEParameterization:
    """Test literal parameterization with Common Table Expressions."""

    def test_simple_cte_parameterizes_literals(self) -> None:
        """Test that simple CTEs still have their literals parameterized."""
        sql = """
        WITH simple_cte AS (
            SELECT id, name
            FROM users
            WHERE age > 25
        )
        SELECT * FROM simple_cte WHERE id = 123
        """

        expression = parse_one(sql, dialect="postgres")
        context = SQLProcessingContext(
            initial_sql_string=sql, current_expression=expression, config=SQLConfig(), dialect="postgres"
        )

        parameterizer = ParameterizeLiterals(placeholder_style="$1")
        result = parameterizer.process(expression, context)

        # Should parameterize both 25 and 123
        assert len(parameterizer.get_parameters()) == 2
        values = [p.value for p in parameterizer.get_parameters()]
        assert 25 in values
        assert 123 in values

        sql_output = result.sql(dialect="postgres") if result else ""
        assert "$1" in sql_output
        assert "$2" in sql_output
        assert "25" not in sql_output
        assert "123" not in sql_output

    def test_recursive_cte_preserves_literals_by_default(self) -> None:
        """Test that recursive CTEs preserve their literals by default."""
        sql = """
        WITH RECURSIVE factorial AS (
            SELECT 1 as n, 1 as fact
            UNION ALL
            SELECT n + 1, fact * (n + 1)
            FROM factorial
            WHERE n < 10
        )
        SELECT * FROM factorial WHERE n = 5
        """

        expression = parse_one(sql, dialect="postgres")
        context = SQLProcessingContext(
            initial_sql_string=sql, current_expression=expression, config=SQLConfig(), dialect="postgres"
        )

        parameterizer = ParameterizeLiterals(placeholder_style="$1")
        result = parameterizer.process(expression, context)

        # Should only parameterize the 5 in the outer query, not literals in recursive CTE
        assert len(parameterizer.get_parameters()) == 1
        assert parameterizer.get_parameters()[0].value == 5

        sql_output = result.sql(dialect="postgres") if result else ""
        # Literals in recursive CTE should be preserved
        assert "1 AS n" in sql_output
        assert "1 AS fact" in sql_output
        assert "n + 1" in sql_output
        assert "n < 10" in sql_output
        # Only the outer query literal should be parameterized
        assert "n = $1" in sql_output

    def test_recursive_cte_with_preserve_disabled(self) -> None:
        """Test that recursive CTE literals can be parameterized if explicitly disabled."""
        sql = """
        WITH RECURSIVE counter AS (
            SELECT 1 as value
            UNION ALL
            SELECT value + 1
            FROM counter
            WHERE value < 5
        )
        SELECT * FROM counter
        """

        expression = parse_one(sql, dialect="postgres")
        context = SQLProcessingContext(
            initial_sql_string=sql, current_expression=expression, config=SQLConfig(), dialect="postgres"
        )

        # Disable preservation of literals in recursive CTEs
        parameterizer = ParameterizeLiterals(placeholder_style="$1", preserve_in_recursive_cte=False)
        result = parameterizer.process(expression, context)

        # Should parameterize literals in expressions: 1 (in value + 1) and 5
        # Note: The '1 as value' is preserved as it's an alias expression
        assert len(parameterizer.get_parameters()) == 2
        values = [p.value for p in parameterizer.get_parameters()]
        assert 1 in values
        assert 5 in values

        sql_output = result.sql(dialect="postgres") if result else ""
        # Literals in expressions should be parameterized
        assert "$1" in sql_output
        assert "$2" in sql_output
        # But alias values remain as literals
        assert "1 AS value" in sql_output

    def test_nested_recursive_ctes(self) -> None:
        """Test behavior with nested CTEs where one is recursive."""
        sql = """
        WITH RECURSIVE rec_cte AS (
            SELECT 1 as level
            UNION ALL
            SELECT level + 1
            FROM rec_cte
            WHERE level < 3
        ),
        normal_cte AS (
            SELECT * FROM rec_cte WHERE level > 2
        )
        SELECT * FROM normal_cte WHERE level = 3
        """

        expression = parse_one(sql, dialect="postgres")
        context = SQLProcessingContext(
            initial_sql_string=sql, current_expression=expression, config=SQLConfig(), dialect="postgres"
        )

        parameterizer = ParameterizeLiterals(placeholder_style="$1")
        result = parameterizer.process(expression, context)

        # Should parameterize literals outside recursive CTE
        assert len(parameterizer.get_parameters()) == 2
        values = [p.value for p in parameterizer.get_parameters()]
        assert 2 in values  # From normal_cte
        assert 3 in values  # From outer query

        sql_output = result.sql(dialect="postgres") if result else ""
        # Recursive CTE literals preserved
        assert "1 AS level" in sql_output
        assert "level + 1" in sql_output
        assert "level < 3" in sql_output
        # Other literals parameterized (order may vary)
        assert "level > $" in sql_output
        assert "level = $" in sql_output

    def test_multiple_recursive_ctes(self) -> None:
        """Test with multiple recursive CTEs in the same query."""
        sql = """
        WITH RECURSIVE
        fib AS (
            SELECT 0 as n, 0 as val
            UNION ALL
            SELECT 1, 1
            UNION ALL
            SELECT n + 1, val + lag_val
            FROM (
                SELECT n, val, LAG(val, 1, 0) OVER (ORDER BY n) as lag_val
                FROM fib
            ) t
            WHERE n < 10
        ),
        powers AS (
            SELECT 1 as n, 2 as val
            UNION ALL
            SELECT n + 1, val * 2
            FROM powers
            WHERE n < 5
        )
        SELECT * FROM fib, powers WHERE fib.n = powers.n
        """

        expression = parse_one(sql, dialect="postgres")
        context = SQLProcessingContext(
            initial_sql_string=sql, current_expression=expression, config=SQLConfig(), dialect="postgres"
        )

        parameterizer = ParameterizeLiterals(placeholder_style="$1")
        result = parameterizer.process(expression, context)

        # All literals in recursive CTEs should be preserved
        assert len(parameterizer.get_parameters()) == 0

        sql_output = result.sql(dialect="postgres") if result else ""
        # All literals should remain as-is
        assert "0 AS n" in sql_output
        assert "0 AS val" in sql_output
        assert "n < 10" in sql_output
        assert "n < 5" in sql_output
        assert "val * 2" in sql_output
