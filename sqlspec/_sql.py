"""Unified SQL factory for creating SQL builders and column expressions with a clean API.

Provides both statement builders (select, insert, update, etc.) and column expressions.
"""

import logging
from typing import Any, Optional, Union

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.builder import Column, Delete, Insert, Merge, Select, Truncate, Update
from sqlspec.exceptions import SQLBuilderError

__all__ = ("Case", "Column", "Delete", "Insert", "Merge", "SQLFactory", "Select", "Truncate", "Update", "sql")

logger = logging.getLogger("sqlspec")

MIN_SQL_LIKE_STRING_LENGTH = 6
MIN_DECODE_ARGS = 2
SQL_STARTERS = {
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "WITH",
    "CALL",
    "DECLARE",
    "BEGIN",
    "END",
    "CREATE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "RENAME",
    "GRANT",
    "REVOKE",
    "SET",
    "SHOW",
    "USE",
    "EXPLAIN",
    "OPTIMIZE",
    "VACUUM",
    "COPY",
}


class SQLFactory:
    """Unified factory for creating SQL builders and column expressions with a fluent API."""

    @classmethod
    def detect_sql_type(cls, sql: str, dialect: DialectType = None) -> str:
        try:
            parsed_expr = sqlglot.parse_one(sql, read=dialect)
            if parsed_expr and parsed_expr.key:
                return parsed_expr.key.upper()
            if parsed_expr:
                command_type = type(parsed_expr).__name__.upper()
                if command_type == "COMMAND" and parsed_expr.this:
                    return str(parsed_expr.this).upper()
                return command_type
        except SQLGlotParseError:
            logger.debug("Failed to parse SQL for type detection: %s", sql[:100])
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning("Unexpected error during SQL type detection for '%s...': %s", sql[:50], e)
        return "UNKNOWN"

    def __init__(self, dialect: DialectType = None) -> None:
        """Initialize the SQL factory.

        Args:
            dialect: Default SQL dialect to use for all builders.
        """
        self.dialect = dialect

    # ===================
    # Callable Interface
    # ===================
    def __call__(self, statement: str, dialect: DialectType = None) -> "Any":
        """Create a SelectBuilder from a SQL string, only allowing SELECT/CTE queries.

        Args:
            statement: The SQL statement string.
            parameters: Optional parameters for the query.
            *filters: Optional filters.
            config: Optional config.
            dialect: Optional SQL dialect.
            **kwargs: Additional parameters.

        Returns:
            SelectBuilder instance.

        Raises:
            SQLBuilderError: If the SQL is not a SELECT/CTE statement.
        """

        try:
            parsed_expr = sqlglot.parse_one(statement, read=dialect or self.dialect)
        except Exception as e:
            msg = f"Failed to parse SQL: {e}"
            raise SQLBuilderError(msg) from e
        actual_type = type(parsed_expr).__name__.upper()
        expr_type_map = {
            "SELECT": "SELECT",
            "INSERT": "INSERT",
            "UPDATE": "UPDATE",
            "DELETE": "DELETE",
            "MERGE": "MERGE",
            "WITH": "WITH",
        }
        actual_type_str = expr_type_map.get(actual_type, actual_type)
        if actual_type_str == "SELECT" or (
            actual_type_str == "WITH" and parsed_expr.this and isinstance(parsed_expr.this, exp.Select)
        ):
            builder = Select(dialect=dialect or self.dialect)
            builder._expression = parsed_expr
            return builder
        msg = (
            f"sql(...) only supports SELECT statements. Detected type: {actual_type_str}. "
            f"Use sql.{actual_type_str.lower()}() instead."
        )
        raise SQLBuilderError(msg)

    # ===================
    # Statement Builders
    # ===================
    def select(self, *columns_or_sql: Union[str, exp.Expression, Column], dialect: DialectType = None) -> "Select":
        builder_dialect = dialect or self.dialect
        if len(columns_or_sql) == 1 and isinstance(columns_or_sql[0], str):
            sql_candidate = columns_or_sql[0].strip()
            if self._looks_like_sql(sql_candidate):
                detected = self.detect_sql_type(sql_candidate, dialect=builder_dialect)
                if detected not in {"SELECT", "WITH"}:
                    msg = (
                        f"sql.select() expects a SELECT or WITH statement, got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists, or ensure the SQL is SELECT/WITH."
                    )
                    raise SQLBuilderError(msg)
                select_builder = Select(dialect=builder_dialect)
                if select_builder._expression is None:
                    select_builder.__post_init__()
                return self._populate_select_from_sql(select_builder, sql_candidate)
        select_builder = Select(dialect=builder_dialect)
        if select_builder._expression is None:
            select_builder.__post_init__()
        if columns_or_sql:
            select_builder.select(*columns_or_sql)
        return select_builder

    def insert(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "Insert":
        builder_dialect = dialect or self.dialect
        builder = Insert(dialect=builder_dialect)
        if builder._expression is None:
            builder.__post_init__()
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                detected = self.detect_sql_type(table_or_sql, dialect=builder_dialect)
                if detected not in {"INSERT", "SELECT"}:
                    msg = (
                        f"sql.insert() expects INSERT or SELECT (for insert-from-select), got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists, "
                        f"or ensure the SQL is INSERT/SELECT."
                    )
                    raise SQLBuilderError(msg)
                return self._populate_insert_from_sql(builder, table_or_sql)
            return builder.into(table_or_sql)
        return builder

    def update(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "Update":
        builder_dialect = dialect or self.dialect
        builder = Update(dialect=builder_dialect)
        if builder._expression is None:
            builder.__post_init__()
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                detected = self.detect_sql_type(table_or_sql, dialect=builder_dialect)
                if detected != "UPDATE":
                    msg = f"sql.update() expects UPDATE statement, got {detected}. Use sql.{detected.lower()}() if a dedicated builder exists."
                    raise SQLBuilderError(msg)
                return self._populate_update_from_sql(builder, table_or_sql)
            return builder.table(table_or_sql)
        return builder

    def delete(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "Delete":
        builder_dialect = dialect or self.dialect
        builder = Delete(dialect=builder_dialect)
        if builder._expression is None:
            builder.__post_init__()
        if table_or_sql and self._looks_like_sql(table_or_sql):
            detected = self.detect_sql_type(table_or_sql, dialect=builder_dialect)
            if detected != "DELETE":
                msg = f"sql.delete() expects DELETE statement, got {detected}. Use sql.{detected.lower()}() if a dedicated builder exists."
                raise SQLBuilderError(msg)
            return self._populate_delete_from_sql(builder, table_or_sql)
        return builder

    def merge(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "Merge":
        builder_dialect = dialect or self.dialect
        builder = Merge(dialect=builder_dialect)
        if builder._expression is None:
            builder.__post_init__()
        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                detected = self.detect_sql_type(table_or_sql, dialect=builder_dialect)
                if detected != "MERGE":
                    msg = f"sql.merge() expects MERGE statement, got {detected}. Use sql.{detected.lower()}() if a dedicated builder exists."
                    raise SQLBuilderError(msg)
                return self._populate_merge_from_sql(builder, table_or_sql)
            return builder.into(table_or_sql)
        return builder

    # ===================
    # SQL Analysis Helpers
    # ===================

    @staticmethod
    def _looks_like_sql(candidate: str, expected_type: Optional[str] = None) -> bool:
        """Efficiently determine if a string looks like SQL.

        Args:
            candidate: String to check
            expected_type: Expected SQL statement type (SELECT, INSERT, etc.)

        Returns:
            True if the string appears to be SQL
        """
        if not candidate or len(candidate.strip()) < MIN_SQL_LIKE_STRING_LENGTH:
            return False

        candidate_upper = candidate.strip().upper()

        if expected_type:
            return candidate_upper.startswith(expected_type.upper())

        # More sophisticated check for SQL vs column names
        # Column names that start with SQL keywords are common (user_id, insert_date, etc.)
        if any(candidate_upper.startswith(starter) for starter in SQL_STARTERS):
            # Additional checks to distinguish real SQL from column names:
            # 1. Real SQL typically has spaces (SELECT ... FROM, INSERT INTO, etc.)
            # 2. Check for common SQL syntax patterns
            return " " in candidate

        return False

    def _populate_insert_from_sql(self, builder: "Insert", sql_string: str) -> "Insert":
        """Parse SQL string and populate INSERT builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]

            if isinstance(parsed_expr, exp.Insert):
                builder._expression = parsed_expr
                return builder

            if isinstance(parsed_expr, exp.Select):
                # The actual conversion logic can be handled by the builder itself
                logger.info("Detected SELECT statement for INSERT - may need target table specification")
                return builder

            # For other statement types, just return the builder as-is
            logger.warning("Cannot create INSERT from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse INSERT SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_select_from_sql(self, builder: "Select", sql_string: str) -> "Select":
        """Parse SQL string and populate SELECT builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]

            if isinstance(parsed_expr, exp.Select):
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create SELECT from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse SELECT SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_update_from_sql(self, builder: "Update", sql_string: str) -> "Update":
        """Parse SQL string and populate UPDATE builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]

            if isinstance(parsed_expr, exp.Update):
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create UPDATE from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse UPDATE SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_delete_from_sql(self, builder: "Delete", sql_string: str) -> "Delete":
        """Parse SQL string and populate DELETE builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]

            if isinstance(parsed_expr, exp.Delete):
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create DELETE from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse DELETE SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_merge_from_sql(self, builder: "Merge", sql_string: str) -> "Merge":
        """Parse SQL string and populate MERGE builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]

            if isinstance(parsed_expr, exp.Merge):
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create MERGE from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse MERGE SQL, falling back to traditional mode: %s", e)
        return builder

    # ===================
    # Column References
    # ===================

    def column(self, name: str, table: Optional[str] = None) -> Column:
        """Create a column reference.

        Args:
            name: Column name.
            table: Optional table name.

        Returns:
            Column object that supports method chaining and operator overloading.
        """
        return Column(name, table)

    def __getattr__(self, name: str) -> Column:
        """Dynamically create column references.

        Args:
            name: Column name.

        Returns:
            Column object that supports method chaining and operator overloading.
        """
        return Column(name)

    # ===================
    # Raw SQL Expressions
    # ===================

    @staticmethod
    def raw(sql_fragment: str) -> exp.Expression:
        """Create a raw SQL expression from a string fragment.

        This method makes it explicit that you are passing raw SQL that should
        be parsed and included directly in the query. Useful for complex expressions,
        database-specific functions, or when you need precise control over the SQL.

        Args:
            sql_fragment: Raw SQL string to parse into an expression.

        Returns:
            SQLGlot expression from the parsed SQL fragment.

        Raises:
            SQLBuilderError: If the SQL fragment cannot be parsed.

        Example:
            ```python
            # Raw column expression with alias
            query = sql.select(
                sql.raw("user.id AS u_id"), "name"
            ).from_("users")

            # Raw function call
            query = sql.select(
                sql.raw("COALESCE(name, 'Unknown')")
            ).from_("users")

            # Raw complex expression
            query = (
                sql.select("*")
                .from_("orders")
                .where(sql.raw("DATE(created_at) = CURRENT_DATE"))
            )

            # Raw window function
            query = sql.select(
                "name",
                sql.raw(
                    "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC)"
                ),
            ).from_("employees")
            ```
        """
        try:
            parsed: Optional[exp.Expression] = exp.maybe_parse(sql_fragment)
            if parsed is not None:
                return parsed
            if sql_fragment.strip().replace("_", "").replace(".", "").isalnum():
                return exp.to_identifier(sql_fragment)
            return exp.Literal.string(sql_fragment)
        except Exception as e:
            msg = f"Failed to parse raw SQL fragment '{sql_fragment}': {e}"
            raise SQLBuilderError(msg) from e

    # ===================
    # Aggregate Functions
    # ===================

    @staticmethod
    def count(column: Union[str, exp.Expression] = "*", distinct: bool = False) -> exp.Expression:
        """Create a COUNT expression.

        Args:
            column: Column to count (default "*").
            distinct: Whether to use COUNT DISTINCT.

        Returns:
            COUNT expression.
        """
        if column == "*":
            return exp.Count(this=exp.Star(), distinct=distinct)
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Count(this=col_expr, distinct=distinct)

    def count_distinct(self, column: Union[str, exp.Expression]) -> exp.Expression:
        """Create a COUNT(DISTINCT column) expression.

        Args:
            column: Column to count distinct values.

        Returns:
            COUNT DISTINCT expression.
        """
        return self.count(column, distinct=True)

    @staticmethod
    def sum(column: Union[str, exp.Expression], distinct: bool = False) -> exp.Expression:
        """Create a SUM expression.

        Args:
            column: Column to sum.
            distinct: Whether to use SUM DISTINCT.

        Returns:
            SUM expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Sum(this=col_expr, distinct=distinct)

    @staticmethod
    def avg(column: Union[str, exp.Expression]) -> exp.Expression:
        """Create an AVG expression.

        Args:
            column: Column to average.

        Returns:
            AVG expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Avg(this=col_expr)

    @staticmethod
    def max(column: Union[str, exp.Expression]) -> exp.Expression:
        """Create a MAX expression.

        Args:
            column: Column to find maximum.

        Returns:
            MAX expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Max(this=col_expr)

    @staticmethod
    def min(column: Union[str, exp.Expression]) -> exp.Expression:
        """Create a MIN expression.

        Args:
            column: Column to find minimum.

        Returns:
            MIN expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Min(this=col_expr)

    # ===================
    # Advanced SQL Operations
    # ===================

    @staticmethod
    def rollup(*columns: Union[str, exp.Expression]) -> exp.Expression:
        """Create a ROLLUP expression for GROUP BY clauses.

        Args:
            *columns: Columns to include in the rollup.

        Returns:
            ROLLUP expression.

        Example:
            ```python
            # GROUP BY ROLLUP(product, region)
            query = (
                sql.select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by(sql.rollup("product", "region"))
            )
            ```
        """
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        return exp.Rollup(expressions=column_exprs)

    @staticmethod
    def cube(*columns: Union[str, exp.Expression]) -> exp.Expression:
        """Create a CUBE expression for GROUP BY clauses.

        Args:
            *columns: Columns to include in the cube.

        Returns:
            CUBE expression.

        Example:
            ```python
            # GROUP BY CUBE(product, region)
            query = (
                sql.select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by(sql.cube("product", "region"))
            )
            ```
        """
        column_exprs = [exp.column(col) if isinstance(col, str) else col for col in columns]
        return exp.Cube(expressions=column_exprs)

    @staticmethod
    def grouping_sets(*column_sets: Union[tuple[str, ...], list[str]]) -> exp.Expression:
        """Create a GROUPING SETS expression for GROUP BY clauses.

        Args:
            *column_sets: Sets of columns to group by.

        Returns:
            GROUPING SETS expression.

        Example:
            ```python
            # GROUP BY GROUPING SETS ((product), (region), ())
            query = (
                sql.select("product", "region", sql.sum("sales"))
                .from_("sales_data")
                .group_by(
                    sql.grouping_sets(("product",), ("region",), ())
                )
            )
            ```
        """
        set_expressions = []
        for column_set in column_sets:
            if isinstance(column_set, (tuple, list)):
                if len(column_set) == 0:
                    set_expressions.append(exp.Tuple(expressions=[]))
                else:
                    columns = [exp.column(col) for col in column_set]
                    set_expressions.append(exp.Tuple(expressions=columns))
            else:
                set_expressions.append(exp.column(column_set))

        return exp.GroupingSets(expressions=set_expressions)

    @staticmethod
    def any(values: Union[list[Any], exp.Expression, str]) -> exp.Expression:
        """Create an ANY expression for use with comparison operators.

        Args:
            values: Values, expression, or subquery for the ANY clause.

        Returns:
            ANY expression.

        Example:
            ```python
            # WHERE id = ANY(subquery)
            subquery = sql.select("user_id").from_("active_users")
            query = (
                sql.select("*")
                .from_("users")
                .where(sql.id.eq(sql.any(subquery)))
            )
            ```
        """
        if isinstance(values, list):
            literals = [SQLFactory._to_literal(v) for v in values]
            return exp.Any(this=exp.Array(expressions=literals))
        if isinstance(values, str):
            # Parse as SQL
            parsed = exp.maybe_parse(values)  # type: ignore[var-annotated]
            if parsed:
                return exp.Any(this=parsed)
            return exp.Any(this=exp.Literal.string(values))
        return exp.Any(this=values)

    @staticmethod
    def not_any_(values: Union[list[Any], exp.Expression, str]) -> exp.Expression:
        """Create a NOT ANY expression for use with comparison operators.

        Args:
            values: Values, expression, or subquery for the NOT ANY clause.

        Returns:
            NOT ANY expression.

        Example:
            ```python
            # WHERE id <> ANY(subquery)
            subquery = sql.select("user_id").from_("blocked_users")
            query = (
                sql.select("*")
                .from_("users")
                .where(sql.id.neq(sql.not_any(subquery)))
            )
            ```
        """
        return SQLFactory.any(values)  # NOT ANY is handled by the comparison operator

    # ===================
    # String Functions
    # ===================

    @staticmethod
    def concat(*expressions: Union[str, exp.Expression]) -> exp.Expression:
        """Create a CONCAT expression.

        Args:
            *expressions: Expressions to concatenate.

        Returns:
            CONCAT expression.
        """
        exprs = [exp.column(expr) if isinstance(expr, str) else expr for expr in expressions]
        return exp.Concat(expressions=exprs)

    @staticmethod
    def upper(column: Union[str, exp.Expression]) -> exp.Expression:
        """Create an UPPER expression.

        Args:
            column: Column to convert to uppercase.

        Returns:
            UPPER expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Upper(this=col_expr)

    @staticmethod
    def lower(column: Union[str, exp.Expression]) -> exp.Expression:
        """Create a LOWER expression.

        Args:
            column: Column to convert to lowercase.

        Returns:
            LOWER expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Lower(this=col_expr)

    @staticmethod
    def length(column: Union[str, exp.Expression]) -> exp.Expression:
        """Create a LENGTH expression.

        Args:
            column: Column to get length of.

        Returns:
            LENGTH expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Length(this=col_expr)

    # ===================
    # Math Functions
    # ===================

    @staticmethod
    def round(column: Union[str, exp.Expression], decimals: int = 0) -> exp.Expression:
        """Create a ROUND expression.

        Args:
            column: Column to round.
            decimals: Number of decimal places.

        Returns:
            ROUND expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        if decimals == 0:
            return exp.Round(this=col_expr)
        return exp.Round(this=col_expr, expression=exp.Literal.number(decimals))

    # ===================
    # Conversion Functions
    # ===================

    @staticmethod
    def _to_literal(value: Any) -> exp.Expression:
        """Convert a Python value to a SQLGlot literal expression.

        Uses SQLGlot's built-in exp.convert() function for optimal dialect-agnostic
        literal creation. Handles all Python primitive types correctly:
        - None -> exp.Null (renders as NULL)
        - bool -> exp.Boolean (renders as TRUE/FALSE or 1/0 based on dialect)
        - int/float -> exp.Literal with is_number=True
        - str -> exp.Literal with is_string=True
        - exp.Expression -> returned as-is (passthrough)

        Args:
            value: Python value or SQLGlot expression to convert.

        Returns:
            SQLGlot expression representing the literal value.
        """
        if isinstance(value, exp.Expression):
            return value
        return exp.convert(value)

    @staticmethod
    def decode(column: Union[str, exp.Expression], *args: Union[str, exp.Expression, Any]) -> exp.Expression:
        """Create a DECODE expression (Oracle-style conditional logic).

        DECODE compares column to each search value and returns the corresponding result.
        If no match is found, returns the default value (if provided) or NULL.

        Args:
            column: Column to compare.
            *args: Alternating search values and results, with optional default at the end.
                  Format: search1, result1, search2, result2, ..., [default]

        Raises:
            ValueError: If fewer than two search/result pairs are provided.

        Returns:
            CASE expression equivalent to DECODE.

        Example:
            ```python
            # DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown')
            sql.decode(
                "status", "A", "Active", "I", "Inactive", "Unknown"
            )
            ```
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        if len(args) < MIN_DECODE_ARGS:
            msg = "DECODE requires at least one search/result pair"
            raise ValueError(msg)

        conditions = []
        default = None

        for i in range(0, len(args) - 1, 2):
            if i + 1 >= len(args):
                # Odd number of args means last one is default
                default = SQLFactory._to_literal(args[i])
                break

            search_val = args[i]
            result_val = args[i + 1]

            search_expr = SQLFactory._to_literal(search_val)
            result_expr = SQLFactory._to_literal(result_val)

            condition = exp.EQ(this=col_expr, expression=search_expr)
            conditions.append(exp.When(this=condition, then=result_expr))

        return exp.Case(ifs=conditions, default=default)

    @staticmethod
    def cast(column: Union[str, exp.Expression], data_type: str) -> exp.Expression:
        """Create a CAST expression for type conversion.

        Args:
            column: Column or expression to cast.
            data_type: Target data type (e.g., 'INT', 'VARCHAR(100)', 'DECIMAL(10,2)').

        Returns:
            CAST expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Cast(this=col_expr, to=exp.DataType.build(data_type))

    @staticmethod
    def coalesce(*expressions: Union[str, exp.Expression]) -> exp.Expression:
        """Create a COALESCE expression.

        Args:
            *expressions: Expressions to coalesce.

        Returns:
            COALESCE expression.
        """
        exprs = [exp.column(expr) if isinstance(expr, str) else expr for expr in expressions]
        return exp.Coalesce(expressions=exprs)

    @staticmethod
    def nvl(column: Union[str, exp.Expression], substitute_value: Union[str, exp.Expression, Any]) -> exp.Expression:
        """Create an NVL (Oracle-style) expression using COALESCE.

        Args:
            column: Column to check for NULL.
            substitute_value: Value to use if column is NULL.

        Returns:
            COALESCE expression equivalent to NVL.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        sub_expr = SQLFactory._to_literal(substitute_value)
        return exp.Coalesce(expressions=[col_expr, sub_expr])

    @staticmethod
    def nvl2(
        column: Union[str, exp.Expression],
        value_if_not_null: Union[str, exp.Expression, Any],
        value_if_null: Union[str, exp.Expression, Any],
    ) -> exp.Expression:
        """Create an NVL2 (Oracle-style) expression using CASE.

        NVL2 returns value_if_not_null if column is not NULL,
        otherwise returns value_if_null.

        Args:
            column: Column to check for NULL.
            value_if_not_null: Value to use if column is NOT NULL.
            value_if_null: Value to use if column is NULL.

        Returns:
            CASE expression equivalent to NVL2.

        Example:
            ```python
            # NVL2(salary, 'Has Salary', 'No Salary')
            sql.nvl2("salary", "Has Salary", "No Salary")
            ```
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        not_null_expr = SQLFactory._to_literal(value_if_not_null)
        null_expr = SQLFactory._to_literal(value_if_null)

        # Create CASE WHEN column IS NOT NULL THEN value_if_not_null ELSE value_if_null END
        is_null = exp.Is(this=col_expr, expression=exp.Null())
        condition = exp.Not(this=is_null)
        when_clause = exp.If(this=condition, true=not_null_expr)

        return exp.Case(ifs=[when_clause], default=null_expr)

    # ===================
    # Bulk Operations
    # ===================

    @staticmethod
    def bulk_insert(table_name: str, column_count: int, placeholder_style: str = "?") -> exp.Expression:
        """Create bulk INSERT expression for executemany operations.

        This is specifically for bulk loading operations like CSV ingestion where
        we need an INSERT expression with placeholders for executemany().

        Args:
            table_name: Name of the table to insert into
            column_count: Number of columns (for placeholder generation)
            placeholder_style: Placeholder style ("?" for SQLite/PostgreSQL, "%s" for MySQL, ":1" for Oracle)

        Returns:
            INSERT expression with proper placeholders for bulk operations

        Example:
            ```python
            from sqlspec import sql

            # SQLite/PostgreSQL style
            insert_expr = sql.bulk_insert("my_table", 3)
            # Creates: INSERT INTO "my_table" VALUES (?, ?, ?)

            # MySQL style
            insert_expr = sql.bulk_insert(
                "my_table", 3, placeholder_style="%s"
            )
            # Creates: INSERT INTO "my_table" VALUES (%s, %s, %s)

            # Oracle style
            insert_expr = sql.bulk_insert(
                "my_table", 3, placeholder_style=":1"
            )
            # Creates: INSERT INTO "my_table" VALUES (:1, :2, :3)
            ```
        """
        return exp.Insert(
            this=exp.Table(this=exp.to_identifier(table_name)),
            expression=exp.Values(
                expressions=[
                    exp.Tuple(expressions=[exp.Placeholder(this=placeholder_style) for _ in range(column_count)])
                ]
            ),
        )

    def truncate(self, table_name: str) -> "Truncate":
        """Create a TRUNCATE TABLE builder.

        Args:
            table_name: Name of the table to truncate

        Returns:
            TruncateTable builder instance

        Example:
            ```python
            from sqlspec import sql

            # Simple truncate
            truncate_sql = sql.truncate_table("my_table").build().sql

            # Truncate with options
            truncate_sql = (
                sql.truncate_table("my_table")
                .cascade()
                .restart_identity()
                .build()
                .sql
            )
            ```
        """
        builder = Truncate(dialect=self.dialect)
        builder._table_name = table_name
        return builder

    # ===================
    # Case Expressions
    # ===================

    @staticmethod
    def case() -> "Case":
        """Create a CASE expression builder.

        Returns:
            CaseExpressionBuilder for building CASE expressions.
        """
        return Case()

    # ===================
    # Window Functions
    # ===================

    def row_number(
        self,
        partition_by: Optional[Union[str, list[str], exp.Expression]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression]] = None,
    ) -> exp.Expression:
        """Create a ROW_NUMBER() window function.

        Args:
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            ROW_NUMBER window function expression.
        """
        return self._create_window_function("ROW_NUMBER", [], partition_by, order_by)

    def rank(
        self,
        partition_by: Optional[Union[str, list[str], exp.Expression]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression]] = None,
    ) -> exp.Expression:
        """Create a RANK() window function.

        Args:
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            RANK window function expression.
        """
        return self._create_window_function("RANK", [], partition_by, order_by)

    def dense_rank(
        self,
        partition_by: Optional[Union[str, list[str], exp.Expression]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression]] = None,
    ) -> exp.Expression:
        """Create a DENSE_RANK() window function.

        Args:
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            DENSE_RANK window function expression.
        """
        return self._create_window_function("DENSE_RANK", [], partition_by, order_by)

    @staticmethod
    def _create_window_function(
        func_name: str,
        func_args: list[exp.Expression],
        partition_by: Optional[Union[str, list[str], exp.Expression]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression]] = None,
    ) -> exp.Expression:
        """Helper to create window function expressions.

        Args:
            func_name: Name of the window function.
            func_args: Arguments to the function.
            partition_by: Columns to partition by.
            order_by: Columns to order by.

        Returns:
            Window function expression.
        """
        func_expr = exp.Anonymous(this=func_name, expressions=func_args)

        over_args: dict[str, Any] = {}

        if partition_by:
            if isinstance(partition_by, str):
                over_args["partition_by"] = [exp.column(partition_by)]
            elif isinstance(partition_by, list):
                over_args["partition_by"] = [exp.column(col) for col in partition_by]
            elif isinstance(partition_by, exp.Expression):
                over_args["partition_by"] = [partition_by]

        if order_by:
            if isinstance(order_by, str):
                over_args["order"] = [exp.column(order_by).asc()]
            elif isinstance(order_by, list):
                over_args["order"] = [exp.column(col).asc() for col in order_by]
            elif isinstance(order_by, exp.Expression):
                over_args["order"] = [order_by]

        return exp.Window(this=func_expr, **over_args)


class Case:
    """Builder for CASE expressions using the SQL factory.

    Example:
        ```python
        from sqlspec import sql

        case_expr = (
            sql.case()
            .when(sql.age < 18, "Minor")
            .when(sql.age < 65, "Adult")
            .else_("Senior")
            .end()
        )
        ```
    """

    def __init__(self) -> None:
        """Initialize the CASE expression builder."""
        self._conditions: list[exp.If] = []
        self._default: Optional[exp.Expression] = None

    def when(self, condition: Union[str, exp.Expression], value: Union[str, exp.Expression, Any]) -> "Case":
        """Add a WHEN clause.

        Args:
            condition: Condition to test.
            value: Value to return if condition is true.

        Returns:
            Self for method chaining.
        """
        cond_expr = exp.maybe_parse(condition) or exp.column(condition) if isinstance(condition, str) else condition
        val_expr = SQLFactory._to_literal(value)

        # SQLGlot uses exp.If for CASE WHEN clauses, not exp.When
        when_clause = exp.If(this=cond_expr, true=val_expr)
        self._conditions.append(when_clause)
        return self

    def else_(self, value: Union[str, exp.Expression, Any]) -> "Case":
        """Add an ELSE clause.

        Args:
            value: Default value to return.

        Returns:
            Self for method chaining.
        """
        self._default = SQLFactory._to_literal(value)
        return self

    def end(self) -> exp.Expression:
        """Complete the CASE expression.

        Returns:
            Complete CASE expression.
        """
        return exp.Case(ifs=self._conditions, default=self._default)


# Create a default SQL factory instance
sql = SQLFactory()
