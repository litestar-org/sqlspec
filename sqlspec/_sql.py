"""Unified SQL factory for creating SQL builders and column expressions with a clean API.

This module provides the `sql` factory object for easy SQL construction:
- `sql` provides both statement builders (select, insert, update, etc.) and column expressions
"""

import logging
from typing import Any, Optional, Union

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.exceptions import SQLBuilderError
from sqlspec.statement.builder import DeleteBuilder, InsertBuilder, MergeBuilder, SelectBuilder, UpdateBuilder

__all__ = ("SQLFactory",)

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
    """Unified factory for creating SQL builders and column expressions with a fluent API.

    Provides both statement builders and column expressions through a single, clean interface.
    Now supports parsing raw SQL strings into appropriate builders for enhanced flexibility.

    Example:
        ```python
        from sqlspec import sql

        # Traditional builder usage (unchanged)
        query = (
            sql.select(sql.id, sql.name)
            .from_("users")
            .where("age > 18")
        )

        # New: Raw SQL parsing
        insert_sql = sql.insert(
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
        )
        select_sql = sql.select(
            "SELECT * FROM users WHERE active = 1"
        )

        # RETURNING clause detection
        returning_insert = sql.insert(
            "INSERT INTO users (name) VALUES ('John') RETURNING id"
        )
        # → When executed, will return SelectResult instead of ExecuteResult

        # Smart INSERT FROM SELECT
        insert_from_select = sql.insert(
            "SELECT id, name FROM source WHERE active = 1"
        )
        # → Will prompt for target table or convert to INSERT FROM SELECT pattern
        ```
    """

    @classmethod
    def detect_sql_type(cls, sql: str, dialect: DialectType = None) -> str:
        try:
            # Minimal parsing just to get the command type
            parsed_expr = sqlglot.parse_one(sql, read=dialect)
            if parsed_expr and parsed_expr.key:
                return parsed_expr.key.upper()
            # Fallback for expressions that might not have a direct 'key'
            # or where key is None (e.g. some DDL without explicit command like SET)
            if parsed_expr:
                # Attempt to get the class name as a fallback, e.g., "Set", "Command"
                command_type = type(parsed_expr).__name__.upper()
                # Handle specific cases like "COMMAND" which might be too generic
                if command_type == "COMMAND" and parsed_expr.this:
                    return str(parsed_expr.this).upper()  # e.g. "SET", "ALTER"
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
    def __call__(
        self,
        statement: str,
        parameters: Optional[Any] = None,
        *filters: Any,
        config: Optional[Any] = None,
        dialect: DialectType = None,
        **kwargs: Any,
    ) -> "Any":
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
        # Map sqlglot expression class to type string
        expr_type_map = {
            "SELECT": "SELECT",
            "INSERT": "INSERT",
            "UPDATE": "UPDATE",
            "DELETE": "DELETE",
            "MERGE": "MERGE",
            "WITH": "WITH",
        }
        actual_type_str = expr_type_map.get(actual_type, actual_type)
        # Only allow SELECT or WITH (if WITH wraps SELECT)
        if actual_type_str == "SELECT" or (
            actual_type_str == "WITH" and parsed_expr.this and isinstance(parsed_expr.this, exp.Select)
        ):
            builder = SelectBuilder(dialect=dialect or self.dialect)
            builder._expression = parsed_expr
            return builder
        # If not SELECT, raise with helpful message
        msg = (
            f"sql(...) only supports SELECT statements. Detected type: {actual_type_str}. "
            f"Use sql.{actual_type_str.lower()}() instead."
        )
        raise SQLBuilderError(msg)

    # ===================
    # Statement Builders
    # ===================
    def select(self, *columns_or_sql: Union[str, exp.Expression], dialect: DialectType = None) -> "SelectBuilder":
        builder_dialect = dialect or self.dialect
        if len(columns_or_sql) == 1 and isinstance(columns_or_sql[0], str):
            sql_candidate = columns_or_sql[0].strip()
            # Check if it actually looks like SQL before parsing
            if self._looks_like_sql(sql_candidate):
                # Validate type
                detected = self.detect_sql_type(sql_candidate, dialect=builder_dialect)
                if detected not in {"SELECT", "WITH"}:
                    msg = (
                        f"sql.select() expects a SELECT or WITH statement, got {detected}. "
                        f"Use sql.{detected.lower()}() if a dedicated builder exists, or ensure the SQL is SELECT/WITH."
                    )
                    raise SQLBuilderError(msg)
                select_builder = SelectBuilder(dialect=builder_dialect)
                if select_builder._expression is None:
                    select_builder.__post_init__()
                return self._populate_select_from_sql(select_builder, sql_candidate)
            # Otherwise treat as column name and fall through to normal column handling
        select_builder = SelectBuilder(dialect=builder_dialect)
        if select_builder._expression is None:
            select_builder.__post_init__()
        if columns_or_sql:
            select_builder.select(*columns_or_sql)
        return select_builder

    def insert(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "InsertBuilder":
        builder_dialect = dialect or self.dialect
        builder = InsertBuilder(dialect=builder_dialect)
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

    def update(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "UpdateBuilder":
        builder_dialect = dialect or self.dialect
        builder = UpdateBuilder(dialect=builder_dialect)
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

    def delete(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "DeleteBuilder":
        builder_dialect = dialect or self.dialect
        builder = DeleteBuilder(dialect=builder_dialect)
        if builder._expression is None:
            builder.__post_init__()
        if table_or_sql and self._looks_like_sql(table_or_sql):
            detected = self.detect_sql_type(table_or_sql, dialect=builder_dialect)
            if detected != "DELETE":
                msg = f"sql.delete() expects DELETE statement, got {detected}. Use sql.{detected.lower()}() if a dedicated builder exists."
                raise SQLBuilderError(msg)
            return self._populate_delete_from_sql(builder, table_or_sql)
        return builder

    def merge(self, table_or_sql: Optional[str] = None, dialect: DialectType = None) -> "MergeBuilder":
        builder_dialect = dialect or self.dialect
        builder = MergeBuilder(dialect=builder_dialect)
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

        # Check for SQL keywords at the beginning
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

    def _populate_insert_from_sql(self, builder: "InsertBuilder", sql_string: str) -> "InsertBuilder":
        """Parse SQL string and populate INSERT builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]
            if parsed_expr is None:
                parsed_expr = sqlglot.parse_one(sql_string, read=self.dialect)

            if isinstance(parsed_expr, exp.Insert):
                # Set the internal expression to the parsed one
                builder._expression = parsed_expr
                return builder

            if isinstance(parsed_expr, exp.Select):
                # Handle INSERT FROM SELECT case - just return builder for now
                # The actual conversion logic can be handled by the builder itself
                logger.info("Detected SELECT statement for INSERT - may need target table specification")
                return builder

            # For other statement types, just return the builder as-is
            logger.warning("Cannot create INSERT from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse INSERT SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_select_from_sql(self, builder: "SelectBuilder", sql_string: str) -> "SelectBuilder":
        """Parse SQL string and populate SELECT builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]
            if parsed_expr is None:
                parsed_expr = sqlglot.parse_one(sql_string, read=self.dialect)

            if isinstance(parsed_expr, exp.Select):
                # Set the internal expression to the parsed one
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create SELECT from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse SELECT SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_update_from_sql(self, builder: "UpdateBuilder", sql_string: str) -> "UpdateBuilder":
        """Parse SQL string and populate UPDATE builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]
            if parsed_expr is None:
                parsed_expr = sqlglot.parse_one(sql_string, read=self.dialect)

            if isinstance(parsed_expr, exp.Update):
                # Set the internal expression to the parsed one
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create UPDATE from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse UPDATE SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_delete_from_sql(self, builder: "DeleteBuilder", sql_string: str) -> "DeleteBuilder":
        """Parse SQL string and populate DELETE builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]
            if parsed_expr is None:
                parsed_expr = sqlglot.parse_one(sql_string, read=self.dialect)

            if isinstance(parsed_expr, exp.Delete):
                # Set the internal expression to the parsed one
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create DELETE from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse DELETE SQL, falling back to traditional mode: %s", e)
        return builder

    def _populate_merge_from_sql(self, builder: "MergeBuilder", sql_string: str) -> "MergeBuilder":
        """Parse SQL string and populate MERGE builder using SQLGlot directly."""
        try:
            # Use SQLGlot directly for parsing - no validation here
            parsed_expr = exp.maybe_parse(sql_string, dialect=self.dialect)  # type: ignore[var-annotated]
            if parsed_expr is None:
                parsed_expr = sqlglot.parse_one(sql_string, read=self.dialect)

            if isinstance(parsed_expr, exp.Merge):
                # Set the internal expression to the parsed one
                builder._expression = parsed_expr
                return builder

            logger.warning("Cannot create MERGE from %s statement", type(parsed_expr).__name__)

        except Exception as e:
            logger.warning("Failed to parse MERGE SQL, falling back to traditional mode: %s", e)
        return builder

    # ===================
    # Column References
    # ===================

    def __getattr__(self, name: str) -> exp.Column:
        """Dynamically create column references.

        Args:
            name: Column name.

        Returns:
            Column expression for the specified column name.
        """
        return exp.column(name)

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
                    # Empty set for grand total
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
            # Convert list to array literal
            literals = [exp.Literal.string(str(v)) if isinstance(v, str) else exp.Literal.number(v) for v in values]
            return exp.Any(this=exp.Array(expressions=literals))
        if isinstance(values, str):
            # Parse as SQL
            parsed = exp.maybe_parse(values)  # type: ignore[var-annotated]
            if parsed:
                return exp.Any(this=parsed)
            return exp.Any(this=exp.Literal.string(values))
        return exp.Any(this=values)

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

        # Build CASE expression
        conditions = []
        default = None

        # Process search/result pairs
        for i in range(0, len(args) - 1, 2):
            if i + 1 >= len(args):
                # Odd number of args means last one is default
                default = exp.Literal.string(str(args[i])) if not isinstance(args[i], exp.Expression) else args[i]
                break

            search_val = args[i]
            result_val = args[i + 1]

            # Create search expression
            if isinstance(search_val, str):
                search_expr = exp.Literal.string(search_val)
            elif isinstance(search_val, (int, float)):
                search_expr = exp.Literal.number(search_val)
            elif isinstance(search_val, exp.Expression):
                search_expr = search_val  # type: ignore[assignment]
            else:
                search_expr = exp.Literal.string(str(search_val))

            # Create result expression
            if isinstance(result_val, str):
                result_expr = exp.Literal.string(result_val)
            elif isinstance(result_val, (int, float)):
                result_expr = exp.Literal.number(result_val)
            elif isinstance(result_val, exp.Expression):
                result_expr = result_val  # type: ignore[assignment]
            else:
                result_expr = exp.Literal.string(str(result_val))

            # Create WHEN condition
            condition = exp.EQ(this=col_expr, expression=search_expr)
            conditions.append(exp.When(this=condition, then=result_expr))

        return exp.Case(ifs=conditions, default=default)

    @staticmethod
    def to_date(date_string: Union[str, exp.Expression], format_mask: Optional[str] = None) -> exp.Expression:
        """Create a TO_DATE expression for converting strings to dates.

        Args:
            date_string: String or expression containing the date to convert.
            format_mask: Optional format mask (e.g., 'YYYY-MM-DD', 'DD/MM/YYYY').

        Returns:
            TO_DATE function expression.
        """
        date_expr = exp.column(date_string) if isinstance(date_string, str) else date_string

        if format_mask:
            format_expr = exp.Literal.string(format_mask)
            return exp.Anonymous(this="TO_DATE", expressions=[date_expr, format_expr])
        return exp.Anonymous(this="TO_DATE", expressions=[date_expr])

    @staticmethod
    def to_char(column: Union[str, exp.Expression], format_mask: Optional[str] = None) -> exp.Expression:
        """Create a TO_CHAR expression for converting values to strings.

        Args:
            column: Column or expression to convert to string.
            format_mask: Optional format mask for dates/numbers.

        Returns:
            TO_CHAR function expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        if format_mask:
            format_expr = exp.Literal.string(format_mask)
            return exp.Anonymous(this="TO_CHAR", expressions=[col_expr, format_expr])
        return exp.Anonymous(this="TO_CHAR", expressions=[col_expr])

    @staticmethod
    def to_string(column: Union[str, exp.Expression]) -> exp.Expression:
        """Create a TO_STRING expression for converting values to strings.

        Args:
            column: Column or expression to convert to string.

        Returns:
            TO_STRING or CAST AS STRING expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        # Use CAST for broader compatibility
        return exp.Cast(this=col_expr, to=exp.DataType.build("STRING"))

    @staticmethod
    def to_number(column: Union[str, exp.Expression], format_mask: Optional[str] = None) -> exp.Expression:
        """Create a TO_NUMBER expression for converting strings to numbers.

        Args:
            column: Column or expression to convert to number.
            format_mask: Optional format mask for the conversion.

        Returns:
            TO_NUMBER function expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column

        if format_mask:
            format_expr = exp.Literal.string(format_mask)
            return exp.Anonymous(this="TO_NUMBER", expressions=[col_expr, format_expr])
        return exp.Anonymous(this="TO_NUMBER", expressions=[col_expr])

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

    # ===================
    # JSON Functions
    # ===================

    @staticmethod
    def to_json(column: Union[str, exp.Expression]) -> exp.Expression:
        """Create a TO_JSON expression for converting values to JSON.

        Args:
            column: Column or expression to convert to JSON.

        Returns:
            TO_JSON function expression.
        """
        col_expr = exp.column(column) if isinstance(column, str) else column
        return exp.Anonymous(this="TO_JSON", expressions=[col_expr])

    @staticmethod
    def from_json(json_column: Union[str, exp.Expression], schema: Optional[str] = None) -> exp.Expression:
        """Create a FROM_JSON expression for parsing JSON strings.

        Args:
            json_column: Column or expression containing JSON string.
            schema: Optional schema specification for the JSON structure.

        Returns:
            FROM_JSON function expression.
        """
        json_expr = exp.column(json_column) if isinstance(json_column, str) else json_column

        if schema:
            schema_expr = exp.Literal.string(schema)
            return exp.Anonymous(this="FROM_JSON", expressions=[json_expr, schema_expr])
        return exp.Anonymous(this="FROM_JSON", expressions=[json_expr])

    @staticmethod
    def json_extract(json_column: Union[str, exp.Expression], path: str) -> exp.Expression:
        """Create a JSON_EXTRACT expression for extracting values from JSON.

        Args:
            json_column: Column or expression containing JSON.
            path: JSON path to extract (e.g., '$.field', '$.array[0]').

        Returns:
            JSON_EXTRACT function expression.
        """
        json_expr = exp.column(json_column) if isinstance(json_column, str) else json_column
        path_expr = exp.Literal.string(path)
        return exp.Anonymous(this="JSON_EXTRACT", expressions=[json_expr, path_expr])

    @staticmethod
    def json_value(json_column: Union[str, exp.Expression], path: str) -> exp.Expression:
        """Create a JSON_VALUE expression for extracting scalar values from JSON.

        Args:
            json_column: Column or expression containing JSON.
            path: JSON path to extract scalar value.

        Returns:
            JSON_VALUE function expression.
        """
        json_expr = exp.column(json_column) if isinstance(json_column, str) else json_column
        path_expr = exp.Literal.string(path)
        return exp.Anonymous(this="JSON_VALUE", expressions=[json_expr, path_expr])

    # ===================
    # NULL Functions
    # ===================

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

        if isinstance(substitute_value, str):
            sub_expr = exp.Literal.string(substitute_value)
        elif isinstance(substitute_value, (int, float)):
            sub_expr = exp.Literal.number(substitute_value)
        elif isinstance(substitute_value, exp.Expression):
            sub_expr = substitute_value  # type: ignore[assignment]
        else:
            sub_expr = exp.Literal.string(str(substitute_value))

        return exp.Coalesce(expressions=[col_expr, sub_expr])

    # ===================
    # Case Expressions
    # ===================

    @staticmethod
    def case() -> "CaseExpressionBuilder":
        """Create a CASE expression builder.

        Returns:
            CaseExpressionBuilder for building CASE expressions.
        """
        return CaseExpressionBuilder()

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
        # Create the function call
        func_expr = exp.Anonymous(this=func_name, expressions=func_args)

        # Build OVER clause
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


class CaseExpressionBuilder:
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
        self._conditions: list[exp.When] = []
        self._default: Optional[exp.Expression] = None

    def when(
        self, condition: Union[str, exp.Expression], value: Union[str, exp.Expression, Any]
    ) -> "CaseExpressionBuilder":
        """Add a WHEN clause.

        Args:
            condition: Condition to test.
            value: Value to return if condition is true.

        Returns:
            Self for method chaining.
        """
        cond_expr = exp.maybe_parse(condition) or exp.column(condition) if isinstance(condition, str) else condition

        if isinstance(value, str):
            val_expr = exp.Literal.string(value)
        elif isinstance(value, (int, float)):
            val_expr = exp.Literal.number(value)
        elif isinstance(value, exp.Expression):
            val_expr = value  # type: ignore[assignment]
        else:
            val_expr = exp.Literal.string(str(value))

        when_clause = exp.When(this=cond_expr, then=val_expr)
        self._conditions.append(when_clause)
        return self

    def else_(self, value: Union[str, exp.Expression, Any]) -> "CaseExpressionBuilder":
        """Add an ELSE clause.

        Args:
            value: Default value to return.

        Returns:
            Self for method chaining.
        """
        if isinstance(value, str):
            self._default = exp.Literal.string(value)
        elif isinstance(value, (int, float)):
            self._default = exp.Literal.number(value)
        elif isinstance(value, exp.Expression):
            self._default = value
        else:
            self._default = exp.Literal.string(str(value))
        return self

    def end(self) -> exp.Expression:
        """Complete the CASE expression.

        Returns:
            Complete CASE expression.
        """
        return exp.Case(ifs=self._conditions, default=self._default)
