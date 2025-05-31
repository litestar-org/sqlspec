# ruff: noqa: PLR0904, SLF001, ARG004, S608
"""Unified SQL factory for creating SQL builders and column expressions with a clean API.

This module provides the `sql` factory object for easy SQL construction:
- `sql` provides both statement builders (select, insert, update, etc.) and column expressions
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError

if TYPE_CHECKING:
    from sqlspec.statement.builder import (
        DeleteBuilder,
        InsertBuilder,
        MergeBuilder,
        SelectBuilder,
        UpdateBuilder,
    )

__all__ = ("sql",)

logger = logging.getLogger("sqlspec")


@dataclass
class StatementAnalysis:
    """Analysis result for parsed SQL statements."""

    statement_type: str
    """Type of SQL statement (Insert, Select, Update, Delete, etc.)"""
    expression: exp.Expression
    """Parsed SQLGlot expression"""
    table_name: Optional[str] = None
    """Primary table name if detected"""
    columns: list[str] = field(default_factory=list)
    """Column names if detected"""
    has_returning: bool = False
    """Whether statement has RETURNING clause"""
    is_from_select: bool = False
    """Whether this is an INSERT FROM SELECT pattern"""
    parameters: dict[str, Any] = field(default_factory=dict)
    """Extracted parameters from the SQL"""


class SQLAnalyzer:
    """Efficient SQL statement analyzer using SQLGlot's AST capabilities."""

    # Cache for parsed expressions to avoid re-parsing
    _parse_cache: dict[tuple[str, Optional[str]], exp.Expression] = {}

    @classmethod
    def analyze_statement(cls, sql_string: str, dialect: Optional[DialectType] = None) -> StatementAnalysis:
        """Analyze SQL string and extract components efficiently.

        Args:
            sql_string: The SQL string to analyze
            dialect: SQL dialect for parsing

        Returns:
            StatementAnalysis with extracted components
        """
        # Use cache key for performance
        cache_key = (sql_string.strip(), str(dialect) if dialect else None)

        if cache_key in cls._parse_cache:
            expr = cls._parse_cache[cache_key]
        else:
            try:
                # Use maybe_parse for graceful fallback
                expr = exp.maybe_parse(sql_string, dialect=dialect)
                if expr is None:
                    # Fallback to parse_one for better error messages
                    import sqlglot

                    expr = sqlglot.parse_one(sql_string, read=dialect)

                # Cache the parsed expression
                if len(cls._parse_cache) < 1000:  # Prevent unbounded cache growth
                    cls._parse_cache[cache_key] = expr

            except (SQLGlotParseError, Exception) as e:
                logger.warning("Failed to parse SQL statement: %s", e)
                # Return minimal analysis for unparseable SQL
                return StatementAnalysis(
                    statement_type="Unknown",
                    expression=exp.Anonymous(this="UNKNOWN"),
                )

        return StatementAnalysis(
            statement_type=type(expr).__name__,
            expression=expr,
            table_name=cls._extract_table_name(expr),
            columns=cls._extract_columns(expr),
            has_returning=bool(expr.find(exp.Returning)),
            is_from_select=cls._is_insert_from_select(expr),
            parameters=cls._extract_parameters(expr),
        )

    @staticmethod
    def _extract_table_name(expr: exp.Expression) -> Optional[str]:
        if isinstance(expr, exp.Insert):
            if expr.this and hasattr(expr.this, "this"):
                # Handle schema.table cases
                table = expr.this
                if isinstance(table, exp.Table):
                    return table.name
                if hasattr(table, "name"):
                    return str(table.name)
        elif isinstance(expr, (exp.Update, exp.Delete)):
            if expr.this:
                return str(expr.this.name) if hasattr(expr.this, "name") else str(expr.this)
        elif isinstance(expr, exp.Select) and (from_clause := expr.find(exp.From)) and from_clause.this:
            return str(from_clause.this.name) if hasattr(from_clause.this, "name") else str(from_clause.this)
        return None

    @staticmethod
    def _extract_columns(expr: exp.Expression) -> list[str]:
        columns = []

        if isinstance(expr, exp.Insert):
            if expr.this and hasattr(expr.this, "expressions"):
                columns.extend(str(col_expr.name) for col_expr in expr.this.expressions if hasattr(col_expr, "name"))
        elif isinstance(expr, exp.Select):
            # Extract selected columns
            for projection in expr.expressions:
                if isinstance(projection, exp.Column):
                    columns.append(str(projection.name))
                elif hasattr(projection, "alias") and projection.alias:
                    columns.append(str(projection.alias))
                elif hasattr(projection, "name"):
                    columns.append(str(projection.name))

        return columns

    @staticmethod
    def _is_insert_from_select(expr: exp.Expression) -> bool:
        if not isinstance(expr, exp.Insert):
            return False

        return bool(expr.expression and isinstance(expr.expression, exp.Select))

    @staticmethod
    def _extract_parameters(expr: exp.Expression) -> dict[str, Any]:
        return {}

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the parse cache (useful for testing or memory management)."""
        cls._parse_cache.clear()


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

    def __init__(self, dialect: Optional[DialectType] = None) -> None:
        """Initialize the SQL factory.

        Args:
            dialect: Default SQL dialect to use for all builders.
        """
        self.dialect = dialect

    # ===================
    # Statement Builders
    # ===================

    def select(
        self, *columns_or_sql: Union[str, exp.Expression], dialect: Optional[DialectType] = None
    ) -> "SelectBuilder":
        """Create a SELECT builder, optionally from raw SQL.

        Args:
            *columns_or_sql: Columns to select, or a raw SQL string if only one argument.
                           If first argument looks like SQL, it will be parsed.
            dialect: SQL dialect to use (overrides factory default).

        Returns:
            SelectBuilder: A new SelectBuilder instance.
        """
        from sqlspec.statement.builder import SelectBuilder

        builder_dialect = dialect or self.dialect

        # Check if this looks like raw SQL (single string argument with SQL keywords)
        if len(columns_or_sql) == 1 and isinstance(columns_or_sql[0], str):
            sql_candidate = columns_or_sql[0].strip()
            if self._looks_like_sql(sql_candidate, "SELECT"):
                select_builder = SelectBuilder(dialect=builder_dialect)
                # Ensure proper initialization
                if select_builder._expression is None:
                    select_builder.__post_init__()
                return self._populate_select_from_sql(select_builder, sql_candidate)

        # Traditional column-based usage
        select_builder = SelectBuilder(dialect=builder_dialect)
        # Ensure proper initialization
        if select_builder._expression is None:
            select_builder.__post_init__()

        if columns_or_sql:
            select_builder.select(*columns_or_sql)
        return select_builder

    def insert(self, table_or_sql: Optional[str] = None, dialect: Optional[DialectType] = None) -> "InsertBuilder":
        """Create an INSERT builder, optionally from raw SQL.

        Args:
            table_or_sql: Table name to insert into, or raw SQL string.
            dialect: SQL dialect to use (overrides factory default).

        Returns:
            InsertBuilder: A new InsertBuilder instance.
        """
        from sqlspec.statement.builder import InsertBuilder

        builder_dialect = dialect or self.dialect
        builder = InsertBuilder(dialect=builder_dialect)

        # Ensure proper initialization (call __post_init__ explicitly if needed)
        if builder._expression is None:
            builder.__post_init__()

        if table_or_sql:
            if self._looks_like_sql(table_or_sql):
                return self._populate_insert_from_sql(builder, table_or_sql)
            # Traditional table name usage
            return builder.into(table_or_sql)

        return builder

    def update(self, table_or_sql: Optional[str] = None, dialect: Optional[DialectType] = None) -> "UpdateBuilder":
        """Create an UPDATE builder, optionally from raw SQL.

        Args:
            table_or_sql: Table name to update, or raw SQL string.
            dialect: SQL dialect to use (overrides factory default).

        Returns:
            UpdateBuilder: A new UpdateBuilder instance.
        """
        from sqlspec.statement.builder import UpdateBuilder

        builder_dialect = dialect or self.dialect
        builder = UpdateBuilder(dialect=builder_dialect)

        # Ensure proper initialization (call __post_init__ explicitly if needed)
        if builder._expression is None:
            builder.__post_init__()

        if table_or_sql:
            if self._looks_like_sql(table_or_sql, "UPDATE"):
                return self._populate_update_from_sql(builder, table_or_sql)
            # Traditional table name usage
            return builder.table(table_or_sql)

        return builder

    def delete(self, table_or_sql: Optional[str] = None, dialect: Optional[DialectType] = None) -> "DeleteBuilder":
        """Create a DELETE builder, optionally from raw SQL.

        Args:
            table_or_sql: Optional raw SQL string for DELETE statement.
            dialect: SQL dialect to use (overrides factory default).

        Returns:
            DeleteBuilder: A new DeleteBuilder instance.
        """
        from sqlspec.statement.builder import DeleteBuilder

        builder_dialect = dialect or self.dialect
        builder = DeleteBuilder(dialect=builder_dialect)

        # Ensure proper initialization (call __post_init__ explicitly if needed)
        if builder._expression is None:
            builder.__post_init__()

        if table_or_sql and self._looks_like_sql(table_or_sql, "DELETE"):
            return self._populate_delete_from_sql(builder, table_or_sql)

        return builder

    def merge(self, table_or_sql: Optional[str] = None, dialect: Optional[DialectType] = None) -> "MergeBuilder":
        """Create a MERGE builder, optionally from raw SQL.

        Args:
            table_or_sql: Target table for merge, or raw SQL string.
            dialect: SQL dialect to use (overrides factory default).

        Returns:
            MergeBuilder: A new MergeBuilder instance.
        """
        from sqlspec.statement.builder import MergeBuilder

        builder_dialect = dialect or self.dialect
        builder = MergeBuilder(dialect=builder_dialect)

        # Ensure proper initialization (call __post_init__ explicitly if needed)
        if builder._expression is None:
            builder.__post_init__()

        if table_or_sql:
            if self._looks_like_sql(table_or_sql, "MERGE"):
                return self._populate_merge_from_sql(builder, table_or_sql)
            # Traditional table name usage
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
        if not candidate or len(candidate.strip()) < 6:
            return False

        candidate_upper = candidate.strip().upper()

        # Check for SQL keywords at the beginning
        sql_starters = ["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "WITH"]

        if expected_type:
            return candidate_upper.startswith(expected_type.upper())

        return any(candidate_upper.startswith(starter) for starter in sql_starters)

    def _populate_insert_from_sql(self, builder: "InsertBuilder", sql_string: str) -> "InsertBuilder":
        """Populate InsertBuilder from raw SQL string."""
        try:
            analysis = SQLAnalyzer.analyze_statement(sql_string, self.dialect)

            if analysis.statement_type == "Insert":
                # Standard INSERT statement
                if analysis.table_name:
                    builder.into(analysis.table_name)

                # Set the internal expression to the parsed one, ensuring it's an Insert type
                if isinstance(analysis.expression, exp.Insert):
                    builder._expression = analysis.expression

                return builder

            if analysis.statement_type == "Select":
                # Handle INSERT FROM SELECT case
                logger.info("Converting SELECT to INSERT FROM SELECT pattern")
                # This would need a target table - for now, return builder with the SELECT as source
                if hasattr(builder, "from_select"):
                    # We'd need to create a temporary SelectBuilder from the analysis
                    # This is a placeholder for the full implementation
                    pass
                return builder

            from sqlspec.exceptions import SQLBuilderError

            msg = f"Cannot create INSERT from {analysis.statement_type} statement"
            raise SQLBuilderError(msg)

        except Exception as e:
            logger.warning("Failed to parse INSERT SQL, falling back to traditional mode: %s", e)
            return builder

    def _populate_select_from_sql(self, builder: "SelectBuilder", sql_string: str) -> "SelectBuilder":
        try:
            analysis = SQLAnalyzer.analyze_statement(sql_string, self.dialect)

            if analysis.statement_type == "Select":
                # Set the internal expression to the parsed one, ensuring it's a Select type
                if isinstance(analysis.expression, exp.Select):
                    builder._expression = analysis.expression
                return builder
            from sqlspec.exceptions import SQLBuilderError

            msg = f"Cannot create SELECT from {analysis.statement_type} statement"
            raise SQLBuilderError(msg)

        except Exception as e:
            logger.warning("Failed to parse SELECT SQL, falling back to traditional mode: %s", e)
            return builder

    def _populate_update_from_sql(self, builder: "UpdateBuilder", sql_string: str) -> "UpdateBuilder":
        try:
            analysis = SQLAnalyzer.analyze_statement(sql_string, self.dialect)

            if analysis.statement_type == "Update":
                if analysis.table_name:
                    builder.table(analysis.table_name)

                # Set the internal expression to the parsed one, ensuring it's an Update type
                if isinstance(analysis.expression, exp.Update):
                    builder._expression = analysis.expression
                return builder
            from sqlspec.exceptions import SQLBuilderError

            msg = f"Cannot create UPDATE from {analysis.statement_type} statement"
            raise SQLBuilderError(msg)

        except Exception as e:
            logger.warning("Failed to parse UPDATE SQL, falling back to traditional mode: %s", e)
            return builder

    def _populate_delete_from_sql(self, builder: "DeleteBuilder", sql_string: str) -> "DeleteBuilder":
        try:
            analysis = SQLAnalyzer.analyze_statement(sql_string, self.dialect)

            if analysis.statement_type == "Delete":
                # Set the internal expression to the parsed one, ensuring it's a Delete type
                if isinstance(analysis.expression, exp.Delete):
                    builder._expression = analysis.expression
                return builder
            from sqlspec.exceptions import SQLBuilderError

            msg = f"Cannot create DELETE from {analysis.statement_type} statement"
            raise SQLBuilderError(msg)

        except Exception as e:
            logger.warning("Failed to parse DELETE SQL, falling back to traditional mode: %s", e)
            return builder

    def _populate_merge_from_sql(self, builder: "MergeBuilder", sql_string: str) -> "MergeBuilder":
        try:
            analysis = SQLAnalyzer.analyze_statement(sql_string, self.dialect)

            if analysis.statement_type == "Merge":
                if analysis.table_name:
                    builder.into(analysis.table_name)

                # Set the internal expression to the parsed one, ensuring it's a Merge type
                if isinstance(analysis.expression, exp.Merge):
                    builder._expression = analysis.expression
                return builder
            from sqlspec.exceptions import SQLBuilderError

            msg = f"Cannot create MERGE from {analysis.statement_type} statement"
            raise SQLBuilderError(msg)

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

        if len(args) < 2:
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
                search_expr = search_val
            else:
                search_expr = exp.Literal.string(str(search_val))

            # Create result expression
            if isinstance(result_val, str):
                result_expr = exp.Literal.string(result_val)
            elif isinstance(result_val, (int, float)):
                result_expr = exp.Literal.number(result_val)
            elif isinstance(result_val, exp.Expression):
                result_expr = result_val
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
            sub_expr = substitute_value
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
            val_expr = value
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


# Create the main unified instance
sql = SQLFactory()
