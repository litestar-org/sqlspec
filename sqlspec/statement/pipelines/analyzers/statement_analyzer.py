"""SQL Statement Analysis Pipeline Component.

This module provides the StatementAnalyzer processor that can extract metadata
and insights from SQL statements as part of the processing pipeline.
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.sql import SQLConfig

__all__ = (
    "StatementAnalysis",
    "StatementAnalyzer",
)

logger = logging.getLogger("sqlspec.statement.analyzers")


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
    tables: list[str] = field(default_factory=list)
    """All table names referenced in the query"""
    complexity_score: int = 0
    """Complexity score based on query structure"""
    uses_subqueries: bool = False
    """Whether the query uses subqueries"""
    join_count: int = 0
    """Number of joins in the query"""
    aggregate_functions: list[str] = field(default_factory=list)
    """List of aggregate functions used"""


class StatementAnalyzer(ProcessorProtocol[exp.Expression]):
    """SQL statement analyzer that extracts metadata and insights from SQL statements.

    This processor analyzes SQL expressions to extract useful metadata without
    modifying the SQL itself. It can be used in pipelines to gather insights
    about query complexity, table usage, etc.
    """

    def __init__(self, cache_size: int = 1000) -> None:
        """Initialize the analyzer.

        Args:
            cache_size: Maximum number of parsed expressions to cache.
        """
        self.cache_size = cache_size
        self._parse_cache: dict[tuple[str, Optional[str]], exp.Expression] = {}
        self._analysis_cache: dict[str, StatementAnalysis] = {}

    def process(
        self,
        expression: exp.Expression,
        dialect: Optional[DialectType] = None,
        config: Optional["SQLConfig"] = None,
    ) -> tuple[exp.Expression, Optional[ValidationResult]]:
        """Process the SQL expression to extract analysis metadata.

        Args:
            expression: The SQL expression to analyze
            dialect: SQL dialect for context
            config: SQL configuration

        Returns:
            Tuple of (unchanged expression, None) - this processor doesn't validate
        """
        # The analyzer doesn't modify the expression or validate it,
        # it just extracts metadata. The analysis result could be stored
        # in the config or attached to the expression somehow, but for now
        # we'll just return the unchanged expression.

        # In a future enhancement, we could store the analysis result
        # in a way that the SQLStatement class can access it.

        return expression, None

    def analyze_statement(self, sql_string: str, dialect: Optional[DialectType] = None) -> StatementAnalysis:
        """Analyze SQL string and extract components efficiently.

        Args:
            sql_string: The SQL string to analyze
            dialect: SQL dialect for parsing

        Returns:
            StatementAnalysis with extracted components
        """
        # Check cache first
        cache_key = sql_string.strip()
        if cache_key in self._analysis_cache:
            return self._analysis_cache[cache_key]

        # Use cache key for expression parsing performance
        parse_cache_key = (sql_string.strip(), str(dialect) if dialect else None)

        if parse_cache_key in self._parse_cache:
            expr = self._parse_cache[parse_cache_key]
        else:
            try:
                # Use maybe_parse for graceful fallback
                expr = exp.maybe_parse(sql_string, dialect=dialect)
                if expr is None:
                    # Fallback to parse_one for better error messages
                    import sqlglot

                    expr = sqlglot.parse_one(sql_string, read=dialect)

                # Cache the parsed expression
                if len(self._parse_cache) < self.cache_size:
                    self._parse_cache[parse_cache_key] = expr

            except (SQLGlotParseError, Exception) as e:
                logger.warning("Failed to parse SQL statement: %s", e)
                # Return minimal analysis for bad SQL
                return StatementAnalysis(
                    statement_type="Unknown",
                    expression=exp.Anonymous(this="UNKNOWN"),
                )

        # Perform the analysis
        analysis = StatementAnalysis(
            statement_type=type(expr).__name__,
            expression=expr,
            table_name=self._extract_primary_table_name(expr),
            columns=self._extract_columns(expr),
            has_returning=bool(expr.find(exp.Returning)),
            is_from_select=self._is_insert_from_select(expr),
            parameters=self._extract_parameters(expr),
            tables=self._extract_all_tables(expr),
            complexity_score=self._calculate_complexity_score(expr),
            uses_subqueries=self._has_subqueries(expr),
            join_count=self._count_joins(expr),
            aggregate_functions=self._extract_aggregate_functions(expr),
        )

        # Cache the analysis result
        if len(self._analysis_cache) < self.cache_size:
            self._analysis_cache[cache_key] = analysis

        return analysis

    def analyze_expression(self, expression: exp.Expression) -> StatementAnalysis:
        """Analyze a SQLGlot expression directly.

        Args:
            expression: The expression to analyze

        Returns:
            StatementAnalysis with extracted components
        """
        return StatementAnalysis(
            statement_type=type(expression).__name__,
            expression=expression,
            table_name=self._extract_primary_table_name(expression),
            columns=self._extract_columns(expression),
            has_returning=bool(expression.find(exp.Returning)),
            is_from_select=self._is_insert_from_select(expression),
            parameters=self._extract_parameters(expression),
            tables=self._extract_all_tables(expression),
            complexity_score=self._calculate_complexity_score(expression),
            uses_subqueries=self._has_subqueries(expression),
            join_count=self._count_joins(expression),
            aggregate_functions=self._extract_aggregate_functions(expression),
        )

    @staticmethod
    def _extract_primary_table_name(expr: exp.Expression) -> Optional[str]:
        """Extract the primary table name from an expression."""
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
        """Extract column names from an expression."""
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
    def _extract_all_tables(expr: exp.Expression) -> list[str]:
        """Extract all table names referenced in the expression."""
        tables = []
        for table in expr.find_all(exp.Table):
            if hasattr(table, "name"):
                table_name = str(table.name)
                if table_name not in tables:
                    tables.append(table_name)
        return tables

    @staticmethod
    def _is_insert_from_select(expr: exp.Expression) -> bool:
        """Check if this is an INSERT FROM SELECT pattern."""
        if not isinstance(expr, exp.Insert):
            return False
        return bool(expr.expression and isinstance(expr.expression, exp.Select))

    @staticmethod
    def _extract_parameters(expr: exp.Expression) -> dict[str, Any]:
        """Extract parameters from the expression."""
        # This could be enhanced to extract actual parameter placeholders
        return {}

    @staticmethod
    def _calculate_complexity_score(expr: exp.Expression) -> int:
        """Calculate a complexity score for the expression."""
        score = 0

        # Base score for statement type
        if isinstance(expr, exp.Select):
            score += 1
        elif isinstance(expr, (exp.Insert, exp.Update, exp.Delete)):
            score += 2

        # Add points for various complex constructs
        score += len(list(expr.find_all(exp.Join))) * 2  # Joins add complexity
        score += len(list(expr.find_all(exp.Subquery))) * 3  # Subqueries add more
        score += len(list(expr.find_all(exp.Window))) * 2  # Window functions
        score += len(list(expr.find_all(exp.CTE))) * 2  # CTEs
        score += len(list(expr.find_all(exp.Case))) * 1  # Case statements

        return score

    @staticmethod
    def _has_subqueries(expr: exp.Expression) -> bool:
        """Check if the expression contains subqueries."""
        return bool(expr.find(exp.Subquery))

    @staticmethod
    def _count_joins(expr: exp.Expression) -> int:
        """Count the number of joins in the expression."""
        return len(list(expr.find_all(exp.Join)))

    @staticmethod
    def _extract_aggregate_functions(expr: exp.Expression) -> list[str]:
        """Extract aggregate function names from the expression."""
        aggregates = []

        # Common aggregate function types in SQLGlot (using only those that exist)
        aggregate_types = [exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max]

        for agg_type in aggregate_types:
            if expr.find(agg_type):  # Check if this aggregate type exists in the expression
                func_name = agg_type.__name__.lower()
                if func_name not in aggregates:
                    aggregates.append(func_name)

        return aggregates

    def clear_cache(self) -> None:
        """Clear both parse and analysis caches."""
        self._parse_cache.clear()
        self._analysis_cache.clear()
