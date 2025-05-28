"""SQL utility functions and helpers.

This module provides common SQL utilities for formatting, analysis,
and transformation of SQL statements.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import OptimizeError, ParseError

from sqlspec.exceptions import SQLConversionError, SQLParsingError

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = (
    "SQLAnalysis",
    "count_parameters",
    "extract_columns",
    "extract_literals",
    "extract_tables",
    "format_sql",
    "get_query_type",
    "minify_sql",
    "normalize_sql",
    "sql_fingerprint",
)

logger = logging.getLogger("sqlspec.sql.utils")


class SQLAnalysis:
    """Analysis result for SQL statements."""

    def __init__(
        self,
        query_type: str,
        tables: list[str],
        columns: list[str],
        parameters: list[str],
        literals: list[Any],
        complexity_score: int,
        line_count: int,
        character_count: int,
    ) -> None:  # Added return type hint
        self.query_type = query_type
        self.tables = tables
        self.columns = columns
        self.parameters = parameters
        self.literals = literals
        self.complexity_score = complexity_score
        self.line_count = line_count
        self.character_count = character_count

    def __repr__(self) -> str:
        return (
            f"SQLAnalysis(type={self.query_type}, tables={len(self.tables)}, "
            f"columns={len(self.columns)}, params={len(self.parameters)}, "
            f"complexity={self.complexity_score})"
        )


def format_sql(sql: str, dialect: DialectType = None, pretty: bool = True) -> str:
    if not sql or not sql.strip():
        return sql

    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
    except ParseError as e:
        err_msg = f"Failed to parse SQL for formatting: {e}"
        raise SQLParsingError(err_msg) from e
    try:
        return parsed.sql(dialect=dialect, pretty=pretty)
    except OptimizeError as e:
        err_msg = f"Failed to generate formatted SQL: {e}"
        raise SQLConversionError(err_msg) from e
    except Exception as e:  # Fallback for other sqlglot errors
        err_msg = f"An unexpected error occurred during SQL formatting: {e}"
        raise SQLConversionError(err_msg) from e


def minify_sql(sql: str, dialect: DialectType = None) -> str:
    """Minify SQL by removing unnecessary whitespace and comments.

    Args:
        sql: SQL string to minify
        dialect: SQL dialect for parsing

    Returns:
        Minified SQL string
    """
    if not sql or not sql.strip():
        return sql

    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
        return parsed.sql(dialect=dialect, pretty=False)
    except (ParseError, OptimizeError) as e:  # Catch specific sqlglot errors and assign to e
        logger.warning("sqlglot parsing/generation failed during minify_sql (%s), falling back to regex.", e)
        sql = re.sub(r"\\s+", " ", sql.strip())
        sql = re.sub(r"--[^\r\n]*", "", sql)  # Remove line comments
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)  # Remove block comments
        return sql.strip()


def extract_tables(sql: str, dialect: DialectType = None) -> list[str]:
    """Extract table names from SQL statement.

    Args:
        sql: SQL string to analyze
        dialect: SQL dialect for parsing

    Returns:
        List of table names found in the SQL
    """
    if not sql or not sql.strip():
        return []

    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
        tables: list[str] = []
        for table_exp in parsed.find_all(exp.Table):
            table_name = table_exp.name
            if table_exp.db:
                table_name = f"{table_exp.db}.{table_name}"
            if table_exp.catalog:
                table_name = f"{table_exp.catalog}.{table_name}"
            tables.append(table_name)
        return sorted(set(tables))
    except ParseError as e:
        logger.warning("Failed to parse SQL for table extraction: %s", str(e)[:100])
        return []


def extract_columns(sql: str, dialect: DialectType = None) -> list[str]:
    """Extract column names from SQL statement.

    Args:
        sql: SQL string to analyze
        dialect: SQL dialect for parsing

    Returns:
        List of column names found in the SQL
    """
    if not sql or not sql.strip():
        return []

    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
        columns: list[str] = [col_exp.sql(dialect=dialect) for col_exp in parsed.find_all(exp.Column)]
        columns.extend(
            identifier_exp.sql(dialect=dialect)
            for identifier_exp in parsed.find_all(exp.Identifier)
            if not isinstance(identifier_exp.parent, (exp.Table, exp.Func))
        )
        return sorted(set(columns))
    except ParseError as e:
        logger.warning("Failed to parse SQL for column extraction: %s", str(e)[:100])
        return []


def get_query_type(sql: str, dialect: DialectType = None) -> str:
    """Determine the type of SQL query.

    Args:
        sql: SQL string to analyze
        dialect: SQL dialect for parsing

    Returns:
        Query type (SELECT, INSERT, UPDATE, DELETE, etc.)
    """
    if not sql or not sql.strip():
        return "UNKNOWN"

    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
        return type(parsed).__name__.upper()
    except Exception:  # noqa: BLE001
        sql_clean = sql.strip().upper()
        if sql_clean.startswith("SELECT"):
            return "SELECT"
        if sql_clean.startswith("INSERT"):
            return "INSERT"
        if sql_clean.startswith("UPDATE"):
            return "UPDATE"
        if sql_clean.startswith("DELETE"):
            return "DELETE"
        if sql_clean.startswith("CREATE"):
            return "CREATE"
        if sql_clean.startswith("ALTER"):
            return "ALTER"
        if sql_clean.startswith("DROP"):
            return "DROP"
        return "UNKNOWN"


def normalize_sql(sql: str, dialect: DialectType = None, remove_comments: bool = True) -> str:
    """Normalize SQL for comparison and caching.

    Args:
        sql: SQL string to normalize
        dialect: SQL dialect for parsing
        remove_comments: Whether to remove comments from the SQL

    Returns:
        Normalized SQL string
    """
    if not sql or not sql.strip():
        return sql

    try:
        normalized_sql = sqlglot.parse_one(sql, read=dialect).sql(dialect=dialect, pretty=False)
        if remove_comments:
            normalized_sql = re.sub(r"--[^\r\n]*", "", normalized_sql)
            normalized_sql = re.sub(r"/\*.*?\*/", "", normalized_sql, flags=re.DOTALL)
        return normalized_sql.lower().strip()  # Return directly
    except ParseError as e:  # Catch specific ParseError
        logger.warning(
            "sqlglot parsing failed during normalize_sql (%s), falling back to basic regex normalization.", e
        )
        # Fallback to basic regex normalization if sqlglot fails
        if remove_comments:
            sql = re.sub(r"--[^\r\n]*", "", sql)
            sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        return " ".join(sql.lower().split())


def sql_fingerprint(sql: str, dialect: DialectType = None) -> str:
    """Generate a fingerprint for SQL statement.

    Args:
        sql: SQL string to fingerprint
        dialect: SQL dialect for parsing

    Returns:
        Unique fingerprint string for the SQL structure
    """
    if not sql or not sql.strip():
        return ""

    try:
        parsed = sqlglot.parse_one(normalize_sql(sql, dialect=dialect), read=dialect)
        for literal_exp in parsed.find_all(exp.Literal):
            literal_exp.replace(exp.Placeholder(this="?"))
        return parsed.sql(pretty=False)
    except (ParseError, OptimizeError) as e:
        logger.warning(
            "sqlglot parsing/generation failed during sql_fingerprint (%s), returning normalized SQL as fingerprint.", e
        )
        return normalize_sql(sql, dialect=dialect)


def count_parameters(sql: str, dialect: DialectType = None) -> dict[str, int]:
    """Count different types of parameters in SQL.

    Args:
        sql: SQL string to analyze
        dialect: SQL dialect for parsing

    Returns:
        Dictionary with parameter type counts
    """
    if not sql or not sql.strip():
        return {"total": 0, "named": 0, "positional": 0}

    counts = {"total": 0, "named": 0, "positional": 0}

    try:
        parsed = sqlglot.parse_one(sql, read=dialect)

        for placeholder in parsed.find_all(exp.Placeholder):
            counts["total"] += 1
            if placeholder.this and not placeholder.this.isdigit():
                counts["named"] += 1
            else:
                counts["positional"] += 1

    except Exception:  # noqa: BLE001
        named_params = len(re.findall(r":\w+", sql))
        positional_params = len(re.findall(r"\?", sql))
        dollar_params = len(re.findall(r"\$\w+", sql))

        counts["named"] = named_params + dollar_params
        counts["positional"] = positional_params
        counts["total"] = counts["named"] + counts["positional"]

    return counts


def extract_literals(sql: str, dialect: DialectType = None) -> list[Any]:
    """Extract literal values from SQL statement.

    Args:
        sql: SQL string to analyze
        dialect: SQL dialect for parsing

    Returns:
        List of literal values found in the SQL
    """
    if not sql or not sql.strip():
        return []

    try:
        return [lit.this for lit in sqlglot.parse_one(sql, read=dialect).find_all(exp.Literal)]
    except ParseError as e:
        logger.warning("Failed to parse SQL for literal extraction: %s", str(e)[:100])
        return []


def analyze_sql(sql: str, dialect: DialectType = None) -> SQLAnalysis:
    """Perform comprehensive analysis of SQL statement.

    Args:
        sql: SQL string to analyze
        dialect: SQL dialect for parsing

    Returns:
        SQLAnalysis object with detailed information
    """
    if not sql or not sql.strip():
        return SQLAnalysis(
            query_type="UNKNOWN",
            tables=[],
            columns=[],
            parameters=[],
            literals=[],
            complexity_score=0,
            line_count=0,
            character_count=0,
        )

    line_count = sql.count("\n") + 1
    character_count = len(sql)

    query_type = get_query_type(sql, dialect)
    tables = extract_tables(sql, dialect)
    columns = extract_columns(sql, dialect)
    literals = extract_literals(sql, dialect)
    param_counts = count_parameters(sql, dialect)
    parameters = [f"param_{i}" for i in range(param_counts["total"])]
    complexity_score = len(tables) * 2 + len(columns) + param_counts["total"] + len(literals) + line_count

    return SQLAnalysis(
        query_type=query_type,
        tables=tables,
        columns=columns,
        parameters=parameters,
        literals=literals,
        complexity_score=complexity_score,
        line_count=line_count,
        character_count=character_count,
    )
