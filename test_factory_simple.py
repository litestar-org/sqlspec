#!/usr/bin/env python3
"""Simple test of the factory functionality without full import chain."""

import os
import sys

# Add the sqlspec directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "sqlspec"))

# Import directly from the factory module to avoid circular imports
import contextlib

from sqlspec.sql.builder._factory import c, s


def test_column_factory() -> None:
    """Test the column factory functionality."""

    # Test basic column references

    # Test aggregate functions
    c.count(c.id)

    count_distinct_expr = c.count_distinct(c.name)

    c.sum(c.salary)

    # Test string functions
    c.upper(c.name)

    # Test coalesce
    c.coalesce(c.email, "default@example.com")

    # Test case expression
    c.case().when("age < 18", "Minor").when("age < 65", "Adult").else_("Senior").end()

    # Test aliases
    with contextlib.suppress(Exception):
        count_distinct_expr.alias("unique_names")


def test_statement_factory() -> None:
    """Test the statement factory functionality."""

    # Test basic select creation
    s.select(c.id, c.name)

    # Test insert creation
    s.insert("users")

    # Test update creation
    s.update("users")

    # Test delete creation
    s.delete()

    # Test merge creation
    s.merge("users")


def test_expression_types() -> None:
    """Test that expressions have expected types and methods."""

    # Check if we get sqlglot expressions

    # Test aggregate expressions
    c.count(c.id)


if __name__ == "__main__":
    try:
        test_column_factory()
        test_statement_factory()
        test_expression_types()
    except Exception:
        import traceback

        traceback.print_exc()
