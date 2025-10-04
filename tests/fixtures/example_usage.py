"""Example usage of SQL formatting utilities."""

from typing import Any

from tests.fixtures.sql_utils import create_tuple_or_dict_parameters, format_placeholder, format_sql_parameters

# Example 1: Direct placeholder formatting
# Before:
# insert_sql = """
# INSERT INTO test_table (name)
# VALUES (%s)
# """ % ("%s" if style == "tuple_binds" else "%(name)s")


# After:
def example_direct_placeholder(style: str, dialect: str = "postgres") -> str:
    """Example of direct placeholder formatting."""
    placeholder = format_placeholder("name", style, dialect)
    return f"""
    INSERT INTO test_table (name)
    VALUES ({placeholder})
    """


# Example 2: Using format_sql_parameters for a more complex query
def example_with_formatting(style: str, dialect: str = "postgres") -> tuple[str, tuple[Any, ...] | dict[str, Any]]:
    """Example of using format_sql_parameters for a query with multiple parameters."""
    sql_template = """
    INSERT INTO test_table (name, id, created_at)
    VALUES ({}, {}, {})
    """

    formatted_sql, empty_parameters = format_sql_parameters(sql_template, ["name", "id", "created_at"], style, dialect)

    return formatted_sql, empty_parameters


def example_param_creation(style: str, name: str, id_value: int) -> tuple[Any, ...] | dict[str, Any]:
    """Example of creating parameter objects based on style."""
    values = [name, id_value]
    field_names = ["name", "id"]

    return create_tuple_or_dict_parameters(values, field_names, style)


def demo_usage() -> None:
    """Demonstrate usage of the SQL utilities."""

    example_direct_placeholder("tuple_binds", "postgres")

    example_direct_placeholder("named_binds", "sqlite")

    _complex_sql, _empty_parameters = example_with_formatting("tuple_binds", "sqlite")

    example_param_creation("tuple_binds", "test_name", 123)
    example_param_creation("named_binds", "test_name", 123)


if __name__ == "__main__":
    demo_usage()
