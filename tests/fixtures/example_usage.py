# ruff: noqa: T201
"""Example usage of SQL formatting utilities."""

from typing import Any, Union

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
def example_with_formatting(
    style: str, dialect: str = "postgres"
) -> tuple[str, Union[tuple[Any, ...], dict[str, Any]]]:
    """Example of using format_sql_parameters for a query with multiple parameters."""
    sql_template = """
    INSERT INTO test_table (name, id, created_at)
    VALUES ({}, {}, {})
    """

    # Get formatted SQL and empty parameters object
    formatted_sql, empty_parameters = format_sql_parameters(sql_template, ["name", "id", "created_at"], style, dialect)

    return formatted_sql, empty_parameters


# Example 3: Creating parameter objects based on style
def example_param_creation(style: str, name: str, id_value: int) -> Union[tuple[Any, ...], dict[str, Any]]:
    """Example of creating parameter objects based on style."""
    values = [name, id_value]
    field_names = ["name", "id"]

    # Create parameters based on style
    return create_tuple_or_dict_parameters(values, field_names, style)


# Usage in tests:
def demo_usage() -> None:
    """Demonstrate usage of the SQL utilities."""
    # Example of tuple_binds style with Postgres dialect
    insert_sql_pg_tuple = example_direct_placeholder("tuple_binds", "postgres")
    print(f"Postgres with tuple binds: {insert_sql_pg_tuple}")
    # Output: INSERT INTO test_table (name) VALUES (%s)

    # Example of named_binds style with SQLite dialect
    insert_sql_sqlite_named = example_direct_placeholder("named_binds", "sqlite")
    print(f"SQLite with named binds: {insert_sql_sqlite_named}")
    # Output: INSERT INTO test_table (name) VALUES (:name)

    # Example of complex query formatting
    complex_sql, empty_parameters = example_with_formatting("tuple_binds", "sqlite")
    print(f"Complex query with SQLite tuple binds: {complex_sql}")
    print(f"Empty parameters object: {empty_parameters}")
    # Output: INSERT INTO test_table (name, id, created_at) VALUES (?, ?, ?)
    # Empty parameters: ()

    # Example of parameter creation
    tuple_parameters = example_param_creation("tuple_binds", "test_name", 123)
    dict_parameters = example_param_creation("named_binds", "test_name", 123)
    print(f"Tuple parameters: {tuple_parameters}")
    print(f"Dict parameters: {dict_parameters}")
    # Output: Tuple parameters: ('test_name', 123)
    # Dict parameters: {'name': 'test_name', 'id': 123}


if __name__ == "__main__":
    demo_usage()
