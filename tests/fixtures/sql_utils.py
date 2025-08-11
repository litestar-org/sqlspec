from typing import Any, Optional, Union


def format_placeholder(field_name: str, style: str, dialect: Optional[str] = None) -> str:
    """Format a placeholder in SQL based on the parameter style.

    Args:
        field_name: The name of the field to format.
        style: The parameter style, either "tuple_binds" or "named_binds".
        dialect: The SQL dialect (e.g., "postgres", "sqlite"). Defaults to None.

    Returns:
        The formatted placeholder string.
    """
    if style == "tuple_binds":
        if dialect in {"sqlite", "duckdb", "aiosqlite"}:
            return "?"
        # Default to Postgres/BigQuery style
        return "%s"
    if dialect == "duckdb":
        return f"${field_name}"
    if dialect in {"sqlite", "aiosqlite"}:
        return f":{field_name}"
    # For postgres and similar
    return f"%({field_name})s"


def format_sql(sql_template: str, field_names: list[str], style: str, dialect: Optional[str] = None) -> str:
    """Format a SQL string by replacing template placeholders with dialect/style-specific placeholders.

    This function can handle multiple placeholders in a single SQL string.

    Args:
        sql_template: A SQL string with {} placeholders.
        field_names: A list of field names corresponding to each placeholder.
        style: The parameter style, either "tuple_binds" or "named_binds".
        dialect: The SQL dialect (e.g., "postgres", "sqlite"). Defaults to None.

    Returns:
        The SQL string with appropriate placeholders.

    Example:
        ```
        sql = format_sql(
            "INSERT INTO table (name, id) VALUES ({}, {})",
            ["name", "id"],
            "tuple_binds",
            "postgres",
        )
        # Result: "INSERT INTO table (name, id) VALUES (%s, %s)"
        ```
    """
    placeholders = [format_placeholder(field, style, dialect) for field in field_names]
    return sql_template.format(*placeholders)


def format_sql_parameters(
    sql_template: str, param_fields: list[str], style: str, dialect: Optional[str] = None
) -> tuple[str, Union[tuple[Any, ...], dict[str, Any]]]:
    """Format SQL template and create the appropriate parameter object based on style.

    Args:
        sql_template: The SQL template with placeholders to be replaced.
        param_fields: List of field names to be used in the SQL.
        style: The parameter style, either "tuple_binds" or "named_binds".
        dialect: The SQL dialect (e.g., "postgres", "sqlite"). Defaults to None.

    Returns:
        A tuple containing the formatted SQL string and an empty parameters object of the correct type.
    """
    formatted_sql = format_sql(sql_template, param_fields, style, dialect)

    # Return appropriate empty parameter container based on style
    empty_parameters: Union[tuple[Any, ...], dict[str, Any]] = () if style == "tuple_binds" else {}

    return formatted_sql, empty_parameters


def create_tuple_or_dict_parameters(
    values: list[Any], field_names: list[str], style: str
) -> Union[tuple[Any, ...], dict[str, Any]]:
    """Create the appropriate parameter object based on values and style.

    Args:
        values: List of values for the parameters.
        field_names: List of field names corresponding to the values.
        style: The parameter style, either "tuple_binds" or "named_binds".

    Returns:
        Either a tuple of values or a dictionary mapping field names to values.
    """
    return tuple(values) if style == "tuple_binds" else dict(zip(field_names, values))
