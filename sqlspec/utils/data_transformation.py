"""Data transformation utilities for SQLSpec.

Provides functions for transforming data structures, particularly for
field name conversion when mapping database results to schema objects.
Used primarily for msgspec field name conversion with rename configurations.
"""

from typing import Any, Callable, Union

__all__ = ("transform_dict_keys",)


def _safe_convert_key(key: Any, converter: Callable[[str], str]) -> Any:
    """Safely convert a key using the converter function.

    Args:
        key: Key to convert (may not be a string).
        converter: Function to convert string keys.

    Returns:
        Converted key if conversion succeeds, original key otherwise.
    """
    if not isinstance(key, str):
        return key

    try:
        return converter(key)
    except (TypeError, ValueError, AttributeError):
        # If conversion fails, return the original key
        return key


def transform_dict_keys(data: Union[dict, list, Any], converter: Callable[[str], str]) -> Union[dict, list, Any]:
    """Transform dictionary keys using the provided converter function.

    Recursively transforms all dictionary keys in a data structure using
    the provided converter function. Handles nested dictionaries, lists
    of dictionaries, and preserves non-dict values unchanged.

    Args:
        data: The data structure to transform. Can be a dict, list, or any other type.
        converter: Function to convert string keys (e.g., camelize, kebabize).

    Returns:
        The transformed data structure with converted keys. Non-dict values
        are returned unchanged.

    Examples:
        Transform snake_case keys to camelCase:

        >>> from sqlspec.utils.text import camelize
        >>> data = {"user_id": 123, "created_at": "2024-01-01"}
        >>> transform_dict_keys(data, camelize)
        {"userId": 123, "createdAt": "2024-01-01"}

        Transform nested structures:

        >>> nested = {
        ...     "user_data": {"first_name": "John", "last_name": "Doe"},
        ...     "order_items": [
        ...         {"item_id": 1, "item_name": "Product A"},
        ...         {"item_id": 2, "item_name": "Product B"},
        ...     ],
        ... }
        >>> transform_dict_keys(nested, camelize)
        {
            "userData": {
                "firstName": "John",
                "lastName": "Doe"
            },
            "orderItems": [
                {"itemId": 1, "itemName": "Product A"},
                {"itemId": 2, "itemName": "Product B"}
            ]
        }
    """
    if isinstance(data, dict):
        return _transform_dict(data, converter)
    if isinstance(data, list):
        return _transform_list(data, converter)
    return data


def _transform_dict(data: dict, converter: Callable[[str], str]) -> dict:
    """Transform a dictionary's keys recursively.

    Args:
        data: Dictionary to transform.
        converter: Function to convert string keys.

    Returns:
        Dictionary with transformed keys and recursively transformed values.
    """
    transformed = {}

    for key, value in data.items():
        # Convert the key using the provided converter function
        # Use safe conversion that handles edge cases without try-except
        converted_key = _safe_convert_key(key, converter)

        # Recursively transform the value
        transformed_value = transform_dict_keys(value, converter)

        transformed[converted_key] = transformed_value

    return transformed


def _transform_list(data: list, converter: Callable[[str], str]) -> list:
    """Transform a list's elements recursively.

    Args:
        data: List to transform.
        converter: Function to convert string keys in nested structures.

    Returns:
        List with recursively transformed elements.
    """
    # Use list comprehension for better performance and avoid try-except in loop
    return [transform_dict_keys(item, converter) for item in data]
