"""BigQuery adapter compiled helpers."""

import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from google.cloud.bigquery import ArrayQueryParameter, ScalarQueryParameter

from sqlspec.core import DriverParameterProfile, ParameterStyle
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.type_guards import has_value_attribute

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ("build_bigquery_profile", "create_bq_parameters")


def _identity(value: Any) -> Any:
    return value


def _tuple_to_list(value: "tuple[Any, ...] | list[Any]") -> "list[Any]":
    if isinstance(value, list):
        return value
    return list(value)


_BQ_TYPE_MAP: dict[type, tuple[str, str | None]] = {
    bool: ("BOOL", None),
    int: ("INT64", None),
    float: ("FLOAT64", None),
    Decimal: ("BIGNUMERIC", None),
    str: ("STRING", None),
    bytes: ("BYTES", None),
    datetime.date: ("DATE", None),
    datetime.time: ("TIME", None),
    dict: ("JSON", None),
}


def _create_array_parameter(name: str, value: Any, array_type: str) -> ArrayQueryParameter:
    """Create BigQuery ARRAY parameter.

    Args:
        name: Parameter name.
        value: Array value (converted to list, empty list if None).
        array_type: BigQuery array element type.

    Returns:
        ArrayQueryParameter instance.
    """
    return ArrayQueryParameter(name, array_type, [] if value is None else list(value))


def _create_json_parameter(name: str, value: Any, json_serializer: "Callable[[Any], str]") -> ScalarQueryParameter:
    """Create BigQuery JSON parameter as STRING type.

    Args:
        name: Parameter name.
        value: JSON-serializable value.
        json_serializer: Function to serialize to JSON string.

    Returns:
        ScalarQueryParameter with STRING type.
    """
    return ScalarQueryParameter(name, "STRING", json_serializer(value))


def _create_scalar_parameter(name: str, value: Any, param_type: str) -> ScalarQueryParameter:
    """Create BigQuery scalar parameter.

    Args:
        name: Parameter name.
        value: Scalar value.
        param_type: BigQuery parameter type (INT64, FLOAT64, etc.).

    Returns:
        ScalarQueryParameter instance.
    """
    return ScalarQueryParameter(name, param_type, value)


def _get_bq_param_type(value: Any) -> tuple[str | None, str | None]:
    """Determine BigQuery parameter type from Python value.

    Args:
        value: Python value to determine BigQuery type for

    Returns:
        Tuple of (parameter_type, array_element_type)
    """
    if value is None:
        return ("STRING", None)

    value_type = type(value)

    if value_type is datetime.datetime:
        return ("TIMESTAMP" if value.tzinfo else "DATETIME", None)

    if value_type in _BQ_TYPE_MAP:
        return _BQ_TYPE_MAP[value_type]

    if isinstance(value, (list, tuple)):
        if not value:
            msg = "Cannot determine BigQuery ARRAY type for empty sequence."
            raise SQLSpecError(msg)
        element_type, _ = _get_bq_param_type(value[0])
        if element_type is None:
            msg = f"Unsupported element type in ARRAY: {type(value[0])}"
            raise SQLSpecError(msg)
        return "ARRAY", element_type

    return None, None


def _get_bq_param_creator_map(json_serializer: "Callable[[Any], str]") -> dict[str, Any]:
    """Get BigQuery parameter creator map with configurable JSON serializer.

    Args:
        json_serializer: Function to serialize dict/list to JSON string.

    Returns:
        Dictionary mapping parameter types to creator functions.
    """
    return {
        "ARRAY": _create_array_parameter,
        "JSON": lambda name, value, _: _create_json_parameter(name, value, json_serializer),
        "SCALAR": _create_scalar_parameter,
    }


def create_bq_parameters(
    parameters: Any, json_serializer: "Callable[[Any], str]"
) -> "list[ArrayQueryParameter | ScalarQueryParameter]":
    """Create BigQuery QueryParameter objects from parameters.

    Args:
        parameters: Dict of named parameters or list of positional parameters
        json_serializer: Function to serialize dict/list to JSON string

    Returns:
        List of BigQuery QueryParameter objects
    """
    if not parameters:
        return []

    bq_parameters: list[ArrayQueryParameter | ScalarQueryParameter] = []
    param_creator_map = _get_bq_param_creator_map(json_serializer)

    if isinstance(parameters, dict):
        for name, value in parameters.items():
            param_name_for_bq = name.lstrip("@")
            actual_value = value.value if has_value_attribute(value) else value
            param_type, array_element_type = _get_bq_param_type(actual_value)

            if param_type == "ARRAY" and array_element_type:
                creator = param_creator_map["ARRAY"]
                bq_parameters.append(creator(param_name_for_bq, actual_value, array_element_type))
            elif param_type == "JSON":
                creator = param_creator_map["JSON"]
                bq_parameters.append(creator(param_name_for_bq, actual_value, None))
            elif param_type:
                creator = param_creator_map["SCALAR"]
                bq_parameters.append(creator(param_name_for_bq, actual_value, param_type))
            else:
                msg = f"Unsupported BigQuery parameter type for value of param '{name}': {type(actual_value)}"
                raise SQLSpecError(msg)

    elif isinstance(parameters, (list, tuple)):
        msg = "BigQuery driver requires named parameters (e.g., @name); positional parameters are not supported"
        raise SQLSpecError(msg)

    return bq_parameters


def build_bigquery_profile() -> "DriverParameterProfile":
    """Create the BigQuery driver parameter profile."""

    return DriverParameterProfile(
        name="BigQuery",
        default_style=ParameterStyle.NAMED_AT,
        supported_styles={ParameterStyle.NAMED_AT, ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.NAMED_AT,
        supported_execution_styles={ParameterStyle.NAMED_AT},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={
            int: _identity,
            float: _identity,
            bytes: _identity,
            datetime.datetime: _identity,
            datetime.date: _identity,
            datetime.time: _identity,
            Decimal: _identity,
            dict: _identity,
            list: _identity,
            type(None): lambda _: None,
        },
        extras={"json_tuple_strategy": "tuple", "type_coercion_overrides": {list: _identity, tuple: _tuple_to_list}},
        default_dialect="bigquery",
    )
