"""Unit tests for Decimal and JSON parameter type inference."""

from decimal import Decimal

from google.cloud.spanner_v1.types.type import TypeCode

from sqlspec.adapters.spanner.type_converter import infer_spanner_param_types
from sqlspec.core import TypedParameter


def test_infer_decimal_param_types() -> None:
    """Verify that non-null Decimal parameters infer as NUMERIC."""
    params = {"price": Decimal("19.99")}
    types = infer_spanner_param_types(params)
    assert "price" in types
    assert types["price"].code == TypeCode.NUMERIC


def test_infer_decimal_array_param_types() -> None:
    """Verify that sequences of Decimal parameters infer as Array(NUMERIC)."""
    params = {"prices": [Decimal("19.99"), Decimal("29.99")]}
    types = infer_spanner_param_types(params)
    assert "prices" in types
    assert types["prices"].code == TypeCode.ARRAY
    assert types["prices"].array_element_type.code == TypeCode.NUMERIC


def test_null_json_param_types() -> None:
    """Verify that null TypedParameter with dict resolves to JSON."""
    params = {"meta": TypedParameter(None, dict)}
    types = infer_spanner_param_types(params)
    assert types["meta"].code == TypeCode.JSON
