"""Unit tests for Decimal and INTERVAL parameter type inference."""

from datetime import timedelta
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


def test_infer_timedelta_param_types() -> None:
    """Verify that timedelta parameters infer as INTERVAL."""
    params = {"duration": timedelta(days=1, hours=2)}
    types = infer_spanner_param_types(params)
    assert "duration" in types
    assert types["duration"].code == TypeCode.INTERVAL


def test_null_timedelta_param_types() -> None:
    """Verify that null TypedParameter with timedelta resolves to INTERVAL."""
    params = {"duration": TypedParameter(None, timedelta)}
    types = infer_spanner_param_types(params)
    assert "duration" in types
    assert types["duration"].code == TypeCode.INTERVAL


def test_null_json_param_types() -> None:
    """Verify that null TypedParameter with dict or JSON resolves to JSON."""
    params = {"meta": TypedParameter(None, dict), "payload": TypedParameter(None, "JSON")}
    types = infer_spanner_param_types(params)
    assert types["meta"].code == TypeCode.JSON
    assert types["payload"].code == TypeCode.JSON
