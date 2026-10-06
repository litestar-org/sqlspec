"""Unit tests for Decimal, JSON, Vector (FLOAT32), and INTERVAL parameter type inference."""

from datetime import timedelta
from decimal import Decimal

import pytest
from google.cloud.spanner_v1.data_types import Interval
from google.cloud.spanner_v1.types.type import TypeCode

from sqlspec.adapters.spanner.type_converter import coerce_params_for_spanner, infer_spanner_param_types
from sqlspec.core import TypedParameter


@pytest.mark.parametrize("duration", [timedelta(), timedelta(days=2, microseconds=123456), timedelta(microseconds=-1)])
def test_coerce_interval_parameters(duration: timedelta) -> None:
    expected = Interval(days=duration.days, nanos=(duration.seconds * 1_000_000 + duration.microseconds) * 1000)
    params = {
        "duration": duration,
        "typed": TypedParameter(duration, "INTERVAL"),
        "durations": [None, duration],
        "tuple_durations": (duration, None),
        "null": TypedParameter(None, timedelta),
    }
    assert coerce_params_for_spanner(params) == {
        "duration": expected,
        "typed": expected,
        "durations": [None, expected],
        "tuple_durations": [expected, None],
        "null": None,
    }


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


def test_infer_boolean_array_param_types() -> None:
    types = infer_spanner_param_types({"flags": [True, False]})
    assert types["flags"].code == TypeCode.ARRAY
    assert types["flags"].array_element_type.code == TypeCode.BOOL


def test_infer_and_coerce_float32_and_vector_params() -> None:
    """Verify FLOAT32 and ARRAY<FLOAT32>/VECTOR TypedParameter inference and coercion."""
    params = {
        "score": TypedParameter(0.25, "FLOAT32"),
        "embedding": TypedParameter([0.1, 0.2, 0.3], "ARRAY<FLOAT32>"),
        "vec": TypedParameter((1, 2, 3), "VECTOR"),
        "null_score": TypedParameter(None, "FLOAT32"),
        "null_embedding": TypedParameter(None, "ARRAY<FLOAT32>"),
    }
    types = infer_spanner_param_types(params)
    assert types["score"].code == TypeCode.FLOAT32
    assert types["embedding"].code == TypeCode.ARRAY
    assert types["embedding"].array_element_type.code == TypeCode.FLOAT32
    assert types["vec"].code == TypeCode.ARRAY
    assert types["vec"].array_element_type.code == TypeCode.FLOAT32
    assert types["null_score"].code == TypeCode.FLOAT32
    assert types["null_embedding"].code == TypeCode.ARRAY
    assert types["null_embedding"].array_element_type.code == TypeCode.FLOAT32

    coerced = coerce_params_for_spanner(params)
    assert coerced is not None
    assert coerced["score"] == 0.25
    assert isinstance(coerced["score"], float)
    assert coerced["embedding"] == [0.1, 0.2, 0.3]
    assert coerced["vec"] == [1.0, 2.0, 3.0]
    assert all(isinstance(v, float) for v in coerced["vec"])
    assert coerced["null_score"] is None
    assert coerced["null_embedding"] is None


def test_infer_interval_timedelta_params() -> None:
    """Verify timedelta and list[timedelta] infer as INTERVAL and Array(INTERVAL)."""
    params = {
        "duration": timedelta(days=1, hours=2),
        "durations": [timedelta(minutes=5), timedelta(minutes=10)],
        "null_duration": TypedParameter(None, timedelta),
        "declared_interval": TypedParameter(timedelta(seconds=30), "INTERVAL"),
    }
    types = infer_spanner_param_types(params)
    assert types["duration"].code == TypeCode.INTERVAL
    assert types["durations"].code == TypeCode.ARRAY
    assert types["durations"].array_element_type.code == TypeCode.INTERVAL
    assert types["null_duration"].code == TypeCode.INTERVAL
    assert types["declared_interval"].code == TypeCode.INTERVAL
