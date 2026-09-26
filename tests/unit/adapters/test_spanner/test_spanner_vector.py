"""Unit tests for Spanner FLOAT32 and ARRAY<FLOAT32> vector parameter typing."""

from google.cloud.spanner_v1.types.type import TypeCode

from sqlspec.adapters.spanner.type_converter import coerce_params_for_spanner, infer_spanner_param_types
from sqlspec.core import TypedParameter


def test_infer_vector_param_types_array_float32() -> None:
    """Verify that TypedParameter with ARRAY<FLOAT32> infers as Array(FLOAT32)."""
    params = {"embedding": TypedParameter([0.1, 0.2, 0.3], "ARRAY<FLOAT32>")}
    types = infer_spanner_param_types(params)
    assert "embedding" in types
    assert types["embedding"].code == TypeCode.ARRAY
    assert types["embedding"].array_element_type.code == TypeCode.FLOAT32


def test_infer_vector_param_types_vector_alias() -> None:
    """Verify that TypedParameter with VECTOR infers as Array(FLOAT32)."""
    params = {"embedding": TypedParameter([0.1, 0.2, 0.3], "VECTOR")}
    types = infer_spanner_param_types(params)
    assert "embedding" in types
    assert types["embedding"].code == TypeCode.ARRAY
    assert types["embedding"].array_element_type.code == TypeCode.FLOAT32


def test_infer_scalar_float32() -> None:
    """Verify that TypedParameter with FLOAT32 infers as FLOAT32."""
    params = {"score": TypedParameter(1.25, "FLOAT32")}
    types = infer_spanner_param_types(params)
    assert "score" in types
    assert types["score"].code == TypeCode.FLOAT32


def test_coerce_vector_params() -> None:
    """Verify that TypedParameter vector is unwrapped into a list of floats."""
    params = {"embedding": TypedParameter((0.1, 0.2, 0.3), "ARRAY<FLOAT32>")}
    coerced = coerce_params_for_spanner(params)
    assert coerced is not None
    assert coerced["embedding"] == [0.1, 0.2, 0.3]


def test_null_float32_param_type() -> None:
    """Verify that NULL TypedParameter with FLOAT32 resolves to param_types.FLOAT32."""
    params = {"score": TypedParameter(None, "FLOAT32")}
    types = infer_spanner_param_types(params)
    assert "score" in types
    assert types["score"].code == TypeCode.FLOAT32
