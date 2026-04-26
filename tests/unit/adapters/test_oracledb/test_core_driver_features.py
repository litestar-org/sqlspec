"""Unit tests for OracleDB ``apply_driver_features`` defaults."""

from sqlspec.adapters.oracledb.core import apply_driver_features
from sqlspec.typing import NUMPY_INSTALLED


def test_apply_driver_features_returns_dict_when_input_none() -> None:
    """``apply_driver_features(None)`` returns a populated defaults dict."""
    features = apply_driver_features(None)

    assert isinstance(features, dict)
    assert "enable_numpy_vectors" in features
    assert "enable_lowercase_column_names" in features
    assert "enable_uuid_binary" in features


def test_apply_driver_features_sets_vector_return_format_default() -> None:
    """``vector_return_format`` defaults to ``"numpy"`` when NumPy is installed,
    otherwise ``"list"``.

    Mirrors the policy in chapter-3/spec.md §3 T5: NumPy users keep zero-copy
    ndarray reads as the default; pure-Python users get list[float|int].
    """
    features = apply_driver_features({})

    expected = "numpy" if NUMPY_INSTALLED else "list"
    assert features["vector_return_format"] == expected


def test_apply_driver_features_preserves_user_vector_return_format() -> None:
    """User-supplied ``vector_return_format`` is not overwritten by the default."""
    features = apply_driver_features({"vector_return_format": "list"})

    assert features["vector_return_format"] == "list"


def test_apply_driver_features_preserves_user_array_return_format() -> None:
    """User-supplied ``"array"`` return format survives the defaults pass."""
    features = apply_driver_features({"vector_return_format": "array"})

    assert features["vector_return_format"] == "array"


def test_oracle_driver_features_typeddict_advertises_vector_return_format() -> None:
    """``OracleDriverFeatures`` exposes ``vector_return_format`` as a NotRequired field."""
    from sqlspec.adapters.oracledb.config import OracleDriverFeatures

    annotations = OracleDriverFeatures.__annotations__
    assert "vector_return_format" in annotations
