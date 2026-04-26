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


def test_apply_driver_features_sets_varchar2_byte_limit_default() -> None:
    """``oracle_varchar2_byte_limit`` defaults to 4000 (Oracle SQL VARCHAR2 limit)."""
    features = apply_driver_features({})

    assert features["oracle_varchar2_byte_limit"] == 4000


def test_apply_driver_features_sets_raw_byte_limit_default() -> None:
    """``oracle_raw_byte_limit`` defaults to 2000 (Oracle SQL RAW limit)."""
    features = apply_driver_features({})

    assert features["oracle_raw_byte_limit"] == 2000


def test_apply_driver_features_preserves_user_varchar2_byte_limit() -> None:
    """User-supplied ``oracle_varchar2_byte_limit`` is not overwritten by the default.

    MAX_STRING_SIZE=EXTENDED databases may set this to 32767 to keep larger
    strings as VARCHAR2 instead of auto-coercing to CLOB.
    """
    features = apply_driver_features({"oracle_varchar2_byte_limit": 32767})

    assert features["oracle_varchar2_byte_limit"] == 32767


def test_apply_driver_features_preserves_user_raw_byte_limit() -> None:
    """User-supplied ``oracle_raw_byte_limit`` is not overwritten by the default."""
    features = apply_driver_features({"oracle_raw_byte_limit": 100})

    assert features["oracle_raw_byte_limit"] == 100


def test_oracle_driver_features_typeddict_advertises_varchar2_byte_limit() -> None:
    """``OracleDriverFeatures`` exposes ``oracle_varchar2_byte_limit``."""
    from sqlspec.adapters.oracledb.config import OracleDriverFeatures

    assert "oracle_varchar2_byte_limit" in OracleDriverFeatures.__annotations__


def test_oracle_driver_features_typeddict_advertises_raw_byte_limit() -> None:
    """``OracleDriverFeatures`` exposes ``oracle_raw_byte_limit``."""
    from sqlspec.adapters.oracledb.config import OracleDriverFeatures

    assert "oracle_raw_byte_limit" in OracleDriverFeatures.__annotations__
