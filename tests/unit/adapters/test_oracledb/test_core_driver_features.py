# pyright: reportArgumentType=false
"""Unit tests for OracleDB ``apply_driver_features`` defaults."""

import inspect
from typing import TYPE_CHECKING, cast

import pytest

import sqlspec.adapters.oracledb._typing as oracle_typing
from sqlspec.adapters.oracledb import config
from sqlspec.adapters.oracledb._typing import OracleAsyncCursor
from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.core import apply_driver_features
from sqlspec.typing import NUMPY_INSTALLED

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection


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


def test_apply_driver_features_honors_numpy_vectors_opt_out() -> None:
    """``enable_numpy_vectors=False`` keeps the driver-native VECTOR return type."""
    features = apply_driver_features({"enable_numpy_vectors": False})
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


def test_apply_driver_features_does_not_default_fetch_tuning_options() -> None:
    """Absent fetch tuning keys should leave python-oracledb defaults untouched."""
    features = apply_driver_features({})
    assert "arraysize" not in features
    assert "prefetchrows" not in features
    assert "fetch_lobs" not in features
    assert "fetch_decimals" not in features


def test_apply_driver_features_preserves_user_fetch_tuning_options() -> None:
    """User-supplied fetch tuning options survive the defaults pass."""
    features = apply_driver_features({"arraysize": 5000, "fetch_decimals": True})
    assert features["arraysize"] == 5000
    assert features["fetch_decimals"] is True


def test_oracle_driver_features_typeddict_advertises_varchar2_byte_limit() -> None:
    """``OracleDriverFeatures`` exposes ``oracle_varchar2_byte_limit``."""
    from sqlspec.adapters.oracledb.config import OracleDriverFeatures

    assert "oracle_varchar2_byte_limit" in OracleDriverFeatures.__annotations__


def test_oracle_driver_features_typeddict_advertises_raw_byte_limit() -> None:
    """``OracleDriverFeatures`` exposes ``oracle_raw_byte_limit``."""
    from sqlspec.adapters.oracledb.config import OracleDriverFeatures

    assert "oracle_raw_byte_limit" in OracleDriverFeatures.__annotations__


def test_config_no_mypyc_attr_oracledb_config_module_has_no_mypyc_attr_import() -> None:
    assert "mypyc_attr" not in config.__dict__


def test_config_no_mypyc_attr_oracledb_configs_importable() -> None:
    assert OracleAsyncConfig.__name__ == "OracleAsyncConfig"
    assert OracleSyncConfig.__name__ == "OracleSyncConfig"


class FakeAsyncRawCursor:
    def __init__(self, *, raise_on_close: bool = False) -> None:
        self.closed = False
        self.raise_on_close = raise_on_close

    def close(self) -> None:
        self.closed = True
        if self.raise_on_close:
            raise RuntimeError("close failed")


class FakeAsyncConnection:
    def __init__(self, cursor: FakeAsyncRawCursor) -> None:
        self.cursor_instance = cursor

    def cursor(self) -> FakeAsyncRawCursor:
        return self.cursor_instance


def test_typing_cursors_oracle_async_cursor_aexit_signature_uses_varargs() -> None:
    parameters = list(inspect.signature(OracleAsyncCursor.__aexit__).parameters.values())
    assert [parameter.name for parameter in parameters] == ["self", "_"]
    assert parameters[1].kind is inspect.Parameter.VAR_POSITIONAL


@pytest.mark.anyio
async def test_typing_cursors_oracle_async_cursor_closes_cursor() -> None:
    raw_cursor = FakeAsyncRawCursor()
    cursor = OracleAsyncCursor(cast("OracleAsyncConnection", FakeAsyncConnection(raw_cursor)))
    await cursor.__aenter__()
    await cursor.__aexit__(None, None, None)
    assert raw_cursor.closed is True


@pytest.mark.anyio
async def test_typing_cursors_oracle_async_cursor_suppresses_close_errors() -> None:
    raw_cursor = FakeAsyncRawCursor(raise_on_close=True)
    cursor = OracleAsyncCursor(cast("OracleAsyncConnection", FakeAsyncConnection(raw_cursor)))
    await cursor.__aenter__()
    await cursor.__aexit__(RuntimeError, RuntimeError("body failed"), None)
    assert raw_cursor.closed is True


def test_typing_exports_oracle_vector_type_alias_removed() -> None:
    assert not hasattr(oracle_typing, "OracleVectorType")
    assert "OracleVectorType" not in oracle_typing.__all__


def test_typing_exports_oracle_sync_cursor_still_importable() -> None:
    from sqlspec.adapters.oracledb._typing import OracleSyncCursor

    assert OracleSyncCursor is oracle_typing.OracleSyncCursor
