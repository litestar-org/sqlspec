# pyright: reportArgumentType=false
"""Unit tests for OracleDB ``apply_driver_features`` defaults."""

import inspect
import io
import shutil
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest

import sqlspec.adapters.oracledb._typing as oracle_typing
from sqlspec.adapters.oracledb import config
from sqlspec.adapters.oracledb._typing import OracleAsyncCursor
from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleDriverFeatures, OracleSyncConfig
from sqlspec.adapters.oracledb.core import (
    OracleEndpoint,
    OracleTopologyConfig,
    apply_driver_features,
    build_duality_etag_update_sql,
    build_tns_description,
    extract_duality_etag,
    extract_duality_view_etag,
    extract_oracle_wallet_zip,
    inject_duality_view_etag,
    sanitize_oracle_text_query,
)
from sqlspec.core import StatementConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import NUMPY_INSTALLED

if TYPE_CHECKING:
    from sqlspec.adapters.oracledb._typing import OracleAsyncConnection


def _apply_features(features: dict[str, object] | None = None) -> dict[str, object]:
    _, resolved_features = apply_driver_features(StatementConfig(), features)
    return resolved_features


def test_apply_driver_features_returns_dict_when_input_none() -> None:
    """``apply_driver_features(None)`` returns a populated defaults dict."""
    features = _apply_features()
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
    features = _apply_features({})
    expected = "numpy" if NUMPY_INSTALLED else "list"
    assert features["vector_return_format"] == expected


def test_apply_driver_features_preserves_user_vector_return_format() -> None:
    """User-supplied ``vector_return_format`` is not overwritten by the default."""
    features = _apply_features({"vector_return_format": "list"})
    assert features["vector_return_format"] == "list"


def test_apply_driver_features_preserves_user_array_return_format() -> None:
    """User-supplied ``"array"`` return format survives the defaults pass."""
    features = _apply_features({"vector_return_format": "array"})
    assert features["vector_return_format"] == "array"


def test_apply_driver_features_honors_numpy_vectors_opt_out() -> None:
    """``enable_numpy_vectors=False`` keeps the driver-native VECTOR return type."""
    features = _apply_features({"enable_numpy_vectors": False})
    assert features["vector_return_format"] == "array"


def test_oracle_driver_features_typeddict_advertises_vector_return_format() -> None:
    """``OracleDriverFeatures`` exposes ``vector_return_format`` as a NotRequired field."""
    from sqlspec.adapters.oracledb.config import OracleDriverFeatures

    annotations = OracleDriverFeatures.__annotations__
    assert "vector_return_format" in annotations


def test_apply_driver_features_sets_varchar2_byte_limit_default() -> None:
    """``oracle_varchar2_byte_limit`` defaults to 4000 (Oracle SQL VARCHAR2 limit)."""
    features = _apply_features({})
    assert features["oracle_varchar2_byte_limit"] == 4000


def test_apply_driver_features_sets_raw_byte_limit_default() -> None:
    """``oracle_raw_byte_limit`` defaults to 2000 (Oracle SQL RAW limit)."""
    features = _apply_features({})
    assert features["oracle_raw_byte_limit"] == 2000


def test_apply_driver_features_preserves_user_varchar2_byte_limit() -> None:
    """User-supplied ``oracle_varchar2_byte_limit`` is not overwritten by the default.

    MAX_STRING_SIZE=EXTENDED databases may set this to 32767 to keep larger
    strings as VARCHAR2 instead of auto-coercing to CLOB.
    """
    features = _apply_features({"oracle_varchar2_byte_limit": 32767})
    assert features["oracle_varchar2_byte_limit"] == 32767


def test_apply_driver_features_preserves_user_raw_byte_limit() -> None:
    """User-supplied ``oracle_raw_byte_limit`` is not overwritten by the default."""
    features = _apply_features({"oracle_raw_byte_limit": 100})
    assert features["oracle_raw_byte_limit"] == 100


def test_apply_driver_features_defaults_fetch_lobs_false() -> None:
    """``fetch_lobs`` defaults to ``False`` while unrelated fetch tuning stays unset."""
    features = _apply_features({})
    assert features["fetch_lobs"] is False
    assert "arraysize" not in features
    assert "prefetchrows" not in features
    assert "fetch_decimals" not in features


def test_apply_driver_features_preserves_user_fetch_lobs_true() -> None:
    """User-supplied ``fetch_lobs=True`` remains an explicit opt-in escape hatch."""
    features = _apply_features({"fetch_lobs": True})
    assert features["fetch_lobs"] is True


def test_apply_driver_features_preserves_user_fetch_tuning_options() -> None:
    """User-supplied fetch tuning options survive the defaults pass."""
    features = _apply_features({"arraysize": 5000, "fetch_decimals": True})
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


def test_apply_driver_features_rejects_unknown_keys() -> None:
    assert OracleDriverFeatures is not None
    with pytest.raises(ImproperConfigurationError, match="Unknown Oracle driver_features"):
        apply_driver_features(StatementConfig(), {"unknown_oracle_knob": True})


def test_build_tns_description_multi_endpoint_and_rejects_injection() -> None:
    topology = OracleTopologyConfig(
        endpoints=(
            OracleEndpoint(host="rac1.example.com", port=1521, protocol="tcp"),
            OracleEndpoint(host="rac2.example.com", port=2484, protocol="tcps"),
        ),
        service_name="orcl_high.adb.oraclecloud.com",
        load_balance=True,
        failover=True,
        retry_count=3,
        retry_delay=2,
        transport_connect_timeout=5,
        sdu=65535,
        source_route=True,
        server="DEDICATED",
        failover_mode_type="SELECT",
        failover_mode_method="BASIC",
    )
    dsn = build_tns_description(topology)
    assert "(HOST=rac1.example.com)(PORT=1521)" in dsn
    assert "(PROTOCOL=TCPS)(HOST=rac2.example.com)(PORT=2484)" in dsn
    assert "(SERVICE_NAME=orcl_high.adb.oraclecloud.com)" in dsn
    assert "(LOAD_BALANCE=ON)" in dsn
    assert "(FAILOVER=ON)" in dsn
    assert "(RETRY_COUNT=3)" in dsn
    assert "(RETRY_DELAY=2)" in dsn
    assert "(TRANSPORT_CONNECT_TIMEOUT=5)" in dsn
    assert "(SDU=65535)" in dsn
    assert "(SOURCE_ROUTE=ON)" in dsn
    assert "(SERVER=DEDICATED)" in dsn
    assert "(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC))" in dsn

    with pytest.raises(ImproperConfigurationError):
        build_tns_description(
            OracleTopologyConfig(endpoints=(OracleEndpoint(host="bad)host", port=1521),), service_name="svc")
        )


def test_extract_oracle_wallet_zip_validates_entries_and_permissions() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("ewallet.pem", b"pem-content")
        zf.writestr("tnsnames.ora", b"orcl_high = (DESCRIPTION=...)")
    extracted_dir = extract_oracle_wallet_zip(buf.getvalue())
    try:
        wallet_path = Path(extracted_dir)
        assert (wallet_path / "ewallet.pem").read_bytes() == b"pem-content"
        assert (wallet_path / "tnsnames.ora").exists()
    finally:
        shutil.rmtree(extracted_dir, ignore_errors=True)

    slip_buf = io.BytesIO()
    with zipfile.ZipFile(slip_buf, "w") as zf:
        zf.writestr("../evil.pem", b"pwn")
    with pytest.raises(ImproperConfigurationError, match="Unsafe path in Oracle wallet archive"):
        extract_oracle_wallet_zip(slip_buf.getvalue())


def test_sanitize_oracle_text_query_escapes_reserved_words_and_syntax() -> None:
    sanitized = sanitize_oracle_text_query("NOT (foo & bar) OR NEAR")
    assert sanitized == "{NOT} ACCUM {foo} ACCUM {bar} ACCUM {OR} ACCUM {NEAR}"
    assert sanitize_oracle_text_query("   !@#$%^&*()   ") == ""


def test_duality_view_etag_helpers_extract_inject_and_build_update_sql() -> None:
    doc = {"_metadata": {"etag": "etag-abc-123"}, "id": 42, "name": "Order"}
    assert extract_duality_etag(doc) == "etag-abc-123"
    assert extract_duality_view_etag(doc) == "etag-abc-123"
    assert extract_duality_etag({"id": 42}) is None

    injected = inject_duality_view_etag({"id": 42}, "etag-999")
    assert injected["_metadata"]["etag"] == "etag-999"

    sql = build_duality_etag_update_sql("orders_dv")
    assert "JSON_VALUE(DATA, '$._metadata.etag') = :expected_etag" in sql
