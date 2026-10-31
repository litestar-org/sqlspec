# pyright: reportArgumentType=false
"""Unit tests for OracleDB ``apply_driver_features`` defaults."""

import inspect
import io
import os
import shutil
import ssl
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

import sqlspec.adapters.oracledb._typing as oracle_typing
from sqlspec.adapters.oracledb import config, parse_sqlnet_ora, parse_tnsnames_ora
from sqlspec.adapters.oracledb._typing import OracleAsyncCursor
from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleDriverFeatures, OracleSyncConfig
from sqlspec.adapters.oracledb.core import (
    OracleEndpoint,
    OracleTopologyConfig,
    apply_driver_features,
    apply_wallet_ssl_context,
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


def test_sanitize_oracle_text_query_near_and_boolean_operators() -> None:
    """NEAR operator should emit functional NEAR((...)) syntax and reject unknown operators."""
    assert sanitize_oracle_text_query("dark roast espresso", operator="NEAR") == "NEAR(({dark}, {roast}, {espresso}))"
    assert sanitize_oracle_text_query("dark roast", operator="near") == "NEAR(({dark}, {roast}))"
    assert sanitize_oracle_text_query("espresso", operator="NEAR") == "{espresso}"
    assert sanitize_oracle_text_query("   !@#$%^&*()   ", operator="NEAR") == ""
    assert sanitize_oracle_text_query("alpha beta", operator="and") == "{alpha} AND {beta}"
    assert sanitize_oracle_text_query("alpha beta", operator=" OR ") == "{alpha} OR {beta}"
    assert sanitize_oracle_text_query("alpha beta", operator="accum") == "{alpha} ACCUM {beta}"

    with pytest.raises(ImproperConfigurationError, match="Unsupported Oracle Text operator"):
        sanitize_oracle_text_query("alpha beta", operator="WITHIN")

    with pytest.raises(ImproperConfigurationError, match="Unsupported Oracle Text operator"):
        sanitize_oracle_text_query("", operator="INVALID")


def test_duality_view_etag_helpers_extract_inject_and_build_update_sql() -> None:
    doc = {"_metadata": {"etag": "etag-abc-123"}, "id": 42, "name": "Order"}
    assert extract_duality_etag(doc) == "etag-abc-123"
    assert extract_duality_view_etag(doc) == "etag-abc-123"
    assert extract_duality_etag({"id": 42}) is None

    injected = inject_duality_view_etag({"id": 42}, "etag-999")
    assert injected["_metadata"]["etag"] == "etag-999"

    sql = build_duality_etag_update_sql("orders_dv")
    assert "JSON_VALUE(DATA, '$._metadata.etag') = :expected_etag" in sql


def test_extract_oracle_wallet_zip_converts_ewallet_p12_to_pem() -> None:
    """Wallet archives containing only ewallet.p12 should materialize a 0o600 ewallet.pem."""
    p12_bytes = _build_test_p12_bytes(b"WalletSecret123")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("ewallet.p12", p12_bytes)
        zf.writestr("tnsnames.ora", b"mydb_high = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb)(PORT=1522)))")

    extracted_dir = extract_oracle_wallet_zip(buf.getvalue(), wallet_password="WalletSecret123")
    try:
        pem_path = extracted_dir / "ewallet.pem"
        assert pem_path.is_file()
        pem_text = pem_path.read_text(encoding="utf-8")
        assert "-----BEGIN PRIVATE KEY-----" in pem_text
        assert "-----BEGIN CERTIFICATE-----" in pem_text
        assert (pem_path.stat().st_mode & 0o777) == 0o600
        assert (extracted_dir.stat().st_mode & 0o777) == 0o700
    finally:
        shutil.rmtree(extracted_dir, ignore_errors=True)

    with pytest.raises(ImproperConfigurationError, match="ewallet\\.p12"):
        extract_oracle_wallet_zip(buf.getvalue(), wallet_password="WrongPassword")

    with pytest.raises(ImproperConfigurationError, match="ewallet\\.p12"):
        extract_oracle_wallet_zip(buf.getvalue())


def test_extract_oracle_wallet_zip_deterministic_hash_and_directory_input(tmp_path: Path) -> None:
    """Wallet extraction should reuse content-hashed temp dirs and accept pre-extracted directories."""
    p12_unencrypted = _build_test_p12_bytes(None)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("ewallet.p12", p12_unencrypted)
        zf.writestr("tnsnames.ora", b"mydb_tp = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb)(PORT=1522)))")

    first_dir = extract_oracle_wallet_zip(buf.getvalue())
    second_dir = extract_oracle_wallet_zip(buf.getvalue())
    try:
        assert first_dir == second_dir
        assert (first_dir / "ewallet.pem").is_file()
    finally:
        shutil.rmtree(first_dir, ignore_errors=True)

    wallet_dir = tmp_path / "existing_wallet"
    wallet_dir.mkdir()
    (wallet_dir / "ewallet.p12").write_bytes(_build_test_p12_bytes(b"DirSecret"))
    (wallet_dir / "tnsnames.ora").write_text(
        "mydb_low = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb)(PORT=1522)))", encoding="utf-8"
    )

    resolved_dir = extract_oracle_wallet_zip(wallet_dir, wallet_password="DirSecret")
    assert resolved_dir == wallet_dir.resolve()
    assert (wallet_dir / "ewallet.pem").is_file()
    assert ((wallet_dir / "ewallet.pem").stat().st_mode & 0o777) == 0o600

    empty_dir = tmp_path / "empty_wallet"
    empty_dir.mkdir()
    with pytest.raises(ImproperConfigurationError):
        extract_oracle_wallet_zip(empty_dir)


def test_parse_tnsnames_ora_and_sqlnet_ora_multiline_and_comments(tmp_path: Path) -> None:
    """TNS and sqlnet parsers should handle comments, multi-line descriptors, and alias lists."""
    tns_content = (
        "# Autonomous Database TNS aliases\n"
        "mydb_high, mydb_tp =\n"
        "  (DESCRIPTION =\n"
        "    (RETRY_COUNT = 20)\n"
        "    (ADDRESS = (PROTOCOL = TCPS)(PORT = 1522)(HOST = adb.us-ashburn-1.oraclecloud.com))\n"
        "    (CONNECT_DATA = (SERVICE_NAME = g123_mydb_high.adb.oraclecloud.com))\n"
        "    (SECURITY = (SSL_SERVER_DN_MATCH = YES))\n"
        "  )\n"
        "\n"
        "mydb_low = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(PORT=1522)(HOST=adb.oraclecloud.com))"
        "(CONNECT_DATA=(SERVICE_NAME=g123_mydb_low.adb.oraclecloud.com)))\n"
    )
    tns_file = tmp_path / "tnsnames.ora"
    tns_file.write_text(tns_content, encoding="utf-8")

    parsed_from_file = parse_tnsnames_ora(tns_file)
    parsed_from_dir = parse_tnsnames_ora(tmp_path)
    parsed_from_text = parse_tnsnames_ora(tns_content)
    assert parsed_from_file == parsed_from_dir == parsed_from_text
    assert "mydb_high" in parsed_from_file
    assert "mydb_tp" in parsed_from_file
    assert parsed_from_file["mydb_high"] == parsed_from_file["mydb_tp"]
    assert "(SERVICE_NAME = g123_mydb_high.adb.oraclecloud.com)" in parsed_from_file["mydb_high"]
    assert "mydb_low" in parsed_from_file
    assert os.environ.get("TNS_ADMIN") is None or str(tmp_path) != os.environ.get("TNS_ADMIN")

    sqlnet_file = tmp_path / "sqlnet.ora"
    sqlnet_file.write_text(
        '# Wallet settings\nWALLET_LOCATION = (SOURCE = (METHOD = file))\nSSL_SERVER_DN_MATCH = "yes"\n',
        encoding="utf-8",
    )
    sqlnet_settings = parse_sqlnet_ora(sqlnet_file)
    assert sqlnet_settings["SSL_SERVER_DN_MATCH"] == "yes"

    eq_dir = tmp_path / "env=prod"
    eq_dir.mkdir()
    (eq_dir / "tnsnames.ora").write_text(tns_content, encoding="utf-8")
    (eq_dir / "sqlnet.ora").write_text("SSL_SERVER_DN_MATCH = no\n", encoding="utf-8")
    assert parse_tnsnames_ora(str(eq_dir))["mydb_low"] == parsed_from_file["mydb_low"]
    assert parse_tnsnames_ora(str(eq_dir / "tnsnames.ora"))["mydb_tp"] == parsed_from_file["mydb_tp"]
    assert parse_sqlnet_ora(str(eq_dir))["SSL_SERVER_DN_MATCH"] == "no"
    assert parse_sqlnet_ora(str(eq_dir / "sqlnet.ora"))["SSL_SERVER_DN_MATCH"] == "no"


def test_extract_oracle_wallet_zip_rejects_symlinks(tmp_path: Path) -> None:
    """Wallet extraction rejects symlinked target directories and symlinked wallet files."""
    p12_bytes = _build_test_p12_bytes(None)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("ewallet.p12", p12_bytes)
        zf.writestr("tnsnames.ora", b"mydb_tp = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb)(PORT=1522)))")

    real_dir = tmp_path / "real_target"
    real_dir.mkdir()
    symlink_target = tmp_path / "symlink_target"
    symlink_target.symlink_to(real_dir)

    with pytest.raises(ImproperConfigurationError, match="symlink"):
        extract_oracle_wallet_zip(buf.getvalue(), target_dir=symlink_target)

    valid_target = tmp_path / "valid_target"
    valid_target.mkdir(mode=0o700)
    decoy_file = tmp_path / "decoy.pem"
    decoy_file.write_text("ORIGINAL", encoding="utf-8")
    (valid_target / "ewallet.pem").symlink_to(decoy_file)

    with pytest.raises(ImproperConfigurationError, match="symlink"):
        extract_oracle_wallet_zip(buf.getvalue(), target_dir=valid_target)
    assert decoy_file.read_text(encoding="utf-8") == "ORIGINAL"


def _build_test_p12_bytes(password: bytes | None = b"WalletSecret123") -> bytes:
    """Generate an in-memory PKCS#12 wallet payload with a private key and self-signed certificate."""
    private_key = ec.generate_private_key(ec.SECP256R1())
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "adb.oraclecloud.com")])
    now = datetime.now(UTC)
    cert = (
        x509
        .CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .sign(private_key, hashes.SHA256())
    )
    encryption = (
        serialization.BestAvailableEncryption(password) if password is not None else serialization.NoEncryption()
    )
    return pkcs12.serialize_key_and_certificates(
        name=b"oracle-wallet", key=private_key, cert=cert, cas=[cert], encryption_algorithm=encryption
    )


def test_apply_wallet_ssl_context_with_config_dir_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """apply_wallet_ssl_context resolves ewallet.pem via config_dir when wallet_location is missing."""
    p12_bytes = _build_test_p12_bytes(None)
    wallet_dir = tmp_path / "wallet_via_config_dir"
    wallet_dir.mkdir()
    (wallet_dir / "ewallet.p12").write_bytes(p12_bytes)
    extract_oracle_wallet_zip(wallet_dir, target_dir=wallet_dir)
    assert (wallet_dir / "ewallet.pem").is_file()

    fake_ctx = ssl.create_default_context()
    fake_ctx.verify_flags |= ssl.VERIFY_X509_STRICT
    monkeypatch.setattr(ssl, "create_default_context", lambda: fake_ctx)

    conn_config: dict[str, Any] = {"config_dir": str(wallet_dir)}
    apply_wallet_ssl_context(conn_config)
    assert "ssl_context" in conn_config
    assert conn_config["ssl_context"] is not None
    assert not (conn_config["ssl_context"].verify_flags & ssl.VERIFY_X509_STRICT)


def test_apply_wallet_ssl_context_noop_when_no_wallet() -> None:
    """apply_wallet_ssl_context does nothing when neither wallet_location nor config_dir is set."""
    conn_config: dict[str, Any] = {"user": "app", "password": "secret"}
    apply_wallet_ssl_context(conn_config)
    assert "ssl_context" not in conn_config
