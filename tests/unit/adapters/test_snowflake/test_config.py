"""Snowflake configuration and authentication normalization tests."""

from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.snowflake.config import SnowflakeConfig
from sqlspec.adapters.snowflake.core import apply_driver_features, build_statement_config, normalize_connection_params
from sqlspec.core import ParameterStyle
from sqlspec.exceptions import ImproperConfigurationError


@pytest.mark.parametrize(
    ("auth_type", "expected"),
    [
        ("snowflake", "snowflake"),
        ("jwt", "SNOWFLAKE_JWT"),
        ("keypair", "SNOWFLAKE_JWT"),
        ("pat", "PROGRAMMATIC_ACCESS_TOKEN"),
        ("wif", "WORKLOAD_IDENTITY"),
        ("oauth", "oauth"),
        ("mfa", "username_password_mfa"),
        ("externalbrowser", "externalbrowser"),
    ],
)
def test_authentication_aliases(auth_type: str, expected: str) -> None:
    assert normalize_connection_params({"auth_type": auth_type}) == {"authenticator": expected, "paramstyle": "qmark"}


@pytest.mark.parametrize(
    "options",
    [
        {"auth_type": "unknown"},
        {"auth_type": "okta"},
        {"auth_type": "jwt", "authenticator": "oauth"},
        {"workload_identity_provider": "unknown"},
        {"auth_token_provider": "token"},
        {"auth_token_provider": lambda: ""},
        {"paramstyle": "unknown"},
        {"session_parameters": "QUERY_TAG=x"},
    ],
)
def test_invalid_connection_options(options: dict[str, Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        normalize_connection_params(options)


def test_rotating_workload_identity_tokens_and_session_options() -> None:
    tokens = iter(["token-one", "token-two"])
    source = {
        "username": "service",
        "auth_type": "wif",
        "auth_token_provider": lambda: next(tokens),
        "workload_identity_provider": "gcp",
        "workload_identity_impersonation_path": "svc@example.com",
        "query_tag": "default",
        "timezone": "UTC",
        "session_parameters": {"QUERY_TAG": "explicit"},
        "pool_max_size": 2,
        "extra": {"application": "test", "user": "ignored"},
    }
    first, second = normalize_connection_params(source), normalize_connection_params(source)
    assert first["token"] == "token-one"
    assert second["token"] == "token-two"
    assert first["authenticator"] == "WORKLOAD_IDENTITY"
    assert first["workload_identity_provider"] == "GCP"
    assert first["workload_identity_impersonation_path"] == ["svc@example.com"]
    assert first["session_parameters"] == {"QUERY_TAG": "explicit", "TIMEZONE": "UTC"}
    assert source["session_parameters"] == {"QUERY_TAG": "explicit"}
    assert source["pool_max_size"] == 2


def test_password_token_provider_and_profile_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "profile")
    assert normalize_connection_params({"auth_token_provider": lambda: "secret"}) == {
        "password": "secret",
        "connection_name": "profile",
        "paramstyle": "qmark",
    }


def test_private_key_path_and_der() -> None:
    assert normalize_connection_params({"private_key": Path("/keys/key.p8"), "private_key_passphrase": "secret"}) == {
        "private_key_file": "/keys/key.p8",
        "private_key_file_pwd": "secret",
        "authenticator": "SNOWFLAKE_JWT",
        "paramstyle": "qmark",
    }
    assert normalize_connection_params({"private_key": b"der"})["private_key"] == b"der"


def test_pem_and_private_key_object_produce_identical_der() -> None:
    serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
    rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.BestAvailableEncryption(b"secret")
    ).decode()
    expected = key.private_bytes(
        serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    assert (
        normalize_connection_params({"private_key": pem, "private_key_passphrase": "secret"})["private_key"] == expected
    )
    assert normalize_connection_params({"private_key": key})["private_key"] == expected


@pytest.mark.parametrize(
    ("paramstyle", "expected"),
    [
        ("qmark", ParameterStyle.QMARK),
        ("numeric", ParameterStyle.POSITIONAL_COLON),
        ("pyformat", ParameterStyle.POSITIONAL_PYFORMAT),
        ("format", ParameterStyle.POSITIONAL_PYFORMAT),
    ],
)
def test_binding_mode_profiles(paramstyle: str, expected: ParameterStyle) -> None:
    config = build_statement_config(paramstyle=paramstyle)
    assert config.dialect == "snowflake"
    assert config.parameter_config.default_execution_parameter_style == expected


def test_config_does_not_resolve_rotating_credentials_until_connect() -> None:
    calls: list[str] = []

    def token() -> str:
        calls.append("called")
        return "credential"

    config = SnowflakeConfig(
        connection_config={"account": "org-acct", "auth_type": "pat", "auth_token_provider": token}
    )
    assert calls == []
    assert config.statement_config.dialect == "snowflake"
    assert config.supports_transactional_ddl is False
    assert config.supports_native_arrow_export is True
    assert config.driver_features["enable_lowercase_column_names"] is True


def test_derive_pool_preserves_base_configuration_and_separates_pool() -> None:
    config = SnowflakeConfig(connection_config={"account": "org-acct", "warehouse": "DEFAULT"}, bind_key="primary")
    derived = config.derive_pool(role="ANALYST", warehouse="BI", bind_key="analytics")
    assert derived.connection_config == {"account": "org-acct", "warehouse": "BI", "role": "ANALYST"}
    assert derived.bind_key == "analytics"
    assert derived.connection_instance is None
    assert config.connection_config == {"account": "org-acct", "warehouse": "DEFAULT"}
    assert config.get_event_runtime_hints().poll_interval == 2.0


@pytest.mark.parametrize("threshold", [True, 0, -1, 1.5])
def test_stage_threshold_requires_positive_integer(threshold: object) -> None:
    with pytest.raises(ImproperConfigurationError, match="positive integer"):
        apply_driver_features(None, {"stage_parquet_threshold_rows": threshold})
