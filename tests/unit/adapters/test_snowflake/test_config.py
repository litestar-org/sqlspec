"""Snowflake configuration and authentication normalization tests."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.snowflake._typing import SnowflakeSessionContext
from sqlspec.adapters.snowflake.config import SnowflakeConfig
from sqlspec.adapters.snowflake.core import apply_driver_features, build_statement_config, normalize_connection_params
from sqlspec.core import ParameterStyle
from sqlspec.driver._common import _DETACHED_CONNECTION
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.correlation import CorrelationContext


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


@pytest.mark.parametrize(
    ("options", "expected_tag"),
    [
        ({"query_tag": "sales-report"}, "sales-report"),
        (
            {"query_tag": "ignored", "session_parameters": {"QUERY_TAG": "nested-report", "TIMEZONE": "Asia/Tokyo"}},
            "nested-report",
        ),
        ({"query_tag": "custom:tag/with spaces"}, "custom:tag/with spaces"),
    ],
)
def test_config_preserves_query_tag_when_correlating_cached_statements(
    monkeypatch: pytest.MonkeyPatch, snowflake_connection: Any, options: dict[str, Any], expected_tag: str
) -> None:
    connector = pytest.importorskip("snowflake.connector")
    connected: list[dict[str, Any]] = []

    def connect(**kwargs: Any) -> Any:
        connected.append(kwargs)
        snowflake_connection.config = SimpleNamespace(session_parameters=dict(kwargs.get("session_parameters", {})))
        snowflake_connection.is_closed = lambda: False
        snowflake_connection.close = lambda: None
        return snowflake_connection

    monkeypatch.setattr(connector, "connect", connect)
    snowflake_connection.default_result = {"description": [("ID", 0, None, None, 18, 0, False)], "rows": [(1,)]}
    config = SnowflakeConfig(
        connection_config=options,
        driver_features={"enable_query_tag_correlation": True},
        statement_config=build_statement_config().replace(enable_caching=True),
    )
    try:
        with config.provide_session() as session:
            for correlation_id in ("request-a", "request-b"):
                with CorrelationContext.context(correlation_id):
                    assert session.select("SELECT 1 AS ID") == [{"id": 1}]
            session.select("SELECT 1 AS ID")
        assert session.connection is _DETACHED_CONNECTION
        assert [call[2] for call in snowflake_connection.calls] == [
            {"_statement_params": {"QUERY_TAG": f"{expected_tag}:request-a"}},
            {"_statement_params": {"QUERY_TAG": f"{expected_tag}:request-b"}},
            {},
        ]
        expected_session = dict(options.get("session_parameters", {}))
        expected_session.setdefault("QUERY_TAG", options.get("query_tag"))
        assert connected[0]["session_parameters"] == expected_session
    finally:
        config.close_pool()


@pytest.mark.parametrize("session_source", ["config", "legacy"])
def test_named_profile_query_tag_is_preserved_for_correlation(
    monkeypatch: pytest.MonkeyPatch, snowflake_connection: Any, session_source: str
) -> None:
    connector = pytest.importorskip("snowflake.connector")

    def connect(**kwargs: Any) -> Any:
        assert kwargs["connection_name"] == "reporting"
        if session_source == "config":
            snowflake_connection.config = SimpleNamespace(session_parameters={"QUERY_TAG": "profile-report"})
        else:
            snowflake_connection._session_parameters = {"QUERY_TAG": "profile-report"}
        snowflake_connection.is_closed = lambda: False
        snowflake_connection.close = lambda: None
        return snowflake_connection

    monkeypatch.setattr(connector, "connect", connect)
    snowflake_connection.default_result = {"description": [("ID", 0, None, None, 18, 0, False)], "rows": [(1,)]}
    config = SnowflakeConfig(
        connection_config={"connection_name": "reporting"}, driver_features={"enable_query_tag_correlation": True}
    )
    try:
        with config.provide_session() as session, CorrelationContext.context("request-a"):
            session.select("SELECT 1 AS ID")
        assert session.connection is _DETACHED_CONNECTION
        assert snowflake_connection.calls[-1][2] == {"_statement_params": {"QUERY_TAG": "profile-report:request-a"}}
    finally:
        config.close_pool()


def test_derive_pool_routes_query_tag_to_session_parameters() -> None:
    """A derived pool forwards an overridden query tag to the connector session."""
    derived = SnowflakeConfig(connection_config={"account": "acct", "user": "u"}).derive_pool(query_tag="etl")

    assert derived.connection_config["query_tag"] == "etl"
    assert "query_tag" not in derived.driver_features
    assert normalize_connection_params(derived.connection_config)["session_parameters"]["QUERY_TAG"] == "etl"


@pytest.mark.parametrize("mode", ["clean", "body-error", "prepare-error"])
def test_snowflake_session_context_detaches_driver_after_release(mode: str) -> None:
    """SnowflakeSessionContext releases the live connection first and detaches the driver across exit and enter failures."""
    conn = MagicMock()
    released_while_attached: list[bool] = []
    holder: list[Any] = []

    def prepare(driver: Any) -> Any:
        holder.append(driver)
        if mode == "prepare-error":
            raise RuntimeError("snowflake prepare failed")
        return driver

    def release(released_conn: Any, **_kwargs: Any) -> None:
        assert released_conn is conn
        released_while_attached.append(holder[0].connection is conn)

    ctx = SnowflakeSessionContext(
        acquire_connection=lambda: conn,
        release_connection=release,
        statement_config=build_statement_config(),
        driver_features={},
        prepare_driver=prepare,
    )
    if mode == "prepare-error":
        with pytest.raises(RuntimeError, match="snowflake prepare failed"), ctx:
            pytest.fail("prepare failure must abort __enter__")
    elif mode == "body-error":
        with pytest.raises(ValueError, match="snowflake body failed"), ctx as session:
            raise ValueError("snowflake body failed")
        assert session is holder[0]
    else:
        with ctx as session:
            assert session.connection is conn

    assert released_while_attached == [True]
    assert holder[0].connection is _DETACHED_CONNECTION
