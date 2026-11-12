"""Snowflake option precedence and connection normalization boundaries."""

from pathlib import Path

import pytest

from sqlspec.adapters.snowflake.core import normalize_connection_params
from sqlspec.exceptions import ImproperConfigurationError


def test_explicit_username_and_session_settings_override_extra() -> None:
    options = {
        "username": "explicit",
        "query_tag": "top-level",
        "session_parameters": {"QUERY_TAG": "session"},
        "extra": {"user": "extra", "query_tag": "extra", "network_timeout": 12},
    }
    normalized = normalize_connection_params(options)
    assert normalized == {
        "user": "explicit",
        "network_timeout": 12,
        "session_parameters": {"QUERY_TAG": "session"},
        "paramstyle": "qmark",
    }
    assert options["username"] == "explicit"


def test_okta_url_and_profile_paths() -> None:
    normalized = normalize_connection_params({
        "auth_type": "okta",
        "authenticator": "https://acme.okta.com",
        "connections_file_path": Path("profiles.toml"),
        "token_file_path": Path("token.txt"),
    })
    assert normalized == {
        "authenticator": "https://acme.okta.com",
        "connections_file_path": "profiles.toml",
        "token_file_path": "token.txt",
        "paramstyle": "qmark",
    }


def test_okta_rejects_untrusted_authenticator_host() -> None:
    with pytest.raises(ImproperConfigurationError):
        normalize_connection_params({"auth_type": "okta", "authenticator": "https://acme.example.com"})


def test_file_passphrase_preserves_non_utf8_bytes() -> None:
    normalized = normalize_connection_params({"private_key_file": "key.p8", "private_key_passphrase": b"\xff"})
    assert normalized["private_key_file_pwd"] == b"\xff"


def test_explicit_account_suppresses_default_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "default-profile")
    assert normalize_connection_params({"account": "org-acct"}) == {"account": "org-acct", "paramstyle": "qmark"}
