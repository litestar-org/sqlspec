"""Tests for environment variable parsing utilities."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from typing_extensions import assert_type

from sqlspec.utils.env import get_config_val, get_config_val_with_aliases, get_env, get_env_with_aliases, is_env_set


def test_get_env_returns_default_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SQLSPEC_TEST_VALUE", raising=False)

    assert get_env("SQLSPEC_TEST_VALUE", 42)() == 42


def test_get_env_parses_override_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_TEST_VALUE", "42")

    assert get_env("SQLSPEC_TEST_VALUE", 1)() == 42


def test_get_env_expands_environment_references(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_ROOT", "/tmp/sqlspec")
    monkeypatch.setenv("SQLSPEC_PATH", "$SQLSPEC_ROOT/data")

    assert get_env("SQLSPEC_PATH", "")() == "/tmp/sqlspec/data"


def test_get_env_with_aliases_prefers_canonical_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_ALIAS_VALUE", "1")
    monkeypatch.setenv("SQLSPEC_CANONICAL_VALUE", "2")

    assert get_env_with_aliases("SQLSPEC_CANONICAL_VALUE", ("SQLSPEC_ALIAS_VALUE",), 0)() == 2


def test_get_env_with_aliases_uses_alias_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SQLSPEC_CANONICAL_VALUE", raising=False)
    monkeypatch.setenv("SQLSPEC_ALIAS_VALUE", "2")

    assert get_env_with_aliases("SQLSPEC_CANONICAL_VALUE", ("SQLSPEC_ALIAS_VALUE",), 0)() == 2


def test_is_env_set_checks_canonical_and_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SQLSPEC_CANONICAL_VALUE", raising=False)
    monkeypatch.delenv("SQLSPEC_ALIAS_VALUE", raising=False)

    assert is_env_set("SQLSPEC_CANONICAL_VALUE", ("SQLSPEC_ALIAS_VALUE",)) is False

    monkeypatch.setenv("SQLSPEC_ALIAS_VALUE", "")

    assert is_env_set("SQLSPEC_CANONICAL_VALUE", ("SQLSPEC_ALIAS_VALUE",)) is True


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Y", "on", "t"])
def test_get_env_parses_true_bool_values(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("SQLSPEC_BOOL_VALUE", value)

    assert get_env("SQLSPEC_BOOL_VALUE", False)() is True


@pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", "N", "off", "f"])
def test_get_env_parses_false_bool_values(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("SQLSPEC_BOOL_VALUE", value)

    assert get_env("SQLSPEC_BOOL_VALUE", True)() is False


def test_get_env_rejects_invalid_bool_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_BOOL_VALUE", "maybe")

    with pytest.raises(ValueError, match="SQLSPEC_BOOL_VALUE"):
        get_env("SQLSPEC_BOOL_VALUE", False)()


def test_get_env_reports_int_parse_errors_with_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_INT_VALUE", "not-int")

    with pytest.raises(ValueError, match="SQLSPEC_INT_VALUE"):
        get_env("SQLSPEC_INT_VALUE", 1)()


def test_get_env_reports_float_parse_errors_with_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_FLOAT_VALUE", "not-float")

    with pytest.raises(ValueError, match="SQLSPEC_FLOAT_VALUE"):
        get_env("SQLSPEC_FLOAT_VALUE", 1.0)()


def test_get_env_parses_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_PATH_VALUE", "/tmp/sqlspec")

    assert get_env("SQLSPEC_PATH_VALUE", Path("."))() == Path("/tmp/sqlspec")


def test_get_env_parses_comma_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_LIST_VALUE", "a, b,c")

    assert get_env("SQLSPEC_LIST_VALUE", [])() == ["a", "b", "c"]


def test_get_env_parses_json_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_LIST_VALUE", "[1, 2, 3]")

    assert get_env("SQLSPEC_LIST_VALUE", [], list[int])() == [1, 2, 3]


def test_get_env_uses_list_type_hint_with_empty_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_LIST_VALUE", "1,2,3")

    assert get_env("SQLSPEC_LIST_VALUE", [], list[int])() == [1, 2, 3]


def test_get_env_uses_list_type_hint_with_none_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_LIST_VALUE", "1,2,3")

    assert get_env("SQLSPEC_LIST_VALUE", None, list[int])() == [1, 2, 3]


def test_get_env_none_default_returns_optional_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_STRING_VALUE", "configured")

    factory = get_env("SQLSPEC_STRING_VALUE", None)
    alias_factory = get_env_with_aliases("SQLSPEC_MISSING_VALUE", ("SQLSPEC_STRING_VALUE",), None)
    config_value = get_config_val("SQLSPEC_STRING_VALUE", None)
    alias_config_value = get_config_val_with_aliases("SQLSPEC_MISSING_VALUE", ("SQLSPEC_STRING_VALUE",), None)

    assert_type(factory, Callable[[], str | None])
    assert_type(alias_factory, Callable[[], str | None])
    assert_type(config_value, str | None)
    assert_type(alias_config_value, str | None)

    assert factory() == "configured"
    assert alias_factory() == "configured"
    assert config_value == "configured"
    assert alias_config_value == "configured"


def test_get_env_parses_json_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_DICT_VALUE", '{"a": 1, "b": "two"}')

    assert get_env("SQLSPEC_DICT_VALUE", {})() == {"a": 1, "b": "two"}


def test_get_env_parses_key_value_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_DICT_VALUE", "a=1,b=two")

    assert get_env("SQLSPEC_DICT_VALUE", {})() == {"a": "1", "b": "two"}


def test_get_env_reports_invalid_list_format_with_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_LIST_VALUE", '{"not": "a list"}')

    with pytest.raises(ValueError, match="SQLSPEC_LIST_VALUE"):
        get_env("SQLSPEC_LIST_VALUE", [], list[int])()


def test_get_env_reports_invalid_dict_format_with_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_DICT_VALUE", "not-a-pair")

    with pytest.raises(ValueError, match="SQLSPEC_DICT_VALUE"):
        get_env("SQLSPEC_DICT_VALUE", {})()


def test_get_config_val_matches_get_env_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_CONFIG_VALUE", "3")

    assert get_config_val("SQLSPEC_CONFIG_VALUE", 1) == 3


def test_get_config_val_with_aliases_uses_canonical_first(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_CONFIG_ALIAS", "1")
    monkeypatch.setenv("SQLSPEC_CONFIG_VALUE", "3")

    assert get_config_val_with_aliases("SQLSPEC_CONFIG_VALUE", ("SQLSPEC_CONFIG_ALIAS",), 1) == 3


def test_get_env_parses_dict_type_hint_with_none_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLSPEC_DICT_VALUE", "a=1")

    assert get_env("SQLSPEC_DICT_VALUE", None, dict[str, Any])() == {"a": "1"}
