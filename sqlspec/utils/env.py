"""Environment variable parsing utilities."""

import json
import os
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Final, Generic, TypeVar, cast, get_args, get_origin, overload

__all__ = (
    "get_config_val",
    "get_config_val_with_aliases",
    "get_env",
    "get_env_with_aliases",
    "is_env_set",
)

TRUE_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes", "y", "on", "t"})
FALSE_VALUES: Final[frozenset[str]] = frozenset({"0", "false", "no", "n", "off", "f"})

T = TypeVar("T")
ParseType = bool | int | float | str | Path | list[Any] | dict[str, Any] | None


class _UnsetType:
    """Sentinel for an omitted type hint."""

    __slots__ = ()


_UNSET = _UnsetType()


class _EnvFactory(Generic[T]):
    """Callable factory for delayed environment parsing."""

    __slots__ = ("_aliases", "_default", "_key", "_type_hint")

    def __init__(self, key: str, aliases: "Sequence[str]", default: T, type_hint: object) -> None:
        self._aliases = aliases
        self._default = default
        self._key = key
        self._type_hint = type_hint

    def __call__(self) -> T:
        if self._aliases:
            return get_config_val_with_aliases(self._key, self._aliases, self._default, self._type_hint)
        return get_config_val(self._key, self._default, self._type_hint)


@overload
def get_env(key: str, default: bool, type_hint: "_UnsetType" = _UNSET) -> "Callable[[], bool]": ...


@overload
def get_env(key: str, default: int, type_hint: "_UnsetType" = _UNSET) -> "Callable[[], int]": ...


@overload
def get_env(key: str, default: float, type_hint: "_UnsetType" = _UNSET) -> "Callable[[], float]": ...


@overload
def get_env(key: str, default: str, type_hint: "_UnsetType" = _UNSET) -> "Callable[[], str]": ...


@overload
def get_env(key: str, default: Path, type_hint: "_UnsetType" = _UNSET) -> "Callable[[], Path]": ...


@overload
def get_env(key: str, default: list[Any], type_hint: "_UnsetType" = _UNSET) -> "Callable[[], list[Any]]": ...


@overload
def get_env(
    key: str, default: dict[str, Any], type_hint: "_UnsetType" = _UNSET
) -> "Callable[[], dict[str, Any]]": ...


@overload
def get_env(key: str, default: None, type_hint: "_UnsetType" = _UNSET) -> "Callable[[], None]": ...


@overload
def get_env(key: str, default: ParseType, type_hint: object) -> "Callable[[], Any]": ...


def get_env(key: str, default: ParseType, type_hint: object = _UNSET) -> "Callable[[], Any]":
    """Return a callable that parses an environment variable on demand.

    Args:
        key: Environment variable name.
        default: Value returned when the variable is unset.
        type_hint: Optional parse target, including generic aliases such as ``list[int]``.

    Returns:
        Callable that returns the parsed value.
    """
    return _EnvFactory(key, (), default, type_hint)


@overload
def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: bool, type_hint: "_UnsetType" = _UNSET
) -> "Callable[[], bool]": ...


@overload
def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: int, type_hint: "_UnsetType" = _UNSET
) -> "Callable[[], int]": ...


@overload
def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: float, type_hint: "_UnsetType" = _UNSET
) -> "Callable[[], float]": ...


@overload
def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: str, type_hint: "_UnsetType" = _UNSET
) -> "Callable[[], str]": ...


@overload
def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: Path, type_hint: "_UnsetType" = _UNSET
) -> "Callable[[], Path]": ...


@overload
def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: list[Any], type_hint: "_UnsetType" = _UNSET
) -> "Callable[[], list[Any]]": ...


@overload
def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: dict[str, Any], type_hint: "_UnsetType" = _UNSET
) -> "Callable[[], dict[str, Any]]": ...


@overload
def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: None, type_hint: "_UnsetType" = _UNSET
) -> "Callable[[], None]": ...


@overload
def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: ParseType, type_hint: object
) -> "Callable[[], Any]": ...


def get_env_with_aliases(
    key: str, aliases: "Sequence[str]", default: ParseType, type_hint: object = _UNSET
) -> "Callable[[], Any]":
    """Return a callable that parses a canonical environment key plus aliases.

    Args:
        key: Canonical environment variable name.
        aliases: Fallback variable names checked in order.
        default: Value returned when none of the variables are set.
        type_hint: Optional parse target, including generic aliases such as ``list[int]``.

    Returns:
        Callable that returns the parsed value.
    """
    return _EnvFactory(key, aliases, default, type_hint)


def get_config_val(key: str, default: T, type_hint: object = _UNSET) -> T:
    """Parse an environment variable value.

    Args:
        key: Environment variable name.
        default: Value returned when the variable is unset.
        type_hint: Optional parse target, including generic aliases such as ``list[int]``.

    Returns:
        Parsed value or the supplied default.
    """
    if key not in os.environ:
        return default
    return _parse_value(key, os.path.expandvars(os.environ[key]), default, type_hint)


def get_config_val_with_aliases(key: str, aliases: "Sequence[str]", default: T, type_hint: object = _UNSET) -> T:
    """Parse a canonical environment variable, falling back to aliases.

    Args:
        key: Canonical environment variable name.
        aliases: Fallback variable names checked in order.
        default: Value returned when none of the variables are set.
        type_hint: Optional parse target, including generic aliases such as ``list[int]``.

    Returns:
        Parsed value or the supplied default.
    """
    for env_key in (key, *aliases):
        if env_key in os.environ:
            return get_config_val(env_key, default, type_hint)
    return default


def is_env_set(key: str, aliases: "Sequence[str]" = ()) -> bool:
    """Return whether a canonical environment variable or alias is present.

    Args:
        key: Canonical environment variable name.
        aliases: Fallback variable names to check.

    Returns:
        ``True`` when any key is present in ``os.environ``.
    """
    return any(env_key in os.environ for env_key in (key, *aliases))


def _coerce_bool(key: str, value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    msg = f"Cannot convert value for key '{key}' to bool: {value!r}"
    raise ValueError(msg)


def _coerce_item(key: str, value: Any, item_type: object) -> Any:
    if item_type is bool:
        return _coerce_bool(key, value)
    if item_type is Path:
        return Path(str(value))
    if item_type is int:
        try:
            return int(value)
        except (TypeError, ValueError) as e:
            msg = f"Cannot convert list item for key '{key}' to int: {value!r}"
            raise ValueError(msg) from e
    if item_type is float:
        try:
            return float(value)
        except (TypeError, ValueError) as e:
            msg = f"Cannot convert list item for key '{key}' to float: {value!r}"
            raise ValueError(msg) from e
    if item_type is str or item_type is _UNSET:
        return str(value)
    if isinstance(item_type, type):
        try:
            return item_type(value)
        except (TypeError, ValueError) as e:
            msg = f"Cannot convert list item for key '{key}': {value!r}"
            raise ValueError(msg) from e
    return value


def _parse_basic_type(key: str, value: str, target_type: object) -> Any:
    if target_type is bool:
        return _coerce_bool(key, value)
    if target_type is int:
        try:
            return int(value)
        except ValueError as e:
            msg = f"Cannot convert value for key '{key}' to int: {value!r}"
            raise ValueError(msg) from e
    if target_type is float:
        try:
            return float(value)
        except ValueError as e:
            msg = f"Cannot convert value for key '{key}' to float: {value!r}"
            raise ValueError(msg) from e
    if target_type is Path:
        return Path(value)
    return value


def _parse_dict(key: str, value: str) -> dict[str, Any]:
    stripped = value.strip()
    if stripped.startswith("{"):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as e:
            msg = f"'{key}' is not valid JSON"
            raise ValueError(msg) from e
        if not isinstance(parsed, dict):
            msg = f"'{key}' is not a valid dict representation"
            raise ValueError(msg)
        return parsed

    result: dict[str, Any] = {}
    for item in value.split(","):
        stripped_item = item.strip()
        if not stripped_item:
            continue
        if "=" not in stripped_item:
            msg = f"'{key}' invalid dict format: missing '=' in {stripped_item!r}"
            raise ValueError(msg)
        item_key, item_value = stripped_item.split("=", 1)
        result[item_key.strip()] = item_value.strip()
    return result


def _parse_list(key: str, value: str, item_type: object) -> list[Any]:
    stripped = value.strip()
    if stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as e:
            msg = f"'{key}' is not valid JSON"
            raise ValueError(msg) from e
        if not isinstance(parsed, list):
            msg = f"'{key}' is not a valid list representation"
            raise ValueError(msg)
        return [_coerce_item(key, item, item_type) for item in parsed]
    if stripped.startswith("{"):
        msg = f"'{key}' is not a valid list representation"
        raise ValueError(msg)
    return [_coerce_item(key, item.strip(), item_type) for item in value.split(",") if item.strip()]


def _resolve_target(default: Any, type_hint: object) -> tuple[object, object]:
    if type_hint is not _UNSET:
        origin = get_origin(type_hint)
        if origin is list:
            args = get_args(type_hint)
            item_type = args[0] if args else str
            return list, item_type
        if origin is dict:
            return dict, _UNSET
        return type_hint, _UNSET
    if isinstance(default, list):
        item_type = type(default[0]) if default else str
        return list, item_type
    if isinstance(default, dict):
        return dict, _UNSET
    if isinstance(default, Path):
        return Path, _UNSET
    if default is None:
        return str, _UNSET
    return type(default), _UNSET


def _parse_value(key: str, value: str, default: T, type_hint: object) -> T:
    target_type, item_type = _resolve_target(default, type_hint)
    if target_type is list:
        return cast("T", _parse_list(key, value, item_type))
    if target_type is dict:
        return cast("T", _parse_dict(key, value))
    return cast("T", _parse_basic_type(key, value, target_type))
