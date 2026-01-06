"""Parameter alignment and validation helpers."""

from collections.abc import Mapping, Sequence
from typing import Any, cast

import sqlspec.exceptions
from sqlspec.core.parameters._types import ParameterProfile, ParameterStyle

__all__ = (
    "EXECUTE_MANY_MIN_ROWS",
    "collect_null_parameter_ordinals",
    "looks_like_execute_many",
    "normalize_parameter_key",
    "validate_parameter_alignment",
)

EXECUTE_MANY_MIN_ROWS: int = 2


def _is_sequence_like(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def normalize_parameter_key(key: Any) -> "tuple[str, int | str]":
    """Normalize a parameter key into an ``(kind, value)`` tuple.

    Args:
        key: Key supplied by the caller (index, name, or adapter-specific token).

    Returns:
        Tuple identifying the key type and canonical value for alignment checks.
    """
    if isinstance(key, str):
        stripped_numeric = key.lstrip("$")
        if stripped_numeric.isdigit():
            return ("index", int(stripped_numeric) - 1)
        if key.isdigit():
            return ("index", int(key) - 1)
        return ("named", key)
    if isinstance(key, int):
        if key > 0:
            return ("index", key - 1)
        return ("index", key)
    return ("named", str(key))


def looks_like_execute_many(parameters: Any) -> bool:
    """Return ``True`` when the payload resembles an ``execute_many`` structure.

    Args:
        parameters: Potential parameter payload to inspect.

    Returns:
        ``True`` if the payload appears to be a sequence of parameter sets.
    """
    if not _is_sequence_like(parameters) or len(parameters) < EXECUTE_MANY_MIN_ROWS:
        return False
    return all(_is_sequence_like(entry) or isinstance(entry, Mapping) for entry in parameters)


def collect_null_parameter_ordinals(parameters: Any, profile: "ParameterProfile") -> "set[int]":
    """Identify placeholder ordinals whose provided values are ``None``.

    Args:
        parameters: Parameter payload supplied by the caller.
        profile: Metadata describing detected placeholders.

    Returns:
        Set of ordinal indices corresponding to ``None`` values.
    """
    if parameters is None:
        return set()

    null_positions: set[int] = set()

    if isinstance(parameters, Mapping):
        name_lookup: dict[str, int] = {}
        for parameter in profile.parameters:
            if parameter.name:
                name_lookup[parameter.name] = parameter.ordinal
                stripped_name = parameter.name.lstrip("@")
                name_lookup.setdefault(stripped_name, parameter.ordinal)
                name_lookup.setdefault(f"@{stripped_name}", parameter.ordinal)

        for key, value in parameters.items():
            if value is not None:
                continue
            key_kind, normalized_key = normalize_parameter_key(key)
            if key_kind == "index" and isinstance(normalized_key, int):
                null_positions.add(normalized_key)
                continue
            if key_kind == "named":
                ordinal = name_lookup.get(str(normalized_key))
                if ordinal is not None:
                    null_positions.add(ordinal)
        return null_positions

    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        for index, value in enumerate(parameters):
            if value is None:
                null_positions.add(index)
        return null_positions

    return null_positions


def _collect_expected_identifiers(parameter_profile: "ParameterProfile") -> "set[tuple[str, int | str]]":
    identifiers: set[tuple[str, int | str]] = set()
    for parameter in parameter_profile.parameters:
        style = parameter.style
        name = parameter.name
        if style in {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.NAMED_PYFORMAT,
        }:
            identifiers.add(("named", name or f"param_{parameter.ordinal}"))
        elif style in {ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_COLON}:
            if name and name.isdigit():
                identifiers.add(("index", int(name) - 1))
            else:
                identifiers.add(("index", parameter.ordinal))
        else:
            identifiers.add(("index", parameter.ordinal))
    return identifiers


def _collect_actual_identifiers(parameters: Any) -> "tuple[set[tuple[str, int | str]], int]":
    if parameters is None:
        return set(), 0
    if isinstance(parameters, Mapping):
        mapping_identifiers = {normalize_parameter_key(key) for key in parameters}
        return mapping_identifiers, len(parameters)
    if looks_like_execute_many(parameters):
        aggregated_identifiers: set[tuple[str, int | str]] = set()
        for entry in parameters:
            entry_identifiers, _ = _collect_actual_identifiers(entry)
            aggregated_identifiers.update(entry_identifiers)
        return aggregated_identifiers, len(aggregated_identifiers)
    if _is_sequence_like(parameters):
        identifiers = {("index", cast("int | str", index)) for index in range(len(parameters))}
        return identifiers, len(parameters)
    identifiers = {("index", cast("int | str", 0))}
    return identifiers, 1


def _format_identifiers(identifiers: "set[tuple[str, int | str]]") -> str:
    if not identifiers:
        return "[]"
    formatted: list[str] = []
    for identifier in sorted(identifiers, key=_identifier_sort_key):
        kind, value = identifier
        if kind == "named":
            formatted.append(str(value))
        elif isinstance(value, int):
            formatted.append(str(value + 1))
        else:
            formatted.append(str(value))
    return "[" + ", ".join(formatted) + "]"


def _identifier_sort_key(item: "tuple[str, int | str]") -> "tuple[str, str]":
    return item[0], str(item[1])


def _normalize_index_identifiers(expected: "set[tuple[str, int | str]]", actual: "set[tuple[str, int | str]]") -> bool:
    """Allow positional payloads to satisfy generated param_N identifiers."""

    if not expected or not actual:
        return False

    expected_named = {value for kind, value in expected if kind == "named"}
    actual_indexes = {value for kind, value in actual if kind == "index"}

    if not expected_named or not actual_indexes:
        return False

    normalized_expected: set[int] = set()
    for name in expected_named:
        if not isinstance(name, str) or not name.startswith("param_"):
            return False
        suffix = name[6:]
        if not suffix.isdigit():
            return False
        normalized_expected.add(int(suffix))

    if not normalized_expected:
        return False

    if not all(isinstance(index, int) for index in actual_indexes):
        return False

    normalized_actual = {int(index) for index in actual_indexes}
    return normalized_actual == normalized_expected


def _validate_single_parameter_set(
    parameter_profile: "ParameterProfile", parameters: Any, batch_index: "int | None" = None
) -> None:
    expected_identifiers = _collect_expected_identifiers(parameter_profile)
    actual_identifiers, actual_count = _collect_actual_identifiers(parameters)
    expected_count = len(expected_identifiers)

    if expected_count == 0 and actual_count == 0:
        return

    prefix = "Parameter count mismatch"
    if batch_index is not None:
        prefix = f"{prefix} in batch {batch_index}"

    if expected_count == 0 and actual_count > 0:
        msg = f"{prefix}: statement does not accept parameters."
        raise sqlspec.exceptions.SQLSpecError(msg)

    if expected_count > 0 and actual_count == 0:
        msg = f"{prefix}: expected {expected_count} parameters, received 0."
        raise sqlspec.exceptions.SQLSpecError(msg)

    if expected_count != actual_count:
        msg = f"{prefix}: {actual_count} parameters provided but {expected_count} placeholders detected."
        raise sqlspec.exceptions.SQLSpecError(msg)

    identifiers_match = expected_identifiers == actual_identifiers or _normalize_index_identifiers(
        expected_identifiers, actual_identifiers
    )

    if not identifiers_match:
        msg = (
            f"{prefix}: expected identifiers {_format_identifiers(expected_identifiers)}, "
            f"received {_format_identifiers(actual_identifiers)}."
        )
        raise sqlspec.exceptions.SQLSpecError(msg)


def validate_parameter_alignment(
    parameter_profile: "ParameterProfile | None", parameters: Any, *, is_many: bool = False
) -> None:
    """Ensure provided parameters align with detected placeholders.

    Args:
        parameter_profile: Placeholder metadata extracted from the statement.
        parameters: Parameter payload the adapter will execute with.
        is_many: Whether the call explicitly targets ``execute_many``.

    Raises:
        SQLSpecError: If counts or identifiers differ between placeholders and payload.
    """
    profile = parameter_profile or ParameterProfile.empty()
    if profile.total_count == 0:
        return

    effective_is_many = is_many or looks_like_execute_many(parameters)

    if effective_is_many:
        if parameters is None:
            if profile.total_count == 0:
                return
            msg = "Parameter count mismatch: expected parameter sets for execute_many."
            raise sqlspec.exceptions.SQLSpecError(msg)
        if not _is_sequence_like(parameters):
            msg = "Parameter count mismatch: expected sequence of parameter sets for execute_many."
            raise sqlspec.exceptions.SQLSpecError(msg)
        if len(parameters) == 0:
            return
        for index, param_set in enumerate(parameters):
            _validate_single_parameter_set(profile, param_set, batch_index=index)
        return

    _validate_single_parameter_set(profile, parameters)
