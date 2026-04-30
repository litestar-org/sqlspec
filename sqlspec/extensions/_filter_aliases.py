"""Shared filter alias helpers for framework extensions."""

from collections.abc import Mapping
from typing import NamedTuple

from sqlspec.utils.text import camelize

__all__ = ("SortField", "SortFieldResolution", "resolve_sort_field_aliases")

SortField = str | set[str] | list[str]


class SortFieldResolution(NamedTuple):
    """Resolved sort-field alias metadata.

    Args:
        default_field: Internal SQL-facing field used when no query value is supplied.
        default_query_value: API-facing default value exposed on the query parameter.
        allowed_fields: Internal SQL-facing allowlist.
        inbound_aliases: API-facing query values mapped to internal field names.
        field_display_names: Internal field names mapped to their preferred API-facing display names.
        allowed_display_names: Display names ordered to match the configured sort fields.
    """

    default_field: str
    default_query_value: str
    allowed_fields: frozenset[str]
    inbound_aliases: dict[str, str]
    field_display_names: dict[str, str]
    allowed_display_names: tuple[str, ...]

    def normalize(self, value: str | None) -> str | None:
        """Normalize a query value to an internal field name.

        Args:
            value: API-facing query value, or ``None`` to request the default field.

        Returns:
            The internal field name if the value is configured, otherwise ``None``.
        """
        if value is None:
            return self.default_field
        return self.inbound_aliases.get(value)


def resolve_sort_field_aliases(
    sort_field: SortField, sort_field_aliases: Mapping[str, str] | None = None, sort_field_camelize: bool = True
) -> SortFieldResolution:
    """Resolve sort-field aliases to a closed allowlist map.

    Args:
        sort_field: Configured SQL-facing sort field or fields.
        sort_field_aliases: Optional API-facing alias to SQL-facing field mapping.
        sort_field_camelize: Whether to generate camel-case aliases for configured fields. Defaults to ``True``.

    Returns:
        Precomputed alias metadata for framework filter providers.

    Raises:
        ValueError: If an alias targets an unknown field or collides with a different field.
    """
    fields = _coerce_fields(sort_field)
    allowed_fields = frozenset(fields)
    inbound_aliases: dict[str, str] = {}
    field_display_names = {field: field for field in fields}

    for field in fields:
        _add_alias(inbound_aliases, alias=field, field=field)

    if sort_field_camelize:
        for field in fields:
            alias = camelize(field)
            _add_alias(inbound_aliases, alias=alias, field=field)
            field_display_names[field] = alias

    if sort_field_aliases:
        for alias, field in sort_field_aliases.items():
            if field not in allowed_fields:
                msg = f"sort field alias '{alias}' targets unknown sort field '{field}'"
                raise ValueError(msg)
            _add_alias(inbound_aliases, alias=alias, field=field)
            field_display_names[field] = alias

    allowed_display_names = tuple(field_display_names[field] for field in fields)
    return SortFieldResolution(
        default_field=fields[0],
        default_query_value=field_display_names[fields[0]],
        allowed_fields=allowed_fields,
        inbound_aliases=inbound_aliases,
        field_display_names=field_display_names,
        allowed_display_names=allowed_display_names,
    )


def _coerce_fields(sort_field: SortField) -> tuple[str, ...]:
    if isinstance(sort_field, str):
        return (sort_field,)
    fields = tuple(sorted(sort_field)) if isinstance(sort_field, set) else tuple(sort_field)
    if not fields:
        msg = "sort_field must include at least one field"
        raise ValueError(msg)
    return fields


def _add_alias(inbound_aliases: dict[str, str], *, alias: str, field: str) -> None:
    existing_field = inbound_aliases.get(alias)
    if existing_field is None or existing_field == field:
        inbound_aliases[alias] = field
        return

    msg = f"ambiguous sort field alias '{alias}' maps to both '{existing_field}' and '{field}'"
    raise ValueError(msg)
