"""Application dependency providers for FastAPI filter injection.

This module provides filter dependency injection for FastAPI routes, allowing
automatic parsing of query parameters into SQLSpec filter objects.
"""

import datetime
import inspect
import typing
from collections.abc import Callable, Mapping
from enum import Enum
from functools import partial
from inspect import isclass
from types import GenericAlias
from typing import Annotated, Any, Literal, NamedTuple, cast
from uuid import UUID

from fastapi import Depends, Query
from fastapi.exceptions import RequestValidationError
from typing_extensions import NotRequired, TypedDict

from sqlspec.core import (
    BeforeAfterFilter,
    BooleanFilter,
    ChoicesFilter,
    FilterTypes,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    NotNullFilter,
    NullFilter,
    OrderByFilter,
    SearchFilter,
)
from sqlspec.utils.text import camelize

__all__ = (
    "DEPENDENCY_DEFAULTS",
    "BooleanOrNone",
    "ChoiceField",
    "DTorNone",
    "DependencyDefaults",
    "FieldNameType",
    "FilterConfig",
    "HashableType",
    "HashableValue",
    "IntOrNone",
    "SortField",
    "SortOrder",
    "SortOrderOrNone",
    "StringOrNone",
    "UuidOrNone",
    "dep_cache",
    "normalize_choice_field_types",
    "provide_filters",
)

DTorNone = datetime.datetime | None
StringOrNone = str | None
UuidOrNone = UUID | None
IntOrNone = int | None
BooleanOrNone = bool | None
SortOrder = Literal["asc", "desc"]
SortOrderOrNone = SortOrder | None
SortField = str | set[str] | list[str]
HashableValue = str | int | float | bool | None
HashableType = HashableValue | tuple[Any, ...] | tuple[tuple[str, Any], ...] | tuple[HashableValue, ...]
_FILTER_CONFIG_KEYS = frozenset({
    "id_filter",
    "created_at",
    "updated_at",
    "pagination_type",
    "search",
    "sort_field",
    "not_in_fields",
    "in_fields",
    "null_fields",
    "not_null_fields",
    "boolean_fields",
    "choice_fields",
})


class DependencyDefaults:
    """Default values for dependency generation."""

    CREATED_FILTER_DEPENDENCY_KEY: str = "created_filter"
    ID_FILTER_DEPENDENCY_KEY: str = "id_filter"
    LIMIT_OFFSET_FILTER_DEPENDENCY_KEY: str = "limit_offset_filter"
    UPDATED_FILTER_DEPENDENCY_KEY: str = "updated_filter"
    ORDER_BY_FILTER_DEPENDENCY_KEY: str = "order_by_filter"
    SEARCH_FILTER_DEPENDENCY_KEY: str = "search_filter"
    DEFAULT_PAGINATION_SIZE: int = 20


DEPENDENCY_DEFAULTS = DependencyDefaults()


class FieldNameType(NamedTuple):
    """Type for field name and associated type information for filter configuration."""

    name: str
    """Name of the field to filter on."""
    type_hint: type[Any] = str
    """Type of the filter value. Defaults to str."""


class ChoiceField:
    """Type for choice field name and allowed choices for filter configuration."""

    __slots__ = ("choices", "name")

    def __init__(self, name: str, choices: list[Any] | tuple[Any, ...] | type[Enum]) -> None:
        self.name = name
        self.choices = choices


def normalize_choice_field_types(choices: list[Any] | tuple[Any, ...] | type[Enum]) -> Any:
    """Normalize choices into a generic type hint (Literal or Enum)."""
    if isclass(choices) and issubclass(choices, Enum):
        return choices
    return cast("Any", typing.Literal).__getitem__(tuple(choices))


class _SortFieldResolution(NamedTuple):
    default_field: str
    default_query_value: str
    allowed_fields: frozenset[str]
    inbound_aliases: dict[str, str]
    field_display_names: dict[str, str]
    allowed_display_names: tuple[str, ...]

    def normalize(self, value: str | None) -> str | None:
        if value is None:
            return self.default_field
        return self.inbound_aliases.get(value)


# Keep FilterConfig field unions and provider signatures in sync with sqlspec.extensions.litestar.providers.
class FilterConfig(TypedDict):
    """Configuration for generated FastAPI filter dependencies.

    All keys are optional. A filter dependency is created only for each enabled
    key. Field names are SQL-facing allowlist values; generated query parameter
    names and order-by aliases remain API-facing.
    """

    id_filter: NotRequired[type[UUID | int | str]]
    """Type of ID filter to enable. When set, creates an ``ids`` collection filter."""
    id_field: NotRequired[str]
    """SQL-facing field name for ID filtering. Defaults to ``"id"``."""
    sort_field: NotRequired[SortField]
    """Allowed SQL-facing field or fields for ``orderBy`` sorting."""
    sort_field_aliases: NotRequired[dict[str, str]]
    """Additional API-facing ``orderBy`` aliases mapped to configured ``sort_field`` values."""
    sort_field_camelize: NotRequired[bool]
    """Whether to accept camel-case aliases for configured sort fields. Defaults to ``True``."""
    sort_order: NotRequired[SortOrder]
    """Default sort order. Defaults to ``"desc"``."""
    pagination_type: NotRequired[Literal["limit_offset"]]
    """Pagination strategy to enable. Currently supports ``"limit_offset"``."""
    pagination_size: NotRequired[int]
    """Default page size for limit/offset pagination."""
    search: NotRequired[str | set[str] | list[str]]
    """SQL-facing field or fields to search. Strings may be comma-separated."""
    search_ignore_case: NotRequired[bool]
    """Whether search filtering is case-insensitive. Defaults to ``False``."""
    created_at: NotRequired[bool]
    """Whether to enable ``created_at`` before/after range filtering."""
    updated_at: NotRequired[bool]
    """Whether to enable ``updated_at`` before/after range filtering."""
    not_in_fields: NotRequired[FieldNameType | set[FieldNameType] | list[str | FieldNameType]]
    """Field or fields that support ``NOT IN`` collection filtering."""
    in_fields: NotRequired[FieldNameType | set[FieldNameType] | list[str | FieldNameType]]
    """Field or fields that support ``IN`` collection filtering."""
    null_fields: NotRequired[str | set[str] | list[str]]
    """Field or fields that support ``IS NULL`` filtering."""
    not_null_fields: NotRequired[str | set[str] | list[str]]
    """Field or fields that support ``IS NOT NULL`` filtering."""
    boolean_fields: NotRequired[str | set[str] | list[str]]
    """Field or fields that support boolean filtering."""
    choice_fields: NotRequired[ChoiceField | set[ChoiceField] | list[str | ChoiceField]]
    """Field or fields that support choices filtering."""


class DependencyCache:
    """Simple dependency cache to memoize dynamically generated dependencies."""

    def __init__(self) -> None:
        self.dependencies: dict[int, Callable[..., list[FilterTypes]]] = {}

    def add_dependencies(self, key: int, dependencies: "Callable[..., list[FilterTypes]]") -> None:
        """Add dependencies to cache.

        Args:
            key: Cache key (hash of config).
            dependencies: Dependency callable to cache.
        """
        self.dependencies[key] = dependencies

    def get_dependencies(self, key: int) -> "Callable[..., list[FilterTypes]] | None":
        """Get dependencies from cache.

        Args:
            key: Cache key (hash of config).

        Returns:
            Cached dependency callable or None if not found.
        """
        return self.dependencies.get(key)


dep_cache = DependencyCache()


def provide_filters(
    config: FilterConfig, dep_defaults: DependencyDefaults = DEPENDENCY_DEFAULTS
) -> "Callable[..., list[FilterTypes]]":
    """Create FastAPI dependency provider for filters based on configuration.

    This function dynamically generates a FastAPI dependency function that parses
    query parameters into SQLSpec filter objects.

    Args:
        config: Filter configuration specifying which filters to enable.
        dep_defaults: Dependency defaults for filter configuration.

    Returns:
        A FastAPI dependency callable that returns list of filters.
    """
    if not _has_filter_config(config):
        return _empty_filter_list

    cache_key = hash(_make_hashable(config))

    cached_dep = dep_cache.get_dependencies(cache_key)
    if cached_dep is not None:
        return cached_dep

    dep = _configured_filter_aggregator(config, dep_defaults)
    dep_cache.add_dependencies(cache_key, dep)
    return dep


def _configured_filter_aggregator(
    config: FilterConfig, dep_defaults: DependencyDefaults = DEPENDENCY_DEFAULTS
) -> "Callable[..., list[FilterTypes]]":
    """Create a FastAPI dependency function that aggregates multiple filter dependencies.

    Args:
        config: Filter configuration.
        dep_defaults: Dependency defaults.

    Returns:
        A FastAPI dependency function that aggregates multiple filter dependencies.
    """
    params: list[inspect.Parameter] = []
    annotations: dict[str, Any] = {}

    if (id_type := config.get("id_filter", False)) is not False:
        _add_dependency(
            params,
            annotations,
            dep_defaults.ID_FILTER_DEPENDENCY_KEY,
            _IdFilterProvider(config.get("id_field", "id"), id_type if isinstance(id_type, type) else object),
        )

    if config.get("created_at", False):
        _add_dependency(
            params,
            annotations,
            dep_defaults.CREATED_FILTER_DEPENDENCY_KEY,
            _BeforeAfterFilterProvider("created_at", "createdBefore", "createdAfter"),
        )

    if config.get("updated_at", False):
        _add_dependency(
            params,
            annotations,
            dep_defaults.UPDATED_FILTER_DEPENDENCY_KEY,
            _BeforeAfterFilterProvider("updated_at", "updatedBefore", "updatedAfter"),
        )

    if config.get("pagination_type") == "limit_offset":
        _add_dependency(
            params,
            annotations,
            dep_defaults.LIMIT_OFFSET_FILTER_DEPENDENCY_KEY,
            _LimitOffsetFilterProvider(config.get("pagination_size", dep_defaults.DEFAULT_PAGINATION_SIZE)),
        )

    if search_fields := config.get("search"):
        _add_dependency(
            params,
            annotations,
            dep_defaults.SEARCH_FILTER_DEPENDENCY_KEY,
            _SearchFilterProvider(search_fields, config.get("search_ignore_case", False)),
        )

    if sort_field := config.get("sort_field"):
        _add_dependency(
            params, annotations, dep_defaults.ORDER_BY_FILTER_DEPENDENCY_KEY, _OrderByProvider(sort_field, config)
        )

    if not_in_fields := config.get("not_in_fields"):
        not_in_fields = {not_in_fields} if isinstance(not_in_fields, (str, FieldNameType)) else not_in_fields
        for field_def in not_in_fields:
            resolved_field: FieldNameType = (
                FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            )

            param_name = f"{resolved_field.name}_not_in_filter"
            _add_dependency(params, annotations, param_name, _CollectionFilterProvider(resolved_field, negated=True))

    if in_fields := config.get("in_fields"):
        in_fields = {in_fields} if isinstance(in_fields, (str, FieldNameType)) else in_fields
        for field_def in in_fields:
            resolved_field = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def

            param_name = f"{resolved_field.name}_in_filter"
            _add_dependency(params, annotations, param_name, _CollectionFilterProvider(resolved_field, negated=False))

    if null_fields := config.get("null_fields"):
        null_fields = {null_fields} if isinstance(null_fields, str) else null_fields
        for field_name in null_fields:
            param_name = f"{field_name}_null_filter"
            _add_dependency(params, annotations, param_name, _NullFilterProvider(field_name, negated=False))

    if not_null_fields := config.get("not_null_fields"):
        not_null_fields = {not_null_fields} if isinstance(not_null_fields, str) else not_null_fields
        for field_name in not_null_fields:
            param_name = f"{field_name}_not_null_filter"
            _add_dependency(params, annotations, param_name, _NullFilterProvider(field_name, negated=True))

    if boolean_fields := config.get("boolean_fields"):
        boolean_fields = {boolean_fields} if isinstance(boolean_fields, str) else boolean_fields
        for field_name in boolean_fields:
            param_name = f"{field_name}_boolean_filter"
            _add_dependency(params, annotations, param_name, _BooleanFilterProvider(field_name))

    if choice_fields := config.get("choice_fields"):
        choice_fields = {choice_fields} if isinstance(choice_fields, ChoiceField) else choice_fields
        for choice_def in choice_fields:
            resolved_choice = ChoiceField(name=choice_def, choices=[]) if isinstance(choice_def, str) else choice_def
            param_name = f"{resolved_choice.name}_choices_filter"
            _add_dependency(
                params, annotations, param_name, _ChoicesFilterProvider(resolved_choice.name, resolved_choice.choices)
            )

    return _make_aggregate_filter_provider(params, annotations)


def _empty_filter_list() -> "list[FilterTypes]":
    return []


def _make_hashable(value: Any) -> HashableType:
    """Convert a value into a hashable type for caching purposes.

    Args:
        value: Any value that needs to be made hashable.

    Returns:
        A hashable version of the value.
    """
    if isinstance(value, dict):
        items = []
        for k in sorted(value.keys()):
            v = value[k]
            items.append((str(k), _make_hashable(v)))
        return tuple(items)
    if isinstance(value, (list, set)):
        hashable_items = [_make_hashable(item) for item in value]
        filtered_items = [item for item in hashable_items if item is not None]
        return tuple(sorted(filtered_items, key=str))
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    return str(value)


def _has_filter_config(config: FilterConfig) -> bool:
    for key in _FILTER_CONFIG_KEYS:
        value = config.get(key)
        if value is not None and value is not False and value != []:
            return True
    return False


def _collection_value_annotation(collection_type: type[Any], value_type: type[Any]) -> Any:
    return GenericAlias(collection_type, (value_type,)) | None


def _query_parameter_annotation(value_annotation: Any, query: Any) -> Any:
    return Annotated[value_annotation, query]


def _set_provider_metadata(
    provider: Any, signature: inspect.Signature, annotations: dict[str, Any]
) -> Callable[..., Any]:
    provider.__signature__ = signature
    provider.__annotations__ = annotations
    return cast("Callable[..., Any]", provider)


def _aggregate_filter_provider(**kwargs: Any) -> list[FilterTypes]:
    filters: list[FilterTypes] = []
    for filter_value in kwargs.values():
        if filter_value is None:
            continue
        if isinstance(filter_value, list):
            filters.extend(filter_value)
        elif (isinstance(filter_value, SearchFilter) and filter_value.value is None) or (
            isinstance(filter_value, OrderByFilter) and filter_value.field_name is None
        ):
            continue
        else:
            filters.append(filter_value)
    return filters


def _make_aggregate_filter_provider(
    parameters: list[inspect.Parameter], annotations: dict[str, Any]
) -> Callable[..., list[FilterTypes]]:
    aggregate_annotations = dict(annotations)
    aggregate_annotations["return"] = list[FilterTypes]
    return _set_provider_metadata(
        partial(_aggregate_filter_provider),
        inspect.Signature(parameters=parameters, return_annotation=list[FilterTypes]),
        aggregate_annotations,
    )


class _IdFilterProvider:
    def __init__(self, field_name: str, id_type: type[Any]) -> None:
        self.field_name = field_name
        self.return_annotation = InCollectionFilter[id_type] | None  # type: ignore[valid-type]
        ids_parameter_annotation = _query_parameter_annotation(
            _collection_value_annotation(list, id_type), Query(alias="ids", description="IDs to filter by.")
        )
        self.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "ids", kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=ids_parameter_annotation
                )
            ],
            return_annotation=self.return_annotation,
        )

    def __call__(self, ids: list[Any] | None = None) -> InCollectionFilter[Any] | None:
        return InCollectionFilter(field_name=self.field_name, values=ids) if ids else None


class _BeforeAfterFilterProvider:
    def __init__(self, field_name: str, before_alias: str, after_alias: str) -> None:
        self.field_name = field_name
        self.before_alias = before_alias
        self.after_alias = after_alias
        self.before_param = f"{field_name}_before"
        self.after_param = f"{field_name}_after"
        self.return_annotation = BeforeAfterFilter | None
        self.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.before_param,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=None,
                    annotation=Annotated[
                        str | None,
                        Query(
                            alias=before_alias,
                            description=f"Filter by {field_name} before this timestamp.",
                            json_schema_extra={"format": "date-time"},
                        ),
                    ],
                ),
                inspect.Parameter(
                    self.after_param,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=None,
                    annotation=Annotated[
                        str | None,
                        Query(
                            alias=after_alias,
                            description=f"Filter by {field_name} after this timestamp.",
                            json_schema_extra={"format": "date-time"},
                        ),
                    ],
                ),
            ],
            return_annotation=self.return_annotation,
        )

    def __call__(self, **kwargs: Any) -> BeforeAfterFilter | None:
        before_dt = self._parse_datetime(kwargs.get(self.before_param), self.before_alias)
        after_dt = self._parse_datetime(kwargs.get(self.after_param), self.after_alias)
        if before_dt or after_dt:
            return BeforeAfterFilter(field_name=self.field_name, before=before_dt, after=after_dt)
        return None

    @staticmethod
    def _parse_datetime(value: Any, alias: str) -> datetime.datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value
        try:
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError, AttributeError):
            msg = f"Invalid date format for {alias}"
            raise RequestValidationError(errors=[{"loc": ("query", alias), "msg": msg, "type": "value_error.datetime"}])


class _LimitOffsetFilterProvider:
    def __init__(self, default_page_size: int) -> None:
        self.default_page_size = default_page_size
        self.return_annotation = LimitOffsetFilter
        self.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "current_page",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=1,
                    annotation=Annotated[
                        int, Query(ge=1, alias="currentPage", description="Page number for pagination.")
                    ],
                ),
                inspect.Parameter(
                    "page_size",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=default_page_size,
                    annotation=Annotated[int, Query(ge=1, alias="pageSize", description="Number of items per page.")],
                ),
            ],
            return_annotation=self.return_annotation,
        )

    def __call__(self, current_page: int = 1, page_size: int | None = None) -> LimitOffsetFilter:
        resolved_page_size = page_size if page_size is not None else self.default_page_size
        return LimitOffsetFilter(limit=resolved_page_size, offset=resolved_page_size * (current_page - 1))


class _SearchFilterProvider:
    def __init__(self, search_fields: str | set[str] | list[str], ignore_case_default: bool) -> None:
        self.search_fields = search_fields
        self.ignore_case_default = ignore_case_default
        self.return_annotation = SearchFilter | None
        self.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "search_string",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=None,
                    annotation=Annotated[str | None, Query(alias="searchString", description="Search term.")],
                ),
                inspect.Parameter(
                    "ignore_case",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=ignore_case_default,
                    annotation=Annotated[
                        bool | None,
                        Query(alias="searchIgnoreCase", description="Whether search should be case-insensitive."),
                    ],
                ),
            ],
            return_annotation=self.return_annotation,
        )

    def __call__(self, search_string: str | None = None, ignore_case: bool | None = None) -> SearchFilter | None:
        field_names: set[Any] = (
            set(self.search_fields.split(",")) if isinstance(self.search_fields, str) else set(self.search_fields)
        )
        if search_string:
            return SearchFilter(
                field_name=field_names,
                value=search_string,
                ignore_case=self.ignore_case_default if ignore_case is None else ignore_case,
            )
        return None


class _OrderByProvider:
    def __init__(self, sort_field: SortField, config: FilterConfig) -> None:
        self.sort_resolution = _resolve_sort_field_aliases(
            sort_field,
            sort_field_aliases=config.get("sort_field_aliases"),
            sort_field_camelize=config.get("sort_field_camelize", True),
        )
        self.sort_order_default: SortOrder = config.get("sort_order", "desc")
        self.allowed_field_names = ", ".join(self.sort_resolution.allowed_display_names)
        self.return_annotation = OrderByFilter
        self.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "field_name",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=self.sort_resolution.default_query_value,
                    annotation=Annotated[str, Query(alias="orderBy", description="Field to order by.")],
                ),
                inspect.Parameter(
                    "sort_order",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=self.sort_order_default,
                    annotation=Annotated[
                        SortOrder | None, Query(alias="sortOrder", description="Sort order ('asc' or 'desc').")
                    ],
                ),
            ],
            return_annotation=self.return_annotation,
        )

    def __call__(self, field_name: str | None = None, sort_order: SortOrder | None = None) -> OrderByFilter:
        query_value = field_name or self.sort_resolution.default_query_value
        resolved_field = self.sort_resolution.normalize(query_value)
        if resolved_field is None:
            msg = f"Invalid orderBy field '{query_value}'. Allowed fields: {self.allowed_field_names}"
            raise RequestValidationError(errors=[{"loc": ("query", "orderBy"), "msg": msg, "type": "value_error"}])
        return OrderByFilter(field_name=resolved_field, sort_order=sort_order or self.sort_order_default)


def _resolve_sort_field_aliases(
    sort_field: SortField, sort_field_aliases: Mapping[str, str] | None = None, sort_field_camelize: bool = True
) -> _SortFieldResolution:
    fields = _coerce_sort_fields(sort_field)
    allowed_fields = frozenset(fields)
    inbound_aliases: dict[str, str] = {}
    field_display_names = {field: field for field in fields}

    for field in fields:
        _add_sort_field_alias(inbound_aliases, alias=field, field=field)

    if sort_field_camelize:
        for field in fields:
            alias = camelize(field)
            _add_sort_field_alias(inbound_aliases, alias=alias, field=field)
            field_display_names[field] = alias

    if sort_field_aliases:
        for alias, field in sort_field_aliases.items():
            if field not in allowed_fields:
                msg = f"sort field alias '{alias}' targets unknown sort field '{field}'"
                raise ValueError(msg)
            _add_sort_field_alias(inbound_aliases, alias=alias, field=field)
            field_display_names[field] = alias

    allowed_display_names = tuple(field_display_names[field] for field in fields)
    return _SortFieldResolution(
        default_field=fields[0],
        default_query_value=field_display_names[fields[0]],
        allowed_fields=allowed_fields,
        inbound_aliases=inbound_aliases,
        field_display_names=field_display_names,
        allowed_display_names=allowed_display_names,
    )


def _coerce_sort_fields(sort_field: SortField) -> tuple[str, ...]:
    if isinstance(sort_field, str):
        return (sort_field,)
    fields = tuple(sorted(sort_field)) if isinstance(sort_field, set) else tuple(sort_field)
    if not fields:
        msg = "sort_field must include at least one field"
        raise ValueError(msg)
    return fields


def _add_sort_field_alias(inbound_aliases: dict[str, str], *, alias: str, field: str) -> None:
    existing_field = inbound_aliases.get(alias)
    if existing_field is None or existing_field == field:
        inbound_aliases[alias] = field
        return

    msg = f"ambiguous sort field alias '{alias}' maps to both '{existing_field}' and '{field}'"
    raise ValueError(msg)


class _CollectionFilterProvider:
    def __init__(self, field: FieldNameType, *, negated: bool) -> None:
        self.field_name = field.name
        self.type_hint = field.type_hint
        self.param_name = f"{field.name}_{'not_in' if negated else 'in'}_values"
        self.filter_cls: Any = NotInCollectionFilter if negated else InCollectionFilter
        query_suffix = "not_in" if negated else "in"
        parameter_annotation = _query_parameter_annotation(
            _collection_value_annotation(set, field.type_hint),
            Query(
                alias=camelize(f"{field.name}_{query_suffix}"), description=f"Filter {field.name} {query_suffix} values"
            ),
        )
        self.return_annotation = self.filter_cls[field.type_hint] | None
        self.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name, kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=parameter_annotation
                )
            ],
            return_annotation=self.return_annotation,
        )

    def __call__(self, **kwargs: Any) -> Any:
        values = kwargs.get(self.param_name)
        return self.filter_cls[self.type_hint](field_name=self.field_name, values=values) if values else None


class _NullFilterProvider:
    def __init__(self, field_name: str, *, negated: bool) -> None:
        self.field_name = field_name
        self.param_name = f"{field_name}_{'is_not_null' if negated else 'is_null'}"
        self.filter_cls: type[Any] = NotNullFilter if negated else NullFilter
        self.return_annotation = self.filter_cls | None
        self.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=None,
                    annotation=Annotated[
                        bool | None,
                        Query(
                            alias=camelize(self.param_name),
                            description=f"Filter where {field_name} {'IS NOT NULL' if negated else 'IS NULL'}",
                        ),
                    ],
                )
            ],
            return_annotation=self.return_annotation,
        )

    def __call__(self, **kwargs: Any) -> Any:
        return self.filter_cls(field_name=self.field_name) if kwargs.get(self.param_name) else None


class _BooleanFilterProvider:
    __slots__ = ("__signature__", "field_name", "param_name", "return_annotation")

    def __init__(self, field_name: str) -> None:
        self.field_name = field_name
        self.param_name = f"{field_name}_boolean"
        self.return_annotation = BooleanFilter | None
        annotation = _query_parameter_annotation(
            bool | None, Query(alias=camelize(self.param_name), description=f"Filter by boolean field {field_name}")
        )
        self.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name, kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=annotation
                )
            ],
            return_annotation=self.return_annotation,
        )

    def __call__(self, **kwargs: Any) -> BooleanFilter | None:
        val = kwargs.get(self.param_name)
        if val is None:
            return None
        return BooleanFilter(field_name=self.field_name, value=val)


class _ChoicesFilterProvider:
    __slots__ = ("__signature__", "choices", "field_name", "param_name", "return_annotation")

    def __init__(self, field_name: str, choices: list[Any] | tuple[Any, ...] | type[Enum]) -> None:
        self.field_name = field_name
        self.choices = choices
        self.param_name = f"{field_name}_choices"
        choices_type = normalize_choice_field_types(choices)
        self.return_annotation = ChoicesFilter[Any] | None
        parameter_annotation = _query_parameter_annotation(
            _collection_value_annotation(list, choices_type),
            Query(alias=camelize(self.param_name), description=f"Filter {field_name} by choices"),
        )
        self.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name, kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=parameter_annotation
                )
            ],
            return_annotation=self.return_annotation,
        )

    def __call__(self, **kwargs: Any) -> Any:
        values = kwargs.get(self.param_name)
        if not values:
            return None
        return ChoicesFilter[Any](field_name=self.field_name, values=values)


def _add_dependency(params: list[inspect.Parameter], annotations: dict[str, Any], name: str, provider: Any) -> None:
    dependency_annotation = _query_parameter_annotation(provider.return_annotation, Depends(provider))
    params.append(inspect.Parameter(name=name, kind=inspect.Parameter.KEYWORD_ONLY, annotation=dependency_annotation))
    annotations[name] = dependency_annotation
