"""Application dependency providers generators.

This module contains functions to create dependency providers for services and filters.
"""

import datetime
import inspect
from collections.abc import Callable, Mapping
from functools import partial
from types import GenericAlias
from typing import Any, Literal, NamedTuple, TypedDict, cast
from uuid import UUID

from litestar.di import Provide
from litestar.exceptions import ValidationException
from litestar.params import Dependency, Parameter
from litestar.utils.signature import ParsedSignature
from typing_extensions import NotRequired

from sqlspec.core import (
    BeforeAfterFilter,
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
    "create_filter_dependencies",
    "dep_cache",
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


class DependencyDefaults:
    FILTERS_DEPENDENCY_KEY: str = "filters"
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
    type_hint: type[Any] = str


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


class FilterConfig(TypedDict):
    """Configuration for generated Litestar filter dependencies.

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


class DependencyCache:
    """Dependency cache for memoizing dynamically generated dependencies."""

    def __init__(self) -> None:
        self.dependencies: dict[int | str, dict[str, Provide]] = {}

    def add_dependencies(self, key: int | str, dependencies: dict[str, Provide]) -> None:
        self.dependencies[key] = dependencies

    def get_dependencies(self, key: int | str) -> dict[str, Provide] | None:
        return self.dependencies.get(key)


dep_cache = DependencyCache()


def create_filter_dependencies(
    config: FilterConfig, dep_defaults: DependencyDefaults = DEPENDENCY_DEFAULTS
) -> dict[str, Provide]:
    """Create a dependency provider for the combined filter function.

    Args:
        config: FilterConfig instance with desired settings.
        dep_defaults: Dependency defaults to use for the filter dependencies

    Returns:
        A dependency provider function for the combined filter function.
    """
    if (deps := dep_cache.get_dependencies(cache_key := hash(_make_hashable(config)))) is not None:
        return deps
    deps = _create_statement_filters(config, dep_defaults)
    dep_cache.add_dependencies(cache_key, deps)
    return deps


def _create_statement_filters(
    config: FilterConfig, dep_defaults: DependencyDefaults = DEPENDENCY_DEFAULTS
) -> dict[str, Provide]:
    """Create filter dependencies based on configuration.

    Args:
        config: Configuration dictionary specifying which filters to enable
        dep_defaults: Dependency defaults to use for the filter dependencies

    Returns:
        Dictionary of filter provider functions
    """
    filters: dict[str, Provide] = {}

    if id_type := config.get("id_filter", False):
        filters[dep_defaults.ID_FILTER_DEPENDENCY_KEY] = _create_provide(
            _bind_provider(
                _IdFilterProvider(config.get("id_field", "id"), id_type if isinstance(id_type, type) else object),
                _provide_id_filter,
            )
        )

    if config.get("created_at", False):
        filters[dep_defaults.CREATED_FILTER_DEPENDENCY_KEY] = _create_provide(
            _build_before_after_provider("created_at", "createdBefore", "createdAfter")
        )

    if config.get("updated_at", False):
        filters[dep_defaults.UPDATED_FILTER_DEPENDENCY_KEY] = _create_provide(
            _build_before_after_provider("updated_at", "updatedBefore", "updatedAfter")
        )

    if config.get("pagination_type") == "limit_offset":
        filters[dep_defaults.LIMIT_OFFSET_FILTER_DEPENDENCY_KEY] = _create_provide(
            _bind_provider(
                _LimitOffsetFilterProvider(config.get("pagination_size", dep_defaults.DEFAULT_PAGINATION_SIZE)),
                _provide_limit_offset_filter,
            )
        )

    if search_fields := config.get("search"):
        filters[dep_defaults.SEARCH_FILTER_DEPENDENCY_KEY] = _create_provide(
            _bind_provider(
                _SearchFilterProvider(search_fields, config.get("search_ignore_case", False)), _provide_search_filter
            )
        )

    if sort_field := config.get("sort_field"):
        filters[dep_defaults.ORDER_BY_FILTER_DEPENDENCY_KEY] = _create_provide(
            _bind_provider(_OrderByProvider(sort_field, config), _provide_order_by_filter)
        )

    if not_in_fields := config.get("not_in_fields"):
        not_in_fields = {not_in_fields} if isinstance(not_in_fields, (str, FieldNameType)) else not_in_fields

        for field_def in not_in_fields:
            resolved = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            filters[f"{resolved.name}_not_in_filter"] = _create_provide(
                _build_in_collection_provider(resolved, negated=True)
            )

    if in_fields := config.get("in_fields"):
        in_fields = {in_fields} if isinstance(in_fields, (str, FieldNameType)) else in_fields

        for field_def in in_fields:
            resolved = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            filters[f"{resolved.name}_in_filter"] = _create_provide(
                _build_in_collection_provider(resolved, negated=False)
            )

    if null_fields := config.get("null_fields"):
        null_fields = {null_fields} if isinstance(null_fields, str) else set(null_fields)
        for field_name in null_fields:
            filters[f"{field_name}_null_filter"] = _create_provide(_build_null_provider(field_name, negated=False))

    if not_null_fields := config.get("not_null_fields"):
        not_null_fields = {not_null_fields} if isinstance(not_null_fields, str) else set(not_null_fields)
        for field_name in not_null_fields:
            filters[f"{field_name}_not_null_filter"] = _create_provide(_build_null_provider(field_name, negated=True))

    if filters:
        filters[dep_defaults.FILTERS_DEPENDENCY_KEY] = _create_provide(_create_filter_aggregate_function(config))

    return filters


def _create_filter_aggregate_function(config: FilterConfig) -> Callable[..., list[FilterTypes]]:
    """Create filter aggregation function based on configuration.

    Args:
        config: The filter configuration.

    Returns:
        Function that returns list of configured filters.
    """

    parameters: dict[str, inspect.Parameter] = {}
    annotations: dict[str, Any] = {}

    if cls := config.get("id_filter"):
        parameters["id_filter"] = inspect.Parameter(
            name="id_filter",
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=Dependency(skip_validation=True),
            annotation=InCollectionFilter[cls],  # type: ignore[valid-type]
        )
        annotations["id_filter"] = InCollectionFilter[cls]  # type: ignore[valid-type]

    if config.get("created_at"):
        parameters["created_filter"] = inspect.Parameter(
            name="created_filter",
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=Dependency(skip_validation=True),
            annotation=BeforeAfterFilter,
        )
        annotations["created_filter"] = BeforeAfterFilter

    if config.get("updated_at"):
        parameters["updated_filter"] = inspect.Parameter(
            name="updated_filter",
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=Dependency(skip_validation=True),
            annotation=BeforeAfterFilter,
        )
        annotations["updated_filter"] = BeforeAfterFilter

    if config.get("search"):
        parameters["search_filter"] = inspect.Parameter(
            name="search_filter",
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=Dependency(skip_validation=True),
            annotation=SearchFilter,
        )
        annotations["search_filter"] = SearchFilter

    if config.get("pagination_type") == "limit_offset":
        parameters["limit_offset_filter"] = inspect.Parameter(
            name="limit_offset_filter",
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=Dependency(skip_validation=True),
            annotation=LimitOffsetFilter,
        )
        annotations["limit_offset_filter"] = LimitOffsetFilter

    if config.get("sort_field"):
        parameters["order_by_filter"] = inspect.Parameter(
            name="order_by_filter",
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=Dependency(skip_validation=True),
            annotation=OrderByFilter,
        )
        annotations["order_by_filter"] = OrderByFilter

    if not_in_fields := config.get("not_in_fields"):
        for field_def in not_in_fields:
            field_def = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            parameters[f"{field_def.name}_not_in_filter"] = inspect.Parameter(
                name=f"{field_def.name}_not_in_filter",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=Dependency(skip_validation=True),
                annotation=NotInCollectionFilter[Any],
            )
            annotations[f"{field_def.name}_not_in_filter"] = NotInCollectionFilter[Any]

    if in_fields := config.get("in_fields"):
        for field_def in in_fields:
            field_def = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            parameters[f"{field_def.name}_in_filter"] = inspect.Parameter(
                name=f"{field_def.name}_in_filter",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=Dependency(skip_validation=True),
                annotation=InCollectionFilter[Any],
            )
            annotations[f"{field_def.name}_in_filter"] = InCollectionFilter[Any]

    if null_fields := config.get("null_fields"):
        null_fields = {null_fields} if isinstance(null_fields, str) else set(null_fields)
        for field_name in null_fields:
            parameters[f"{field_name}_null_filter"] = inspect.Parameter(
                name=f"{field_name}_null_filter",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=Dependency(skip_validation=True),
                annotation=NullFilter | None,
            )
            annotations[f"{field_name}_null_filter"] = NullFilter | None

    if not_null_fields := config.get("not_null_fields"):
        not_null_fields = {not_null_fields} if isinstance(not_null_fields, str) else set(not_null_fields)
        for field_name in not_null_fields:
            parameters[f"{field_name}_not_null_filter"] = inspect.Parameter(
                name=f"{field_name}_not_null_filter",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=Dependency(skip_validation=True),
                annotation=NotNullFilter | None,
            )
            annotations[f"{field_name}_not_null_filter"] = NotNullFilter | None

    return _make_aggregate_filter_provider(list(parameters.values()), annotations)


def _make_hashable(value: Any) -> HashableType:
    """Convert a value into a hashable type for caching purposes.

    Args:
        value: Any value that needs to be made hashable.

    Returns:
        A hashable version of the value.
    """
    if isinstance(value, dict):
        items = []
        for k in sorted(value.keys()):  # pyright: ignore
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


def _set_provider_metadata(
    provider: Any, signature: inspect.Signature, annotations: dict[str, Any]
) -> Callable[..., Any]:
    provider.__signature__ = signature
    provider.__annotations__ = annotations
    return cast("Callable[..., Any]", provider)


def _bind_provider(context: Any, provider: Callable[..., Any]) -> Callable[..., Any]:
    return _set_provider_metadata(partial(provider, context), context.signature, context.annotations)


def _create_provide(provider: Callable[..., Any]) -> Provide:
    dependency = Provide(provider, sync_to_thread=False)
    dependency.parsed_fn_signature = ParsedSignature.from_signature(
        inspect.signature(provider), getattr(provider, "__annotations__", {})
    )
    return dependency


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


def _collection_value_annotation(collection_type: type[Any], value_type: type[Any]) -> Any:
    return GenericAlias(collection_type, (value_type,)) | None


class _CollectionFilterProvider:
    """Per-field `IN` / `NOT IN` provider with a unique parameter name (issue #435)."""

    def __init__(self, field: FieldNameType, *, negated: bool) -> None:
        self.type_hint = field.type_hint
        self.field_name = field.name
        self.param_name = f"{field.name}_values"
        self.filter_cls: Any = NotInCollectionFilter if negated else InCollectionFilter
        parameter_annotation = _collection_value_annotation(list, field.type_hint)
        self.return_annotation = self.filter_cls[field.type_hint] | None
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(
                        query=camelize(f"{field.name}_{'not_in' if negated else 'in'}"), default=None, required=False
                    ),
                    annotation=parameter_annotation,
                )
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {self.param_name: parameter_annotation, "return": self.return_annotation}

    def __call__(self, **kwargs: Any) -> Any:
        values = kwargs.get(self.param_name)
        return self.filter_cls[self.type_hint](field_name=self.field_name, values=values) if values else None


class _NullFilterProvider:
    def __init__(self, field_name: str, *, negated: bool) -> None:
        suffix = "is_not_null" if negated else "is_null"
        self.field_name = field_name
        self.param_name = f"{field_name}_{suffix}"
        self.filter_cls: type[Any] = NotNullFilter if negated else NullFilter
        self.return_annotation = self.filter_cls | None
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(query=camelize(self.param_name), default=None, required=False),
                    annotation=bool | None,
                )
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {self.param_name: bool | None, "return": self.return_annotation}

    def __call__(self, **kwargs: Any) -> Any:
        return self.filter_cls(field_name=self.field_name) if kwargs.get(self.param_name) else None


class _BeforeAfterFilterProvider:
    """Before/after provider with unique parameter names for sibling dependencies."""

    def __init__(self, field_name: str, before_alias: str, after_alias: str) -> None:
        self.field_name = field_name
        self.before_param = f"{field_name}_before"
        self.after_param = f"{field_name}_after"
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.before_param,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(query=before_alias, default=None, required=False),
                    annotation=DTorNone,
                ),
                inspect.Parameter(
                    self.after_param,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(query=after_alias, default=None, required=False),
                    annotation=DTorNone,
                ),
            ],
            return_annotation=BeforeAfterFilter,
        )
        self.annotations = {self.before_param: DTorNone, self.after_param: DTorNone, "return": BeforeAfterFilter}

    def __call__(self, **kwargs: Any) -> BeforeAfterFilter:
        return BeforeAfterFilter(self.field_name, kwargs.get(self.before_param), kwargs.get(self.after_param))


class _IdFilterProvider:
    def __init__(self, field_name: str, id_type: type[Any]) -> None:
        self.field_name = field_name
        parameter_annotation = _collection_value_annotation(list, id_type)
        self.return_annotation = InCollectionFilter[id_type]  # type: ignore[valid-type]
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "ids",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(query="ids", default=None, required=False),
                    annotation=parameter_annotation,
                )
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {"ids": parameter_annotation, "return": self.return_annotation}

    def __call__(self, ids: list[Any] | None = None) -> InCollectionFilter[Any]:
        return InCollectionFilter(field_name=self.field_name, values=ids)


class _LimitOffsetFilterProvider:
    def __init__(self, default_page_size: int) -> None:
        self.default_page_size = default_page_size
        self.return_annotation = LimitOffsetFilter
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "current_page",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(ge=1, query="currentPage", default=1, required=False),
                    annotation=int,
                ),
                inspect.Parameter(
                    "page_size",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(query="pageSize", ge=1, default=default_page_size, required=False),
                    annotation=int,
                ),
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {"current_page": int, "page_size": int, "return": self.return_annotation}

    def __call__(self, current_page: int = 1, page_size: int | None = None) -> LimitOffsetFilter:
        resolved_page_size = page_size if page_size is not None else self.default_page_size
        return LimitOffsetFilter(resolved_page_size, resolved_page_size * (current_page - 1))


class _SearchFilterProvider:
    def __init__(self, search_fields: str | set[str] | list[str], ignore_case_default: bool) -> None:
        self.search_fields = search_fields
        self.ignore_case_default = ignore_case_default
        self.return_annotation = SearchFilter
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "search_string",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(title="Field to search", query="searchString", default=None, required=False),
                    annotation=StringOrNone,
                ),
                inspect.Parameter(
                    "ignore_case",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(
                        title="Search should be case sensitive",
                        query="searchIgnoreCase",
                        default=ignore_case_default,
                        required=False,
                    ),
                    annotation=BooleanOrNone,
                ),
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {"search_string": StringOrNone, "ignore_case": BooleanOrNone, "return": SearchFilter}

    def __call__(self, search_string: StringOrNone = None, ignore_case: BooleanOrNone = None) -> SearchFilter:
        field_names: set[Any] = (
            set(self.search_fields.split(",")) if isinstance(self.search_fields, str) else set(self.search_fields)
        )
        return SearchFilter(
            field_name=field_names,
            value=search_string,
            ignore_case=self.ignore_case_default if ignore_case is None else ignore_case,
        )


class _OrderByProvider:
    def __init__(self, sort_field: SortField, config: FilterConfig) -> None:
        self.sort_resolution = _resolve_sort_field_aliases(
            sort_field,
            sort_field_aliases=config.get("sort_field_aliases"),
            sort_field_camelize=config.get("sort_field_camelize", True),
        )
        self.allowed_field_names = ", ".join(self.sort_resolution.allowed_display_names)
        self.sort_order_default: SortOrder = config.get("sort_order", "desc")
        self.return_annotation = OrderByFilter
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "field_name",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(
                        title="Order by field",
                        query="orderBy",
                        default=self.sort_resolution.default_query_value,
                        required=False,
                    ),
                    annotation=StringOrNone,
                ),
                inspect.Parameter(
                    "sort_order",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=Parameter(
                        title="Field to search", query="sortOrder", default=self.sort_order_default, required=False
                    ),
                    annotation=SortOrderOrNone,
                ),
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {"field_name": StringOrNone, "sort_order": SortOrderOrNone, "return": OrderByFilter}

    def __call__(self, field_name: StringOrNone = None, sort_order: SortOrderOrNone = None) -> OrderByFilter:
        resolved_field = (
            self.sort_resolution.normalize(field_name) if field_name else self.sort_resolution.default_field
        )
        if resolved_field is None:
            msg = f"Invalid orderBy field '{field_name}'. Allowed fields: {self.allowed_field_names}"
            raise ValidationException(detail=msg)
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


def _provide_collection_filter(context: _CollectionFilterProvider, **kwargs: Any) -> Any:
    values = kwargs.get(context.param_name)
    return context.filter_cls[context.type_hint](field_name=context.field_name, values=values) if values else None


def _provide_null_filter(context: _NullFilterProvider, **kwargs: Any) -> Any:
    return context.filter_cls(field_name=context.field_name) if kwargs.get(context.param_name) else None


def _provide_before_after_filter(context: _BeforeAfterFilterProvider, **kwargs: Any) -> BeforeAfterFilter:
    return BeforeAfterFilter(context.field_name, kwargs.get(context.before_param), kwargs.get(context.after_param))


def _provide_id_filter(context: _IdFilterProvider, ids: list[Any] | None = None) -> InCollectionFilter[Any]:
    return InCollectionFilter(field_name=context.field_name, values=ids)


def _provide_limit_offset_filter(
    context: _LimitOffsetFilterProvider, current_page: int = 1, page_size: int | None = None
) -> LimitOffsetFilter:
    resolved_page_size = page_size if page_size is not None else context.default_page_size
    return LimitOffsetFilter(resolved_page_size, resolved_page_size * (current_page - 1))


def _provide_search_filter(
    context: _SearchFilterProvider, search_string: StringOrNone = None, ignore_case: BooleanOrNone = None
) -> SearchFilter:
    field_names: set[Any] = (
        set(context.search_fields.split(",")) if isinstance(context.search_fields, str) else set(context.search_fields)
    )
    return SearchFilter(
        field_name=field_names,
        value=search_string,
        ignore_case=context.ignore_case_default if ignore_case is None else ignore_case,
    )


def _provide_order_by_filter(
    context: _OrderByProvider, field_name: StringOrNone = None, sort_order: SortOrderOrNone = None
) -> OrderByFilter:
    resolved_field = (
        context.sort_resolution.normalize(field_name) if field_name else context.sort_resolution.default_field
    )
    if resolved_field is None:
        msg = f"Invalid orderBy field '{field_name}'. Allowed fields: {context.allowed_field_names}"
        raise ValidationException(detail=msg)
    return OrderByFilter(field_name=resolved_field, sort_order=sort_order or context.sort_order_default)


def _build_in_collection_provider(field: FieldNameType, *, negated: bool) -> Callable[..., Any]:
    return _bind_provider(_CollectionFilterProvider(field, negated=negated), _provide_collection_filter)


def _build_null_provider(field_name: str, *, negated: bool) -> Callable[..., Any]:
    return _bind_provider(_NullFilterProvider(field_name, negated=negated), _provide_null_filter)


def _build_before_after_provider(field_name: str, before_alias: str, after_alias: str) -> Callable[..., Any]:
    return _bind_provider(
        _BeforeAfterFilterProvider(field_name, before_alias, after_alias), _provide_before_after_filter
    )
