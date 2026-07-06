"""Application dependency providers generators.

This module contains functions to create dependency providers for services and filters.
"""

import copy
import datetime
import inspect
import typing
from collections.abc import Callable, Mapping
from enum import Enum
from functools import partial
from inspect import isclass
from types import GenericAlias
from typing import Annotated, Any, Literal, NamedTuple, TypedDict, TypeVar, cast
from uuid import UUID

from litestar.di import NamedDependency, Provide
from litestar.exceptions import ValidationException
from litestar.params import QueryParameter, SkipValidation
from litestar.utils.signature import ParsedSignature
from typing_extensions import NotRequired

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
    "create_filter_dependencies",
    "dep_cache",
    "normalize_choice_field_types",
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
_ProviderT = TypeVar("_ProviderT")


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


# Keep FilterConfig field unions and provider signatures in sync with sqlspec.extensions.fastapi.providers.
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
    boolean_fields: NotRequired[str | set[str] | list[str]]
    """Field or fields that support boolean filtering."""
    choice_fields: NotRequired[ChoiceField | set[ChoiceField] | list[str | ChoiceField]]
    """Field or fields that support choices filtering."""


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

    if boolean_fields := config.get("boolean_fields"):
        boolean_fields = {boolean_fields} if isinstance(boolean_fields, str) else set(boolean_fields)
        for field_name in boolean_fields:
            filters[f"{field_name}_boolean_filter"] = _create_provide(
                _bind_provider(_BooleanFilterProvider(field_name), _provide_boolean_filter)
            )

    if choice_fields := config.get("choice_fields"):
        choice_fields = {choice_fields} if isinstance(choice_fields, ChoiceField) else choice_fields
        for choice_def in choice_fields:
            resolved_choice = ChoiceField(name=choice_def, choices=[]) if isinstance(choice_def, str) else choice_def
            filters[f"{resolved_choice.name}_choices_filter"] = _create_provide(
                _bind_provider(
                    _ChoicesFilterProvider(resolved_choice.name, resolved_choice.choices), _provide_choices_filter
                )
            )

    if filters:
        filters[dep_defaults.FILTERS_DEPENDENCY_KEY] = _create_provide(_configured_filter_aggregator(config))

    return filters


def _configured_filter_aggregator(config: FilterConfig) -> Callable[..., list[FilterTypes]]:
    """Create filter aggregation function based on configuration.

    Args:
        config: The filter configuration.

    Returns:
        Function that returns list of configured filters.
    """

    parameters: dict[str, inspect.Parameter] = {}
    annotations: dict[str, Any] = {}
    annotation: Any

    if cls := config.get("id_filter"):
        annotation = NamedDependency[SkipValidation[InCollectionFilter[cls]]]  # type: ignore[valid-type]
        parameters["id_filter"] = inspect.Parameter(
            name="id_filter", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
        )
        annotations["id_filter"] = annotation

    if config.get("created_at"):
        annotation = NamedDependency[SkipValidation[BeforeAfterFilter]]
        parameters["created_filter"] = inspect.Parameter(
            name="created_filter", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
        )
        annotations["created_filter"] = annotation

    if config.get("updated_at"):
        annotation = NamedDependency[SkipValidation[BeforeAfterFilter]]
        parameters["updated_filter"] = inspect.Parameter(
            name="updated_filter", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
        )
        annotations["updated_filter"] = annotation

    if config.get("search"):
        annotation = NamedDependency[SkipValidation[SearchFilter]]
        parameters["search_filter"] = inspect.Parameter(
            name="search_filter", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
        )
        annotations["search_filter"] = annotation

    if config.get("pagination_type") == "limit_offset":
        annotation = NamedDependency[SkipValidation[LimitOffsetFilter]]
        parameters["limit_offset_filter"] = inspect.Parameter(
            name="limit_offset_filter", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
        )
        annotations["limit_offset_filter"] = annotation

    if config.get("sort_field"):
        annotation = NamedDependency[SkipValidation[OrderByFilter]]
        parameters["order_by_filter"] = inspect.Parameter(
            name="order_by_filter", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
        )
        annotations["order_by_filter"] = annotation

    if not_in_fields := config.get("not_in_fields"):
        for field_def in not_in_fields:
            field_def = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            annotation = NamedDependency[SkipValidation[NotInCollectionFilter[Any]]]
            parameters[f"{field_def.name}_not_in_filter"] = inspect.Parameter(
                name=f"{field_def.name}_not_in_filter",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=annotation,
            )
            annotations[f"{field_def.name}_not_in_filter"] = annotation

    if in_fields := config.get("in_fields"):
        for field_def in in_fields:
            field_def = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            annotation = NamedDependency[SkipValidation[InCollectionFilter[Any]]]
            parameters[f"{field_def.name}_in_filter"] = inspect.Parameter(
                name=f"{field_def.name}_in_filter", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
            )
            annotations[f"{field_def.name}_in_filter"] = annotation

    if null_fields := config.get("null_fields"):
        null_fields = {null_fields} if isinstance(null_fields, str) else set(null_fields)
        for field_name in null_fields:
            annotation = NamedDependency[SkipValidation[NullFilter]] | None
            parameters[f"{field_name}_null_filter"] = inspect.Parameter(
                name=f"{field_name}_null_filter", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
            )
            annotations[f"{field_name}_null_filter"] = annotation

    if not_null_fields := config.get("not_null_fields"):
        not_null_fields = {not_null_fields} if isinstance(not_null_fields, str) else set(not_null_fields)
        for field_name in not_null_fields:
            annotation = NamedDependency[SkipValidation[NotNullFilter]] | None
            parameters[f"{field_name}_not_null_filter"] = inspect.Parameter(
                name=f"{field_name}_not_null_filter",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=annotation,
            )
            annotations[f"{field_name}_not_null_filter"] = annotation

    if boolean_fields := config.get("boolean_fields"):
        boolean_fields = {boolean_fields} if isinstance(boolean_fields, str) else set(boolean_fields)
        for field_name in boolean_fields:
            annotation = NamedDependency[SkipValidation[BooleanFilter]] | None
            parameters[f"{field_name}_boolean_filter"] = inspect.Parameter(
                name=f"{field_name}_boolean_filter", kind=inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation
            )
            annotations[f"{field_name}_boolean_filter"] = annotation

    if choice_fields := config.get("choice_fields"):
        choice_fields = {choice_fields} if isinstance(choice_fields, ChoiceField) else choice_fields
        for choice_def in choice_fields:
            resolved_choice = ChoiceField(name=choice_def, choices=[]) if isinstance(choice_def, str) else choice_def
            annotation = NamedDependency[SkipValidation[ChoicesFilter[Any]]] | None
            parameters[f"{resolved_choice.name}_choices_filter"] = inspect.Parameter(
                name=f"{resolved_choice.name}_choices_filter",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=annotation,
            )
            annotations[f"{resolved_choice.name}_choices_filter"] = annotation

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


def _query_parameter_annotation(value_annotation: Any, query: Any) -> Any:
    return Annotated[value_annotation, query]


def _memoize_deepcopy(original: Any, copied: _ProviderT, memo: dict[int, Any]) -> _ProviderT:
    memo[id(original)] = copied
    return copied


class _CollectionFilterProvider:
    """Per-field `IN` / `NOT IN` provider with a unique parameter name (issue #435)."""

    def __init__(self, field: FieldNameType, *, negated: bool) -> None:
        self.type_hint = field.type_hint
        self.field_name = field.name
        self.param_name = f"{field.name}_values"
        self.filter_cls: Any = NotInCollectionFilter if negated else InCollectionFilter
        self.return_annotation = self.filter_cls[field.type_hint] | None
        annotation = _query_parameter_annotation(
            _collection_value_annotation(list, field.type_hint),
            QueryParameter(name=camelize(f"{field.name}_{'not_in' if negated else 'in'}"), required=False),
        )
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name, kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=annotation
                )
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {self.param_name: annotation, "return": self.return_annotation}

    def __call__(self, **kwargs: Any) -> Any:
        values = kwargs.get(self.param_name)
        return self.filter_cls[self.type_hint](field_name=self.field_name, values=values) if values else None

    def __deepcopy__(self, memo: dict[int, Any]) -> "_CollectionFilterProvider":
        return _memoize_deepcopy(
            self,
            _CollectionFilterProvider(
                FieldNameType(self.field_name, self.type_hint), negated=self.filter_cls is NotInCollectionFilter
            ),
            memo,
        )


class _NullFilterProvider:
    def __init__(self, field_name: str, *, negated: bool) -> None:
        suffix = "is_not_null" if negated else "is_null"
        self.field_name = field_name
        self.param_name = f"{field_name}_{suffix}"
        self.filter_cls: type[Any] = NotNullFilter if negated else NullFilter
        self.return_annotation = self.filter_cls | None
        annotation = _query_parameter_annotation(
            bool | None, QueryParameter(name=camelize(self.param_name), required=False)
        )
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name, kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=annotation
                )
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {self.param_name: annotation, "return": self.return_annotation}

    def __call__(self, **kwargs: Any) -> Any:
        return self.filter_cls(field_name=self.field_name) if kwargs.get(self.param_name) else None

    def __deepcopy__(self, memo: dict[int, Any]) -> "_NullFilterProvider":
        return _memoize_deepcopy(
            self, _NullFilterProvider(self.field_name, negated=self.filter_cls is NotNullFilter), memo
        )


class _BeforeAfterFilterProvider:
    """Before/after provider with unique parameter names for sibling dependencies."""

    def __init__(self, field_name: str, before_alias: str, after_alias: str) -> None:
        self.field_name = field_name
        self.before_alias = before_alias
        self.after_alias = after_alias
        self.before_param = f"{field_name}_before"
        self.after_param = f"{field_name}_after"
        before_annotation = _query_parameter_annotation(DTorNone, QueryParameter(name=before_alias, required=False))
        after_annotation = _query_parameter_annotation(DTorNone, QueryParameter(name=after_alias, required=False))
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.before_param, kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=before_annotation
                ),
                inspect.Parameter(
                    self.after_param, kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=after_annotation
                ),
            ],
            return_annotation=BeforeAfterFilter,
        )
        self.annotations = {
            self.before_param: before_annotation,
            self.after_param: after_annotation,
            "return": BeforeAfterFilter,
        }

    def __call__(self, **kwargs: Any) -> BeforeAfterFilter:
        return BeforeAfterFilter(self.field_name, kwargs.get(self.before_param), kwargs.get(self.after_param))

    def __deepcopy__(self, memo: dict[int, Any]) -> "_BeforeAfterFilterProvider":
        return _memoize_deepcopy(
            self, _BeforeAfterFilterProvider(self.field_name, self.before_alias, self.after_alias), memo
        )


class _IdFilterProvider:
    def __init__(self, field_name: str, id_type: type[Any]) -> None:
        self.field_name = field_name
        self.id_type = id_type
        self.return_annotation = InCollectionFilter[id_type]  # type: ignore[valid-type]
        annotation = _query_parameter_annotation(
            _collection_value_annotation(list, id_type), QueryParameter(name="ids", required=False)
        )
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter("ids", kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=annotation)
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {"ids": annotation, "return": self.return_annotation}

    def __call__(self, ids: list[Any] | None = None) -> InCollectionFilter[Any]:
        return InCollectionFilter(field_name=self.field_name, values=ids)

    def __deepcopy__(self, memo: dict[int, Any]) -> "_IdFilterProvider":
        return _memoize_deepcopy(self, _IdFilterProvider(self.field_name, self.id_type), memo)


class _LimitOffsetFilterProvider:
    def __init__(self, default_page_size: int) -> None:
        self.default_page_size = default_page_size
        self.return_annotation = LimitOffsetFilter
        current_annotation = _query_parameter_annotation(int, QueryParameter(name="currentPage", required=False, ge=1))
        size_annotation = _query_parameter_annotation(int, QueryParameter(name="pageSize", required=False, ge=1))
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "current_page", kind=inspect.Parameter.KEYWORD_ONLY, default=1, annotation=current_annotation
                ),
                inspect.Parameter(
                    "page_size",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=default_page_size,
                    annotation=size_annotation,
                ),
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {
            "current_page": current_annotation,
            "page_size": size_annotation,
            "return": self.return_annotation,
        }

    def __call__(self, current_page: int = 1, page_size: int | None = None) -> LimitOffsetFilter:
        resolved_page_size = page_size if page_size is not None else self.default_page_size
        return LimitOffsetFilter(resolved_page_size, resolved_page_size * (current_page - 1))

    def __deepcopy__(self, memo: dict[int, Any]) -> "_LimitOffsetFilterProvider":
        return _memoize_deepcopy(self, _LimitOffsetFilterProvider(self.default_page_size), memo)


class _SearchFilterProvider:
    def __init__(self, search_fields: str | set[str] | list[str], ignore_case_default: bool) -> None:
        self.search_fields = search_fields
        self.ignore_case_default = ignore_case_default
        self.return_annotation = SearchFilter
        search_annotation = _query_parameter_annotation(
            StringOrNone, QueryParameter(name="searchString", required=False, title="Field to search")
        )
        ignore_annotation = _query_parameter_annotation(
            BooleanOrNone,
            QueryParameter(name="searchIgnoreCase", required=False, title="Search should be case sensitive"),
        )
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "search_string", kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=search_annotation
                ),
                inspect.Parameter(
                    "ignore_case",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=ignore_case_default,
                    annotation=ignore_annotation,
                ),
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {
            "search_string": search_annotation,
            "ignore_case": ignore_annotation,
            "return": SearchFilter,
        }

    def __call__(self, search_string: StringOrNone = None, ignore_case: BooleanOrNone = None) -> SearchFilter:
        field_names: set[Any] = (
            set(self.search_fields.split(",")) if isinstance(self.search_fields, str) else set(self.search_fields)
        )
        return SearchFilter(
            field_name=field_names,
            value=search_string,
            ignore_case=self.ignore_case_default if ignore_case is None else ignore_case,
        )

    def __deepcopy__(self, memo: dict[int, Any]) -> "_SearchFilterProvider":
        return _memoize_deepcopy(
            self, _SearchFilterProvider(copy.deepcopy(self.search_fields, memo), self.ignore_case_default), memo
        )


class _OrderByProvider:
    def __init__(self, sort_field: SortField, config: FilterConfig) -> None:
        self.sort_field = sort_field
        self.sort_field_aliases = config.get("sort_field_aliases")
        self.sort_field_camelize = config.get("sort_field_camelize", True)
        self.sort_resolution = _resolve_sort_field_aliases(
            sort_field, sort_field_aliases=self.sort_field_aliases, sort_field_camelize=self.sort_field_camelize
        )
        self.allowed_field_names = ", ".join(self.sort_resolution.allowed_display_names)
        self.sort_order_default: SortOrder = config.get("sort_order", "desc")
        self.return_annotation = OrderByFilter
        field_annotation = _query_parameter_annotation(
            StringOrNone, QueryParameter(name="orderBy", required=False, title="Order by field")
        )
        order_annotation = _query_parameter_annotation(
            SortOrderOrNone, QueryParameter(name="sortOrder", required=False, title="Field to search")
        )
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    "field_name",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=self.sort_resolution.default_query_value,
                    annotation=field_annotation,
                ),
                inspect.Parameter(
                    "sort_order",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    default=self.sort_order_default,
                    annotation=order_annotation,
                ),
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {"field_name": field_annotation, "sort_order": order_annotation, "return": OrderByFilter}

    def __call__(self, field_name: StringOrNone = None, sort_order: SortOrderOrNone = None) -> OrderByFilter:
        resolved_field = (
            self.sort_resolution.normalize(field_name) if field_name else self.sort_resolution.default_field
        )
        if resolved_field is None:
            msg = f"Invalid orderBy field '{field_name}'. Allowed fields: {self.allowed_field_names}"
            raise ValidationException(detail=msg)
        return OrderByFilter(field_name=resolved_field, sort_order=sort_order or self.sort_order_default)

    def __deepcopy__(self, memo: dict[int, Any]) -> "_OrderByProvider":
        config: FilterConfig = {"sort_order": self.sort_order_default, "sort_field_camelize": self.sort_field_camelize}
        if self.sort_field_aliases is not None:
            config["sort_field_aliases"] = copy.deepcopy(self.sort_field_aliases, memo)
        return _memoize_deepcopy(self, _OrderByProvider(copy.deepcopy(self.sort_field, memo), config), memo)


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


class _BooleanFilterProvider:
    __slots__ = ("annotations", "field_name", "param_name", "return_annotation", "signature")

    def __init__(self, field_name: str) -> None:
        self.field_name = field_name
        self.param_name = f"{field_name}_boolean"
        self.return_annotation = BooleanFilter | None
        annotation = _query_parameter_annotation(
            bool | None, QueryParameter(name=camelize(self.param_name), required=False)
        )
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name, kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=annotation
                )
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {self.param_name: annotation, "return": self.return_annotation}

    def __deepcopy__(self, memo: dict[int, Any]) -> "_BooleanFilterProvider":
        return _memoize_deepcopy(self, _BooleanFilterProvider(self.field_name), memo)


class _ChoicesFilterProvider:
    __slots__ = ("annotations", "choices", "field_name", "param_name", "return_annotation", "signature")

    def __init__(self, field_name: str, choices: list[Any] | tuple[Any, ...] | type[Enum]) -> None:
        self.field_name = field_name
        self.choices = choices
        self.param_name = f"{field_name}_choices"
        choices_type = normalize_choice_field_types(choices)
        self.return_annotation = ChoicesFilter[Any] | None
        annotation = _query_parameter_annotation(
            _collection_value_annotation(list, choices_type),
            QueryParameter(name=camelize(self.param_name), required=False),
        )
        self.signature = inspect.Signature(
            parameters=[
                inspect.Parameter(
                    self.param_name, kind=inspect.Parameter.KEYWORD_ONLY, default=None, annotation=annotation
                )
            ],
            return_annotation=self.return_annotation,
        )
        self.annotations = {self.param_name: annotation, "return": self.return_annotation}

    def __deepcopy__(self, memo: dict[int, Any]) -> "_ChoicesFilterProvider":
        return _memoize_deepcopy(self, _ChoicesFilterProvider(self.field_name, copy.deepcopy(self.choices, memo)), memo)


def _provide_boolean_filter(context: _BooleanFilterProvider, **kwargs: Any) -> BooleanFilter | None:
    val = kwargs.get(context.param_name)
    if val is None:
        return None
    return BooleanFilter(field_name=context.field_name, value=val)


def _provide_choices_filter(context: _ChoicesFilterProvider, **kwargs: Any) -> Any:
    values = kwargs.get(context.param_name)
    if not values:
        return None
    return ChoicesFilter[Any](field_name=context.field_name, values=values)
