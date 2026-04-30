# ruff: noqa: B008
"""Application dependency providers generators.

This module contains functions to create dependency providers for services and filters.
"""

import datetime
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, TypedDict, cast
from uuid import UUID

from litestar.di import Provide
from litestar.exceptions import ValidationException
from litestar.params import Dependency, Parameter
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
from sqlspec.extensions._filter_aliases import resolve_sort_field_aliases
from sqlspec.utils.singleton import SingletonMeta
from sqlspec.utils.text import camelize

if TYPE_CHECKING:
    from sqlglot import exp

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


class DependencyCache(metaclass=SingletonMeta):
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


def _build_in_collection_provider(field: FieldNameType, *, negated: bool) -> Callable[..., Any]:
    """Build a per-field `IN` / `NOT IN` filter provider with a unique parameter name.

    Litestar's dependency resolver collapses parameters across distinct ``Provide()``
    instances by Python parameter name, ignoring per-instance ``Parameter(query=...)``
    aliases. Giving each provider a unique parameter name (via ``__signature__``)
    prevents siblings in the same family from cross-binding (issue #435).
    """
    type_hint = field.type_hint
    field_name = field.name
    param_name = f"{field_name}_values"
    alias = camelize(f"{field_name}_{'not_in' if negated else 'in'}")
    filter_cls: Any = NotInCollectionFilter if negated else InCollectionFilter
    annotation = list[type_hint] | None  # type: ignore[valid-type]
    return_annotation = filter_cls[type_hint] | None

    def provide(**kwargs: Any) -> Any:
        values = kwargs.get(param_name)
        return filter_cls[type_hint](field_name=field_name, values=values) if values else None

    provide.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        parameters=[
            inspect.Parameter(
                param_name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=Parameter(query=alias, default=None, required=False),
                annotation=annotation,
            )
        ],
        return_annotation=return_annotation,
    )
    provide.__annotations__ = {param_name: annotation, "return": return_annotation}
    return provide


def _build_null_provider(field_name: str, *, negated: bool) -> Callable[..., Any]:
    """Build a per-field ``IS NULL`` / ``IS NOT NULL`` provider with a unique parameter name (issue #435)."""
    suffix = "is_not_null" if negated else "is_null"
    param_name = f"{field_name}_{suffix}"
    alias = camelize(f"{field_name}_{suffix}")
    filter_cls: type[Any] = NotNullFilter if negated else NullFilter
    annotation = bool | None
    return_annotation = filter_cls | None

    def provide(**kwargs: Any) -> Any:
        flag = kwargs.get(param_name)
        return filter_cls(field_name=field_name) if flag else None

    provide.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        parameters=[
            inspect.Parameter(
                param_name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=Parameter(query=alias, default=None, required=False),
                annotation=annotation,
            )
        ],
        return_annotation=return_annotation,
    )
    provide.__annotations__ = {param_name: annotation, "return": return_annotation}
    return provide


def _build_before_after_provider(field_name: str, before_alias: str, after_alias: str) -> Callable[..., Any]:
    """Build a ``BeforeAfterFilter`` provider with unique ``before``/``after`` parameter names (issue #435).

    ``created_at`` and ``updated_at`` providers both used ``before``/``after``, so when
    enabled together they cross-bound to whichever query alias Litestar resolved first.
    """
    before_param = f"{field_name}_before"
    after_param = f"{field_name}_after"

    def provide(**kwargs: Any) -> BeforeAfterFilter:
        return BeforeAfterFilter(field_name, kwargs.get(before_param), kwargs.get(after_param))

    provide.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        parameters=[
            inspect.Parameter(
                before_param,
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=Parameter(query=before_alias, default=None, required=False),
                annotation=DTorNone,
            ),
            inspect.Parameter(
                after_param,
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=Parameter(query=after_alias, default=None, required=False),
                annotation=DTorNone,
            ),
        ],
        return_annotation=BeforeAfterFilter,
    )
    provide.__annotations__ = {before_param: DTorNone, after_param: DTorNone, "return": BeforeAfterFilter}
    return provide


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

    if config.get("id_filter", False):

        def provide_id_filter(  # pyright: ignore[reportUnknownParameterType]
            ids: list[str] | None = Parameter(query="ids", default=None, required=False),
        ) -> InCollectionFilter:  # pyright: ignore[reportMissingTypeArgument]
            return InCollectionFilter(field_name=config.get("id_field", "id"), values=ids)

        filters[dep_defaults.ID_FILTER_DEPENDENCY_KEY] = Provide(provide_id_filter, sync_to_thread=False)  # pyright: ignore[reportUnknownArgumentType]

    if config.get("created_at", False):
        filters[dep_defaults.CREATED_FILTER_DEPENDENCY_KEY] = Provide(
            _build_before_after_provider("created_at", "createdBefore", "createdAfter"), sync_to_thread=False
        )

    if config.get("updated_at", False):
        filters[dep_defaults.UPDATED_FILTER_DEPENDENCY_KEY] = Provide(
            _build_before_after_provider("updated_at", "updatedBefore", "updatedAfter"), sync_to_thread=False
        )

    if config.get("pagination_type") == "limit_offset":

        def provide_limit_offset_pagination(
            current_page: int = Parameter(ge=1, query="currentPage", default=1, required=False),
            page_size: int = Parameter(
                query="pageSize",
                ge=1,
                default=config.get("pagination_size", dep_defaults.DEFAULT_PAGINATION_SIZE),
                required=False,
            ),
        ) -> LimitOffsetFilter:
            return LimitOffsetFilter(page_size, page_size * (current_page - 1))

        filters[dep_defaults.LIMIT_OFFSET_FILTER_DEPENDENCY_KEY] = Provide(
            provide_limit_offset_pagination, sync_to_thread=False
        )

    if search_fields := config.get("search"):

        def provide_search_filter(
            search_string: StringOrNone = Parameter(
                title="Field to search", query="searchString", default=None, required=False
            ),
            ignore_case: BooleanOrNone = Parameter(
                title="Search should be case sensitive",
                query="searchIgnoreCase",
                default=config.get("search_ignore_case", False),
                required=False,
            ),
        ) -> SearchFilter:
            field_names: set[str | exp.Expression] = (
                set(search_fields.split(",")) if isinstance(search_fields, str) else set(search_fields)
            )
            return SearchFilter(field_name=field_names, value=search_string, ignore_case=ignore_case or False)

        filters[dep_defaults.SEARCH_FILTER_DEPENDENCY_KEY] = Provide(provide_search_filter, sync_to_thread=False)

    if sort_field := config.get("sort_field"):
        sort_resolution = resolve_sort_field_aliases(
            sort_field,
            sort_field_aliases=config.get("sort_field_aliases"),
            sort_field_camelize=config.get("sort_field_camelize", True),
        )
        allowed_field_names = ", ".join(sort_resolution.allowed_display_names)
        sort_order_default = config.get("sort_order", "desc")

        def provide_order_by(
            field_name: StringOrNone = Parameter(
                title="Order by field", query="orderBy", default=sort_resolution.default_query_value, required=False
            ),
            sort_order: SortOrderOrNone = Parameter(
                title="Field to search", query="sortOrder", default=sort_order_default, required=False
            ),
        ) -> OrderByFilter:
            resolved_field = sort_resolution.normalize(field_name) if field_name else sort_resolution.default_field
            if resolved_field is None:
                msg = f"Invalid orderBy field '{field_name}'. Allowed fields: {allowed_field_names}"
                raise ValidationException(detail=msg)
            return OrderByFilter(field_name=resolved_field, sort_order=sort_order or sort_order_default)

        filters[dep_defaults.ORDER_BY_FILTER_DEPENDENCY_KEY] = Provide(provide_order_by, sync_to_thread=False)

    if not_in_fields := config.get("not_in_fields"):
        not_in_fields = {not_in_fields} if isinstance(not_in_fields, (str, FieldNameType)) else not_in_fields

        for field_def in not_in_fields:
            resolved = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            filters[f"{resolved.name}_not_in_filter"] = Provide(
                _build_in_collection_provider(resolved, negated=True), sync_to_thread=False
            )

    if in_fields := config.get("in_fields"):
        in_fields = {in_fields} if isinstance(in_fields, (str, FieldNameType)) else in_fields

        for field_def in in_fields:
            resolved = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            filters[f"{resolved.name}_in_filter"] = Provide(
                _build_in_collection_provider(resolved, negated=False), sync_to_thread=False
            )

    if null_fields := config.get("null_fields"):
        null_fields = {null_fields} if isinstance(null_fields, str) else set(null_fields)
        for field_name in null_fields:
            filters[f"{field_name}_null_filter"] = Provide(
                _build_null_provider(field_name, negated=False), sync_to_thread=False
            )

    if not_null_fields := config.get("not_null_fields"):
        not_null_fields = {not_null_fields} if isinstance(not_null_fields, str) else set(not_null_fields)
        for field_name in not_null_fields:
            filters[f"{field_name}_not_null_filter"] = Provide(
                _build_null_provider(field_name, negated=True), sync_to_thread=False
            )

    if filters:
        filters[dep_defaults.FILTERS_DEPENDENCY_KEY] = Provide(
            _create_filter_aggregate_function(config), sync_to_thread=False
        )

    return filters


def _create_filter_aggregate_function(config: FilterConfig) -> Callable[..., list[FilterTypes]]:  # noqa: C901
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
                annotation=NotInCollectionFilter[field_def.type_hint],  # type: ignore
            )
            annotations[f"{field_def.name}_not_in_filter"] = NotInCollectionFilter[field_def.type_hint]  # type: ignore

    if in_fields := config.get("in_fields"):
        for field_def in in_fields:
            field_def = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
            parameters[f"{field_def.name}_in_filter"] = inspect.Parameter(
                name=f"{field_def.name}_in_filter",
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=Dependency(skip_validation=True),
                annotation=InCollectionFilter[field_def.type_hint],  # type: ignore
            )
            annotations[f"{field_def.name}_in_filter"] = InCollectionFilter[field_def.type_hint]  # type: ignore

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

    def provide_filters(**kwargs: FilterTypes) -> list[FilterTypes]:
        """Aggregate filter dependencies based on configuration.

        Args:
            **kwargs: Filter parameters dynamically provided based on configuration.

        Returns:
            List of configured filters.
        """
        filters: list[FilterTypes] = []
        if id_filter := kwargs.get("id_filter"):
            filters.append(id_filter)
        if created_filter := kwargs.get("created_filter"):
            filters.append(created_filter)
        if limit_offset := kwargs.get("limit_offset_filter"):
            filters.append(limit_offset)
        if updated_filter := kwargs.get("updated_filter"):
            filters.append(updated_filter)
        if (
            (search_filter := cast("SearchFilter | None", kwargs.get("search_filter")))
            and search_filter is not None  # pyright: ignore[reportUnnecessaryComparison]
            and search_filter.field_name is not None  # pyright: ignore[reportUnnecessaryComparison]
            and search_filter.value is not None  # pyright: ignore[reportUnnecessaryComparison]
        ):
            filters.append(search_filter)
        if (
            (order_by := cast("OrderByFilter | None", kwargs.get("order_by_filter")))
            and order_by is not None  # pyright: ignore[reportUnnecessaryComparison]
            and order_by.field_name is not None  # pyright: ignore[reportUnnecessaryComparison]
        ):
            filters.append(order_by)

        if not_in_fields := config.get("not_in_fields"):
            not_in_fields = {not_in_fields} if isinstance(not_in_fields, (str, FieldNameType)) else not_in_fields
            for field_def in not_in_fields:
                field_def = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
                filter_ = kwargs.get(f"{field_def.name}_not_in_filter")
                if filter_ is not None:
                    filters.append(filter_)

        if in_fields := config.get("in_fields"):
            in_fields = {in_fields} if isinstance(in_fields, (str, FieldNameType)) else in_fields
            for field_def in in_fields:
                field_def = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def
                filter_ = kwargs.get(f"{field_def.name}_in_filter")
                if filter_ is not None:
                    filters.append(filter_)

        if null_fields := config.get("null_fields"):
            null_fields = {null_fields} if isinstance(null_fields, str) else set(null_fields)
            for field_name in null_fields:
                filter_ = kwargs.get(f"{field_name}_null_filter")
                if filter_ is not None:
                    filters.append(filter_)

        if not_null_fields := config.get("not_null_fields"):
            not_null_fields = {not_null_fields} if isinstance(not_null_fields, str) else set(not_null_fields)
            for field_name in not_null_fields:
                filter_ = kwargs.get(f"{field_name}_not_null_filter")
                if filter_ is not None:
                    filters.append(filter_)

        return filters

    provide_filters.__signature__ = inspect.Signature(  # type: ignore
        parameters=list(parameters.values()), return_annotation=list[FilterTypes]
    )
    provide_filters.__annotations__ = annotations
    provide_filters.__annotations__["return"] = list[FilterTypes]

    return provide_filters
