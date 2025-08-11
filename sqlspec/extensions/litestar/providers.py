# ruff: noqa: B008
"""Application dependency providers generators.

This module contains functions to create dependency providers for services and filters.
"""

import datetime
import inspect
from collections.abc import Callable
from typing import Any, Literal, NamedTuple, Optional, TypedDict, Union, cast
from uuid import UUID

from litestar.di import Provide
from litestar.params import Dependency, Parameter
from typing_extensions import NotRequired

from sqlspec.core.filters import (
    BeforeAfterFilter,
    FilterTypes,
    InCollectionFilter,
    LimitOffsetFilter,
    NotInCollectionFilter,
    OrderByFilter,
    SearchFilter,
)
from sqlspec.utils.singleton import SingletonMeta
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
    "SortOrder",
    "SortOrderOrNone",
    "StringOrNone",
    "UuidOrNone",
    "create_filter_dependencies",
    "dep_cache",
)

DTorNone = Optional[datetime.datetime]
StringOrNone = Optional[str]
UuidOrNone = Optional[UUID]
IntOrNone = Optional[int]
BooleanOrNone = Optional[bool]
SortOrder = Literal["asc", "desc"]
SortOrderOrNone = Optional[SortOrder]
HashableValue = Union[str, int, float, bool, None]
HashableType = Union[HashableValue, tuple[Any, ...], tuple[tuple[str, Any], ...], tuple[HashableValue, ...]]


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
    """Configuration for generating dynamic filters."""

    id_filter: NotRequired[type[Union[UUID, int, str]]]
    id_field: NotRequired[str]
    sort_field: NotRequired[str]
    sort_order: NotRequired[SortOrder]
    pagination_type: NotRequired[Literal["limit_offset"]]
    pagination_size: NotRequired[int]
    search: NotRequired[Union[str, set[str], list[str]]]
    search_ignore_case: NotRequired[bool]
    created_at: NotRequired[bool]
    updated_at: NotRequired[bool]
    not_in_fields: NotRequired[Union[FieldNameType, set[FieldNameType], list[Union[str, FieldNameType]]]]
    in_fields: NotRequired[Union[FieldNameType, set[FieldNameType], list[Union[str, FieldNameType]]]]


class DependencyCache(metaclass=SingletonMeta):
    """Dependency cache for memoizing dynamically generated dependencies."""

    def __init__(self) -> None:
        self.dependencies: dict[Union[int, str], dict[str, Provide]] = {}

    def add_dependencies(self, key: Union[int, str], dependencies: dict[str, Provide]) -> None:
        self.dependencies[key] = dependencies

    def get_dependencies(self, key: Union[int, str]) -> Optional[dict[str, Provide]]:
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
            ids: Optional[list[str]] = Parameter(query="ids", default=None, required=False),
        ) -> InCollectionFilter:  # pyright: ignore[reportMissingTypeArgument]
            return InCollectionFilter(field_name=config.get("id_field", "id"), values=ids)

        filters[dep_defaults.ID_FILTER_DEPENDENCY_KEY] = Provide(provide_id_filter, sync_to_thread=False)  # pyright: ignore[reportUnknownArgumentType]

    if config.get("created_at", False):

        def provide_created_filter(
            before: DTorNone = Parameter(query="createdBefore", default=None, required=False),
            after: DTorNone = Parameter(query="createdAfter", default=None, required=False),
        ) -> BeforeAfterFilter:
            return BeforeAfterFilter("created_at", before, after)

        filters[dep_defaults.CREATED_FILTER_DEPENDENCY_KEY] = Provide(provide_created_filter, sync_to_thread=False)

    if config.get("updated_at", False):

        def provide_updated_filter(
            before: DTorNone = Parameter(query="updatedBefore", default=None, required=False),
            after: DTorNone = Parameter(query="updatedAfter", default=None, required=False),
        ) -> BeforeAfterFilter:
            return BeforeAfterFilter("updated_at", before, after)

        filters[dep_defaults.UPDATED_FILTER_DEPENDENCY_KEY] = Provide(provide_updated_filter, sync_to_thread=False)

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
            field_names = set(search_fields.split(",")) if isinstance(search_fields, str) else set(search_fields)

            return SearchFilter(
                field_name=field_names,
                value=search_string,  # type: ignore[arg-type]
                ignore_case=ignore_case or False,
            )

        filters[dep_defaults.SEARCH_FILTER_DEPENDENCY_KEY] = Provide(provide_search_filter, sync_to_thread=False)

    if sort_field := config.get("sort_field"):

        def provide_order_by(
            field_name: StringOrNone = Parameter(
                title="Order by field", query="orderBy", default=sort_field, required=False
            ),
            sort_order: SortOrderOrNone = Parameter(
                title="Field to search", query="sortOrder", default=config.get("sort_order", "desc"), required=False
            ),
        ) -> OrderByFilter:
            return OrderByFilter(field_name=field_name, sort_order=sort_order)  # type: ignore[arg-type]

        filters[dep_defaults.ORDER_BY_FILTER_DEPENDENCY_KEY] = Provide(provide_order_by, sync_to_thread=False)

    if not_in_fields := config.get("not_in_fields"):
        not_in_fields = {not_in_fields} if isinstance(not_in_fields, (str, FieldNameType)) else not_in_fields

        for field_def in not_in_fields:
            field_def = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def

            def create_not_in_filter_provider(  # pyright: ignore
                field_name: FieldNameType,
            ) -> Callable[..., Optional[NotInCollectionFilter[field_def.type_hint]]]:  # type: ignore
                def provide_not_in_filter(  # pyright: ignore
                    values: Optional[list[field_name.type_hint]] = Parameter(  # type: ignore
                        query=camelize(f"{field_name.name}_not_in"), default=None, required=False
                    ),
                ) -> Optional[NotInCollectionFilter[field_name.type_hint]]:  # type: ignore
                    return (
                        NotInCollectionFilter[field_name.type_hint](field_name=field_name.name, values=values)  # type: ignore
                        if values
                        else None
                    )

                return provide_not_in_filter  # pyright: ignore

            provider = create_not_in_filter_provider(field_def)  # pyright: ignore
            filters[f"{field_def.name}_not_in_filter"] = Provide(provider, sync_to_thread=False)  # pyright: ignore

    if in_fields := config.get("in_fields"):
        in_fields = {in_fields} if isinstance(in_fields, (str, FieldNameType)) else in_fields

        for field_def in in_fields:
            field_def = FieldNameType(name=field_def, type_hint=str) if isinstance(field_def, str) else field_def

            def create_in_filter_provider(  # pyright: ignore
                field_name: FieldNameType,
            ) -> Callable[..., Optional[InCollectionFilter[field_def.type_hint]]]:  # type: ignore # pyright: ignore
                def provide_in_filter(  # pyright: ignore
                    values: Optional[list[field_name.type_hint]] = Parameter(  # type: ignore # pyright: ignore
                        query=camelize(f"{field_name.name}_in"), default=None, required=False
                    ),
                ) -> Optional[InCollectionFilter[field_name.type_hint]]:  # type: ignore # pyright: ignore
                    return (
                        InCollectionFilter[field_name.type_hint](field_name=field_name.name, values=values)  # type: ignore  # pyright: ignore
                        if values
                        else None
                    )

                return provide_in_filter  # pyright: ignore

            provider = create_in_filter_provider(field_def)  # type: ignore
            filters[f"{field_def.name}_in_filter"] = Provide(provider, sync_to_thread=False)  # pyright: ignore

    if filters:
        filters[dep_defaults.FILTERS_DEPENDENCY_KEY] = Provide(
            _create_filter_aggregate_function(config), sync_to_thread=False
        )

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
            (search_filter := cast("Optional[SearchFilter]", kwargs.get("search_filter")))
            and search_filter is not None  # pyright: ignore[reportUnnecessaryComparison]
            and search_filter.field_name is not None  # pyright: ignore[reportUnnecessaryComparison]
            and search_filter.value is not None  # pyright: ignore[reportUnnecessaryComparison]
        ):
            filters.append(search_filter)
        if (
            (order_by := cast("Optional[OrderByFilter]", kwargs.get("order_by_filter")))
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
        return filters

    provide_filters.__signature__ = inspect.Signature(  # type: ignore
        parameters=list(parameters.values()), return_annotation=list[FilterTypes]
    )
    provide_filters.__annotations__ = annotations
    provide_filters.__annotations__["return"] = list[FilterTypes]

    return provide_filters
