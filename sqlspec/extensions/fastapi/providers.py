"""Application dependency providers for FastAPI filter injection.

This module provides filter dependency injection for FastAPI routes, allowing
automatic parsing of query parameters into SQLSpec filter objects.
"""

import datetime
import inspect
from typing import TYPE_CHECKING, Annotated, Any, Literal, NamedTuple
from uuid import UUID

from fastapi import Depends, Query
from fastapi.exceptions import RequestValidationError
from typing_extensions import NotRequired, TypedDict

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
from sqlspec.utils.singleton import SingletonMeta
from sqlspec.utils.text import camelize

if TYPE_CHECKING:
    from collections.abc import Callable

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
    "dep_cache",
    "provide_filters",
)

DTorNone = datetime.datetime | None
StringOrNone = str | None
UuidOrNone = UUID | None
IntOrNone = int | None
BooleanOrNone = bool | None
SortOrder = Literal["asc", "desc"]
SortOrderOrNone = SortOrder | None
HashableValue = str | int | float | bool | None
HashableType = HashableValue | tuple[Any, ...] | tuple[tuple[str, Any], ...] | tuple[HashableValue, ...]


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


class FilterConfig(TypedDict):
    """Configuration for generating dynamic filters for FastAPI."""

    id_filter: NotRequired[type[UUID | int | str]]
    """Type of ID filter to enable (UUID, int, or str). When set, enables collection filtering by IDs."""
    id_field: NotRequired[str]
    """Field name for ID filtering. Defaults to 'id'."""
    sort_field: NotRequired[str | set[str]]
    """Default field(s) to use for sorting."""
    sort_order: NotRequired[SortOrder]
    """Default sort order ('asc' or 'desc'). Defaults to 'desc'."""
    pagination_type: NotRequired[Literal["limit_offset"]]
    """When set to 'limit_offset', enables pagination with page size and current page parameters."""
    pagination_size: NotRequired[int]
    """Default pagination page size. Defaults to DEFAULT_PAGINATION_SIZE (20)."""
    search: NotRequired[str | set[str]]
    """Field(s) to enable search filtering on. Can be comma-separated string or set of field names."""
    search_ignore_case: NotRequired[bool]
    """When True, search is case-insensitive. Defaults to False."""
    created_at: NotRequired[bool]
    """When True, enables created_at date range filtering. Uses 'created_at' field by default."""
    updated_at: NotRequired[bool]
    """When True, enables updated_at date range filtering. Uses 'updated_at' field by default."""
    not_in_fields: NotRequired[FieldNameType | set[FieldNameType]]
    """Fields that support not-in collection filtering. Can be single field or set of fields with type info."""
    in_fields: NotRequired[FieldNameType | set[FieldNameType]]
    """Fields that support in-collection filtering. Can be single field or set of fields with type info."""
    null_fields: NotRequired[str | set[str]]
    """Fields that support IS NULL filtering. Can be single field name or set of field names."""
    not_null_fields: NotRequired[str | set[str]]
    """Fields that support IS NOT NULL filtering. Can be single field name or set of field names."""


class DependencyCache(metaclass=SingletonMeta):
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


def _empty_filter_list() -> "list[FilterTypes]":
    return []


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

    Example:
        from fastapi import Depends, FastAPI
        from sqlspec.extensions.fastapi import SQLSpecPlugin, FilterConfig

        app = FastAPI()
        db_ext = SQLSpecPlugin(sql, app)

        @app.get("/users")
        async def list_users(
            filters = Depends(
                db_ext.provide_filters({
                    "id_filter": UUID,
                    "search": "name,email",
                    "pagination_type": "limit_offset",
                })
            ),
        ):
            stmt = sql("SELECT * FROM users")
            for filter in filters:
                stmt = filter.append_to_statement(stmt)
            result = await db.execute(stmt)
            return result.all()
    """
    filter_keys = {
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
    }

    has_filters = False
    for key in filter_keys:
        value = config.get(key)
        if value is not None and value is not False and value != []:
            has_filters = True
            break

    if not has_filters:
        return _empty_filter_list

    cache_key = hash(_make_hashable(config))

    cached_dep = dep_cache.get_dependencies(cache_key)
    if cached_dep is not None:
        return cached_dep

    dep = _create_filter_aggregate_function_fastapi(config, dep_defaults)
    dep_cache.add_dependencies(cache_key, dep)
    return dep


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


def _create_filter_aggregate_function_fastapi(  # noqa: C901
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

    if config.get("id_filter", False) is not False:

        def provide_id_filter(
            ids: Annotated[list[Any] | None, Query(alias="ids", description="IDs to filter by.")] = None,
        ) -> InCollectionFilter[Any] | None:
            return InCollectionFilter(field_name=config.get("id_field", "id"), values=ids) if ids else None

        params.append(
            inspect.Parameter(
                name=dep_defaults.ID_FILTER_DEPENDENCY_KEY,
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=Annotated["InCollectionFilter[Any] | None", Depends(provide_id_filter)],
            )
        )
        annotations[dep_defaults.ID_FILTER_DEPENDENCY_KEY] = Annotated[
            "InCollectionFilter[Any] | None", Depends(provide_id_filter)
        ]

    if config.get("created_at", False):

        def provide_created_at_filter(
            before: Annotated[
                str | None,
                Query(
                    alias="createdBefore",
                    description="Filter by created date before this timestamp.",
                    json_schema_extra={"format": "date-time"},
                ),
            ] = None,
            after: Annotated[
                str | None,
                Query(
                    alias="createdAfter",
                    description="Filter by created date after this timestamp.",
                    json_schema_extra={"format": "date-time"},
                ),
            ] = None,
        ) -> "BeforeAfterFilter | None":
            before_dt = None
            after_dt = None

            if before is not None:
                try:
                    before_dt = datetime.datetime.fromisoformat(before.replace("Z", "+00:00"))
                except (ValueError, TypeError, AttributeError):
                    msg = "Invalid date format for createdBefore"
                    raise RequestValidationError(
                        errors=[{"loc": ("query", "createdBefore"), "msg": msg, "type": "value_error.datetime"}]
                    )

            if after is not None:
                try:
                    after_dt = datetime.datetime.fromisoformat(after.replace("Z", "+00:00"))
                except (ValueError, TypeError, AttributeError):
                    msg = "Invalid date format for createdAfter"
                    raise RequestValidationError(
                        errors=[{"loc": ("query", "createdAfter"), "msg": msg, "type": "value_error.datetime"}]
                    )

            return (
                BeforeAfterFilter(field_name="created_at", before=before_dt, after=after_dt)
                if before_dt or after_dt
                else None
            )

        param_name = dep_defaults.CREATED_FILTER_DEPENDENCY_KEY
        params.append(
            inspect.Parameter(
                name=param_name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=Annotated["BeforeAfterFilter | None", Depends(provide_created_at_filter)],
            )
        )
        annotations[param_name] = Annotated["BeforeAfterFilter | None", Depends(provide_created_at_filter)]

    if config.get("updated_at", False):

        def provide_updated_at_filter(
            before: Annotated[
                str | None,
                Query(
                    alias="updatedBefore",
                    description="Filter by updated date before this timestamp.",
                    json_schema_extra={"format": "date-time"},
                ),
            ] = None,
            after: Annotated[
                str | None,
                Query(
                    alias="updatedAfter",
                    description="Filter by updated date after this timestamp.",
                    json_schema_extra={"format": "date-time"},
                ),
            ] = None,
        ) -> "BeforeAfterFilter | None":
            before_dt = None
            after_dt = None

            if before is not None:
                try:
                    before_dt = datetime.datetime.fromisoformat(before.replace("Z", "+00:00"))
                except (ValueError, TypeError, AttributeError):
                    msg = "Invalid date format for updatedBefore"
                    raise RequestValidationError(
                        errors=[{"loc": ("query", "updatedBefore"), "msg": msg, "type": "value_error.datetime"}]
                    )

            if after is not None:
                try:
                    after_dt = datetime.datetime.fromisoformat(after.replace("Z", "+00:00"))
                except (ValueError, TypeError, AttributeError):
                    msg = "Invalid date format for updatedAfter"
                    raise RequestValidationError(
                        errors=[{"loc": ("query", "updatedAfter"), "msg": msg, "type": "value_error.datetime"}]
                    )

            return (
                BeforeAfterFilter(field_name="updated_at", before=before_dt, after=after_dt)
                if before_dt or after_dt
                else None
            )

        param_name = dep_defaults.UPDATED_FILTER_DEPENDENCY_KEY
        params.append(
            inspect.Parameter(
                name=param_name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=Annotated["BeforeAfterFilter | None", Depends(provide_updated_at_filter)],
            )
        )
        annotations[param_name] = Annotated["BeforeAfterFilter | None", Depends(provide_updated_at_filter)]

    if config.get("pagination_type") == "limit_offset":

        def provide_limit_offset_pagination(
            current_page: Annotated[
                int, Query(ge=1, alias="currentPage", description="Page number for pagination.")
            ] = 1,
            page_size: Annotated[
                int, Query(ge=1, alias="pageSize", description="Number of items per page.")
            ] = config.get("pagination_size", dep_defaults.DEFAULT_PAGINATION_SIZE),
        ) -> LimitOffsetFilter:
            return LimitOffsetFilter(limit=page_size, offset=page_size * (current_page - 1))

        param_name = dep_defaults.LIMIT_OFFSET_FILTER_DEPENDENCY_KEY
        params.append(
            inspect.Parameter(
                name=param_name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=Annotated[LimitOffsetFilter, Depends(provide_limit_offset_pagination)],
            )
        )
        annotations[param_name] = Annotated[LimitOffsetFilter, Depends(provide_limit_offset_pagination)]

    if search_fields := config.get("search"):

        def provide_search_filter(
            search_string: Annotated[str | None, Query(alias="searchString", description="Search term.")] = None,
            ignore_case: Annotated[
                bool | None, Query(alias="searchIgnoreCase", description="Whether search should be case-insensitive.")
            ] = config.get("search_ignore_case", False),
        ) -> "SearchFilter | None":
            field_names = set(search_fields.split(",")) if isinstance(search_fields, str) else search_fields

            return (
                SearchFilter(field_name=field_names, value=search_string, ignore_case=ignore_case or False)
                if search_string
                else None
            )

        param_name = dep_defaults.SEARCH_FILTER_DEPENDENCY_KEY
        params.append(
            inspect.Parameter(
                name=param_name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=Annotated["SearchFilter | None", Depends(provide_search_filter)],
            )
        )
        annotations[param_name] = Annotated["SearchFilter | None", Depends(provide_search_filter)]

    if sort_field := config.get("sort_field"):
        sort_order_default = config.get("sort_order", "desc")
        default_field = sort_field if isinstance(sort_field, str) else next(iter(sort_field))

        def provide_order_by(
            field_name: Annotated[str, Query(alias="orderBy", description="Field to order by.")] = default_field,
            sort_order: Annotated[
                SortOrder | None, Query(alias="sortOrder", description="Sort order ('asc' or 'desc').")
            ] = sort_order_default,
        ) -> OrderByFilter:
            return OrderByFilter(field_name=field_name, sort_order=sort_order or sort_order_default)

        param_name = dep_defaults.ORDER_BY_FILTER_DEPENDENCY_KEY
        params.append(
            inspect.Parameter(
                name=param_name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=Annotated[OrderByFilter, Depends(provide_order_by)],
            )
        )
        annotations[param_name] = Annotated[OrderByFilter, Depends(provide_order_by)]

    if not_in_fields := config.get("not_in_fields"):
        not_in_fields = {not_in_fields} if isinstance(not_in_fields, (str, FieldNameType)) else not_in_fields
        for field_def in not_in_fields:

            def create_not_in_filter_provider(
                field_name: FieldNameType = field_def,
            ) -> "Callable[..., NotInCollectionFilter[Any] | None]":
                def provide_not_in_filter(
                    values: Annotated[
                        set[Any] | None,
                        Query(
                            alias=camelize(f"{field_name.name}_not_in"),
                            description=f"Filter {field_name.name} not in values",
                        ),
                    ] = None,
                ) -> "NotInCollectionFilter[Any] | None":
                    return NotInCollectionFilter(field_name=field_name.name, values=values) if values else None

                return provide_not_in_filter

            not_in_provider = create_not_in_filter_provider()
            param_name = f"{field_def.name}_not_in_filter"
            params.append(
                inspect.Parameter(
                    name=param_name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    annotation=Annotated["NotInCollectionFilter[Any] | None", Depends(not_in_provider)],
                )
            )
            annotations[param_name] = Annotated["NotInCollectionFilter[Any] | None", Depends(not_in_provider)]

    if in_fields := config.get("in_fields"):
        in_fields = {in_fields} if isinstance(in_fields, (str, FieldNameType)) else in_fields
        for field_def in in_fields:

            def create_in_filter_provider(
                field_name: FieldNameType = field_def,
            ) -> "Callable[..., InCollectionFilter[Any] | None]":
                def provide_in_filter(
                    values: Annotated[
                        set[Any] | None,
                        Query(
                            alias=camelize(f"{field_name.name}_in"), description=f"Filter {field_name.name} in values"
                        ),
                    ] = None,
                ) -> "InCollectionFilter[Any] | None":
                    return InCollectionFilter(field_name=field_name.name, values=values) if values else None

                return provide_in_filter

            in_provider = create_in_filter_provider()
            param_name = f"{field_def.name}_in_filter"
            params.append(
                inspect.Parameter(
                    name=param_name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    annotation=Annotated["InCollectionFilter[Any] | None", Depends(in_provider)],
                )
            )
            annotations[param_name] = Annotated["InCollectionFilter[Any] | None", Depends(in_provider)]

    if null_fields := config.get("null_fields"):
        null_fields = {null_fields} if isinstance(null_fields, str) else null_fields
        for field_name in null_fields:

            def create_null_filter_provider(fname: str = field_name) -> "Callable[..., NullFilter | None]":
                def provide_null_filter(
                    is_null: Annotated[
                        bool | None,
                        Query(alias=camelize(f"{fname}_is_null"), description=f"Filter where {fname} IS NULL"),
                    ] = None,
                ) -> "NullFilter | None":
                    return NullFilter(field_name=fname) if is_null else None

                return provide_null_filter

            null_provider = create_null_filter_provider()
            param_name = f"{field_name}_null_filter"
            params.append(
                inspect.Parameter(
                    name=param_name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    annotation=Annotated["NullFilter | None", Depends(null_provider)],
                )
            )
            annotations[param_name] = Annotated["NullFilter | None", Depends(null_provider)]

    if not_null_fields := config.get("not_null_fields"):
        not_null_fields = {not_null_fields} if isinstance(not_null_fields, str) else not_null_fields
        for field_name in not_null_fields:

            def create_not_null_filter_provider(fname: str = field_name) -> "Callable[..., NotNullFilter | None]":
                def provide_not_null_filter(
                    is_not_null: Annotated[
                        bool | None,
                        Query(alias=camelize(f"{fname}_is_not_null"), description=f"Filter where {fname} IS NOT NULL"),
                    ] = None,
                ) -> "NotNullFilter | None":
                    return NotNullFilter(field_name=fname) if is_not_null else None

                return provide_not_null_filter

            not_null_provider = create_not_null_filter_provider()
            param_name = f"{field_name}_not_null_filter"
            params.append(
                inspect.Parameter(
                    name=param_name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    annotation=Annotated["NotNullFilter | None", Depends(not_null_provider)],
                )
            )
            annotations[param_name] = Annotated["NotNullFilter | None", Depends(not_null_provider)]

    _aggregate_filter_function.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        parameters=params, return_annotation=Annotated["list[FilterTypes]", _aggregate_filter_function]
    )

    return _aggregate_filter_function


def _aggregate_filter_function(**kwargs: Any) -> "list[FilterTypes]":
    """Aggregate filter dependencies based on configuration.

    Args:
        **kwargs: Filter parameters dynamically provided based on configuration.

    Returns:
        List of configured filters.
    """
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
