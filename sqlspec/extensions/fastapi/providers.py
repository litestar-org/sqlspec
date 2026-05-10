"""Application dependency providers for FastAPI filter injection.

This module provides filter dependency injection for FastAPI routes, allowing
automatic parsing of query parameters into SQLSpec filter objects.
"""

import datetime
import inspect
from types import GenericAlias
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
from sqlspec.extensions._filter_aliases import resolve_sort_field_aliases
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
    "SortField",
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
    search: NotRequired[str | set[str]]
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
    null_fields: NotRequired[str | set[str]]
    """Field or fields that support ``IS NULL`` filtering."""
    not_null_fields: NotRequired[str | set[str]]
    """Field or fields that support ``IS NOT NULL`` filtering."""


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
    if not _has_filter_config(config):
        return _empty_filter_list

    cache_key = hash(_make_hashable(config))

    cached_dep = dep_cache.get_dependencies(cache_key)
    if cached_dep is not None:
        return cached_dep

    dep = _create_filter_aggregate_function(config, dep_defaults)
    dep_cache.add_dependencies(cache_key, dep)
    return dep


def _create_filter_aggregate_function(
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

    return _AggregateFilterProvider(params, annotations)


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


class _AggregateFilterProvider:
    def __init__(self, parameters: list[inspect.Parameter], annotations: dict[str, Any]) -> None:
        self.__signature__ = inspect.Signature(
            parameters=parameters, return_annotation=Annotated["list[FilterTypes]", self]
        )
        self.__annotations__ = annotations
        self.__annotations__["return"] = list[FilterTypes]

    def __call__(self, **kwargs: Any) -> list[FilterTypes]:
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
    def __init__(self, search_fields: str | set[str], ignore_case_default: bool) -> None:
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
        self.sort_resolution = resolve_sort_field_aliases(
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


def _add_dependency(params: list[inspect.Parameter], annotations: dict[str, Any], name: str, provider: Any) -> None:
    dependency_annotation = _query_parameter_annotation(provider.return_annotation, Depends(provider))
    params.append(inspect.Parameter(name=name, kind=inspect.Parameter.KEYWORD_ONLY, annotation=dependency_annotation))
    annotations[name] = dependency_annotation
