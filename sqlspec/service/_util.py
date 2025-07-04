from collections.abc import Sequence
from functools import partial
from typing import Any, Optional, TypeVar, Union, cast, overload

from sqlspec.driver.mixins._result_utils import _DEFAULT_TYPE_DECODERS, _default_msgspec_deserializer
from sqlspec.exceptions import SQLSpecError
from sqlspec.service.pagination import OffsetPagination
from sqlspec.statement.filters import FilterTypeT, LimitOffsetFilter, StatementFilter
from sqlspec.typing import BaseModel, DataclassProtocol, ModelDTOT, ModelT, Struct, convert, get_type_adapter
from sqlspec.utils.type_guards import is_dataclass, is_msgspec_struct, is_pydantic_model

__all__ = ("ResultConverter", "find_filter")


T = TypeVar("T")


def find_filter(
    filter_type: "type[FilterTypeT]", filters: "Optional[Sequence[StatementFilter]]" = None
) -> "Optional[FilterTypeT]":
    """Get the filter specified by filter type from the filters.

    Args:
        filter_type: The type of filter to find.
        filters: filter types to apply to the query

    Returns:
        The match filter instance or None
    """
    if filters is None:
        return None
    return next(
        (cast("Optional[FilterTypeT]", filter_) for filter_ in filters if isinstance(filter_, filter_type)), None
    )


# TODO: add overloads for each type of pagination in the future
class ResultConverter:
    """Simple mixin to help convert to dictionary or list of dictionaries to specified schema type.

    Single objects are transformed to the supplied schema type, and lists of objects are transformed into a list of the supplied schema type.

    Args:
        data: A database model instance or row mapping.
              Type: :class:`~sqlspec.typing.ModelDictT`

    Returns:
        The converted schema object.
    """

    @overload
    def to_schema(
        self,
        data: "ModelT",
        total: "int | None" = None,
        filters: "Sequence[StatementFilter] | None" = None,
        *,
        schema_type: None = None,
    ) -> "ModelT": ...
    @overload
    def to_schema(
        self,
        data: "dict[str, Any] | Struct | BaseModel | DataclassProtocol",
        total: "int | None" = None,
        filters: "Sequence[StatementFilter] | None" = None,
        *,
        schema_type: "type[ModelDTOT]",
    ) -> "ModelDTOT": ...
    @overload
    def to_schema(
        self,
        data: "Sequence[ModelT]",
        total: "int | None" = None,
        filters: "Sequence[StatementFilter] | None" = None,
        *,
        schema_type: None = None,
    ) -> "OffsetPagination[ModelT]": ...
    @overload
    def to_schema(
        self,
        data: "Sequence[dict[str, Any] | Struct | BaseModel | DataclassProtocol]",
        total: "int | None" = None,
        filters: "Sequence[StatementFilter] | None" = None,
        *,
        schema_type: "type[ModelDTOT]",
    ) -> "OffsetPagination[ModelDTOT]": ...
    def to_schema(
        self,
        data: "ModelT | Sequence[ModelT] | dict[str, Any] | Struct | BaseModel | DataclassProtocol | Sequence[dict[str, Any] | Struct | BaseModel | DataclassProtocol]",
        total: "int | None" = None,
        filters: "Sequence[StatementFilter] | None" = None,
        *,
        schema_type: "type[ModelDTOT] | None" = None,
    ) -> "Union[ModelT,   ModelDTOT ,  OffsetPagination[ModelT] , OffsetPagination[ModelDTOT]]":
        if not isinstance(data, Sequence):
            if schema_type is None:
                return cast("ModelT", data)
            if is_dataclass(schema_type):
                return cast("ModelDTOT", schema_type(**data))  # type: ignore[operator]
            if is_msgspec_struct(schema_type):
                return cast(
                    "ModelDTOT",
                    convert(
                        obj=data,
                        type=schema_type,
                        from_attributes=True,
                        dec_hook=partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS),
                    ),
                )
            if is_pydantic_model(schema_type):  # pyright: ignore
                return cast(
                    "ModelDTOT",
                    get_type_adapter(schema_type).validate_python(data, from_attributes=True),  # pyright: ignore
                )
        assert isinstance(data, Sequence)
        limit_offset = find_filter(LimitOffsetFilter, filters=filters)
        if schema_type is None:
            return OffsetPagination[ModelT](
                items=cast("list[ModelT]", data),
                limit=limit_offset.limit if limit_offset else len(data),
                offset=limit_offset.offset if limit_offset else 0,
                total=total if total is not None else len(data),
            )
        converted_items: Sequence[ModelDTOT]
        if is_dataclass(schema_type):
            converted_items = [schema_type(**item) for item in data]  # type: ignore[operator]
        elif is_msgspec_struct(schema_type):
            converted_items = convert(
                obj=data,
                type=list[schema_type],  # type: ignore[valid-type]
                from_attributes=True,
                dec_hook=partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS),
            )
        elif is_pydantic_model(schema_type):  # pyright: ignore
            converted_items = get_type_adapter(list[schema_type]).validate_python(data, from_attributes=True)  # type: ignore[valid-type] # pyright: ignore[reportUnknownArgumentType]
        else:
            # This will also catch the case where a single item had an unrecognized schema_type
            # if it somehow bypassed the initial single-item checks.
            msg = "`schema_type` should be a valid Dataclass, Pydantic model or Msgspec struct"
            raise SQLSpecError(msg)

        return OffsetPagination[ModelDTOT](
            items=cast("list[ModelDTOT]", converted_items),
            limit=limit_offset.limit if limit_offset else len(data),
            offset=limit_offset.offset if limit_offset else 0,
            total=total if total is not None else len(data),
        )
