# ruff: noqa: DOC202
import datetime
from collections.abc import Sequence
from enum import Enum
from functools import partial
from pathlib import Path, PurePath
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Optional,
    Union,
    cast,
    overload,
)
from uuid import UUID

from sqlglot import parse_one
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import SQLConversionError, SQLParsingError, SQLSpecError
from sqlspec.sql.statement import SQLStatement, Statement
from sqlspec.typing import (
    ConnectionT,
    ModelDTOT,
    ModelT,
    convert,
    get_type_adapter,
    is_dataclass,
    is_msgspec_struct,
    is_pydantic_model,
)

if TYPE_CHECKING:
    from sqlspec.sql.result import ArrowResult

__all__ = (
    "ArrowMixin",
    "ParquetMixin",
    "ResultConverter",
    "SQLTranslatorMixin",
)


class SQLTranslatorMixin(Generic[ConnectionT]):
    """Mixin for drivers supporting SQL translation."""

    dialect: str

    def convert_to_dialect(
        self,
        sql: "Statement",
        to_dialect: DialectType = None,
        pretty: bool = True,
    ) -> str:
        """Convert a SQL query to a different dialect.

        Args:
            sql: The SQL query string to convert.
            to_dialect: The target dialect to convert to.
            pretty: Whether to pretty-print the SQL query.

        Returns:
            The converted SQL query string.

        Raises:
            SQLParsingError: If the SQL query cannot be parsed.
            SQLConversionError: If the SQL query cannot be converted to the target dialect.
        """
        if isinstance(sql, str):
            try:
                parsed = parse_one(sql, dialect=self.dialect)
            except Exception as e:
                error_msg = f"Failed to parse SQL: {e!s}"
                raise SQLParsingError(error_msg) from e
        elif isinstance(sql, SQLStatement):
            parsed = sql.expression
        else:
            parsed = sql
        if to_dialect is None:
            to_dialect = self.dialect
        parsed = parsed.expression if isinstance(parsed, SQLStatement) else parsed
        try:
            return parsed.sql(dialect=to_dialect, pretty=pretty)
        except Exception as e:
            error_msg = f"Failed to convert SQL to {to_dialect}: {e!s}"
            raise SQLConversionError(error_msg) from e


_DEFAULT_TYPE_DECODERS = [  # pyright: ignore[reportUnknownVariableType]
    (lambda x: x is UUID, lambda t, v: t(v.hex)),  # pyright: ignore[reportUnknownLambdaType,reportUnknownMemberType]
    (lambda x: x is datetime.datetime, lambda t, v: t(v.isoformat())),  # pyright: ignore[reportUnknownLambdaType,reportUnknownMemberType]
    (lambda x: x is datetime.date, lambda t, v: t(v.isoformat())),  # pyright: ignore[reportUnknownLambdaType,reportUnknownMemberType]
    (lambda x: x is datetime.time, lambda t, v: t(v.isoformat())),  # pyright: ignore[reportUnknownLambdaType,reportUnknownMemberType]
    (lambda x: x is Enum, lambda t, v: t(v.value)),  # pyright: ignore[reportUnknownLambdaType,reportUnknownMemberType]
]


def _default_msgspec_deserializer(
    target_type: Any,
    value: Any,
    type_decoders: "Union[Sequence[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]], None]" = None,
) -> Any:  # pragma: no cover
    """Transform values non-natively supported by ``msgspec``

    Args:
        target_type: Encountered type
        value: Value to coerce
        type_decoders: Optional sequence of type decoders

    Raises:
        TypeError: If the value cannot be coerced to the target type

    Returns:
        A ``msgspec``-supported type
    """

    if isinstance(value, target_type):
        return value

    if type_decoders:
        for predicate, decoder in type_decoders:
            if predicate(target_type):
                return decoder(target_type, value)

    if issubclass(target_type, (Path, PurePath, UUID)):
        return target_type(value)

    try:
        return target_type(value)
    except Exception as e:
        msg = f"Unsupported type: {type(value)!r}"
        raise TypeError(msg) from e


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
    @staticmethod
    def to_schema(data: "ModelT", *, schema_type: None = None) -> "ModelT": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: "type[ModelDTOT]") -> "ModelDTOT": ...
    @overload
    @staticmethod
    def to_schema(data: "Sequence[ModelT]", *, schema_type: None = None) -> "Sequence[ModelT]": ...
    @overload
    @staticmethod
    def to_schema(data: "Sequence[dict[str, Any]]", *, schema_type: "type[ModelDTOT]") -> "Sequence[ModelDTOT]": ...

    @staticmethod
    def to_schema(
        data: "Union[ModelT, Sequence[ModelT], dict[str, Any], Sequence[dict[str, Any]], ModelDTOT, Sequence[ModelDTOT]]",
        *,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelT, Sequence[ModelT], ModelDTOT, Sequence[ModelDTOT]]":
        if schema_type is None:
            if not isinstance(data, Sequence):
                return cast("ModelT", data)
            return cast("Sequence[ModelT]", data)
        if is_dataclass(schema_type):
            if not isinstance(data, Sequence):
                # data is assumed to be dict[str, Any] as per the method's overloads
                return cast("ModelDTOT", schema_type(**data))  # type: ignore[operator]
            # data is assumed to be Sequence[dict[str, Any]]
            return cast("Sequence[ModelDTOT]", [schema_type(**item) for item in data])  # type: ignore[operator]
        if is_msgspec_struct(schema_type):
            if not isinstance(data, Sequence):
                return cast(
                    "ModelDTOT",
                    convert(
                        obj=data,
                        type=schema_type,
                        from_attributes=True,
                        dec_hook=partial(
                            _default_msgspec_deserializer,
                            type_decoders=_DEFAULT_TYPE_DECODERS,
                        ),
                    ),
                )
            return cast(
                "Sequence[ModelDTOT]",
                convert(
                    obj=data,
                    type=list[schema_type],  # type: ignore[valid-type]
                    from_attributes=True,
                    dec_hook=partial(
                        _default_msgspec_deserializer,
                        type_decoders=_DEFAULT_TYPE_DECODERS,
                    ),
                ),
            )

        if schema_type is not None and is_pydantic_model(schema_type):
            if not isinstance(data, Sequence):
                return cast(
                    "ModelDTOT",
                    get_type_adapter(schema_type).validate_python(data, from_attributes=True),  # pyright: ignore
                )
            return cast(
                "Sequence[ModelDTOT]",
                get_type_adapter(list[schema_type]).validate_python(data, from_attributes=True),  # type: ignore[valid-type] # pyright: ignore[reportUnknownArgumentType]
            )

        msg = "`schema_type` should be a valid Dataclass, Pydantic model or Msgspec struct"
        raise SQLSpecError(msg)


class ArrowMixin(Generic[ConnectionT]):
    """Optional mixin for drivers that support Apache Arrow data format.

    Provides methods to select data and return it in Arrow format for high-performance
    data interchange and analytics workflows.
    """

    def select_to_arrow(self, statement: "Statement", parameters: "Optional[dict[str, Any]]" = None) -> "ArrowResult":
        """Execute a SELECT statement and return results in Apache Arrow format.

        Args:
            statement: The SELECT statement to execute.
            parameters: Optional parameters for the statement.

        Returns:
            ArrowResult containing the query results in Arrow format.
        """
        msg = "Arrow support not implemented by this driver"
        raise NotImplementedError(msg)


class ParquetMixin(Generic[ConnectionT]):
    """Optional mixin for drivers that support Parquet file format operations.

    Provides methods to select data and export it directly to Parquet format,
    or to read from Parquet files into the database.
    """

    def select_to_parquet(
        self,
        statement: "Statement",
        parameters: "Optional[dict[str, Any]]" = None,
        file_path: "Optional[Union[str, Path]]" = None,
    ) -> "Union[bytes, None]":
        """Execute a SELECT statement and return/save results in Parquet format.

        Args:
            statement: The SELECT statement to execute.
            parameters: Optional parameters for the statement.
            file_path: Optional file path to save Parquet data. If None, returns bytes.

        Returns:
            Parquet data as bytes if file_path is None, otherwise None after saving.

        Note:
            This is an optional capability. Drivers that support Parquet should
            implement this method to provide efficient columnar data export.
        """
        msg = "Parquet support not implemented by this driver"
        raise NotImplementedError(msg)
