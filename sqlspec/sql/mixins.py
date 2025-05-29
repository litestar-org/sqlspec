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

from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import SQLConversionError, SQLSpecError
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig
from sqlspec.typing import (
    ConnectionT,
    ModelDTOT,
    ModelT,
    StatementParameterType,
    convert,
    get_type_adapter,
    is_dataclass,
    is_msgspec_struct,
    is_pydantic_model,
)

if TYPE_CHECKING:
    from sqlspec.sql.filters import StatementFilter
    from sqlspec.sql.result import ArrowResult

__all__ = (
    "AsyncArrowMixin",
    "AsyncParquetMixin",
    "ResultConverter",
    "SQLTranslatorMixin",
    "SyncArrowMixin",
    "SyncArrowMixin",
    "SyncParquetMixin",
)


class SQLTranslatorMixin(Generic[ConnectionT]):
    """Mixin for drivers supporting SQL translation."""

    dialect: str

    def convert_to_dialect(
        self,
        statement: "Statement",
        to_dialect: DialectType = None,
        pretty: bool = True,
    ) -> str:
        """Convert a SQL query to a different dialect.

        Args:
            statement: The SQL query string to convert.
            to_dialect: The target dialect to convert to.
            pretty: Whether to pretty-print the SQL query.

        Raises:
            SQLConversionError: If the SQL query cannot be converted to the target dialect.

        Returns:
            The converted SQL query string.
        """
        parsed_expression: exp.Expression
        if statement is not None and isinstance(statement, SQLStatement):
            if statement.expression is None:
                msg = "Statement could not be parsed"
                raise SQLConversionError(msg)
            parsed_expression = statement.expression
        elif isinstance(statement, exp.Expression):
            parsed_expression = statement
        else:
            parsed_expression = parse_one(statement, dialect=self.dialect)

        target_dialect = to_dialect if to_dialect is not None else self.dialect

        try:
            return parsed_expression.sql(dialect=target_dialect, pretty=pretty)
        except Exception as e:
            error_msg = f"Failed to convert SQL expression to {target_dialect}: {e!s}"
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


class SyncArrowMixin(Generic[ConnectionT]):
    """Optional mixin for sync drivers that support Apache Arrow data format.

    Provides methods to select data and return it in Arrow format for high-performance
    data interchange and analytics workflows.
    """

    def select_to_arrow(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results in Apache Arrow format.

        Args:
            statement: The SELECT statement to execute.
            parameters: Optional parameters for the statement.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            ArrowResult containing the query results in Arrow format.
        """
        msg = "Arrow support not implemented by this driver"
        raise NotImplementedError(msg)


class AsyncArrowMixin(Generic[ConnectionT]):
    """Optional mixin for async drivers that support Apache Arrow data format.

    Provides methods to select data and return it in Arrow format for high-performance
    data interchange and analytics workflows.
    """

    async def select_to_arrow(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results in Apache Arrow format.

        Args:
            statement: The SELECT statement to execute.
            parameters: Optional parameters for the statement.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            ArrowResult containing the query results in Arrow format.
        """
        msg = "Arrow support not implemented by this driver"
        raise NotImplementedError(msg)


class SyncParquetMixin(Generic[ConnectionT]):
    """Optional mixin for drivers that support Parquet file format operations.

    Provides methods to select data and export it directly to Parquet format,
    or to read from Parquet files into the database.
    """

    def to_parquet(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: "Any",
    ) -> "Union[bytes, None]":
        """Execute a SELECT statement and return/save results in Parquet format.

        Args:
            statement: The SELECT statement to execute.
            parameters: Optional parameters for the statement.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            Parquet data as bytes if file_path is None, otherwise None after saving.

        Note:
            This is an optional capability. Drivers that support Parquet should
            implement this method to provide efficient columnar data export.
        """
        msg = "Parquet support not implemented by this driver"
        raise NotImplementedError(msg)


class AsyncParquetMixin(Generic[ConnectionT]):
    """Optional mixin for drivers that support Parquet file format operations.

    Provides methods to select data and export it directly to Parquet format,
    or to read from Parquet files into the database.
    """

    async def to_parquet(
        self,
        statement: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: "Any",
    ) -> "Union[bytes, None]":
        """Execute a SELECT statement and return/save results in Parquet format.

        Args:
            statement: The SELECT statement to execute.
            parameters: Optional parameters for the statement.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            statement_config: Optional statement configuration.
            **kwargs: Additional keyword arguments.

        Returns:
            Parquet data as bytes if file_path is None, otherwise None after saving.

        Note:
            This is an optional capability. Drivers that support Parquet should
            implement this method to provide efficient columnar data export.
        """
        msg = "Parquet support not implemented by this driver"
        raise NotImplementedError(msg)
