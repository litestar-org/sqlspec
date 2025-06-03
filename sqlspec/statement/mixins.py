# Test comment for re-linting
import datetime
from abc import ABC, abstractmethod
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
    Protocol,
    Union,
    cast,
    overload,
)
from uuid import UUID

from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import SQLConversionError, SQLSpecError
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import (
    ConnectionT,
    Counter,
    Histogram,
    ModelDTOT,
    ModelT,
    SQLParameterType,
    Tracer,
    convert,
    get_type_adapter,
    is_dataclass,
    is_msgspec_struct,
    is_pydantic_model,
)
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

if TYPE_CHECKING:
    from sqlspec.config import InstrumentationConfig
    from sqlspec.statement.builder import QueryBuilder
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.result import ArrowResult

__all__ = (
    "AsyncArrowMixin",
    "AsyncParquetMixin",
    "ResultConverter",
    "SQLTranslatorMixin",
    "SyncArrowMixin",
    "SyncParquetMixin",
)


class ExporterMixinProtocol(Protocol[ConnectionT]):
    dialect: str
    config: SQLConfig
    instrumentation_config: "InstrumentationConfig"
    _tracer: "Optional[Tracer]"
    _query_counter: "Optional[Counter]"
    _error_counter: "Optional[Counter]"
    _latency_histogram: "Optional[Histogram]"

    def _build_statement(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        filters: "Optional[list[StatementFilter]]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL": ...

    def _connection(self, connection: "Optional[ConnectionT]" = None) -> "ConnectionT": ...

    @staticmethod
    def returns_rows(expression: "Optional[exp.Expression]") -> bool: ...


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
        if statement is not None and isinstance(statement, SQL):
            if statement.expression is None:
                msg = "Statement could not be parsed"
                raise SQLConversionError(msg)
            parsed_expression = statement.expression
        elif isinstance(statement, exp.Expression):
            parsed_expression = statement
        else:
            try:
                parsed_expression = parse_one(statement, dialect=self.dialect)
            except Exception as e:
                error_msg = f"Failed to parse SQL statement: {e!s}"
                raise SQLConversionError(error_msg) from e

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

    if type_decoders:
        for predicate, decoder in type_decoders:
            if predicate(target_type):
                return decoder(target_type, value)

    # Handle built-in type decoders
    if target_type is UUID and isinstance(value, UUID):
        return value.hex
    if target_type in {datetime.datetime, datetime.date, datetime.time} and hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(target_type, type) and issubclass(target_type, Enum) and isinstance(value, Enum):
        return value.value
    if isinstance(value, target_type):
        return value
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


class SyncArrowMixin(ExporterMixinProtocol[ConnectionT], ABC, Generic[ConnectionT]):
    """Optional mixin for sync drivers that support Apache Arrow data format.

    Provides methods to select data and return it in Arrow format for high-performance
    data interchange and analytics workflows.
    """

    @abstractmethod
    def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "ConnectionT",
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Actual implementation for fetching Arrow data. To be implemented by drivers."""
        msg = "Arrow support's _select_to_arrow_impl not implemented by this driver"
        raise NotImplementedError(msg)

    def select_to_arrow(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results in Apache Arrow format.

        Args:
            statement: The SELECT statement or builder to execute.
            parameters: Optional parameters for the statement.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
            **kwargs: Additional keyword arguments for the driver's implementation.

        Raises:
            TypeError: If the driver does not support Arrow.

        Returns:
            ArrowResult containing the query results in Arrow format.
        """
        if (
            not hasattr(self, "_build_statement")
            or not hasattr(self, "_connection")
            or not hasattr(self, "returns_rows")
            or not hasattr(self, "config")
            or not hasattr(self, "dialect")
        ):
            msg = "SyncArrowMixin used with a class missing required DriverAdapter attributes/methods."
            raise TypeError(msg)

        with instrument_operation(self, "select_to_arrow", "database"):
            # The 'self.config' refers to the default config on the driver.
            # 'config' param is an override.
            stmt_obj = self._build_statement(
                statement, parameters=parameters, filters=list(filters), config=config or self.config
            )
            # Validation is typically handled by _build_statement via SQLConfig
            # or can be called explicitly if needed: stmt_obj.validate()

            if not self.returns_rows(stmt_obj.expression):
                op_type = (
                    str(stmt_obj.expression.key).upper()
                    if stmt_obj.expression and hasattr(stmt_obj.expression, "key")
                    else "UNKNOWN"
                )
                msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)

            conn_to_use = self._connection(connection)
            return self._select_to_arrow_impl(stmt_obj, conn_to_use, **kwargs)


class AsyncArrowMixin(ExporterMixinProtocol[ConnectionT], ABC, Generic[ConnectionT]):
    """Optional mixin for async drivers that support Apache Arrow data format.

    Provides methods to select data and return it in Arrow format for high-performance
    data interchange and analytics workflows.
    """

    @abstractmethod
    async def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",
        connection: "ConnectionT",
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Actual implementation for fetching Arrow data. To be implemented by drivers."""
        msg = "Arrow support's _select_to_arrow_impl not implemented by this driver"
        raise NotImplementedError(msg)

    async def select_to_arrow(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",
    ) -> "ArrowResult":
        """Execute a SELECT statement and return results in Apache Arrow format.

        Args:
            statement: The SELECT statement or builder to execute.
            parameters: Optional parameters for the statement.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
            **kwargs: Additional keyword arguments for the driver's implementation.


        Raises:
            TypeError: If the driver does not support Arrow.

        Returns:
            ArrowResult containing the query results in Arrow format.
        """
        if (
            not hasattr(self, "_build_statement")
            or not hasattr(self, "_connection")
            or not hasattr(self, "returns_rows")
            or not hasattr(self, "config")
            or not hasattr(self, "dialect")
        ):
            msg = "AsyncArrowMixin used with a class missing required DriverAdapter attributes/methods."
            raise TypeError(msg)

        async with instrument_operation_async(self, "select_to_arrow", "database"):
            stmt_obj = self._build_statement(
                statement, parameters=parameters, filters=list(filters), config=config or self.config
            )

            if not self.returns_rows(stmt_obj.expression):
                op_type = (
                    str(stmt_obj.expression.key).upper()
                    if stmt_obj.expression and hasattr(stmt_obj.expression, "key")
                    else "UNKNOWN"
                )
                msg = f"Cannot fetch Arrow table for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)

            conn_to_use = self._connection(connection)
            return await self._select_to_arrow_impl(stmt_obj, conn_to_use, **kwargs)


class SyncParquetMixin(ExporterMixinProtocol[ConnectionT], ABC, Generic[ConnectionT]):
    """Optional mixin for drivers that support Parquet file format operations.

    Provides methods to select data and export it directly to Parquet format,
    or to read from Parquet files into the database.
    """

    @abstractmethod
    def _to_parquet_impl(
        self,
        stmt_obj: "SQL",
        connection: "ConnectionT",
        **kwargs: "Any",  # e.g., file_path
    ) -> "Union[bytes, None]":
        """Actual implementation for exporting to Parquet. To be implemented by drivers."""
        msg = "Parquet support's _to_parquet_impl not implemented by this driver"
        raise NotImplementedError(msg)

    def to_parquet(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",  # e.g., file_path for saving, or options for pyarrow.parquet.write_table
    ) -> "Union[bytes, None]":
        """Execute a SELECT statement and return/save results in Parquet format.

        Args:
            statement: The SELECT statement or builder to execute.
            parameters: Optional parameters for the statement.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
            **kwargs: Additional keyword arguments, such as `file_path` to save the
                      Parquet data to a file. If `file_path` is not provided,
                      the Parquet data may be returned as bytes.

        Raises:
            TypeError: If the driver does not support Parquet.

        Returns:
            Parquet data as bytes if `file_path` is not in `kwargs`, otherwise None after saving.
        """
        if (
            not hasattr(self, "_build_statement")
            or not hasattr(self, "_connection")
            or not hasattr(self, "returns_rows")
            or not hasattr(self, "config")
            or not hasattr(self, "dialect")
        ):
            msg = "SyncParquetMixin used with a class missing required DriverAdapter attributes/methods."
            raise TypeError(msg)

        with instrument_operation(self, "to_parquet", "database"):
            stmt_obj = self._build_statement(
                statement, parameters=parameters, filters=list(filters), config=config or self.config
            )

            if not self.returns_rows(stmt_obj.expression):
                op_type = (
                    str(stmt_obj.expression.key).upper()
                    if stmt_obj.expression and hasattr(stmt_obj.expression, "key")
                    else "UNKNOWN"
                )
                msg = f"Cannot export Parquet for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)

            conn_to_use = self._connection(connection)
            return self._to_parquet_impl(stmt_obj, conn_to_use, **kwargs)


class AsyncParquetMixin(ExporterMixinProtocol[ConnectionT], ABC, Generic[ConnectionT]):
    """Optional mixin for drivers that support Parquet file format operations.

    Provides methods to select data and export it directly to Parquet format,
    or to read from Parquet files into the database.
    """

    @abstractmethod
    async def _to_parquet_impl(
        self,
        stmt_obj: "SQL",
        connection: "ConnectionT",
        **kwargs: "Any",  # e.g., file_path
    ) -> "Union[bytes, None]":
        """Actual implementation for exporting to Parquet. To be implemented by drivers."""
        msg = "Parquet support's _to_parquet_impl not implemented by this driver"
        raise NotImplementedError(msg)

    async def to_parquet(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: "Any",  # e.g., file_path
    ) -> "Union[bytes, None]":
        """Execute a SELECT statement and return/save results in Parquet format.

        Args:
            statement: The SELECT statement or builder to execute.
            parameters: Optional parameters for the statement.
            *filters: Statement filters to apply.
            connection: Optional connection override.
            config: Optional statement configuration.
            **kwargs: Additional keyword arguments, such as `file_path` to save the
                      Parquet data to a file. If `file_path` is not provided,
                      the Parquet data may be returned as bytes.

        Raises:
            TypeError: If the driver does not support Parquet.

        Returns:
            Parquet data as bytes if `file_path` is not in `kwargs`, otherwise None after saving.
        """
        if (
            not hasattr(self, "_build_statement")
            or not hasattr(self, "_connection")
            or not hasattr(self, "returns_rows")
            or not hasattr(self, "config")
            or not hasattr(self, "dialect")
        ):
            msg = "AsyncParquetMixin used with a class missing required DriverAdapter attributes/methods."
            raise TypeError(msg)

        async with instrument_operation_async(self, "to_parquet", "database"):
            stmt_obj = self._build_statement(
                statement, parameters=parameters, filters=list(filters), config=config or self.config
            )

            if not self.returns_rows(stmt_obj.expression):
                op_type = (
                    str(stmt_obj.expression.key).upper()
                    if stmt_obj.expression and hasattr(stmt_obj.expression, "key")
                    else "UNKNOWN"
                )
                msg = f"Cannot export Parquet for a non-query statement. Command type: {op_type}"
                raise TypeError(msg)

            conn_to_use = self._connection(connection)
            return await self._to_parquet_impl(stmt_obj, conn_to_use, **kwargs)
