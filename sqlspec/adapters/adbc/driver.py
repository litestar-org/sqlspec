# ruff: noqa: PLR0915, C901, PLR0912, PLR0911
import contextlib
import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast, overload

import sqlglot
from adbc_driver_manager.dbapi import Connection, Cursor
from sqlglot import exp as sqlglot_exp

from sqlspec.base import SyncDriverAdapterProtocol
from sqlspec.exceptions import ParameterStyleMismatchError, SQLParsingError
from sqlspec.mixins import SQLTranslatorMixin, SyncArrowBulkOperationsMixin
from sqlspec.typing import ArrowTable, StatementParameterType

if TYPE_CHECKING:
    from sqlspec.typing import ArrowTable, ModelDTOT, StatementParameterType, T

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection


class AdbcDriver(
    SyncArrowBulkOperationsMixin["AdbcConnection"],
    SQLTranslatorMixin["AdbcConnection"],
    SyncDriverAdapterProtocol["AdbcConnection"],
):
    """ADBC Sync Driver Adapter."""

    connection: AdbcConnection
    __supports_arrow__: ClassVar[bool] = True
    dialect: str = "adbc"

    def __init__(self, connection: "AdbcConnection") -> None:
        """Initialize the ADBC driver adapter."""
        self.connection = connection
        self.dialect = self._get_dialect(connection)  # Store detected dialect

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Get the database dialect based on the driver name.

        Args:
            connection: The ADBC connection object.

        Returns:
            The database dialect.
        """
        driver_name = connection.adbc_get_info()["vendor_name"].lower()
        if "postgres" in driver_name:
            return "postgres"
        if "bigquery" in driver_name:
            return "bigquery"
        if "sqlite" in driver_name:
            return "sqlite"
        if "duckdb" in driver_name:
            return "duckdb"
        if "mysql" in driver_name:
            return "mysql"
        if "snowflake" in driver_name:
            return "snowflake"
        return "postgres"  # default to postgresql dialect

    @staticmethod
    def _cursor(connection: "AdbcConnection", *args: Any, **kwargs: Any) -> "Cursor":
        return connection.cursor(*args, **kwargs)

    @contextmanager
    def _with_cursor(self, connection: "AdbcConnection") -> Generator["Cursor", None, None]:
        cursor = self._cursor(connection)
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _process_sql_params(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        **kwargs: Any,
    ) -> "tuple[str, Optional[tuple[Any, ...]]]":  # Always returns tuple or None for params
        # 1. Merge parameters and kwargs
        merged_params: Optional[Union[dict[str, Any], Sequence[Any], Any]] = None  # Allow Any for scalar
        if kwargs:
            if isinstance(parameters, dict):
                merged_params = {**parameters, **kwargs}
            elif parameters is not None:
                msg = "Cannot mix positional parameters with keyword arguments for adbc driver."
                raise ParameterStyleMismatchError(msg)
            else:
                merged_params = kwargs
        elif parameters is not None:
            merged_params = parameters
        # else merged_params remains None

        # 2. SQLGlot Parsing - Use the determined dialect for reading if possible
        # For ADBC, the input SQL style might be generic, so a neutral read dialect or auto-detection might be safer initially.
        # However, self.dialect is set in __init__ based on the connection.
        # If the input SQL is guaranteed to match self.dialect, this is fine.
        # If SQL can be :name style while dialect is e.g. postgres, sqlglot needs to know.
        # Defaulting to parsing with the target dialect; an explicit read_dialect can be added if needed.
        try:
            # The `read` dialect might need to be more flexible if input SQL doesn't match target ADBC driver dialect.
            # For now, assume input SQL should be somewhat compatible or generic enough for sqlglot to parse with target dialect hints.
            parsed_expression = sqlglot.parse_one(sql, read=self.dialect)
        except Exception as e:
            msg = f"ADBC ({self.dialect}): Failed to parse SQL with sqlglot: {e}. SQL: {sql}"
            raise SQLParsingError(msg) from e

        sql_named_param_nodes = [
            node for node in parsed_expression.find_all(sqlglot_exp.Parameter) if node.name and not node.name.isdigit()
        ]
        sql_placeholder_nodes = list(
            parsed_expression.find_all(sqlglot_exp.Placeholder)
        )  # Represents '?' and also ':name' for some dialects

        # More robust detection of numeric parameters (e.g., $1, :1)
        sql_numeric_nodes = []
        for node in parsed_expression.find_all(sqlglot_exp.Parameter):
            is_numeric = False
            if node.name and isinstance(node.name, str) and node.name.isdigit():  # e.g. @1
                is_numeric = True
            elif not node.name and node.this:
                # Combined and corrected multi-line condition
                is_identifier_numeric = (
                    isinstance(node.this, sqlglot_exp.Identifier)
                    and node.this.this
                    and isinstance(node.this.this, str)
                    and node.this.this.isdigit()
                )
                if (
                    (isinstance(node.this, str) and node.this.isdigit())
                    or is_identifier_numeric
                    or isinstance(node.this, sqlglot.exp.Number)
                ):  # Corrected: sqlglot.exp.Number
                    is_numeric = True
            if is_numeric:
                sql_numeric_nodes.append(node)

        # Numeric placeholders like :1, :2 (parsed as Placeholder(this="1"))
        numeric_placeholder_nodes = [
            p_node for p_node in sql_placeholder_nodes if isinstance(p_node.this, str) and p_node.this.isdigit()
        ]

        # 3. Handle No Parameters Case
        if merged_params is None:
            if sql_named_param_nodes or sql_placeholder_nodes or sql_numeric_nodes:
                placeholder_types = set()
                if sql_named_param_nodes:
                    placeholder_types.add("named (e.g., :name, @name, $name)")
                if sql_placeholder_nodes:
                    placeholder_types.add("positional ('?')")
                if sql_numeric_nodes:
                    placeholder_types.add("numeric (e.g., $1, :1)")
                msg = (
                    f"ADBC ({self.dialect}): SQL statement contains {', '.join(placeholder_types) if placeholder_types else 'unknown'} "
                    f"parameter placeholders, but no parameters were provided. SQL: {sql}"
                )
                raise SQLParsingError(msg)
            # ADBC execute often expects list/tuple for parameters argument, even if empty.
            return sql, ()

        final_sql: str
        final_params_seq: list[Any] = []

        target_dialect = self.dialect  # Dialect determined from ADBC connection

        if isinstance(merged_params, dict):
            has_qmark_placeholders = any(
                p_node.this is None  # Removed isinstance check as p_node is already known to be Placeholder
                for p_node in sql_placeholder_nodes
            )

            if has_qmark_placeholders or numeric_placeholder_nodes or sql_numeric_nodes:
                msg = (
                    f"ADBC ({target_dialect}): Dictionary parameters provided, but SQL uses positional "
                    f"placeholders ('?', $N, or :N). Use named placeholders (e.g. :name, @name, $name)."
                )
                raise ParameterStyleMismatchError(msg)

            # Validate keys
            colon_named_placeholder_nodes = [
                p_node for p_node in sql_placeholder_nodes if isinstance(p_node.this, str) and not p_node.this.isdigit()
            ]
            all_ast_named_params = {node.name for node in sql_named_param_nodes if node.name}
            all_ast_named_params.update(
                node.this for node in colon_named_placeholder_nodes if isinstance(node.this, str)
            )

            provided_keys = set(merged_params.keys())
            missing_keys = all_ast_named_params - provided_keys
            if missing_keys:
                msg = f"ADBC ({target_dialect}): Named parameters {missing_keys} found in SQL but not provided. SQL: {sql}"
                raise SQLParsingError(msg)
            extra_keys = provided_keys - all_ast_named_params
            if extra_keys:
                logger.warning(
                    f"ADBC ({target_dialect}): Parameters {extra_keys} provided but not found in SQL. Behavior depends on underlying driver. SQL: {sql}"
                )

            # Transformation logic: convert all named styles to '?' and populate final_params_seq
            final_params_seq.clear()

            def _convert_any_named_to_qmark(node: sqlglot_exp.Expression) -> sqlglot_exp.Expression:
                param_name_to_check = None
                if isinstance(node, sqlglot_exp.Parameter) and node.name and not node.name.isdigit():
                    param_name_to_check = node.name
                elif (
                    isinstance(node, sqlglot_exp.Placeholder) and isinstance(node.this, str) and not node.this.isdigit()
                ):
                    param_name_to_check = node.this

                if param_name_to_check and param_name_to_check in merged_params:  # merged_params is a dict here
                    final_params_seq.append(merged_params[param_name_to_check])
                    return sqlglot_exp.Placeholder()  # Anonymous placeholder '?'
                return node

            transformed_expression = parsed_expression.transform(_convert_any_named_to_qmark, copy=True)
            final_sql = transformed_expression.sql(dialect=target_dialect)

        elif isinstance(merged_params, (list, tuple)):
            # Collect all types of placeholders from SQL
            qmark_placeholders_count = sum(1 for p_node in sql_placeholder_nodes if p_node.this is None)

            # Named placeholders from sqlglot_exp.Parameter (e.g., @name, $name)
            # sql_named_param_nodes is already defined

            # Named placeholders from sqlglot_exp.Placeholder (e.g., :name)
            colon_named_placeholder_nodes = [
                p_node for p_node in sql_placeholder_nodes if isinstance(p_node.this, str) and not p_node.this.isdigit()
            ]

            # Numeric placeholders (e.g., $1, :1) - from both Parameter and Placeholder nodes
            # numeric_placeholder_nodes and sql_numeric_nodes are already defined

            # Get all named parameters in order of appearance in the AST
            ordered_named_params_in_ast = []
            # We need to iterate through the AST to get them in order. find_all gives traversal order.
            for node in parsed_expression.find_all(sqlglot_exp.Parameter, sqlglot_exp.Placeholder):
                if isinstance(node, sqlglot_exp.Parameter) and node.name and not node.name.isdigit():
                    ordered_named_params_in_ast.append(node.name)
                elif (
                    isinstance(node, sqlglot_exp.Placeholder) and isinstance(node.this, str) and not node.this.isdigit()
                ):
                    ordered_named_params_in_ast.append(node.this)

            # Remove duplicates while preserving order for mapping if needed (though duplicates might be intended by user in some complex SQLs)
            # For now, let's assume unique placeholders for mapping tuple to names. If SQL has duplicate named params,
            # this logic might need adjustment or clarification on expected behavior.
            # A simpler approach for now: just count unique types.

            has_any_named_placeholders = bool(sql_named_param_nodes or colon_named_placeholder_nodes)
            has_any_positional_or_numeric = bool(
                qmark_placeholders_count or numeric_placeholder_nodes or sql_numeric_nodes
            )

            if has_any_named_placeholders:
                if has_any_positional_or_numeric:
                    msg = f"ADBC ({target_dialect}): Sequence parameters provided, but SQL mixes named and positional/numeric placeholders. This is not supported."
                    raise ParameterStyleMismatchError(msg)

                # SQL has only named placeholders. Try to map sequence params to them by order.
                # Re-extract ordered unique named parameter *names* for mapping.
                # find_all preserves order. To get unique names in order:
                unique_ordered_named_param_names = list(
                    dict.fromkeys(ordered_named_params_in_ast)
                )  # Python 3.7+ dict preserves insertion order

                if len(unique_ordered_named_param_names) != len(merged_params):
                    msg = (
                        f"ADBC ({target_dialect}): Sequence parameters provided (count: {len(merged_params)}), "
                        f"but SQL has {len(unique_ordered_named_param_names)} unique named placeholders. Counts must match."
                    )
                    raise SQLParsingError(msg)

                # Create a temporary dict from named params and sequence values
                temp_dict_params = dict(zip(unique_ordered_named_param_names, merged_params))

                # Now, use the same transformation as the dict instance block
                final_params_seq.clear()

                def _convert_any_named_to_qmark_for_seq(node: sqlglot_exp.Expression) -> sqlglot_exp.Expression:
                    param_name_to_check = None
                    if isinstance(node, sqlglot_exp.Parameter) and node.name and not node.name.isdigit():
                        param_name_to_check = node.name
                    elif (
                        isinstance(node, sqlglot_exp.Placeholder)
                        and isinstance(node.this, str)
                        and not node.this.isdigit()
                    ):
                        param_name_to_check = node.this

                    if param_name_to_check and param_name_to_check in temp_dict_params:
                        final_params_seq.append(temp_dict_params[param_name_to_check])
                        return sqlglot_exp.Placeholder()  # Anonymous placeholder '?'
                    return node

                transformed_expression = parsed_expression.transform(_convert_any_named_to_qmark_for_seq, copy=True)
                final_sql = transformed_expression.sql(dialect=target_dialect)

            elif has_any_positional_or_numeric:  # SQL has only positional/numeric (or qmark) placeholders
                final_sql = parsed_expression.sql(
                    dialect=target_dialect
                )  # Ensure it's in target dialect form (e.g. ? -> $1)
                expected_param_count = (
                    qmark_placeholders_count + len(numeric_placeholder_nodes) + len(sql_numeric_nodes)
                )
                if expected_param_count != len(merged_params):
                    msg = (
                        f"ADBC ({target_dialect}): Parameter count mismatch. SQL expects {expected_param_count} "
                        f"positional parameters, but {len(merged_params)} were provided. SQL: {sql}, Processed SQL: {final_sql}"
                    )
                    raise SQLParsingError(msg)
                final_params_seq.extend(merged_params)
            else:  # No placeholders in SQL, but sequence params provided
                if merged_params:  # If params is not an empty tuple/list
                    msg = f"ADBC ({target_dialect}): Sequence parameters provided, but SQL has no placeholders."
                    raise SQLParsingError(msg)
                # If merged_params is empty sequence and no placeholders, that's fine.
                final_sql = sql  # No transformation needed
                # final_params_seq remains empty, which is correct

        elif merged_params is not None:  # Scalar parameter
            qmark_placeholders_count = sum(1 for p_node in sql_placeholder_nodes if p_node.this is None)
            colon_named_placeholder_nodes = [
                p_node for p_node in sql_placeholder_nodes if isinstance(p_node.this, str) and not p_node.this.isdigit()
            ]

            total_named_placeholders = len(sql_named_param_nodes) + len(colon_named_placeholder_nodes)
            total_positional_or_numeric_placeholders = (
                qmark_placeholders_count + len(numeric_placeholder_nodes) + len(sql_numeric_nodes)
            )

            if total_named_placeholders > 0 and total_positional_or_numeric_placeholders > 0:
                # SQL mixes named and positional/numeric types, not allowed with scalar param.
                msg = (
                    f"ADBC ({target_dialect}): Scalar parameter provided, but SQL mixes named and positional/numeric "
                    f"placeholders. Use a single placeholder of one type."
                )
                raise ParameterStyleMismatchError(msg)

            if total_named_placeholders == 1 and total_positional_or_numeric_placeholders == 0:
                # Case: Scalar param with exactly one NAMED placeholder (e.g., :name, @name)
                final_params_seq.clear()
                single_named_param_name = None
                if sql_named_param_nodes:  # @name style
                    single_named_param_name = sql_named_param_nodes[0].name
                elif colon_named_placeholder_nodes:  # :name style
                    single_named_param_name = colon_named_placeholder_nodes[0].this

                def _convert_the_one_named_to_qmark(node: sqlglot_exp.Expression) -> sqlglot_exp.Expression:
                    # This function assumes single_named_param_name is correctly identified
                    if (isinstance(node, sqlglot_exp.Parameter) and node.name == single_named_param_name) or (
                        isinstance(node, sqlglot_exp.Placeholder) and node.this == single_named_param_name
                    ):
                        final_params_seq.append(merged_params)  # Directly use scalar
                        return sqlglot_exp.Placeholder()  # Anonymous placeholder '?'
                    return node

                transformed_expression = parsed_expression.transform(_convert_the_one_named_to_qmark, copy=True)
                final_sql = transformed_expression.sql(dialect=target_dialect)

            elif total_positional_or_numeric_placeholders == 1 and total_named_placeholders == 0:
                # Case: Scalar param with exactly one POSITIONAL/NUMERIC placeholder (e.g., ?, $1)
                final_sql = parsed_expression.sql(dialect=target_dialect)  # Ensure correct dialect form (e.g. ? -> $1)
                final_params_seq.append(merged_params)

            elif total_named_placeholders == 0 and total_positional_or_numeric_placeholders == 0:
                # Case: Scalar param, but SQL has NO placeholders at all.
                msg = f"ADBC ({target_dialect}): Scalar parameter provided, but SQL has no parameter placeholders."
                raise SQLParsingError(msg)

            else:
                # Any other scenario: e.g. multiple named, multiple positional, etc.
                msg = (
                    f"ADBC ({target_dialect}): Scalar parameter provided, but SQL placeholder configuration is invalid. "
                    f"Found {total_named_placeholders} named and {total_positional_or_numeric_placeholders} positional/numeric placeholders. "
                    f"Expected exactly one placeholder of a consistent type for a scalar parameter."
                )
                raise ParameterStyleMismatchError(msg)

        else:
            return sql, ()

        return final_sql, tuple(final_params_seq)

    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    def select(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AdbcConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch data from the database.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            results = cursor.fetchall()  # pyright: ignore
            if not results:
                return []

            column_names = [col[0] for col in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            if schema_type is not None:
                return [cast("ModelDTOT", schema_type(**dict(zip(column_names, row)))) for row in results]  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            return [dict(zip(column_names, row)) for row in results]  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportUnknownArgumentType]
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is None:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return schema_type(**dict(zip(column_names, result)))  # type: ignore[return-value]

    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    def select_one_or_none(
        self,
        sql: str,
        parameters: Optional["StatementParameterType"] = None,
        /,
        *,
        connection: Optional["AdbcConnection"] = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]":
        """Fetch one row from the database.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if schema_type is None:
                return dict(zip(column_names, result))  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
            return schema_type(**dict(zip(column_names, result)))  # type: ignore[return-value]

    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            result = self.check_not_found(result)  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType,reportUnknownArgumentType]
            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownVariableType]
            return schema_type(result[0])  # type: ignore[call-arg]

    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if result is None:
                return None
            if schema_type is None:
                return result[0]  # pyright: ignore[reportUnknownVariableType]
            return schema_type(result[0])  # type: ignore[call-arg]

    def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cursor.rowcount if hasattr(cursor, "rowcount") else -1

    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return result.

        Returns:
            The first row of results.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)
        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            result = cursor.fetchall()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if not result:
                return None

            first_row = result[0]

            column_names = [c[0] for c in cursor.description or []]  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

            result_dict = dict(zip(column_names, first_row))

            if schema_type is None:
                return result_dict
            return cast("ModelDTOT", schema_type(**result_dict))

    def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a script.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("str", cursor.statusmessage) if hasattr(cursor, "statusmessage") else "DONE"  # pyright: ignore[reportUnknownMemberType,reportAttributeAccessIssue]

    # --- Arrow Bulk Operations ---

    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[AdbcConnection]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":
        """Execute a SQL query and return results as an Apache Arrow Table.

        Returns:
            The results of the query as an Apache Arrow Table.
        """
        connection = self._connection(connection)
        sql, parameters = self._process_sql_params(sql, parameters, **kwargs)

        with self._with_cursor(connection) as cursor:
            cursor.execute(sql, parameters)  # pyright: ignore[reportUnknownMemberType]
            return cast("ArrowTable", cursor.fetch_arrow_table())  # pyright: ignore[reportUnknownMemberType]
