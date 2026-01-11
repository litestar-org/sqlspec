"""BigQuery-specific data dictionary for metadata queries."""

from typing import TYPE_CHECKING, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo

__all__ = ("BigQueryDataDictionary",)

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery.driver import BigQueryDriver


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class BigQueryDataDictionary(SyncDataDictionaryBase):
    """BigQuery-specific sync data dictionary."""

    dialect: ClassVar[str] = "bigquery"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: "BigQueryDriver") -> "VersionInfo | None":
        """Return BigQuery version information.

        Args:
            driver: BigQuery driver instance.

        Returns:
            None because BigQuery does not expose version info.

        """
        _ = driver
        return None

    def get_feature_flag(self, driver: "BigQueryDriver", feature: str) -> bool:
        """Check if BigQuery supports a specific feature.

        Args:
            driver: BigQuery driver instance.
            feature: Feature name to check.

        Returns:
            True if feature is supported, False otherwise.

        """
        _ = driver
        return self.resolve_feature_flag(feature, None)

    def get_optimal_type(self, driver: "BigQueryDriver", type_category: str) -> str:
        """Get optimal BigQuery type for a category.

        Args:
            driver: BigQuery driver instance.
            type_category: Type category.

        Returns:
            BigQuery-specific type name.

        """
        _ = driver
        return self.get_dialect_config().get_optimal_type(type_category)

    def get_tables(self, driver: "BigQueryDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables sorted by topological dependency order using BigQuery catalog."""
        self._log_schema_introspect(driver, schema_name=schema, table_name=None, operation="tables")
        if schema:
            tables_table = f"`{schema}.INFORMATION_SCHEMA.TABLES`"
            kcu_table = f"`{schema}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`"
            rc_table = f"`{schema}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`"
        else:
            tables_table = "INFORMATION_SCHEMA.TABLES"
            kcu_table = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
            rc_table = "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"

        query_text = self.get_query_text("tables_by_schema").format(
            tables_table=tables_table, kcu_table=kcu_table, rc_table=rc_table
        )
        return driver.select(query_text, schema_type=TableMetadata)

    def get_columns(
        self, driver: "BigQueryDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_prefix = f"`{schema}`." if schema else ""
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema, table_name=None, operation="columns")
            query_text = self.get_query_text("columns_by_schema").format(schema_prefix=schema_prefix)
            return driver.select(query_text, schema_name=schema, schema_type=ColumnMetadata)

        self._log_table_describe(driver, schema_name=schema, table_name=table, operation="columns")
        query_text = self.get_query_text("columns_by_table").format(schema_prefix=schema_prefix)
        return driver.select(query_text, table_name=table, schema_name=schema, schema_type=ColumnMetadata)

    def get_indexes(
        self, driver: "BigQueryDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema, table_name=None, operation="indexes")
            return driver.select(self.get_query("indexes_by_schema"), schema_type=IndexMetadata)

        self._log_table_describe(driver, schema_name=schema, table_name=table, operation="indexes")
        return driver.select(self.get_query("indexes_by_table"), schema_type=IndexMetadata)

    def get_foreign_keys(
        self, driver: "BigQueryDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema, table_name=None, operation="foreign_keys")
        else:
            self._log_table_describe(driver, schema_name=schema, table_name=table, operation="foreign_keys")
        if schema:
            kcu_table = f"`{schema}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`"
            rc_table = f"`{schema}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`"
        else:
            kcu_table = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
            rc_table = "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"

        if table is None:
            query_text = self.get_query_text("foreign_keys_by_schema").format(kcu_table=kcu_table, rc_table=rc_table)
            return driver.select(query_text, schema_name=schema, schema_type=ForeignKeyMetadata)

        query_text = self.get_query_text("foreign_keys_by_table").format(kcu_table=kcu_table, rc_table=rc_table)
        return driver.select(query_text, table_name=table, schema_name=schema, schema_type=ForeignKeyMetadata)
