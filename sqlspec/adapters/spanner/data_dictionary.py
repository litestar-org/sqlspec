"""Spanner metadata queries using INFORMATION_SCHEMA."""

from typing import TYPE_CHECKING, ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.driver import SyncDataDictionaryBase
from sqlspec.typing import ColumnMetadata, ForeignKeyMetadata, IndexMetadata, TableMetadata, VersionInfo

__all__ = ("SpannerDataDictionary",)

if TYPE_CHECKING:
    from sqlspec.adapters.spanner.driver import SpannerSyncDriver


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SpannerDataDictionary(SyncDataDictionaryBase):
    """Fetch table, column, and index metadata from Spanner."""

    dialect: ClassVar[str] = "spanner"

    def __init__(self) -> None:
        super().__init__()

    def get_version(self, driver: "SpannerSyncDriver") -> "VersionInfo | None":
        """Get Spanner version information.

        Args:
            driver: Spanner driver instance.

        Returns:
            None since Spanner does not expose version information.

        """
        _ = driver
        return None

    def get_feature_flag(self, driver: "SpannerSyncDriver", feature: str) -> bool:
        """Check if Spanner supports a specific feature.

        Args:
            driver: Spanner driver instance.
            feature: Feature name to check.

        Returns:
            True if feature is supported, False otherwise.

        """
        _ = driver
        return self.resolve_feature_flag(feature, None)

    def get_optimal_type(self, driver: "SpannerSyncDriver", type_category: str) -> str:
        """Get optimal Spanner type for a category.

        Args:
            driver: Spanner driver instance.
            type_category: Type category.

        Returns:
            Spanner-specific type name.

        """
        _ = driver
        return self.get_dialect_config().get_optimal_type(type_category)

    def get_tables(self, driver: "SpannerSyncDriver", schema: "str | None" = None) -> "list[TableMetadata]":
        """Get tables using INFORMATION_SCHEMA."""
        schema_name = self.resolve_schema(schema)
        self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="tables")
        return driver.select(self.get_query("tables_by_schema"), schema_name=schema_name, schema_type=TableMetadata)

    def get_columns(
        self, driver: "SpannerSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Get column information for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="columns")
            return driver.select(
                self.get_query("columns_by_schema"), schema_name=schema_name, schema_type=ColumnMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="columns")
        return driver.select(
            self.get_query("columns_by_table"), table_name=table, schema_name=schema_name, schema_type=ColumnMetadata
        )

    def get_indexes(
        self, driver: "SpannerSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Get index metadata for a table or schema."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="indexes")
            return driver.select(
                self.get_query("indexes_by_schema"), schema_name=schema_name, schema_type=IndexMetadata
            )

        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="indexes")
        return driver.select(
            self.get_query("indexes_by_table"), table_name=table, schema_name=schema_name, schema_type=IndexMetadata
        )

    def get_foreign_keys(
        self, driver: "SpannerSyncDriver", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        schema_name = self.resolve_schema(schema)
        if table is None:
            self._log_schema_introspect(driver, schema_name=schema_name, table_name=None, operation="foreign_keys")
            return driver.select(
                self.get_query("foreign_keys_by_schema"), schema_name=schema_name, schema_type=ForeignKeyMetadata
            )
        self._log_table_describe(driver, schema_name=schema_name, table_name=table, operation="foreign_keys")
        return driver.select(
            self.get_query("foreign_keys_by_table"),
            table_name=table,
            schema_name=schema_name,
            schema_type=ForeignKeyMetadata,
        )
