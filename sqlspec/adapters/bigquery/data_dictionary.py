"""BigQuery-specific data dictionary for metadata queries."""

from typing import TYPE_CHECKING, Any, cast

from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase, VersionInfo
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.bigquery.driver import BigQueryDriver
    from sqlspec.driver import ForeignKeyMetadata

logger = get_logger("adapters.bigquery.data_dictionary")

__all__ = ("BigQuerySyncDataDictionary",)


class BigQuerySyncDataDictionary(SyncDataDictionaryBase):
    """BigQuery-specific sync data dictionary."""

    def get_version(self, driver: SyncDriverAdapterBase) -> "VersionInfo | None":
        """Get BigQuery version information.

        BigQuery is a cloud service without traditional versioning.
        Returns a fixed version to indicate feature availability.

        Args:
            driver: BigQuery driver instance

        Returns:
            Fixed version info indicating current BigQuery capabilities
        """
        # BigQuery is a cloud service - return a fixed version
        # indicating modern feature support
        logger.debug("BigQuery cloud service - using fixed version")
        return VersionInfo(1, 0, 0)

    def get_feature_flag(self, driver: SyncDriverAdapterBase, feature: str) -> bool:
        """Check if BigQuery supports a specific feature.

        Args:
            driver: BigQuery driver instance
            feature: Feature name to check

        Returns:
            True if feature is supported, False otherwise
        """
        # BigQuery feature support based on current capabilities
        feature_checks = {
            "supports_json": True,  # Native JSON type
            "supports_arrays": True,  # ARRAY types
            "supports_structs": True,  # STRUCT types
            "supports_geography": True,  # GEOGRAPHY type
            "supports_returning": False,  # No RETURNING clause
            "supports_upsert": True,  # MERGE statement
            "supports_window_functions": True,
            "supports_cte": True,
            "supports_transactions": True,  # Multi-statement transactions
            "supports_prepared_statements": True,
            "supports_schemas": True,  # Datasets and projects
            "supports_partitioning": True,  # Table partitioning
            "supports_clustering": True,  # Table clustering
            "supports_uuid": False,  # No native UUID, use STRING
        }

        return feature_checks.get(feature, False)

    def get_optimal_type(self, driver: SyncDriverAdapterBase, type_category: str) -> str:
        """Get optimal BigQuery type for a category.

        Args:
            driver: BigQuery driver instance
            type_category: Type category

        Returns:
            BigQuery-specific type name
        """
        type_map = {
            "json": "JSON",
            "uuid": "STRING",
            "boolean": "BOOL",
            "timestamp": "TIMESTAMP",
            "text": "STRING",
            "blob": "BYTES",
            "array": "ARRAY",
            "struct": "STRUCT",
            "geography": "GEOGRAPHY",
            "numeric": "NUMERIC",
            "bignumeric": "BIGNUMERIC",
        }
        return type_map.get(type_category, "STRING")

    def get_columns(
        self, driver: SyncDriverAdapterBase, table: str, schema: "str | None" = None
    ) -> "list[dict[str, Any]]":
        """Get column information for a table using INFORMATION_SCHEMA.

        Args:
            driver: BigQuery driver instance
            table: Table name to query columns for
            schema: Schema name (dataset name in BigQuery)

        Returns:
            List of column metadata dictionaries with keys:
                - column_name: Name of the column
                - data_type: BigQuery data type
                - is_nullable: Whether column allows NULL (YES/NO)
                - column_default: Default value if any
        """
        bigquery_driver = cast("BigQueryDriver", driver)

        if schema:
            sql = f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM `{schema}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = '{table}'
                ORDER BY ordinal_position
            """
        else:
            sql = f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{table}'
                ORDER BY ordinal_position
            """

        result = bigquery_driver.execute(sql)
        return result.data or []

    def get_tables(self, driver: "SyncDriverAdapterBase", schema: "str | None" = None) -> "list[str]":
        """Get list of tables in schema."""
        bigquery_driver = cast("BigQueryDriver", driver)
        if schema:
            sql = f"SELECT table_name FROM `{schema}.INFORMATION_SCHEMA.TABLES` WHERE table_type = 'BASE TABLE'"
        else:
            sql = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE'"

        result = bigquery_driver.execute(sql)
        return [row["table_name"] for row in result.data]

    def get_foreign_keys(
        self, driver: "SyncDriverAdapterBase", table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Get foreign key metadata."""
        from sqlspec.driver import ForeignKeyMetadata

        bigquery_driver = cast("BigQueryDriver", driver)

        dataset = schema
        if dataset:
            kcu_table = f"`{dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`"
            rc_table = f"`{dataset}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`"
        else:
            kcu_table = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
            rc_table = "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS"

        where_clauses = []
        if table:
            where_clauses.append(f"kcu.table_name = '{table}'")

        where_str = " AND ".join(where_clauses)
        if where_str:
            where_str = "WHERE " + where_str

        sql = f"""
            SELECT
                kcu.table_name,
                kcu.column_name,
                pk_kcu.table_name AS referenced_table_name,
                pk_kcu.column_name AS referenced_column_name,
                kcu.constraint_name,
                kcu.table_schema,
                pk_kcu.table_schema AS referenced_table_schema
            FROM {kcu_table} kcu
            JOIN {rc_table} rc ON kcu.constraint_name = rc.constraint_name
            JOIN {kcu_table} pk_kcu
              ON rc.unique_constraint_name = pk_kcu.constraint_name
              AND kcu.ordinal_position = pk_kcu.ordinal_position
            {where_str}
        """

        try:
            result = bigquery_driver.execute(sql)
            return [
                ForeignKeyMetadata(
                    table_name=row["table_name"],
                    column_name=row["column_name"],
                    referenced_table=row["referenced_table_name"],
                    referenced_column=row["referenced_column_name"],
                    constraint_name=row["constraint_name"],
                    schema=row["table_schema"],
                    referenced_schema=row["referenced_table_schema"],
                )
                for row in result.data
            ]
        except Exception:
            logger.warning("Failed to fetch foreign keys from BigQuery")
            return []

    def list_available_features(self) -> "list[str]":
        """List available BigQuery feature flags.

        Returns:
            List of supported feature names
        """
        return [
            "supports_json",
            "supports_arrays",
            "supports_structs",
            "supports_geography",
            "supports_returning",
            "supports_upsert",
            "supports_window_functions",
            "supports_cte",
            "supports_transactions",
            "supports_prepared_statements",
            "supports_schemas",
            "supports_partitioning",
            "supports_clustering",
            "supports_uuid",
        ]
