"""BigQuery-specific data dictionary for metadata queries."""

from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase, VersionInfo
from sqlspec.utils.logging import get_logger

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
