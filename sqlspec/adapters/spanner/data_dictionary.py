"""Spanner metadata queries using INFORMATION_SCHEMA."""

from typing import TYPE_CHECKING, Any, ClassVar, cast

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary import (
    ColumnMetadata,
    DDLResult,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataResult,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    TableMetadata,
    VersionInfo,
)
from sqlspec.driver import SyncDataDictionaryBase

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.spanner.driver import SpannerSyncDriver

__all__ = ("SpannerDataDictionary",)

_DEFAULT_METADATA_DOMAINS = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "routines",
    "privileges",
    "dependencies",
    "ddl",
    "system",
)

_SPANNER_INFORMATION_SCHEMA_WARNINGS = (
    "Spanner information schema rows are filtered by IAM and database role privileges.",
)

_SPANNER_POSTGRESQL_WARNING = "PostgreSQL-dialect Spanner metadata requires live runtime coverage before enablement."

_SPANNER_DDL_WARNINGS = (
    "Spanner DDL metadata uses the Database Admin API and requires database DDL permissions.",
    "Pending schema updates might not be reflected in the returned DDL.",
)

_SPANNER_SYSTEM_WARNINGS = (
    "Spanner SPANNER_SYS metadata is opt-in, permission-aware, and intended for operational diagnostics.",
)


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

    def get_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None, *, mode: str | None = None
    ) -> "MetadataCapabilityProfile":
        """Get Spanner replacement data-dictionary capability profile."""
        _ = driver
        requested_domains = tuple(domains) if domains is not None else _DEFAULT_METADATA_DOMAINS
        normalized_mode = _normalize_spanner_metadata_mode(mode)
        capabilities = tuple(
            _spanner_capability_for_domain(domain, mode=normalized_mode) for domain in requested_domains
        )
        return MetadataCapabilityProfile(self.dialect, adapter=type(self).__name__, capabilities=capabilities)

    def get_ddl(self, driver: Any, object_name: str, schema: "str | None" = None) -> "MetadataResult":
        """Get Spanner DDL through the Database Admin API."""
        ddl_statements = _get_spanner_ddl_statements(driver)
        identity = ObjectIdentity(
            name=object_name,
            object_type="object",
            schema=schema,
            dialect=self.dialect,
            source=MetadataSource.NATIVE_API,
        )
        capability = _spanner_capability_for_domain("ddl", mode="googlesql")
        if not ddl_statements:
            return MetadataResult(domain="ddl", capability=capability, items=(), warnings=_SPANNER_DDL_WARNINGS)
        ddl = _select_spanner_ddl_for_object(ddl_statements, object_name)
        result = DDLResult(
            identity=identity,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.NATIVE_API,
            ddl=ddl,
            warnings=_SPANNER_DDL_WARNINGS,
        )
        return MetadataResult(domain="ddl", capability=capability, items=(result,), warnings=_SPANNER_DDL_WARNINGS)


def _spanner_capability_for_domain(domain: str, *, mode: str) -> "MetadataCapability":
    if mode == "postgresql":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.UNSUPPORTED,
            fidelity=MetadataFidelity.UNSUPPORTED,
            source=MetadataSource.UNKNOWN,
            risks=(MetadataRisk.VERSION_GATED,),
            warnings=(_SPANNER_POSTGRESQL_WARNING,),
        )
    if domain == "ddl":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.NATIVE_API,
            risks=(MetadataRisk.PRIVILEGED,),
            warnings=_SPANNER_DDL_WARNINGS,
        )
    if domain == "system":
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.PARTIAL,
            source=MetadataSource.SYSTEM_VIEW,
            risks=(MetadataRisk.PRIVILEGED, MetadataRisk.EXPENSIVE),
            warnings=_SPANNER_SYSTEM_WARNINGS,
        )
    if domain in _DEFAULT_METADATA_DOMAINS or domain in {"database", "sequences", "change_streams", "property_graphs"}:
        return MetadataCapability(
            domain=domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.INFORMATION_SCHEMA,
            risks=(MetadataRisk.PRIVILEGED,),
            warnings=_SPANNER_INFORMATION_SCHEMA_WARNINGS,
        )
    return MetadataCapability.unsupported(domain)


def _normalize_spanner_metadata_mode(mode: str | None) -> str:
    normalized = (mode or "googlesql").lower()
    if normalized in {"googlesql", "google_sql", "spanner_googlesql"}:
        return "googlesql"
    if normalized in {"postgres", "postgresql", "spangres", "spanner_postgresql"}:
        return "postgresql"
    return normalized


def _get_spanner_ddl_statements(driver: Any) -> "tuple[str, ...]":
    database = _get_spanner_database(driver)
    if database is None:
        return ()
    get_ddl = getattr(database, "get_ddl", None)
    if callable(get_ddl):
        return tuple(str(statement) for statement in cast("Sequence[object]", get_ddl()))
    admin_api = _get_spanner_database_admin_api(database)
    database_name = getattr(database, "name", None)
    if admin_api is None or database_name is None:
        return ()
    response = admin_api.get_database_ddl(database=database_name)
    statements = getattr(response, "statements", ())
    return tuple(str(statement) for statement in cast("Sequence[object]", statements))


def _get_spanner_database(driver: Any) -> Any:
    database = getattr(driver, "database", None)
    if database is not None:
        return database
    connection = getattr(driver, "connection", None)
    session = getattr(connection, "_session", None)
    return getattr(session, "_database", None)


def _get_spanner_database_admin_api(database: Any) -> Any:
    admin_api = getattr(database, "database_admin_api", None)
    if admin_api is not None:
        return admin_api
    instance = getattr(database, "_instance", None)
    client = getattr(instance, "_client", None)
    return getattr(client, "database_admin_api", None)


def _select_spanner_ddl_for_object(statements: "tuple[str, ...]", object_name: str) -> str:
    normalized_name = object_name.lower()
    for statement in statements:
        if normalized_name in statement.lower():
            return statement
    return "\n".join(statements)
