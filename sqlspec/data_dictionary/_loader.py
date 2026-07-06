from importlib import resources
from importlib.resources import as_file
from typing import TYPE_CHECKING

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary._registry import get_dialect_config, normalize_dialect_mode, normalize_dialect_name
from sqlspec.data_dictionary._types import (
    MetadataCapability,
    MetadataFidelity,
    MetadataQuery,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
)
from sqlspec.exceptions import SQLFileNotFoundError
from sqlspec.loader import SQLFileLoader
from sqlspec.utils.text import slugify

if TYPE_CHECKING:
    import sys
    from collections.abc import Iterable

    if sys.version_info >= (3, 11):
        from importlib.resources.abc import Traversable
    else:
        from importlib.abc import Traversable

    from sqlspec.core.statement import SQL
    from sqlspec.data_dictionary._types import DialectConfig, VersionInfo

__all__ = ("DataDictionaryLoader", "get_data_dictionary_loader")


SQL_RESOURCE_PACKAGE = "sqlspec.data_dictionary"
SQL_RESOURCE_NAME = "sql"


@mypyc_attr(allow_interpreted_subclasses=False)
class DataDictionaryLoader:
    """Loads and manages data dictionary SQL for all dialects."""

    __slots__ = ("_domain_loaders", "_loaded_dialects", "_loaded_domain_paths", "_sql_loaders")

    def __init__(self) -> None:
        """Initialize the data dictionary loader."""
        self._sql_loaders: dict[str, SQLFileLoader] = {}
        self._domain_loaders: dict[tuple[str, str, str | None], SQLFileLoader] = {}
        self._loaded_dialects: set[str] = set()
        self._loaded_domain_paths: set[tuple[str, str, str | None]] = set()

    def _get_loader(self, dialect: str) -> "SQLFileLoader":
        """Return or create a SQL loader for a dialect.

        Args:
            dialect: Dialect name.

        Returns:
            SQLFileLoader instance.
        """
        loader = self._sql_loaders.get(dialect)
        if loader is None:
            loader = SQLFileLoader()
            self._sql_loaders[dialect] = loader
        return loader

    def _ensure_dialect_loaded(self, dialect: str) -> None:
        """Lazy load SQL files for a dialect.

        Args:
            dialect: Dialect name.

        Raises:
            SQLFileNotFoundError: When the dialect SQL directory is missing.
        """
        if dialect in self._loaded_dialects:
            return
        dialect_resource = _sql_resource_root().joinpath(dialect)
        if not dialect_resource.is_dir():
            raise SQLFileNotFoundError(str(dialect_resource))
        loader = self._get_loader(dialect)
        with as_file(dialect_resource) as dialect_path:
            loader.load_sql(dialect_path)
        self._loaded_dialects.add(dialect)

    def get_query(self, dialect: str, query_name: str) -> "SQL":
        """Get SQL query for a specific dialect and operation.

        Args:
            dialect: Dialect name.
            query_name: Query name to fetch.

        Returns:
            SQL object for the named query.
        """
        self._ensure_dialect_loaded(dialect)
        loader = self._get_loader(dialect)
        return loader.get_sql(query_name)

    def get_query_text(self, dialect: str, query_name: str) -> str:
        """Get raw SQL text for a specific dialect and operation.

        Args:
            dialect: Dialect name.
            query_name: Query name to fetch.

        Returns:
            Raw SQL text for the named query.
        """
        self._ensure_dialect_loaded(dialect)
        loader = self._get_loader(dialect)
        return loader.get_query_text(query_name)

    def _get_domain_loader(self, dialect: str, domain: str, mode: str | None) -> "SQLFileLoader":
        """Return or create a SQL loader for a dialect/domain/mode pack."""
        key = (dialect, domain, mode)
        loader = self._domain_loaders.get(key)
        if loader is None:
            loader = SQLFileLoader()
            self._domain_loaders[key] = loader
        return loader

    def _domain_path_candidates(
        self, dialect: str, domain: str, mode: str | None
    ) -> "tuple[tuple[str | None, Traversable], ...]":
        """Return candidate resource paths for a domain query pack."""
        dialect_resource = _sql_resource_root().joinpath(dialect)
        if mode is None:
            return ((None, dialect_resource.joinpath(domain)),)
        return (
            (mode, dialect_resource.joinpath(domain).joinpath(mode)),
            (mode, dialect_resource.joinpath(mode).joinpath(domain)),
        )

    def _ensure_domain_loaded(self, dialect: str, domain: str, mode: str | None) -> bool:
        """Lazy load SQL files for a dialect/domain/mode pack.

        Args:
            dialect: Canonical dialect name.
            domain: Metadata domain name.
            mode: Optional dialect mode name.

        Returns:
            True when a matching domain path exists and has been loaded.
        """
        key = (dialect, domain, mode)
        if key in self._loaded_domain_paths:
            return True

        for resolved_mode, domain_resource in self._domain_path_candidates(dialect, domain, mode):
            if not domain_resource.is_dir():
                continue
            loader = self._get_domain_loader(dialect, domain, resolved_mode)
            with as_file(domain_resource) as domain_path:
                loader.load_sql(domain_path)
            self._loaded_domain_paths.add((dialect, domain, resolved_mode))
            if resolved_mode == mode:
                return True
        return False

    def _unsupported_domain_query(
        self,
        dialect: str,
        domain: str,
        query_name: str,
        *,
        mode: str | None = None,
        source: "MetadataSource | str" = MetadataSource.UNKNOWN,
        risks: "tuple[MetadataRisk | str, ...]" = (),
        warnings: "tuple[str, ...] | None" = None,
    ) -> MetadataQuery:
        """Return a standard unsupported query result."""
        warning_tuple = warnings
        if warning_tuple is None:
            warning_tuple = (f"No data-dictionary query found for {dialect}/{domain}/{query_name}",)
        return MetadataQuery.unsupported(
            dialect=dialect,
            domain=domain,
            name=query_name,
            mode=mode,
            source=source,
            risks=risks,
            warnings=warning_tuple,
        )

    def _feature_gate_query(
        self,
        dialect: str,
        domain: str,
        query_name: str,
        *,
        mode: str | None,
        version: "VersionInfo | None",
        required_features: "tuple[str, ...]",
    ) -> MetadataQuery | None:
        """Return an unsupported result when query feature gates fail."""
        if not required_features:
            return None
        config = get_dialect_config(dialect)
        for feature in required_features:
            flag = config.get_feature_flag(feature)
            required_version = config.get_feature_version(feature)
            if flag is False:
                return self._unsupported_domain_query(
                    dialect,
                    domain,
                    query_name,
                    mode=mode,
                    risks=(MetadataRisk.VERSION_GATED,),
                    warnings=(f"{dialect}/{domain}/{query_name} requires {feature}",),
                )
            if version is not None and required_version is not None and version < required_version:
                return self._unsupported_domain_query(
                    dialect,
                    domain,
                    query_name,
                    mode=mode,
                    risks=(MetadataRisk.VERSION_GATED,),
                    warnings=(f"{dialect}/{domain}/{query_name} requires {feature} >= {required_version}",),
                )
            if flag is None and required_version is None:
                return self._unsupported_domain_query(
                    dialect,
                    domain,
                    query_name,
                    mode=mode,
                    risks=(MetadataRisk.VERSION_GATED,),
                    warnings=(f"{dialect}/{domain}/{query_name} requires unknown feature {feature}",),
                )
        return None

    def get_domain_query(
        self,
        dialect: str,
        domain: str,
        query_name: str,
        *,
        mode: str | None = None,
        version: "VersionInfo | None" = None,
        required_features: "tuple[str, ...]" = (),
    ) -> MetadataQuery:
        """Get a data-dictionary query by dialect, domain, and query name.

        Args:
            dialect: Dialect or dialect alias.
            domain: Metadata domain name.
            query_name: Query name inside the domain pack.
            mode: Optional SQL dialect mode for multi-mode engines.
            version: Optional database version used for feature gates.
            required_features: Feature flags/version gates required by the query.

        Returns:
            MetadataQuery containing SQL when supported, otherwise an unsupported status.
        """
        normalized_dialect = normalize_dialect_name(dialect)
        normalized_domain = _normalize_domain_key(domain)
        normalized_query = _normalize_query_key(query_name)
        normalized_mode = normalize_dialect_mode(normalized_dialect, mode)

        gated = self._feature_gate_query(
            normalized_dialect,
            normalized_domain,
            normalized_query,
            mode=normalized_mode,
            version=version,
            required_features=required_features,
        )
        if gated is not None:
            return gated

        if not self._ensure_domain_loaded(normalized_dialect, normalized_domain, normalized_mode):
            return self._unsupported_domain_query(
                normalized_dialect, normalized_domain, normalized_query, mode=normalized_mode
            )

        loader = self._get_domain_loader(normalized_dialect, normalized_domain, normalized_mode)
        if not loader.has_query(normalized_query):
            return self._unsupported_domain_query(
                normalized_dialect, normalized_domain, normalized_query, mode=normalized_mode
            )

        capability = MetadataCapability(
            domain=normalized_domain,
            support=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.CATALOG,
        )
        return MetadataQuery(
            dialect=normalized_dialect,
            domain=normalized_domain,
            name=normalized_query,
            mode=normalized_mode,
            sql=loader.get_sql(normalized_query),
            capability=capability,
        )

    def get_domain_queries(
        self,
        dialect: str,
        domain: str,
        query_names: "Iterable[str]",
        *,
        mode: str | None = None,
        version: "VersionInfo | None" = None,
        required_features: "tuple[str, ...]" = (),
    ) -> "dict[str, MetadataQuery]":
        """Get multiple data-dictionary queries from one domain pack.

        Args:
            dialect: Dialect or dialect alias.
            domain: Metadata domain name.
            query_names: Query names inside the domain pack.
            mode: Optional SQL dialect mode for multi-mode engines.
            version: Optional database version used for feature gates.
            required_features: Feature flags/version gates required by every query.

        Returns:
            Ordered mapping of normalized query names to metadata query results.
        """
        results: dict[str, MetadataQuery] = {}
        for query_name in query_names:
            normalized_query = _normalize_query_key(query_name)
            results[normalized_query] = self.get_domain_query(
                dialect, domain, normalized_query, mode=mode, version=version, required_features=required_features
            )
        return results

    def get_domain_query_text(
        self,
        dialect: str,
        domain: str,
        query_name: str,
        *,
        mode: str | None = None,
        version: "VersionInfo | None" = None,
        required_features: "tuple[str, ...]" = (),
    ) -> str | None:
        """Get raw SQL text for a domain query, or None when unsupported."""
        query = self.get_domain_query(
            dialect, domain, query_name, mode=mode, version=version, required_features=required_features
        )
        return query.query_text

    def get_dialect_config(self, dialect: str) -> "DialectConfig":
        """Get static configuration for a dialect.

        Args:
            dialect: Dialect name.

        Returns:
            DialectConfig for the dialect.
        """
        return get_dialect_config(dialect)

    def list_dialects(self) -> "list[str]":
        """List available SQL dialects.

        Returns:
            List of dialect names with SQL directories.
        """
        sql_root = _sql_resource_root()
        if not sql_root.is_dir():
            return []
        return sorted([path.name for path in sql_root.iterdir() if path.is_dir()])


_loader_instance: DataDictionaryLoader | None = None


def get_data_dictionary_loader() -> DataDictionaryLoader:
    """Get singleton data dictionary loader instance.

    Returns:
        DataDictionaryLoader singleton.
    """
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = DataDictionaryLoader()
    return _loader_instance


def _sql_resource_root() -> "Traversable":
    return resources.files(SQL_RESOURCE_PACKAGE).joinpath(SQL_RESOURCE_NAME)


def _normalize_domain_key(name: str) -> str:
    return slugify(name, separator="_")


def _normalize_query_key(name: str) -> str:
    return ".".join(slugify(part, separator="_") for part in name.split("."))
