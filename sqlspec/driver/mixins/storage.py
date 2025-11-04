"""Storage bridge mixin shared by sync and async drivers."""

from typing import TYPE_CHECKING, Any, cast

from mypy_extensions import trait

from sqlspec.exceptions import StorageCapabilityError
from sqlspec.storage import (
    AsyncStoragePipeline,
    StorageBridgeJob,
    StorageCapabilities,
    StorageDestination,
    StorageFormat,
    StorageTelemetry,
    SyncStoragePipeline,
    create_storage_bridge_job,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.core import StatementConfig, StatementFilter
    from sqlspec.core.result import ArrowResult
    from sqlspec.core.statement import SQL
    from sqlspec.typing import StatementParameters

__all__ = ("StorageDriverMixin",)


CAPABILITY_HINTS: dict[str, str] = {
    "arrow_export_enabled": "native Arrow export",
    "arrow_import_enabled": "native Arrow import",
    "parquet_export_enabled": "native Parquet export",
    "parquet_import_enabled": "native Parquet import",
}


@trait
class StorageDriverMixin:
    """Mixin providing capability-aware storage bridge helpers."""

    __slots__ = ()
    storage_pipeline_factory: "type[SyncStoragePipeline | AsyncStoragePipeline] | None" = None
    driver_features: dict[str, Any]

    def storage_capabilities(self) -> StorageCapabilities:
        """Return cached storage capabilities for the active driver."""

        capabilities = self.driver_features.get("storage_capabilities")
        if capabilities is None:
            msg = "Storage capabilities are not configured for this driver."
            raise StorageCapabilityError(msg, capability="storage_capabilities")
        return cast("StorageCapabilities", dict(capabilities))

    def select_to_storage(
        self,
        statement: "SQL | str",
        destination: StorageDestination,
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, Any] | None" = None,
        format_hint: StorageFormat | None = None,
        telemetry: StorageTelemetry | None = None,
    ) -> "StorageBridgeJob | Awaitable[StorageBridgeJob]":
        """Stream a SELECT statement directly into storage."""

        self._raise_not_implemented("select_to_storage")
        raise NotImplementedError

    def select_to_arrow(
        self,
        statement: "SQL | str",
        /,
        *parameters: "StatementParameters | StatementFilter",
        partitioner: "dict[str, Any] | None" = None,
        memory_pool: Any | None = None,
        statement_config: "StatementConfig | None" = None,
    ) -> "ArrowResult | Awaitable[ArrowResult]":
        """Execute a SELECT that returns an ArrowResult."""

        self._raise_not_implemented("select_to_arrow")
        raise NotImplementedError

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob | Awaitable[StorageBridgeJob]":
        """Load Arrow data into the target table."""

        self._raise_not_implemented("load_from_arrow")
        raise NotImplementedError

    def load_from_storage(
        self,
        table: str,
        source: StorageDestination,
        *,
        file_format: StorageFormat,
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob | Awaitable[StorageBridgeJob]":
        """Load artifacts from storage into the target table."""

        self._raise_not_implemented("load_from_storage")
        raise NotImplementedError

    def stage_artifact(self, request: "dict[str, Any]") -> "dict[str, Any]":
        """Provision staging metadata for adapters that require remote URIs."""

        self._raise_not_implemented("stage_artifact")
        raise NotImplementedError

    def flush_staging_artifacts(self, artifacts: "list[dict[str, Any]]", *, error: Exception | None = None) -> None:
        """Clean up staged artifacts after a job completes."""

        if artifacts:
            self._raise_not_implemented("flush_staging_artifacts")

    def get_storage_job(self, job_id: str) -> StorageBridgeJob | None:
        """Fetch a previously created job handle."""

        return None

    def _storage_pipeline(self) -> "SyncStoragePipeline | AsyncStoragePipeline":
        factory = self.storage_pipeline_factory
        if factory is None:
            if getattr(self, "is_async", False):
                return AsyncStoragePipeline()
            return SyncStoragePipeline()
        return factory()

    def _raise_not_implemented(self, capability: str) -> None:
        msg = f"{capability} is not implemented for this driver"
        remediation = "Override StorageDriverMixin methods on the adapter to enable this capability."
        raise StorageCapabilityError(msg, capability=capability, remediation=remediation)

    def _require_capability(self, capability_flag: str) -> None:
        capabilities = self.storage_capabilities()
        if capabilities.get(capability_flag, False):
            return
        human_label = CAPABILITY_HINTS.get(capability_flag, capability_flag)
        remediation = "Check adapter supports this capability or stage artifacts via storage pipeline."
        msg = f"{human_label} is not available for this adapter"
        raise StorageCapabilityError(msg, capability=capability_flag, remediation=remediation)

    def _attach_partition_telemetry(self, telemetry: StorageTelemetry, partitioner: "dict[str, Any] | None") -> None:
        if not partitioner:
            return
        extra = dict(telemetry.get("extra", {}))
        extra["partitioner"] = partitioner
        telemetry["extra"] = extra

    def _create_storage_job(
        self, produced: StorageTelemetry, provided: StorageTelemetry | None = None, *, status: str = "completed"
    ) -> StorageBridgeJob:
        merged = cast("StorageTelemetry", dict(produced))
        if provided:
            merged.update(provided)
        return create_storage_bridge_job(status, merged)
