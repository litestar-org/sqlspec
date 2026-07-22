"""Shared driver lifecycle contracts: connection pooling and on_connection_create hooks."""

from typing import cast

import pytest

from tests.integration.adapters._shared._cases import (
    DriverCaseContext,
    async_driver_params_with,
    sync_driver_params_with,
)
from tests.integration.adapters._shared.behaviors import (
    AsyncConfigFactory,
    SyncConfigFactory,
    assert_async_connection_hook_contract,
    assert_async_connection_instance_contract,
    assert_async_custom_json_serializer_contract,
    assert_async_lowercase_columns_contract,
    assert_async_pooling_contract,
    assert_async_uuid_feature_contract,
    assert_sync_connection_hook_contract,
    assert_sync_connection_instance_contract,
    assert_sync_custom_json_serializer_contract,
    assert_sync_custom_type_adapters_contract,
    assert_sync_lowercase_columns_contract,
    assert_sync_pooling_contract,
    assert_sync_uuid_feature_contract,
)


def _sync_factory(context: DriverCaseContext) -> SyncConfigFactory:
    assert context.make_config is not None, (
        f"{context.case.adapter} declares lifecycle support without a config factory"
    )
    return cast("SyncConfigFactory", context.make_config)


def _async_factory(context: DriverCaseContext) -> AsyncConfigFactory:
    assert context.make_config is not None, (
        f"{context.case.adapter} declares lifecycle support without a config factory"
    )
    return cast("AsyncConfigFactory", context.make_config)


@pytest.mark.parametrize("sync_lifecycle_driver_case", sync_driver_params_with("supports_pooling"), indirect=True)
def test_sync_pooling_contract(sync_lifecycle_driver_case: DriverCaseContext) -> None:
    """Sync pooled configs share data across sessions drawn from the same pool."""
    assert sync_lifecycle_driver_case.case.supports_pooling
    assert_sync_pooling_contract(_sync_factory(sync_lifecycle_driver_case), sync_lifecycle_driver_case.case)


@pytest.mark.parametrize("async_lifecycle_driver_case", async_driver_params_with("supports_pooling"), indirect=True)
async def test_async_pooling_contract(async_lifecycle_driver_case: DriverCaseContext) -> None:
    """Async pooled configs share data across sessions drawn from the same pool."""
    assert async_lifecycle_driver_case.case.supports_pooling
    await assert_async_pooling_contract(_async_factory(async_lifecycle_driver_case), async_lifecycle_driver_case.case)


@pytest.mark.parametrize(
    "sync_lifecycle_driver_case", sync_driver_params_with("supports_connection_instance"), indirect=True
)
def test_sync_connection_instance_contract(sync_lifecycle_driver_case: DriverCaseContext) -> None:
    """Sync configs honor an injected connection_instance pool."""
    assert sync_lifecycle_driver_case.case.supports_connection_instance
    assert_sync_connection_instance_contract(_sync_factory(sync_lifecycle_driver_case), sync_lifecycle_driver_case.case)


@pytest.mark.parametrize(
    "async_lifecycle_driver_case", async_driver_params_with("supports_connection_instance"), indirect=True
)
async def test_async_connection_instance_contract(async_lifecycle_driver_case: DriverCaseContext) -> None:
    """Async configs honor an injected connection_instance pool."""
    assert async_lifecycle_driver_case.case.supports_connection_instance
    await assert_async_connection_instance_contract(
        _async_factory(async_lifecycle_driver_case), async_lifecycle_driver_case.case
    )


@pytest.mark.parametrize(
    "sync_lifecycle_driver_case", sync_driver_params_with("supports_connection_hook"), indirect=True
)
def test_sync_connection_hook_contract(sync_lifecycle_driver_case: DriverCaseContext) -> None:
    """Sync adapters invoke the on_connection_create driver-feature hook."""
    assert sync_lifecycle_driver_case.case.supports_connection_hook
    assert_sync_connection_hook_contract(_sync_factory(sync_lifecycle_driver_case), sync_lifecycle_driver_case.case)


@pytest.mark.parametrize(
    "async_lifecycle_driver_case", async_driver_params_with("supports_connection_hook"), indirect=True
)
async def test_async_connection_hook_contract(async_lifecycle_driver_case: DriverCaseContext) -> None:
    """Async adapters invoke the on_connection_create driver-feature hook."""
    assert async_lifecycle_driver_case.case.supports_connection_hook
    await assert_async_connection_hook_contract(
        _async_factory(async_lifecycle_driver_case), async_lifecycle_driver_case.case
    )


@pytest.mark.parametrize(
    "sync_lifecycle_driver_case", sync_driver_params_with("supports_lowercase_columns"), indirect=True
)
def test_sync_lowercase_columns_contract(sync_lifecycle_driver_case: DriverCaseContext) -> None:
    """Sync drivers honor the lowercase-column-name driver feature (default on; uppercase when disabled)."""
    assert sync_lifecycle_driver_case.case.supports_lowercase_columns
    assert_sync_lowercase_columns_contract(_sync_factory(sync_lifecycle_driver_case), sync_lifecycle_driver_case.case)


@pytest.mark.parametrize(
    "async_lifecycle_driver_case", async_driver_params_with("supports_lowercase_columns"), indirect=True
)
async def test_async_lowercase_columns_contract(async_lifecycle_driver_case: DriverCaseContext) -> None:
    """Async drivers honor the lowercase-column-name driver feature (default on; uppercase when disabled)."""
    assert async_lifecycle_driver_case.case.supports_lowercase_columns
    await assert_async_lowercase_columns_contract(
        _async_factory(async_lifecycle_driver_case), async_lifecycle_driver_case.case
    )


@pytest.mark.parametrize("sync_lifecycle_driver_case", sync_driver_params_with("supports_uuid_feature"), indirect=True)
def test_sync_uuid_feature_contract(sync_lifecycle_driver_case: DriverCaseContext) -> None:
    """Sync drivers honor the UUID driver feature (enabled binds/returns uuid.UUID; disabled returns raw form)."""
    assert sync_lifecycle_driver_case.case.supports_uuid_feature
    assert_sync_uuid_feature_contract(_sync_factory(sync_lifecycle_driver_case), sync_lifecycle_driver_case.case)


@pytest.mark.parametrize(
    "async_lifecycle_driver_case", async_driver_params_with("supports_uuid_feature"), indirect=True
)
async def test_async_uuid_feature_contract(async_lifecycle_driver_case: DriverCaseContext) -> None:
    """Async drivers honor the UUID driver feature (enabled binds/returns uuid.UUID; disabled returns raw form)."""
    assert async_lifecycle_driver_case.case.supports_uuid_feature
    await assert_async_uuid_feature_contract(
        _async_factory(async_lifecycle_driver_case), async_lifecycle_driver_case.case
    )


@pytest.mark.parametrize(
    "sync_lifecycle_driver_case", sync_driver_params_with("supports_custom_json_serializer"), indirect=True
)
def test_sync_custom_json_serializer_contract(sync_lifecycle_driver_case: DriverCaseContext) -> None:
    """Sync drivers invoke a custom json_serializer driver feature when binding a dict to a JSON column."""
    assert sync_lifecycle_driver_case.case.supports_custom_json_serializer
    assert_sync_custom_json_serializer_contract(
        _sync_factory(sync_lifecycle_driver_case), sync_lifecycle_driver_case.case
    )


@pytest.mark.parametrize(
    "async_lifecycle_driver_case", async_driver_params_with("supports_custom_json_serializer"), indirect=True
)
async def test_async_custom_json_serializer_contract(async_lifecycle_driver_case: DriverCaseContext) -> None:
    """Async drivers invoke a custom json_serializer driver feature when binding a dict to a JSON column."""
    assert async_lifecycle_driver_case.case.supports_custom_json_serializer
    await assert_async_custom_json_serializer_contract(
        _async_factory(async_lifecycle_driver_case), async_lifecycle_driver_case.case
    )


@pytest.mark.parametrize(
    "sync_lifecycle_driver_case", sync_driver_params_with("supports_custom_type_adapters"), indirect=True
)
def test_sync_custom_type_adapters_contract(sync_lifecycle_driver_case: DriverCaseContext) -> None:
    """Sync drivers hydrate JSON columns to dict/list with custom type adapters enabled (str without)."""
    assert sync_lifecycle_driver_case.case.supports_custom_type_adapters
    assert_sync_custom_type_adapters_contract(
        _sync_factory(sync_lifecycle_driver_case), sync_lifecycle_driver_case.case
    )
