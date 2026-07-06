"""Tests for replacement data-dictionary metadata contracts."""

from typing import cast

from sqlspec.data_dictionary import (
    DDLResult,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataResult,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    TableDetails,
)


def test_metadata_capability_round_trip() -> None:
    """Capability records serialize stable string vocabularies."""
    capability = MetadataCapability(
        domain="tables",
        support=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.CATALOG,
        risks=(MetadataRisk.PRIVILEGED,),
        warnings=("filtered by privileges",),
    )

    payload = capability.to_dict()

    assert payload == {
        "domain": "tables",
        "support": "supported",
        "fidelity": "native",
        "source": "catalog",
        "risks": ("privileged",),
        "warnings": ("filtered by privileges",),
    }
    assert MetadataCapability.from_dict(payload) == capability


def test_table_metadata_includes_identity_and_source() -> None:
    """Rich table metadata carries object identity and source fidelity."""
    identity = ObjectIdentity(
        name="orders",
        object_type="table",
        schema="public",
        catalog="app",
        dialect="postgres",
        quoted_name='"public"."orders"',
        source=MetadataSource.CATALOG,
    )
    table = TableDetails(
        identity=identity,
        table_type="BASE TABLE",
        ddl=DDLResult(
            identity=identity,
            status=MetadataSupport.SUPPORTED,
            fidelity=MetadataFidelity.NATIVE,
            source=MetadataSource.NATIVE_API,
            ddl="CREATE TABLE orders (id integer)",
        ),
    )

    assert table.identity == identity
    assert table.source == MetadataSource.CATALOG
    table_payload = table.to_dict()
    identity_payload = cast("dict[str, object]", table_payload["identity"])
    assert identity_payload["quoted_name"] == '"public"."orders"'
    assert table.ddl is not None
    assert table.ddl.to_dict()["fidelity"] == "native"


def test_capability_profile_reports_unsupported_domains() -> None:
    """Profiles distinguish unsupported from empty metadata."""
    profile = MetadataCapabilityProfile(
        dialect="sqlite",
        adapter="sqlite",
        capabilities=(
            MetadataCapability(domain="tables", support=MetadataSupport.SUPPORTED),
            MetadataCapability(domain="privileges", support=MetadataSupport.UNSUPPORTED),
        ),
    )

    assert profile.get("tables").support == MetadataSupport.SUPPORTED
    assert profile.get("privileges").support == MetadataSupport.UNSUPPORTED
    assert profile.get("routines").support == MetadataSupport.UNKNOWN
    profile_payload = profile.to_dict()
    capabilities = cast("tuple[dict[str, object], ...]", profile_payload["capabilities"])
    assert capabilities[1]["support"] == "unsupported"


def test_metadata_result_reports_unsupported_without_false_empty() -> None:
    """Unsupported metadata domains use a uniform result wrapper."""
    result = MetadataResult.unsupported("privileges")

    assert result.domain == "privileges"
    assert result.items == ()
    assert result.capability.support == MetadataSupport.UNSUPPORTED
    result_payload = result.to_dict()
    capability_payload = cast("dict[str, object]", result_payload["capability"])
    assert capability_payload["support"] == "unsupported"
