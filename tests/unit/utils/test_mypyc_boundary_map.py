"""Tests for mypyc boundary inventory reporting."""

import importlib.util
from pathlib import Path
from types import ModuleType


def _load_mypyc_boundary_map_module() -> ModuleType:
    module_path = Path(__file__).resolve().parents[3] / "tools" / "scripts" / "mypyc_boundary_map.py"
    spec = importlib.util.spec_from_file_location("mypyc_boundary_map_for_tests", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_boundary_map_reports_expected_hot_groups() -> None:
    module = _load_mypyc_boundary_map_module()

    boundary_map = module.build_boundary_map()

    assert boundary_map["summary"] == {
        "config_runtime_edges": 2,
        "adapter_config_core_edges": 17,
        "interpreted_to_compiled_adapter_edges": 16,
        "serializer_bridges": 18,
        "storage_arrow_edges": 3,
        "any_audit_seams": 8,
        "exclusion_revalidation_buckets": 7,
    }


def test_build_boundary_map_captures_config_and_storage_runtime_edges() -> None:
    module = _load_mypyc_boundary_map_module()

    boundary_map = module.build_boundary_map()

    config_boundaries = {
        (entry["from_module"], entry["to_module"]): entry for entry in boundary_map["config_runtime_boundaries"]
    }
    config_runtime_edge = config_boundaries[("sqlspec/config.py", "sqlspec/core/config_runtime.py")]
    assert config_runtime_edge["classification"] == "interpreted_runtime_helper_boundary"
    assert config_runtime_edge["from_status"] == "interpreted"
    assert config_runtime_edge["to_status"] == "interpreted"

    module_loader_edge = config_boundaries[("sqlspec/config.py", "sqlspec/utils/module_loader.py")]
    assert module_loader_edge["classification"] == "interpreted_optional_dependency_boundary"
    assert module_loader_edge["sites"][1]["symbol"] == "_build_storage_capabilities"

    storage_boundaries = {
        (entry["from_module"], entry["to_module"]): entry for entry in boundary_map["storage_arrow_boundaries"]
    }
    pipeline_edge = storage_boundaries[("sqlspec/storage/pipeline.py", "sqlspec/storage/_utils.py")]
    assert pipeline_edge["classification"] == "interpreted_to_interpreted_arrow_boundary"
    assert pipeline_edge["sites"][3]["symbol"] == "SyncStoragePipeline.write_arrow"


def test_build_boundary_map_lists_adapter_config_to_compiled_core_crossings() -> None:
    module = _load_mypyc_boundary_map_module()

    boundary_map = module.build_boundary_map()

    adapter_edges = {
        (entry["from_module"], entry["to_module"]): entry for entry in boundary_map["adapter_config_core_boundaries"]
    }

    psqlpy_edge = adapter_edges[("sqlspec/adapters/psqlpy/config.py", "sqlspec/adapters/psqlpy/core.py")]
    assert psqlpy_edge["classification"] == "interpreted_to_compiled"
    assert psqlpy_edge["helpers"] == [
        "apply_driver_features",
        "build_connection_config",
        "build_postgres_extension_probe_names",
        "default_statement_config",
        "resolve_postgres_extension_state",
        "resolve_runtime_statement_config",
    ]

    asyncpg_edge = adapter_edges[("sqlspec/adapters/asyncpg/config.py", "sqlspec/adapters/asyncpg/core.py")]
    assert asyncpg_edge["classification"] == "interpreted_to_compiled"
    assert "register_json_codecs" in asyncpg_edge["helpers"]
    assert "register_pgvector_support" in asyncpg_edge["helpers"]

    mock_edge = adapter_edges[("sqlspec/adapters/mock/config.py", "sqlspec/adapters/mock/core.py")]
    assert mock_edge["classification"] == "same_mode_import"
    assert mock_edge["to_status"] == "interpreted"


def test_build_boundary_map_tracks_serializer_and_any_seams() -> None:
    module = _load_mypyc_boundary_map_module()

    boundary_map = module.build_boundary_map()

    serializer_edges = {entry["from_module"]: entry for entry in boundary_map["serializer_bridges"]}
    assert serializer_edges["sqlspec/adapters/psqlpy/core.py"]["terminal_module"] == "sqlspec/_serialization.py"
    assert serializer_edges["sqlspec/adapters/psqlpy/core.py"]["helpers"] == ["to_json"]
    assert serializer_edges["sqlspec/adapters/asyncpg/core.py"]["helpers"] == ["from_json", "to_json"]
    assert serializer_edges["sqlspec/core/parameters/_registry.py"]["classification"] == (
        "compiled_to_interpreted_json_boundary"
    )

    any_seams = {
        (entry["module"], entry["symbol"]): entry for entry in boundary_map["any_audit_matrix"]
    }
    assert any_seams[("sqlspec/config.py", "_DriverFeatureHookWrapper.__init__")]["annotation"] == "Callable[..., Any]"
    assert any_seams[("sqlspec/storage/pipeline.py", "_encode_arrow_payload")]["annotation"] == (
        "write_options: dict[str, Any] | None"
    )


def test_build_boundary_map_seeds_exclusion_revalidation_buckets() -> None:
    module = _load_mypyc_boundary_map_module()

    boundary_map = module.build_boundary_map()

    exclusion_seed = boundary_map["exclusion_revalidation_seed"]
    assert exclusion_seed["sqlspec/utils/arrow_helpers.py"]["bucket"] == "hard_block"
    assert exclusion_seed["sqlspec/adapters/**/data_dictionary.py"]["bucket"] == "hard_block"
    assert exclusion_seed["sqlspec/builder/_vector_expressions.py"]["bucket"] == "helper_split"
    assert exclusion_seed["sqlspec/data_dictionary/_loader.py"]["bucket"] == "helper_split"
    assert exclusion_seed["sqlspec/dialects/**"]["bucket"] == "low_roi"
