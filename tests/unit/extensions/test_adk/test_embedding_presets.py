"""Unit tests for the ADK embedding preset registry."""

import pytest

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk.memory.presets import (
    EMBEDDING_PRESETS,
    EmbeddingPreset,
    register_embedding_preset,
    resolve_embedding_config,
)


def test_default_presets_register_canonical_names() -> None:
    """The bundled preset catalog covers the locked Vertex and OpenAI models."""
    for name in (
        "gemini-embedding-002",
        "gemini-embedding-001",
        "embeddinggemma-300m",
        "text-embedding-005",
        "text-embedding-004",
        "text-embedding-3-large",
        "text-embedding-3-small",
        "text-embedding-ada-002",
    ):
        assert name in EMBEDDING_PRESETS


def test_resolve_with_preset_returns_preset_dimensions() -> None:
    resolved = resolve_embedding_config({"embedding_preset": "gemini-embedding-002"})
    assert resolved.dim == 1536
    assert resolved.precision == "float32"
    assert resolved.normalize is True
    assert resolved.source == "embedding_preset"
    assert resolved.preset is not None
    assert resolved.preset.name == "gemini-embedding-002"


def test_resolve_with_explicit_dimension_overrides_preset() -> None:
    resolved = resolve_embedding_config({"embedding_dimension": 3072, "embedding_preset": "embeddinggemma-300m"})
    assert resolved.dim == 3072
    assert resolved.precision == "float32"
    assert resolved.source == "embedding_dimension"
    assert resolved.preset is not None  # preset retained for diagnostics


def test_resolve_with_explicit_precision_and_normalize() -> None:
    resolved = resolve_embedding_config({
        "embedding_dimension": 1024,
        "embedding_precision": "halfvec",
        "embedding_normalize": False,
    })
    assert resolved.dim == 1024
    assert resolved.precision == "halfvec"
    assert resolved.normalize is False


def test_resolve_empty_config_raises_with_preset_table() -> None:
    with pytest.raises(ImproperConfigurationError, match="gemini-embedding-002"):
        resolve_embedding_config(None)


def test_resolve_unknown_preset_lists_available_presets() -> None:
    with pytest.raises(ImproperConfigurationError, match="text-embedding-3-large"):
        resolve_embedding_config({"embedding_preset": "no-such-model"})


def test_resolve_rejects_non_int_dimension() -> None:
    with pytest.raises(ImproperConfigurationError, match="must be an int"):
        resolve_embedding_config({"embedding_dimension": "1024"})


def test_register_embedding_preset_extends_registry() -> None:
    custom = EmbeddingPreset(name="custom-mini", dim=256, precision="float32", normalize=True, note="Test-only preset.")
    try:
        register_embedding_preset("custom-mini", custom)
        resolved = resolve_embedding_config({"embedding_preset": "custom-mini"})
        assert resolved.dim == 256
        assert resolved.preset is custom
    finally:
        EMBEDDING_PRESETS.pop("custom-mini", None)
