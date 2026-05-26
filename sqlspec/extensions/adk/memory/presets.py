"""Embedding preset registry for the ADK memory store.

Resolution order (highest priority first):

1. ``embedding_dimension`` explicit override on the ``memory`` block
2. ``embedding_preset`` name resolved against :data:`EMBEDDING_PRESETS`
3. Raise ``ADKConfigError`` with the preset table referenced in the error message.

Presets capture the dimension, default precision, normalization expectation, and
a human-readable note used in dim-mismatch errors. Application code can register
runtime extensions via :func:`register_embedding_preset`.
"""

from dataclasses import dataclass
from typing import Final, NoReturn

from sqlspec.exceptions import ImproperConfigurationError

__all__ = (
    "DEFAULT_EMBEDDING_PRESET",
    "EMBEDDING_PRESETS",
    "EmbeddingPreset",
    "ResolvedEmbeddingConfig",
    "register_embedding_preset",
    "resolve_embedding_config",
)


@dataclass(frozen=True, slots=True)
class EmbeddingPreset:
    """Static description of a known embedding model output."""

    name: str
    dim: int
    precision: str
    normalize: bool
    note: str


@dataclass(frozen=True, slots=True)
class ResolvedEmbeddingConfig:
    """Resolved embedding configuration used by the memory store."""

    dim: int
    precision: str
    normalize: bool
    source: str
    preset: "EmbeddingPreset | None" = None


_DEFAULT_PRESETS: Final[tuple[EmbeddingPreset, ...]] = (
    EmbeddingPreset(
        name="gemini-embedding-002",
        dim=1536,
        precision="float32",
        normalize=True,
        note="Google Vertex AI gemini-embedding-002, normalized cosine vectors.",
    ),
    EmbeddingPreset(
        name="gemini-embedding-001",
        dim=768,
        precision="float32",
        normalize=True,
        note="Google Vertex AI gemini-embedding-001 (legacy generation).",
    ),
    EmbeddingPreset(
        name="embeddinggemma-300m",
        dim=768,
        precision="float32",
        normalize=True,
        note="Google EmbeddingGemma 300M open-weights model.",
    ),
    EmbeddingPreset(
        name="text-embedding-005",
        dim=768,
        precision="float32",
        normalize=True,
        note="Google Vertex AI text-embedding-005.",
    ),
    EmbeddingPreset(
        name="text-embedding-004",
        dim=768,
        precision="float32",
        normalize=True,
        note="Google Vertex AI text-embedding-004 (legacy generation).",
    ),
    EmbeddingPreset(
        name="text-embedding-3-large",
        dim=3072,
        precision="float32",
        normalize=True,
        note="OpenAI text-embedding-3-large; supports MRL truncation.",
    ),
    EmbeddingPreset(
        name="text-embedding-3-small",
        dim=1536,
        precision="float32",
        normalize=True,
        note="OpenAI text-embedding-3-small; supports MRL truncation.",
    ),
    EmbeddingPreset(
        name="text-embedding-ada-002",
        dim=1536,
        precision="float32",
        normalize=True,
        note="OpenAI text-embedding-ada-002 (legacy).",
    ),
)

EMBEDDING_PRESETS: dict[str, EmbeddingPreset] = {preset.name: preset for preset in _DEFAULT_PRESETS}


def register_embedding_preset(name: str, preset: EmbeddingPreset) -> None:
    """Register or replace an embedding preset at runtime.

    Args:
        name: Preset key. Lowercased registry lookup is intentionally exact.
        preset: ``EmbeddingPreset`` value.
    """
    EMBEDDING_PRESETS[name] = preset


DEFAULT_EMBEDDING_PRESET: Final[str] = "gemini-embedding-002"


def resolve_embedding_config(memory_config: "dict[str, object] | None") -> ResolvedEmbeddingConfig:
    """Resolve a :class:`ResolvedEmbeddingConfig` from an ADK memory config mapping.

    Args:
        memory_config: ``extension_config["adk"]["memory"]`` mapping.

    Returns:
        Resolved embedding configuration. Falls back to
        :data:`DEFAULT_EMBEDDING_PRESET` when neither ``embedding_dimension`` nor
        ``embedding_preset`` is supplied.

    Raises:
        ImproperConfigurationError: When the named preset is unknown or
            ``embedding_dimension`` is not an int.
    """
    config = memory_config or {}
    preset_name = config.get("embedding_preset")
    explicit_dim = config.get("embedding_dimension")
    explicit_precision = config.get("embedding_precision")
    explicit_normalize = config.get("embedding_normalize")

    preset = None
    if isinstance(preset_name, str):
        preset = EMBEDDING_PRESETS.get(preset_name)
        if preset is None:
            _raise_unknown_preset(preset_name)

    if explicit_dim is not None:
        if not isinstance(explicit_dim, int):
            msg = f"embedding_dimension must be an int, got {type(explicit_dim).__name__}"
            raise ImproperConfigurationError(msg)
        return ResolvedEmbeddingConfig(
            dim=explicit_dim,
            precision=str(explicit_precision) if explicit_precision else (preset.precision if preset else "float32"),
            normalize=bool(explicit_normalize)
            if explicit_normalize is not None
            else (preset.normalize if preset else True),
            source="embedding_dimension",
            preset=preset,
        )

    if preset is None:
        preset = EMBEDDING_PRESETS[DEFAULT_EMBEDDING_PRESET]
        source = "default"
    else:
        source = "embedding_preset"

    return ResolvedEmbeddingConfig(
        dim=preset.dim,
        precision=str(explicit_precision) if explicit_precision else preset.precision,
        normalize=bool(explicit_normalize) if explicit_normalize is not None else preset.normalize,
        source=source,
        preset=preset,
    )


def _raise_unknown_preset(name: str) -> NoReturn:
    available = ", ".join(sorted(EMBEDDING_PRESETS))
    msg = f"Unknown embedding preset {name!r}. Available presets: {available}"
    raise ImproperConfigurationError(msg)
