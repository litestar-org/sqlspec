"""The Litestar extension exposes the canonical configuration type."""

from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar import LitestarConfig as PublicLitestarConfig
from sqlspec.extensions.litestar.config import LitestarConfig as ExtensionLitestarConfig


def test_litestar_config_reexports() -> None:
    assert PublicLitestarConfig is LitestarConfig
    assert ExtensionLitestarConfig is LitestarConfig
