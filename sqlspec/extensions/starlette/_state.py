from dataclasses import dataclass
from typing import Literal

from sqlspec.extensions._framework_common import BaseConfigState

__all__ = ("CommitMode", "SQLSpecConfigState")

CommitMode = Literal["manual", "autocommit", "autocommit_include_redirect"]


@dataclass
class SQLSpecConfigState(BaseConfigState):
    """Internal state for each database configuration.

    Tracks all configuration parameters needed for middleware and session management.
    """

    sqlcommenter_framework: str = "starlette"
