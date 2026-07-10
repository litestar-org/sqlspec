from dataclasses import dataclass
from typing import Literal

from sqlspec.extensions._framework_common import BaseConfigState

__all__ = ("CommitMode", "SanicConfigState")

CommitMode = Literal["manual", "autocommit", "autocommit_include_redirect"]


@dataclass
class SanicConfigState(BaseConfigState):
    """Internal state for a Sanic database configuration.

    Tracks the keys and behavior needed to bind one SQLSpec config into a
    Sanic app and its request context.
    """

    sqlcommenter_framework: str = "sanic"
