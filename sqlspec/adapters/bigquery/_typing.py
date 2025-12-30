from typing import TYPE_CHECKING

from google.cloud.bigquery import Client

if TYPE_CHECKING:
    from typing import TypeAlias

    BigQueryConnection: TypeAlias = Client
else:
    BigQueryConnection = Client

__all__ = ("BigQueryConnection",)
