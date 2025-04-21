from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from google.cloud.bigquery import LoadJobConfig, QueryJobConfig

if TYPE_CHECKING:
    from google.api_core.client_info import ClientInfo
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import Credentials

__all__ = ("BigQueryConnectionConfigCommon",)


@dataclass
class BigQueryConnectionConfigCommon:
    """Common configuration options for BigQuery."""

    project: "Optional[str]" = field(default=None)
    """Google Cloud project ID."""
    location: "Optional[str]" = field(default=None)
    """Default geographic location for jobs and datasets."""
    credentials: "Optional[Credentials]" = field(default=None, hash=False)
    """Credentials to use for authentication."""
    dataset_id: "Optional[str]" = field(default=None)
    """Default dataset ID to use if not specified in queries."""
    credentials_path: "Optional[str]" = field(default=None)
    """Path to Google Cloud service account key file (JSON). If None, attempts default authentication."""
    client_options: "Optional[ClientOptions]" = field(default=None, hash=False)
    """Client options used to set user options on the client (e.g., api_endpoint)."""
    default_query_job_config: "Optional[QueryJobConfig]" = field(default=None, hash=False)
    """Default QueryJobConfig settings."""
    default_load_job_config: "Optional[LoadJobConfig]" = field(default=None, hash=False)
    """Default LoadJobConfig settings."""
    client_info: "Optional[ClientInfo]" = field(default=None, hash=False)
    """Client info used to send a user-agent string along with API requests."""

    def __post_init__(self) -> None:
        """Post-initialization hook."""
        if self.default_query_job_config is None:
            self.default_query_job_config = QueryJobConfig(default_dataset=self.dataset_id)
