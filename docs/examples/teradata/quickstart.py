"""Configure a Teradata Vantage connection without opening a session."""

# --8<-- [start:example]
import os

from sqlspec.adapters.teradata import TeradataConfig

config = TeradataConfig(
    connection_config={
        "host": os.getenv("TERADATA_HOST", "td.example.internal"),
        "user": os.getenv("TERADATA_USER", "app"),
        "password": os.getenv("TERADATA_PASSWORD", ""),
        "dbs_port": "1025",
        "tmode": "TERA",
    }
)
# --8<-- [end:example]

__all__ = ("main",)


def main() -> TeradataConfig:
    """Return a configuration without connecting to an external database."""
    return config


if __name__ == "__main__":
    print(main())
