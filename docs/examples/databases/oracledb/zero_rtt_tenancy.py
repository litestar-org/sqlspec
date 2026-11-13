"""Read Oracle client-context attributes from a captured tenant lease."""

# --8<-- [start:example]
import asyncio
import os

from sqlspec import TenantContext
from sqlspec.adapters.oracledb import OracleAsyncClientContextStrategy, OracleAsyncConfig, OraclePoolParams


async def run_example() -> dict[str, object]:
    """Bind a tenant and read its native client-context attributes.

    Returns:
        The tenant and region reported by the server.
    """
    config = OracleAsyncConfig(
        connection_config=OraclePoolParams(
            dsn=os.getenv("ORACLE_DSN", "localhost:1521/FREEPDB1"),
            user=os.getenv("ORACLE_USER", "app"),
            password=os.getenv("ORACLE_PASSWORD", "oracle"),
            min=1,
            max=2,
        ),
        driver_features={
            "tenant_strategy": OracleAsyncClientContextStrategy(variable_keys=("region",)),
            "use_tenant_id_as_sharding_key": True,
            "enable_otel_trace_context": True,
        },
    )
    try:
        with TenantContext.scope("acme-corp", variables={"region": "us-west"}):
            async with config.provide_session() as session:
                return await session.select_one(
                    "SELECT SYS_CONTEXT('CLIENTCONTEXT', 'TENANT_ID') AS tenant_id, "
                    "SYS_CONTEXT('CLIENTCONTEXT', 'region') AS region FROM DUAL"
                )
    finally:
        await config.close_pool()


# --8<-- [end:example]

__all__ = ("main", "run_example")


def main() -> None:
    """Execute the Oracle tenant-context example."""
    if not os.getenv("ORACLE_DSN"):
        print("Set ORACLE_DSN, ORACLE_USER, and ORACLE_PASSWORD to run this example.")
        return
    print(asyncio.run(run_example()))


if __name__ == "__main__":
    main()
