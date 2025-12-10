# Google Cloud SQL and AlloyDB Connector Guide

This guide covers integration of Google Cloud SQL and AlloyDB connectors with SQLSpec, providing simplified authentication, automatic SSL configuration, and streamlined connection management for Google Cloud managed PostgreSQL instances.

## Overview

Google Cloud provides official Python connectors for Cloud SQL and AlloyDB that handle authentication, SSL encryption, and network routing automatically. SQLSpec integrates these connectors through the AsyncPG adapter using a connection factory pattern.

### When to Use Cloud Connectors

Use Cloud SQL or AlloyDB connectors when:

- Deploying applications on Google Cloud Platform (GCP)
- Requiring IAM-based database authentication
- Managing SSL certificates automatically
- Connecting to private IP instances from GCP resources
- Simplifying credential rotation and management

### Supported Adapters

- **AsyncPG**: Full support (recommended)
- **Psycopg**: Not officially supported by Google connectors
- **Psqlpy**: Architecturally incompatible (internal Rust driver)
- **ADBC**: Incompatible (URI-only interface)

For unsupported adapters, use [Cloud SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy) as an alternative.

## Quick Start

### Installation

Install the connector packages alongside SQLSpec:

```bash
# Cloud SQL connector
pip install sqlspec[asyncpg] cloud-sql-python-connector

# AlloyDB connector
pip install sqlspec[asyncpg] cloud-alloydb-python-connector

# Both connectors
pip install sqlspec[asyncpg] cloud-sql-python-connector cloud-alloydb-python-connector
```

### Basic Cloud SQL Connection

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

sql = SQLSpec()

config = AsyncpgConfig(
    connection_config={
        "user": "postgres",
        "password": "secret",
        "database": "mydb",
    },
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "my-project:us-central1:my-instance",
    }
)
sql.add_config(config)

async with sql.provide_session(config) as session:
    users = await session.select_all("SELECT * FROM users")
    print(users)
```

### Basic AlloyDB Connection

```python
config = AsyncpgConfig(
    connection_config={
        "user": "postgres",
        "password": "secret",
        "database": "mydb",
    },
    driver_features={
        "enable_alloydb": True,
        "alloydb_instance_uri": "projects/my-project/locations/us-central1/clusters/my-cluster/instances/my-instance",
    }
)
```

## Configuration Reference

### Cloud SQL Driver Features

All Cloud SQL configuration is specified in `driver_features`:

```python
driver_features = {
    "enable_cloud_sql": True,  # Auto-detected when package installed
    "cloud_sql_instance": "project:region:instance",  # Required
    "cloud_sql_enable_iam_auth": False,  # Optional (default: False)
    "cloud_sql_ip_type": "PRIVATE",  # Optional (default: "PRIVATE")
}
```

**Field Descriptions**:

- `enable_cloud_sql`: Enable Cloud SQL connector integration. Defaults to `True` when `cloud-sql-python-connector` is installed.
- `cloud_sql_instance`: Instance connection name in format `"project:region:instance"`. Required when connector is enabled.
- `cloud_sql_enable_iam_auth`: Use IAM authentication instead of password. Defaults to `False`.
- `cloud_sql_ip_type`: IP address type for connection. Options: `"PUBLIC"`, `"PRIVATE"`, or `"PSC"`. Defaults to `"PRIVATE"`.

### AlloyDB Driver Features

All AlloyDB configuration is specified in `driver_features`:

```python
driver_features = {
    "enable_alloydb": True,  # Auto-detected when package installed
    "alloydb_instance_uri": "projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE",  # Required
    "alloydb_enable_iam_auth": False,  # Optional (default: False)
    "alloydb_ip_type": "PRIVATE",  # Optional (default: "PRIVATE")
}
```

**Field Descriptions**:

- `enable_alloydb`: Enable AlloyDB connector integration. Defaults to `True` when `cloud-alloydb-python-connector` is installed.
- `alloydb_instance_uri`: Instance URI in full resource path format. Required when connector is enabled.
- `alloydb_enable_iam_auth`: Use IAM authentication instead of password. Defaults to `False`.
- `alloydb_ip_type`: IP address type for connection. Options: `"PUBLIC"`, `"PRIVATE"`, or `"PSC"`. Defaults to `"PRIVATE"`.

## Authentication Methods

### Application Default Credentials (ADC)

The simplest method for GCP deployments. Connectors automatically use credentials from:

1. Environment variable `GOOGLE_APPLICATION_CREDENTIALS`
2. Cloud Run / Cloud Functions / GKE service account
3. Compute Engine default service account
4. gcloud CLI credentials (local development)

```python
# No explicit credentials needed - ADC handles it
config = AsyncpgConfig(
    connection_config={
        "user": "postgres",
        "password": "secret",
        "database": "mydb",
    },
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "my-project:us-central1:my-instance",
    }
)
```

### IAM Database Authentication

Passwordless authentication using Google Cloud IAM:

```python
config = AsyncpgConfig(
    connection_config={
        "user": "my-service-account@my-project.iam",  # IAM principal
        "database": "mydb",
    },
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "my-project:us-central1:my-instance",
        "cloud_sql_enable_iam_auth": True,
    }
)
```

**Requirements**:

1. Cloud SQL instance has IAM authentication enabled
2. IAM principal has `roles/cloudsql.client` role
3. Database user exists and is granted IAM authentication

**Setup Commands**:

```sql
-- Create IAM user in database
CREATE ROLE "my-service-account@my-project.iam" WITH LOGIN;
GRANT ALL ON DATABASE mydb TO "my-service-account@my-project.iam";
```

### Password Authentication

Traditional username/password authentication:

```python
config = AsyncpgConfig(
    connection_config={
        "user": "postgres",
        "password": "secret",  # From Secret Manager recommended
        "database": "mydb",
    },
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "my-project:us-central1:my-instance",
        "cloud_sql_enable_iam_auth": False,  # Explicit (default)
    }
)
```

## IP Type Selection

### Private IP (Default)

Connect via VPC private network:

```python
driver_features = {
    "enable_cloud_sql": True,
    "cloud_sql_instance": "my-project:us-central1:my-instance",
    "cloud_sql_ip_type": "PRIVATE",
}
```

**When to use**:

- Application runs on GCP (Cloud Run, GKE, Compute Engine)
- VPC peering configured
- Security requirement to avoid public internet

**Requirements**:

- Cloud SQL instance has private IP enabled
- VPC network properly configured
- Serverless VPC Access connector (for Cloud Run/Functions)

### Public IP

Connect via public internet:

```python
driver_features = {
    "enable_cloud_sql": True,
    "cloud_sql_instance": "my-project:us-central1:my-instance",
    "cloud_sql_ip_type": "PUBLIC",
}
```

**When to use**:

- Development from local machine
- External services outside GCP
- No VPC networking configured

**Security**: Traffic is SSL encrypted but uses public IP. Configure authorized networks in Cloud SQL for additional security.

### Private Service Connect (PSC)

AlloyDB-specific option for enhanced security:

```python
driver_features = {
    "enable_alloydb": True,
    "alloydb_instance_uri": "projects/p/locations/r/clusters/c/instances/i",
    "alloydb_ip_type": "PSC",
}
```

**When to use**:

- AlloyDB only (not available for Cloud SQL)
- Strict security and compliance requirements
- Dedicated network isolation

## Multi-Database Configuration

Use separate configs with unique `bind_key` values:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

sql = SQLSpec()

# Cloud SQL production database
cloud_sql_config = AsyncpgConfig(
    connection_config={"user": "app", "password": "secret", "database": "prod"},
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "prod-project:us-central1:prod-db",
    },
    bind_key="cloud_sql"
)
sql.add_config(cloud_sql_config)

# AlloyDB analytics database
alloydb_config = AsyncpgConfig(
    connection_config={"user": "analytics", "password": "secret", "database": "warehouse"},
    driver_features={
        "enable_alloydb": True,
        "alloydb_instance_uri": "projects/analytics/locations/us-central1/clusters/warehouse/instances/primary",
    },
    bind_key="alloydb"
)
sql.add_config(alloydb_config)

# Use different databases
async with sql.provide_session(cloud_sql_config) as session:
    users = await session.select_all("SELECT * FROM users")

async with sql.provide_session(alloydb_config) as session:
    analytics = await session.select_all("SELECT * FROM events")
```

## Migration from Direct Connections

### Before (Direct DSN)

```python
config = AsyncpgConfig(
    connection_config={
        "dsn": "postgresql://user:pass@10.0.0.5:5432/mydb",
        "ssl": ssl_context,  # Manual SSL setup
    }
)
```

### After (Cloud SQL Connector)

```python
config = AsyncpgConfig(
    connection_config={
        "user": "user",
        "password": "pass",
        "database": "mydb",
    },
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "my-project:us-central1:my-instance",
    }
)
```

**Benefits**:

- No manual SSL certificate management
- Automatic credential rotation (with IAM auth)
- Simplified configuration
- Built-in connection retry logic

## Troubleshooting

### Error: "Cannot enable both Cloud SQL and AlloyDB connectors simultaneously"

**Cause**: Single config has both `enable_cloud_sql=True` and `enable_alloydb=True`.

**Solution**: Use separate configs with unique `bind_key` values for each database.

### Error: "cloud_sql_instance required when enable_cloud_sql is True"

**Cause**: Connector enabled but instance name not provided.

**Solution**: Add instance name to `driver_features`:

```python
driver_features = {
    "enable_cloud_sql": True,
    "cloud_sql_instance": "project:region:instance",
}
```

### Error: "Invalid Cloud SQL instance format"

**Cause**: Instance name doesn't match `"project:region:instance"` format.

**Solution**: Verify instance connection name from Cloud Console:

```bash
gcloud sql instances describe INSTANCE_NAME --format="value(connectionName)"
```

### Error: "cloud-sql-python-connector package not installed"

**Cause**: Connector package missing.

**Solution**: Install the connector:

```bash
pip install cloud-sql-python-connector
```

### IAM Authentication Fails

**Common causes**:

1. Database user not created with IAM authentication
2. IAM principal missing `roles/cloudsql.client` role
3. Cloud SQL instance doesn't have IAM authentication enabled

**Solutions**:

```bash
# Grant IAM role
gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="serviceAccount:SERVICE_ACCOUNT" \
    --role="roles/cloudsql.client"

# Enable IAM on instance
gcloud sql instances patch INSTANCE_NAME --database-flags=cloudsql.iam_authentication=on

# Create IAM user in database
psql -h INSTANCE_IP -U postgres -d DATABASE
CREATE ROLE "SERVICE_ACCOUNT@PROJECT.iam" WITH LOGIN;
GRANT ALL ON DATABASE mydb TO "SERVICE_ACCOUNT@PROJECT.iam";
```

### Private IP Connection Timeout

**Common causes**:

1. VPC peering not configured
2. Serverless VPC Access connector missing (Cloud Run/Functions)
3. Wrong IP type selected

**Solutions**:

- Verify VPC network configuration
- Create Serverless VPC Access connector for serverless environments
- Use `"PUBLIC"` IP type for testing (not recommended for production)

## Performance Considerations

### Connection Pool Configuration

Connectors work seamlessly with AsyncPG connection pooling:

```python
config = AsyncpgConfig(
    connection_config={
        "min_size": 2,
        "max_size": 10,
        "max_inactive_connection_lifetime": 300,
    },
    driver_features={
        "enable_cloud_sql": True,
        "cloud_sql_instance": "my-project:us-central1:my-instance",
    }
)
```

**Recommendations**:

- Start with `min_size=2` and `max_size=10` for most applications
- Increase `max_size` for high-traffic applications
- Set `max_inactive_connection_lifetime` to match Cloud SQL timeout settings

### Connection Overhead

Initial connection creation is slightly slower due to SSL handshake and authentication:

- Direct DSN: ~50-100ms
- Cloud SQL connector: ~100-200ms
- AlloyDB connector: ~100-200ms

Connection pooling amortizes this overhead across many queries.

## Security Best Practices

1. **Use IAM Authentication**: Eliminates password management and enables automatic credential rotation
2. **Prefer Private IP**: Keeps traffic within GCP network
3. **Store Secrets Securely**: Use Secret Manager for passwords (when not using IAM)
4. **Least Privilege**: Grant minimal database permissions to IAM principals
5. **Enable Audit Logging**: Monitor database access with Cloud Audit Logs
6. **Regular Updates**: Keep connector packages updated for security patches

## Limitations

### Unsupported Adapters

**Psqlpy**: Architecturally incompatible due to internal Rust driver. Use AsyncPG instead or Cloud SQL Auth Proxy for direct connections.

**ADBC**: URI-only interface incompatible with connection factory pattern. Use AsyncPG with `select_to_arrow()` for Arrow integration, or Cloud SQL Auth Proxy.

**Psycopg**: Not officially supported by Google Cloud connectors. GitHub issue tracking psycopg3 support has been open since 2021. Use AsyncPG as alternative.

### Alternative: Cloud SQL Auth Proxy

For unsupported adapters, use [Cloud SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy):

```bash
# Start proxy
cloud-sql-proxy my-project:us-central1:my-instance --port 5432

# Connect with any adapter
config = PsqlpyConfig(  # Or ADBC, psycopg, etc.
    connection_config={"dsn": "postgresql://localhost:5432/mydb"}
)
```

## References

- [Cloud SQL Python Connector Documentation](https://cloud.google.com/sql/docs/postgres/connect-connectors)
- [AlloyDB Python Connector Documentation](https://cloud.google.com/alloydb/docs/auth-proxy/connect)
- [AsyncPG Adapter Guide](/guides/adapters/asyncpg.md)
- [SQLSpec Configuration Documentation](/reference/configuration.rst)
