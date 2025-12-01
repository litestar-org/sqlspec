# BigQuery Adapter Skill

**Adapter:** Google BigQuery (Serverless, Analytics)
**Category:** Database Adapter
**Status:** Active

## Description

Expert guidance for using SQLSpec's BigQuery adapter for Google Cloud BigQuery. BigQuery is Google's serverless, highly scalable enterprise data warehouse designed for analytics and large-scale data processing. Unlike traditional databases, BigQuery is optimized for analytical workloads (OLAP) rather than transactional operations (OLTP).

This adapter provides serverless connectivity with no connection pooling (serverless architecture), native Arrow/Parquet support, built-in query caching, cost controls, and integration with BigQuery ML and Gemini AI. It requires GCS (Google Cloud Storage) staging for data loading operations.

## When to Use BigQuery

- **Analytics and data warehousing** (OLAP workloads)
- **Large-scale data processing** (petabyte-scale datasets)
- **Serverless architecture** (no infrastructure management)
- **Machine learning integration** (BigQuery ML)
- **AI/semantic search** (Gemini integration, vector search)
- **Cross-cloud analytics** (AWS, Azure data via Omni)
- **Cost-controlled queries** (maximum_bytes_billed)
- **Real-time analytics** (streaming inserts, continuous queries)

## Configuration

```python
from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriverFeatures

config = BigQueryConfig(
    connection_config={
        # Required:
        "project": "my-gcp-project",
        "location": "US",  # or "EU", "asia-northeast1", etc.

        # Optional authentication:
        "credentials_path": "/path/to/service-account.json",
        # OR use default credentials (Application Default Credentials)

        # Dataset context:
        "dataset_id": "my_dataset",  # Default dataset for queries

        # Performance & cost:
        "use_query_cache": True,     # Enable query cache (default: True)
        "maximum_bytes_billed": 10 * 1024**3,  # 10 GB limit

        # Timeouts:
        "query_timeout_ms": 30000,   # 30 seconds
        "job_timeout_ms": 600000,    # 10 minutes

        # Advanced features:
        "enable_bigquery_ml": True,
        "enable_gemini_integration": True,
        "enable_vector_search": True,
        "enable_cross_cloud": False,
        "enable_bigquery_omni": False,

        # Data format preferences:
        "use_avro_logical_types": True,
        "parquet_enable_list_inference": True,

        # Security:
        "enable_column_level_security": False,
        "enable_row_level_security": False,

        # BigQuery editions (pricing tiers):
        "edition": "STANDARD",  # or "ENTERPRISE", "ENTERPRISE_PLUS"
        "reservation_id": None,  # Slot reservation ID
    },
    driver_features=BigQueryDriverFeatures(
        # Callbacks for monitoring:
        on_job_start=lambda job_id: print(f"Query started: {job_id}"),
        on_job_complete=lambda job_id, result: print(f"Query done: {job_id}"),
        on_connection_create=lambda conn: print("Connection created"),

        # Custom JSON serializer (optional):
        json_serializer=custom_encoder,

        # UUID handling:
        enable_uuid_conversion=True,  # Default: True

        # Reuse existing connection:
        connection_instance=None,  # Optional pre-existing BigQuery client
    )
)

# Use with context manager
with config.provide_session() as session:
    result = session.execute("SELECT * FROM `my-project.my_dataset.users`")
```

## Parameter Style

**Named**: `@param`, `@user_id`, etc.

```python
# Single parameter
result = session.execute(
    "SELECT * FROM users WHERE id = @user_id",
    {"user_id": 123}
)

# Multiple parameters
result = session.execute(
    "SELECT * FROM users WHERE status = @status AND age > @min_age",
    {"status": "active", "min_age": 18}
)

# Array parameters
result = session.execute(
    "SELECT * FROM users WHERE id IN UNNEST(@user_ids)",
    {"user_ids": [1, 2, 3, 4, 5]}
)
```

## Special Features

### No Connection Pooling (Serverless)

Unlike traditional databases, BigQuery is serverless and does not use connection pooling:

```python
# BigQuery uses NoPoolSyncConfig (no pool management)
config = BigQueryConfig(connection_config={...})

# Each session is a lightweight client wrapper
with config.provide_session() as session:
    # No pool acquisition - just API calls
    result = session.execute("SELECT COUNT(*) FROM my_table")
```

**Implications**:
- No pool exhaustion issues
- No connection lifecycle management
- Pay-per-query pricing model
- Ideal for variable/bursty workloads

### GCS Staging for Data Loading

BigQuery requires GCS (Google Cloud Storage) for data loading operations:

```python
# Load data from GCS (required)
session.load_parquet(
    "gs://my-bucket/data/users.parquet",  # GCS path (gs://)
    "my_dataset.users"
)

# Local files NOT supported directly
# Upload to GCS first, then load
session.load_parquet(
    "/local/path/users.parquet",  # ❌ Will fail
    "my_dataset.users"
)
```

**Workaround for local files**:
```python
from google.cloud import storage

# 1. Upload to GCS
storage_client = storage.Client()
bucket = storage_client.bucket("my-bucket")
blob = bucket.blob("temp/users.parquet")
blob.upload_from_filename("/local/path/users.parquet")

# 2. Load from GCS
session.load_parquet(
    "gs://my-bucket/temp/users.parquet",
    "my_dataset.users"
)

# 3. Clean up (optional)
blob.delete()
```

### BigQuery ML Integration

Run machine learning models directly in SQL:

```python
config = BigQueryConfig(
    connection_config={
        "project": "my-project",
        "enable_bigquery_ml": True,
    }
)

with config.provide_session() as session:
    # Create ML model
    session.execute("""
        CREATE OR REPLACE MODEL `my_dataset.user_churn_model`
        OPTIONS(model_type='logistic_reg', input_label_cols=['churned']) AS
        SELECT
            tenure,
            monthly_charges,
            total_charges,
            churned
        FROM `my_dataset.users`
    """)

    # Make predictions
    result = session.execute("""
        SELECT
            user_id,
            predicted_churned,
            predicted_churned_probs[OFFSET(1)].prob as churn_probability
        FROM ML.PREDICT(MODEL `my_dataset.user_churn_model`,
            TABLE `my_dataset.new_users`)
    """).all()
```

**Supported model types**: linear_reg, logistic_reg, kmeans, matrix_factorization, dnn_classifier, boosted_tree_classifier, automl_classifier, arima_plus, etc.

### Gemini AI Integration

Use Gemini for semantic search and AI-powered queries:

```python
config = BigQueryConfig(
    connection_config={
        "project": "my-project",
        "enable_gemini_integration": True,
    }
)

with config.provide_session() as session:
    # Generate embeddings
    session.execute("""
        CREATE OR REPLACE TABLE `my_dataset.product_embeddings` AS
        SELECT
            product_id,
            description,
            ML.GENERATE_EMBEDDING(
                MODEL `my_dataset.gemini_embedding_model`,
                STRUCT(description AS content)
            ) AS embedding
        FROM `my_dataset.products`
    """)

    # Semantic search
    result = session.execute("""
        SELECT
            product_id,
            description,
            distance
        FROM VECTOR_SEARCH(
            TABLE `my_dataset.product_embeddings`,
            'embedding',
            (SELECT ML.GENERATE_EMBEDDING(
                MODEL `my_dataset.gemini_embedding_model`,
                STRUCT(@query AS content)
            ) AS query_embedding),
            top_k => 10
        )
    """, {"query": "wireless headphones with noise cancellation"}).all()
```

### Vector Search

Built-in vector similarity search:

```python
config = BigQueryConfig(
    connection_config={
        "project": "my-project",
        "enable_vector_search": True,
    }
)

with config.provide_session() as session:
    # Create vector index for faster search
    session.execute("""
        CREATE VECTOR INDEX IF NOT EXISTS embedding_index
        ON `my_dataset.embeddings`(embedding)
        OPTIONS(distance_type='COSINE', index_type='IVF')
    """)

    # Vector search with index
    result = session.execute("""
        SELECT
            id,
            content,
            distance
        FROM VECTOR_SEARCH(
            TABLE `my_dataset.embeddings`,
            'embedding',
            (SELECT @query_vector AS query_embedding),
            top_k => 10,
            distance_type => 'COSINE'
        )
    """, {"query_vector": embedding_list}).all()
```

### Query Caching & Cost Controls

BigQuery caches query results automatically:

```python
config = BigQueryConfig(
    connection_config={
        "use_query_cache": True,  # Default: True (free tier)
        "maximum_bytes_billed": 100 * 1024**3,  # 100 GB limit
    }
)

# Same query within 24 hours uses cache (free)
result1 = session.execute("SELECT COUNT(*) FROM large_table")
result2 = session.execute("SELECT COUNT(*) FROM large_table")  # Cached!
```

**Cost savings**:
- Cached queries: $0 (free)
- Maximum bytes billed: Prevent runaway costs
- Query preview: `CREATE TABLE ... AS SELECT ... LIMIT 0` (free)

### Job Monitoring Callbacks

Monitor query execution with callbacks:

```python
def log_job_start(job_id: str) -> None:
    print(f"[START] Query job: {job_id}")

def log_job_complete(job_id: str, result: Any) -> None:
    print(f"[DONE] Query job: {job_id}")
    print(f"  Rows: {result.total_rows}")
    print(f"  Bytes billed: {result.total_bytes_billed}")

config = BigQueryConfig(
    driver_features={
        "on_job_start": log_job_start,
        "on_job_complete": log_job_complete,
    }
)
```

## Performance Features

### Native Arrow Export

Direct Arrow integration for high-performance result retrieval:

```python
import pyarrow as pa

# Export to Arrow (zero-copy)
result = session.execute("SELECT * FROM large_table").to_arrow()
arrow_table: pa.Table = result

# Convert to Pandas (efficient)
df = arrow_table.to_pandas()
```

**Performance**: 10-100x faster than row-by-row iteration for large results.

### Native Parquet Import/Export

Built-in Parquet support:

```python
# Export to Parquet (via GCS)
session.execute("""
    EXPORT DATA OPTIONS(
        uri='gs://my-bucket/exports/users-*.parquet',
        format='PARQUET'
    ) AS
    SELECT * FROM my_dataset.users
""")

# Import from Parquet (via GCS)
session.load_parquet(
    "gs://my-bucket/data/users.parquet",
    "my_dataset.users_import"
)
```

### Partitioned Tables

Optimize query performance and reduce costs:

```python
# Create partitioned table (by date)
session.execute("""
    CREATE TABLE `my_dataset.events`
    (
        event_id STRING,
        event_name STRING,
        event_timestamp TIMESTAMP
    )
    PARTITION BY DATE(event_timestamp)
""")

# Query specific partition (reduced cost)
result = session.execute("""
    SELECT COUNT(*)
    FROM `my_dataset.events`
    WHERE DATE(event_timestamp) = '2025-01-15'
""")
```

**Cost savings**: Query only relevant partitions, not entire table.

### Clustered Tables

Further optimize queries:

```python
# Create clustered table
session.execute("""
    CREATE TABLE `my_dataset.users`
    (
        user_id INT64,
        country STRING,
        signup_date DATE
    )
    PARTITION BY signup_date
    CLUSTER BY country, user_id
""")

# Queries filtering on country/user_id are faster
result = session.execute("""
    SELECT * FROM `my_dataset.users`
    WHERE country = 'US' AND user_id > 10000
""")
```

## BigQuery-Specific Features

### Standard SQL vs Legacy SQL

Always use Standard SQL (default):

```python
# Standard SQL (recommended)
result = session.execute("""
    SELECT * FROM `my-project.my_dataset.users`
    WHERE created_at >= '2025-01-01'
""")

# Legacy SQL (deprecated, avoid)
# Uses [project:dataset.table] syntax
```

### Wildcard Tables

Query multiple tables with patterns:

```python
# Query all sharded tables
result = session.execute("""
    SELECT COUNT(*)
    FROM `my_dataset.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20250131'
""").all()
```

### Nested and Repeated Fields

BigQuery supports complex data types:

```python
# Create table with nested/repeated fields
session.execute("""
    CREATE TABLE `my_dataset.orders` (
        order_id INT64,
        customer STRUCT<
            name STRING,
            email STRING
        >,
        items ARRAY<STRUCT<
            product_id INT64,
            quantity INT64,
            price FLOAT64
        >>
    )
""")

# Query nested fields
result = session.execute("""
    SELECT
        order_id,
        customer.name,
        (SELECT SUM(item.quantity * item.price)
         FROM UNNEST(items) AS item) AS total
    FROM `my_dataset.orders`
""").all()
```

### Cross-Cloud Queries (Omni)

Query data in AWS or Azure:

```python
config = BigQueryConfig(
    connection_config={
        "project": "my-project",
        "enable_cross_cloud": True,
        "enable_bigquery_omni": True,
    }
)

# Query S3 data
result = session.execute("""
    SELECT COUNT(*)
    FROM EXTERNAL_QUERY(
        'projects/my-project/locations/aws-us-east-1/connections/my-connection',
        '''SELECT * FROM s3_table'''
    )
""").all()
```

## Best Practices

1. **Use partitioned tables** - Reduce query costs by 10-100x for time-series data
2. **Cluster frequently filtered columns** - Further improve query performance
3. **Enable query cache** - Free cached results within 24 hours
4. **Set maximum_bytes_billed** - Prevent unexpected costs from large queries
5. **Use GCS for data loading** - Required for bulk import operations
6. **Leverage BigQuery ML** - Build models without data export
7. **Monitor job callbacks** - Track query performance and costs
8. **Use Standard SQL** - Modern syntax, better performance than Legacy SQL
9. **Avoid SELECT *** - Query only needed columns to reduce bytes processed
10. **Use approximate aggregations** - APPROX_COUNT_DISTINCT() is faster and cheaper

## Common Issues

### "Quota exceeded: Your project exceeded quota for free query bytes scanned"

**Problem**: Exceeded free tier or query limits.

**Solution**:
```python
# Set cost controls
config = BigQueryConfig(
    connection_config={
        "maximum_bytes_billed": 10 * 1024**3,  # 10 GB limit
    }
)

# OR optimize queries:
# - Use partitioned tables
# - Query fewer columns (avoid SELECT *)
# - Use clustering
# - Enable query cache
```

### "Not found: Dataset my_project:my_dataset"

**Problem**: Dataset doesn't exist or wrong project.

**Solution**:
```python
# Create dataset first
from google.cloud import bigquery

client = bigquery.Client(project="my-project")
dataset = bigquery.Dataset("my-project.my_dataset")
dataset.location = "US"
client.create_dataset(dataset, exists_ok=True)

# OR use fully qualified table names
result = session.execute("""
    SELECT * FROM `my-project.my_dataset.users`
""")
```

### "Could not load file from local path"

**Problem**: Trying to load local files without GCS staging.

**Solution**:
```python
# Upload to GCS first
from google.cloud import storage

storage_client = storage.Client()
bucket = storage_client.bucket("my-bucket")
blob = bucket.blob("staging/data.parquet")
blob.upload_from_filename("/local/path/data.parquet")

# Then load from GCS
session.load_parquet(
    "gs://my-bucket/staging/data.parquet",
    "my_dataset.my_table"
)
```

### "Access Denied: BigQuery BigQuery: Permission denied"

**Problem**: Service account lacks required permissions.

**Solution**:
```bash
# Grant BigQuery Data Editor role
gcloud projects add-iam-policy-binding my-project \
    --member="serviceAccount:my-sa@my-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

# Grant BigQuery Job User role (for queries)
gcloud projects add-iam-policy-binding my-project \
    --member="serviceAccount:my-sa@my-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"
```

### Query timeout errors

**Problem**: Long-running queries exceed timeout.

**Solution**:
```python
# Increase timeouts
config = BigQueryConfig(
    connection_config={
        "query_timeout_ms": 600000,  # 10 minutes
        "job_timeout_ms": 3600000,   # 1 hour
    }
)

# OR optimize query:
# - Add WHERE filters
# - Use partitioning/clustering
# - Break into smaller queries
```

## Important Notes

### ⚠️ No Transactional DDL

BigQuery does **NOT** support transactional DDL. This means:
- DDL statements (CREATE, ALTER, DROP) are NOT automatically rolled back on error
- Each DDL statement is atomic but not transactional
- Plan DDL operations carefully and consider backups before schema changes
- BigQuery is designed for analytics (OLAP), not transactions (OLTP)

**Note**: BigQuery supports DML transactions (INSERT, UPDATE, DELETE) within BEGIN...COMMIT blocks, but DDL is always auto-committed.

```python
# DML transactions (supported)
with config.provide_session() as session:
    session.begin()
    session.execute("INSERT INTO users VALUES (1, 'Alice')")
    session.execute("UPDATE users SET name = 'Bob' WHERE id = 1")
    session.commit()  # Both succeed or both fail

# DDL is NOT transactional
with config.provide_session() as session:
    session.begin()
    session.execute("CREATE TABLE temp (id INT64)")  # Auto-commits
    session.execute("INSERT INTO nonexistent VALUES (1)")  # Fails
    session.rollback()  # temp table still exists!
```

### Serverless Architecture

- No connection pooling required
- Pay-per-query pricing (not per-connection)
- Automatic scaling (no capacity planning)
- Ideal for variable workloads
- Not suitable for low-latency OLTP

### Data Storage & Costs

- **Storage**: $0.02/GB/month (active), $0.01/GB/month (long-term)
- **Queries**: $5/TB processed (on-demand) or slot reservations
- **Free tier**: 1 TB queries/month, 10 GB storage
- **Cost optimization**: Partitioning, clustering, query cache

### Security Best Practices

- Use service accounts with minimal permissions
- Enable column/row-level security for sensitive data
- Audit queries with Cloud Logging
- Use VPC Service Controls for data exfiltration prevention
- Encrypt data at rest (automatic) and in transit (TLS)

### Performance Tuning

- Use partitioned tables for time-series data
- Cluster on frequently filtered columns
- Avoid SELECT * - query only needed columns
- Use approximate aggregations when exact counts not needed
- Pre-aggregate data in materialized views
- Monitor query execution with INFORMATION_SCHEMA

## Performance Benchmarks

BigQuery performance compared to traditional warehouses:

- **Query performance**: 10-1000x faster than traditional data warehouses (depends on data size)
- **Scaling**: Automatically scales to petabytes
- **Arrow export**: 10-100x faster than row-by-row for large results
- **Cached queries**: Instant (0ms) for repeated queries within 24 hours

**Cost comparison** (1 TB query):
- On-demand: $5
- Monthly flat-rate (100 slots): ~$2,000/month (unlimited queries)
- Cached query: $0 (free)

**Best for**:
- Analytics and reporting (OLAP)
- Large-scale batch processing
- Machine learning pipelines
- Ad-hoc data exploration

**Not ideal for**:
- Low-latency OLTP (<100ms)
- Frequent small queries (use Cloud SQL/Spanner)
- Real-time streaming updates (use Bigtable/Firestore)
