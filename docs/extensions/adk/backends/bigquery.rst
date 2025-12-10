==================
BigQuery Backend
==================

Overview
========

Google Cloud BigQuery is a serverless, highly scalable data warehouse optimized for analytics workloads. This makes it an excellent choice for storing and analyzing large volumes of AI agent session and event data.

**Key Features:**

- **Serverless**: No infrastructure management required
- **Scalable**: Handles petabyte-scale data seamlessly
- **Analytics-Optimized**: Built-in support for complex queries and aggregations
- **Cost-Effective**: Pay only for queries run (bytes scanned) and storage used
- **JSON Support**: Native JSON type for flexible state and metadata storage
- **Partitioning & Clustering**: Automatic query optimization for cost and performance

Installation
============

Install SQLSpec with BigQuery support:

.. code-block:: bash

   pip install sqlspec[bigquery] google-genai

Configuration
=============

Basic Configuration
-------------------

.. code-block:: python

   from sqlspec.adapters.bigquery import BigQueryConfig
   from sqlspec.adapters.bigquery.adk import BigQueryADKStore

   config = BigQueryConfig(
       connection_config={
           "project": "my-gcp-project",
           "dataset_id": "my_dataset",
       }
   )

   store = BigQueryADKStore(config)
   await store.create_tables()

Authentication
--------------

BigQuery supports multiple authentication methods:

**Application Default Credentials (Recommended for Development):**

.. code-block:: bash

   gcloud auth application-default login

**Service Account:**

.. code-block:: python

   from google.oauth2 import service_account

   credentials = service_account.Credentials.from_service_account_file(
       "path/to/service-account-key.json"
   )

   config = BigQueryConfig(
       connection_config={
           "project": "my-gcp-project",
           "dataset_id": "my_dataset",
           "credentials": credentials,
       }
   )

Advanced Configuration
----------------------

.. code-block:: python

   config = BigQueryConfig(
       connection_config={
           "project": "my-gcp-project",
           "dataset_id": "my_dataset",
           "location": "us-central1",
           "use_query_cache": True,
           "maximum_bytes_billed": 100000000,  # 100 MB limit
           "query_timeout_ms": 30000,
       }
   )

Schema
======

The BigQuery ADK store creates two partitioned and clustered tables:

Sessions Table
--------------

.. code-block:: sql

   CREATE TABLE `dataset.adk_sessions` (
       id STRING NOT NULL,
       app_name STRING NOT NULL,
       user_id STRING NOT NULL,
       state JSON NOT NULL,
       create_time TIMESTAMP NOT NULL,
       update_time TIMESTAMP NOT NULL
   )
   PARTITION BY DATE(create_time)
   CLUSTER BY app_name, user_id

Events Table
------------

.. code-block:: sql

   CREATE TABLE `dataset.adk_events` (
       id STRING NOT NULL,
       session_id STRING NOT NULL,
       app_name STRING NOT NULL,
       user_id STRING NOT NULL,
       invocation_id STRING,
       author STRING,
       actions BYTES,
       long_running_tool_ids_json STRING,
       branch STRING,
       timestamp TIMESTAMP NOT NULL,
       content JSON,
       grounding_metadata JSON,
       custom_metadata JSON,
       partial BOOL,
       turn_complete BOOL,
       interrupted BOOL,
       error_code STRING,
       error_message STRING
   )
   PARTITION BY DATE(timestamp)
   CLUSTER BY session_id, timestamp

Cost Optimization
=================

BigQuery charges based on the amount of data scanned by queries. The ADK store implements several optimizations:

Partitioning
------------

Both tables are **partitioned by date**:

- Sessions: Partitioned by ``DATE(create_time)``
- Events: Partitioned by ``DATE(timestamp)``

This significantly reduces query costs when filtering by date ranges.

Clustering
----------

Tables are **clustered** for efficient filtering:

- Sessions: Clustered by ``(app_name, user_id)``
- Events: Clustered by ``(session_id, timestamp)``

Clustering optimizes queries that filter or join on these columns.

Query Best Practices
--------------------

.. code-block:: python

   # Good: Leverages clustering
   sessions = await store.list_sessions("my_app", "user_123")

   # Good: Leverages partitioning + clustering
   from datetime import datetime, timedelta, timezone

   yesterday = datetime.now(timezone.utc) - timedelta(days=1)
   recent_events = await store.get_events(
       session_id="session_id",
       after_timestamp=yesterday
   )

   # Good: Uses LIMIT to control data scanned
   events = await store.get_events(
       session_id="session_id",
       limit=100
   )

Cost Monitoring
---------------

Set query byte limits to prevent runaway costs:

.. code-block:: python

   config = BigQueryConfig(
       connection_config={
           "project": "my-project",
           "dataset_id": "my_dataset",
           "maximum_bytes_billed": 10000000,  # 10 MB limit
       }
   )

Performance Characteristics
===========================

BigQuery is optimized for different workloads than traditional OLTP databases:

**Strengths:**

- **Analytics Queries**: Excellent for aggregating and analyzing large volumes of session/event data
- **Scalability**: Handles millions of sessions and billions of events effortlessly
- **Serverless**: No connection pooling or infrastructure management
- **Concurrent Reads**: High degree of read parallelism

**Considerations:**

- **Eventual Consistency**: May take a few seconds for writes to be visible in queries
- **DML Performance**: Individual INSERT/UPDATE/DELETE operations are slower than OLTP databases
- **Cost Model**: Pay-per-query model requires careful query optimization
- **No Foreign Keys**: Implements cascade delete manually (two DELETE statements)

When to Use BigQuery
====================

**Ideal For:**

- Large-scale AI agent deployments with millions of users
- Analytics and insights on agent interactions
- Long-term storage of conversation history
- Multi-region deployments requiring global scalability
- Applications already using Google Cloud Platform

**Consider Alternatives When:**

- Need high-frequency transactional updates (use PostgreSQL/Oracle)
- Require immediate consistency (use PostgreSQL/Oracle)
- Running on-premises or other cloud providers (use PostgreSQL/DuckDB)
- Development/testing with small data volumes (use SQLite/DuckDB)

Example: Full Application
==========================

Follow the same control flow as :doc:`/examples/extensions/adk/basic_aiosqlite` but swap in
``BigQueryConfig`` and ``BigQueryADKStore``. The store API is identical across adapters, so session
creation, event append, and session replay code stays unchanged.

Migration from Other Databases
===============================

Migrating from PostgreSQL/MySQL to BigQuery:

.. code-block:: python

   # Export from PostgreSQL
   from sqlspec.adapters.asyncpg import AsyncpgConfig
   from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore

   pg_config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
   pg_store = AsyncpgADKStore(pg_config)

   # Import to BigQuery
   from sqlspec.adapters.bigquery import BigQueryConfig
   from sqlspec.adapters.bigquery.adk import BigQueryADKStore

   bq_config = BigQueryConfig(connection_config={...})
   bq_store = BigQueryADKStore(bq_config)
   await bq_store.create_tables()

   # Migrate sessions
   sessions = await pg_store.list_sessions("my_app", "user_123")
   for session in sessions:
       await bq_store.create_session(
           session["id"],
           session["app_name"],
           session["user_id"],
           session["state"]
       )

   # Migrate events
   for session in sessions:
       events = await pg_store.get_events(session["id"])
       for event in events:
           await bq_store.append_event(event)

Troubleshooting
===============

Common Issues
-------------

**403 Forbidden Error:**

.. code-block:: text

   google.api_core.exceptions.Forbidden: 403 Access Denied

**Solution:** Ensure your credentials have BigQuery permissions:

- ``BigQuery User`` - Run queries
- ``BigQuery Data Editor`` - Create/modify tables
- ``BigQuery Data Viewer`` - Read data

**404 Not Found Error:**

.. code-block:: text

   google.api_core.exceptions.NotFound: 404 Dataset not found

**Solution:** Create the dataset first:

.. code-block:: bash

   bq mk --dataset my-project:my_dataset

**High Query Costs:**

**Solution:** Enable query cost limits and use partitioning/clustering effectively:

.. code-block:: python

   config = BigQueryConfig(
       connection_config={
           "project": "my-project",
           "dataset_id": "my_dataset",
           "maximum_bytes_billed": 100000000,  # 100 MB limit
           "use_query_cache": True,
       }
   )

API Reference
=============

.. autoclass:: sqlspec.adapters.bigquery.adk.BigQueryADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

See Also
========

- :doc:`../quickstart` - Quick start guide
- :doc:`../schema` - Database schema details
- :doc:`../api` - Full API reference
- `BigQuery Documentation <https://cloud.google.com/bigquery/docs>`_
- `BigQuery Best Practices <https://cloud.google.com/bigquery/docs/best-practices>`_
