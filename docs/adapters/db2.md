# IBM Db2 Adapter

The IBM Db2 adapter provides comprehensive database connectivity, query execution, connection pooling, and metadata reflection for IBM Db2 databases (LUW, z/OS, and IBM i).

---

## Installation

Install SQLSpec with the `db2` optional extra:

```bash
pip install sqlspec[db2]
# or with uv
uv add sqlspec --extra db2
```

This installs `ibm_db>=3.2.5`. The `db2` SQLGlot dialect is included in-tree within SQLSpec with zero external dialect dependencies.

---

## Platform Support & Prerequisites

- **Precompiled Wheels**: Wheels are available for Linux x86_64, Windows, and macOS ARM64.
- **Linux ARM64 (aarch64)**: IBM does not supply a native `clidriver` for Linux ARM64. For deployments on ARM64 Linux servers (e.g. AWS Graviton), run inside an x86_64 container via emulation (`--platform linux/amd64`) or utilize `arrow-odbc` with an ODBC driver bridge.
- **Mainframe (z/OS) and IBM i (AS400)**: Direct connectivity to Db2 on z/OS or IBM i requires an IBM Db2 Connect license file (`db2conpe.lic`) placed in the `clidriver/license` directory.

---

## Configuration

Initialize `Db2Config` with connection and pool parameters:

```python
from sqlspec.adapters.db2 import Db2Config

config = Db2Config(
    connection_config={
        "database": "SAMPLE",
        "hostname": "localhost",
        "port": 50000,
        "uid": "db2inst1",
        "pwd": "password",
        "protocol": "TCPIP",
    },
    pool_config={"min_size": 2, "max_size": 10, "timeout": 30.0, "recycle": 3600, "pre_ping": True},
)
```

### Connection String (DSN)

You can also provide a raw connection string or DSN:

```python
config = Db2Config(
    connection_config={
        "connection_string": ("DATABASE=SAMPLE;HOSTNAME=db2host;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=password;")
    }
)
```

---

## Querying and Transactions

### Synchronous Execution

```python
with config.provide_session() as driver:
    # Basic query
    users = driver.execute("SELECT id, name FROM users WHERE active = ?", (1,))

    # Transactions
    with driver.transaction():
        driver.execute("INSERT INTO users (id, name) VALUES (?, ?)", (101, "Ada"))
```

### Async Execution (Offloaded)

Because `ibm_db` is a synchronous C-extension (`threadsafety = 0`), asynchronous frameworks offload operations to worker threads:

```python
from sqlspec.utils.sync_tools import async_


def fetch_active_users():
    with config.provide_session() as driver:
        return driver.execute("SELECT id, name FROM users WHERE active = ?", (1,))


users = await async_(fetch_active_users)()
```

---

## Apache Arrow Analytics

The Db2 adapter supports two Apache Arrow workflows:

### 1. In-Memory Arrow Conversion (`ibm_db`)

Export query results directly into PyArrow Tables, RecordBatches, or DataFrames:

```python
with config.provide_session() as driver:
    result = driver.select_to_arrow("SELECT * FROM transactions")

    # PyArrow Table
    table = result.get_data()

    # Pandas DataFrame
    df = table.to_pandas()

    # Polars DataFrame
    import polars as pl

    pldf = pl.from_arrow(table)
```

### 2. Native Columnar Streaming (`arrow-odbc`)

For high-volume analytics, use `sqlspec.adapters.arrow_odbc` with the Db2 ODBC driver (`libdb2o.so`) to stream RecordBatches directly from block cursors without intermediate Python tuple allocations:

```python
from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig

odbc_config = ArrowOdbcConfig(
    connection_config={
        "connection_string": (
            "Driver={IBM DB2 ODBC DRIVER};Database=SAMPLE;Hostname=localhost;Port=50000;Protocol=TCPIP;"
            "Uid=db2inst1;Pwd=password;"
        )
    }
)

with odbc_config.provide_session() as driver:
    result = driver.select_to_arrow("SELECT * FROM large_fact_table", return_format="batches")
```

---

## Schema Reflection & Data Dictionary

Inspect Db2 system catalog views (`SYSCAT.TABLES`, `SYSCAT.COLUMNS`, `SYSCAT.INDEXES`, `SYSCAT.REFERENCES`):

```python
with config.provide_session() as driver:
    # Reflect tables
    tables = driver.data_dictionary.get_tables(driver, schema="DB2INST1")

    # Reflect columns
    columns = driver.data_dictionary.get_columns(driver, table="USERS", schema="DB2INST1")

    # Primary keys from columns metadata
    pks = [col for col in columns if col.get("is_primary")]

    # Reflect foreign keys and constraints
    fks = driver.data_dictionary.get_foreign_keys(driver, table="ORDERS", schema="DB2INST1")
    constraints = driver.data_dictionary.get_constraints(driver, table="USERS", schema="DB2INST1")
```
