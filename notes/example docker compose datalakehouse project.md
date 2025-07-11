# example docker compose datalakehouse project

showcasing the use of iceberg with jdbc postgresql iceberg  catalog, minio storage, with configured spark, trino, flink to use iceberg and jupyterlab with examples for each spark, trino, flink, also pyiceberg, the examples should contain ddl, dml, and iceberg features like hidden partitioning, data compaction, filter push downs, snapshotting etc

## Phase 1: Core Services (Trino, MinIO, PostgreSQL, JupyterLab)

Project Structure (End of Phase 1):
datalakehouse-iceberg-jdbc/
├── docker-compose.yml
├── config/
│   ├── trino/
│   │   └── catalog/
│   │       └── iceberg.properties
│   └── postgres/
│       └── init.sql
├── notebooks/
│   └── 01-trino-iceberg-getting-started.ipynb
├── s3data/             # Empty at start, MinIO populates it
└── README.md

### 1. docker-compose.yml (Initial Version for Phase 1)

* Path: datalakehouse-iceberg-jdbc/docker-compose.yml

```yaml
services:
  minio:
    image: minio/minio:RELEASE.2023-05-04T21-44-30Z # Using a specific stable version
    container_name: minio
    ports:
      - "9000:9000"  # API
      - "9001:9001"  # Console
    volumes:
      - ./s3data:/data
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password
      MINIO_DEFAULT_BUCKETS: iceberg-warehouse
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3
    networks: # Added network for consistency from the start
      - datalakehouse_network

  postgres:
    image: postgres:15 # Using a recent stable version
    container_name: postgres_catalog
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: iceberg
      POSTGRES_PASSWORD: icebergpassword
      POSTGRES_DB: iceberg_catalog # Database for Iceberg catalog
    volumes:
      - ./config/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U iceberg -d iceberg_catalog"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks: # Added network
      - datalakehouse_network

  trino-coordinator:
    image: trinodb/trino:426 # Using a recent stable version
    container_name: trino-coordinator
    ports:
      - "8080:8080"
    volumes:
      - ./config/trino:/etc/trino
    # Environment variables TRINO_NODE_ID and TRINO_ENVIRONMENT are often set
    # in trino config files (node.properties, config.properties) but can be here too.
    # For simplicity, we'll rely on default Trino config and iceberg catalog.
    depends_on:
      minio:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks: # Added network
      - datalakehouse_network

  jupyterlab:
    image: jupyter/scipy-notebook:latest # Includes Python and common data science libraries
    container_name: jupyterlab
    ports:
      - "8888:8888"
    volumes:
      - ./notebooks:/home/jovyan/work
      - ./config:/home/jovyan/work/config # To access config files from notebook if needed
      - ./s3data:/home/jovyan/work/s3data # To inspect MinIO data directly if needed
    environment:
      JUPYTER_ENABLE_LAB: "yes"
      JUPYTER_TOKEN: "icebergrocks" # Set a token for JupyterLab
    depends_on:
      - trino-coordinator
      - minio
      - postgres
    command: start-notebook.sh --NotebookApp.token='icebergrocks' --NotebookApp.password=''
    networks: # Added network
      - datalakehouse_network

volumes:
  postgres_data:

networks:
  datalakehouse_network:
    name: datalakehouse_network
```

### 2. config/postgres/init.sql

* Path: datalakehouse-iceberg-jdbc/config/postgres/init.sql

```sql
-- config/postgres/init.sql
-- No special DDL needed here for Iceberg JDBC catalog itself,
-- the Iceberg library will create the necessary tables (e.g., iceberg_tables, iceberg_namespaces)
-- within the 'iceberg_catalog' database when it's first used.
-- This script primarily ensures the database and user exist, which is handled by
-- POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB environment variables in docker-compose.

SELECT 'Database iceberg_catalog and user iceberg are expected to be created by Docker entrypoint based on environment variables.';
-- You could add: CREATE SCHEMA IF NOT EXISTS iceberg; -- if you want a specific schema for catalog tables
-- but Iceberg usually manages this within its default "public" or based on connection string.
```

### 3. config/trino/catalog/iceberg.properties

* Path: datalakehouse-iceberg-jdbc/config/trino/catalog/iceberg.properties

```conf
# config/trino/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://postgres_catalog:5432/iceberg_catalog
iceberg.jdbc-catalog.connection-user=iceberg
iceberg.jdbc-catalog.connection-password=icebergpassword
iceberg.jdbc-catalog.default-warehouse-dir=s3a://iceberg-warehouse/ # Default path within the bucket for tables in this catalog

# S3 Configuration for MinIO
# For Trino, these are typically prefixed with 'hive.' historically, even for Iceberg connector.
hive.s3.aws-access-key=admin
hive.s3.aws-secret-key=password
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true # Required for MinIO
hive.s3.ssl.enabled=false      # MinIO is running on HTTP by default in this setup

# Optional: Allow schema creation by users
# iceberg.jdbc-catalog.create-namespace-if-not-exists=true
# By default, Trino allows creating schemas if the catalog connector supports it.
```

### 4. notebooks/01-trino-iceberg-getting-started.ipynb

* Path: datalakehouse-iceberg-jdbc/notebooks/01-trino-iceberg-getting-started.ipynb

```json
{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Trino Iceberg Datalakehouse - Getting Started\n",
    "\n",
    "This notebook demonstrates basic DDL and DML operations on Iceberg tables using Trino."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Install Trino client if not already in the Jupyter image\n",
    "%pip install trino sqlalchemy PyHive pandas"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "from sqlalchemy import create_engine\n",
    "import pandas as pd\n",
    "\n",
    "TRINO_HOST = 'trino-coordinator' # Service name in docker-compose\n",
    "TRINO_PORT = 8080\n",
    "TRINO_USER = 'testuser' # Can be any string, Trino by default doesn't enforce auth in this setup\n",
    "CATALOG = 'iceberg' # Catalog name as defined in iceberg.properties\n",
    "\n",
    "# Connection string for Trino\n",
    "trino_conn_str = f'trino://{TRINO_USER}@{TRINO_HOST}:{TRINO_PORT}/{CATALOG}'\n",
    "engine = create_engine(trino_conn_str)\n",
    "\n",
    "def run_trino_query(query, fetch_results=True):\n",
    "    \"\"\"Executes a Trino query and optionally fetches results into a Pandas DataFrame.\"\"\"\n",
    "    with engine.connect() as connection:\n",
    "        # For queries that modify data or schema, autocommit is usually the default or not needed to be set explicitly for Trino\n",
    "        # For DML/DDL, we might not always fetch results\n",
    "        result_proxy = connection.execute(query)\n",
    "        if fetch_results and result_proxy.returns_rows:\n",
    "            df = pd.DataFrame(result_proxy.fetchall(), columns=result_proxy.keys())\n",
    "            return df\n",
    "        elif fetch_results: # No rows returned but fetch_results was true\n",
    "            return pd.DataFrame(columns=result_proxy.keys() if result_proxy.returns_rows else [])\n",
    "        else:\n",
    "            print(f\"Query executed successfully (returns_rows={result_proxy.returns_rows}).\")\n",
    "            # For DDL/DML, we might want to check row count if available\n",
    "            # print(f\"Rows affected (approx): {result_proxy.rowcount}\") # rowcount might not be reliable for all statements/drivers\n",
    "            return None\n",
    "\n",
    "print(f\"Connected to Trino: {trino_conn_str}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 1. Create Schema (Namespace in Iceberg)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "SCHEMA_NAME = 'my_schema'\n",
    "run_trino_query(f\"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_NAME} WITH (location = 's3a://iceberg-warehouse/{SCHEMA_NAME}/')\", fetch_results=False)\n",
    "print(f\"Schema '{SCHEMA_NAME}' created or already exists.\")\n",
    "\n",
    "print(\"\\nAvailable schemas in Iceberg catalog:\")\n",
    "schemas_df = run_trino_query(f\"SHOW SCHEMAS FROM {CATALOG}\")\n",
    "print(schemas_df)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 2. Create an Iceberg Table"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "TABLE_NAME = 'employees'\n",
    "FQN_TABLE_NAME = f\"{CATALOG}.{SCHEMA_NAME}.{TABLE_NAME}\"\n",
    "\n",
    "create_table_sql = f\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS {FQN_TABLE_NAME} (\n",
    "    id INT,\n",
    "    name VARCHAR,\n",
    "    department VARCHAR,\n",
    "    salary DECIMAL(10, 2),\n",
    "    hire_date DATE\n",
    ")\n",
    "WITH (\n",
    "    format = 'PARQUET',\n",
    "    partitioning = ARRAY['department']\n",
    ")\n",
    "\"\"\"\n",
    "run_trino_query(create_table_sql, fetch_results=False)\n",
    "print(f\"Table '{FQN_TABLE_NAME}' created or already exists.\")\n",
    "\n",
    "print(f\"\\nTables in schema '{SCHEMA_NAME}':\")\n",
    "tables_df = run_trino_query(f\"SHOW TABLES FROM {CATALOG}.{SCHEMA_NAME}\")\n",
    "print(tables_df)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 3. Insert Data (DML)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "insert_sql = f\"\"\"\n",
    "INSERT INTO {FQN_TABLE_NAME} VALUES\n",
    "(1, 'Alice Smith', 'Engineering', 90000.00, DATE '2020-01-15'),\n",
    "(2, 'Bob Johnson', 'Engineering', 85000.00, DATE '2019-07-01'),\n",
    "(3, 'Charlie Brown', 'HR', 70000.00, DATE '2021-03-10'),\n",
    "(4, 'Diana Green', 'Sales', 95000.00, DATE '2018-05-22'),\n",
    "(5, 'Edward Black', 'Sales', 105000.00, DATE '2017-11-30')\n",
    "\"\"\"\n",
    "run_trino_query(insert_sql, fetch_results=False)\n",
    "print(f\"Data inserted into {FQN_TABLE_NAME}.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 4. Select Data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "print(\"All employees:\")\n",
    "all_employees_df = run_trino_query(f\"SELECT * FROM {FQN_TABLE_NAME}\")\n",
    "print(all_employees_df)\n",
    "\n",
    "print(\"\\nEngineering department employees (filter pushdown check):\")\n",
    "eng_employees_df = run_trino_query(f\"SELECT * FROM {FQN_TABLE_NAME} WHERE department = 'Engineering'\")\n",
    "print(eng_employees_df)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 5. Iceberg Table Metadata (Snapshots, Manifests, Files)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "print(\"\\nTable snapshots (history):\")\n",
    "history_df = run_trino_query(f\"SELECT * FROM {CATALOG}.{SCHEMA_NAME}.\\\"{TABLE_NAME}$history\\\"\") # Note escaped quotes for table name\n",
    "print(history_df)\n",
    "\n",
    "print(\"\\nTable manifest files:\")\n",
    "manifests_df = run_trino_query(f\"SELECT * FROM {CATALOG}.{SCHEMA_NAME}.\\\"{TABLE_NAME}$manifests\\\"\")\n",
    "print(manifests_df)\n",
    "\n",
    "print(\"\\nTable data files:\")\n",
    "files_df = run_trino_query(f\"SELECT file_path, record_count, partition FROM {CATALOG}.{SCHEMA_NAME}.\\\"{TABLE_NAME}$files\\\"\")\n",
    "print(files_df)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 6. Hidden Partitioning Example"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "EVENTS_TABLE_NAME = 'events'\n",
    "FQN_EVENTS_TABLE = f\"{CATALOG}.{SCHEMA_NAME}.{EVENTS_TABLE_NAME}\"\n",
    "\n",
    "create_hidden_partition_table_sql = f\"\"\"\n",
    "CREATE TABLE IF NOT EXISTS {FQN_EVENTS_TABLE} (\n",
    "    event_id VARCHAR,\n",
    "    event_type VARCHAR,\n",
    "    event_ts TIMESTAMP(6),  -- High precision timestamp\n",
    "    user_id INT\n",
    ")\n",
    "WITH (\n",
    "    format = 'PARQUET',\n",
    "    partitioning = ARRAY['day(event_ts)'] -- Hidden partitioning on event_ts by day\n",
    ")\n",
    "\"\"\"\n",
    "run_trino_query(create_hidden_partition_table_sql, fetch_results=False)\n",
    "print(f\"Table '{FQN_EVENTS_TABLE}' with hidden partitioning created.\")\n",
    "\n",
    "insert_events_sql = f\"\"\"\n",
    "INSERT INTO {FQN_EVENTS_TABLE} VALUES\n",
    "('event1', 'click', TIMESTAMP '2023-10-26 10:00:00.123456', 101),\n",
    "('event2', 'view', TIMESTAMP '2023-10-26 11:30:00.654321', 102),\n",
    "('event3', 'purchase', TIMESTAMP '2023-10-27 09:15:00.000000', 101),\n",
    "('event4', 'click', TIMESTAMP '2023-10-27 14:00:00.987654', 103)\n",
    "\"\"\"\n",
    "run_trino_query(insert_events_sql, fetch_results=False)\n",
    "print(f\"Data inserted into '{FQN_EVENTS_TABLE}'.\")\n",
    "\n",
    "print(\"\\nEvents from 2023-10-26 (filter pushdown on hidden partition):\")\n",
    "events_26_df = run_trino_query(f\"SELECT * FROM {FQN_EVENTS_TABLE} WHERE event_ts >= TIMESTAMP '2023-10-26 00:00:00' AND event_ts < TIMESTAMP '2023-10-27 00:00:00'\")\n",
    "print(events_26_df)\n",
    "\n",
    "print(\"\\nPartitions for events table (shows transformed partition values):\")\n",
    "try:\n",
    "    event_partitions_df = run_trino_query(f\"SELECT * FROM {CATALOG}.{SCHEMA_NAME}.\\\"{EVENTS_TABLE_NAME}$partitions\\\"\")\n",
    "    print(event_partitions_df)\n",
    "except Exception as e:\n",
    "    print(f\"Could not query partitions directly: {e}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 7. Data Compaction (OPTIMIZE)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Insert more data to potentially create smaller files in employees table\n",
    "insert_more_employees_sql = f\"\"\"\n",
    "INSERT INTO {FQN_TABLE_NAME} VALUES\n",
    "(6, 'Fiona White', 'Engineering', 75000.00, DATE '2023-01-10'),\n",
    "(7, 'George Yellow', 'HR', 65000.00, DATE '2023-03-15')\n",
    "\"\"\"\n",
    "run_trino_query(insert_more_employees_sql, fetch_results=False) # New snapshot\n",
    "run_trino_query(insert_more_employees_sql, fetch_results=False) # Another new snapshot\n",
    "print(\"Inserted more data into employees table to create more files/snapshots.\")\n",
    "\n",
    "print(\"\\nTable files before OPTIMIZE:\")\n",
    "files_before_optimize_df = run_trino_query(f\"SELECT file_path, record_count FROM {CATALOG}.{SCHEMA_NAME}.\\\"{TABLE_NAME}$files\\\"\")\n",
    "print(files_before_optimize_df)\n",
    "\n",
    "print(\"\\nRunning OPTIMIZE (minor compaction by default on Trino):\")\n",
    "try:\n",
    "    run_trino_query(f\"OPTIMIZE {FQN_TABLE_NAME}\", fetch_results=False)\n",
    "    print(\"OPTIMIZE command executed.\")\n",
    "    print(\"\\nTable files after OPTIMIZE:\")\n",
    "    files_after_optimize_df = run_trino_query(f\"SELECT file_path, record_count FROM {CATALOG}.{SCHEMA_NAME}.\\\"{TABLE_NAME}$files\\\"\")\n",
    "    print(files_after_optimize_df)\n",
    "    \n",
    "    print(\"\\nTable snapshots after OPTIMIZE (should show a 'replace' operation):\")\n",
    "    history_after_optimize_df = run_trino_query(f\"SELECT snapshot_id, operation FROM {CATALOG}.{SCHEMA_NAME}.\\\"{TABLE_NAME}$history\\\" ORDER BY committed_at DESC\")\n",
    "    print(history_after_optimize_df.head())\n",
    "except Exception as e:\n",
    "    print(f\"OPTIMIZE command failed or is not fully supported for this setup: {e}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 8. Time Travel / Snapshot Reading"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "snapshots_df = run_trino_query(f\"SELECT * FROM {CATALOG}.{SCHEMA_NAME}.\\\"{TABLE_NAME}$history\\\" ORDER BY committed_at ASC\")\n",
    "print(\"\\nAvailable snapshots for 'employees':\")\n",
    "print(snapshots_df)\n",
    "\n",
    "if len(snapshots_df) > 1:\n",
    "    # Try to get a snapshot before the last data modification (e.g., before OPTIMIZE or last INSERT)\n",
    "    # This depends on how many operations were performed. Let's pick the first data snapshot.\n",
    "    # The first snapshot is often table creation (empty), so pick one that likely has data.\n",
    "    # Find first 'append' operation snapshot ID\n",
    "    first_append_snapshot_id = None\n",
    "    for index, row in snapshots_df.iterrows():\n",
    "        if row['operation'] == 'append':\n",
    "            first_append_snapshot_id = row['snapshot_id']\n",
    "            break\n",
    "            \n",
    "    if first_append_snapshot_id:\n",
    "        print(f\"\\nQuerying data from snapshot ID {first_append_snapshot_id} (first append operation):\")\n",
    "        query_snapshot_sql = f\"SELECT * FROM {FQN_TABLE_NAME} FOR VERSION AS OF {first_append_snapshot_id}\"\n",
    "        snapshot_data_df = run_trino_query(query_snapshot_sql)\n",
    "        print(snapshot_data_df)\n",
    "    else:\n",
    "        print(\"\\nCould not find an 'append' snapshot for time travel example.\")\n",
    "else:\n",
    "    print(\"\\nNot enough snapshots to demonstrate time travel.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 9. Show Table DDL"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "print(f\"\\nShow create table for '{FQN_TABLE_NAME}':\")\n",
    "create_table_stmt_df = run_trino_query(f\"SHOW CREATE TABLE {FQN_TABLE_NAME}\")\n",
    "if not create_table_stmt_df.empty:\n",
    "    print(create_table_stmt_df.iloc[0,0])\n",
    "else:\n",
    "    print(\"Could not retrieve DDL.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 10. Clean up (Optional)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# print(run_trino_query(f\"DROP TABLE IF EXISTS {FQN_TABLE_NAME}\", fetch_results=False))\n",
    "# print(run_trino_query(f\"DROP TABLE IF EXISTS {FQN_EVENTS_TABLE}\", fetch_results=False))\n",
    "# print(run_trino_query(f\"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA_NAME}\", fetch_results=False))\n",
    "# print(\"\\nSchemas after potential cleanup:\")\n",
    "# schemas_after_cleanup_df = run_trino_query(f\"SHOW SCHEMAS FROM {CATALOG}\")\n",
    "# print(schemas_after_cleanup_df)\n",
    "\n",
    "print(\"\\nTrino Iceberg Datalakehouse Demo (Phase 1) completed.\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
```

5. README.md (Initial Version for Phase 1)

* Path: datalakehouse-iceberg-jdbc/README.md

(# Datalakehouse with Iceberg, MinIO, PostgreSQL, Trino, Spark, Flink, and JupyterLab

This project demonstrates a Docker Compose setup for a datalakehouse using Apache Iceberg.

## Phase 1: Core Services**

* **MinIO:** S3-compatible object storage for Iceberg table data.
* **PostgreSQL:** Stores Iceberg metadata using the JDBC catalog.
* **Trino:** Query engine to interact with Iceberg tables.
* **JupyterLab:** For running example notebooks (Python, Trino CLI via Python).

## Prerequisites

* Docker
* Docker Compose

### Setup (Phase 1)

1. **Clone the repository (or create the files as described).**
2. **Start the services:**

   ```bash
   docker-compose up -d
   ```

3. **Access Services:**
   * **MinIO Console:** [http://localhost:9001](http://localhost:9001) (Credentials: `admin`/`password`)
     * You should see the `iceberg-warehouse` bucket created.
   * **Trino UI:** [http://localhost:8080](http://localhost:8080)
   * **JupyterLab:** [http://localhost:8888](http://localhost:8888) (Token: `icebergrocks`)
     * Navigate to the `work/` directory to find the notebooks.
   * **PostgreSQL:** Can be accessed on `localhost:5432` (Credentials: `iceberg`/`icebergpassword`, Database: `iceberg_catalog`)

4. **Run the Jupyter Notebook:**
   * Open `01-trino-iceberg-getting-started.ipynb` in JupyterLab.
   * Execute the cells to create schemas, tables, insert data, and query using Trino.

## Teardown

```bash
docker-compose down -v # -v removes volumes including MinIO data and PostgreSQL data
```

Next Steps

* Add Spark service with Iceberg configuration.
* Add Flink service with Iceberg configuration.
* Develop more advanced examples for each engine.

---
)

## Phase 2: Integrate Spark with Iceberg Support

Changes and Additions:

* Update docker-compose.yml: Add Spark master and worker services. Configure Spark to use the Iceberg SQL extensions and connect to the JDBC catalog and MinIO.
* Spark Configuration Files:
  * `spark-defaults.conf`: For default Spark configurations related to Iceberg.
  * Potentially environment variables in docker-compose.yml for Spark.
* New Jupyter Notebook (02-spark-iceberg-examples.ipynb): For Spark-specific DDL, DML, and Iceberg feature demonstrations.

Let's detail these steps.

### Step 1: Update docker-compose.yml to include Spark

We'll add a Spark master and a Spark worker. For simplicity in this example, we'll embed the necessary Iceberg and S3/PostgreSQL Jars directly by specifying them in Spark's configuration. In a production setup, you might build a custom Spark image or use a Kubernetes-based deployment.

```yaml
services:
  minio:
    image: minio/minio:RELEASE.2023-05-04T21-44-30Z
    container_name: minio
    ports:
      - "9000:9000"  # API
      - "9001:9001"  # Console
    volumes:
      - ./s3data:/data
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password
      MINIO_DEFAULT_BUCKETS: iceberg-warehouse # Bucket for Iceberg data
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3
    networks:
      - datalakehouse_network

  postgres:
    image: postgres:15
    container_name: postgres_catalog
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: iceberg
      POSTGRES_PASSWORD: icebergpassword
      POSTGRES_DB: iceberg_catalog
    volumes:
      - ./config/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U iceberg -d iceberg_catalog"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - datalakehouse_network

  trino-coordinator:
    image: trinodb/trino:426
    container_name: trino-coordinator
    ports:
      - "8080:8080"
    volumes:
      - ./config/trino:/etc/trino
    environment:
      TRINO_NODE_ID: trino-coordinator-1
      TRINO_ENVIRONMENT: development
    depends_on:
      minio:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - datalakehouse_network

  # --- Spark Services ---
  spark-master:
    image: bitnami/spark:3.5 # Using a Bitnami Spark image for convenience, includes Hadoop client
    container_name: spark-master
    ports:
      - "8081:8080"  # Spark Master Web UI
      - "7077:7077"  # Spark Master RPC
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      # Pass S3 and Iceberg config to Spark jobs via spark-defaults if not baked into image
    volumes:
      - ./config/spark/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
      - ./notebooks:/opt/bitnami/spark/work-dir/notebooks # For Spark to access notebooks if needed (e.g. for spark-submit)
      - ./jars:/opt/bitnami/spark/jars/custom # For custom jars if needed
    depends_on:
      minio:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - datalakehouse_network

  spark-worker:
    image: bitnami/spark:3.5
    container_name: spark-worker-1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077 # Connect to the Spark master
      - SPARK_WORKER_MEMORY=1G
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - ./config/spark/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
      - ./jars:/opt/bitnami/spark/jars/custom
    depends_on:
      - spark-master
    networks:
      - datalakehouse_network

  jupyterlab:
    image: jupyter/pyspark-notebook:spark-3.5.0 # Use an image with PySpark pre-installed
    container_name: jupyterlab
    ports:
      - "8888:8888" # JupyterLab UI
      - "4040:4040" # Spark UI for PySpark driver
    volumes:
      - ./notebooks:/home/jovyan/work
      - ./config:/home/jovyan/work/config
      - ./s3data:/home/jovyan/work/s3data
      - ./jars:/home/jovyan/jars # Mount jars to be accessible by PySpark in Jupyter
    environment:
      JUPYTER_ENABLE_LAB: "yes"
      JUPYTER_TOKEN: "icebergrocks"
      # Spark Configuration for PySpark in JupyterLab
      # These will be used when a SparkSession is created in the notebook
      PYSPARK_SUBMIT_ARGS: >-
        --master spark://spark-master:7077
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
        --conf spark.sql.catalog.spark_catalog.type=hive # Will be overridden by JDBC specific settings
        --conf spark.sql.catalog.iceberg_catalog.type=jdbc
        --conf spark.sql.catalog.iceberg_catalog.uri=jdbc:postgresql://postgres_catalog:5432/iceberg_catalog
        --conf spark.sql.catalog.iceberg_catalog.jdbc.user=iceberg
        --conf spark.sql.catalog.iceberg_catalog.jdbc.password=icebergpassword
        --conf spark.sql.catalog.iceberg_catalog.driver=org.postgresql.Driver
        --conf spark.sql.catalog.iceberg_catalog.warehouse=s3a://iceberg-warehouse/  # Default S3 warehouse for this catalog
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
        --conf spark.hadoop.fs.s3a.access.key=admin
        --conf spark.hadoop.fs.s3a.secret.key=password
        --conf spark.hadoop.fs.s3a.path.style.access=true
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
        --conf spark.driver.extraJavaOptions="-Daws.region=us-east-1 -Daws.overrideDefaultRegion=true" # Dummy region for S3 client
        --conf spark.executor.extraJavaOptions="-Daws.region=us-east-1 -Daws.overrideDefaultRegion=true"
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0
        pyspark-shell
    depends_on:
      - trino-coordinator
      - minio
      - postgres
      - spark-master
    networks:
      - datalakehouse_network

volumes:
  postgres_data:

networks:
  datalakehouse_network: # Define the network explicitly
    name: datalakehouse_network
```

Key Changes in docker-compose.yml:

* Spark Master & Worker: Added spark-master and spark-worker-1.
* JupyterLab Image: Changed to jupyter/pyspark-notebook:spark-3.5.0 which comes with PySpark.
* JupyterLab PYSPARK_SUBMIT_ARGS: This is crucial. It configures the PySpark session created within JupyterLab:
  * --master spark://spark-master:7077: Connects to our Spark cluster.
  * Iceberg SQL Extensions: spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  * Iceberg JDBC Catalog (iceberg_catalog):
    * spark.sql.catalog.iceberg_catalog.type=jdbc
    * spark.sql.catalog.iceberg_catalog.uri=jdbc:postgresql://postgres_catalog:5432/iceberg_catalog
    * Credentials for PostgreSQL.
    * spark.sql.catalog.iceberg_catalog.driver=org.postgresql.Driver
    * spark.sql.catalog.iceberg_catalog.warehouse=s3a://iceberg-warehouse/: Specifies the root path in MinIO where Spark will write data for tables under this catalog.
  * S3/MinIO Configuration:
    * spark.hadoop.fs.s3a.endpoint=<http://minio:9000>
    * S3 credentials (admin/password).
    * path.style.access=true (for MinIO).
    * SSL disabled for MinIO.
  * --packages: This tells Spark to download the necessary Iceberg, Hadoop-AWS (for S3A), and PostgreSQL JDBC driver dependencies.
    * org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1 (Adjust version based on Spark and desired Iceberg version. 1.5.0 is recent for Spark 3.5)
    * org.apache.hadoop:hadoop-aws:3.3.4 (Ensure compatibility with Spark's Hadoop version. Bitnami Spark 3.5 usually uses Hadoop 3.3.x)
    * org.postgresql:postgresql:42.6.0 (PostgreSQL JDBC driver)
* Network: Explicitly defined datalakehouse_network and assigned all services to it to ensure consistent name resolution.
* spark-defaults.conf Volume: Mounted into Spark master and worker, though most critical config is now in PYSPARK_SUBMIT_ARGS for Jupyter. This file can be a fallback or for spark-submit jobs.
* jars directory: Added a ./jars directory and mounted it. This is useful if you prefer to download JARS manually and place them there, then reference them using spark.jars or by adding them to classpath. For now, --packages is handling it.

#### Step 2: Create config/spark/spark-defaults.conf

This file provides default configurations for Spark applications, including those submitted via spark-submit. The configurations in PYSPARK_SUBMIT_ARGS for JupyterLab will generally take precedence for notebooks.

* File: config/spark/spark-defaults.conf

```
# --- Iceberg SQL Extensions ---
spark.sql.extensions                       org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# --- Default Iceberg Catalog (using JDBC) ---
# This defines a catalog named 'iceberg_catalog'
spark.sql.catalog.iceberg_catalog             org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_catalog.catalog-impl org.apache.iceberg.jdbc.JdbcCatalog
spark.sql.catalog.iceberg_catalog.uri         jdbc:postgresql://postgres_catalog:5432/iceberg_catalog
spark.sql.catalog.iceberg_catalog.jdbc.user   iceberg
spark.sql.catalog.iceberg_catalog.jdbc.password icebergpassword
spark.sql.catalog.iceberg_catalog.driver      org.postgresql.Driver
spark.sql.catalog.iceberg_catalog.warehouse   s3a://iceberg-warehouse/

# --- S3A Configuration for MinIO (needed by Spark workers and driver) ---
spark.hadoop.fs.s3a.endpoint               http://minio:9000
spark.hadoop.fs.s3a.access.key             admin
spark.hadoop.fs.s3a.secret.key             password
spark.hadoop.fs.s3a.path.style.access      true
spark.hadoop.fs.s3a.connection.ssl.enabled false
# Adding a dummy region can sometimes help with S3 clients
spark.driver.extraJavaOptions              -Daws.region=us-east-1
spark.executor.extraJavaOptions            -Daws.region=us-east-1


# --- Jars ---
# If not using --packages, you would list jars here or use spark.jars.packages
# Example:
# spark.jars.packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0

# --- Spark Master ---
# spark.master                               spark://spark-master:7077 # Only if not set by PYSPARK_SUBMIT_ARGS or spark-submit
```

Note on Jars:
The --packages option in PYSPARK_SUBMIT_ARGS is convenient for Jupyter as it fetches the jars at session startup. For spark-submit jobs, you'd typically either:

* Use --packages in spark-submit.
* Specify spark.jars.packages in spark-defaults.conf.
* Build a custom Docker image with these jars pre-loaded into Spark's jars directory.
* Manually download jars into the mounted ./jars volume and use spark.jars pointing to them.
For this project, we will primarily rely on the --packages approach for Jupyter.

### Step 3: Create notebooks/02-spark-iceberg-examples.ipynb

This notebook will contain PySpark code to interact with Iceberg.

* File: notebooks/02-spark-iceberg-examples.ipynb

```py
# Cell 1: Import PySpark and Create SparkSession
# The SparkSession should be automatically configured by PYSPARK_SUBMIT_ARGS
# defined in docker-compose.yml for the jupyterlab service.

from pyspark.sql import SparkSession
import os

# Attempt to build SparkSession. If PYSPARK_SUBMIT_ARGS are set, this will use them.
# If running locally without those env vars, you'd configure it here.
try:
    spark = SparkSession.builder.appName("IcebergSparkDemo").getOrCreate()
    print("SparkSession created successfully!")
    print(f"Spark version: {spark.version}")
    # Verify Iceberg catalog configuration
    # spark.sql("SHOW CURRENT NAMESPACE").show() # Should show default once catalog is used
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    print("Please ensure your PYSPARK_SUBMIT_ARGS are correctly set in docker-compose.yml")
    spark = None

# Define the catalog name we configured
iceberg_catalog_name = "iceberg_catalog" # Must match spark.sql.catalog.iceberg_catalog in config

# Cell 2: Create a Database/Schema in Iceberg using Spark
if spark:
    db_name = "spark_schema"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {iceberg_catalog_name}.{db_name}").show()
    spark.sql(f"USE {iceberg_catalog_name}.{db_name}")
    spark.sql(f"SHOW DATABASES IN {iceberg_catalog_name}").show()
    # In Spark, `DATABASE` and `SCHEMA` are often used interchangeably.
    # Iceberg uses `NAMESPACE`.

# Cell 3: Create an Iceberg Table with Spark SQL
if spark:
    table_name = "spark_orders"
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {iceberg_catalog_name}.{db_name}.{table_name} (
        order_id STRING,
        customer_id STRING,
        order_date DATE,
        amount DECIMAL(10, 2),
        category STRING
    )
    USING iceberg
    PARTITIONED BY (category, days(order_date)) -- Column partitioning and hidden partitioning by day
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'format-version'='2'
    )
    """).show()
    spark.sql(f"SHOW TABLES IN {iceberg_catalog_name}.{db_name}").show()

# Cell 4: Insert Data using Spark SQL
if spark:
    spark.sql(f"""
    INSERT INTO {iceberg_catalog_name}.{db_name}.{table_name} VALUES
    ('ORD001', 'CUST101', DATE '2023-01-15', 100.50, 'electronics'),
    ('ORD002', 'CUST102', DATE '2023-01-16', 75.20, 'books'),
    ('ORD003', 'CUST101', DATE '2023-01-16', 250.00, 'electronics'),
    ('ORD004', 'CUST103', DATE '2023-01-17', 45.99, 'home'),
    ('ORD005', 'CUST102', DATE '2023-01-18', 120.00, 'books')
    """)
    print(f"Data inserted into {db_name}.{table_name}")

# Cell 5: Select Data using Spark SQL
if spark:
    print(f"Querying all data from {db_name}.{table_name}:")
    spark.sql(f"SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name} ORDER BY order_date").show()

    print(f"\nQuerying electronics orders (demonstrating partition filter pushdown):")
    spark.sql(f"""
    SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name}
    WHERE category = 'electronics' AND order_date = DATE '2023-01-16'
    """).show()
    # To see the query plan:
    # spark.sql(f"EXPLAIN SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name} WHERE category = 'electronics' AND order_date = DATE '2023-01-16'").show(truncate=False)


# Cell 6: DataFrame API for Writing and Reading
if spark:
    from pyspark.sql.functions import col, to_date, lit
    from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("category", StringType(), True)
    ])
    data = [
        ('ORD006', 'CUST104', '2023-01-19', 300.00, 'electronics'),
        ('ORD007', 'CUST105', '2023-01-19', 22.50, 'books')
    ]
    # Convert string dates to DateType for DataFrame creation
    from datetime import datetime
    data_typed = [(r[0], r[1], datetime.strptime(r[2], '%Y-%m-%d').date(), r[3], r[4]) for r in data]

    new_orders_df = spark.createDataFrame(data_typed, schema=schema)
    print("\nNew orders DataFrame:")
    new_orders_df.show()

    # Append data using DataFrameWriter
    new_orders_df.writeTo(f"{iceberg_catalog_name}.{db_name}.{table_name}").append()
    # Alternatively, for older Spark/Iceberg versions or different configurations:
    # new_orders_df.write.format("iceberg").mode("append").save(f"{iceberg_catalog_name}.{db_name}.{table_name}")

    print(f"\nData after DataFrame append:")
    spark.sql(f"SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name} ORDER BY order_date, order_id").show()

# Cell 7: Iceberg Metadata - Snapshots, History, Manifests, Files
if spark:
    print(f"\nSnapshots for {db_name}.{table_name}:")
    spark.sql(f"SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name}.history").show()

    print(f"\nManifests for {db_name}.{table_name}:")
    spark.sql(f"SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name}.manifests").show(truncate=False)

    print(f"\nData Files for {db_name}.{table_name}:")
    spark.sql(f"SELECT file_path, record_count, partition FROM {iceberg_catalog_name}.{db_name}.{table_name}.files").show(truncate=False)

    print(f"\nPartitions for {db_name}.{table_name}:")
    # This shows how data is partitioned based on `category` and `order_date_day` (hidden transform)
    spark.sql(f"SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name}.partitions").show(truncate=False)


# Cell 8: Time Travel Queries
if spark:
    history_df = spark.sql(f"SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name}.history ORDER BY made_current_at")
    history_list = history_df.collect()

    if len(history_list) > 1:
        # Get the snapshot ID of the first insert operation (before the DataFrame append)
        # Assuming the first operation was the SQL INSERT and second was DataFrame append
        first_op_snapshot_id = history_list[0]["snapshot_id"] # The very first snapshot after table creation might be empty if no data was inserted then.
                                                              # The first data snapshot is what we usually want.
        
        # Find the snapshot *after* the first batch of INSERTs
        # The history table shows when a snapshot was *made current*.
        # The first row is the earliest snapshot.
        if len(history_list) >= 2 and history_list[1]["operation"] == "append": # Assuming first user data insert is an append
             target_snapshot_id = history_list[1]["snapshot_id"] # This would be after the first SQL INSERT
             print(f"\nQuerying table state AS OF VERSION {target_snapshot_id} (after initial SQL INSERT):")
             spark.read.option("snapshot-id", target_snapshot_id)\
                  .table(f"{iceberg_catalog_name}.{db_name}.{table_name}")\
                  .orderBy("order_date", "order_id").show()

        latest_snapshot_id = history_list[-1]["snapshot_id"]
        print(f"\nQuerying table state AS OF LATEST SNAPSHOT {latest_snapshot_id} (current state):")
        spark.read.table(f"{iceberg_catalog_name}.{db_name}.{table_name}").orderBy("order_date", "order_id").show()

        # Time travel using timestamp (requires made_current_at timestamp)
        # if len(history_list) >= 2:
        #     timestamp_before_last_append = history_list[-2]["made_current_at"] # Timestamp of the snapshot before the last one
        #     print(f"\nQuerying table state AS OF TIMESTAMP '{timestamp_before_last_append}':")
        #     spark.read.option("as-of-timestamp", str(timestamp_before_last_append.timestamp() * 1000)) \ # ms since epoch
        #          .table(f"{iceberg_catalog_name}.{db_name}.{table_name}") \
        #          .orderBy("order_date", "order_id").show()


# Cell 9: Schema Evolution (Example: Add a new column)
if spark:
    print(f"\nSchema before evolution:")
    spark.sql(f"DESCRIBE {iceberg_catalog_name}.{db_name}.{table_name}").show()

    # Add a new column 'is_returned'
    spark.sql(f"ALTER TABLE {iceberg_catalog_name}.{db_name}.{table_name} ADD COLUMN is_returned BOOLEAN")
    print(f"\nSchema after adding 'is_returned' column:")
    spark.sql(f"DESCRIBE {iceberg_catalog_name}.{db_name}.{table_name}").show()

    # Insert data with the new column
    spark.sql(f"""
    INSERT INTO {iceberg_catalog_name}.{db_name}.{table_name}
    VALUES ('ORD008', 'CUST101', DATE '2023-01-20', 55.00, 'home', true)
    """)

    print(f"\nData after inserting with new column (old rows will have null for 'is_returned'):")
    spark.sql(f"SELECT order_id, category, order_date, amount, is_returned FROM {iceberg_catalog_name}.{db_name}.{table_name} ORDER BY order_date, order_id").show()


# Cell 10: Data Compaction (Rewrite Data Files - Small File Compaction)
# Iceberg procedures are called using CALL
if spark:
    # Insert some more data to potentially create small files
    spark.sql(f"INSERT INTO {iceberg_catalog_name}.{db_name}.{table_name} VALUES ('ORD009', 'CUST106', DATE '2023-01-21', 10.00, 'books', false)")
    spark.sql(f"INSERT INTO {iceberg_catalog_name}.{db_name}.{table_name} VALUES ('ORD010', 'CUST106', DATE '2023-01-21', 12.00, 'books', false)")

    print(f"\nData Files before compaction for {db_name}.{table_name} (partition: category=books):")
    spark.sql(f"""
        SELECT file_path, record_count, file_size_in_bytes
        FROM {iceberg_catalog_name}.{db_name}.{table_name}.files
        WHERE partition.category = 'books'
    """).show(truncate=False)

    # Execute rewrite_data_files procedure for compaction
    # This will compact small files into larger ones, default strategy is 'sort' which also sorts data within files
    # For very small tables, the effect might be limited or group all into one file per partition.
    try:
        print(f"\nRunning data compaction (rewrite_data_files) for table {db_name}.{table_name}...")
        # You can specify sorting options or a where clause to limit compaction scope
        result_df = spark.sql(f"CALL {iceberg_catalog_name}.system.rewrite_data_files(table => '{db_name}.{table_name}', strategy => 'sort', sort_order => 'order_date ASC, amount DESC', options => map('min-input-files','1'))")
        # options => map('min-input-files','1') to force compaction even with few files for demo
        result_df.show()
        print("Compaction procedure finished.")

        print(f"\nData Files after compaction for {db_name}.{table_name} (partition: category=books):")
        spark.sql(f"""
            SELECT file_path, record_count, file_size_in_bytes
            FROM {iceberg_catalog_name}.{db_name}.{table_name}.files
            WHERE partition.category = 'books'
        """).show(truncate=False)

        print(f"\nSnapshots after compaction:")
        spark.sql(f"SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name}.history ORDER BY made_current_at DESC").show()

    except Exception as e:
        print(f"Error during compaction: {e}")
        print("Compaction procedures might require specific configurations or Iceberg versions.")

# Cell 11: Filter Pushdown Demonstration
# The table is partitioned by (category, days(order_date))
# Spark should be able to push down filters on `category` and `order_date`.
if spark:
    query_with_filters = f"""
    SELECT * FROM {iceberg_catalog_name}.{db_name}.{table_name}
    WHERE category = 'electronics' AND order_date >= DATE '2023-01-15' AND order_date < DATE '2023-01-20'
    """
    print("\nQuery with partition column filters:")
    spark.sql(query_with_filters).show()

    print("\nEXPLAIN plan for the query (look for PushedFilters in ParquetScan or IcebergScan):")
    spark.sql(f"EXPLAIN {query_with_filters}").show(truncate=False, n=100)

# Cell 12: Interoperability Check - Read data created by Trino (if my_schema.employees exists)
# Ensure the Trino notebook (01-trino-iceberg-getting-started.ipynb) has been run to create this table.
if spark:
    trino_table_name = "my_schema.employees" # From Trino notebook
    try:
        print(f"\nAttempting to read Trino-created table: {iceberg_catalog_name}.{trino_table_name}")
        # Spark needs to know the schema. If Trino created it, Spark should discover it via the JDBC catalog.
        spark.sql(f"DESCRIBE {iceberg_catalog_name}.{trino_table_name}").show()
        spark.sql(f"SELECT * FROM {iceberg_catalog_name}.{trino_table_name} LIMIT 5").show()
        print("Successfully read Trino-created table with Spark.")
    except Exception as e:
        print(f"Could not read Trino-created table {iceberg_catalog_name}.{trino_table_name}: {e}")
        print("Ensure the Trino notebook was run first and the table exists.")

# Cell 13: Clean up (Optional)
if spark:
    # spark.sql(f"DROP TABLE IF EXISTS {iceberg_catalog_name}.{db_name}.{table_name}")
    # spark.sql(f"DROP DATABASE IF EXISTS {iceberg_catalog_name}.{db_name}")
    # spark.sql(f"SHOW DATABASES IN {iceberg_catalog_name}").show()
    pass

# Cell 14: Stop SparkSession (important in scripts, less so in interactive notebooks but good practice)
if spark:
    # spark.stop() # Commented out to allow re-running cells easily
    print("\nSpark Iceberg Datalakehouse Demo (Phase 2) completed.")
```

### Step 4: Update README.md

Add instructions for Spark.

(# Datalakehouse with Iceberg, MinIO, PostgreSQL, Trino, Spark, Flink, and JupyterLab

This project demonstrates a Docker Compose setup for a datalakehouse using Apache Iceberg.

**Services:**

* **MinIO:** S3-compatible object storage for Iceberg table data.
* **PostgreSQL:** Stores Iceberg metadata using the JDBC catalog.
* **Trino:** Query engine to interact with Iceberg tables.
* **Spark (Master & Worker):** Processing engine for batch and streaming, integrated with Iceberg.
* **JupyterLab:** For running example notebooks (Python, Trino CLI via Python, PySpark).

## Prerequisites

* Docker
* Docker Compose

## Setup

1. **Clone the repository (or create the files as described).**
2. **Create a local `./jars` directory (optional, for manual jar management):**

   ```bash
   mkdir jars
   ```

3. **Start the services:**

   ```bash
   docker-compose up -d --build # --build if you change jupyterlab Dockerfile or for spark image changes if any
   ```

   Wait for all services to become healthy. You can check with `docker-compose ps`.
   The first time, Spark in JupyterLab will download packages specified in `PYSPARK_SUBMIT_ARGS`, which might take a few minutes.

4. **Access Services:**
   * **MinIO Console:** [http://localhost:9001](http://localhost:9001) (Credentials: `admin`/`password`)
     * You should see the `iceberg-warehouse` bucket. Table data will appear here under catalog-defined paths (e.g., `iceberg-warehouse/spark_schema/`).
   * **PostgreSQL:** `localhost:5432` (User: `iceberg`, Pass: `icebergpassword`, DB: `iceberg_catalog`)
     * Tables like `iceberg_tables`, `iceberg_namespace_properties` will be here.
   * **Trino UI:** [http://localhost:8080](http://localhost:8080)
   * **Spark Master UI:** [http://localhost:8081](http://localhost:8081)
     * You should see the connected worker(s) and running/completed applications (including your PySpark session from JupyterLab).
   * **JupyterLab:** [http://localhost:8888](http://localhost:8888) (Token: `icebergrocks`)
     * Navigate to the `work/` directory.
     * When you run PySpark cells in `02-spark-iceberg-examples.ipynb`, a Spark application will appear in the Spark Master UI. The Spark Application UI (for driver logs, stages, etc.) can be accessed via `http://localhost:4040` (or the port shown in Jupyter logs/Spark UI).

5. **Run the Jupyter Notebooks:**
   * Start with `01-trino-iceberg-getting-started.ipynb` to create some initial tables with Trino.
   * Then, run `02-spark-iceberg-examples.ipynb` to interact with Iceberg using Spark. This notebook will create its own tables and can also read tables created by Trino if the catalog configuration is consistent.

## Teardown

```bash
docker-compose down -v # -v removes volumes including MinIO data and PostgreSQL data
```

Next Steps

* Add Flink service with Iceberg configuration.
* Develop more advanced examples for each engine, including pyiceberg.
)

**To run this Phase 2:**

1. Ensure all files from Phase 1 are in place and updated.
2. Create the new `config/spark/spark-defaults.conf` and `notebooks/02-spark-iceberg-examples.ipynb` files.
3. Create an empty directory `jars` in the project root: `mkdir jars`. (This is good practice even if initially unused by `--packages`).
4. Stop any existing containers: `docker-compose down -v`.
5. Start all services: `docker-compose up -d --build`. The `--build` flag is good practice if you changed the JupyterLab image or any service that might have a build step (though none do explicitly here yet, it doesn't hurt).
6. Wait for services, especially for JupyterLab to download Spark packages on its first PySpark execution.
7. Access JupyterLab, open `02-spark-iceberg-examples.ipynb`, and run the cells. You can also run the Trino notebook again to verify interoperability. Check the Spark Master UI (`localhost:8081`) and MinIO to see tables and data.

This completes the Spark integration.

## Phase 3: Integrating Apache Flink

This phase will add Apache Flink JobManager and TaskManager services to our Docker Compose setup, configure them for Iceberg, and provide a Jupyter notebook with PyFlink examples.

### Step 1: Prepare for Flink JARs

Flink requires specific JAR files for Iceberg integration, S3 access (MinIO), and potentially for the JDBC catalog if used directly (though Flink often uses Iceberg's catalog objects).

* Create a directory for Flink JARs:
   In your project's root directory (datalakehouse-iceberg-jdbc), create the following path:
   mkdir -p ./jars/flink

* Download the required JARs:
   You'll need to download the following JARs and place them into the ./jars/flink directory.
  * Iceberg Flink Runtime: This connects Flink to Iceberg. For Flink 1.18.x and Iceberg 1.5.0:
    * iceberg-flink-runtime-1.18-1.5.0.jar
    * Download from Maven Central (e.g., <https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.0/iceberg-flink-runtime-1.18-1.5.0.jar>)
  * PostgreSQL JDBC Driver: For the JDBC catalog. We'll use the same version as with Trino and Spark for consistency.
    * postgresql-42.6.0.jar
    * Download from Maven Central (e.g., <https://jdbc.postgresql.org/download/postgresql-42.6.0.jar>)
  * AWS S3 Hadoop Filesystem (for Flink to talk to MinIO via S3A): Flink official images usually bundle Hadoop, but flink-s3-fs-hadoop is the recommended S3 connector. Often, the Hadoop distribution bundled with Flink has these, but it's safer to ensure it's explicitly available for usrlib.
    * flink-s3-fs-hadoop-1.18.1.jar (Match your Flink version, e.g., Flink 1.18.1)
    * Download from Maven Central (e.g. <https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar>)
    * Note: Sometimes, you might also need hadoop-aws.jar and its dependencies if not fully provided by Flink's core. For this setup, flink-s3-fs-hadoop should be sufficient as it packages necessary components.
   Place these three JAR files into your ./jars/flink/ directory. Your PYSPARK_SUBMIT_ARGS for JupyterLab already pulls hadoop-aws which is generally compatible. Flink's S3 connectors are specifically built for Flink.

### Step 2: Update docker-compose.yml to include Flink and update JupyterLab

We'll add Flink JobManager, TaskManager, and modify JupyterLab to install PyFlink.

```yaml
services:
  minio:
    image: minio/minio:RELEASE.2023-05-04T21-44-30Z
    container_name: minio
    ports:
      - "9000:9000"  # API
      - "9001:9001"  # Console
    volumes:
      - ./s3data:/data
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password
      MINIO_DEFAULT_BUCKETS: iceberg-warehouse
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3
    networks:
      - datalakehouse_network

  postgres:
    image: postgres:15
    container_name: postgres_catalog
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: iceberg
      POSTGRES_PASSWORD: icebergpassword
      POSTGRES_DB: iceberg_catalog
    volumes:
      - ./config/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U iceberg -d iceberg_catalog"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - datalakehouse_network

  trino-coordinator:
    image: trinodb/trino:426
    container_name: trino-coordinator
    ports:
      - "8080:8080"
    volumes:
      - ./config/trino:/etc/trino
    environment:
      TRINO_NODE_ID: trino-coordinator-1
      TRINO_ENVIRONMENT: development
    depends_on:
      minio:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - datalakehouse_network

  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    ports:
      - "8081:8080"
      - "7077:7077"
    environment:
      - SPARK_MODE=master
      # ... (other Spark env vars as before)
    volumes:
      - ./config/spark/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
      - ./notebooks:/opt/bitnami/spark/work-dir/notebooks
      - ./jars:/opt/bitnami/spark/jars/custom # General jars mount
    depends_on:
      minio:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - datalakehouse_network

  spark-worker:
    image: bitnami/spark:3.5
    container_name: spark-worker-1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      # ... (other Spark env vars as before)
    volumes:
      - ./config/spark/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
      - ./jars:/opt/bitnami/spark/jars/custom
    depends_on:
      - spark-master
    networks:
      - datalakehouse_network

  # --- Flink Services ---
  flink-jobmanager:
    image: flink:1.20.1-scala_2.12-java17 # Using Flink 1.18.1
    container_name: flink-jobmanager
    ports:
      - "8088:8081"  # Flink Web UI (Note: Spark Master UI is on 8081, so Flink UI is on 8088)
      - "6123:6123"  # JobManager RPC
    command: jobmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      # S3/MinIO Configuration for Flink
      - AWS_ACCESS_KEY_ID=admin # For flink-s3-fs-hadoop
      - AWS_SECRET_ACCESS_KEY=password # For flink-s3-fs-hadoop
      # These are Flink specific S3 settings, also recognized by flink-s3-fs-hadoop
      - s3.endpoint=http://minio:9000
      - s3.path.style.access=true
      - s3.access-key=admin
      - s3.secret-key=password
    volumes:
      - ./jars/flink:/opt/flink/usrlib # Mount downloaded JARs here for SQL Client & Table API
      - ./config/flink:/opt/flink/conf # For flink-conf.yaml if needed
    depends_on:
      minio:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - datalakehouse_network

  flink-taskmanager:
    image: flink:1.20.1-scala_2.12-java17
    container_name: flink-taskmanager-1
    command: taskmanager
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager
      # S3/MinIO Configuration for Flink (also needed by task managers)
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - s3.endpoint=http://minio:9000
      - s3.path.style.access=true
      - s3.access-key=admin
      - s3.secret-key=password
      # Configure TaskManager resources (optional, defaults are usually fine for demo)
      # - TASK_MANAGER_NUMBER_OF_TASK_SLOTS=2
    volumes:
      - ./jars/flink:/opt/flink/usrlib
      - ./config/flink:/opt/flink/conf
    depends_on:
      - flink-jobmanager
    networks:
      - datalakehouse_network

  jupyterlab:
    # We need to build a custom image to include PyFlink and other Flink related tools if desired
    build:
      context: ./config/jupyter # Create a new directory for Jupyter Dockerfile
      dockerfile: Dockerfile
    container_name: jupyterlab
    ports:
      - "8888:8888" # JupyterLab UI
      - "4040:4040" # Spark UI for PySpark driver
      # Flink local PyFlink web UI if running mini cluster in notebook
      # For Flink 1.18+, Python UDF worker might use port range 6000-7000 if not configured
    volumes:
      - ./notebooks:/home/jovyan/work
      - ./config:/home/jovyan/work/config
      - ./s3data:/home/jovyan/work/s3data
      - ./jars:/home/jovyan/jars # Access to all jars
    environment:
      JUPYTER_ENABLE_LAB: "yes"
      JUPYTER_TOKEN: "icebergrocks"
      # Spark Configuration for PySpark in JupyterLab (as before)
      PYSPARK_SUBMIT_ARGS: >-
        --master spark://spark-master:7077
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        --conf spark.sql.catalog.iceberg_catalog.type=jdbc
        --conf spark.sql.catalog.iceberg_catalog.uri=jdbc:postgresql://postgres_catalog:5432/iceberg_catalog
        --conf spark.sql.catalog.iceberg_catalog.jdbc.user=iceberg
        --conf spark.sql.catalog.iceberg_catalog.jdbc.password=icebergpassword
        --conf spark.sql.catalog.iceberg_catalog.driver=org.postgresql.Driver
        --conf spark.sql.catalog.iceberg_catalog.warehouse=s3a://iceberg-warehouse/
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
        --conf spark.hadoop.fs.s3a.access.key=admin
        --conf spark.hadoop.fs.s3a.secret.key=password
        --conf spark.hadoop.fs.s3a.path.style.access=true
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
        --conf spark.driver.extraJavaOptions="-Daws.region=us-east-1 -Daws.overrideDefaultRegion=true"
        --conf spark.executor.extraJavaOptions="-Daws.region=us-east-1 -Daws.overrideDefaultRegion=true"
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0
        pyspark-shell
      # For PyFlink to find the downloaded JARs, if needed by local mini-cluster
      # FLINK_HOME: /opt/flink # If PyFlink needs it
      # HADOOP_CONF_DIR: /etc/hadoop/conf # If needed
    depends_on:
      - trino-coordinator
      - minio
      - postgres
      - spark-master
      - flink-jobmanager # Jupyter depends on Flink being up
    networks:
      - datalakehouse_network

volumes:
  postgres_data:

networks:
  datalakehouse_network:
    name: datalakehouse_network
```

Key changes in docker-compose.yml for Flink:

* Flink Services: flink-jobmanager and flink-taskmanager are added.
  * Using flink:1.18.1-scala_2.12-java11.
  * Flink Web UI exposed on port 8088 (to avoid conflict with Spark Master on 8081).
  * S3/MinIO configuration is provided via environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and Flink's s3.* properties). These are standard ways Flink's S3 connectors pick up credentials.
  * The ./jars/flink directory is mounted to /opt/flink/usrlib. Flink automatically loads JARs from this directory, making them available for SQL DDL/DML and Table API programs.
  * A ./config/flink volume is mounted for flink-conf.yaml, though we might not need many custom settings there initially.
* JupyterLab Service:
  * Now uses a build context. We need to create a Dockerfile to install PyFlink.
  * The jupyter/pyspark-notebook image is a good base.
  * Depends on flink-jobmanager to ensure Flink cluster is available.

### Step 3: Create Dockerfile for JupyterLab (config/jupyter/Dockerfile)

Create a new directory config/jupyter/ and place the following Dockerfile inside it:

* File: config/jupyter/Dockerfile

```
ARG BASE_CONTAINER=jupyter/pyspark-notebook:spark-3.5.0
FROM ${BASE_CONTAINER}

USER root

# Install PyFlink and any other dependencies
# Match PyFlink version with Flink cluster version (e.g., 1.18.1)
RUN pip install --no-cache-dir apache-flink==1.18.1 pyiceberg==0.6.1

# Optionally, install netcat or other tools for debugging connections
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

USER ${NB_UID} # Switch back to default notebook user

# You can also copy a pre-configured flink-conf.yaml here if PyFlink needs it
# COPY flink-conf.yaml /opt/flink/conf/
# ENV FLINK_CONF_DIR /opt/flink/conf
```

* This Dockerfile installs apache-flink==1.18.1 (PyFlink) and pyiceberg==0.6.1 (for the PyIceberg examples later).
* netcat is added for potential debugging.

### Step 4: Create config/flink/flink-conf.yaml (Optional - for basic S3 settings if not via env vars)

For our setup, environment variables in docker-compose.yml for JobManager and TaskManager should be sufficient for S3 configuration. However, if you prefer, you can also put S3 settings in flink-conf.yaml.

Create ./config/flink/flink-conf.yaml:

```
# Basic configuration. Most S3 settings are passed via environment variables
# in docker-compose.yml, which flink-s3-fs-hadoop can pick up.

# Example of how you *could* set S3 creds here if not using env vars:
# s3.access-key: admin
# s3.secret-key: password
# s3.endpoint: http://minio:9000
# s3.path.style.access: true

# Ensure the usrlib directory is recognized if not by default
# classloader.resolve-order: parent-first # or child-first, depending on needs. parent-first is often safer.

# High-availability (HA) settings (not configured for this simple setup)
# state.backend: filesystem
# state.checkpoints.dir: s3a://iceberg-warehouse/flink-checkpoints/
# state.savepoints.dir: s3a://iceberg-warehouse/flink-savepoints/
# high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory # Example for K8s
# high-availability.storageDir: s3a://iceberg-warehouse/flink-ha/

# Make sure the TaskManager can find the JobManager
jobmanager.rpc.address: flink-jobmanager
```

The S3 settings in docker-compose.yml (as s3.endpoint, s3.access-key, etc., and AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) are generally picked up by flink-s3-fs-hadoop. The environment variables are convenient for Docker.

### Step 5: Create notebooks/03-flink-iceberg-examples.ipynb

This notebook will use PyFlink to interact with Iceberg tables. It will primarily use the Table API and Flink SQL. The execution will mostly be local ("mini-cluster" mode within PyFlink) for ease of use in Jupyter, but configured to use the shared catalog and S3.
File: notebooks/03-flink-iceberg-examples.ipynb

```
# Cell 1: Setup PyFlink Environment and Install/Import packages
# PyFlink should be installed via the Dockerfile for the JupyterLab service.
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.expressions import lit, col
from pyflink.table.types import RowType, ecológicaRecordType # For schema definition if needed

# Define connection parameters (should match docker-compose and other services)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
iceberg_catalog_URL = "jdbc:postgresql://postgres_catalog:5432/iceberg_catalog"
iceberg_catalog_USER = "iceberg"
iceberg_catalog_PASSWORD = "icebergpassword"
ICEBERG_WAREHOUSE_PATH = "s3a://iceberg-warehouse/" # Root for this catalog

# Catalog name for Flink to use
FLINK_ICEBERG_CATALOG_NAME = "iceberg_flink_catalog" # Flink's instance of the Iceberg catalog

# Cell 2: Initialize Flink Table Environment
# Using Blink planner (default) and streaming mode for this example, can also use batch
env_settings = EnvironmentSettings.in_streaming_mode() # or .in_batch_mode()
# For local execution within Jupyter:
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

# --- Configure S3A for Flink (globally for the local Flink execution) ---
# These are crucial for PyFlink local execution to access MinIO
table_env.get_config().get_configuration().set_string("fs.s3a.endpoint", MINIO_ENDPOINT)
table_env.get_config().get_configuration().set_string("fs.s3a.access.key", MINIO_ACCESS_KEY)
table_env.get_config().get_configuration().set_string("fs.s3a.secret.key", MINIO_SECRET_KEY)
table_env.get_config().get_configuration().set_string("fs.s3a.path.style.access", "true")
table_env.get_config().get_configuration().set_string("fs.s3a.connection.ssl.enabled", "false")

# --- Add necessary JARs for local PyFlink execution ---
# The Flink cluster (JobManager/TaskManager) gets JARs from the mounted /opt/flink/usrlib.
# For local PyFlink execution (like in this notebook), we need to tell PyFlink where to find these JARs.
# The JARs are mounted into /home/jovyan/jars in the JupyterLab container.
jar_paths = [
    "file:///home/jovyan/jars/flink/iceberg-flink-runtime-1.18-1.5.0.jar",
    "file:///home/jovyan/jars/flink/postgresql-42.6.0.jar", # For JDBC Catalog
    "file:///home/jovyan/jars/flink/flink-s3-fs-hadoop-1.18.1.jar" # For S3 access
]
table_env.get_config().set("pipeline.jars", ";".join(jar_paths))
# For older PyFlink versions, you might use:
# table_env.get_config().get_configuration().set_string("pipeline.classpaths", ";".join(jar_paths))


print("Flink TableEnvironment initialized for local execution.")
print(f"Make sure these JARs exist: {jar_paths}")
print("If submitting to remote cluster, JARs in /opt/flink/usrlib of JM/TM are used.")

# Cell 3: Create an Iceberg Catalog in Flink
# This DDL is executed by the Flink environment.
# The catalog properties point to the shared PostgreSQL and MinIO.
create_catalog_ddl = f"""
CREATE CATALOG {FLINK_ICEBERG_CATALOG_NAME}
WITH (
    'type'='iceberg',
    'catalog-type'='jdbc',
    'uri'='{iceberg_catalog_URL}',
    'jdbc.user'='{iceberg_catalog_USER}',
    'jdbc.password'='{iceberg_catalog_PASSWORD}',
    'warehouse'='{ICEBERG_WAREHOUSE_PATH}',
    'property-version'='1'
)
"""
try:
    table_env.execute_sql(create_catalog_ddl)
    print(f"Iceberg catalog '{FLINK_ICEBERG_CATALOG_NAME}' created successfully.")
    table_env.execute_sql(f"USE CATALOG {FLINK_ICEBERG_CATALOG_NAME}")
    print(f"Using catalog: {FLINK_ICEBERG_CATALOG_NAME}")
except Exception as e:
    print(f"Error creating or using Flink Iceberg catalog: {e}")
    print("Ensure JARs are correctly loaded and PostgreSQL/MinIO are accessible.")

# Cell 4: Create a Database/Schema in the Iceberg Catalog using Flink SQL
DB_NAME_FLINK = "flink_schema"
table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME_FLINK}")
table_env.execute_sql(f"USE {DB_NAME_FLINK}") # Sets the current database for the session
print(f"Using database: {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}")
table_env.execute_sql(f"SHOW DATABASES").print()

# Cell 5: Create an Iceberg Table using Flink SQL
FLINK_TABLE_NAME = "sensor_readings"
create_table_flink_sql = f"""
CREATE TABLE IF NOT EXISTS {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} (
    sensor_id STRING,
    temperature DOUBLE,
    ts TIMESTAMP(3),  -- Event time timestamp
    -- metadata for event time and watermarking if using Flink's advanced streaming
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
)
PARTITIONED BY (sensor_id) -- Simple partitioning by sensor_id
WITH (
    'format-version'='2',
    'write.upsert.enabled'='false' -- Keep it simple, no upserts for now
)
"""
# The WATERMARK definition is for Flink's event time processing, Iceberg stores 'ts' as a regular timestamp.
try:
    table_env.execute_sql(create_table_flink_sql)
    print(f"Table '{FLINK_TABLE_NAME}' created successfully.")
    table_env.execute_sql(f"SHOW TABLES").print()
except Exception as e:
    print(f"Error creating Flink Iceberg table: {e}")

# Cell 6: Batch Insert Data using Flink SQL (INSERT INTO)
# For batch inserts, you might switch to batch mode or use INSERT INTO for streaming appends.
# Here, INSERT INTO will work in streaming mode as simple appends.
insert_data_flink_sql = f"""
INSERT INTO {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} VALUES
('sensorA', 22.5, TIMESTAMP '2023-10-26 10:00:00.123'),
('sensorB', 25.1, TIMESTAMP '2023-10-26 10:00:05.456'),
('sensorA', 22.7, TIMESTAMP '2023-10-26 10:01:00.789'),
('sensorC', 19.3, TIMESTAMP '2023-10-26 10:01:05.000')
"""
try:
    # For INSERT statements, execute_sql returns a TableResult which needs to be waited upon
    # or collected if it's a select. For inserts, it triggers the job.
    # In local mode, this might execute quickly.
    table_result_insert = table_env.execute_sql(insert_data_flink_sql)
    table_result_insert.wait() # Wait for the insert job to finish for batch-like behavior
    print(f"Data inserted into '{FLINK_TABLE_NAME}'. Job status: {table_result_insert.get_job_client().get_job_status()}")
except Exception as e:
    print(f"Error inserting data with Flink SQL: {e}")

# Cell 7: Select Data using Flink SQL (Batch Query)
# This will run a Flink job to read the data.
print(f"\nSelecting all data from '{FLINK_TABLE_NAME}':")
try:
    table_result_select = table_env.execute_sql(f"SELECT sensor_id, temperature, ts FROM {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME}")
    table_result_select.print() # This collects and prints results for bounded queries
except Exception as e:
    print(f"Error selecting data with Flink SQL: {e}")

# Cell 8: Streaming Read from Iceberg Table (Illustrative)
# Flink excels at streaming reads. This sets up a streaming query.
# To actually see continuous output, you'd typically sink it to another table or print sink.
print(f"\nSetting up a streaming select from '{FLINK_TABLE_NAME}' (conceptual):")
# For a true streaming read that updates, you might use a print sink or another sink.
# table_env.execute_sql(f"SELECT * FROM {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s') */")\
#    .execute().print() # This would run indefinitely in a real Flink job

# For demonstration in a notebook, a bounded read is easier.
# The previous select already demonstrated reading.
# A streaming read would look like:
# table_streaming_read = table_env.from_path(f"{FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME}")
# table_streaming_read.execute().print() # This would run continuously or until Flink stops it.


# Cell 9: Writing to Iceberg Table using Table API from a DataStream (Streaming Insert)
# Create a simple DataStream
s_env = StreamExecutionEnvironment.get_execution_environment()
# Ensure S3A configuration for DataStream API if it uses it directly (usually Table API handles it)
# s_env.getConfig().setGlobalJobParameter("fs.s3a.endpoint", MINIO_ENDPOINT) ... etc.

# Example data for streaming insert
stream_data = [
    ('sensorB', 26.0, java.sql.Timestamp.valueOf("2023-10-26 10:02:00.000")),
    ('sensorA', 22.9, java.sql.Timestamp.valueOf("2023-10-26 10:02:30.100"))
]

# Define RowType for schema matching
# Note: Flink's TimestampType(3) for SQL corresponds to java.sql.Timestamp
from pyflink.common.typeinfo import Types as FlinkTypes
pyflink_type_info = FlinkTypes.ROW_NAMED(
    ['sensor_id', 'temperature', 'ts'],
    [FlinkTypes.STRING(), FlinkTypes.DOUBLE(), FlinkTypes.SQL_TIMESTAMP()]
)

data_stream = s_env.from_collection(stream_data, type_info=pyflink_type_info)

# Convert DataStream to Table
input_table = table_env.from_data_stream(data_stream,
                           col("sensor_id"),
                           col("temperature"),
                           col("ts").cast(DataTypes.TIMESTAMP(3)) # Ensure correct timestamp precision
                          )
table_env.create_temporary_view("source_stream", input_table)

print("\nInserting data via Table API from a DataStream:")
try:
    # Use SQL Insert from the temporary view, or Table API's execute_insert
    # statement_set = table_env.create_statement_set()
    # statement_set.add_insert(f"{FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME}", input_table)
    # job_client_streaming_insert = statement_set.execute().get_job_client()

    # Simpler: direct SQL insert from the view
    streaming_insert_sql = f"INSERT INTO {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} SELECT sensor_id, temperature, ts FROM source_stream"
    job_client_streaming_insert = table_env.execute_sql(streaming_insert_sql).get_job_client()

    if job_client_streaming_insert:
        print(f"Streaming insert job submitted. Job ID: {job_client_streaming_insert.get_job_id()}")
        # In a real streaming job, it would run until cancelled.
        # For this notebook example, this will process the finite stream and complete.
        # We might need to wait for it for the data to be queryable immediately.
        # However, `wait()` is not available on JobClient directly in all PyFlink versions/contexts for streaming.
        # We can check status.
        # For this example, let's assume it finishes quickly with the bounded stream.
        # To ensure data is committed for next select, might need a short pause or a more robust way to check completion.
        import time
        time.sleep(10) # Give Flink some time to process and commit for this demo.
        print(f"Assumed streaming insert for '{FLINK_TABLE_NAME}' completed processing the collection.")

except Exception as e:
    print(f"Error inserting data with Flink Table API: {e}")


# Cell 10: Verify Data After Streaming Insert
print(f"\nSelecting all data from '{FLINK_TABLE_NAME}' after streaming insert:")
try:
    table_env.execute_sql(f"SELECT sensor_id, temperature, ts FROM {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} ORDER BY ts").print()
except Exception as e:
    print(f"Error selecting data: {e}")


# Cell 11: Iceberg Metadata (Snapshots) - Flink doesn't expose these as easily as Spark/Trino SQL tables
# To inspect Iceberg metadata like snapshots, history, files, manifests with Flink,
# you typically use the Iceberg Java API or use Spark/Trino connected to the same catalog.
# Flink SQL focuses on DML and DDL.
# For example, to see snapshots, you'd use Trino or Spark as in other notebooks.
print("\nTo inspect Iceberg table metadata (snapshots, files, etc.):")
print("Use Trino: SELECT * FROM iceberg.flink_schema.\"sensor_readings$history\"")
print("Or Spark: spark.sql(f\"SELECT * FROM iceberg_catalog.flink_schema.sensor_readings.history\").show()")


# Cell 12: Compaction / Maintenance
# Iceberg specific maintenance operations like compaction are often triggered via Spark or Iceberg's own procedures.
# Flink can benefit from tables being compacted, but Flink itself (as of 1.18)
# doesn't have direct SQL commands like Spark's "CALL system.rewrite_data_files()".
# You would run compaction using Spark or the Iceberg API against the tables Flink uses.
print("\nData compaction for Iceberg tables used by Flink would typically be done by Spark or Iceberg Java API.")
# Example Spark command from notebook 02:
# spark.sql(f"CALL iceberg_catalog.system.rewrite_data_files(table => 'flink_schema.sensor_readings', strategy => 'sort')").show()

# Cell 13: Schema Evolution (Example: Add a new column via Flink SQL)
# Flink supports schema evolution for Iceberg tables.
try:
    print(f"\nSchema before evolution for {FLINK_TABLE_NAME}:")
    table_env.execute_sql(f"DESCRIBE {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME}").print()

    table_env.execute_sql(f"ALTER TABLE {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} ADD COLUMNS (location STRING)")
    print(f"\nSchema after adding 'location' column:")
    table_env.execute_sql(f"DESCRIBE {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME}").print()

    # Insert data with the new column
    insert_new_schema_sql = f"""
    INSERT INTO {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} VALUES
    ('sensorA', 23.0, TIMESTAMP '2023-10-26 10:03:00.000', 'room1'),
    ('sensorB', 26.5, TIMESTAMP '2023-10-26 10:03:30.000', 'room2')
    """
    evolve_insert_result = table_env.execute_sql(insert_new_schema_sql)
    evolve_insert_result.wait()
    print("Data inserted with new schema.")

    print(f"\nData from {FLINK_TABLE_NAME} after schema evolution:")
    table_env.execute_sql(f"SELECT sensor_id, temperature, ts, location FROM {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} ORDER BY ts").print()

except Exception as e:
    print(f"Error during schema evolution or subsequent insert: {e}")


# Cell 14: Clean up (Optional)
# table_env.execute_sql(f"DROP TABLE IF EXISTS {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME}")
# table_env.execute_sql(f"DROP DATABASE IF EXISTS {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}")
# table_env.execute_sql(f"DROP CATALOG IF EXISTS {FLINK_ICEBERG_CATALOG_NAME}")
print("\nFlink Iceberg Datalakehouse Demo (Phase 3) completed.")

# Note: For long-running streaming jobs, you would typically submit them to the Flink cluster
# using `flink run` CLI or via the Flink Dashboard, not run them directly to completion in a notebook cell.
# This notebook uses local execution mode for PyFlink, which is good for development and testing.
```

Key points in Flink Notebook:

* Environment Setup: Initializes StreamTableEnvironment. Crucially, it sets S3 configurations and pipeline.jars for the local PyFlink execution context. This allows the notebook itself to interact with MinIO and use Iceberg without relying on a remote Flink cluster for every command.
* Catalog Creation: Uses Flink SQL DDL (CREATE CATALOG) to register the existing Iceberg JDBC catalog. This catalog is named iceberg_flink_catalog within Flink.
* DDL/DML: Demonstrates CREATE DATABASE, USE DATABASE, CREATE TABLE (with partitioning), INSERT INTO, and SELECT using Flink SQL.
* Table API for Streaming Insert: Shows how to create a DataStream, convert it to a Flink Table, and insert it into an Iceberg table.
* Schema Evolution: Demonstrates ALTER TABLE ADD COLUMNS.
* Metadata/Compaction: Points out that these are typically handled by other tools like Spark or Trino when working with Flink.

### Step 6: Update README.md

Add instructions for Flink and how to run the new notebook.
(# Datalakehouse with Iceberg, MinIO, PostgreSQL, Trino, Spark, Flink, and JupyterLab

This project demonstrates a Docker Compose setup for a datalakehouse using Apache Iceberg.

**Services:**

* **MinIO:** S3-compatible object storage for Iceberg table data.
* **PostgreSQL:** Stores Iceberg metadata using the JDBC catalog.
* **Trino:** Query engine to interact with Iceberg tables.
* **Spark (Master & Worker):** Processing engine for batch, integrated with Iceberg.
* **Flink (JobManager & TaskManager):** Stream and batch processing engine, integrated with Iceberg.
* **JupyterLab:** For running example notebooks (Python, Trino CLI, PySpark, PyFlink).

## Prerequisites

* Docker
* Docker Compose

## Setup

1. **Clone the repository (or create the files as described).**
2. **Create JARs directories and download Flink JARs:**

    ```bash
    mkdir -p ./jars/flink
    # mkdir -p ./jars/spark # if you were to manage spark jars manually
    ```

    Download the following JARs into `./jars/flink/`:
    * `iceberg-flink-runtime-1.18-1.5.0.jar` (or matching your Iceberg/Flink versions)
    * `postgresql-42.6.0.jar`
    * `flink-s3-fs-hadoop-1.18.1.jar` (or matching your Flink version)
    (See Phase 3 documentation for download links)

3. **Create JupyterLab Dockerfile context:**

    ```bash
    mkdir -p ./config/jupyter
    ```

    Place the `Dockerfile` (provided in Phase 3) into `./config/jupyter/`.

4. **Start the services:**

    ```bash
    docker-compose up -d --build
    ```

    The `--build` flag is important because we added a Dockerfile for the JupyterLab service.
    Wait for all services to become healthy. Check with `docker-compose ps`.
    * The first time, JupyterLab will build its image (installing PyFlink, PyIceberg), which might take a few minutes.
    * Spark in JupyterLab will download its packages.
    * Flink JobManager/TaskManager will start and load JARs from `/opt/flink/usrlib`.

5. **Access Services:**
    * **MinIO Console:** `http://localhost:9001` (Credentials: `admin`/`password`)
    * **PostgreSQL:** `localhost:5432` (User: `iceberg`, Pass: `icebergpassword`, DB: `iceberg_catalog`)
    * **Trino UI:** `http://localhost:8080`
    * **Spark Master UI:** `http://localhost:8081`
    * **Flink Web UI:** `http://localhost:8088` (Note the port is 8088)
    * **JupyterLab:** `http://localhost:8888` (Token: `icebergrocks`)
        * Navigate to the `work/` directory.
        * PySpark Application UI (for driver): `http://localhost:4040`

6. **Run the Jupyter Notebooks:**
    * `01-trino-iceberg-getting-started.ipynb` (Trino examples)
    * `02-spark-iceberg-examples.ipynb` (Spark examples)
    * `03-flink-iceberg-examples.ipynb` (Flink examples)
      * The Flink notebook uses a local PyFlink execution environment but is configured to use the shared Iceberg catalog (PostgreSQL) and data storage (MinIO). JARs specified in the notebook should be present in `./jars/flink` as they are also mounted to `/home/jovyan/jars`.

### Teardown

```bash
docker-compose down -v # -v removes volumes including MinIO data and PostgreSQL data
```

)

Further Exploration (PyIceberg)

* Add a notebook 04-pyiceberg-examples.ipynb to demonstrate direct interaction with Iceberg tables using the PyIceberg library.
Project Structure Overview
datalakehouse-iceberg-jdbc/
├── docker-compose.yml
├── config/
│   ├── flink/
│   │   └── flink-conf.yaml  # Optional Flink config
│   ├── jupyter/
│   │   └── Dockerfile       # For custom JupyterLab image with PyFlink
│   ├── postgres/
│   │   └── init.sql
│   ├── spark/
│   │   └── spark-defaults.conf
│   └── trino/
│       └── catalog/
│           └── iceberg.properties
├── jars/                    # For manually managed JARs
│   ├── flink/               # Flink specific JARs (iceberg-flink-runtime, postgresql, flink-s3)
│   └── spark/               # (If managing Spark JARs manually)
├── notebooks/
│   ├── 01-trino-iceberg-getting-started.ipynb
│   ├── 02-spark-iceberg-examples.ipynb
│   ├── 03-flink-iceberg-examples.ipynb
│   └── # (Soon: 04-pyiceberg-examples.ipynb)
├── s3data/                  # MinIO local storage (mounted)
└── README.md

**To run this Phase 3:**

1. Ensure all files from Phase 1 and Phase 2 are in place and updated as per the instructions above.
2. Create the `./jars/flink/` directory and download the specified JARs into it.
3. Create the `./config/jupyter/Dockerfile` with the content provided.
4. Create the optional `./config/flink/flink-conf.yaml`.
5. If running, stop the current stack: `docker-compose down -v`.
6. Build and start the services: `docker-compose up -d --build`. (The `--build` is crucial for JupyterLab).
7. Wait for services. Check the Flink UI at `http://localhost:8088` to see if JobManager and TaskManager(s) are running.
8. Access JupyterLab, open `03-flink-iceberg-examples.ipynb`, and run the cells. Check MinIO for new table data under `iceberg-warehouse/flink_schema/`.

This completes the Flink integration. The next step will be to add the PyIceberg examples notebook.
