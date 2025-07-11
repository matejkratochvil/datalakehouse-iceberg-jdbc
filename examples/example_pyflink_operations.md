# PyFlink examples

```py
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.expressions import lit, col
from pyflink.table.types import RowType, ecológicaRecordType # For schema definition if needed
```

## Define connection parameters (should match docker-compose and other services)

```py
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
iceberg_catalog_URL = "jdbc:postgresql://postgres:5432/iceberg_catalog"
iceberg_catalog_USER = "iceberg"
iceberg_catalog_PASSWORD = "icebergpassword"
ICEBERG_WAREHOUSE_PATH = "s3a://iceberg-warehouse/" # Root for this catalog
```

```python
#Catalog name for Flink to use
FLINK_ICEBERG_CATALOG_NAME = "iceberg_flink_catalog" # Flink's instance of the Iceberg catalog
```

## Initialize Flink Table Environment

```py
# Using Blink planner (default) and streaming mode for this example, can also use batch
env_settings = EnvironmentSettings.in_streaming_mode() # or .in_batch_mode()
# For local execution within Jupyter:
table_env = StreamTableEnvironment.create(environment_settings=env_settings)
```

## Configure S3A for Flink (globally for the local Flink execution)

```py
# These are crucial for PyFlink local execution to access MinIO
table_env.get_config().get_configuration().set_string("fs.s3a.endpoint", MINIO_ENDPOINT)
table_env.get_config().get_configuration().set_string("fs.s3a.access.key", MINIO_ACCESS_KEY)
table_env.get_config().get_configuration().set_string("fs.s3a.secret.key", MINIO_SECRET_KEY)
table_env.get_config().get_configuration().set_string("fs.s3a.path.style.access", "true")
table_env.get_config().get_configuration().set_string("fs.s3a.connection.ssl.enabled", "false")
```

## Add necessary JARs for local PyFlink execution

```py
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
```

```py
print("Flink TableEnvironment initialized for local execution.")
print(f"Make sure these JARs exist: {jar_paths}")
print("If submitting to remote cluster, JARs in /opt/flink/usrlib of JM/TM are used.")
```

## Create an Iceberg Catalog in Flink

```py
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
```

```py
table_env.execute_sql(create_catalog_ddl)
print(f"Iceberg catalog '{FLINK_ICEBERG_CATALOG_NAME}' created successfully.")
table_env.execute_sql(f"USE CATALOG {FLINK_ICEBERG_CATALOG_NAME}")
print(f"Using catalog: {FLINK_ICEBERG_CATALOG_NAME}")
```

## Create a Database/Schema in the Iceberg Catalog using Flink SQL

```py
DB_NAME_FLINK = "flink_schema"
table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME_FLINK}")
table_env.execute_sql(f"USE {DB_NAME_FLINK}") # Sets the current database for the session
print(f"Using database: {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}")
table_env.execute_sql(f"SHOW DATABASES").print()
```

## Create an Iceberg Table using Flink SQL

```py
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
```

```py
## The WATERMARK definition is for Flink's event time processing, Iceberg stores 'ts' as a regular timestamp
table_env.execute_sql(create_table_flink_sql)
print(f"Table '{FLINK_TABLE_NAME}' created successfully.")
table_env.execute_sql(f"SHOW TABLES").print()
```

## Batch Insert Data using Flink SQL (INSERT INTO)

For batch inserts, you might switch to batch mode or use INSERT INTO for streaming appends.
Here, INSERT INTO will work in streaming mode as simple appends.

```py
insert_data_flink_sql = f"""
INSERT INTO {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} VALUES
('sensorA', 22.5, TIMESTAMP '2023-10-26 10:00:00.123'),
('sensorB', 25.1, TIMESTAMP '2023-10-26 10:00:05.456'),
('sensorA', 22.7, TIMESTAMP '2023-10-26 10:01:00.789'),
('sensorC', 19.3, TIMESTAMP '2023-10-26 10:01:05.000')
"""
# For INSERT statements, execute_sql returns a TableResult which needs to be waited upon
# or collected if it's a select. For inserts, it triggers the job.
# In local mode, this might execute quickly.
table_result_insert = table_env.execute_sql(insert_data_flink_sql)
table_result_insert.wait() # Wait for the insert job to finish for batch-like behavior
print(f"Data inserted into '{FLINK_TABLE_NAME}'. Job status: {table_result_insert.get_job_client().get_job_status()}")
```

## Select Data using Flink SQL (Batch Query)

```py
# This will run a Flink job to read the data.
print(f"\nSelecting all data from '{FLINK_TABLE_NAME}':")
table_result_select = table_env.execute_sql(f"SELECT sensor_id, temperature, ts FROM {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME}")
table_result_select.print() # This collects and prints results for bounded queries
```

## Streaming Read from Iceberg Table (Illustrative)

```py
# Flink excels at streaming reads. This sets up a streaming query.
# To actually see continuous output, you'd typically sink it to another table or print sink.
print(f"\nSetting up a streaming select from '{FLINK_TABLE_NAME}' (conceptual):")
# For a true streaming read that updates, you might use a print sink or another sink.
table_env.execute_sql(f"SELECT * FROM {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s') */")\
    .execute().print() # This would run indefinitely in a real Flink job

# For demonstration in a notebook, a bounded read is easier.
# The previous select already demonstrated reading.
# A streaming read would look like:
```py
table_streaming_read = table_env.from_path(f"{FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME}")
table_streaming_read.execute().print() # This would run continuously or until Flink stops it.
```

## Writing to Iceberg Table using Table API from a DataStream (Streaming Insert)

```py
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
```

## Convert DataStream to Table

```py
input_table = table_env.from_data_stream(data_stream,
                           col("sensor_id"),
                           col("temperature"),
                           col("ts").cast(DataTypes.TIMESTAMP(3)) # Ensure correct timestamp precision
                          )
table_env.create_temporary_view("source_stream", input_table)
```

```py
print("\nInserting data via Table API from a DataStream:")
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
```

## Verify Data After Streaming Insert

```py
print(f"\nSelecting all data from '{FLINK_TABLE_NAME}' after streaming insert:")
table_env.execute_sql(f"""
SELECT sensor_id, temperature, ts 
FROM {FLINK_ICEBERG_CATALOG_NAME}.{DB_NAME_FLINK}.{FLINK_TABLE_NAME} 
ORDER BY ts
""").print()
```

## Iceberg Metadata (Snapshots) - Flink doesn't expose these as easily as Spark/Trino SQL tables

- To inspect Iceberg metadata like snapshots, history, files, manifests with Flink,
you typically use the Iceberg Java API or use Spark/Trino connected to the same catalog.

- Flink SQL focuses on DML and DDL.
- For example, to see snapshots, you'd use Trino or Spark as in other notebooks.
- To inspect Iceberg table metadata (snapshots, files, etc.):
  - Use Trino: `SELECT * FROM iceberg.flink_schema."sensor_readings$history"`
  - Or Spark: `spark.sql(f"SELECT * FROM iceberg_catalog.flink_schema.sensor_readings.history").show()`

## Compaction / Maintenance

- Iceberg specific maintenance operations like compaction are often triggered via Spark or Iceberg's own procedures.
- Flink can benefit from tables being compacted, but Flink itself (as of 1.18)
  doesn't have direct SQL commands like Spark's "CALL system.rewrite_data_files()".
- You would run compaction using Spark or the Iceberg API against the tables Flink uses.

Example Spark command from notebook 02:

```py
spark.sql(f"CALL iceberg_catalog.system.rewrite_data_files(table => 'flink_schema.sensor_readings', strategy => 'sort')").show()
```

## Schema Evolution (Example: Add a new column via Flink SQL)

```py
# Flink supports schema evolution for Iceberg tables.
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

print("\nFlink Iceberg Datalakehouse Demo (Phase 3) completed.")
```

Note: For long-running streaming jobs, you would typically submit them to the Flink cluster
using `flink run` CLI or via the Flink Dashboard, not run them directly to completion in a notebook cell.

This notebook uses local execution mode for PyFlink, which is good for development and testing.

Key points:

- Environment Setup: Initializes StreamTableEnvironment. Crucially, it sets S3 configurations and pipeline.jars for the local PyFlink execution context. This allows the notebook itself to interact with MinIO and use Iceberg without relying on a remote Flink cluster for every command.
- Catalog Creation: Uses Flink SQL DDL (CREATE CATALOG) to register the existing Iceberg JDBC catalog. This catalog is named iceberg_flink_catalog within Flink.
- DDL/DML: Demonstrates CREATE DATABASE, USE DATABASE, CREATE TABLE (with partitioning), INSERT INTO, and SELECT using Flink SQL.
- Table API for Streaming Insert: Shows how to create a DataStream, convert it to a Flink Table, and insert it into an Iceberg table.
- Schema Evolution: Demonstrates ALTER TABLE ADD COLUMNS.
- Metadata/Compaction: Points out that these are typically handled by other tools like Spark or Trino when working with Flink.
