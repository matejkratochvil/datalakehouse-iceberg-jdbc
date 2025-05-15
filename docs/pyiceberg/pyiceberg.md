# Getting started with PyIceberg

PyIceberg is a Python implementation for accessing Iceberg tables, without the need of a JVM.

## Installation

Before installing PyIceberg, make sure that you're on an up-to-date version of `pip`:

```sh
pip install --upgrade pip
```

You can install the latest release version from pypi:

```sh
pip install "pyiceberg[s3fs,hive]"
```

You can mix and match optional dependencies depending on your needs:

| Key           | Description:                                                              |
| ------------- | ------------------------------------------------------------------------- |
| hive          | Support for the Hive metastore                                            |
| sql-postgres  | Support for SQL Catalog backed by Postgresql                              |
| sql-sqlite    | Support for SQL Catalog backed by SQLite                                  |
| pyarrow       | PyArrow as a FileIO implementation to interact with the object store      |
| pandas        | Installs both PyArrow and Pandas                                          |
| duckdb        | Installs both PyArrow and DuckDB                                          |
| polars        | Installs Polars                                                           |
| s3fs          | S3FS as a FileIO implementation to interact with the object store         |
| snappy        | Support for snappy Avro compression                                       |
| rest-sigv4    | Support for generating AWS SIGv4 authentication headers for REST Catalogs |

You either need to install `s3fs` or `pyarrow` to be able to fetch files from an object store.

## Connecting to a catalog

Iceberg leverages the [catalog to have one centralized place to organize the tables](https://iceberg.apache.org/terms/#catalog). This can be a traditional Hive catalog to store your Iceberg tables next to the rest, a vendor solution like the AWS Glue catalog, or an implementation of Icebergs' own [REST protocol](https://github.com/apache/iceberg/tree/main/open-api). Checkout the [configuration](pyiceberg-configuration.md) page to find all the configuration details.

For the sake of demonstration, we'll configure the catalog to use the `SqlCatalog` implementation, which will store information in a local `sqlite` database. We'll also configure the catalog to store data files in the local filesystem instead of an object store. This should not be used in production due to the limited scalability.

Create a temporary location for Iceberg:

```shell
mkdir /tmp/warehouse
```

Open a Python 3 REPL to set up the catalog:

```python
from pyiceberg.catalog import load_catalog

warehouse_path = "/tmp/warehouse"
catalog = load_catalog(
    "default",
    **{
        'type': 'sql',
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)
```

The `sql` catalog works for testing locally without needing another service. If you want to try out another catalog, please [check out the configuration](pyiceberg-configuration.md#catalogs).

## Write a PyArrow dataframe

Let's take the Taxi dataset, and write this to an Iceberg table.

First download one month of data:

```shell
curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet -o /tmp/yellow_tripdata_2023-01.parquet
```

Load it into your PyArrow dataframe:

```python
import pyarrow.parquet as pq

df = pq.read_table("/tmp/yellow_tripdata_2023-01.parquet")
```

Create a new Iceberg table:

```python
catalog.create_namespace("default")

table = catalog.create_table(
    "default.taxi_dataset",
    schema=df.schema,
)
```

Append the dataframe to the table:

```python
table.append(df)
len(table.scan().to_arrow())
```

3066766 rows have been written to the table.

Now generate a tip-per-mile feature to train the model on:

```python
import pyarrow.compute as pc

df = df.append_column("tip_per_mile", pc.divide(df["tip_amount"], df["trip_distance"]))
```

Evolve the schema of the table with the new column:

```python
with table.update_schema() as update_schema:
    update_schema.union_by_name(df.schema)
```

And now we can write the new dataframe to the Iceberg table:

```python
table.overwrite(df)
print(table.scan().to_arrow())
```

And the new column is there:

```python
taxi_dataset(
  1: VendorID: optional long,
  2: tpep_pickup_datetime: optional timestamp,
  3: tpep_dropoff_datetime: optional timestamp,
  ..
  ..
  19: airport_fee: optional double,
  20: tip_per_mile: optional double
),
```

```python
df = table.scan(row_filter="tip_per_mile > 0").to_arrow()
len(df)
```

### Explore Iceberg data and metadata files

Since the catalog was configured to use the local filesystem, we can explore how Iceberg saved data and metadata files from the above operations.

```shell
find /tmp/warehouse/
```
