# PyIceberg API

PyIceberg is based around catalogs to load tables. First step is to instantiate a catalog that loads tables. Let's use the following configuration to define a catalog called `prod`:

```yaml
catalog:
  prod:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret
```

Note that multiple catalogs can be defined in the same `.pyiceberg.yaml`:

```yaml
catalog:
  hive:
    uri: thrift://127.0.0.1:9083
    s3.endpoint: http://127.0.0.1:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
  rest:
    uri: https://rest-server:8181/
    warehouse: my-warehouse
```

and loaded in python by calling `load_catalog(name="hive")` and `load_catalog(name="rest")`.

This information must be placed inside a file called `.pyiceberg.yaml` located either in the `$HOME` or `%USERPROFILE%` directory (depending on whether the operating system is Unix-based or Windows-based, respectively), in the current working directory, or in the `$PYICEBERG_HOME` directory (if the corresponding environment variable is set).

For more details on possible configurations refer to the [configuration](pyiceberg-configuration.md).

Then load the `prod` catalog:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "docs",
    **{
        "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:9000",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
)
```

Let's create a namespace:

```python
catalog.create_namespace("docs_example")
```

And then list them:

```python
ns = catalog.list_namespaces()

assert ns == [("docs_example",)]
```

And then list tables in the namespace:

```python
catalog.list_tables("docs_example")
```

## Create a table

To create a table from a catalog:

```python
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)

schema = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
    NestedField(
        field_id=5,
        name="details",
        field_type=StructType(
            NestedField(
                field_id=4, name="created_by", field_type=StringType(), required=False
            ),
        ),
        required=False,
    ),
)

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

partition_spec = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

# Sort on the symbol
sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

catalog.create_table(
    identifier="docs_example.bids",
    schema=schema,
    location="s3://pyiceberg",
    partition_spec=partition_spec,
    sort_order=sort_order,
)
```

When the table is created, all IDs in the schema are re-assigned to ensure uniqueness.

To create a table using a pyarrow schema:

```python
import pyarrow as pa

schema = pa.schema(
    [
        pa.field("foo", pa.string(), nullable=True),
        pa.field("bar", pa.int32(), nullable=False),
        pa.field("baz", pa.bool_(), nullable=True),
    ]
)

catalog.create_table(
    identifier="docs_example.bids",
    schema=schema,
)
```

To create a table with some subsequent changes atomically in a transaction:

```python
with catalog.create_table_transaction(
    identifier="docs_example.bids",
    schema=schema,
    location="s3://pyiceberg",
    partition_spec=partition_spec,
    sort_order=sort_order,
) as txn:
    with txn.update_schema() as update_schema:
        update_schema.add_column(path="new_column", field_type=StringType())

    with txn.update_spec() as update_spec:
        update_spec.add_identity("symbol")

    txn.set_properties(test_a="test_aa", test_b="test_b", test_c="test_c")
```

## Load a table

### Catalog table

Loading the `bids` table:

```python
table = catalog.load_table("docs_example.bids")
# Equivalent to:
table = catalog.load_table(("docs_example", "bids"))
# The tuple syntax can be used if the namespace or table contains a dot.
```

This returns a `Table` that represents an Iceberg table that can be queried and altered.

### Static table

To load a table directly from a metadata file (i.e., **without** using a catalog), you can use a `StaticTable` as follows:

```python
from pyiceberg.table import StaticTable

static_table = StaticTable.from_metadata(
    "s3://warehouse/wh/nyc.db/taxis/metadata/00002-6ea51ce3-62aa-4197-9cf8-43d07c3440ca.metadata.json"
)
```

The static-table is considered read-only.

Alternatively, if your table metadata directory contains a `version-hint.text` file, you can just specify
the table root path, and the latest metadata file will be picked automatically.

```python
from pyiceberg.table import StaticTable

static_table = StaticTable.from_metadata(
    "s3://warehouse/wh/nyc.db/taxis
)
```

## Check if a table exists

```python
catalog.table_exists("docs_example.bids")
```

`True` if the table already exists.

## Write support

Write support is added through Arrow. Let's consider an Arrow Table:

```python
import pyarrow as pa

df = pa.Table.from_pylist(
    [
        {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
        {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
        {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
        {"city": "Paris", "lat": 48.864716, "long": 2.349014},
    ],
)
```

Next, create a table based on the schema:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
)

tbl = catalog.create_table("default.cities", schema=schema)
```

Now write the data to the table:

"Fast append"

- PyIceberg default to the [fast append](https://iceberg.apache.org/spec/#snapshots) to minimize the amount of data written. This enables quick writes, reducing the possibility of conflicts. The downside of the fast append is that it creates more metadata than a normal commit. [Compaction is planned](https://github.com/apache/iceberg-python/issues/270) and will automatically rewrite all the metadata when a threshold is hit, to maintain performant reads.

```python
tbl.append(df)

# or

tbl.overwrite(df)
```

The data is written to the table, and when the table is read using `tbl.scan().to_arrow()`:

```python
pyarrow.Table
city: string
lat: double
long: double
----
city: [["Amsterdam","San Francisco","Drachten","Paris"]]
lat: [[52.371807,37.773972,53.11254,48.864716]]
long: [[4.896029,-122.431297,6.0989,2.349014]]
```

You both can use `append(df)` or `overwrite(df)` since there is no data yet. If we want to add more data, we can use `.append()` again:

```python
df = pa.Table.from_pylist(
    [{"city": "Groningen", "lat": 53.21917, "long": 6.56667}],
)

tbl.append(df)
```

When reading the table `tbl.scan().to_arrow()` you can see that `Groningen` is now also part of the table:

```python
pyarrow.Table
city: string
lat: double
long: double
----
city: [["Amsterdam","San Francisco","Drachten","Paris"],["Groningen"]]
lat: [[52.371807,37.773972,53.11254,48.864716],[53.21917]]
long: [[4.896029,-122.431297,6.0989,2.349014],[6.56667]]
```

The nested lists indicate the different Arrow buffers, where the first write results into a buffer, and the second append in a separate buffer. This is expected since it will read two parquet files.

To avoid any type errors during writing, you can enforce the PyArrow table types using the Iceberg table schema:

```python
from pyiceberg.catalog import load_catalog
import pyarrow as pa

catalog = load_catalog("default")
table = catalog.load_table("default.cities")
schema = table.schema().as_arrow()

df = pa.Table.from_pylist(
    [{"city": "Groningen", "lat": 53.21917, "long": 6.56667}], schema=schema
)

table.append(df)
```

You can delete some of the data from the table by calling `tbl.delete()` with a desired `delete_filter`.

```python
tbl.delete(delete_filter="city == 'Paris'")
```

In the above example, any records where the city field value equals to `Paris` will be deleted.
Running `tbl.scan().to_arrow()` will now yield:

```python
pyarrow.Table
city: string
lat: double
long: double
----
city: [["Amsterdam","San Francisco","Drachten"],["Groningen"]]
lat: [[52.371807,37.773972,53.11254],[53.21917]]
long: [[4.896029,-122.431297,6.0989],[6.56667]]
```

### Partial overwrites

When using the `overwrite` API, you can use an `overwrite_filter` to delete data that matches the filter before appending new data into the table.

For example, with an iceberg table created as:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
)

tbl = catalog.create_table("default.cities", schema=schema)
```

And with initial data populating the table:

```python
import pyarrow as pa
df = pa.Table.from_pylist(
    [
        {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
        {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
        {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
        {"city": "Paris", "lat": 48.864716, "long": 2.349014},
    ],
)
tbl.append(df)
```

You can overwrite the record of `Paris` with a record of `New York`:

```python
from pyiceberg.expressions import EqualTo
df = pa.Table.from_pylist(
    [
        {"city": "New York", "lat": 40.7128, "long": 74.0060},
    ]
)
tbl.overwrite(df, overwrite_filter=EqualTo('city', "Paris"))
```

This produces the following result with `tbl.scan().to_arrow()`:

```python
pyarrow.Table
city: large_string
lat: double
long: double
----
city: [["New York"],["Amsterdam","San Francisco","Drachten"]]
lat: [[40.7128],[52.371807,37.773972,53.11254]]
long: [[74.006],[4.896029,-122.431297,6.0989]]
```

If the PyIceberg table is partitioned, you can use `tbl.dynamic_partition_overwrite(df)` to replace the existing partitions with new ones provided in the dataframe. The partitions to be replaced are detected automatically from the provided arrow table.
For example, with an iceberg table with a partition specified on `"city"` field:

```python
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, NestedField, StringType

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
)

tbl = catalog.create_table(
    "default.cities",
    schema=schema,
    partition_spec=PartitionSpec(PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="city_identity"))
)
```

And we want to overwrite the data for the partition of `"Paris"`:

```python
import pyarrow as pa

df = pa.Table.from_pylist(
    [
        {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
        {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
        {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
        {"city": "Paris", "lat": -48.864716, "long": -2.349014},
    ],
)
tbl.append(df)
```

Then we can call `dynamic_partition_overwrite` with this arrow table:

```python
df_corrected = pa.Table.from_pylist([
    {"city": "Paris", "lat": 48.864716, "long": 2.349014}
])
tbl.dynamic_partition_overwrite(df_corrected)
```

This produces the following result with `tbl.scan().to_arrow()`:

```python
pyarrow.Table
city: large_string
lat: double
long: double
----
city: [["Paris"],["Amsterdam"],["Drachten"],["San Francisco"]]
lat: [[48.864716],[52.371807],[53.11254],[37.773972]]
long: [[2.349014],[4.896029],[6.0989],[-122.431297]]
```

### Upsert

PyIceberg supports upsert operations, meaning that it is able to merge an Arrow table into an Iceberg table. Rows are considered the same based on the [identifier field](https://iceberg.apache.org/spec/?column-projection#identifier-field-ids). If a row is already in the table, it will update that row. If a row cannot be found, it will insert that new row.

Consider the following table, with some data:

```python
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

import pyarrow as pa

schema = Schema(
    NestedField(1, "city", StringType(), required=True),
    NestedField(2, "inhabitants", IntegerType(), required=True),
    # Mark City as the identifier field, also known as the primary-key
    identifier_field_ids=[1]
)

tbl = catalog.create_table("default.cities", schema=schema)

arrow_schema = pa.schema(
    [
        pa.field("city", pa.string(), nullable=False),
        pa.field("inhabitants", pa.int32(), nullable=False),
    ]
)

# Write some data
df = pa.Table.from_pylist(
    [
        {"city": "Amsterdam", "inhabitants": 921402},
        {"city": "San Francisco", "inhabitants": 808988},
        {"city": "Drachten", "inhabitants": 45019},
        {"city": "Paris", "inhabitants": 2103000},
    ],
    schema=arrow_schema
)
tbl.append(df)
```

Next, we'll upsert a table into the Iceberg table:

```python
df = pa.Table.from_pylist(
    [
        # Will be updated, the inhabitants has been updated
        {"city": "Drachten", "inhabitants": 45505},

        # New row, will be inserted
        {"city": "Berlin", "inhabitants": 3432000},

        # Ignored, already exists in the table
        {"city": "Paris", "inhabitants": 2103000},
    ],
    schema=arrow_schema
)
upd = tbl.upsert(df)

assert upd.rows_updated == 1
assert upd.rows_inserted == 1
```

PyIceberg will automatically detect which rows need to be updated, inserted or can simply be ignored.

## Inspecting tables

To explore the table metadata, tables can be inspected.
"Time Travel"
    To inspect a tables's metadata with the time travel feature, call the inspect table method with the `snapshot_id` argument.
    Time travel is supported on all metadata tables except `snapshots` and `refs`.
    ```python
    table.inspect.entries(snapshot_id=805611270568163028)
    ```

### Snapshots

Inspect the snapshots of the table:

```python
table.inspect.snapshots()
```

```python
pyarrow.Table
committed_at: timestamp[ms] not null
snapshot_id: int64 not null
parent_id: int64
operation: string
manifest_list: string not null
summary: map<string, string>
  child 0, entries: struct<key: string not null, value: string> not null
      child 0, key: string not null
      child 1, value: string
----
committed_at: [[2024-03-15 15:01:25.682,2024-03-15 15:01:25.730,2024-03-15 15:01:25.772]]
snapshot_id: [[805611270568163028,3679426539959220963,5588071473139865870]]
parent_id: [[null,805611270568163028,3679426539959220963]]
operation: [["append","overwrite","append"]]
manifest_list: [["s3://warehouse/default/table_metadata_snapshots/metadata/snap-805611270568163028-0-43637daf-ea4b-4ceb-b096-a60c25481eb5.avro","s3://warehouse/default/table_metadata_snapshots/metadata/snap-3679426539959220963-0-8be81019-adf1-4bb6-a127-e15217bd50b3.avro","s3://warehouse/default/table_metadata_snapshots/metadata/snap-5588071473139865870-0-1382dd7e-5fbc-4c51-9776-a832d7d0984e.avro"]]
summary: [[keys:["added-files-size","added-data-files","added-records","total-data-files","total-delete-files","total-records","total-files-size","total-position-deletes","total-equality-deletes"]values:["5459","1","3","1","0","3","5459","0","0"],keys:["added-files-size","added-data-files","added-records","total-data-files","total-records",...,"total-equality-deletes","total-files-size","deleted-data-files","deleted-records","removed-files-size"]values:["5459","1","3","1","3",...,"0","5459","1","3","5459"],keys:["added-files-size","added-data-files","added-records","total-data-files","total-delete-files","total-records","total-files-size","total-position-deletes","total-equality-deletes"]values:["5459","1","3","2","0","6","10918","0","0"]]]
```

### Partitions

Inspect the partitions of the table:

```python
table.inspect.partitions()
```

### Manifests

To show a table's current file manifests:

```python
table.inspect.manifests()
```

### Metadata Log Entries

To show table metadata log entries:

```python
table.inspect.metadata_log_entries()
```

```python
pyarrow.Table
timestamp: timestamp[ms] not null
file: string not null
latest_snapshot_id: int64
latest_schema_id: int32
latest_sequence_number: int64
----
timestamp: [[2024-04-28 17:03:00.214,2024-04-28 17:03:00.352,2024-04-28 17:03:00.445,2024-04-28 17:03:00.498]]
file: [["s3://warehouse/default/table_metadata_log_entries/metadata/00000-0b3b643b-0f3a-4787-83ad-601ba57b7319.metadata.json","s3://warehouse/default/table_metadata_log_entries/metadata/00001-f74e4b2c-0f89-4f55-822d-23d099fd7d54.metadata.json","s3://warehouse/default/table_metadata_log_entries/metadata/00002-97e31507-e4d9-4438-aff1-3c0c5304d271.metadata.json","s3://warehouse/default/table_metadata_log_entries/metadata/00003-6c8b7033-6ad8-4fe4-b64d-d70381aeaddc.metadata.json"]]
latest_snapshot_id: [[null,3958871664825505738,1289234307021405706,7640277914614648349]]
latest_schema_id: [[null,0,0,0]]
latest_sequence_number: [[null,0,0,0]]
```

### History

To show a table's history:

```python
table.inspect.history()
```

```python
pyarrow.Table
made_current_at: timestamp[ms] not null
snapshot_id: int64 not null
parent_id: int64
is_current_ancestor: bool not null
----
made_current_at: [[2024-06-18 16:17:48.768,2024-06-18 16:17:49.240,2024-06-18 16:17:49.343,2024-06-18 16:17:49.511]]
snapshot_id: [[4358109269873137077,3380769165026943338,4358109269873137077,3089420140651211776]]
parent_id: [[null,4358109269873137077,null,4358109269873137077]]
is_current_ancestor: [[true,false,true,true]]
```

### Files

Inspect the data files in the current snapshot of the table:

```python
table.inspect.files()
```

To show only data files or delete files in the current snapshot, use `table.inspect.data_files()` and `table.inspect.delete_files()` respectively.

## Add Files

Expert Iceberg users may choose to commit existing parquet files to the Iceberg table as data files, without rewriting them.

```python
# Given that these parquet files have schema consistent with the Iceberg table

file_paths = [
    "s3a://warehouse/default/existing-1.parquet",
    "s3a://warehouse/default/existing-2.parquet",
]

# They can be added to the table without rewriting them

tbl.add_files(file_paths=file_paths)

# A new snapshot is committed to the table with manifests pointing to the existing parquet files
```

**Name Mapping**:  

- Because `add_files` uses existing files without writing new parquet files that are aware of the Iceberg's schema, it requires the Iceberg's table to have a [Name Mapping](https://iceberg.apache.org/spec/?h=name+mapping#name-mapping-serialization) (The Name mapping maps the field names within the parquet files to the Iceberg field IDs). Hence, `add_files` requires that there are no field IDs in the parquet file's metadata, and creates a new Name Mapping based on the table's current schema if the table doesn't already have one.

**Partitions**:

- `add_files` only requires the client to read the existing parquet files' metadata footer to infer the partition value of each file. This implementation also supports adding files to Iceberg tables with partition transforms like `MonthTransform`, and `TruncateTransform` which preserve the order of the values after the transformation (Any Transform that has the `preserves_order` property set to True is supported). Please note that if the column statistics of the `PartitionField`'s source column are not present in the parquet metadata, the partition value is inferred as `None`.

**Maintenance Operations**:

- Because `add_files` commits the existing parquet files to the Iceberg Table as any other data file, destructive maintenance operations like expiring snapshots will remove them.

## Schema evolution

PyIceberg supports full schema evolution through the Python API. It takes care of setting the field-IDs and makes sure that only non-breaking changes are done (can be overridden).  
In the examples below, the `.update_schema()` is called from the table itself.

```python
with table.update_schema() as update:
    update.add_column("some_field", IntegerType(), "doc")
```

You can also initiate a transaction if you want to make more changes than just evolving the schema:

```python
with table.transaction() as transaction:
    with transaction.update_schema() as update_schema:
        update.add_column("some_other_field", IntegerType(), "doc")
    # ... Update properties etc
```

### Union by Name

Using `.union_by_name()` you can merge another schema into an existing schema without having to worry about field-IDs:

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType, LongType

catalog = load_catalog()

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
)

table = catalog.create_table("default.locations", schema)

new_schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
    NestedField(10, "population", LongType(), required=False),
)

with table.update_schema() as update:
    update.union_by_name(new_schema)
```

Now the table has the union of the two schemas `print(table.schema())`:

```python
table {
  1: city: optional string
  2: lat: optional double
  3: long: optional double
  4: population: optional long
}
```

### Add column

Using `add_column` you can add a column, without having to worry about the field-id:

```python
with table.update_schema() as update:
    update.add_column("retries", IntegerType(), "Number of retries to place the bid")
    # In a struct
    update.add_column("details", StructType())

with table.update_schema() as update:
    update.add_column(("details", "confirmed_by"), StringType(), "Name of the exchange")
```

A complex type must exist before columns can be added to it. Fields in complex types are added in a tuple.

### Rename column

Renaming a field in an Iceberg table is simple:

```python
with table.update_schema() as update:
    update.rename_column("retries", "num_retries")
    # This will rename `confirmed_by` to `processed_by` in the `details` struct
    update.rename_column(("details", "confirmed_by"), "processed_by")
```

### Move column

Move order of fields:

```python
with table.update_schema() as update:
    update.move_first("symbol")
    # This will move `bid` after `ask`
    update.move_after("bid", "ask")
    # This will move `confirmed_by` before `exchange` in the `details` struct
    update.move_before(("details", "confirmed_by"), ("details", "exchange"))
```

### Update column

Update a fields' type, description or required.

```python
with table.update_schema() as update:
    # Promote a float to a double
    update.update_column("bid", field_type=DoubleType())
    # Make a field optional
    update.update_column("symbol", required=False)
    # Update the documentation
    update.update_column("symbol", doc="Name of the share on the exchange")
```

Be careful, some operations are not compatible, but can still be done at your own risk by setting `allow_incompatible_changes`:

```python
with table.update_schema(allow_incompatible_changes=True) as update:
    # Incompatible change, cannot require an optional field
    update.update_column("symbol", required=True)
```

### Delete column

Delete a field, careful this is a incompatible change (readers/writers might expect this field):

```python
with table.update_schema(allow_incompatible_changes=True) as update:
    update.delete_column("some_field")
    # In a struct
    update.delete_column(("details", "confirmed_by"))
```

## Partition evolution

PyIceberg supports partition evolution. See the [partition evolution](https://iceberg.apache.org/spec/#partition-evolution)
for more details.

The API to use when evolving partitions is the `update_spec` API on the table.

```python
with table.update_spec() as update:
    update.add_field("id", BucketTransform(16), "bucketed_id")
    update.add_field("event_ts", DayTransform(), "day_ts")
```

Updating the partition spec can also be done as part of a transaction with other operations.

```python
with table.transaction() as transaction:
    with transaction.update_spec() as update_spec:
        update_spec.add_field("id", BucketTransform(16), "bucketed_id")
        update_spec.add_field("event_ts", DayTransform(), "day_ts")
    # ... Update properties etc
```

### Add fields

New partition fields can be added via the `add_field` API which takes in the field name to partition on,
the partition transform, and an optional partition name. If the partition name is not specified,
one will be created.

```python
with table.update_spec() as update:
    update.add_field("id", BucketTransform(16), "bucketed_id")
    update.add_field("event_ts", DayTransform(), "day_ts")
    # identity is a shortcut API for adding an IdentityTransform
    update.identity("some_field")
```

### Remove fields

Partition fields can also be removed via the `remove_field` API if it no longer makes sense to partition on those fields.

```python
with table.update_spec() as update:
    # Remove the partition field with the name
    update.remove_field("some_partition_name")
```

### Rename fields

Partition fields can also be renamed via the `rename_field` API.

```python
with table.update_spec() as update:
    # Rename the partition field with the name bucketed_id to sharded_id
    update.rename_field("bucketed_id", "sharded_id")
```

## Table properties

Set and remove properties through the `Transaction` API:

```python
with table.transaction() as transaction:
    transaction.set_properties(abc="def")

assert table.properties == {"abc": "def"}

with table.transaction() as transaction:
    transaction.remove_properties("abc")

assert table.properties == {}
```

Or, without context manager:

```python
table = table.transaction().set_properties(abc="def").commit_transaction()

assert table.properties == {"abc": "def"}

table = table.transaction().remove_properties("abc").commit_transaction()

assert table.properties == {}
```

## Snapshot properties

Optionally, Snapshot properties can be set while writing to a table using `append` or `overwrite` API:

```python
tbl.append(df, snapshot_properties={"abc": "def"})
# or
tbl.overwrite(df, snapshot_properties={"abc": "def"})

assert tbl.metadata.snapshots[-1].summary["abc"] == "def"
```

## Snapshot Management

Manage snapshots with operations through the `Table` API:

```python
# To run a specific operation
table.manage_snapshots().create_tag(snapshot_id, "tag123").commit()
# To run multiple operations
table.manage_snapshots()
    .create_tag(snapshot_id1, "tag123")
    .create_tag(snapshot_id2, "tag456")
    .commit()
# Operations are applied on commit.
```

You can also use context managers to make more changes:

```python
with table.manage_snapshots() as ms:
    ms.create_branch(snapshot_id1, "Branch_A").create_tag(snapshot_id2, "tag789")
```

## Views

### Check if a view exists

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")
catalog.view_exists("default.bar")
```

## Table Statistics Management

Manage table statistics with operations through the `Table` API:

```python
# To run a specific operation
table.update_statistics().set_statistics(statistics_file=statistics_file).commit()
# To run multiple operations
table.update_statistics()
  .set_statistics(statistics_file1)
  .remove_statistics(snapshot_id2)
  .commit()
# Operations are applied on commit.
```

You can also use context managers to make more changes:

```python
with table.update_statistics() as update:
    update.set_statistics(statistics_file)
    update.remove_statistics(snapshot_id2)
```

## Query the data

To query a table, a table scan is needed.  
A table scan accepts a filter, columns, optionally a limit and a snapshot ID:

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual

catalog = load_catalog("default")
table = catalog.load_table("nyc.taxis")

scan = table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
    limit=100,
)

# Or filter using a string predicate
scan = table.scan(
    row_filter="trip_distance > 10.0",
)

[task.file.file_path for task in scan.plan_files()]
```

The low level API `plan_files` methods returns a set of tasks that provide the files that might contain matching rows:

```json
[
  "s3://warehouse/wh/nyc/taxis/data/00003-4-42464649-92dd-41ad-b83b-dea1a2fe4b58-00001.parquet"
]
```

In this case it is up to the engine itself to filter the file itself.
Below, `to_arrow()` and `to_duckdb()` that already do this for you.

### Apache Arrow

Using PyIceberg it is filter out data from a huge table and pull it into a PyArrow table:

```python
table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_arrow()
```

This will return a PyArrow table:

```python
pyarrow.Table
VendorID: int64
tpep_pickup_datetime: timestamp[us, tz=+00:00]
tpep_dropoff_datetime: timestamp[us, tz=+00:00]
----
VendorID: [[2,1,2,1,1,...,2,2,2,2,2],[2,1,1,1,2,...,1,1,2,1,2],...,[2,2,2,2,2,...,2,6,6,2,2],[2,2,2,2,2,...,2,2,2,2,2]]
tpep_pickup_datetime: [[2021-04-01 00:28:05.000000,...,2021-04-30 23:44:25.000000]]
tpep_dropoff_datetime: [[2021-04-01 00:47:59.000000,...,2021-05-01 00:14:47.000000]]
```

This will only pull in the files that that might contain matching rows.

One can also return a PyArrow RecordBatchReader, if reading one record batch at a time is preferred:

```python
table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_arrow_batch_reader()
```

### Pandas

PyIceberg makes it easy to filter out data from a huge table and pull it into a Pandas dataframe locally. This will only fetch the relevant Parquet files for the query and apply the filter. This will reduce IO and therefore improve performance and reduce cost.

```python
table.scan(
    row_filter="trip_distance >= 10.0",
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_pandas()
```

This will return a Pandas dataframe:

```python
        VendorID      tpep_pickup_datetime     tpep_dropoff_datetime
0              2 2021-04-01 00:28:05+00:00 2021-04-01 00:47:59+00:00
1              1 2021-04-01 00:39:01+00:00 2021-04-01 00:57:39+00:00
...          ...                       ...                       ...
116979         2 2021-04-30 23:33:00+00:00 2021-04-30 23:59:00+00:00
116980         2 2021-04-30 23:44:25+00:00 2021-05-01 00:14:47+00:00

[116981 rows x 3 columns]
```

Use Pandas 2 or later, because it stores the data in an Apache Arrow backend which avoids copies of data.

### DuckDB

A table scan can also be converted into a in-memory DuckDB table:

```python
con = table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_duckdb(table_name="distant_taxi_trips")
```

Using the cursor that we can run queries on the DuckDB table:

```python
print(
    con.execute(
        "SELECT tpep_dropoff_datetime - tpep_pickup_datetime AS duration FROM distant_taxi_trips LIMIT 4"
    ).fetchall()
)
[
    (datetime.timedelta(seconds=1194),),
    (datetime.timedelta(seconds=1118),),
    (datetime.timedelta(seconds=1697),),
    (datetime.timedelta(seconds=1581),),
]
```

### Polars

PyIceberg data can be analyzed and accessed through Polars using either DataFrame or LazyFrame.
If your code utilizes the Apache Iceberg data scanning and retrieval API and then analyzes the resulting DataFrame in Polars, use the `table.scan().to_polars()` API.
If the intent is to utilize Polars' high-performance filtering and retrieval functionalities, use LazyFrame exported from the Iceberg table with the `table.to_polars()` API.

```python
# Get LazyFrame
iceberg_table.to_polars()

# Get Data Frame
iceberg_table.scan().to_polars()
```

#### Working with Polars DataFrame

PyIceberg makes it easy to filter out data from a huge table and pull it into a Polars dataframe locally. This will only fetch the relevant Parquet files for the query and apply the filter. This will reduce IO and therefore improve performance and reduce cost.

```python
schema = Schema(
    NestedField(field_id=1, name='ticket_id', field_type=LongType(), required=True),
    NestedField(field_id=2, name='customer_id', field_type=LongType(), required=True),
    NestedField(field_id=3, name='issue', field_type=StringType(), required=False),
    NestedField(field_id=4, name='created_at', field_type=TimestampType(), required=True),
  required=True
)

iceberg_table = catalog.create_table(
    identifier='default.product_support_issues',
    schema=schema
)

pa_table_data = pa.Table.from_pylist(
    [
        {'ticket_id': 1, 'customer_id': 546, 'issue': 'User Login issue', 'created_at': 1650020000000000},
        {'ticket_id': 2, 'customer_id': 547, 'issue': 'Payment not going through', 'created_at': 1650028640000000},
        ...
        {'ticket_id': 20, 'customer_id': 565, 'issue': 'Unable to download invoice', 'created_at': 1650184160000000},
        {'ticket_id': 21, 'customer_id': 566, 'issue': 'Incorrect billing amount', 'created_at': 1650192800000000},
    ], schema=iceberg_table.schema().as_arrow()
)

iceberg_table.append(
    df=pa_table_data
)

table.scan(
    row_filter="ticket_id > 10",
).to_polars()
```

This will return a Polars DataFrame:

```table
shape: (11, 4)
┌───────────┬─────────────┬────────────────────────────┬─────────────────────┐
│ ticket_id ┆ customer_id ┆ issue                      ┆ created_at          │
│ ---       ┆ ---         ┆ ---                        ┆ ---                 │
│ i64       ┆ i64         ┆ str                        ┆ datetime[μs]        │
╞═══════════╪═════════════╪════════════════════════════╪═════════════════════╡
│ 11        ┆ 556         ┆ Website not loading        ┆ 2022-04-16 10:53:20 │
│ 12        ┆ 557         ┆ Incorrect order received   ┆ 2022-04-16 13:17:20 │
│ …         ┆ …           ┆ …                          ┆ …                   │
│ 20        ┆ 565         ┆ Unable to download invoice ┆ 2022-04-17 08:29:20 │
│ 21        ┆ 566         ┆ Incorrect billing amount   ┆ 2022-04-17 10:53:20 │
└───────────┴─────────────┴────────────────────────────┴─────────────────────┘
```

#### Working with Polars LazyFrame

PyIceberg supports creation of a Polars LazyFrame based on an Iceberg Table.

using the above code example:

```python
lf = iceberg_table.to_polars().filter(pl.col("ticket_id") > 10)
print(lf.collect())
```

This above code snippet returns a Polars LazyFrame and defines a filter to be executed by Polars:

```table
shape: (11, 4)
┌───────────┬─────────────┬────────────────────────────┬─────────────────────┐
│ ticket_id ┆ customer_id ┆ issue                      ┆ created_at          │
│ ---       ┆ ---         ┆ ---                        ┆ ---                 │
│ i64       ┆ i64         ┆ str                        ┆ datetime[μs]        │
╞═══════════╪═════════════╪════════════════════════════╪═════════════════════╡
│ 11        ┆ 556         ┆ Website not loading        ┆ 2022-04-16 10:53:20 │
│ 12        ┆ 557         ┆ Incorrect order received   ┆ 2022-04-16 13:17:20 │
│ …         ┆ …           ┆ …                          ┆ …                   │
│ 20        ┆ 565         ┆ Unable to download invoice ┆ 2022-04-17 08:29:20 │
│ 21        ┆ 566         ┆ Incorrect billing amount   ┆ 2022-04-17 10:53:20 │
└───────────┴─────────────┴────────────────────────────┴─────────────────────┘
```
