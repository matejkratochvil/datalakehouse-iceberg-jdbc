# Writes via DataFrames

Branch writes via DataFrames can be performed by providing a branch identifier, `branch_yourBranch` in the operation.

```scala
// To insert into 'audit' branch
val data: DataFrame = ...
data.writeTo("prod.db.table.branch_audit").append()
```

```scala
// To overwrite 'audit' branch
val data: DataFrame = ...
data.writeTo("prod.db.table.branch_audit").overwritePartitions()
```

## Writing with DataFrames

Spark 3 introduced the new `DataFrameWriterV2` API for writing to tables using data frames. The v2 API is recommended for several reasons:

- CTAS, RTAS, and overwrite by filter are supported
- All operations consistently write columns to a table by name
- Hidden partition expressions are supported in `partitionedBy`
- Overwrite behavior is explicit, either dynamic or by a user-supplied filter
- The behavior of each operation corresponds to SQL statements
  - `df.writeTo(t).create()` is equivalent to `CREATE TABLE AS SELECT`
  - `df.writeTo(t).replace()` is equivalent to `REPLACE TABLE AS SELECT`
  - `df.writeTo(t).append()` is equivalent to `INSERT INTO`
  - `df.writeTo(t).overwritePartitions()` is equivalent to dynamic `INSERT OVERWRITE`

The v1 DataFrame `write` API is still supported, but is not recommended.

Danger

When writing with the v1 DataFrame API in Spark 3, use `saveAsTable` or `insertInto` to load tables with a catalog. Using `format("iceberg")` loads an isolated table reference that will not automatically refresh tables used by queries.

### Appending data

To append a dataframe to an Iceberg table, use `append`:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").append()
```

### Overwriting data

To overwrite partitions dynamically, use `overwritePartitions()`:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").overwritePartitions()
```

To explicitly overwrite partitions, use `overwrite` to supply a filter:

```scala
data.writeTo("prod.db.table").overwrite($"level" === "INFO")
```

### Creating tables

To run a CTAS or RTAS, use `create`, `replace`, or `createOrReplace` operations:

```scala
val data: DataFrame = ...
data.writeTo("prod.db.table").create()
```

If you have replaced the default Spark catalog (`spark_catalog`) with Iceberg's `SparkSessionCatalog`, do:

```scala
val data: DataFrame = ...
data.writeTo("db.table").using("iceberg").create()
```

Create and replace operations support table configuration methods, like `partitionedBy` and `tableProperty`:

The Iceberg table location can also be specified by the `location` table property:

### Schema Merge

While inserting or updating Iceberg is capable of resolving schema mismatch at runtime. If configured, Iceberg will perform an automatic schema evolution as follows:

- A new column is present in the source but not in the target table.
  The new column is added to the target table. Column values are set to `NULL` in all the rows already present in the table
- A column is present in the target but not in the source.
  The target column value is set to `NULL` when inserting or left unchanged when updating the row.

The target table must be configured to accept any schema change by setting the property `write.spark.accept-any-schema` to `true`.

```sql
ALTER TABLE prod.db.sample SET TBLPROPERTIES (
  'write.spark.accept-any-schema'='true'
)
```

The writer must enable the `mergeSchema` option.

```scala
data.writeTo("prod.db.sample").option("mergeSchema","true").append()
```

## Writing Distribution Modes

Iceberg's default Spark writers require that the data in each spark task is clustered by partition values. This distribution is required to minimize the number of file handles that are held open while writing. By default, starting in Iceberg 1.2.0, Iceberg also requests that Spark pre-sort data to be written to fit this distribution. The request to Spark is done through the table property `write.distribution-mode` with the value `hash`. Spark doesn't respect distribution mode in CTAS/RTAS before 3.5.0.

Let's go through writing the data against below sample table:

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (days(ts), category)
```

To write data to the sample table, data needs to be sorted by `days(ts), category` but this is taken care of automatically by the default `hash` distribution. Previously this would have required manually sorting, but this is no longer the case.

```sql
INSERT INTO prod.db.sample
SELECT id, data, category, ts FROM another_table
```

There are 3 options for `write.distribution-mode`

- `none` - This is the previous default for Iceberg.  
  This mode does not request any shuffles or sort to be performed automatically by Spark. Because no work is done automatically by Spark, the data must be _manually_ sorted by partition value. The data must be sorted either within each spark task, or globally within the entire dataset. A global sort will minimize the number of output files.  
  A sort can be avoided by using the Spark [write fanout](https://iceberg.apache.org/docs/latest/spark-configuration/#write-options) property but this will cause all file handles to remain open until each write task has completed.
- `hash` - This mode is the new default and requests that Spark uses a hash-based exchange to shuffle the incoming write data before writing.  
  Practically, this means that each row is hashed based on the row's partition value and then placed in a corresponding Spark task based upon that value. Further division and coalescing of tasks may take place because of [Spark's Adaptive Query planning](https://iceberg.apache.org/docs/latest/spark-writes/#controlling-file-sizes).
- `range` - This mode requests that Spark perform a range based exchange to shuffle the data before writing.  
  This is a two stage procedure which is more expensive than the `hash` mode. The first stage samples the data to be written based on the partition and sort columns. The second stage uses the range information to shuffle the input data into Spark tasks. Each task gets an exclusive range of the input data which clusters the data by partition and also globally sorts.  
  While this is more expensive than the hash distribution, the global ordering can be beneficial for read performance if sorted columns are used during queries. This mode is used by default if a table is created with a sort-order. Further division and coalescing of tasks may take place because of [Spark's Adaptive Query planning](https://iceberg.apache.org/docs/latest/spark-writes/#controlling-file-sizes).

## Controlling File Sizes

When writing data to Iceberg with Spark, it's important to note that Spark cannot write a file larger than a Spark task and a file cannot span an Iceberg partition boundary. This means although Iceberg will always roll over a file when it grows to [`write.target-file-size-bytes`](https://iceberg.apache.org/docs/latest/configuration/#write-properties), but unless the Spark task is large enough that will not happen. The size of the file created on disk will also be much smaller than the Spark task since the on disk data will be both compressed and in columnar format as opposed to Spark's uncompressed row representation. This means a 100 megabyte Spark task will create a file much smaller than 100 megabytes even if that task is writing to a single Iceberg partition. If the task writes to multiple partitions, the files will be even smaller than that.

To control what data ends up in each Spark task use a [`write distribution mode`](https://iceberg.apache.org/docs/latest/spark-writes/#writing-distribution-modes) or manually repartition the data.

To adjust Spark's task size it is important to become familiar with Spark's various Adaptive Query Execution (AQE) parameters. When the `write.distribution-mode` is not `none`, AQE will control the coalescing and splitting of Spark tasks during the exchange to try to create tasks of `spark.sql.adaptive.advisoryPartitionSizeInBytes` size. These settings will also affect any user performed re-partitions or sorts. It is important again to note that this is the in-memory Spark row size and not the on disk columnar-compressed size, so a larger value than the target file size will need to be specified. The ratio of in-memory size to on disk size is data dependent. Future work in Spark should allow Iceberg to automatically adjust this parameter at write time to match the `write.target-file-size-bytes`.
