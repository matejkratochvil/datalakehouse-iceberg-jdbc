# Trino Metadata tables

The connector exposes several metadata tables for each Iceberg table. These metadata tables contain information about the internal structure of the Iceberg table.
You can query each metadata table by appending the metadata table name to the table name:

```sql
SELECT * FROM "test_table$properties";
```

## `$properties` table

The `$properties` table provides access to general information about Iceberg table configuration and any additional metadata key/value pairs that the table is tagged with.

You can retrieve the properties of the current snapshot of the Iceberg table `test_table` by using the following query:

```sql
SELECT * FROM "test_table$properties";
```

| key                 | value   |
|---------------------|---------|
| write.format.default| PARQUET |

## `$history` table

The `$history` table provides a log of the metadata changes performed on the Iceberg table.

You can retrieve the changelog of the Iceberg table `test_table` by using the following query:

```sql
SELECT * FROM "test_table$history";
```

| made_current_at                  | snapshot_id          | parent_id            | is_current_ancestor|
|----------------------------------|----------------------|----------------------|--------------------|
|2022-01-10 08:11:20 Europe/Vienna | 8667764846443717831  |  `<null>`            |  true              |
|2022-01-10 08:11:34 Europe/Vienna | 7860805980949777961  | 8667764846443717831  |  true              |

The output of the query has the following columns:

| Name                  | Type                          | Description                                                          |
| --------------------- | ----------------------------- | -------------------------------------------------------------------- |
| `made_current_at`     | `TIMESTAMP(3) WITH TIME ZONE` | The time when the snapshot became active.                            |
| `snapshot_id`         | `BIGINT`                      | The identifier of the snapshot.                                      |
| `parent_id`           | `BIGINT`                      | The identifier of the parent snapshot.                               |
| `is_current_ancestor` | `BOOLEAN`                     | Whether or not this snapshot is an ancestor of the current snapshot. |

## `$metadata_log_entries` table

The `$metadata_log_entries` table provides a view of metadata log entries of the Iceberg table.

You can retrieve the information about the metadata log entries of the Iceberg table `test_table` by using the following query:

```sql
SELECT * FROM "test_table$metadata_log_entries";
```

|timestamp                             |                                                              file                                                          | latest_snapshot_id  | latest_schema_id | latest_sequence_number |  
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------|---------------------|------------------|------------------------|  
|2024-01-16 15:55:31.172 Europe/Vienna | hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/00000-39174715-be2a-48fa-9949-35413b8b736e.metadata.json | 1221802298419195590 |                0 |                      1 |
|2024-01-16 17:19:56.118 Europe/Vienna | hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/00001-e40178c9-271f-4a96-ad29-eed5e7aef9b0.metadata.json | 7124386610209126943 |                0 |                      2 |

The output of the query has the following columns:

| Name                     | Type                          | Description                                                          |
| ------------------------ | ----------------------------- | -------------------------------------------------------------------- |
| `timestamp`              | `TIMESTAMP(3) WITH TIME ZONE` | The time when the metadata was created.                              |
| `file`                   | `VARCHAR`                     | The location of the metadata file.                                   |
| `latest_snapshot_id`     | `BIGINT`                      | The identifier of the latest snapshot when the metadata was updated. |
| `latest_schema_id`       | `INTEGER`                     | The identifier of the latest schema when the metadata was updated.   |
| `latest_sequence_number` | `BIGINT`                      | The data sequence number of the metadata file.                       |

## `$snapshots` table

The `$snapshots` table provides a detailed view of snapshots of the Iceberg table. A snapshot consists of one or more file manifests,
and the complete table contents are represented by the union of all the data files in those manifests.

You can retrieve the information about the snapshots of the Iceberg table `test_table` by using the following query:

```sql
SELECT * FROM "test_table$snapshots";
```

| committed_at                     | snapshot_id          | parent_id            | operation          | manifest_list                                                                                                                            | summary                                                                                                                                                                                                                     |
|----------------------------------|----------------------|----------------------|--------------------|------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|2022-01-10 08:11:20 Europe/Vienna | 8667764846443717831  |  `<null>`            |  append            |   hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/snap-8667764846443717831-1-100cf97e-6d56-446e-8961-afdaded63bc4.avro | {changed-partition-count=0, total-equality-deletes=0, total-position-deletes=0, total-delete-files=0, total-files-size=0, total-records=0, total-data-files=0}                                                              |
|2022-01-10 08:11:34 Europe/Vienna | 7860805980949777961  | 8667764846443717831  |  append            |   hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/snap-7860805980949777961-1-faa19903-1455-4bb8-855a-61a1bbafbaa7.avro | {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=1, total-position-deletes=0, added-files-size=442, total-delete-files=0, total-files-size=442, total-records=1, total-data-files=1} |

The output of the query has the following columns:

| Name            | Type                          | Description                                                                                                                                                                                                           |
| --------------- | ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `committed_at`  | `TIMESTAMP(3) WITH TIME ZONE` | The time when the snapshot became active.                                                                                                                                                                             |
| `snapshot_id`   | `BIGINT`                      | The identifier for the snapshot.                                                                                                                                                                                      |
| `parent_id`     | `BIGINT`                      | The identifier for the parent snapshot.                                                                                                                                                                               |
| `operation`     | `VARCHAR`                     | The type of operation performed on the Iceberg table. The supported operation types in Iceberg are:  - `append`, - `replace`, - `overwrite`, - `delete` when data is deleted from the table and no new data is added. |
| `manifest_list` | `VARCHAR`                     | The list of Avro manifest files containing the detailed information about the snapshot changes.                                                                                                                       |
| `summary`       | `map(VARCHAR, VARCHAR)`       | A summary of the changes made from the previous snapshot to the current snapshot.                                                                                                                                     |

## `$manifests` and `$all_manifests` tables

The `$manifests` and `$all_manifests` tables provide a detailed overview of the manifests corresponding to the snapshots performed in the log of the Iceberg table. The `$manifests` table contains data for the current snapshot. The `$all_manifests` table contains data for all snapshots.

You can retrieve the information about the manifests of the Iceberg table `test_table` by using the following query (subset) 

```sql
SELECT * FROM test_table$manifests;
```

| path                                                                                                           | length | partition_spec_id | added_snapshot_id  | added_data_files_count | added_rows_count | partition_summaries                                                                                                                                                  |
| -------------------------------------------------------------------------------------------------------------- | ------ | ----------------- | ------------------ | ---------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| hdfs://hadoop-master:9000/user/hive/warehouse/test_table/metadata/faa19903-1455-4bb8-855a-61a1bbafbaa7-m0.avro | 6277   | 0                 | 7860805980949777961| 1                      | 100              | {{contains_null=false, contains_nan=false, lower_bound=1, upper_bound=1}, {contains_null=false, contains_nan=false, lower_bound=2021-01-12, upper_bound=2021-01-12}} |

The output of the query has the following columns:

| Name                        | Type                                                                                                | Description                                                                             |
| --------------------------- | --------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `path`                      | `VARCHAR`                                                                                           | The manifest file location.                                                             |
| `length`                    | `BIGINT`                                                                                            | The manifest file length.                                                               |
| `partition_spec_id`         | `INTEGER`                                                                                           | The identifier for the partition specification used to write the manifest file.         |
| `added_snapshot_id`         | `BIGINT`                                                                                            | The identifier of the snapshot during which this manifest entry has been added.         |
| `added_data_files_count`    | `INTEGER`                                                                                           | The number of data files with status `ADDED` in the manifest file.                      |
| `added_rows_count`          | `BIGINT`                                                                                            | The total number of rows in all data files with status `ADDED` in the manifest file.    |
| `existing_data_files_count` | `INTEGER`                                                                                           | The number of data files with status `EXISTING` in the manifest file.                   |
| `existing_rows_count`       | `BIGINT`                                                                                            | The total number of rows in all data files with status `EXISTING` in the manifest file. |
| `deleted_data_files_count`  | `INTEGER`                                                                                           | The number of data files with status `DELETED` in the manifest file.                    |
| `deleted_rows_count`        | `BIGINT`                                                                                            | The total number of rows in all data files with status `DELETED` in the manifest file.  |
| `partition_summaries`       | `ARRAY(row(contains_null BOOLEAN, contains_nan BOOLEAN, lower_bound VARCHAR, upper_bound VARCHAR))` | Partition range metadata.                                                               |

## `$partitions` table

The `$partitions` table provides a detailed overview of the partitions of the Iceberg table.

You can retrieve the information about the partitions of the Iceberg table `test_table` by using the following query:

```sql
SELECT * FROM "test_table$partitions";
```

|partition              | record_count  | file_count    | total_size    |  data                                                |
|-----------------------|---------------|---------------|---------------|------------------------------------------------------|
|{c1=1, c2=2021-01-12}  |  2            | 2             |  884          | {c3={min=1.0, max=2.0, null_count=0, nan_count=NULL}}|
|{c1=1, c2=2021-01-13}  |  1            | 1             |  442          | {c3={min=1.0, max=1.0, null_count=0, nan_count=NULL}}|

The output of the query has the following columns:

| Name           | Type                                                                    | Description                                                                                   |
| -------------- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `partition`    | `ROW(...)`                                                              | A row that contains the mapping of the partition column names to the partition column values. |
| `record_count` | `BIGINT`                                                                | The number of records in the partition.                                                       |
| `file_count`   | `BIGINT`                                                                | The number of files mapped in the partition.                                                  |
| `total_size`   | `BIGINT`                                                                | The size of all the files in the partition.                                                   |
| `data`         | `ROW(... ROW (min ..., max ... , null_count BIGINT, nan_count BIGINT))` | Partition range metadata.                                                                     |

## `$files` table

The `$files` table provides a detailed overview of the data files in current snapshot of the Iceberg table.  
To retrieve the information about the data files of the Iceberg table `test_table`, use the following query (subset):

```sql
SELECT * FROM "test_table$files";
```

| content | file_path                                                                                                                     | record_count | file_format | file_size_in_bytes | column_sizes        | value_counts    |
| ------- | ----------------------------------------------------------------------------------------------------------------------------- | ------------ | ----------- | ------------------ | ------------------- | --------------- |
| 0       | hdfs://hadoop-master:9000/user/hive/warehouse/test_table/data/c1=3/c2=2021-01-14/af9872b2-40f3-428f-9c87-186d2750d84e.parquet | 1            | PARQUET     | 442                | {1=40, 2=40, 3=44}  | {1=1, 2=1, 3=1} |

The output of the query has the following columns:

| Name                 | Type                   | Description                                                                                                                                  |
| -------------------- | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `content`            | `INTEGER`              | Type of content stored in the file. The supported content types in Iceberg are:  - `DATA(0)` - `POSITION_DELETES(1)` - `EQUALITY_DELETES(2)` |
| `file_path`          | `VARCHAR`              | The data file location.                                                                                                                      |
| `file_format`        | `VARCHAR`              | The format of the data file.                                                                                                                 |
| `spec_id`            | `INTEGER`              | Spec ID used to track the file containing a row.                                                                                             |
| `partition`          | `ROW(...)`             | A row that contains the mapping of the partition column names to the partition column values.                                                |
| `record_count`       | `BIGINT`               | The number of entries contained in the data file.                                                                                            |
| `file_size_in_bytes` | `BIGINT`               | The data file size                                                                                                                           |
| `column_sizes`       | `map(INTEGER, BIGINT)` | Mapping between the Iceberg column ID and its corresponding size in the file.                                                                |
| `value_counts`       | `map(INTEGER, BIGINT)` | Mapping between the Iceberg column ID and its corresponding count of entries in the file.                                                    |
| `null_value_counts`  | `map(INTEGER, BIGINT)` | Mapping between the Iceberg column ID and its corresponding count of `NULL` values in the file.                                              |
| `nan_value_counts`   | `map(INTEGER, BIGINT)` | Mapping between the Iceberg column ID and its corresponding count of non- numerical values in the file.                                      |
| `lower_bounds`       | `map(INTEGER, BIGINT)` | Mapping between the Iceberg column ID and its corresponding lower bound in the file.                                                         |
| `upper_bounds`       | `map(INTEGER, BIGINT)` | Mapping between the Iceberg column ID and its corresponding upper bound in the file.                                                         |
| `key_metadata`       | `VARBINARY`            | Metadata about the encryption key used to encrypt this file, if applicable.                                                                  |
| `split_offsets`      | `array(BIGINT)`        | List of recommended split locations.                                                                                                         |
| `equality_ids`       | `array(INTEGER)`       | The set of field IDs used for equality comparison in equality delete files.                                                                  |
| `sort_order_id`      | `INTEGER`              | ID representing sort order for this file.                                                                                                    |
| `readable_metrics`   | `JSON`                 | File metrics in human-readable form.                                                                                                         |

## `$entries` and `$all_entries` tables

The `$entries` and `$all_entries` tables provide the table’s manifest entries for both data and delete files. The `$entries` table contains data for the current snapshot. The `$all_entries` table contains data for all snapshots.

To retrieve the information about the entries of the Iceberg table `test_table`, use the following query:

```sql
SELECT * FROM "test_table$entries";
```

Abbreviated sample output:

| status | snapshot_id    | sequence_number | file_sequence_number | data_file                               | readable_metrics                                 |
|--------|----------------|-----------------|----------------------|-----------------------------------------|--------------------------------------------------|
| 2      | 57897183625154 | 0               | 0                    | {"content":0,...,"sort_order_id":0}     | {"c1":{"column_size":103,...,"upper_bound":3}}   |

The metadata tables include the following columns:

| Name                   | Type      | Description                                                                                                                   |
| ---------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `status`               | `INTEGER` | Numeric status to track additions and deletions. Deletes are informational only:  - `EXISTING(0)` - `ADDED(1)` - `DELETED(2)` |
| `snapshot_id`          | `BIGINT`  | The snapshot ID of the reference.                                                                                             |
| `sequence_number`      | `BIGINT`  | Data sequence number of the file. Inherited when null and status is 1.                                                        |
| `file_sequence_number` | `BIGINT`  | File sequence number indicating when the file was added. Inherited when null and status is 1.                                 |
| `data_file`            | `ROW`     | Metadata including file path, file format, file size and other information.                                                   |
| `readable_metrics`     | `JSON`    | JSON-formatted file metrics such as column size, value count, and others.                                                     |

## `$refs` table

The `$refs` table provides information about Iceberg references including branches and tags.

You can retrieve the references of the Iceberg table `test_table` by using the following query:

```sql
SELECT * FROM "test_table$refs";
```

| name           | type   | snapshot_id   | max_reference_age_in_ms | min_snapshots_to_keep | max_snapshot_age_in_ms |
|----------------|--------|---------------|-------------------------|-----------------------|------------------------|
| example_tag    | TAG    | 10000000000   | 10000                   | null                  | null                   |
| example_branch | BRANCH | 20000000000   | 20000                   | 2                     | 30000                  |

The output of the query has the following columns:

| Name                      | Type      | Description                                                                                   |
| ------------------------- | --------- | --------------------------------------------------------------------------------------------- |
| `name`                    | `VARCHAR` | Name of the reference.                                                                        |
| `type`                    | `VARCHAR` | Type of the reference, either `BRANCH` or `TAG`.                                              |
| `snapshot_id`             | `BIGINT`  | The snapshot ID of the reference.                                                             |
| `max_reference_age_in_ms` | `BIGINT`  | The maximum age of the reference before it could be expired.                                  |
| `min_snapshots_to_keep`   | `INTEGER` | For branch only, the minimum number of snapshots to keep in a branch.                         |
| `max_snapshot_age_in_ms`  | `BIGINT`  | For branch only, the max snapshot age allowed. Older snapshots in the branch will be expired. |

#### Metadata columns

In addition to the defined columns, the Iceberg connector automatically exposes path metadata as a hidden column in each table:

- `$partition`: Partition path for this row
- `$path`: Full file system path name of the file for this row
- `$file_modified_time`: Timestamp of the last modification of the file for this row

You can use these columns in your SQL statements like any other column. This can be selected directly, or used in conditional statements.
For example, you can inspect the file path for each record:

```sql
SELECT *, "$partition", "$path", "$file_modified_time"
FROM example.web.page_views;
```

Retrieve all records that belong to a specific file using `"$path"` filter:

```sql
SELECT *
FROM example.web.page_views
WHERE "$path" = '/usr/iceberg/table/web.page_views/data/file_01.parquet';
```

Retrieve all records that belong to a specific file using `"$file_modified_time"` filter:

```sql
SELECT *
FROM example.web.page_views
WHERE "$file_modified_time" = CAST('2022-07-01 01:02:03.456 UTC' AS TIMESTAMP WITH TIME ZONE);
```

#### System tables

The connector exposes metadata tables in the system schema.

## `iceberg_tables` tableheading

The `iceberg_tables` table allows listing only Iceberg tables from a given catalog. The `SHOW TABLES` statement, `information_schema.tables`, and `jdbc.tables` will all return all tables that exist in the underlying metastore,
even if the table cannot be handled in any way by the iceberg connector. This can happen if other connectors like Hive or Delta Lake, use the same metastore, catalog, and schema to store its tables.

The table includes following columns:

| Name           | Type      | Description                             |
| -------------- | --------- | --------------------------------------- |
| `table_schema` | `VARCHAR` | The name of the schema the table is in. |
| `table_name`   | `VARCHAR` | The name of the table.                  |

The following query lists Iceberg tables from all schemas in the `example` catalog.

```sql
SELECT * FROM example.system.iceberg_tables;
```

| table_schema | table_name  |
|--------------|-------------|
| tpcds        | store_sales |
| tpch         | nation      |
| tpch         | region      |
| tpch         | orders      |
