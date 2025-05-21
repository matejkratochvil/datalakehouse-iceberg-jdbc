# PyIceberg Configuration

## Setting Configuration Values

There are three ways to pass in configuration:

- Using the `.pyiceberg.yaml` configuration file (Recommended)
- Through environment variables
- By passing in credentials through the CLI or the Python API

The configuration file can be stored in either the directory specified by the `PYICEBERG_HOME` environment variable, the home directory, or current working directory (in this order).

To change the path searched for the `.pyiceberg.yaml`, you can overwrite the `PYICEBERG_HOME` environment variable.

Another option is through environment variables:

```sh
export PYICEBERG_CATALOG__DEFAULT__URI=thrift://localhost:9083
export PYICEBERG_CATALOG__DEFAULT__S3__ACCESS_KEY_ID=username
export PYICEBERG_CATALOG__DEFAULT__S3__SECRET_ACCESS_KEY=password
```

The environment variable picked up by Iceberg starts with `PYICEBERG_` and then follows the yaml structure below, where a double underscore `__` represents a nested field, and the underscore `_` is converted into a dash `-`.

For example, `PYICEBERG_CATALOG__DEFAULT__S3__ACCESS_KEY_ID`, sets `s3.access-key-id` on the `default` catalog.

## Tables

Iceberg tables support table properties to configure table behavior.

### Write options

| Key                                      | Options                            | Default                    | Description                                                                                  |
|------------------------------------------|------------------------------------|----------------------------|----------------------------------------------------------------------------------------------|
| `write.parquet.compression-codec`        | `{uncompressed,zstd,gzip,snappy}`  | zstd                       | Sets the Parquet compression coddec.                                                         |
| `write.parquet.compression-level`        | Integer                            | null                       | Parquet compression level for the codec. If not set, it is up to PyIceberg                   |
| `write.parquet.row-group-limit`          | Number of rows                     | 1048576                    | The upper bound of the number of entries within a single row group                           |
| `write.parquet.page-size-bytes`          | Size in bytes                      | 1MB                        | Set a target threshold for the approximate encoded size of data pages within a column chunk  |
| `write.parquet.page-row-limit`           | Number of rows                     | 20000                      | Set a target threshold for the maximum number of rows within a column chunk                  |
| `write.parquet.dict-size-bytes`          | Size in bytes                      | 2MB                        | Set the dictionary page size limit per row group                                             |
| `write.metadata.previous-versions-max`   | Integer                            | 100                        | The max number of previous version metadata files to keep before deleting after commit.      |
| `write.metadata.delete-after-commit.enabled` | Boolean                        | False                      | Whether to automatically delete old *tracked* metadata files after each table commit. It will retain a number of the most recent metadata files, which can be set using property `write.metadata.previous-versions-max`. |
| `write.object-storage.enabled`           | Boolean                            | False                      | Enables the [`ObjectStoreLocationProvider`](#object-store-location-provider) that adds a hash component to file paths. |
| `write.object-storage.partitioned-paths` | Boolean                            | True                       | Controls whether [partition values are included in file paths](#partition-exclusion) when object storage is enabled    |
| `write.py-location-provider.impl`        | String of form `module.ClassName`  | null                       | Optional, custom `LocationProvider` implementation                                          |
| `write.data.path`                        | String pointing to location        | `{metadata.location}/data` | Sets the location under which data is written.                                               |
| `write.metadata.path`                    | String pointing to location        | `{metadata.location}/metadata` | Sets the location under which metadata is written.                                       |

### Table behavior options

| Key                                  | Options             | Default       | Description                                        |
| ------------------------------------ | ------------------- | ------------- | -------------------------------------------------- |
| `commit.manifest.target-size-bytes`  | Size in bytes       | 8388608 (8MB) | Target size when merging manifest files            |
| `commit.manifest.min-count-to-merge` | Number of manifests | 100           | Target size when merging manifest files            |
| `commit.manifest-merge.enabled`      | Boolean             | False         | Whether to automatically merge manifests on writes |

Unlike Java implementation, PyIceberg default to the [fast append](pyiceberg-api.md#write-support) and thus `commit.manifest-merge.enabled` is set to `False` by default.

## FileIO

Iceberg works with the concept of a FileIO which is a pluggable module for reading, writing, and deleting files. By default, PyIceberg will try to initialize the FileIO that's suitable for the scheme (`s3://` etc.) and will use the first one that's installed.

- **s3**, **s3a**, **s3n**: `PyArrowFileIO`, `FsspecFileIO`
- **file**: `PyArrowFileIO`
- **hdfs**: `PyArrowFileIO`
- **oss**: `PyArrowFileIO`

You can also set the FileIO explicitly:

| Key        | Example                          | Description                                                                                     |
| ---------- | -------------------------------- | ----------------------------------------------------------------------------------------------- |
| py-io-impl | pyiceberg.io.fsspec.FsspecFileIO | Sets the FileIO explicitly to an implementation, and will fail explicitly if it can't be loaded |

For the FileIO there are several configuration options available:

### S3

| Key                         | Example                    | Description                                                                                                                                                                                                                                                             |
|-----------------------------|----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| s3.endpoint                 | <https://10.0.19.25/>      | Configure an alternative endpoint of the S3 service for the FileIO to access. This could be used to use S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud.               |
| s3.access-key-id            | admin                      | Configure the static access key id used to access the FileIO.                                                                                                                                                                                                           |
| s3.secret-access-key        | password                   | Configure the static secret access key used to access the FileIO.                                                                                                                                                                                                       |
| s3.session-token            | AQoDYXdzEJr...             | Configure the static session token used to access the FileIO.                                                                                                                                                                                                           |
| s3.role-session-name        | session                    | An optional identifier for the assumed role session.                                                                                                                                                                                                                    |
| s3.role-arn                 | arn:aws:...                | AWS Role ARN. If provided instead of access_key and secret_key, temporary credentials will be fetched by assuming this role.                                                                                                                                            |
| s3.signer                   | bearer                     | Configure the signature version of the FileIO.                                                                                                                                                                                                                          |
| s3.signer.uri               | <http://my.signer:8080/s3> | Configure the remote signing uri if it differs from the catalog uri. Remote signing is only implemented for `FsspecFileIO`. The final request is sent to `<s3.signer.uri>/<s3.signer.endpoint>`.                                                                        |
| s3.signer.endpoint          | v1/main/s3-sign            | Configure the remote signing endpoint. Remote signing is only implemented for `FsspecFileIO`. The final request is sent to `<s3.signer.uri>/<s3.signer.endpoint>`. (default : v1/aws/s3/sign).                                                                          |
| s3.region                   | us-west-2                  | Configure the default region used to initialize an `S3FileSystem`. `PyArrowFileIO` attempts to automatically tries to resolve the region if this isn't set (only supported for AWS S3 Buckets).                                                                         |
| s3.resolve-region           | False                      | Only supported for `PyArrowFileIO`, when enabled, it will always try to resolve the location of the bucket (only supported for AWS S3 Buckets).                                                                                                                         |
| s3.proxy-uri                | <http://my.proxy.com:8080> | Configure the proxy server to be used by the FileIO.                                                                                                                                                                                                                    |
| s3.connect-timeout          | 60.0                       | Configure socket connection timeout, in seconds.                                                                                                                                                                                                                        |
| s3.request-timeout          | 60.0                       | Configure socket read timeouts on Windows and macOS, in seconds.                                                                                                                                                                                                        |
| s3.force-virtual-addressing | False                      | Whether to use virtual addressing of buckets. If true, then virtual addressing is always enabled. If false, then virtual addressing is only enabled if endpoint_override is empty. This can be used for non-AWS backends that only support virtual hosted-style access. |

### PyArrow

| Key                             | Example | Description                                                                                                                   |
| ------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------- |
| pyarrow.use-large-types-on-read | True    | Use large PyArrow types i.e. large_string, large_binary and large_list field types on table scans. The default value is True. |

## Location Providers

Apache Iceberg uses the concept of a `LocationProvider` to manage file paths for a table's data files. In PyIceberg, the
`LocationProvider` module is designed to be pluggable, allowing customization for specific use cases, and to additionally determine metadata file locations. The
`LocationProvider` for a table can be specified through table properties.

Both data file and metadata file locations can be customized by configuring the table properties [`write.data.path` and `write.metadata.path`](#write-options), respectively.

For more granular control, you can override the `LocationProvider`'s `new_data_location` and `new_metadata_location` methods to define custom logic for generating file paths. See [`Loading a Custom Location Provider`](#loading-a-custom-location-provider).

PyIceberg defaults to the [`SimpleLocationProvider`](#simple-location-provider) for managing file paths.

### Simple Location Provider

The `SimpleLocationProvider` provides paths prefixed by `{location}/data/`, where `location` comes from the [table metadata](https://iceberg.apache.org/spec/#table-metadata-fields). This can be overridden by setting [`write.data.path` table configuration](#write-options).

For example, a non-partitioned table might have a data file with location:

```yaml
s3://bucket/ns/table/data/0000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
```

When the table is partitioned, files under a given partition are grouped into a subdirectory, with that partition key
and value as the directory name - this is known as the *Hive-style* partition path format. For example, a table
partitioned over a string column `category` might have a data file with location:

```yaml
s3://bucket/ns/table/data/category=orders/0000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
```

### Object Store Location Provider

PyIceberg offers the `ObjectStoreLocationProvider`, and an optional [partition-exclusion](#partition-exclusion)
optimization, designed for tables stored in object storage. For additional context and motivation concerning these configurations,
see their [documentation for Iceberg's Java implementation](https://iceberg.apache.org/docs/latest/aws/#object-store-file-layout).

When several files are stored under the same prefix, cloud object stores such as S3 often [throttle requests on prefixes](https://repost.aws/knowledge-center/http-5xx-errors-s3),
resulting in slowdowns. The `ObjectStoreLocationProvider` counteracts this by injecting deterministic hashes, in the form of binary directories,
into file paths, to distribute files across a larger number of object store prefixes.

Paths are prefixed by `{location}/data/`, where `location` comes from the [table metadata](https://iceberg.apache.org/spec/#table-metadata-fields),
in a similar manner to the [`SimpleLocationProvider`](#simple-location-provider). This can be overridden by setting [`write.data.path` table configuration](#write-options).

For example, a table partitioned over a string column `category` might have a data file with location: (note the additional binary directories)

```txt
s3://bucket/ns/table/data/0101/0110/1001/10110010/category=orders/0000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
```

The `ObjectStoreLocationProvider` is enabled for a table by explicitly setting its `write.object-storage.enabled` table
property to `True`.

#### Partition Exclusion

When the `ObjectStoreLocationProvider` is used, the table property `write.object-storage.partitioned-paths`, which
defaults to `True`, can be set to `False` as an additional optimization for object stores. This omits partition keys and
values from data file paths *entirely* to further reduce key size. With it disabled, the same data file above would
instead be written to: (note the absence of `category=orders`)

```txt
s3://bucket/ns/table/data/1101/0100/1011/00111010-00000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
```

### Loading a Custom Location Provider

Similar to FileIO, a custom `LocationProvider` may be provided for a table by concretely subclassing the abstract base
class `LocationProvider`

The table property `write.py-location-provider.impl` should be set to the fully-qualified name of the custom
`LocationProvider` (i.e. `mymodule.MyLocationProvider`). Recall that a `LocationProvider` is configured per-table,
permitting different location provision for different tables. Note also that Iceberg's Java implementation uses a
different table property, `write.location-provider.impl`, for custom Java implementations.

An example, custom `LocationProvider` implementation is shown below.

```py
import uuid

class UUIDLocationProvider(LocationProvider):
    def __init__(self, table_location: str, table_properties: Properties):
        super().__init__(table_location, table_properties)

    def new_data_location(self, data_file_name: str, partition_key: Optional[PartitionKey] = None) -> str:
        # Can use any custom method to generate a file path given the partitioning information and file name
        prefix = f"{self.table_location}/{uuid.uuid4()}"
        return f"{prefix}/{partition_key.to_path()}/{data_file_name}" if partition_key else f"{prefix}/{data_file_name}"
```

## Catalogs

PyIceberg currently has native catalog type support for REST, SQL, Hive, Glue and DynamoDB.
Alternatively, you can also directly set the catalog implementation:

| Key             | Example                      | Description                                                                                      |
| --------------- | ---------------------------- | ------------------------------------------------------------------------------------------------ |
| type            | rest                         | Type of catalog, one of `rest`, `sql`, `hive`, `glue`, `dymamodb`. Default to `rest`             |
| py-catalog-impl | mypackage.mymodule.MyCatalog | Sets the catalog explicitly to an implementation, and will fail explicitly if it can't be loaded |

### REST Catalog

```yaml
catalog:
  default:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret

  default-mtls-secured-catalog:
    uri: https://rest-catalog/ws/
    ssl:
      client:
        cert: /absolute/path/to/client.crt
        key: /absolute/path/to/client.key
      cabundle: /absolute/path/to/cabundle.pem
```

| Key                 | Example                          | Description                                                                                        |
| ------------------- | -------------------------------- | -------------------------------------------------------------------------------------------------- |
| uri                 | <https://rest-catalog/ws>        | URI identifying the REST Server                                                                    |
| ugi                 | t-1234:secret                    | Hadoop UGI for Hive client.                                                                        |
| credential          | t-1234:secret                    | Credential to use for OAuth2 credential flow when initializing the catalog                         |
| token               | FEW23.DFSDF.FSDF                 | Bearer token value to use for `Authorization` header                                               |
| scope               | openid offline corpds:ds:profile | Desired scope of the requested security token (default : catalog)                                  |
| resource            | rest_catalog.iceberg.com         | URI for the target resource or service                                                             |
| audience            | rest_catalog                     | Logical name of target resource or service                                                         |
| rest.sigv4-enabled  | true                             | Sign requests to the REST Server using AWS SigV4 protocol                                          |
| rest.signing-region | us-east-1                        | The region to use when SigV4 signing a request                                                     |
| rest.signing-name   | execute-api                      | The service signing name to use when SigV4 signing a request                                       |
| oauth2-server-uri   | <https://auth-service/cc>        | Authentication URL to use for client credentials authentication (default: uri + 'v1/oauth/tokens') |
| snapshot-loading-mode | refs                           | The snapshots to return in the body of the metadata. Setting the value to `all` would return the full set of snapshots currently valid for the table. Setting the value to `refs` would load all snapshots referenced by branches or tags. |

#### Headers in RESTCatalog

To configure custom headers in RESTCatalog, include them in the catalog properties with the prefix `header.`. This
ensures that all HTTP requests to the REST service include the specified headers.

```yaml
catalog:
  default:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret
    header.content-type: application/vnd.api+json
```

Specific headers defined by the RESTCatalog spec include:

| Key                                  | Options                               | Default              | Description                                                                                                                                                                                        |
| ------------------------------------ | ------------------------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `header.X-Iceberg-Access-Delegation` | `{vended-credentials,remote-signing}` | `vended-credentials` | Signal to the server that the client supports delegated access via a comma-separated list of access mechanisms. The server may choose to supply access via any or none of the requested mechanisms |

### SQL Catalog

The SQL catalog requires a database for its backend. PyIceberg supports PostgreSQL and SQLite through psycopg2. The database connection has to be configured using the `uri` property. The init_catalog_tables is optional and defaults to True. If it is set to False, the catalog tables will not be created when the SQLCatalog is initialized. See SQLAlchemy's [documentation for URL format](https://docs.sqlalchemy.org/en/20/core/engines.html#backend-specific-urls):

For PostgreSQL:

```yaml
catalog:
  default:
    type: sql
    uri: postgresql+psycopg2://username:password@localhost/mydatabase
    init_catalog_tables: false
```

In the case of SQLite:

```yaml
catalog:
  default:
    type: sql
    uri: sqlite:////tmp/pyiceberg.db
    init_catalog_tables: false
```

| Key           | Example                                                      | Default | Description                                                                        |
| ------------- | ------------------------------------------------------------ | ------- | ---------------------------------------------------------------------------------- |
| uri           | postgresql+psycopg2://username:password@localhost/mydatabase |         | SQLAlchemy backend URL for the catalog database (see documentation for URL format) |
| echo          | true                                                         | false   | SQLAlchemy engine echo param                                                       |
| pool_pre_ping | true                                                         | false   | SQLAlchemy engine pool_pre_ping param                                              |

### In Memory Catalog

The in-memory catalog is built on top of `SqlCatalog` and uses SQLite in-memory database for its backend.

It is useful for test, demo, and playground but not in production as it does not support concurrent access.

```yaml
catalog:
  default:
    type: in-memory
    warehouse: /tmp/pyiceberg/warehouse
```

| Key       | Example                  | Default                       | Description                                                          |
| --------- |--------------------------|-------------------------------|----------------------------------------------------------------------|
| warehouse | /tmp/pyiceberg/warehouse | file:///tmp/iceberg/warehouse | The directory where the in-memory catalog will store its data files. |

### Hive Catalog

```yaml
catalog:
  default:
    uri: thrift://localhost:9083
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
```

| Key                          | Example | Description                       |
|------------------------------| ------- | --------------------------------- |
| hive.hive2-compatible        | true    | Using Hive 2.x compatibility mode |
| hive.kerberos-authentication | true    | Using authentication via Kerberos |

When using Hive 2.x, make sure to set the compatibility flag:

```yaml
catalog:
  default:
...
    hive.hive2-compatible: true
```

### Custom Catalog Implementations

If you want to load any custom catalog implementation, you can set catalog configurations like the following:

```yaml
catalog:
  default:
    py-catalog-impl: mypackage.mymodule.MyCatalog
    custom-key1: value1
    custom-key2: value2
```

## Unified AWS Credentials

You can explicitly set the AWS credentials for both Glue/DynamoDB Catalog and S3 FileIO by configuring `client.*` properties. For example:

```yaml
catalog:
  default:
    type: glue
    client.access-key-id: <ACCESS_KEY_ID>
    client.secret-access-key: <SECRET_ACCESS_KEY>
    client.region: <REGION_NAME>
```

configures the AWS credentials for both Glue Catalog and S3 FileIO.

| Key                      | Example         | Description                                                                                            |
| ------------------------ | --------------- | ------------------------------------------------------------------------------------------------------ |
| client.region            | us-east-1       | Set the region of both the Glue/DynamoDB Catalog and the S3 FileIO                                     |
| client.access-key-id     | admin           | Configure the static access key id used to access both the Glue/DynamoDB Catalog and the S3 FileIO     |
| client.secret-access-key | password        | Configure the static secret access key used to access both the Glue/DynamoDB Catalog and the S3 FileIO |
| client.session-token     | AQoDYXdzEJr...  | Configure the static session token used to access both the Glue/DynamoDB Catalog and the S3 FileIO     |
| client.role-session-name | session         | An optional identifier for the assumed role session.                                                   |
| client.role-arn          | arn:aws:...     | AWS Role ARN. If provided instead of access_key and secret_key, temporary credentials will be fetched by assuming this role. |

`client.*` properties will be overridden by service-specific properties if they are set. For example, if `client.region` is set to `us-west-1` and `s3.region` is set to `us-east-1`, the S3 FileIO will use `us-east-1` as the region.

## Concurrency

PyIceberg uses multiple threads to parallelize operations. The number of workers can be configured by supplying a `max-workers` entry in the configuration file, or by setting the `PYICEBERG_MAX_WORKERS` environment variable. The default value depends on the system hardware and Python version. See [the Python documentation](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor) for more details.
