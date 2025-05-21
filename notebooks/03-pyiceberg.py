# %% [markdown]
# #### PyIceberg Catalog Initialization

# %%
sql_user="iceberg"
sql_password="icebergpassword"


# %%
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "iceberg_jdbc",
    type="sql",
    uri=f"postgresql+psycopg2://{sql_user}:{sql_password}@postgres_catalog:5432/iceberg_catalog",
    warehouse="s3://iceberg-warehouse/",
    s3_endpoint="http://minio:9000",
    s3_access_key_id="admin",
    s3_secret_access_key="password",
    s3_path_style_access=True,
    s3_region="eu-central-1
    "region_name": <aws region>,
    "aws_access_key_id": <aws access_key>,
    "aws_secret_access_key": <aws secret key>,
)


# %% [markdown]
# #### Create Namespace

# %%
catalog.create_namespace("pyiceberg_demo")

# %%
catalog.list_namespaces()

# %% [markdown]
# #### Create Table

# %%
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType
from pyiceberg.partitioning import PartitionSpec

# schema = Schema(
#     fields=[
#         ("id", IntegerType(), False),
#         ("name", StringType(), False),
#         ("dept", StringType(), True)
#     ]
# )

# %%
# partition_spec = (PartitionSpec
#                   .builder_for(schema)
#                   .identity("dept")
#                   ).build()

# %%
table.schema

# %%
# catalog.create_table(
#     identifier="pyiceberg_demo.people",
#     schema=schema,
#     partition_spec=partition_spec
# )

# %%


# %%


# %% [markdown]
# #### Insert Data

# %%
table = catalog.load_table("pyiceberg_demo.people")
table.append([
    {"id": 1, "name": "Alice", "dept": "Engineering"},
    {"id": 2, "name": "Bob", "dept": "HR"}
])

# %% [markdown]
# #### Read Data

# %%
scan = table.scan().to_arrow()
df = scan.to_pandas()
df

# %%
import numpy as np
import pandas as pd
import pyarrow as pa

df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                   'two': ['foo', 'bar', 'baz'],
                   'three': [True, False, True]},
                   index=list('abc'))


table = pa.Table.from_pandas(df)

# %%
catalog.create_table("tab",table.schema)

# %%



