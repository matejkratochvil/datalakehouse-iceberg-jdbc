# %%
from pyiceberg import __version__

__version__

# %% [markdown]
# ## Load NYC Taxi/Limousine Trip Data
# 
# For this notebook, we will use the New York City Taxi and Limousine Commision Trip Record Data that's available on the AWS Open Data Registry. This contains data of trips taken by taxis and for-hire vehicles in New York City. We'll save this into an iceberg table called `taxis`.
# 
# First, load the Parquet file using PyArrow:

# %%
import pyarrow.parquet as pq

tbl_taxis = pq.read_table('/home/iceberg/data/yellow_tripdata_2021-04.parquet')
tbl_taxis

# %% [markdown]
# ## Creating the table
# 
# Next, create the namespace, and the `taxis` table from the schema that's derived from the Arrow schema:

# %%
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

cat = load_catalog('default')

try:
    cat.create_namespace('default')
except NamespaceAlreadyExistsError:
    pass

# %%
from pyiceberg.exceptions import NoSuchTableError

try:
    cat.drop_table('default.taxis')
except NoSuchTableError:
    pass

tbl = cat.create_table(
    'default.taxis',
    schema=tbl_taxis.schema
)

tbl

# %% [markdown]
# ## Write the actual data into the table
# 
# This will create a new snapshot on the table:

# %%
tbl.overwrite(tbl_taxis)

tbl

# %% [markdown]
# ## Append more data
# 
# Let's append another month of data to the table:

# %%
tbl.append(pq.read_table('/home/iceberg/data/yellow_tripdata_2021-05.parquet'))
tbl

# %% [markdown]
# ## Load data into a PyArrow Dataframe
# 
# We'll fetch the table using the REST catalog that comes with the setup.

# %%
tbl = cat.load_table('default.taxis')

sc = tbl.scan(row_filter="tpep_pickup_datetime >= '2021-05-01T00:00:00.000000'")

# %%
df = sc.to_arrow().to_pandas()

# %%
len(df)

# %%
df.info()

# %%
df

# %%
df.hist(column='fare_amount')

# %%
import numpy as np
from scipy import stats

stats.zscore(df['fare_amount'])

# Remove everything larger than 3 stddev
df = df[(np.abs(stats.zscore(df['fare_amount'])) < 3)]
# Remove everything below zero
df = df[df['fare_amount'] > 0]

# %%
df.hist(column='fare_amount')

# %% [markdown]
# # DuckDB
# 
# Use DuckDB to Query the PyArrow Dataframe directly.

# %%
%load_ext sql
%config SqlMagic.autopandas = True
%config SqlMagic.feedback = False
%config SqlMagic.displaycon = False
%sql duckdb:///:memory:

# %%
%sql SELECT * FROM df LIMIT 20

# %%
%%sql --save tip_amount --no-execute

SELECT tip_amount
FROM df

# %%
%sqlplot histogram --table df --column tip_amount --bins 22 --with tip_amount

# %%
%%sql --save tip_amount_filtered --no-execute

WITH tip_amount_stddev AS (
    SELECT STDDEV_POP(tip_amount) AS tip_amount_stddev
    FROM df
)

SELECT tip_amount
FROM df, tip_amount_stddev
WHERE tip_amount > 0
  AND tip_amount < tip_amount_stddev * 3

# %%
%sqlplot histogram --table tip_amount_filtered --column tip_amount --bins 50 --with tip_amount_filtered



