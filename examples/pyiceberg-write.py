# %%
from pyiceberg import __version__

__version__

# %% [markdown]
# # Write support
# 
# This notebook demonstrates writing to Iceberg tables using PyIceberg.

# %%
from pyiceberg.catalog import load_catalog

catalog = load_catalog('default')

# %% [markdown]
# # Loading data using Arrow
# 
# PyArrow is used to load a Parquet file into memory, and using PyIceberg this data can be written to an Iceberg table.

# %%
import pyarrow.parquet as pq

df = pq.read_table("/home/iceberg/data/yellow_tripdata_2022-01.parquet")

df

# %% [markdown]
# # Create an Iceberg table
# 
# Next create the Iceberg table directly from the `pyarrow.Table`.

# %%
table_name = "default.taxi_dataset"

try:
    # In case the table already exists
    catalog.drop_table(table_name)
except:
    pass

table = catalog.create_table(table_name, schema=df.schema)

table

# %% [markdown]
# # Write the data
# 
# Let's append the data to the table. Appending or overwriting is equivalent since the table is empty. Next we can query the table and see that the data is there.

# %%
table.append(df)  # or table.overwrite(df)

assert len(table.scan().to_arrow()) == len(df)

table.scan().to_arrow()

# %%
str(table.current_snapshot())

# %% [markdown]
# # Append data
# 
# Let's append another month of data to the table

# %%
df = pq.read_table("/home/iceberg/data/yellow_tripdata_2022-02.parquet")
table.append(df)

# %%
str(table.current_snapshot())

# %% [markdown]
# # Feature generation
# 
# Consider that we want to train a model to determine which features contribute to the tip amount. `tip_per_mile` is a good target to train the model on. When we try to append the data, we need to evolve the schema first.

# %%
import pyarrow.compute as pc

df = table.scan().to_arrow()
df = df.append_column("tip_per_mile", pc.divide(df["tip_amount"], df["trip_distance"]))

try:
    table.overwrite(df)
except ValueError as e:
    print(f"Error: {e}")

# %%
with table.update_schema() as upd:
    upd.union_by_name(df.schema)

print(str(table.schema()))

# %%
table.overwrite(df)

table


