# Examples

Here are some examples to try out.
See the `examples/` directory for more comprehensive usage examples.

## Basic Read & Write

```python
import pandas as pd
import ray
from lance_ray import read_lance, write_lance

# Initialize Ray
ray.init()

# Create sample data
sample_data = {
    "user_id": range(100),
    "name": [f"User_{i}" for i in range(100)],
    "age": [20 + (i % 50) for i in range(100)],
    "score": [50.0 + (i % 100) * 0.5 for i in range(100)],
}
df = pd.DataFrame(sample_data)

# Create Ray dataset
ds = ray.data.from_pandas(df)

# Write to Lance format
write_lance(ds, "sample_dataset.lance")

# Read Lance dataset back
ds = read_lance("sample_dataset.lance")

# Perform distributed operations
filtered_ds = ds.filter(lambda row: row["age"] > 30)
print(f"Filtered count: {filtered_ds.count()}")

# Read with column selection and filtering
ds_filtered = read_lance(
    "sample_dataset.lance",
    columns=["user_id", "name", "score"],
    filter="score > 75.0"
)
print(f"Schema: {ds_filtered.schema()}")
```

## Data Evolution

```python
# Add columns using metadata catalog
from lance_ray import add_columns
import pyarrow as pa

def add_computed_column(batch: pa.RecordBatch) -> pa.RecordBatch:
    df = batch.to_pandas()
    df['computed'] = df['score'] * 2 + df['user_id']
    return pa.RecordBatch.from_pandas(df[["computed"]])

add_columns(
    uri="sample_dataset.lance",
    transform=add_computed_column,
    concurrency=4
)
```

## Using Namespace

For enterprise environments with metadata catalogs, you can use Lance Namespace integration:

```python
import ray
from lance_ray import read_lance, write_lance

# Initialize Ray
ray.init()

# Create a Ray dataset
data = ray.data.range(1000).map(lambda row: {"id": row["id"], "value": row["id"] * 2})

# Write to Lance format using metadata catalog
write_lance(
    data,
    namespace_impl="dir",
    namespace_properties={"root": "/path/to/tables"},
    table_id=["my_table"],
)

# Read Lance dataset back using metadata catalog
ray_dataset = read_lance(
    namespace_impl="dir",
    namespace_properties={"root": "/path/to/tables"},
    table_id=["my_table"],
)

# Perform distributed operations
result = ray_dataset.filter(lambda row: row["value"] > 100).count()
print(f"Filtered count: {result}")
```

The package dependency comes with the directory and REST namespace implementations to use by default.
To use another implementation, install the specific extra dependency. 
For example to use it with AWS Glue catalog:

```shell
pip install lance-namespace[glue]
```

And then you can do:

```python
import ray
from lance_ray import read_lance, write_lance

# Initialize Ray
ray.init()

# Create a Ray dataset
data = ray.data.range(1000).map(lambda row: {"id": row["id"], "value": row["id"] * 2})

# Write to Lance format using metadata catalog
write_lance(
    data, 
    namespace_impl="glue",
    namespace_properties={},
    table_id=["default", "my_table"]
)

# Read Lance dataset back using metadata catalog
ray_dataset = read_lance(
    namespace_impl="glue",
    namespace_properties={},
    table_id=["default", "my_table"],
)

# Perform distributed operations
result = ray_dataset.filter(lambda row: row["value"] > 100).count()
print(f"Filtered count: {result}")
```
