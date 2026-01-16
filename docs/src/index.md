# Lance-Ray Integration

Welcome to the Lance-Ray documentation! 
Lance-Ray combines the distributed computing capabilities of [Ray](https://ray.io/) 
with the efficient Lance storage format, 
enabling scalable data processing workflows with optimal performance.

## Features

- **Distributed Lance Operations**: Leverage Ray's distributed computing for Lance dataset operations
- **Seamless Data Conversion**: Easy conversion between Ray datasets and Lance datasets
- **Optimized I/O**: Efficient reading and writing of Lance datasets with Ray integration
- **Schema Validation**: Automatic schema compatibility checking between Ray and Lance
- **Flexible Filtering**: Support for complex filtering pushdown on distributed Lance data
- **Data Evolution**: Support for data evolution to add new columns and distributedly backfill data using a Ray UDF
- **Catalog Integration**: Support for working with Lance datasets stored in various catalog services (e.g. Hive MetaStore, Iceberg REST Catalog, Unity, Gravitino, AWS Glue, etc.)

## Quickstart

### Installation

```shell
pip install lance-ray
```

To install a prerelease version:

```shell
pip install lance-ray==0.2.0b1 --extra-index-url https://pypi.fury.io/lance-format/
```

### Simple Example

```python
import ray
from lance_ray import read_lance, write_lance

# Initialize Ray
ray.init()

# Create a Ray dataset
data = ray.data.range(1000).map(lambda row: {"id": row["id"], "value": row["id"] * 2})

# Write to Lance format
write_lance(data, "my_dataset.lance")

# Read Lance dataset back as Ray dataset
ray_dataset = read_lance("my_dataset.lance")

# Perform distributed operations
result = ray_dataset.filter(lambda row: row["value"] > 100).count()
print(f"Filtered count: {result}")
```