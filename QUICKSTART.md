# Petrinex Python API - Quick Start Guide

## Installation

```bash
pip install git+https://github.com/yourusername/petrinex-python-api.git
```

## Basic Usage

```python
from petrinex import PetrinexVolumetricsClient

# Initialize
client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")

# Load data (recommended for Unity Catalog)
df = client.read_updated_after_as_spark_df_via_pandas(
    "2026-01-01",
    pandas_read_kwargs={"dtype": str, "encoding": "latin1"}
)
```

## Databricks Workflow

1. **Upload Notebook**: Import `databricks_example.ipynb` to your Databricks workspace
2. **Install Package**: Run the installation cell
3. **Configure**: Update catalog/schema names in cell 8
4. **Run**: Execute all cells
5. **Schedule**: Create a job to run regularly

## Key Parameters

### Updated After Date
- Format: `"YYYY-MM-DD"`
- Example: `"2026-01-01"` loads all files updated after Jan 1, 2026

### Pandas Read Kwargs (Recommended)
```python
{
    "dtype": str,           # Avoid mixed-type issues
    "encoding": "latin1"    # Handle special characters
}
```

### Table Configuration
```python
catalog_name = "main"       # Your Unity Catalog name
schema_name = "petrinex"    # Your schema name
table_name = "volumetrics_raw"
```

## Common Patterns

### Incremental Load
```python
# Get last update from existing table
last_update = spark.sql("""
    SELECT MAX(file_updated_ts) as last_update
    FROM main.petrinex.volumetrics_raw
""").collect()[0]["last_update"]

# Load only new data
df = client.read_updated_after_as_spark_df_via_pandas(
    last_update.split()[0],  # Convert datetime to date
    pandas_read_kwargs={"dtype": str, "encoding": "latin1"}
)
```

### Partitioning Strategy
```python
df_partitioned = df.withColumn(
    "year", F.substring(F.col("production_month"), 1, 4)
).withColumn(
    "month", F.substring(F.col("production_month"), 6, 2)
)

df_partitioned.write \
    .format("delta") \
    .partitionBy("year", "month") \
    .saveAsTable("catalog.schema.table")
```

## Troubleshooting

### Issue: "ANY FILE privilege required"
**Solution**: Use `read_updated_after_as_spark_df_via_pandas()` instead of `read_updated_after_as_spark_df()`

### Issue: Mixed type column errors
**Solution**: Add `"dtype": str` to `pandas_read_kwargs`

### Issue: Special characters not displaying
**Solution**: Add `"encoding": "latin1"` to `pandas_read_kwargs`

### Issue: Out of memory
**Solution**: Load smaller date ranges or increase driver memory

## Files in This Repo

- `petrinex/` - Main package code
- `README.md` - Complete documentation
- `example.py` - Python script example
- `databricks_example.ipynb` - Databricks notebook example
- `QUICKSTART.md` - This file
- `LICENSE` - MIT License

## Support

- 📖 [Full Documentation](README.md)
- 💻 [Example Notebook](databricks_example.ipynb)
- 🐛 [Report Issues](https://github.com/yourusername/petrinex-python-api/issues)

