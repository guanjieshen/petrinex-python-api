# Petrinex Python API

Load Alberta Petrinex data (Volumetrics, NGL) into Spark/pandas DataFrames.

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- ✅ **Unity Catalog Compatible** - No `ANY FILE` privilege needed
- ✅ **Memory Efficient** - Handles 60+ files without OOM
- ✅ **Multiple Data Types** - Volumetrics and NGL support
- ✅ **Zero Config** - Handles ZIP extraction, encoding, malformed rows automatically

## Quick Start

```bash
pip install petrinex
```

```python
from petrinex import PetrinexClient

# Volumetrics
client = PetrinexClient(spark=spark, data_type="Vol")
df = client.read_spark_df(updated_after="2025-12-01")

# NGL and Marketable Gas
ngl_client = PetrinexClient(spark=spark, data_type="NGL")
ngl_df = ngl_client.read_spark_df(updated_after="2025-12-01")
```

## Supported Data Types

| Data Type | Description |
|-----------|-------------|
| `Vol` | Conventional Volumetrics (oil & gas production) |
| `NGL` | NGL and Marketable Gas Volumes |

## API

### Load Data

```python
# Spark DataFrame (recommended)
df = client.read_spark_df(updated_after="2025-12-01")

# pandas DataFrame (for smaller datasets)
pdf = client.read_pandas_df(updated_after="2025-12-01")
```

**Date Options:**
- `updated_after="2025-12-01"` - Files modified after this date
- `from_date="2021-01-01"` - All data from this production month onwards

### List Files

```python
files = client.list_updated_after("2025-12-01")
for f in files:
    print(f"{f.production_month} | {f.updated_ts}")
```

### Provenance Columns

Every DataFrame includes:
- `production_month` - Production period (YYYY-MM)
- `file_updated_ts` - When Petrinex updated the file
- `source_url` - Download URL

## Examples

See [example.py](example.py) and [databricks_example.ipynb](databricks_example.ipynb) for complete examples.

### Incremental Updates

```python
# Get last processed timestamp
last_update = spark.sql(
    "SELECT MAX(file_updated_ts) FROM main.petrinex.volumetrics"
).first()[0]

# Load only new/changed files
df = client.read_spark_df(updated_after=last_update.split()[0])
```

### Historical Backfill

```python
# Load all data from 2020 onwards
df = client.read_spark_df(from_date="2020-01-01")

# Save to Delta
df.write.format("delta").mode("overwrite") \
  .saveAsTable("main.petrinex.volumetrics")
```

## Installation

```bash
# From PyPI
pip install petrinex

# From GitHub
pip install git+https://github.com/guanjieshen/petrinex-python-api.git

# For development
git clone https://github.com/guanjieshen/petrinex-python-api.git
cd petrinex-python-api
pip install -e ".[dev]"
```

## Databricks

### Quick Setup

1. Sync this repo to Databricks Repos
2. Open [databricks_example.ipynb](databricks_example.ipynb)
3. Run it

### Usage in Databricks Notebooks

```python
# Install directly from GitHub
%pip install git+https://github.com/guanjieshen/petrinex-python-api.git

from petrinex import PetrinexClient

# Create client
client = PetrinexClient(spark=spark, data_type="Vol")

# Option 1: Load directly as Spark DataFrame (recommended)
df = client.read_spark_df(updated_after="2025-12-01")
display(df)

# Option 2: Load as pandas DataFrame
pdf = client.read_pandas_df(updated_after="2025-12-01")

# Convert pandas to Spark DataFrame if needed
df_from_pandas = spark.createDataFrame(pdf)
display(df_from_pandas)
```

**Alternative - Install from Specific Branch:**
```python
%pip install git+https://github.com/guanjieshen/petrinex-python-api.git@feature/ngl-gas-support
```

## Testing

```bash
# Unit tests only
pytest tests/ -v

# Include integration tests (requires network)
pytest tests/ -v -m integration

# With coverage
pytest tests/ --cov=petrinex
```

## Requirements

- Python 3.7+
- PySpark 3.0+
- pandas, requests, beautifulsoup4, lxml

## Troubleshooting

| Issue | Fix |
|-------|-----|
| "No data loaded" | Use an earlier date |
| Memory errors | Increase driver memory or use `from_date` for backfills |
| `AttributeError` | Restart kernel: `%restart_python` |

## Links

- 📦 [PyPI](https://pypi.org/project/petrinex/)
- 📓 [Example Notebook](databricks_example.ipynb)
- 🧪 [Tests](tests/)
- 📋 [Changelog](CHANGELOG.md)
- 🐛 [Issues](https://github.com/guanjieshen/petrinex-python-api/issues)

## License

MIT - see [LICENSE](LICENSE)

---

Made for the Alberta energy data community 🛢️
