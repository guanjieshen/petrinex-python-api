# Petrinex Python API

Load Alberta Petrinex data (Volumetrics, NGL) into Spark/pandas DataFrames.

[![PyPI version](https://img.shields.io/pypi/v/petrinex.svg)](https://pypi.org/project/petrinex/)
[![Downloads](https://pepy.tech/badge/petrinex)](https://pepy.tech/project/petrinex)
[![Build Status](https://github.com/guanjieshen/petrinex-python-api/workflows/Tests/badge.svg)](https://github.com/guanjieshen/petrinex-python-api/actions?query=workflow%3ATests)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- ✅ **Databricks Serverless** - Full Unity Catalog support
- ✅ **Memory Efficient** - Handles 100+ files without OOM
- ✅ **Zero Config** - Automatic ZIP extraction, encoding, error handling
- ✅ **Multiple Data Types** - Volumetrics (Vol) and NGL support

> **Note:** Currently supports Alberta (AB) jurisdiction only.

## Quick Start

```bash
pip install petrinex
```

```python
from petrinex import PetrinexClient

# Volumetrics data
client = PetrinexClient(spark=spark, data_type="Vol")
df = client.read_spark_df(updated_after="2025-12-01")

# NGL and Marketable Gas
ngl_client = PetrinexClient(spark=spark, data_type="NGL")
ngl_df = ngl_client.read_spark_df(updated_after="2025-12-01")
```

## API

### Load Data

```python
# Spark DataFrame (recommended)
df = client.read_spark_df(updated_after="2025-12-01")

# pandas DataFrame
pdf = client.read_pandas_df(updated_after="2025-12-01")

# Date range
df = client.read_spark_df(from_date="2021-01-01", end_date="2023-12-31")
```

**Date Parameters:**
- `updated_after="2025-12-01"` - Files modified after this date
- `from_date="2021-01-01"` - All data from production month onwards
- `end_date="2023-12-31"` - Optional end date (use with `from_date`)

### Download Files

Download Petrinex files to your local machine. Files are extracted from ZIP and organized in subdirectories by production month:

```python
# Download recent updates
paths = client.download_files(
    output_dir="./petrinex_data",
    updated_after="2025-12-01"
)
# Creates: ./petrinex_data/2025-12/Vol_2025-12.csv

# Historical range
paths = client.download_files(
    output_dir="./data",
    from_date="2021-01-01",
    end_date="2023-12-31"
)
```

### Large Data Loads (Unity Catalog)

For large data loads (20+ files), write directly to Unity Catalog to avoid memory issues and timeouts:

```python
# Write directly to UC table (avoids memory accumulation)
df = client.read_spark_df(
    from_date="2020-01-01",
    uc_table="main.petrinex.volumetrics"
)

# Incremental updates
df = client.read_spark_df(
    updated_after="2025-12-01",
    uc_table="main.petrinex.volumetrics"
)

# Full refresh (truncate first)
spark.sql("TRUNCATE TABLE main.petrinex.volumetrics")
df = client.read_spark_df(from_date="2020-01-01", uc_table="main.petrinex.volumetrics")
```

**Benefits:**
- ✅ No memory accumulation
- ✅ No Spark Connect timeouts
- ✅ Automatic schema evolution
- ✅ Handles 100+ files
- ✅ Provenance & schema validation

**Safety Features:**
- Provenance validation (checks for required columns)
- Schema validation (ensures compatibility)
- Schema evolution (adds new columns automatically)
- Append-only mode (no accidental overwrites)

## Databricks

```python
%pip install git+https://github.com/guanjieshen/petrinex-python-api.git

from petrinex import PetrinexClient

client = PetrinexClient(spark=spark, data_type="Vol")
df = client.read_spark_df(updated_after="2025-12-01")
display(df)
```

See [databricks_example.ipynb](databricks_example.ipynb) for complete example.

## Data Types

| Type | Description |
|------|-------------|
| `Vol` | Conventional Volumetrics (oil & gas production) |
| `NGL` | NGL and Marketable Gas Volumes |

## Installation

```bash
# From PyPI
pip install petrinex

# From GitHub
pip install git+https://github.com/guanjieshen/petrinex-python-api.git

# Development
git clone https://github.com/guanjieshen/petrinex-python-api.git
cd petrinex-python-api
pip install -e ".[dev]"
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=petrinex --cov-report=html

# Integration tests (requires network)
pytest tests/ -v -m integration
```

## Links

- 📦 [PyPI](https://pypi.org/project/petrinex/)
- 📓 [Databricks Example](databricks_example.ipynb)
- 📋 [Changelog](CHANGELOG.md)
- 🧪 [Tests](tests/)

## License

MIT License - Copyright (c) 2026 Guanjie Shen
