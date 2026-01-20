# Petrinex Python API

Load Alberta Petrinex data (Volumetrics, NGL) into Spark/pandas DataFrames.

> **Note:** Currently supports Alberta (AB) jurisdiction only.

[![PyPI version](https://img.shields.io/pypi/v/petrinex.svg)](https://pypi.org/project/petrinex/)
[![Downloads](https://pepy.tech/badge/petrinex)](https://pepy.tech/project/petrinex)
[![Build Status](https://github.com/guanjieshen/petrinex-python-api/workflows/Tests/badge.svg)](https://github.com/guanjieshen/petrinex-python-api/actions?query=workflow%3ATests)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- ✅ Unity Catalog & Databricks Serverless compatible
- ✅ Memory efficient - handles 60+ files without OOM
- ✅ Zero config - automatic ZIP extraction, encoding, error handling
- ✅ Multiple data types - Volumetrics and NGL support

## Quick Start

```bash
pip install petrinex
```

```python
from petrinex import PetrinexClient

# Volumetrics (Alberta only)
client = PetrinexClient(spark=spark, jurisdiction="AB", data_type="Vol")
df = client.read_spark_df(updated_after="2025-12-01")

# NGL and Marketable Gas
ngl_client = PetrinexClient(spark=spark, jurisdiction="AB", data_type="NGL")
ngl_df = ngl_client.read_spark_df(updated_after="2025-12-01")
```

## API

### Load Data

```python
# Spark DataFrame (recommended)
df = client.read_spark_df(updated_after="2025-12-01")

# pandas DataFrame
pdf = client.read_pandas_df(updated_after="2025-12-01")
```

**Date Options:**
- `updated_after="2025-12-01"` - Files modified after this date
- `from_date="2021-01-01"` - All data from production month onwards

### Supported Data Types

| Type | Description |
|------|-------------|
| `Vol` | Conventional Volumetrics |
| `NGL` | NGL and Marketable Gas Volumes |

## Databricks

```python
# Install from GitHub
%pip install git+https://github.com/guanjieshen/petrinex-python-api.git

from petrinex import PetrinexClient

client = PetrinexClient(spark=spark, data_type="Vol")
df = client.read_spark_df(updated_after="2025-12-01")
display(df)
```

See [databricks_example.ipynb](databricks_example.ipynb) for complete example.

## Examples

### Incremental Updates

```python
last_update = spark.sql(
    "SELECT MAX(file_updated_ts) FROM main.petrinex.volumetrics"
).first()[0]

df = client.read_spark_df(updated_after=last_update.split()[0])
```

### Historical Backfill

```python
df = client.read_spark_df(from_date="2020-01-01")
df.write.format("delta").mode("overwrite").saveAsTable("main.petrinex.volumetrics")
```

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
pytest tests/ -v                    # Unit tests
pytest tests/ -v -m integration     # Include integration tests
pytest tests/ --cov=petrinex        # With coverage
```

## Links

- 📦 [PyPI](https://pypi.org/project/petrinex/)
- 📓 [Databricks Example](databricks_example.ipynb)
- 🧪 [Tests](tests/)
- 📋 [Changelog](CHANGELOG.md)
- 🤝 [Contributing](CONTRIBUTING.md)

## License

MIT License - Copyright (c) 2026 Guanjie Shen

See [LICENSE](LICENSE) for full details.
