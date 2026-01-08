# Petrinex Python API

Load Alberta Petrinex volumetric data into Spark/pandas DataFrames - Unity Catalog compatible, memory efficient.

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Quick Start

```bash
pip install petrinex
```

```python
from petrinex import PetrinexVolumetricsClient

client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")

# Incremental updates
df = client.read_spark_df(updated_after="2025-12-01")

# Historical backfill
df = client.read_spark_df(from_date="2021-01-01")

df.show()
```

## Why Use This?

- ✅ **Memory Efficient** - Handles 60+ files without OOM
- ✅ **Zero Config** - Handles ZIP extraction, encoding, malformed rows automatically
- ✅ **Databricks Ready** - Works in notebooks and repos

## API Reference

### Load Data

```python
# Returns Spark DataFrame
df = client.read_spark_df(updated_after="2025-12-01")

# Returns pandas DataFrame
pdf = client.read_pandas_df(updated_after="2025-12-01")
```

**Date Options:**
- `updated_after="2025-12-01"` - Files modified after this date (incremental)
- `from_date="2021-01-01"` - All data from this production month onwards (backfill)

### List Available Files

```python
files = client.list_updated_after("2025-12-01")
for f in files:
    print(f"{f.production_month} | {f.updated_ts}")
```

### Added Columns

Every DataFrame includes:
- `production_month` - Production period (YYYY-MM)
- `file_updated_ts` - When Petrinex last updated the file
- `source_url` - Download URL

## Examples

### Incremental Updates

```python
# Get last processed timestamp
last_update = spark.sql(
    "SELECT MAX(file_updated_ts) FROM main.petrinex.volumetrics"
).first()[0]

# Load only new/changed files
df = client.read_spark_df(updated_after=last_update.split()[0])
```

### Historical Load

```python
# Load everything from 2020 onwards
df = client.read_spark_df(from_date="2020-01-01")

# Save to Delta
df.write.format("delta").mode("overwrite") \
  .saveAsTable("main.petrinex.volumetrics")
```

### Last 6 Months

```python
from datetime import datetime, timedelta

six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
df = client.read_spark_df(updated_after=six_months_ago)
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
pip install -e .
```

## Databricks

See [databricks_example.ipynb](databricks_example.ipynb) for a complete working example.

1. Sync this repo to Databricks Repos
2. Open the notebook
3. Run it

## Troubleshooting

| Issue | Fix |
|-------|-----|
| "No data loaded" | Use an earlier date (files may not be published yet) |
| Memory errors | Increase driver memory or use `from_date` for backfills |
| `AttributeError` | Restart kernel/clear cache: `%restart_python` |

## What's Happening Under the Hood

1. Scrapes Petrinex PublicData page for file metadata
2. Filters files by your date criteria
3. Downloads and extracts nested ZIP files
4. Parses CSVs with robust error handling (handles bad rows, encoding)
5. Unions incrementally with auto-checkpointing (memory efficient)
6. Returns a single Spark DataFrame

## Requirements

- Python 3.7+
- PySpark 3.0+
- pandas, requests, beautifulsoup4, lxml

## License

MIT - see [LICENSE](LICENSE)

## Links

- 📦 [PyPI](https://pypi.org/project/petrinex/)
- 📓 [Example Notebook](databricks_example.ipynb)
- 🐛 [Issues](https://github.com/guanjieshen/petrinex-python-api/issues)

---

Made for the Alberta energy data community 🛢️
