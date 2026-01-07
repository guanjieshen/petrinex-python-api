# Petrinex Python API

Load Alberta Petrinex volumetric data into Spark DataFrames - memory efficient and Unity Catalog compatible.

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Quick Start

### Install

```bash
pip install petrinex
```

### Use

```python
from petrinex import PetrinexVolumetricsClient

client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")

# Returns Spark DataFrame (sensible defaults already set)
df = client.read_spark_df("2025-12-01")

df.show()
```

## Features

- ✅ **Unity Catalog Compatible** - No `ANY FILE` privilege needed
- ✅ **Memory Efficient** - Incremental loading with auto-checkpointing
- ✅ **Robust** - Handles ZIP files, missing data, encoding automatically
- ✅ **Databricks Ready** - Import directly from repos

## Databricks

Use the included notebook `databricks_example.ipynb`:

1. Sync this repo to Databricks Repos
2. Open the notebook
3. Run all cells

That's it!

## Installation Options

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

## API

### Initialize Client

```python
from petrinex import PetrinexVolumetricsClient

client = PetrinexVolumetricsClient(
    spark=spark,
    jurisdiction="AB",        # Jurisdiction code
    file_format="CSV",        # File format
    request_timeout_s=60      # HTTP timeout
)
```

### Load Data

**Spark DataFrame (recommended for large data):**
```python
# Sensible defaults already configured - just specify the date!
df = client.read_spark_df("2025-12-01")
```

**Pandas DataFrame (for smaller data):**
```python
pdf = client.read_pandas_df("2025-12-01")
```

**Progress shown automatically:**
```
Loading 1/60: 2021-01... ✓ (544,221 rows)
Loading 2/60: 2021-02... ✓ (498,832 rows)
...
✓ Successfully loaded 60 file(s)
```

**Automatic optimizations:**
- All Petrinex CSV quirks handled automatically
- `dtype=str` - Avoids mixed-type issues
- `encoding="latin1"` - Handles special characters
- `on_bad_lines="skip"` - Handles malformed rows
- `engine="python"` - Robust parsing

### List Files

```python
files = client.list_updated_after("2025-12-01")
for f in files:
    print(f"{f.production_month} | {f.updated_ts}")
```

### Save to Delta

```python
df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("main.petrinex.volumetrics")
```

## What It Does

1. Scrapes Petrinex PublicData page for file metadata
2. Downloads files updated after your cutoff date
3. Extracts from nested ZIP files automatically
4. Loads CSV with robust error handling
5. Unions DataFrames incrementally (memory efficient)
6. Adds provenance columns (source, date, URL)

## Provenance Columns

All DataFrames include:
- `production_month` - Production period (YYYY-MM)
- `file_updated_ts` - File update timestamp
- `source_url` - Download URL

## Memory Efficient

- Unions DataFrames incrementally (not all at once)
- Auto-checkpoints every 10 files
- Typical usage: 16-32 GB driver memory for 60 files

## Error Handling

Automatically handles:
- ✅ 404 errors (skips missing files)
- ✅ Malformed CSV lines (skips bad rows)
- ✅ Nested ZIP files (extracts automatically)
- ✅ Encoding issues (uses latin-1)
- ✅ Schema drift (aligns columns)

## Examples

### Last 6 Months

```python
from datetime import datetime, timedelta

six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
df = client.read_spark_df(six_months_ago)
```

### Incremental Updates

```python
# Get last update from existing table
last_update = spark.sql("SELECT MAX(file_updated_ts) FROM petrinex.volumetrics").first()[0]

# Load only new files
df_new = client.read_spark_df(last_update.split()[0])
```

### Use Pandas DataFrame

```python
# For smaller datasets or local analysis
pdf = client.read_pandas_df("2025-12-01")

# Now it's a pandas DataFrame
pdf.head()
pdf.to_csv("petrinex_data.csv")
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "No data loaded" | Use earlier date (files may not be published yet) |
| Memory errors | Increase driver memory: `spark.conf.set("spark.driver.memory", "32g")` |
| Import errors | Ensure PySpark is installed: `pip install pyspark` |

## Requirements

- Python 3.7+
- PySpark 3.0+
- pandas 1.2+
- requests, beautifulsoup4, lxml

## Publishing

### For Maintainers

**Test:**
```bash
git tag v0.1.0-test
git push origin v0.1.0-test
```

**Production:**
- Create GitHub Release with tag `v0.1.0`
- Package auto-publishes to PyPI

See [.github/workflows/README.md](.github/workflows/README.md) for details.

## License

MIT License - see [LICENSE](LICENSE)

## Changelog

### v0.2.0 (Latest)
- **API Changes:**
  - ✅ Renamed: `read_updated_after_as_spark_df_via_pandas()` → `read_spark_df()`
  - ✅ New: `read_pandas_df()` method for pandas DataFrame output
  - ✅ Old method kept as deprecated alias (backward compatible)
- **Improved UX:**
  - ✅ Sensible defaults now automatic - no need to specify `pandas_read_kwargs`
  - ✅ Simpler API: `client.read_spark_df("2025-12-01")` just works!
  - ✅ Automatic handling of Petrinex CSV quirks (encoding, malformed rows, types)

### v0.1.0
- Initial release
- Unity Catalog compatible
- Memory-efficient incremental loading
- Automatic error handling

## Links

- 📦 [PyPI Package](https://pypi.org/project/petrinex/)
- 💻 [Example Notebook](databricks_example.ipynb)
- 🐛 [Report Issues](https://github.com/guanjieshen/petrinex-python-api/issues)

---

**Made for the Alberta energy data community** 🛢️
