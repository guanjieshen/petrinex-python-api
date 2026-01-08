# Contributing

## Development Setup

```bash
git clone https://github.com/guanjieshen/petrinex-python-api.git
cd petrinex-python-api
pip install -e .
```

## Publishing (Maintainers Only)

### Test PyPI

```bash
git tag v0.2.0-test
git push origin v0.2.0-test
```

Check: https://test.pypi.org/project/petrinex/

### Production PyPI

1. Create a GitHub Release with tag `v0.2.0`
2. Package auto-publishes to PyPI via GitHub Actions

See [.github/workflows/README.md](.github/workflows/README.md) for CI/CD setup.

## Testing

```python
from petrinex import PetrinexVolumetricsClient

# Test in local PySpark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()

client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")
df = client.read_spark_df(updated_after="2025-12-01")
print(f"✓ Loaded {df.count()} rows")
```

## Code Style

- Python 3.7+ compatible
- Type hints where helpful
- Docstrings for public methods
- Keep it simple and readable

