# Tests

## Running Tests

### All tests (unit only, no network required)
```bash
pytest tests/ -v
```

### Include integration tests (requires network)
```bash
pytest tests/ -v -m integration
```

### Run specific test file
```bash
pytest tests/test_client.py -v
```

### Run with coverage
```bash
pytest tests/ --cov=petrinex --cov-report=html
```

## Test Organization

- `test_client.py` - Unit tests for PetrinexClient (no network required)
- `test_integration.py` - Integration tests with real Petrinex API (requires network)

## Requirements

```bash
pip install pytest pytest-cov
```

## CI/CD

Tests are automatically run on:
- Pull requests
- Pushes to main branch
- Tagged releases

See `.github/workflows/test.yml` for CI configuration.

