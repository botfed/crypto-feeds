# Python Bindings - Symbol Registry Integration

The Python bindings have been updated to use integer symbol IDs from the symbol registry for efficient market data access.

## What Changed

### New: `PySymbolRegistry` Class

A new class that exposes the symbol registry to Python:

```python
import crypto_feeds

registry = crypto_feeds.PySymbolRegistry()

# Look up symbol ID (accepts multiple formats)
btc_id = registry.lookup("BTCUSDT", "spot")  # Returns: 0
btc_id = registry.lookup("BTC-USDT", "spot") # Returns: 0 (same ID)
btc_id = registry.lookup("BTC/USDT", "spot") # Returns: 0 (same ID)
btc_id = registry.lookup("BTC_USDT", "spot") # Returns: 0 (same ID)

# Get canonical symbol name
canonical = registry.get_symbol(btc_id)  # Returns: "SPOT-BTC-USDT"
```

### Updated: `PyMarketData` Methods

All methods now accept integer symbol IDs instead of string symbols:

```python
market_data = crypto_feeds.PyMarketData()

# Old way (no longer works):
# bid = market_data.get_bid("binance", "BTCUSDT")

# New way (using symbol IDs):
btc_id = registry.lookup("BTC_USDT", "spot")
bid = market_data.get_bid("binance", btc_id)
ask = market_data.get_ask("binance", btc_id)
midquote = market_data.get_midquote("binance", btc_id)
mean = market_data.get_midquote_mean(btc_id)
```

## Quick Start

### Install

```bash
# Using maturin (development)
maturin develop --features python

# Or install from wheel
pip install target/wheels/crypto_feeds-0.1.0-*.whl
```

### Basic Usage

```python
import crypto_feeds

# Initialize registry
registry = crypto_feeds.PySymbolRegistry()

# Look up symbol IDs
btc_usdt_id = registry.lookup("BTC_USDT", "spot")
eth_usdt_id = registry.lookup("ETH_USDT", "spot")

# Create feed manager and start feeds
config = crypto_feeds.PyAppConfig.from_dict({
    "spot": {
        "binance": ["BTC_USDT", "ETH_USDT"],
    },
    "perp": {}
})

manager = crypto_feeds.PyFeedManager()
manager.start_spot_feeds(config)

# Get market data using symbol IDs
market_data = manager.get_market_data()
bid = market_data.get_bid("binance", btc_usdt_id)
ask = market_data.get_ask("binance", btc_usdt_id)
```

## Available Examples

### 1. `verify_install.py`
Quick verification that PySymbolRegistry is installed correctly.

```bash
python verify_install.py
```

### 2. `simple_demo.py`
Demonstrates registry usage without requiring live feeds.

```bash
python simple_demo.py
```

### 3. `test_registry.py`
Comprehensive test suite for the symbol registry.

```bash
python test_registry.py
```

### 4. `example_usage.py`
Full example with live market data feeds.

```bash
python example_usage.py
```

## Important Notes

### Symbol Formats

- **Registry Lookup**: Accepts any format (BTCUSDT, BTC-USDT, BTC/USDT, BTC_USDT)
- **Config Files**: Must use underscore format (BTC_USDT) for exchange mappers
- **Canonical Format**: SPOT-BTC-USDT or PERP-BTC-USDT

### Instrument Types

- `"spot"` - Spot markets
- `"perp"` - Perpetual futures

### Benefits

1. **Performance**: Integer lookups are faster than string comparisons
2. **Memory**: Reduced memory usage in market data storage
3. **Flexibility**: Multiple symbol formats map to the same ID
4. **Type Safety**: Symbol IDs are validated at lookup time

## API Reference

See [PYTHON_API.md](PYTHON_API.md) for complete API documentation.

## Troubleshooting

### Module doesn't have PySymbolRegistry

Reinstall the package:
```bash
maturin develop --features python
```

### Invalid symbol errors

Make sure config uses underscore format (BTC_USDT) and the symbol exists in `configs/symbols.yaml`.

### No data returning

Ensure feeds are started and connected (check log output).
