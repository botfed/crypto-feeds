# Python API Reference - Symbol Registry

## Overview

The Python bindings now use integer symbol IDs for efficient lookups, powered by the `SymbolRegistry`.

## Classes

### PySymbolRegistry

Registry for looking up symbol IDs.

#### Methods

```python
registry = crypto_feeds.PySymbolRegistry()
```

**`lookup(symbol: str, instrument_type: str) -> Optional[int]`**

Look up a symbol and return its integer ID.

- **symbol**: Symbol string in any supported format (e.g., "BTCUSDT", "BTC-USDT", "BTC/USDT", "BTC_USDT")
- **instrument_type**: Either "spot" or "perp"
- **Returns**: Integer symbol ID, or None if not found

```python
btc_spot_id = registry.lookup("BTCUSDT", "spot")  # Returns: 0
btc_spot_id = registry.lookup("BTC-USDT", "spot") # Returns: 0 (same ID)
eth_perp_id = registry.lookup("ETH/USDT", "perp") # Returns: 7
```

**`get_symbol(symbol_id: int) -> Optional[str]`**

Get the canonical symbol name from an integer ID.

- **symbol_id**: Integer symbol ID
- **Returns**: Canonical symbol string (e.g., "SPOT-BTC-USDT"), or None if not found

```python
canonical = registry.get_symbol(0)  # Returns: "SPOT-BTC-USDT"
```

### PyMarketData

**All methods now use integer symbol IDs instead of string symbols.**

#### Methods

```python
market_data = crypto_feeds.PyMarketData()
```

**`get_bid(exchange: str, symbol_id: int) -> Optional[float]`**

Get the bid price for a symbol.

```python
bid = market_data.get_bid("binance", btc_spot_id)
```

**`get_ask(exchange: str, symbol_id: int) -> Optional[float]`**

Get the ask price for a symbol.

**`get_bid_qty(exchange: str, symbol_id: int) -> Optional[float]`**

Get the bid quantity.

**`get_ask_qty(exchange: str, symbol_id: int) -> Optional[float]`**

Get the ask quantity.

**`get_midquote(exchange: str, symbol_id: int) -> Optional[float]`**

Get the midquote price (average of bid and ask).

```python
mid = market_data.get_midquote("coinbase", btc_spot_id)
```

**`get_spread(exchange: str, symbol_id: int) -> Optional[float]`**

Get the spread (ask - bid).

**`get_midquote_mean(symbol_id: int) -> Optional[float]`**

Get the mean midquote across all exchanges (only includes quotes from the last 1 second).

```python
mean = market_data.get_midquote_mean(btc_spot_id)
```

**`get_market_data(exchange: str, symbol_id: int, py: Python) -> Optional[dict]`**

Get a dictionary with all market data fields.

```python
data = market_data.get_market_data("binance", btc_spot_id)
# Returns: {'bid': 96000.0, 'ask': 96001.0, 'bid_qty': 1.5, 'ask_qty': 2.0, 'received_ts': 1735237989000}
```

## Migration Example

### Before (string symbols):

```python
market_data = crypto_feeds.PyMarketData()
bid = market_data.get_bid("binance", "BTCUSDT")
```

### After (integer symbol IDs):

```python
registry = crypto_feeds.PySymbolRegistry()
market_data = crypto_feeds.PyMarketData()

# Look up symbol ID once
btc_spot_id = registry.lookup("BTCUSDT", "spot")

# Use ID for all subsequent calls
bid = market_data.get_bid("binance", btc_spot_id)
ask = market_data.get_ask("binance", btc_spot_id)
mid = market_data.get_midquote("binance", btc_spot_id)
```

## Benefits

1. **Performance**: Integer lookups are faster than string comparisons
2. **Memory**: Reduced memory usage in market data storage
3. **Flexibility**: Multiple symbol formats map to the same ID
4. **Type Safety**: Symbol IDs are validated at lookup time

## Symbol Formats Supported

All these formats map to the same symbol ID:
- `BTCUSDT`
- `BTC-USDT`
- `BTC/USDT`
- `BTC_USDT`

## Complete Example

```python
import crypto_feeds

# Initialize
registry = crypto_feeds.PySymbolRegistry()
manager = crypto_feeds.PyFeedManager()
market_data = manager.get_market_data()

# Look up symbols
btc_spot = registry.lookup("BTCUSDT", "spot")
eth_spot = registry.lookup("ETHUSDT", "spot")
btc_perp = registry.lookup("BTCUSDT", "perp")

# Get market data using IDs
btc_bid = market_data.get_bid("binance", btc_spot)
eth_mid = market_data.get_midquote("coinbase", eth_spot)
btc_perp_mean = market_data.get_midquote_mean(btc_perp)

# Get canonical names
print(registry.get_symbol(btc_spot))  # "SPOT-BTC-USDT"
print(registry.get_symbol(btc_perp))  # "PERP-BTC-USDT"
```
