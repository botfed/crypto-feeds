# Crypto Feeds

Real-time BBO (Best Bid/Offer) market data feeds for spot and perpetual futures from multiple exchanges, with Python bindings.

## Supported Exchanges

### Spot Markets
- Binance
- Coinbase
- Bybit
- Kraken
- MEXC

### Perpetual Futures
- Binance
- Bybit
- MEXC
- Lighter

## Building

### Prerequisites
- Rust (latest stable)
- Python 3.8+ (for Python bindings only)
- maturin (for Python bindings only)

### Build Rust Binaries

```bash
# Build and run Rust binaries (without Python dependencies)
cargo build --release
cargo run --bin spot_only
cargo run --bin perp_only
cargo run --bin lighter_only
cargo run --bin spot_perp_arb
```

### Build Python Extension

```bash
# Install maturin
pip install maturin

# Build the wheel (automatically enables the "python" feature)
maturin build --release

# Install in a virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install target/wheels/crypto_feeds-*.whl
```

**Note**: The Python bindings are implemented as an optional feature. Rust binaries compile without PyO3 dependencies, while the Python extension is built with the `python` feature enabled automatically by maturin.

## Python Usage

### Enabling Rust Logging (Optional)

By default, Rust logging is **disabled** for Python users. To see connection logs, errors, and debug information from the Rust layer, you must explicitly enable logging:

```python
import crypto_feeds

# Enable logging with desired level
# Levels: "trace", "debug", "info", "warn", "error"
crypto_feeds.init_logging("info")

# Now proceed with normal usage
```

**Note**: Logging can only be initialized once. Subsequent calls to `init_logging()` will be ignored.

### Basic Example

```python
import time
import crypto_feeds

# Load configuration from YAML file
config = crypto_feeds.PyAppConfig.from_file("configs/config.yaml")

# Create feed manager
manager = crypto_feeds.PyFeedManager()

# Start spot feeds
manager.start_spot_feeds(config)

# Start perp feeds
manager.start_perp_feeds(config)

# Get market data object
market_data = manager.get_market_data()

# Wait for data to arrive
time.sleep(5)

# Get bid/ask for a symbol
bid = market_data.get_bid("binance", "btcusdt")
ask = market_data.get_ask("binance", "btcusdt")
midquote = market_data.get_midquote("binance", "btcusdt")
spread = market_data.get_spread("binance", "btcusdt")

print(f"Binance BTCUSDT - Bid: {bid}, Ask: {ask}, Mid: {midquote}, Spread: {spread}")

# Get all symbols for an exchange
symbols = market_data.get_all_symbols("binance")
print(f"Available symbols on Binance: {symbols}")

# Shutdown when done
manager.shutdown()
```

### Configuration from Dictionary

```python
import crypto_feeds

# Create config from Python dictionary
config_dict = {
    "spot": {
        "binance": ["btcusdt", "ethusdt"],
        "coinbase": ["BTC-USD", "ETH-USD"]
    },
    "perp": {
        "binance": ["btcusdt"],
        "bybit": ["BTCUSDT"]
    }
}

config = crypto_feeds.PyAppConfig.from_dict(config_dict)

# Use config as before
manager = crypto_feeds.PyFeedManager()
manager.start_spot_feeds(config)
manager.start_perp_feeds(config)
```

## API Reference

### Module Functions

- `init_logging(level: str = "info")`: Enable Rust logging. Must be called explicitly to see internal logs. Valid levels: "trace", "debug", "info", "warn", "error". Can only be called once.

### PyAppConfig

- `from_file(path: str) -> PyAppConfig`: Load configuration from YAML file
- `from_dict(dict: dict) -> PyAppConfig`: Create configuration from Python dictionary
- `to_dict() -> dict`: Convert configuration to dictionary

### PyFeedManager

- `__init__()`: Create new feed manager
- `start_spot_feeds(config: PyAppConfig)`: Start spot market feeds
- `start_perp_feeds(config: PyAppConfig)`: Start perpetual futures feeds
- `get_market_data() -> PyMarketData`: Get market data accessor
- `shutdown()`: Shutdown all feeds

### PyMarketData

- `get_bid(exchange: str, symbol: str) -> Optional[float]`: Get best bid price
- `get_ask(exchange: str, symbol: str) -> Optional[float]`: Get best ask price
- `get_bid_qty(exchange: str, symbol: str) -> Optional[float]`: Get best bid quantity
- `get_ask_qty(exchange: str, symbol: str) -> Optional[float]`: Get best ask quantity
- `get_midquote(exchange: str, symbol: str) -> Optional[float]`: Get midpoint price
- `get_spread(exchange: str, symbol: str) -> Optional[float]`: Get bid-ask spread
- `get_all_symbols(exchange: str) -> list[str]`: Get all available symbols for an exchange
- `get_market_data(exchange: str, symbol: str) -> Optional[dict]`: Get full market data as dictionary

## Configuration File Format

Create a YAML file (e.g., `configs/config.yaml`):

```yaml
spot:
  binance: ["btcusdt", "ethusdt", "aixbtusdt"]
  coinbase: ["BTC-USD", "ETH-USD"]
  kraken: ["XBT/USD", "ETH/USD"]
  mexc: ["BTCUSDT", "ETHUSDT"]
  bybit: ["BTCUSDT", "ETHUSDT"]

perp:
  binance: ["btcusdt", "aixbtusdt"]
  bybit: ["BTCUSDT", "AIXBTUSDT"]
  mexc: ["BTC_USDT", "AIXBT_USDT"]
  lighter: ["BTC", "AERO"]
```

## Notes

- Symbol formats vary by exchange (e.g., "btcusdt" for Binance, "BTC-USD" for Coinbase)
- Market data is updated in real-time via WebSocket connections
- Thread-safe access to market data through Arc<Mutex<>> internally
- Returns `None` for symbols with no data yet

## Example Test Script

See `test_feeds.py` for a complete example that demonstrates:
- Loading config from file
- Starting spot and perp feeds
- Accessing market data
- Creating config from dictionary
- Listing all symbols
- Proper shutdown

Run the test:
```bash
source venv/bin/activate
python test_feeds.py
```



## Building crypto-feeds Python Bindings on Ubuntu

### Prerequisites

Install system dependencies:
```bash
sudo apt update
sudo apt install -y build-essential libssl-dev pkg-config protobuf-compiler
```

Install Rust:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

Install uv (Python package manager):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
```

### Installation

Add to your `pyproject.toml`:
```toml
dependencies = [
    "crypto-feeds @ git+https://github.com/botfed/crypto-feeds.git",
]
```

Then sync:
```bash
uv sync
```

### Verification
```bash
uv run python -c "import crypto_feeds; print('OK')"
```