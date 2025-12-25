#!/usr/bin/env python3
"""
Simple example of using exchange feeds from Python.
"""

import time
import crypto_feeds

# Optional: Enable Rust logging (disabled by default)
# Levels: "trace", "debug", "info", "warn", "error"
# crypto_feeds.init_logging("info")

# Create config from dictionary (or use from_file for YAML)
config = crypto_feeds.PyAppConfig.from_dict({
    "spot": {
        "binance": ["btcusdt", "ethusdt"],
        "coinbase": ["BTC-USD", "ETH-USD"]
    },
    "perp": {
        "binance": ["btcusdt"],
        "bybit": ["BTCUSDT"]
    }
})

# Create manager and start feeds
manager = crypto_feeds.PyFeedManager()
manager.start_spot_feeds(config)
manager.start_perp_feeds(config)

# Get market data accessor
market_data = manager.get_market_data()

# Wait for data to arrive
print("Waiting for market data...")
time.sleep(5)

# Access market data
print("\nSpot Markets:")
for exchange in ["binance", "coinbase"]:
    for symbol in market_data.get_all_symbols(exchange):
        bid = market_data.get_bid(exchange, symbol)
        ask = market_data.get_ask(exchange, symbol)
        if bid and ask:
            mid = market_data.get_midquote(exchange, symbol)
            print(f"{exchange.upper()} {symbol}: Mid={mid:.2f} Spread={ask-bid:.4f}")

print("\nPerp Markets:")
for exchange in ["binance", "bybit"]:
    for symbol in market_data.get_all_symbols(exchange):
        mid = market_data.get_midquote(exchange, symbol)
        if mid:
            print(f"{exchange.upper()} {symbol}: Mid={mid:.2f}")

# Clean shutdown
manager.shutdown()
