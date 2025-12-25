#!/usr/bin/env python3
"""
Example showing how to enable Rust logging from Python.
"""

import time
import crypto_feeds

# Enable Rust logging to see connection status, errors, etc.
# This is OFF by default and must be called explicitly
print("Enabling Rust logging at INFO level...")
crypto_feeds.init_logging("info")

# Create config
config = crypto_feeds.PyAppConfig.from_dict({
    "spot": {
        "binance": ["btcusdt"],
    },
    "perp": {
        "binance": ["btcusdt"],
    }
})

# Create manager and start feeds
# With logging enabled, you'll see connection messages like:
# - "Connecting feed binance_spot attempt 1"
# - "Connected to binance_spot"
# - etc.
print("\nStarting feeds (watch for Rust log messages)...")
manager = crypto_feeds.PyFeedManager()
manager.start_spot_feeds(config)
manager.start_perp_feeds(config)

# Get market data accessor
market_data = manager.get_market_data()

# Wait for data
print("Waiting for market data...")
time.sleep(5)

# Access data
bid = market_data.get_bid("binance", "btcusdt")
ask = market_data.get_ask("binance", "btcusdt")
if bid and ask:
    print(f"\nBinance BTCUSDT: Bid={bid:.2f}, Ask={ask:.2f}")
else:
    print("\nNo data received yet")

# Clean shutdown
print("\nShutting down...")
manager.shutdown()
print("Done!")
