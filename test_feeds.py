#!/usr/bin/env python3
"""
Test script for exchange feeds Python bindings.
Demonstrates how to use the BBO spot and perp feeds from Python.
"""

import time
import crypto_feeds

# Method 1: Load config from YAML file
print("=" * 60)
print("Method 1: Loading config from file")
print("=" * 60)

config = crypto_feeds.PyAppConfig.from_file("configs/config.yaml")
print("Config loaded from file:")
print(config.to_dict())
print()

# Create feed manager
manager = crypto_feeds.PyFeedManager()

# Start spot feeds
print("Starting spot feeds...")
manager.start_spot_feeds(config)

# Start perp feeds
print("Starting perp feeds...")
manager.start_perp_feeds(config)

# Get market data object
market_data = manager.get_market_data()

# Wait for data to arrive
print("\nWaiting for market data (10 seconds)...")
time.sleep(10)

print("\n" + "=" * 60)
print("Spot Market Data Examples")
print("=" * 60)

# Check Binance spot BTC
exchanges = ["binance", "coinbase", "bybit", "kraken", "mexc"]
symbols_to_check = {
    "binance": ["btcusdt", "ethusdt"],
    "coinbase": ["BTC-USD", "ETH-USD"],
    "bybit": ["BTCUSDT", "ETHUSDT"],
    "kraken": ["XBT/USD", "ETH/USD"],
    "mexc": ["BTCUSDT", "ETHUSDT"]
}

for exchange in exchanges:
    print(f"\n{exchange.upper()} Spot:")
    for symbol in symbols_to_check.get(exchange, []):
        bid = market_data.get_bid(exchange, symbol)
        ask = market_data.get_ask(exchange, symbol)
        midquote = market_data.get_midquote(exchange, symbol)
        spread = market_data.get_spread(exchange, symbol)

        if bid is not None and ask is not None:
            print(f"  {symbol}:")
            print(f"    Bid: {bid:.6f}")
            print(f"    Ask: {ask:.6f}")
            print(f"    Mid: {midquote:.6f}")
            print(f"    Spread: {spread:.4f}")

print("\n" + "=" * 60)
print("Perp Market Data Examples")
print("=" * 60)

perp_symbols = {
    "binance": ["btcusdt", "aixbtusdt"],
    "bybit": ["BTCUSDT", "AIXBTUSDT"],
    "mexc": ["BTC_USDT", "AIXBT_USDT"],
    "lighter": ["BTC", "AERO"]
}

for exchange in ["binance", "bybit", "mexc", "lighter"]:
    print(f"\n{exchange.upper()} Perp:")
    for symbol in perp_symbols.get(exchange, []):
        bid = market_data.get_bid(exchange, symbol)
        ask = market_data.get_ask(exchange, symbol)
        midquote = market_data.get_midquote(exchange, symbol)

        if bid is not None and ask is not None:
            print(f"  {symbol}:")
            print(f"    Bid: {bid:.6f}")
            print(f"    Ask: {ask:.6f}")
            print(f"    Mid: {midquote:.6f}")

print("\n" + "=" * 60)
print("Method 2: Creating config from dictionary")
print("=" * 60)

# Method 2: Create config from dictionary
custom_config_dict = {
    "spot": {
        "binance": ["btcusdt", "ethusdt"],
        "coinbase": ["BTC-USD", "ETH-USD"]
    },
    "perp": {
        "binance": ["btcusdt"],
        "bybit": ["BTCUSDT"]
    }
}

custom_config = crypto_feeds.PyAppConfig.from_dict(custom_config_dict)
print("Custom config created from dict:")
print(custom_config.to_dict())

print("\n" + "=" * 60)
print("Getting all available symbols")
print("=" * 60)

for exchange in exchanges:
    symbols = market_data.get_all_symbols(exchange)
    if symbols:
        print(f"{exchange}: {', '.join(symbols)}")

print("\n" + "=" * 60)
print("Full market data for a symbol")
print("=" * 60)

full_data = market_data.get_market_data("binance", "btcusdt")
if full_data:
    print("Binance BTCUSDT full market data:")
    print(f"  {full_data}")

print("\n" + "=" * 60)
print("Test complete!")
print("=" * 60)

# Shutdown feeds
manager.shutdown()
