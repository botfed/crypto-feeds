#!/usr/bin/env python3
"""
Simple demonstration of the PySymbolRegistry without requiring live feeds.

This shows how to:
1. Look up symbol IDs using various formats
2. Get canonical symbol names from IDs
3. Use symbol IDs with market data methods
"""

import crypto_feeds

print("=" * 70)
print("PySymbolRegistry - Simple Demo")
print("=" * 70)

# Create the registry
registry = crypto_feeds.PySymbolRegistry()

print("\n1. Looking up symbol IDs with different formats:")
print("-" * 70)

# All these formats map to the same ID
formats = ["BTCUSDT", "BTC-USDT", "BTC/USDT", "BTC_USDT"]
for fmt in formats:
    symbol_id = registry.lookup(fmt, "spot")
    canonical = registry.get_symbol(symbol_id)
    print(f"   {fmt:12} -> ID: {symbol_id:3} -> Canonical: {canonical}")

print("\n2. Looking up different quote currencies:")
print("-" * 70)

symbols = [
    ("BTC_USDT", "spot"),
    ("BTC_USDC", "spot"),
    ("BTC_USD", "spot"),
    ("ETH_USDT", "perp"),
    ("SOL_USDT", "spot"),
]

for symbol, itype in symbols:
    symbol_id = registry.lookup(symbol, itype)
    if symbol_id is not None:
        canonical = registry.get_symbol(symbol_id)
        print(f"   {symbol:12} ({itype:4}) -> ID: {symbol_id:3} -> {canonical}")
    else:
        print(f"   {symbol:12} ({itype:4}) -> Not found in registry")

print("\n3. Using symbol IDs with market data methods:")
print("-" * 70)

# Create market data instance
market_data = crypto_feeds.PyMarketData()

# Look up a symbol ID
btc_id = registry.lookup("BTC_USDT", "spot")
print(f"   BTC_USDT spot ID: {btc_id}")

# These methods now accept integer symbol IDs
print(f"\n   Using symbol ID {btc_id} with market data methods:")
print(f"   - get_bid('binance', {btc_id}): {market_data.get_bid('binance', btc_id)}")
print(f"   - get_ask('binance', {btc_id}): {market_data.get_ask('binance', btc_id)}")
print(f"   - get_midquote('binance', {btc_id}): {market_data.get_midquote('binance', btc_id)}")
print(f"   - get_midquote_mean({btc_id}): {market_data.get_midquote_mean(btc_id)}")
print("\n   (All return None since no feeds are running)")

print("\n4. Error handling:")
print("-" * 70)

# Invalid instrument type
try:
    registry.lookup("BTC_USDT", "invalid")
except ValueError as e:
    print(f"   ✓ Invalid instrument type: {e}")

# Symbol not in registry
result = registry.lookup("NOTREAL_USDT", "spot")
print(f"   ✓ Non-existent symbol returns: {result}")

# Invalid symbol ID
result = registry.get_symbol(9999)
print(f"   ✓ Invalid ID returns: {result}")

print("\n" + "=" * 70)
print("Demo completed successfully!")
print("=" * 70)
print("\nKey Takeaways:")
print("• Registry accepts multiple formats: BTCUSDT, BTC-USDT, BTC/USDT, BTC_USDT")
print("• All formats map to the same integer ID")
print("• Use integer IDs with all PyMarketData methods for efficiency")
print("• Canonical format is SPOT-BTC-USDT or PERP-BTC-USDT")
print("=" * 70)
