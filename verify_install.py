#!/usr/bin/env python3
"""Quick verification that PySymbolRegistry is installed correctly."""

import crypto_feeds

print("Testing PySymbolRegistry installation...")
print("-" * 50)

# Create registry
registry = crypto_feeds.PySymbolRegistry()
print("✓ PySymbolRegistry created successfully")

# Test lookup
btc_id = registry.lookup("BTCUSDT", "spot")
print(f"✓ lookup('BTCUSDT', 'spot') = {btc_id}")

# Test reverse lookup
canonical = registry.get_symbol(btc_id)
print(f"✓ get_symbol({btc_id}) = {canonical}")

# Test with market data
market_data = crypto_feeds.PyMarketData()
print("✓ PyMarketData created successfully")

# Verify method signature accepts symbol_id (int)
result = market_data.get_bid("binance", btc_id)
print(f"✓ get_bid('binance', {btc_id}) = {result}")

print("-" * 50)
print("All checks passed! PySymbolRegistry is working correctly.")
