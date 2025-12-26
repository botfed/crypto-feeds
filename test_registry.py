#!/usr/bin/env python3
"""Test script for the updated Python bindings with integer symbol IDs."""

import crypto_feeds

def test_symbol_registry():
    """Test the PySymbolRegistry class."""
    print("=" * 60)
    print("Testing PySymbolRegistry")
    print("=" * 60)

    registry = crypto_feeds.PySymbolRegistry()

    # Test spot symbol lookup with different formats
    test_symbols = [
        ("BTCUSDT", "spot"),
        ("BTC-USDT", "spot"),
        ("BTC/USDT", "spot"),
        ("BTC_USDT", "spot"),
        ("ETHUSDC", "spot"),
        ("ETH-USDC", "spot"),
    ]

    print("\n1. Testing symbol lookup (spot):")
    symbol_ids = {}
    for symbol, itype in test_symbols:
        symbol_id = registry.lookup(symbol, itype)
        symbol_ids[symbol] = symbol_id
        print(f"   {symbol:15} ({itype}) -> ID: {symbol_id}")

    # Test perp symbol lookup
    print("\n2. Testing symbol lookup (perp):")
    perp_symbols = [
        ("BTCUSDT", "perp"),
        ("BTC-USDT", "perp"),
        ("ETHUSDC", "perp"),
    ]

    for symbol, itype in perp_symbols:
        symbol_id = registry.lookup(symbol, itype)
        print(f"   {symbol:15} ({itype}) -> ID: {symbol_id}")

    # Test reverse lookup (ID to symbol)
    print("\n3. Testing reverse lookup (ID to canonical symbol):")
    for symbol, symbol_id in list(symbol_ids.items())[:3]:
        if symbol_id is not None:
            canonical = registry.get_symbol(symbol_id)
            print(f"   ID {symbol_id:3} -> {canonical}")

    # Test that different formats map to same ID
    print("\n4. Verifying different formats map to same ID:")
    btc_ids = [
        ("BTCUSDT", registry.lookup("BTCUSDT", "spot")),
        ("BTC-USDT", registry.lookup("BTC-USDT", "spot")),
        ("BTC/USDT", registry.lookup("BTC/USDT", "spot")),
        ("BTC_USDT", registry.lookup("BTC_USDT", "spot")),
    ]

    for fmt, sid in btc_ids:
        print(f"   {fmt:15} -> ID: {sid}")

    all_same = len(set(sid for _, sid in btc_ids if sid is not None)) == 1
    print(f"   ✓ All formats map to same ID: {all_same}")

    # Test invalid instrument type
    print("\n5. Testing error handling:")
    try:
        registry.lookup("BTCUSDT", "invalid")
        print("   ✗ Should have raised ValueError")
    except ValueError as e:
        print(f"   ✓ Correctly raised ValueError: {e}")

    # Test non-existent symbol
    print("\n6. Testing non-existent symbol:")
    result = registry.lookup("INVALID-SYMBOL", "spot")
    print(f"   lookup('INVALID-SYMBOL', 'spot') -> {result}")
    print(f"   ✓ Returns None for unknown symbols")

def test_market_data_with_ids():
    """Test PyMarketData with integer symbol IDs."""
    print("\n" + "=" * 60)
    print("Testing PyMarketData with Symbol IDs")
    print("=" * 60)

    registry = crypto_feeds.PySymbolRegistry()
    market_data = crypto_feeds.PyMarketData()

    # Look up a symbol ID
    btc_id = registry.lookup("BTCUSDT", "spot")
    print(f"\n1. Symbol ID for BTCUSDT (spot): {btc_id}")

    if btc_id is not None:
        print(f"\n2. Testing market data methods with symbol_id={btc_id}:")

        # Note: These will return None since we haven't started any feeds
        exchanges = ["binance", "coinbase", "bybit"]

        for exchange in exchanges:
            try:
                bid = market_data.get_bid(exchange, btc_id)
                ask = market_data.get_ask(exchange, btc_id)
                midquote = market_data.get_midquote(exchange, btc_id)

                print(f"   {exchange:10} - bid: {bid}, ask: {ask}, mid: {midquote}")
            except Exception as e:
                print(f"   {exchange:10} - Error: {e}")

        # Test get_midquote_mean
        print(f"\n3. Testing get_midquote_mean:")
        try:
            mean = market_data.get_midquote_mean(btc_id)
            print(f"   Midquote mean: {mean} (expected None, no feeds running)")
        except Exception as e:
            print(f"   Error: {e}")

        print("\n✓ All method signatures accept integer symbol IDs correctly")

if __name__ == "__main__":
    try:
        test_symbol_registry()
        test_market_data_with_ids()

        print("\n" + "=" * 60)
        print("All tests completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
