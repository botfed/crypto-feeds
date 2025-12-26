#!/usr/bin/env python3
"""
Example usage of crypto_feeds with integer symbol IDs.

This demonstrates the new API using the SymbolRegistry for efficient
symbol lookups with integer IDs.
"""

import crypto_feeds
import time

def main():
    print("=" * 70)
    print("Crypto Feeds - Example Usage with Symbol Registry")
    print("=" * 70)

    # Initialize the symbol registry
    registry = crypto_feeds.PySymbolRegistry()

    # Look up symbol IDs for the assets we're interested in
    # Note: Registry lookup accepts any format (BTCUSDT, BTC-USDT, BTC/USDT, BTC_USDT)
    # But config must use underscore format (BTC_USDT) for exchange mappers
    symbols_to_watch = [
        ("BTC_USDT", "spot", "Bitcoin USDT spot"),
        ("ETH_USDT", "spot", "Ethereum USDT spot"),
        ("BTC_USD", "spot", "Bitcoin USD spot"),
        ("ETH_USD", "spot", "Ethereum USD spot"),
        ("BTC_USDT", "perp", "Bitcoin perpetual"),
        ("ETH_USDT", "perp", "Ethereum perpetual"),
    ]

    print("\n1. Looking up Symbol IDs:")
    print("-" * 70)
    symbol_map = {}
    for symbol, itype, description in symbols_to_watch:
        symbol_id = registry.lookup(symbol, itype)
        symbol_map[(symbol, itype)] = (symbol_id, description)

        canonical = registry.get_symbol(symbol_id) if symbol_id is not None else "N/A"
        print(f"   {description:25} | ID: {symbol_id:3} | Canonical: {canonical}")

    # Create configuration using underscore format (required by mappers)
    config = crypto_feeds.PyAppConfig.from_dict({
        "spot": {
            "binance": ["BTC_USDT", "ETH_USDT"],
            "coinbase": ["BTC_USD", "ETH_USD"],
        },
        "perp": {
            "binance": ["BTC_USDT", "ETH_USDT"],
            "bybit": ["BTC_USDT", "ETH_USDT"],
        }
    })

    # Create feed manager
    manager = crypto_feeds.PyFeedManager()

    print("\n2. Starting Market Data Feeds:")
    print("-" * 70)
    try:
        manager.start_spot_feeds(config)
        print("   ✓ Spot feeds started")
        manager.start_perp_feeds(config)
        print("   ✓ Perp feeds started")
    except Exception as e:
        print(f"   Error starting feeds: {e}")
        return

    # Get market data reference
    market_data = manager.get_market_data()

    print("\n3. Fetching Market Data (using Symbol IDs):")
    print("-" * 70)
    print("   Waiting 3 seconds for data to populate...")
    time.sleep(3)

    exchanges = ["binance", "coinbase", "bybit"]

    # Only check exchanges that are configured for the symbol
    exchange_symbols = {
        "binance": [(sid, desc) for (sym, itype), (sid, desc) in symbol_map.items()
                    if sid is not None and "USDT" in desc],
        "coinbase": [(sid, desc) for (sym, itype), (sid, desc) in symbol_map.items()
                     if sid is not None and "USD" in desc and "USDT" not in desc],
        "bybit": [(sid, desc) for (sym, itype), (sid, desc) in symbol_map.items()
                  if sid is not None and "USDT" in desc and "perp" in desc.lower()],
    }

    for exchange, symbols in exchange_symbols.items():
        if not symbols:
            continue
        print(f"\n   {exchange.upper()}:")
        for symbol_id, description in symbols:
            try:
                # Use symbol_id instead of string symbol
                bid = market_data.get_bid(exchange, symbol_id)
                ask = market_data.get_ask(exchange, symbol_id)
                bid_qty = market_data.get_bid_qty(exchange, symbol_id)
                ask_qty = market_data.get_ask_qty(exchange, symbol_id)
                midquote = market_data.get_midquote(exchange, symbol_id)
                spread = market_data.get_spread(exchange, symbol_id)

                if bid is not None and ask is not None:
                    print(f"      {description:25} | "
                          f"Bid: {bid:>10.2f} ({bid_qty:>10.4f}) | "
                          f"Ask: {ask:>10.2f} ({ask_qty:>10.4f}) | "
                          f"Mid: {midquote:>10.2f} | "
                          f"Spread: {spread:>8.2f}")
            except Exception as e:
                # Exchange may not have this symbol/type combination
                pass

    # Calculate cross-exchange midquote means
    print("\n4. Cross-Exchange Midquote Means:")
    print("-" * 70)
    for (symbol, itype), (symbol_id, description) in symbol_map.items():
        if symbol_id is None:
            continue

        try:
            mean = market_data.get_midquote_mean(symbol_id)
            if mean is not None:
                print(f"   {description:25} (ID: {symbol_id:3}): ${mean:>10.2f}")
        except Exception as e:
            pass

    # Demonstrate getting full market data dict
    print("\n5. Getting Full Market Data Objects:")
    print("-" * 70)
    btc_usdt_spot_id = registry.lookup("BTC_USDT", "spot")
    btc_usd_spot_id = registry.lookup("BTC_USD", "spot")
    if btc_usdt_spot_id is not None:
        try:
            data = market_data.get_market_data("binance", btc_usdt_spot_id)
            if data:
                print(f"   binance BTC/USDT spot: {data}")
        except Exception as e:
            pass
    if btc_usd_spot_id is not None:
        try:
            data = market_data.get_market_data("coinbase", btc_usd_spot_id)
            if data:
                print(f"   coinbase BTC/USD spot: {data}")
        except Exception as e:
            pass

    print("\n6. Shutting Down:")
    print("-" * 70)
    manager.shutdown()
    print("   ✓ Feeds stopped")

    print("\n" + "=" * 70)
    print("Example completed successfully!")
    print("=" * 70)

if __name__ == "__main__":
    # Initialize logging
    crypto_feeds.init_logging("info")

    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
