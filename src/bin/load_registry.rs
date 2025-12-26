use crypto_feeds::{market_data::InstrumentType, symbol_registry::REGISTRY};

fn main() {
    println!("Loading symbol registry...\n");

    // Count symbols
    let mut count = 0;
    let mut canonical_symbols = Vec::new();

    for id in 0..500 {
        if let Some(symbol) = REGISTRY.get_symbol(id) {
            canonical_symbols.push((id, symbol.to_string()));
            count += 1;
        }
    }

    println!("✓ Loaded {} canonical symbols\n", count);

    // Display all canonical symbols
    println!("Canonical Symbols:");
    println!("{:-<60}", "");
    for (id, symbol) in &canonical_symbols {
        println!("{:3} | {}", id, symbol);
    }
    println!();

    // Test some lookups
    println!("Testing Lookups:");
    println!("{:-<60}", "");

    let test_cases = vec![
        "BTCUSDT",         // Binance spot
        "BTC-USDT",        // Dash format
        "BTC/USDT",        // Kraken format
        "BTC/USD",         // Kraken format
        "BTCUSDT-PERP",    // Binance perp
        "ETH-USDC-PERP",   // Dash perp
        "ETHUSDC",         // Spot
        "SOL/USDC",        // Kraken
        "VIRTUAL-USDT",    // Dash
        "AIXBT_USDC_PERP", // Underscore perp
    ];
    let mut itype = InstrumentType::Spot;

    for test in &test_cases {
        match REGISTRY.lookup(test, &itype) {
            Some(id) => {
                let canonical = REGISTRY.get_symbol(*id).unwrap();
                println!("{:20} -> ID {:3} ({})", test, id, canonical);
            }
            None => {
                println!("{:20} -> NOT FOUND", test);
            }
        }
    }
    itype = InstrumentType::Perp;
    for test in &test_cases {
        match REGISTRY.lookup(test, &itype) {
            Some(id) => {
                let canonical = REGISTRY.get_symbol(*id).unwrap();
                println!("{:20} -> ID {:3} ({})", test, id, canonical);
            }
            None => {
                println!("{:20} -> NOT FOUND", test);
            }
        }
    }
    println!();

    // Show registry statistics
    println!("Registry Statistics:");
    println!("{:-<60}", "");
    println!("Total canonical symbols: {}", count);
    println!("Maximum capacity: 500");
    println!("Utilization: {:.1}%", (count as f64 / 500.0) * 100.0);
}
