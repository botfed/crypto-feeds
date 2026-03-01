use crate::market_data::{AllMarketData, MarketDataCollection};
use crate::symbol_registry::{MAX_SYMBOLS, REGISTRY};
use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::Notify;

pub async fn print_bbo_data(market_data: Arc<AllMarketData>, shutdown: Arc<Notify>) -> Result<()> {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                break Ok(());
            }
            _ = interval.tick() => {
                println!("\n========== Market Data Snapshot ==========");
                print_market_collection("Binance ", &market_data.binance);
                print_market_collection("Coinbase", &market_data.coinbase);
                print_market_collection("Bybit", &market_data.bybit);
                print_market_collection("Kraken", &market_data.kraken);
                print_market_collection("MEXC", &market_data.mexc);
                print_market_collection("Lighter", &market_data.lighter);
            }
        }
    }
}

pub fn print_market_collection(exchange_name: &str, collection: &MarketDataCollection) {
    println!("\n--- {} ---", exchange_name);
    for id in 0..MAX_SYMBOLS {
        if let Some(md) = collection.latest(&id) {
            if let Some(mid) = md.midquote()
                && let Some(normalized) = REGISTRY.get_symbol(id)
            {
                let now = Utc::now();
                let exch_ts = md
                    .exchange_ts
                    .map(|t| t.format("%H:%M:%S%.3f").to_string())
                    .unwrap_or_else(|| "N/A".into());
                let recv_ts = md
                    .received_ts
                    .map(|t| t.format("%H:%M:%S%.3f").to_string())
                    .unwrap_or_else(|| "N/A".into());
                let exch_lat = md
                    .exchange_ts
                    .map(|t| format!("{}", (now - t).num_milliseconds()))
                    .unwrap_or_else(|| "N/A".into());
                let recv_lat = md
                    .received_ts
                    .map(|t| format!("{}", (now - t).num_milliseconds()))
                    .unwrap_or_else(|| "N/A".into());
                println!(
                    "  {}: ${:.6} | bid: ${:.6} ({:.2}) | ask: ${:.6} ({:.2}) | exch: {} | recv: {} | exch_lat: {}ms | recv_lat: {}ms",
                    normalized,
                    mid,
                    md.bid.unwrap_or(0.0),
                    md.bid_qty.unwrap_or(0.0),
                    md.ask.unwrap_or(0.0),
                    md.ask_qty.unwrap_or(0.0),
                    exch_ts,
                    recv_ts,
                    exch_lat,
                    recv_lat,
                );
            }
        }
    }
}
