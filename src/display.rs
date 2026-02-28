use crate::market_data::{AllMarketData, MarketDataCollection};
use crate::symbol_registry::REGISTRY;
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
                if let Ok(binance) = market_data.binance.lock() {
                    print_market_collection("Binance ", &binance);
                }
                if let Ok(coinbase) = market_data.coinbase.lock() {
                    print_market_collection("Coinbase", &coinbase);
                }
                if let Ok(bybit) = market_data.bybit.lock() {
                    print_market_collection("Bybit", &bybit);
                }
                if let Ok(kraken) = market_data.kraken.lock() {
                    print_market_collection("Kraken", &kraken);
                }
                if let Ok(mexc) = market_data.mexc.lock() {
                    print_market_collection("MEXC", &mexc);
                }
                if let Ok(lighter) = market_data.lighter.lock() {
                    print_market_collection("Lighter", &lighter);
                }
            }
        }
    }
}

pub fn print_market_collection(exchange_name: &str, collection: &MarketDataCollection) {
    if collection.data.is_empty() {
        return;
    }
    println!("\n--- {} ---", exchange_name);
    let count = collection.data.len();
    for id in 0..count {
        if let Some(md) = collection.get(&id) {
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