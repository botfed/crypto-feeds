
use anyhow::{Context, Result};
use log::error;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;

use crypto_feeds::app_config::{load_config, load_perp, AppConfig};
use crypto_feeds::mappers::*;
use crypto_feeds::market_data::{AllMarketData, InstrumentType, MarketDataCollection};
use crypto_feeds::symbol_registry::{REGISTRY};

#[tokio::main]
async fn main() -> Result<()> {
    // env_logger::init();

    let cfg: AppConfig = load_config("configs/config.yaml").context("loading config.yaml")?;

    let market_data = Arc::new(AllMarketData::new());
    let shutdown = Arc::new(Notify::new());

    let mut handles = Vec::new();

    // _ = load_spot(&mut handles, &cfg, &market_data, &shutdown);
    _ = load_perp(&mut handles, &cfg, &market_data, &shutdown);

    {
        let md = Arc::clone(&market_data);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = print_bbo_data(md, shutdown).await {
                error!("print bbo data exited with error {:?}", e);
            }
        }));
    }

    signal::ctrl_c().await?;
    shutdown.notify_waiters();
    tokio::time::timeout(Duration::from_secs(5), async {
        for h in handles {
            let _ = h.await;
        }
    })
    .await?;
    Ok(())
}
async fn print_bbo_data(market_data: Arc<AllMarketData>, shutdown: Arc<Notify>) -> Result<()> {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                break Ok(());
            }
            _ = interval.tick() => {
                println!("\n========== Market Data Snapshot ==========");

                if let Ok(binance) = market_data.binance.lock() {
                    print_market_collection("Binance ", &binance, &BinanceMapper);
                }

                if let Ok(coinbase) = market_data.coinbase.lock() {
                    print_market_collection("Coinbase", &coinbase, &CoinbaseMapper);
                }

                if let Ok(bybit) = market_data.bybit.lock() {
                    print_market_collection("Bybit", &bybit, &BybitMapper);
                }

                if let Ok(kraken) = market_data.kraken.lock() {
                    print_market_collection("Kraken", &kraken, &KrakenMapper);
                }

                if let Ok(mexc) = market_data.mexc.lock() {
                    print_market_collection("MEXC", &mexc, &MexcMapper);
                }

                if let Ok(lighter) = market_data.lighter.lock() {
                    print_market_collection("Lighter", &lighter, &LighterMapper);
                }
            }
        }
    }
}

fn print_market_collection(
    exchange_name: &str,
    collection: &MarketDataCollection,
    mapper: &dyn SymbolMapper,
) {
    if collection.data.is_empty() {
        return;
    }
    let itype = InstrumentType::Perp;

    println!("\n--- {} ---", exchange_name);

    // Sort symbols for consistent output
    let count = collection.data.len();

    for id in 0..count {
        if let Some(md) = collection.get(&id) {
            if let Some(mid) = md.midquote() && let Some(normalized) = REGISTRY.get_symbol(id){

                println!(
                    "  {}: ${:.6} | bid: ${:.6} ({:.2}) | ask: ${:.6} ({:.2})",
                    normalized,
                    mid,
                    md.bid.unwrap_or(0.0),
                    md.bid_qty.unwrap_or(0.0),
                    md.ask.unwrap_or(0.0),
                    md.ask_qty.unwrap_or(0.0)
                );
            }
        }
    }
}
