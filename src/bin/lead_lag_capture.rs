use anyhow::{Context, Result};
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;
use tokio::time::{self, MissedTickBehavior};

use crypto_feeds::app_config::{load_config, load_perp, load_spot, AppConfig};
use crypto_feeds::market_data::{AllMarketData, Exchange, MarketDataCollection};
use crypto_feeds::symbol_registry::{REGISTRY, SymbolId};
use crypto_feeds::market_data::InstrumentType;

/// One entry in our sampling plan: which exchange + symbol to read each tick.
struct SampleTarget {
    exchange_name: &'static str,
    canonical: String,
    collection: Arc<MarketDataCollection>,
    symbol_id: SymbolId,
}

fn build_targets(cfg: &AppConfig, market_data: &AllMarketData) -> Vec<SampleTarget> {
    let exchanges: &[(&str, Exchange, &Arc<MarketDataCollection>)] = &[
        ("binance", Exchange::Binance, &market_data.binance),
        ("coinbase", Exchange::Coinbase, &market_data.coinbase),
        ("bybit", Exchange::Bybit, &market_data.bybit),
        ("okx", Exchange::Okx, &market_data.okx),
        ("kraken", Exchange::Kraken, &market_data.kraken),
    ];

    let mut targets = Vec::new();

    for &(name, _, ref coll) in exchanges {
        // Spot symbols
        if let Some(syms) = cfg.spot.get(name) {
            for raw in syms {
                if let Some(&id) = REGISTRY.lookup(raw, &InstrumentType::Spot) {
                    let canonical = REGISTRY.get_symbol(id).unwrap_or(raw).to_string();
                    targets.push(SampleTarget {
                        exchange_name: name,
                        canonical,
                        collection: Arc::clone(coll),
                        symbol_id: id,
                    });
                }
            }
        }
        // Perp symbols
        if let Some(syms) = cfg.perp.get(name) {
            for raw in syms {
                if let Some(&id) = REGISTRY.lookup(raw, &InstrumentType::Perp) {
                    let canonical = REGISTRY.get_symbol(id).unwrap_or(raw).to_string();
                    targets.push(SampleTarget {
                        exchange_name: name,
                        canonical,
                        collection: Arc::clone(coll),
                        symbol_id: id,
                    });
                }
            }
        }
    }

    targets
}

fn format_ts(ts: Option<chrono::DateTime<Utc>>) -> String {
    match ts {
        Some(t) => t.timestamp_nanos_opt().map_or(String::new(), |n| n.to_string()),
        None => String::new(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/lead_lag.yaml".to_string());

    let output_path = std::env::args().nth(2).unwrap_or_else(|| {
        let ts = Utc::now().format("%Y%m%d_%H%M%S");
        format!("data/lead_lag_{}.csv.gz", ts)
    });

    // Ensure output directory exists
    if let Some(parent) = std::path::Path::new(&output_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let cfg: AppConfig = load_config(&config_path)
        .with_context(|| format!("loading {}", config_path))?;

    let market_data = Arc::new(AllMarketData::new());
    let shutdown = Arc::new(Notify::new());

    let mut handles = Vec::new();
    load_spot(&mut handles, &cfg, &market_data, &shutdown)?;
    load_perp(&mut handles, &cfg, &market_data, &shutdown)?;

    let sample_ms = cfg.sample_interval_ms;
    let targets = build_targets(&cfg, &market_data);
    eprintln!("Sampling {} targets every {}ms -> {}", targets.len(), sample_ms, output_path);
    for t in &targets {
        eprintln!("  {} / {}", t.exchange_name, t.canonical);
    }

    // Gzipped CSV writer
    let file = std::fs::File::create(&output_path)?;
    let gz = GzEncoder::new(file, Compression::fast());
    let mut wtr = BufWriter::new(gz);

    // Header
    write!(wtr, "sample_ts,canonical_symbol,exchange,bid,ask,bid_qty,ask_qty,exchange_ts,received_ts")?;
    writeln!(wtr)?;

    // Sampling loop
    let shutdown_clone = Arc::clone(&shutdown);
    let sample_handle = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(sample_ms));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let shutdown_fut = shutdown_clone.notified();
        tokio::pin!(shutdown_fut);

        let mut row_count: u64 = 0;

        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = &mut shutdown_fut => {
                    eprintln!("Finalizing gzip CSV ({} rows written)", row_count);
                    let gz = wtr.into_inner().expect("flush BufWriter");
                    let _ = gz.finish();
                    return;
                }
            }

            let sample_ts = Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(0);

            for t in &targets {
                let md = match t.collection.latest(&t.symbol_id) {
                    Some(md) => md,
                    None => continue,
                };

                let bid = md.bid.map_or(String::new(), |v| v.to_string());
                let ask = md.ask.map_or(String::new(), |v| v.to_string());
                let bid_qty = md.bid_qty.map_or(String::new(), |v| v.to_string());
                let ask_qty = md.ask_qty.map_or(String::new(), |v| v.to_string());
                let exchange_ts = format_ts(md.exchange_ts);
                let received_ts = format_ts(md.received_ts);

                let _ = writeln!(
                    wtr,
                    "{},{},{},{},{},{},{},{},{}",
                    sample_ts,
                    t.canonical,
                    t.exchange_name,
                    bid,
                    ask,
                    bid_qty,
                    ask_qty,
                    exchange_ts,
                    received_ts,
                );

                row_count += 1;
            }

            // Flush every 10k rows (~1.5s of data)
            if row_count % 10_000 == 0 {
                let _ = wtr.flush();
            }
        }
    });

    signal::ctrl_c().await?;
    eprintln!("\nShutting down...");
    shutdown.notify_waiters();

    tokio::time::timeout(Duration::from_secs(5), async {
        let _ = sample_handle.await;
        for h in handles {
            let _ = h.await;
        }
    })
    .await?;

    Ok(())
}
