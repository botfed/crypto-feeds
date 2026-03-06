use anyhow::{Context, Result};
use log::error;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;

use crypto_feeds::analytics::Analytics;
use crypto_feeds::app_config::{load_aerodrome, load_config, load_perp, load_spot, AppConfig};
use crypto_feeds::display::{init_display_logger, print_bbo_with_analytics};
use crypto_feeds::market_data::AllMarketData;
use crypto_feeds::snapshot::{run_snapshot_task, AllSnapshotData, SnapshotConfig};

#[tokio::main]
async fn main() -> Result<()> {
    init_display_logger(log::LevelFilter::Info);
    let cfg: AppConfig = load_config("configs/config.yaml").context("loading config.yaml")?;

    let market_data = Arc::new(AllMarketData::new());
    let shutdown = Arc::new(Notify::new());

    let mut handles = Vec::new();

    // Start spot, perp, and on-chain feeds
    let _ = load_spot(&mut handles, &cfg, &market_data, &shutdown);
    let _ = load_perp(&mut handles, &cfg, &market_data, &shutdown);
    let _ = load_aerodrome(&mut handles, &cfg, &market_data, &shutdown);

    // Start snapshot engine (100ms interval, 65536 buffer ≈ 109 min at 100ms)
    // Must be >= 36_000 to support 1-hour analytics (fills/hr, median spread, etc.)
    let snap_data = Arc::new(AllSnapshotData::new(65536));
    let snap_config = SnapshotConfig {
        interval_ms: 100,
        buffer_capacity: 65536,
    };
    {
        let tick = Arc::clone(&market_data);
        let snap = Arc::clone(&snap_data);
        let sd = Arc::clone(&shutdown);
        handles.push(tokio::spawn(run_snapshot_task(tick, snap, snap_config, sd)));
    }

    // Create analytics and start display
    let analytics = Arc::new(Analytics::new(
        Arc::clone(&market_data),
        Arc::clone(&snap_data),
    ));
    {
        let md = Arc::clone(&market_data);
        let a = Arc::clone(&analytics);
        let sd = Arc::clone(&shutdown);
        handles.push(tokio::spawn(async move {
            if let Err(e) = print_bbo_with_analytics(md, a, sd).await {
                error!("display exited with error {:?}", e);
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
