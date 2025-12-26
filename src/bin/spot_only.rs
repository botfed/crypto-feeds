use crypto_feeds::*;

use anyhow::{Context, Result};
use log::error;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;

use crypto_feeds::app_config::*;
use crypto_feeds::display::print_bbo_data;


#[tokio::main]
async fn main() -> Result<()> {
    // env_logger::init();

    let cfg: AppConfig = load_config("configs/config.yaml").context("loading config.yaml")?;

    let market_data = Arc::new(AllMarketData::new());
    let shutdown = Arc::new(Notify::new());

    let mut handles = Vec::new();

    _ = load_spot(&mut handles, &cfg, &market_data, &shutdown);

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
