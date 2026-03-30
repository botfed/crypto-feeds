use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use log::{error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::exchanges::connection::calculate_backoff;
use crate::market_data::MarketDataCollection;

use super::{PoolEntry, do_multicall, fetch_pool_configs};

struct DexFeed {
    data: Arc<MarketDataCollection>,
    configs: Vec<super::PoolConfig>,
    poll_calls: Vec<super::Multicall3Call>,
}

async fn run_feed(
    aero: Option<&DexFeed>,
    uni: Option<&DexFeed>,
    provider: &impl Provider,
) -> Result<()> {
    // Build combined multicall
    let aero_n = aero.map_or(0, |f| f.poll_calls.len());
    let uni_n = uni.map_or(0, |f| f.poll_calls.len());

    let mut combined = Vec::with_capacity(aero_n + uni_n);
    if let Some(f) = aero {
        combined.extend_from_slice(&f.poll_calls);
    }
    if let Some(f) = uni {
        combined.extend_from_slice(&f.poll_calls);
    }

    let sub = provider.subscribe_blocks().await.context("subscribe_blocks")?;
    let mut stream = sub.into_stream();
    let mut blocks_processed = 0u64;

    while let Some(block) = stream.next().await {
        let block_number = block.inner.number;
        let block_ts = Utc
            .timestamp_opt(block.inner.timestamp as i64, 0)
            .single()
            .unwrap_or_else(Utc::now);
        let received_ts = Utc::now();

        match do_multicall(provider, combined.clone()).await {
            Ok(results) => {
                if let Some(f) = aero {
                    let slice = &results[..aero_n];
                    let states = super::aerodrome::decode_poll_results(
                        slice, &f.configs, block_number, block_ts,
                    );
                    for (state_opt, cfg) in states.iter().zip(f.configs.iter()) {
                        if let (Some(state), Some(id)) = (state_opt, cfg.symbol_id) {
                            f.data.push(&id, state.to_market_data(cfg, received_ts));
                        }
                    }
                }
                if let Some(f) = uni {
                    let slice = &results[aero_n..aero_n + uni_n];
                    let states = super::uniswap::decode_poll_results(
                        slice, &f.configs, block_number, block_ts,
                    );
                    for (state_opt, cfg) in states.iter().zip(f.configs.iter()) {
                        if let (Some(state), Some(id)) = (state_opt, cfg.symbol_id) {
                            f.data.push(&id, state.to_market_data(cfg, received_ts));
                        }
                    }
                }
                blocks_processed += 1;
                if blocks_processed == 1 || blocks_processed % 100 == 0 {
                    info!(
                        "Onchain block {} processed ({} total, {} calls)",
                        block_number, blocks_processed, combined.len()
                    );
                }
            }
            Err(e) => {
                error!("Onchain poll error at block {}: {:#}", block_number, e);
            }
        }
    }

    Err(anyhow::anyhow!("block subscription ended"))
}

pub async fn listen_onchain(
    aero_data: Option<Arc<MarketDataCollection>>,
    aero_pools: Vec<PoolEntry>,
    uni_data: Option<Arc<MarketDataCollection>>,
    uni_pools: Vec<PoolEntry>,
    rpc_url: String,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let mut retry_count = 0u32;

    loop {
        let attempt_start = std::time::Instant::now();

        let result = tokio::select! {
            _ = shutdown.notified() => {
                info!("Onchain feed shutdown");
                return Ok(());
            }
            result = async {
                let ws = WsConnect::new(&rpc_url);
                let provider = ProviderBuilder::new()
                    .connect_ws(ws)
                    .await
                    .context("WS connect failed")?;

                // Init pool configs
                let aero_feed = if let Some(ref data) = aero_data {
                    if !aero_pools.is_empty() {
                        let configs = fetch_pool_configs(&provider, &aero_pools, "Aerodrome")
                            .await
                            .context("fetch Aerodrome pool configs")?;
                        for cfg in &configs {
                            info!(
                                "Aerodrome pool {} ({}) {}/{}  ts={} fee={} dec={}/{} invert={}",
                                cfg.label, cfg.address, cfg.symbol0, cfg.symbol1,
                                cfg.tick_spacing, cfg.fee, cfg.decimals0, cfg.decimals1, cfg.invert_price,
                            );
                        }
                        let poll_calls = super::aerodrome::build_poll_calls(&configs);
                        Some(DexFeed { data: Arc::clone(data), configs, poll_calls })
                    } else { None }
                } else { None };

                let uni_feed = if let Some(ref data) = uni_data {
                    if !uni_pools.is_empty() {
                        let configs = fetch_pool_configs(&provider, &uni_pools, "Uniswap")
                            .await
                            .context("fetch Uniswap pool configs")?;
                        for cfg in &configs {
                            info!(
                                "Uniswap pool {} ({}) {}/{}  ts={} fee={} dec={}/{} invert={}",
                                cfg.label, cfg.address, cfg.symbol0, cfg.symbol1,
                                cfg.tick_spacing, cfg.fee, cfg.decimals0, cfg.decimals1, cfg.invert_price,
                            );
                        }
                        let poll_calls = super::uniswap::build_poll_calls(&configs);
                        Some(DexFeed { data: Arc::clone(data), configs, poll_calls })
                    } else { None }
                } else { None };

                run_feed(aero_feed.as_ref(), uni_feed.as_ref(), &provider).await
            } => result,
        };

        let was_long_lived = attempt_start.elapsed() > Duration::from_secs(60);
        if was_long_lived { retry_count = 0; } else { retry_count += 1; }

        let backoff = calculate_backoff(
            retry_count,
            Duration::from_secs(1),
            Duration::from_secs(60),
        );

        match result {
            Ok(()) => break,
            Err(e) => {
                error!("Onchain feed error: {:#}. Reconnecting in {:?}", e, backoff);
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = shutdown.notified() => {
                        info!("Onchain shutdown during backoff");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
