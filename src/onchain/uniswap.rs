use alloy::primitives::{Bytes, U256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::sol;
use alloy::sol_types::SolCall;
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use log::{error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::exchanges::connection::calculate_backoff;
use crate::market_data::MarketDataCollection;

use super::{Multicall3Call, PoolConfig, PoolEntry, PoolState, do_multicall, fetch_pool_configs};

const CALLS_PER_POOL: usize = 2; // slot0, liquidity

sol! {
    // Uniswap V3 slot0
    function slot0() external view returns (
        uint160 sqrtPriceX96,
        int24 tick,
        uint16 observationIndex,
        uint16 observationCardinality,
        uint16 observationCardinalityNext,
        uint8 feeProtocol,
        bool unlocked
    );
    function liquidity() external view returns (uint128);
}

fn build_poll_calls(configs: &[PoolConfig]) -> Vec<Multicall3Call> {
    let s0_cd: Bytes = slot0Call {}.abi_encode().into();
    let liq_cd: Bytes = liquidityCall {}.abi_encode().into();

    let mut calls = Vec::with_capacity(configs.len() * CALLS_PER_POOL);
    for cfg in configs {
        for cd in [&s0_cd, &liq_cd] {
            calls.push(Multicall3Call { target: cfg.address, callData: cd.clone() });
        }
    }
    calls
}

fn decode_poll_results(
    results: &[Bytes],
    configs: &[PoolConfig],
    block_number: u64,
    block_ts: chrono::DateTime<Utc>,
) -> Vec<Option<PoolState>> {
    configs
        .iter()
        .enumerate()
        .map(|(i, cfg)| {
            let off = i * CALLS_PER_POOL;

            let s0 = match slot0Call::abi_decode_returns(&results[off]) {
                Ok(v) => v,
                Err(e) => {
                    error!(
                        "Uniswap pool {} slot0 decode failed (data len={}): {}",
                        cfg.label,
                        results[off].len(),
                        e
                    );
                    return None;
                }
            };

            let liq: u128 = match liquidityCall::abi_decode_returns(&results[off + 1]) {
                Ok(v) => v,
                Err(e) => {
                    error!("Uniswap pool {} liquidity decode failed: {}", cfg.label, e);
                    return None;
                }
            };

            let u160_limbs = s0.sqrtPriceX96.into_limbs();
            let sqrt_price_x96 = U256::from_limbs([u160_limbs[0], u160_limbs[1], u160_limbs[2], 0]);
            Some(PoolState {
                sqrt_price_x96,
                active_tick: s0.tick.as_i32(),
                liquidity: liq,
                block_number,
                block_ts,
            })
        })
        .collect()
}

async fn run_feed(
    data: &Arc<MarketDataCollection>,
    rpc_url: &str,
    pools: &[PoolEntry],
) -> Result<()> {
    let ws = WsConnect::new(rpc_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("WS connect failed")?;

    let pool_configs = fetch_pool_configs(&provider, pools, "Uniswap")
        .await
        .context("fetch pool configs")?;

    for cfg in &pool_configs {
        info!(
            "Uniswap pool {} ({}) {}/{}  ts={} fee={} dec={}/{} invert={}",
            cfg.label, cfg.address, cfg.symbol0, cfg.symbol1,
            cfg.tick_spacing, cfg.fee, cfg.decimals0, cfg.decimals1, cfg.invert_price,
        );
    }

    let poll_calls = build_poll_calls(&pool_configs);

    // Test poll to surface decode issues immediately
    let test_results = do_multicall(&provider, poll_calls.clone())
        .await
        .context("initial test poll failed")?;
    let test_states = decode_poll_results(&test_results, &pool_configs, 0, Utc::now());
    for (state_opt, cfg) in test_states.iter().zip(pool_configs.iter()) {
        match state_opt {
            Some(state) => info!(
                "Uniswap {} test poll OK: tick={} liq={}",
                cfg.label, state.active_tick, state.liquidity
            ),
            None => error!("Uniswap {} test poll FAILED — check slot0 ABI", cfg.label),
        }
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

        match do_multicall(&provider, poll_calls.clone()).await {
            Ok(results) => {
                let states = decode_poll_results(&results, &pool_configs, block_number, block_ts);
                for (state_opt, cfg) in states.iter().zip(pool_configs.iter()) {
                    if let (Some(state), Some(id)) = (state_opt, cfg.symbol_id) {
                        let md = state.to_market_data(cfg, received_ts);
                        data.push(&id, md);
                    }
                }
                blocks_processed += 1;
                if blocks_processed == 1 || blocks_processed % 100 == 0 {
                    info!(
                        "Uniswap block {} processed ({} total)",
                        block_number, blocks_processed
                    );
                }
            }
            Err(e) => {
                error!("Uniswap poll error at block {}: {}", block_number, e);
            }
        }
    }

    Err(anyhow::anyhow!("block subscription ended"))
}

pub async fn listen_uniswap(
    data: Arc<MarketDataCollection>,
    rpc_url: String,
    pools: Vec<PoolEntry>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let mut retry_count = 0u32;

    loop {
        let attempt_start = std::time::Instant::now();

        tokio::select! {
            _ = shutdown.notified() => {
                info!("Uniswap feed shutdown");
                break;
            }
            result = run_feed(&data, &rpc_url, &pools) => {
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
                        error!("Uniswap feed error: {}. Reconnecting in {:?}", e, backoff);
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = shutdown.notified() => {
                                info!("Uniswap shutdown during backoff");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
