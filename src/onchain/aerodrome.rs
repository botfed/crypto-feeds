use alloy::primitives::{Address, Bytes, U256, address};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::TransactionRequest;
use alloy::sol;
use alloy::sol_types::SolCall;
use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::StreamExt;
use log::{error, info, warn};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use crate::exchanges::connection::calculate_backoff;
use crate::market_data::{InstrumentType, MarketData, MarketDataCollection};
use crate::symbol_registry::{REGISTRY, SymbolId};

const MULTICALL3: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");
const CALLS_PER_POOL: usize = 3; // slot0, liquidity, stakedLiquidity

sol! {
    struct Multicall3Call {
        address target;
        bytes callData;
    }

    function aggregate(Multicall3Call[] calls) external payable returns (uint256 blockNumber, bytes[] returnData);

    // CL pool state (per-block)
    function slot0() external view returns (
        uint160 sqrtPriceX96,
        int24 tick,
        uint16 observationIndex,
        uint16 observationCardinality,
        uint16 observationCardinalityNext,
        bool unlocked
    );
    function liquidity() external view returns (uint128);
    function stakedLiquidity() external view returns (uint128);

    // CL pool config (one-time)
    function token0() external view returns (address);
    function token1() external view returns (address);
    function fee() external view returns (uint24);
    function tickSpacing() external view returns (int24);

    // ERC20
    function decimals() external view returns (uint8);
    function symbol() external view returns (string);
}

// ---------------------------------------------------------------------------
// Config (YAML-driven)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct AeroConfig {
    /// Environment variable name containing the WSS RPC URL (e.g. "BASE_RPC_WSS")
    pub rpc_env: String,
    pub pools: Vec<AeroPoolEntry>,
}

impl AeroConfig {
    pub fn rpc_url(&self) -> Result<String> {
        let _ = dotenv::dotenv();
        std::env::var(&self.rpc_env)
            .with_context(|| format!("env var '{}' not set", self.rpc_env))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AeroPoolEntry {
    pub address: String,
    pub symbol: String,
    #[serde(default)]
    pub invert_price: bool,
}

// ---------------------------------------------------------------------------
// Pool config (resolved at startup)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AeroPoolConfig {
    pub address: Address,
    pub token0: Address,
    pub token1: Address,
    pub symbol0: String,
    pub symbol1: String,
    pub decimals0: u8,
    pub decimals1: u8,
    pub tick_spacing: i32,
    pub fee: u32,
    pub invert_price: bool,
    pub symbol_id: Option<SymbolId>,
    pub label: String,
}

// ---------------------------------------------------------------------------
// Pool state (per-block)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AeroPoolState {
    pub sqrt_price_x96: U256,
    pub active_tick: i32,
    pub liquidity: u128,
    pub staked_liquidity: u128,
    pub block_number: u64,
    pub block_ts: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// CL math
// ---------------------------------------------------------------------------

fn sqrt_price_x96_to_f64(v: U256) -> f64 {
    // sqrtPrice = sqrtPriceX96 / 2^96
    // Split into integer + fraction for maximum f64 precision.
    let shifted: U256 = v >> 96;
    let int_limbs = shifted.into_limbs();
    let integer_part = int_limbs[0] as f64;

    let frac_mask = (U256::from(1u64) << 96) - U256::from(1u64);
    let frac: U256 = v & frac_mask;
    let frac_limbs = frac.into_limbs();
    let frac_f64 = (frac_limbs[1] as f64) * 2.0_f64.powi(64) + (frac_limbs[0] as f64);
    integer_part + frac_f64 / 2.0_f64.powi(96)
}

fn tick_to_sqrt_price(tick: i32) -> f64 {
    1.0001_f64.powf(tick as f64 / 2.0)
}

impl AeroPoolState {
    pub fn to_market_data(&self, cfg: &AeroPoolConfig, received_ts: DateTime<Utc>) -> MarketData {
        let sqrt_p = sqrt_price_x96_to_f64(self.sqrt_price_x96);
        let tick_lower = self.active_tick.div_euclid(cfg.tick_spacing) * cfg.tick_spacing;
        let tick_upper = tick_lower + cfg.tick_spacing;
        let sqrt_p_lower = tick_to_sqrt_price(tick_lower);
        let sqrt_p_upper = tick_to_sqrt_price(tick_upper);
        let l = self.liquidity as f64;

        // On-chain price is in token1_wei / token0_wei.  Multiply by
        // 10^(decimals0 - decimals1) to get human-readable token1 / token0.
        let decimal_adj = 10_f64.powi(cfg.decimals0 as i32 - cfg.decimals1 as i32);

        // True pool price from sqrtPriceX96
        let raw_price = sqrt_p * sqrt_p; // token1_wei per token0_wei
        let (price, bid_qty, ask_qty) = if cfg.invert_price {
            // Price = token0/token1, quantities in token1
            let scale = 10_f64.powi(cfg.decimals1 as i32);
            (
                1.0 / (raw_price * decimal_adj),
                l * (sqrt_p_upper - sqrt_p) / scale,
                l * (sqrt_p - sqrt_p_lower) / scale,
            )
        } else {
            // Price = token1/token0, quantities in token0
            let scale = 10_f64.powi(cfg.decimals0 as i32);
            (
                raw_price * decimal_adj,
                l * (1.0 / sqrt_p_lower - 1.0 / sqrt_p) / scale,
                l * (1.0 / sqrt_p - 1.0 / sqrt_p_upper) / scale,
            )
        };

        MarketData {
            bid: Some(price),
            ask: Some(price),
            bid_qty: Some(bid_qty),
            ask_qty: Some(ask_qty),
            exchange_ts: Some(self.block_ts),
            received_ts: Some(received_ts),
        }
    }
}

// ---------------------------------------------------------------------------
// Multicall helpers
// ---------------------------------------------------------------------------

async fn do_multicall(provider: &impl Provider, calls: Vec<Multicall3Call>) -> Result<Vec<Bytes>> {
    let calldata = aggregateCall { calls }.abi_encode();
    let tx = TransactionRequest::default()
        .to(MULTICALL3)
        .input(calldata.into());
    let result = provider.call(tx).await.context("multicall eth_call failed")?;
    let decoded = aggregateCall::abi_decode_returns(&result)
        .context("multicall abi decode failed")?;
    Ok(decoded.returnData)
}

// ---------------------------------------------------------------------------
// Init: fetch static pool config via multicall
// ---------------------------------------------------------------------------

async fn fetch_pool_configs(
    provider: &impl Provider,
    entries: &[AeroPoolEntry],
) -> Result<Vec<AeroPoolConfig>> {
    let pool_addrs: Vec<Address> = entries
        .iter()
        .map(|e| e.address.parse::<Address>().context("invalid pool address"))
        .collect::<Result<_>>()?;

    // Step 1: token0, token1, fee, tickSpacing for each pool
    let config_calls_per_pool = 4;
    let mut calls = Vec::with_capacity(entries.len() * config_calls_per_pool);
    let t0_cd: Bytes = token0Call {}.abi_encode().into();
    let t1_cd: Bytes = token1Call {}.abi_encode().into();
    let fee_cd: Bytes = feeCall {}.abi_encode().into();
    let ts_cd: Bytes = tickSpacingCall {}.abi_encode().into();

    for &addr in &pool_addrs {
        for cd in [&t0_cd, &t1_cd, &fee_cd, &ts_cd] {
            calls.push(Multicall3Call { target: addr, callData: cd.clone() });
        }
    }

    let results = do_multicall(provider, calls).await.context("init pool info multicall")?;

    struct RawPoolInfo {
        token0: Address,
        token1: Address,
        fee: u32,
        tick_spacing: i32,
    }

    #[derive(Clone)]
    struct TokenInfo {
        decimals: u8,
        symbol: String,
    }

    let mut raw_infos = Vec::with_capacity(entries.len());
    let mut token_set: HashMap<Address, TokenInfo> = HashMap::new();

    for i in 0..entries.len() {
        let off = i * config_calls_per_pool;
        let t0: Address = token0Call::abi_decode_returns(&results[off])?;
        let t1: Address = token1Call::abi_decode_returns(&results[off + 1])?;
        let f = feeCall::abi_decode_returns(&results[off + 2])?;
        let ts = tickSpacingCall::abi_decode_returns(&results[off + 3])?;

        token_set.entry(t0).or_insert(TokenInfo { decimals: 18, symbol: "???".into() });
        token_set.entry(t1).or_insert(TokenInfo { decimals: 18, symbol: "???".into() });
        raw_infos.push(RawPoolInfo {
            token0: t0,
            token1: t1,
            fee: f.to::<u32>(),
            tick_spacing: ts.as_i32(),
        });
    }

    // Step 2: fetch decimals + symbol for all unique tokens (2 calls per token)
    let unique_tokens: Vec<Address> = token_set.keys().copied().collect();
    let dec_cd: Bytes = decimalsCall {}.abi_encode().into();
    let sym_cd: Bytes = symbolCall {}.abi_encode().into();
    let token_calls: Vec<Multicall3Call> = unique_tokens
        .iter()
        .flat_map(|&addr| [
            Multicall3Call { target: addr, callData: dec_cd.clone() },
            Multicall3Call { target: addr, callData: sym_cd.clone() },
        ])
        .collect();

    let token_results = do_multicall(provider, token_calls).await.context("init token info multicall")?;

    for (i, &addr) in unique_tokens.iter().enumerate() {
        let off = i * 2;
        let decimals = decimalsCall::abi_decode_returns(&token_results[off]).unwrap_or(18);
        let sym = symbolCall::abi_decode_returns(&token_results[off + 1])
            .unwrap_or_else(|_| "???".into());
        token_set.insert(addr, TokenInfo { decimals, symbol: sym });
    }

    // Build configs
    let configs = entries
        .iter()
        .zip(raw_infos.iter())
        .zip(pool_addrs.iter())
        .map(|((entry, raw), &addr)| {
            let t0_info = &token_set[&raw.token0];
            let t1_info = &token_set[&raw.token1];
            let symbol_id = REGISTRY.lookup(&entry.symbol, &InstrumentType::Spot).copied();
            if symbol_id.is_none() {
                warn!(
                    "Aerodrome pool {} symbol '{}' not found in registry",
                    entry.address, entry.symbol
                );
            }
            AeroPoolConfig {
                address: addr,
                token0: raw.token0,
                token1: raw.token1,
                symbol0: t0_info.symbol.clone(),
                symbol1: t1_info.symbol.clone(),
                decimals0: t0_info.decimals,
                decimals1: t1_info.decimals,
                tick_spacing: raw.tick_spacing,
                fee: raw.fee,
                invert_price: entry.invert_price,
                symbol_id,
                label: entry.symbol.clone(),
            }
        })
        .collect();

    Ok(configs)
}

// ---------------------------------------------------------------------------
// Per-block poll
// ---------------------------------------------------------------------------

fn build_poll_calls(configs: &[AeroPoolConfig]) -> Vec<Multicall3Call> {
    let s0_cd: Bytes = slot0Call {}.abi_encode().into();
    let liq_cd: Bytes = liquidityCall {}.abi_encode().into();
    let sliq_cd: Bytes = stakedLiquidityCall {}.abi_encode().into();

    let mut calls = Vec::with_capacity(configs.len() * CALLS_PER_POOL);
    for cfg in configs {
        for cd in [&s0_cd, &liq_cd, &sliq_cd] {
            calls.push(Multicall3Call { target: cfg.address, callData: cd.clone() });
        }
    }
    calls
}

fn decode_poll_results(
    results: &[Bytes],
    configs: &[AeroPoolConfig],
    block_number: u64,
    block_ts: DateTime<Utc>,
) -> Vec<Option<AeroPoolState>> {
    configs
        .iter()
        .enumerate()
        .map(|(i, _cfg)| {
            let off = i * CALLS_PER_POOL;
            let s0 = slot0Call::abi_decode_returns(&results[off]).ok()?;
            let liq: u128 = liquidityCall::abi_decode_returns(&results[off + 1]).ok()?;
            let sliq: u128 = stakedLiquidityCall::abi_decode_returns(&results[off + 2]).ok()?;
            // Widen U160 → U256 via limbs
            let u160_limbs = s0.sqrtPriceX96.into_limbs();
            let sqrt_price_x96 = U256::from_limbs([u160_limbs[0], u160_limbs[1], u160_limbs[2], 0]);
            Some(AeroPoolState {
                sqrt_price_x96,
                active_tick: s0.tick.as_i32(),
                liquidity: liq,
                staked_liquidity: sliq,
                block_number,
                block_ts,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Main feed loop
// ---------------------------------------------------------------------------

async fn run_feed(
    data: &Arc<MarketDataCollection>,
    config: &AeroConfig,
) -> Result<()> {
    let rpc_url = config.rpc_url()?;
    let ws = WsConnect::new(&rpc_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("WS connect failed")?;

    let pool_configs = fetch_pool_configs(&provider, &config.pools)
        .await
        .context("fetch pool configs")?;

    for cfg in &pool_configs {
        info!(
            "Aerodrome pool {} ({}) {}/{}  ts={} fee={} dec={}/{} invert={}",
            cfg.label, cfg.address, cfg.symbol0, cfg.symbol1,
            cfg.tick_spacing, cfg.fee, cfg.decimals0, cfg.decimals1, cfg.invert_price,
        );
    }

    let poll_calls = build_poll_calls(&pool_configs);

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
                        "Aerodrome block {} processed ({} total)",
                        block_number, blocks_processed
                    );
                }
            }
            Err(e) => {
                error!("Aerodrome poll error at block {}: {}", block_number, e);
            }
        }
    }

    Err(anyhow::anyhow!("block subscription ended"))
}

pub async fn listen_aerodrome(
    data: Arc<MarketDataCollection>,
    config: AeroConfig,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let mut retry_count = 0u32;

    loop {
        let attempt_start = std::time::Instant::now();

        tokio::select! {
            _ = shutdown.notified() => {
                info!("Aerodrome feed shutdown");
                break;
            }
            result = run_feed(&data, &config) => {
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
                        error!("Aerodrome feed error: {}. Reconnecting in {:?}", e, backoff);
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = shutdown.notified() => {
                                info!("Aerodrome shutdown during backoff");
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
