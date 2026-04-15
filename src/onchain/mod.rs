pub mod aerodrome;
pub mod listener;
pub mod uniswap;

use alloy::primitives::{Address, Bytes, U256, address};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use alloy::sol;
use alloy::sol_types::SolCall;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use log::warn;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Write as _;

use crate::market_data::{InstrumentType, MarketData};
use crate::symbol_registry::{REGISTRY, SymbolId};

pub const MULTICALL3: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");

sol! {
    struct Multicall3Call {
        address target;
        bytes callData;
    }

    struct Call3 {
        address target;
        bool allowFailure;
        bytes callData;
    }

    struct McResult {
        bool success;
        bytes returnData;
    }

    function aggregate(Multicall3Call[] calls) external payable returns (uint256 blockNumber, bytes[] returnData);
    function aggregate3(Call3[] calls) external payable returns (McResult[] returnData);

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
pub struct OnchainConfig {
    /// Environment variable name containing the WSS RPC URL
    pub rpc_env: String,
    #[serde(default)]
    pub aerodrome: Option<DexPoolsConfig>,
    #[serde(default)]
    pub uniswap: Option<DexPoolsConfig>,
}

impl OnchainConfig {
    pub fn rpc_url(&self) -> Result<String> {
        let _ = dotenv::dotenv();
        std::env::var(&self.rpc_env)
            .with_context(|| format!("env var '{}' not set", self.rpc_env))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DexPoolsConfig {
    pub pools: Vec<PoolEntry>,
}

impl DexPoolsConfig {
    /// Return only pools whose address parses as a valid EVM address,
    /// logging a warning for each rejected entry.
    pub fn validated_pools(&self, dex_name: &str) -> Vec<PoolEntry> {
        self.pools
            .iter()
            .filter_map(|e| {
                if e.address.parse::<Address>().is_err() {
                    warn!(
                        "{} pool '{}': invalid address '{}', skipping",
                        dex_name, e.symbol, e.address,
                    );
                    return None;
                }
                // Normalize wrapped tokens: CLANKER_WETH → CLANKER_ETH
                let mut entry = e.clone();
                let parts: Vec<&str> = entry.symbol.split('_').collect();
                if parts.len() == 2 {
                    entry.symbol = format!("{}_{}", normalize_token(parts[0]), normalize_token(parts[1]));
                }
                Some(entry)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PoolEntry {
    pub address: String,
    pub symbol: String,
    #[serde(default)]
    pub invert_price: bool,
}

// ---------------------------------------------------------------------------
// Pool config (resolved at startup)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PoolConfig {
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
pub struct PoolState {
    pub sqrt_price_x96: U256,
    pub active_tick: i32,
    pub liquidity: u128,
    pub block_number: u64,
    pub block_ts: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// CL math
// ---------------------------------------------------------------------------

pub fn sqrt_price_x96_to_f64(v: U256) -> f64 {
    let shifted: U256 = v >> 96;
    let int_limbs = shifted.into_limbs();
    let integer_part = int_limbs[0] as f64;

    let frac_mask = (U256::from(1u64) << 96) - U256::from(1u64);
    let frac: U256 = v & frac_mask;
    let frac_limbs = frac.into_limbs();
    let frac_f64 = (frac_limbs[1] as f64) * 2.0_f64.powi(64) + (frac_limbs[0] as f64);
    integer_part + frac_f64 / 2.0_f64.powi(96)
}

pub fn tick_to_sqrt_price(tick: i32) -> f64 {
    1.0001_f64.powf(tick as f64 / 2.0)
}

impl PoolState {
    pub fn to_market_data(&self, cfg: &PoolConfig, received_ts: DateTime<Utc>) -> MarketData {
        let sqrt_p = sqrt_price_x96_to_f64(self.sqrt_price_x96);
        let tick_lower = self.active_tick.div_euclid(cfg.tick_spacing) * cfg.tick_spacing;
        let tick_upper = tick_lower + cfg.tick_spacing;
        let sqrt_p_lower = tick_to_sqrt_price(tick_lower);
        let sqrt_p_upper = tick_to_sqrt_price(tick_upper);
        let l = self.liquidity as f64;

        let decimal_adj = 10_f64.powi(cfg.decimals0 as i32 - cfg.decimals1 as i32);

        let raw_price = sqrt_p * sqrt_p;
        let (price, bid_qty, ask_qty) = if cfg.invert_price {
            let scale = 10_f64.powi(cfg.decimals1 as i32);
            (
                1.0 / (raw_price * decimal_adj),
                l * (sqrt_p_upper - sqrt_p) / scale,
                l * (sqrt_p - sqrt_p_lower) / scale,
            )
        } else {
            let scale = 10_f64.powi(cfg.decimals0 as i32);
            (
                raw_price * decimal_adj,
                l * (1.0 / sqrt_p_lower - 1.0 / sqrt_p) / scale,
                l * (1.0 / sqrt_p - 1.0 / sqrt_p_upper) / scale,
            )
        };

        // Spread bid/ask by full pool fee — the fee is the one-sided trading cost
        let fee_frac = cfg.fee as f64 / 1_000_000.0;

        MarketData {
            bid: Some(price * (1.0 - fee_frac)),
            ask: Some(price * (1.0 + fee_frac)),
            bid_qty: Some(bid_qty),
            ask_qty: Some(ask_qty),
            exchange_ts_raw: Some(self.block_ts),
            exchange_ts: None,
            received_ts: Some(received_ts),
            received_instant: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Multicall helper
// ---------------------------------------------------------------------------

pub async fn do_multicall(provider: &impl Provider, calls: Vec<Multicall3Call>) -> Result<Vec<Bytes>> {
    let calldata = aggregateCall { calls }.abi_encode();
    let tx = TransactionRequest::default()
        .to(MULTICALL3)
        .input(calldata.into());
    let result = provider.call(tx).await.context("multicall eth_call failed")?;
    let decoded = aggregateCall::abi_decode_returns(&result)
        .context("multicall abi decode failed")?;
    Ok(decoded.returnData)
}

/// Like `do_multicall` but uses `aggregate3` with `allowFailure=true`,
/// so a single reverted sub-call does not kill the entire batch.
pub async fn do_try_multicall(provider: &impl Provider, calls: Vec<Call3>) -> Result<Vec<McResult>> {
    let calldata = aggregate3Call { calls }.abi_encode();
    let tx = TransactionRequest::default()
        .to(MULTICALL3)
        .input(calldata.into());
    let result = provider.call(tx).await.context("try_multicall eth_call failed")?;
    let decoded = aggregate3Call::abi_decode_returns(&result)
        .context("try_multicall abi decode failed")?;
    Ok(decoded)
}

// ---------------------------------------------------------------------------
// Token symbol normalisation (WETH → ETH, WBTC/cbBTC → BTC)
// ---------------------------------------------------------------------------

pub fn normalize_token(sym: &str) -> &str {
    match sym {
        "WETH" => "ETH",
        "WBTC" | "cbBTC" => "BTC",
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Init: fetch static pool config via multicall
// ---------------------------------------------------------------------------

pub async fn fetch_pool_configs(
    provider: &impl Provider,
    entries: &[PoolEntry],
    dex_name: &str,
) -> Result<Vec<PoolConfig>> {
    // Entries reaching here should already be validated by validated_pools(),
    // but guard again so fetch_pool_configs is safe to call directly.
    let mut valid: Vec<(usize, Address)> = Vec::new();
    for (i, e) in entries.iter().enumerate() {
        match e.address.parse::<Address>() {
            Ok(addr) => valid.push((i, addr)),
            Err(err) => {
                warn!("{} pool '{}' ({}): invalid address, skipping: {}", dex_name, e.symbol, e.address, err);
            }
        }
    }
    if valid.is_empty() {
        warn!("{}: no valid pool addresses", dex_name);
        return Ok(Vec::new());
    }

    // Step 1: token0, token1, fee, tickSpacing — allow individual failures
    let calls_per_pool = 4;
    let t0_cd: Bytes = token0Call {}.abi_encode().into();
    let t1_cd: Bytes = token1Call {}.abi_encode().into();
    let fee_cd: Bytes = feeCall {}.abi_encode().into();
    let ts_cd: Bytes = tickSpacingCall {}.abi_encode().into();

    let calls: Vec<Call3> = valid
        .iter()
        .flat_map(|&(_, addr)| {
            [&t0_cd, &t1_cd, &fee_cd, &ts_cd].into_iter().map(move |cd| Call3 {
                target: addr,
                allowFailure: true,
                callData: cd.clone(),
            })
        })
        .collect();

    let results = do_try_multicall(provider, calls).await.context("init pool info multicall")?;

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

    let mut resolved: Vec<(usize, Address, RawPoolInfo)> = Vec::new();
    let mut token_set: HashMap<Address, TokenInfo> = HashMap::new();

    for (vi, &(ei, addr)) in valid.iter().enumerate() {
        let off = vi * calls_per_pool;
        let slot = &results[off..off + calls_per_pool];

        // Check all 4 sub-calls succeeded on-chain
        if let Some(pos) = slot.iter().position(|r| !r.success) {
            let call_name = ["token0", "token1", "fee", "tickSpacing"][pos];
            warn!(
                "{} pool '{}' ({}): {}() reverted on-chain, skipping",
                dex_name, entries[ei].symbol, entries[ei].address, call_name,
            );
            continue;
        }

        // Try ABI-decode; a valid address pointing at the wrong contract may
        // return garbage that doesn't decode.
        let decoded = (|| -> Result<RawPoolInfo> {
            Ok(RawPoolInfo {
                token0: token0Call::abi_decode_returns(&slot[0].returnData)?,
                token1: token1Call::abi_decode_returns(&slot[1].returnData)?,
                fee: feeCall::abi_decode_returns(&slot[2].returnData)?.to::<u32>(),
                tick_spacing: tickSpacingCall::abi_decode_returns(&slot[3].returnData)?.as_i32(),
            })
        })();

        match decoded {
            Ok(info) => {
                token_set.entry(info.token0).or_insert(TokenInfo { decimals: 18, symbol: "???".into() });
                token_set.entry(info.token1).or_insert(TokenInfo { decimals: 18, symbol: "???".into() });
                resolved.push((ei, addr, info));
            }
            Err(err) => {
                warn!(
                    "{} pool '{}' ({}): metadata decode failed, skipping: {}",
                    dex_name, entries[ei].symbol, entries[ei].address, err,
                );
            }
        }
    }

    if resolved.is_empty() {
        warn!("{}: no pools resolved successfully", dex_name);
        return Ok(Vec::new());
    }

    // Step 2: fetch decimals + symbol for all unique tokens
    let unique_tokens: Vec<Address> = token_set.keys().copied().collect();
    let dec_cd: Bytes = decimalsCall {}.abi_encode().into();
    let sym_cd: Bytes = symbolCall {}.abi_encode().into();
    let token_calls: Vec<Call3> = unique_tokens
        .iter()
        .flat_map(|&addr| [
            Call3 { target: addr, allowFailure: true, callData: dec_cd.clone() },
            Call3 { target: addr, allowFailure: true, callData: sym_cd.clone() },
        ])
        .collect();

    let token_results = do_try_multicall(provider, token_calls).await.context("init token info multicall")?;

    for (i, &addr) in unique_tokens.iter().enumerate() {
        let off = i * 2;
        let decimals = token_results[off]
            .success
            .then(|| decimalsCall::abi_decode_returns(&token_results[off].returnData).ok())
            .flatten()
            .unwrap_or(18);
        let sym = token_results[off + 1]
            .success
            .then(|| symbolCall::abi_decode_returns(&token_results[off + 1].returnData).ok())
            .flatten()
            .unwrap_or_else(|| "???".into());
        token_set.insert(addr, TokenInfo { decimals, symbol: sym });
    }

    // Log a single summary of skipped pools (if any)
    let skipped = entries.len() - resolved.len();
    if skipped > 0 {
        let mut msg = format!("{}: skipped {}/{} pool(s):", dex_name, skipped, entries.len());
        for (i, e) in entries.iter().enumerate() {
            if !resolved.iter().any(|(ei, _, _)| *ei == i) {
                let _ = write!(msg, " {}", e.symbol);
            }
        }
        warn!("{}", msg);
    }

    let configs = resolved
        .iter()
        .map(|(ei, addr, raw)| {
            let entry = &entries[*ei];
            let t0_info = &token_set[&raw.token0];
            let t1_info = &token_set[&raw.token1];
            let symbol_id = REGISTRY.lookup(&entry.symbol, &InstrumentType::Spot).copied();
            if symbol_id.is_none() {
                warn!(
                    "{} pool {} symbol '{}' not found in registry",
                    dex_name, entry.address, entry.symbol
                );
            }

            // Auto-detect invert_price from on-chain token ordering.
            // sqrtPriceX96² = token1/token0.  We want price = QUOTE/BASE.
            // If token0 matches the quote side, non-inverted gives BASE/QUOTE → need invert.
            let invert_price = {
                let parts: Vec<&str> = entry.symbol.split('_').collect();
                if parts.len() == 2 {
                    let quote_norm = normalize_token(parts[1]);
                    let t0_norm = normalize_token(&t0_info.symbol);
                    let invert = t0_norm.eq_ignore_ascii_case(quote_norm);
                    if !invert && !normalize_token(&t1_info.symbol).eq_ignore_ascii_case(quote_norm) {
                        warn!(
                            "{} pool {} ({}/{}): neither token matches quote '{}', defaulting invert=false",
                            dex_name, entry.address, t0_info.symbol, t1_info.symbol, parts[1]
                        );
                    }
                    invert
                } else {
                    entry.invert_price
                }
            };

            PoolConfig {
                address: *addr,
                token0: raw.token0,
                token1: raw.token1,
                symbol0: t0_info.symbol.clone(),
                symbol1: t1_info.symbol.clone(),
                decimals0: t0_info.decimals,
                decimals1: t1_info.decimals,
                tick_spacing: raw.tick_spacing,
                fee: raw.fee,
                invert_price,
                symbol_id,
                label: entry.symbol.clone(),
            }
        })
        .collect();

    Ok(configs)
}
