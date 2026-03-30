use alloy::primitives::{Bytes, U256};
use alloy::sol;
use alloy::sol_types::SolCall;
use chrono::Utc;

use super::{Multicall3Call, PoolConfig, PoolState};

const CALLS_PER_POOL: usize = 3; // slot0, liquidity, stakedLiquidity

sol! {
    // Aerodrome CL pool slot0 (no feeProtocol field)
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
}

pub fn build_poll_calls(configs: &[PoolConfig]) -> Vec<Multicall3Call> {
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

pub fn decode_poll_results(
    results: &[Bytes],
    configs: &[PoolConfig],
    block_number: u64,
    block_ts: chrono::DateTime<Utc>,
) -> Vec<Option<PoolState>> {
    configs
        .iter()
        .enumerate()
        .map(|(i, _cfg)| {
            let off = i * CALLS_PER_POOL;
            let s0 = slot0Call::abi_decode_returns(&results[off]).ok()?;
            let liq: u128 = liquidityCall::abi_decode_returns(&results[off + 1]).ok()?;
            let _staked: u128 = stakedLiquidityCall::abi_decode_returns(&results[off + 2]).ok()?;
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

