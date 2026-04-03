//! Fetches 24h volume per symbol per exchange from REST APIs.
//! Used for liquidity-adjusted sigma_k and display.

use crate::app_config::AppConfig;
use crate::mappers::{BinanceMapper, BybitMapper, KucoinMapper, MexcMapper, OkxMapper, SymbolMapper};
use crate::market_data::{Exchange, InstrumentType};
use log::{info, warn};
use serde::Deserialize;
use std::collections::HashMap;

/// (Exchange, registry_canonical) → 24h volume in base currency.
/// Registry canonical format: "SPOT-BTC-USDT", "PERP-BTC-USDT", etc.
pub type VolumeMap = HashMap<(Exchange, String), f64>;

/// Convert config symbol "BTC_USDT" + instrument type → registry canonical "SPOT-BTC-USDT"
fn to_registry_key(config_sym: &str, itype: InstrumentType) -> String {
    let prefix = match itype {
        InstrumentType::Spot => "SPOT",
        InstrumentType::Perp => "PERP",
        _ => "OTHER",
    };
    format!("{}-{}", prefix, config_sym.replace('_', "-"))
}

/// Fetch 24h volumes for all configured exchanges. Non-fatal: errors are logged and skipped.
pub async fn fetch_all_volumes(cfg: &AppConfig) -> VolumeMap {
    let mut map = VolumeMap::new();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    // Collect all configured symbols per exchange
    let spot_syms = &cfg.spot;
    let perp_syms = &cfg.perp;

    // Binance spot
    if let Some(syms) = spot_syms.get("binance") {
        if let Err(e) = fetch_binance_spot(&client, syms, &mut map).await {
            warn!("Volume fetch failed for binance_spot: {}", e);
        }
    }
    // Binance perp
    if let Some(syms) = perp_syms.get("binance") {
        if let Err(e) = fetch_binance_perp(&client, syms, &mut map).await {
            warn!("Volume fetch failed for binance_perp: {}", e);
        }
    }
    // Bybit spot
    if let Some(syms) = spot_syms.get("bybit") {
        if let Err(e) = fetch_bybit(&client, syms, "spot", Exchange::Bybit, &mut map).await {
            warn!("Volume fetch failed for bybit_spot: {}", e);
        }
    }
    // Bybit perp
    if let Some(syms) = perp_syms.get("bybit") {
        if let Err(e) = fetch_bybit(&client, syms, "linear", Exchange::Bybit, &mut map).await {
            warn!("Volume fetch failed for bybit_perp: {}", e);
        }
    }
    // OKX spot
    if let Some(syms) = spot_syms.get("okx") {
        if let Err(e) = fetch_okx(&client, syms, "SPOT", Exchange::Okx, &mut map).await {
            warn!("Volume fetch failed for okx_spot: {}", e);
        }
    }
    // OKX perp
    if let Some(syms) = perp_syms.get("okx") {
        if let Err(e) = fetch_okx(&client, syms, "SWAP", Exchange::Okx, &mut map).await {
            warn!("Volume fetch failed for okx_perp: {}", e);
        }
    }
    // Coinbase perp
    if let Some(syms) = perp_syms.get("coinbase") {
        if let Err(e) = fetch_coinbase(&client, syms, InstrumentType::Perp, &mut map).await {
            warn!("Volume fetch failed for coinbase_perp: {}", e);
        }
    }
    // Kucoin spot
    if let Some(syms) = spot_syms.get("kucoin") {
        if let Err(e) = fetch_kucoin_spot(&client, syms, &mut map).await {
            warn!("Volume fetch failed for kucoin_spot: {}", e);
        }
    }
    // Kucoin perp
    if let Some(syms) = perp_syms.get("kucoin") {
        if let Err(e) = fetch_kucoin_perp(&client, syms, &mut map).await {
            warn!("Volume fetch failed for kucoin_perp: {}", e);
        }
    }
    // Coinbase spot
    if let Some(syms) = spot_syms.get("coinbase") {
        if let Err(e) = fetch_coinbase(&client, syms, InstrumentType::Spot, &mut map).await {
            warn!("Volume fetch failed for coinbase_spot: {}", e);
        }
    }
    // MEXC spot
    if let Some(syms) = spot_syms.get("mexc") {
        if let Err(e) = fetch_mexc_spot(&client, syms, &mut map).await {
            warn!("Volume fetch failed for mexc_spot: {}", e);
        }
    }
    // BingX perp
    if let Some(syms) = perp_syms.get("bingx") {
        if let Err(e) = fetch_bingx_perp(&client, syms, &mut map).await {
            warn!("Volume fetch failed for bingx_perp: {}", e);
        }
    }
    // Hyperliquid perp
    if let Some(syms) = perp_syms.get("hyperliquid") {
        if let Err(e) = fetch_hyperliquid_perp(&client, syms, &mut map).await {
            warn!("Volume fetch failed for hyperliquid_perp: {}", e);
        }
    }

    info!("Fetched 24h volume for {} exchange-symbol pairs", map.len());
    map
}

// ── Binance ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct BinanceTicker24h {
    symbol: String,
    volume: String,
}

async fn fetch_binance_spot(
    client: &reqwest::Client,
    symbols: &[String],
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let tickers: Vec<BinanceTicker24h> = client
        .get("https://api.binance.com/api/v3/ticker/24hr")
        .send().await?.json().await?;
    let mapper = BinanceMapper;
    let wanted: HashMap<String, String> = symbols.iter().filter_map(|s| {
        let native = mapper.denormalize(s, InstrumentType::Spot).ok()?;
        Some((native, s.clone()))
    }).collect();
    for t in &tickers {
        if let Some(canonical) = wanted.get(&t.symbol) {
            if let Ok(vol) = t.volume.parse::<f64>() {
                map.insert((Exchange::Binance, to_registry_key(canonical, InstrumentType::Spot)), vol);
            }
        }
    }
    Ok(())
}

async fn fetch_binance_perp(
    client: &reqwest::Client,
    symbols: &[String],
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let tickers: Vec<BinanceTicker24h> = client
        .get("https://fapi.binance.com/fapi/v1/ticker/24hr")
        .send().await?.json().await?;
    let mapper = BinanceMapper;
    let wanted: HashMap<String, String> = symbols.iter().filter_map(|s| {
        let native = mapper.denormalize(s, InstrumentType::Perp).ok()?;
        Some((native, s.clone()))
    }).collect();
    for t in &tickers {
        if let Some(canonical) = wanted.get(&t.symbol) {
            if let Ok(vol) = t.volume.parse::<f64>() {
                map.insert((Exchange::Binance, to_registry_key(canonical, InstrumentType::Perp)), vol);
            }
        }
    }
    Ok(())
}

// ── Bybit ────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct BybitTickerResult {
    list: Vec<BybitTicker>,
}

#[derive(Deserialize)]
struct BybitTicker {
    symbol: String,
    #[serde(rename = "volume24h")]
    volume_24h: String,
}

#[derive(Deserialize)]
struct BybitResponse {
    result: BybitTickerResult,
}

async fn fetch_bybit(
    client: &reqwest::Client,
    symbols: &[String],
    category: &str,
    exchange: Exchange,
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let url = format!("https://api.bybit.com/v5/market/tickers?category={}", category);
    let resp: BybitResponse = client.get(&url).send().await?.json().await?;
    let mapper = BybitMapper;
    let itype = if category == "spot" { InstrumentType::Spot } else { InstrumentType::Perp };
    let wanted: HashMap<String, String> = symbols.iter().filter_map(|s| {
        let native = mapper.denormalize(s, itype).ok()?;
        Some((native, s.clone()))
    }).collect();
    for t in &resp.result.list {
        if let Some(canonical) = wanted.get(&t.symbol) {
            if let Ok(vol) = t.volume_24h.parse::<f64>() {
                map.insert((exchange, to_registry_key(canonical, itype)), vol);
            }
        }
    }
    Ok(())
}

// ── OKX ──────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct OkxTicker {
    #[serde(rename = "instId")]
    inst_id: String,
    /// For SPOT: base currency volume. For SWAP: contract volume (not base).
    #[serde(rename = "vol24h")]
    vol_24h: String,
    /// For SPOT: quote currency volume. For SWAP: base currency volume.
    #[serde(rename = "volCcy24h")]
    vol_ccy_24h: String,
}

#[derive(Deserialize)]
struct OkxTickerResponse {
    data: Vec<OkxTicker>,
}

async fn fetch_okx(
    client: &reqwest::Client,
    symbols: &[String],
    inst_type: &str,
    exchange: Exchange,
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let url = format!("https://www.okx.com/api/v5/market/tickers?instType={}", inst_type);
    let resp: OkxTickerResponse = client.get(&url).send().await?.json().await?;
    let mapper = OkxMapper;
    let itype = if inst_type == "SPOT" { InstrumentType::Spot } else { InstrumentType::Perp };
    let wanted: HashMap<String, String> = symbols.iter().filter_map(|s| {
        let native = mapper.denormalize(s, itype).ok()?;
        Some((native, s.clone()))
    }).collect();
    for t in &resp.data {
        if let Some(canonical) = wanted.get(&t.inst_id) {
            // SPOT: vol24h is base currency. SWAP: volCcy24h is base currency.
            let vol_str = if inst_type == "SPOT" { &t.vol_24h } else { &t.vol_ccy_24h };
            if let Ok(vol) = vol_str.parse::<f64>() {
                map.insert((exchange, to_registry_key(canonical, itype)), vol);
            }
        }
    }
    Ok(())
}

// ── Kucoin ───────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct KucoinSpotTickerData {
    ticker: Vec<KucoinSpotTicker>,
}

#[derive(Deserialize)]
struct KucoinSpotTicker {
    symbol: String,
    vol: String,
}

#[derive(Deserialize)]
struct KucoinSpotResponse {
    code: String,
    data: KucoinSpotTickerData,
}

async fn fetch_kucoin_spot(
    client: &reqwest::Client,
    symbols: &[String],
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let resp: KucoinSpotResponse = client
        .get("https://api.kucoin.com/api/v1/market/allTickers")
        .send().await?.json().await?;
    if resp.code != "200000" {
        anyhow::bail!("Kucoin allTickers returned code: {}", resp.code);
    }
    let mapper = KucoinMapper;
    let wanted: HashMap<String, String> = symbols.iter().filter_map(|s| {
        let native = mapper.denormalize(s, InstrumentType::Spot).ok()?;
        Some((native, s.clone()))
    }).collect();
    for t in &resp.data.ticker {
        if let Some(canonical) = wanted.get(&t.symbol) {
            if let Ok(vol) = t.vol.parse::<f64>() {
                map.insert((Exchange::Kucoin, to_registry_key(canonical, InstrumentType::Spot)), vol);
            }
        }
    }
    Ok(())
}

// Kucoin perp: use contracts/active endpoint (already fetched for multipliers)
#[derive(Deserialize)]
struct KucoinContract {
    symbol: String,
    multiplier: f64,
    #[serde(rename = "volumeOf24h")]
    volume_of_24h: Option<f64>,
}

#[derive(Deserialize)]
struct KucoinContractsResp {
    code: String,
    data: Vec<KucoinContract>,
}

async fn fetch_kucoin_perp(
    client: &reqwest::Client,
    symbols: &[String],
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let resp: KucoinContractsResp = client
        .get("https://api-futures.kucoin.com/api/v1/contracts/active")
        .send().await?.json().await?;
    if resp.code != "200000" {
        anyhow::bail!("Kucoin contracts returned code: {}", resp.code);
    }
    let mapper = KucoinMapper;
    let wanted: HashMap<String, String> = symbols.iter().filter_map(|s| {
        let native = mapper.denormalize(s, InstrumentType::Perp).ok()?;
        Some((native, s.clone()))
    }).collect();
    for c in &resp.data {
        if let Some(canonical) = wanted.get(&c.symbol) {
            // volumeOf24h is in base units already (BTC, not contracts)
            if let Some(vol) = c.volume_of_24h {
                map.insert((Exchange::Kucoin, to_registry_key(canonical, InstrumentType::Perp)), vol);
            }
        }
    }
    Ok(())
}

// ── Coinbase ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CoinbaseProduct {
    product_id: String,
    volume_24h: Option<String>,
}

#[derive(Deserialize)]
struct CoinbaseResponse {
    products: Vec<CoinbaseProduct>,
}

async fn fetch_coinbase(
    client: &reqwest::Client,
    symbols: &[String],
    itype: InstrumentType,
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let resp: CoinbaseResponse = client
        .get("https://api.coinbase.com/api/v3/brokerage/market/products")
        .send().await?.json().await?;
    // Coinbase product_id format: "BTC-USD"
    let wanted: HashMap<String, String> = symbols.iter().map(|s| {
        // canonical "BTC_USD" → coinbase "BTC-USD"
        let parts: Vec<&str> = s.split('_').collect();
        let pid = if parts.len() == 2 { format!("{}-{}", parts[0], parts[1]) } else { s.clone() };
        (pid, s.clone())
    }).collect();
    for p in &resp.products {
        if let Some(canonical) = wanted.get(&p.product_id) {
            if let Some(vol_str) = &p.volume_24h {
                if let Ok(vol) = vol_str.parse::<f64>() {
                    map.insert((Exchange::Coinbase, to_registry_key(canonical, itype)), vol);
                }
            }
        }
    }
    Ok(())
}

// ── MEXC ─────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct MexcTicker {
    symbol: String,
    volume: String,
}

async fn fetch_mexc_spot(
    client: &reqwest::Client,
    symbols: &[String],
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let tickers: Vec<MexcTicker> = client
        .get("https://api.mexc.com/api/v3/ticker/24hr")
        .send().await?.json().await?;
    let mapper = MexcMapper;
    let wanted: HashMap<String, String> = symbols.iter().filter_map(|s| {
        let native = mapper.denormalize(s, InstrumentType::Spot).ok()?;
        Some((native, s.clone()))
    }).collect();
    for t in &tickers {
        if let Some(canonical) = wanted.get(&t.symbol) {
            if let Ok(vol) = t.volume.parse::<f64>() {
                map.insert((Exchange::Mexc, to_registry_key(canonical, InstrumentType::Spot)), vol);
            }
        }
    }
    Ok(())
}

// ── BingX ────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct BingxTicker {
    symbol: String,
    volume: String,
}

#[derive(Deserialize)]
struct BingxResponse {
    data: Vec<BingxTicker>,
}

async fn fetch_bingx_perp(
    client: &reqwest::Client,
    symbols: &[String],
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let resp: BingxResponse = client
        .get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker")
        .send().await?.json().await?;
    // BingX symbol format: "BTC-USDT" (matches canonical with _ → -)
    let wanted: HashMap<String, String> = symbols.iter().map(|s| {
        (s.replace('_', "-"), s.clone())
    }).collect();
    for t in &resp.data {
        if let Some(canonical) = wanted.get(&t.symbol) {
            if let Ok(vol) = t.volume.parse::<f64>() {
                map.insert((Exchange::Bingx, to_registry_key(canonical, InstrumentType::Perp)), vol);
            }
        }
    }
    Ok(())
}

// ── Hyperliquid ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct HlMeta {
    universe: Vec<HlAsset>,
}

#[derive(Deserialize)]
struct HlAsset {
    name: String,
}

#[derive(Deserialize)]
struct HlAssetCtx {
    #[serde(rename = "dayNtlVlm")]
    day_ntl_vlm: String,
    #[serde(rename = "markPx")]
    mark_px: String,
}

async fn fetch_hyperliquid_perp(
    client: &reqwest::Client,
    symbols: &[String],
    map: &mut VolumeMap,
) -> anyhow::Result<()> {
    let body = serde_json::json!({"type": "metaAndAssetCtxs"});
    let resp: (HlMeta, Vec<HlAssetCtx>) = client
        .post("https://api.hyperliquid.xyz/info")
        .json(&body)
        .send().await?.json().await?;
    // Hyperliquid uses asset names: "BTC", "ETH", etc.
    // Config symbols: "BTC_USDC" → asset "BTC"
    let wanted: HashMap<String, String> = symbols.iter().filter_map(|s| {
        let base = s.split('_').next()?;
        Some((base.to_string(), s.clone()))
    }).collect();
    for (asset, ctx) in resp.0.universe.iter().zip(resp.1.iter()) {
        if let Some(canonical) = wanted.get(&asset.name) {
            let ntl: f64 = ctx.day_ntl_vlm.parse().unwrap_or(0.0);
            let px: f64 = ctx.mark_px.parse().unwrap_or(0.0);
            if px > 0.0 {
                let base_vol = ntl / px;
                map.insert((Exchange::Hyperliquid, to_registry_key(canonical, InstrumentType::Perp)), base_vol);
            }
        }
    }
    Ok(())
}
