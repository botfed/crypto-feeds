use crate::exchanges::*;
use crate::market_data::{AllMarketData, ClockCorrectionConfig};
use crate::onchain::OnchainConfig;
use anyhow::{Context, Result};
use log::error;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Debug, Deserialize, Clone)]
pub struct VolModelConfig {
    /// Directory containing per-symbol YAML param files.
    pub params_dir: String,
    /// Directory containing 1m CSV bar data (per-symbol subdirectories).
    pub bar_data_dir: String,
    /// Which HAR variant to use: "har_ols" or "har_qlike".
    #[serde(default = "default_har_model")]
    pub model: String,
    /// Number of days of historical bars to load for warmup.
    #[serde(default = "default_warmup_days")]
    pub warmup_days: u32,
}

fn default_har_model() -> String {
    "har_ols".to_string()
}

fn default_warmup_days() -> u32 {
    2
}

/// Per-asset vol config within the vol_engine section.
#[derive(Debug, Deserialize, Clone)]
pub struct AssetVolConfig {
    pub init_ann_vol: f64,
    pub floor_ann: Option<f64>,
}

/// Vol engine config — nested under fair_price.vol_engine.
#[derive(Debug, Deserialize, Clone)]
pub struct VolEngineConfig {
    #[serde(rename = "type", default = "default_vol_engine_type")]
    pub engine_type: String,
    #[serde(default = "default_init_ann_vol")]
    pub default_init_ann_vol: f64,
    #[serde(default)]
    pub halflife_s: f64,
    #[serde(default = "default_floor_ann")]
    pub floor_ann: f64,
    #[serde(default = "default_bar_min")]
    pub bar_min: usize,
    #[serde(default = "default_max_bars")]
    pub max_bars: usize,
    #[serde(flatten)]
    pub assets: HashMap<String, AssetVolConfig>,
}

fn default_bar_min() -> usize {
    1
}

fn default_max_bars() -> usize {
    1440
}

fn default_vol_engine_type() -> String {
    "static".to_string()
}

fn default_init_ann_vol() -> f64 {
    1.0
}

fn default_floor_ann() -> f64 {
    0.50
}

impl Default for VolEngineConfig {
    fn default() -> Self {
        Self {
            engine_type: default_vol_engine_type(),
            default_init_ann_vol: default_init_ann_vol(),
            halflife_s: 0.0,
            floor_ann: default_floor_ann(),
            bar_min: default_bar_min(),
            max_bars: default_max_bars(),
            assets: HashMap::new(),
        }
    }
}

impl VolEngineConfig {
    /// Get initial annualized vol for an asset, falling back to the default.
    pub fn init_ann_vol(&self, asset: &str) -> f64 {
        self.assets
            .get(asset)
            .map(|a| a.init_ann_vol)
            .unwrap_or(self.default_init_ann_vol)
    }

    /// Get floor_ann for an asset, falling back to the global default.
    pub fn floor_ann_for(&self, asset: &str) -> f64 {
        self.assets
            .get(asset)
            .and_then(|a| a.floor_ann)
            .unwrap_or(self.floor_ann)
    }
}

fn default_model() -> String { "adaptive_filter".to_string() }
fn default_sigma_mode() -> String { "instant_spread".to_string() }
fn default_bias_ewma_halflife_s() -> f64 { 3.0 }
fn default_spread_ewma_halflife_s() -> f64 { 3.0 }
fn default_sigma_k_floor() -> f64 { 1e-6 }

/// Top-level fair_price config section.
#[derive(Debug, Deserialize, Clone)]
pub struct FairPriceParamsConfig {
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_sigma_mode")]
    pub sigma_mode: String,
    #[serde(default = "default_bias_ewma_halflife_s")]
    pub bias_ewma_halflife_s: f64,
    #[serde(default = "default_spread_ewma_halflife_s")]
    pub spread_ewma_halflife_s: f64,
    #[serde(default = "default_sigma_k_floor")]
    pub sigma_k_floor: f64,
    /// GG-specific: max age in seconds before an exchange is excluded. 0 = no cutoff.
    #[serde(default)]
    pub max_latency_s: f64,
    /// GG-specific: exp decay half-life in seconds. 0 = no decay.
    #[serde(default)]
    pub decay_halflife_s: f64,
    #[serde(default)]
    pub vol_engine: VolEngineConfig,
    /// FP engine background drain interval in milliseconds. Default 100. Set to -1 to disable.
    #[serde(default = "default_fp_drain_interval_ms")]
    pub drain_interval_ms: i64,
    /// Bias process noise in bps/√s. Controls how fast biases drift in the augmented filter.
    /// 0.1 ≈ bias can drift ~0.1 bps/√s ≈ 2.4 bps/√min.
    #[serde(default = "default_bias_process_noise")]
    pub bias_process_noise_bps_per_sqrt_s: f64,
    /// Initial bias uncertainty in bps. Default 0.5. Controls how aggressively biases
    /// adjust on the first few ticks. Lower = more trust in the initial bias (0).
    #[serde(default = "default_bias_init_uncertainty_bps")]
    pub bias_init_uncertainty_bps: f64,
    /// Apply volume-based liquidity adjustment to sigma_k. Default false.
    #[serde(default)]
    pub liquidity_adjustment: bool,
    /// Exponent for liquidity adjustment: vol_adj = (max_vol/vol_k)^exponent.
    /// 0.5 = sqrt (gentle), 1.0 = linear (aggressive). Default 0.75.
    #[serde(default = "default_liq_adj_exponent")]
    pub liq_adj_exponent: f64,
    /// Overall scale factor for sigma_k. Applied after spread + liq_adj.
    /// < 1.0 = trust mids more (higher gain). Default 1.0 (no change).
    #[serde(default = "default_sigma_scale")]
    pub sigma_scale: f64,
    /// Per-exchange volume trust multiplier. Penalizes exchanges with inflated self-reported volume.
    /// E.g., bingx: 0.1 divides reported volume by 10. Default 1.0 for unlisted exchanges.
    #[serde(default)]
    pub volume_adjustments: std::collections::HashMap<String, f64>,
}

fn default_fp_drain_interval_ms() -> i64 { 100 }
fn default_bias_process_noise() -> f64 { 0.1 }
fn default_bias_init_uncertainty_bps() -> f64 { 0.5 }
fn default_liq_adj_exponent() -> f64 { 0.75 }
fn default_sigma_scale() -> f64 { 1.0 }

impl Default for FairPriceParamsConfig {
    fn default() -> Self {
        Self {
            model: default_model(),
            sigma_mode: default_sigma_mode(),
            bias_ewma_halflife_s: default_bias_ewma_halflife_s(),
            spread_ewma_halflife_s: default_spread_ewma_halflife_s(),
            sigma_k_floor: default_sigma_k_floor(),
            max_latency_s: 0.0,
            decay_halflife_s: 0.0,
            vol_engine: VolEngineConfig::default(),
            drain_interval_ms: default_fp_drain_interval_ms(),
            bias_process_noise_bps_per_sqrt_s: default_bias_process_noise(),
            bias_init_uncertainty_bps: default_bias_init_uncertainty_bps(),
            liquidity_adjustment: false,
            liq_adj_exponent: default_liq_adj_exponent(),
            sigma_scale: default_sigma_scale(),
            volume_adjustments: std::collections::HashMap::new(),
        }
    }
}

impl FairPriceParamsConfig {
    pub fn parse_model(&self) -> crate::fair_price::FairPriceModel {
        match self.model.as_str() {
            "gg" | "gonzalo_granger" => crate::fair_price::FairPriceModel::GonzaloGranger {
                max_latency_ms: self.max_latency_s * 1000.0,
                decay_halflife_ms: self.decay_halflife_s * 1000.0,
            },
            _ => crate::fair_price::FairPriceModel::AdaptiveFilter,
        }
    }

    pub fn parse_sigma_mode(&self) -> crate::fair_price::SigmaMode {
        match self.sigma_mode.as_str() {
            "ewma_spread" => crate::fair_price::SigmaMode::EwmaSpread,
            "static" => crate::fair_price::SigmaMode::Static,
            _ => crate::fair_price::SigmaMode::InstantSpread,
        }
    }

    /// Build a VolProvider from this config for the given group names.
    pub fn to_vol_provider(&self, group_names: &[String]) -> crate::vol_provider::VolProvider {
        let ve = &self.vol_engine;
        let groups: Vec<(f64, f64, f64)> = group_names
            .iter()
            .map(|name| (ve.init_ann_vol(name), ve.halflife_s * 1000.0, ve.floor_ann_for(name)))
            .collect();

        match ve.engine_type.as_str() {
            "ewma" => {
                crate::vol_provider::VolProvider::new_ewma(&groups)
            }
            "gk_ewma" => {
                crate::vol_provider::VolProvider::new_gk_ewma(&groups, ve.bar_min, ve.max_bars)
            }
            _ => {
                let ann_vols = group_names.iter().map(|n| ve.init_ann_vol(n)).collect();
                crate::vol_provider::VolProvider::new_static(ann_vols)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub spot: HashMap<String, Vec<String>>,

    #[serde(default)]
    pub perp: HashMap<String, Vec<String>>,

    #[serde(default = "default_sample_interval_ms")]
    pub sample_interval_ms: u64,

    #[serde(default)]
    pub onchain: Option<OnchainConfig>,

    #[serde(default)]
    pub fair_price: FairPriceParamsConfig,

    #[serde(default)]
    pub vol_models: Option<VolModelConfig>,

    #[serde(default)]
    pub clock_correction: ClockCorrectionConfig,
}

fn default_sample_interval_ms() -> u64 {
    10
}

pub fn load_config(path: &str) -> Result<AppConfig> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read config file: {path}"))?;

    let config: AppConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse yaml config: {path}"))?;

    Ok(config)
}

pub fn load_spot(
    handles: &mut Vec<JoinHandle<()>>,
    cfg: &AppConfig,
    market_data: &Arc<AllMarketData>,
    shutdown: &Arc<Notify>,
) -> Result<()> {
    // Helper: grab spot symbols for an exchange and make them spawn-friendly ('static)
    let spot_syms = |exchange: &str| -> Option<Arc<[String]>> {
        cfg.spot.get(exchange).cloned().map(Arc::<[String]>::from)
    };
    if let Some(syms) = spot_syms("binance") {
        let data = Arc::clone(&market_data.binance);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = binance::listen_spot_bbo(data, &symbol_refs, shutdown).await {
                error!("Binance spot listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = spot_syms("coinbase") {
        let data = Arc::clone(&market_data.coinbase);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = coinbase::listen_spot_bbo(data, &symbol_refs, shutdown).await {
                error!("Coinbase spot listener exited with error {:?}", e);
            }
        }));
    }

    if let Some(syms) = spot_syms("mexc") {
        let data = Arc::clone(&market_data.mexc);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = mexc::listen_spot_bbo(data, &symbol_refs, shutdown).await {
                error!("Mexc spot listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = spot_syms("bybit") {
        let data = Arc::clone(&market_data.bybit);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = bybit::listen_spot_bbo(data, &symbol_refs, shutdown).await {
                error!("Bybit spot listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = spot_syms("kraken") {
        let data = Arc::clone(&market_data.kraken);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = kraken::listen_spot_bbo(data, &symbol_refs, shutdown).await {
                error!("Kraken spot listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = spot_syms("okx") {
        let data = Arc::clone(&market_data.okx);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = okx::listen_spot_bbo(data, &symbol_refs, shutdown).await {
                error!("OKX spot listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = spot_syms("kucoin") {
        let data = Arc::clone(&market_data.kucoin);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = kucoin::listen_spot_bbo(data, &symbol_refs, shutdown).await {
                error!("KuCoin spot listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = spot_syms("bingx") {
        let data = Arc::clone(&market_data.bingx);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = bingx::listen_spot_bbo(data, &symbol_refs, shutdown).await {
                error!("BingX spot listener exited with error {:?}", e);
            }
        }));
    }
    Ok(())
}

pub fn load_onchain(
    handles: &mut Vec<JoinHandle<()>>,
    cfg: &AppConfig,
    market_data: &Arc<AllMarketData>,
    shutdown: &Arc<Notify>,
) -> Result<()> {
    if let Some(ref onchain_cfg) = cfg.onchain {
        let rpc_url = onchain_cfg.rpc_url()?;

        let aero_data = onchain_cfg.aerodrome.as_ref().map(|_| Arc::clone(&market_data.aerodrome));
        let aero_pools = onchain_cfg.aerodrome.as_ref().map(|a| a.validated_pools("Aerodrome")).unwrap_or_default();
        let uni_data = onchain_cfg.uniswap.as_ref().map(|_| Arc::clone(&market_data.uniswap));
        let uni_pools = onchain_cfg.uniswap.as_ref().map(|u| u.validated_pools("Uniswap")).unwrap_or_default();

        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = crate::onchain::listener::listen_onchain(
                aero_data, aero_pools, uni_data, uni_pools, rpc_url, shutdown,
            ).await {
                error!("Onchain listener exited with error {:?}", e);
            }
        }));
    }
    Ok(())
}

pub fn load_perp(
    handles: &mut Vec<JoinHandle<()>>,
    cfg: &AppConfig,
    market_data: &Arc<AllMarketData>,
    shutdown: &Arc<Notify>,
) -> Result<()> {
    // Helper: grab spot symbols for an exchange and make them spawn-friendly ('static)
    let perp_syms = |exchange: &str| -> Option<Arc<[String]>> {
        cfg.perp.get(exchange).cloned().map(Arc::<[String]>::from)
    };
    if let Some(syms) = perp_syms("binance") {
        let data = Arc::clone(&market_data.binance);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = binance::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Binance perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("coinbase") {
        let data = Arc::clone(&market_data.coinbase);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = coinbase::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Coinbase perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("mexc") {
        let data = Arc::clone(&market_data.mexc);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = mexc::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Mexc perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("bybit") {
        let data = Arc::clone(&market_data.bybit);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = bybit::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Bybit perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("kraken") {
        let data = Arc::clone(&market_data.kraken);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = kraken::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Kraken perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("lighter") {
        let data = Arc::clone(&market_data.lighter);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = lighter::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Lighter perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("extended") {
        let data = Arc::clone(&market_data.extended);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = extended::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Extended perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("nado") {
        let data = Arc::clone(&market_data.nado);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = nado::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Nado perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("okx") {
        let data = Arc::clone(&market_data.okx);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = okx::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("OKX perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("kucoin") {
        let data = Arc::clone(&market_data.kucoin);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = kucoin::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("KuCoin perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("bingx") {
        let data = Arc::clone(&market_data.bingx);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = bingx::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("BingX perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("apex") {
        let data = Arc::clone(&market_data.apex);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = apex::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Apex perp listener exited with error {:?}", e);
            }
        }));
    }
    if let Some(syms) = perp_syms("hyperliquid") {
        let data = Arc::clone(&market_data.hyperliquid);
        let shutdown = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let symbol_refs: Vec<&str> = syms.iter().map(|s| s.as_str()).collect();
            if let Err(e) = hyperliquid::listen_perp_bbo(data, &symbol_refs, shutdown).await {
                error!("Hyperliquid perp listener exited with error {:?}", e);
            }
        }));
    }
    Ok(())
}
