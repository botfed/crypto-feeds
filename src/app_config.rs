use crate::exchanges::*;
use crate::market_data::AllMarketData;
use crate::onchain::OnchainConfig;
use anyhow::{Context, Result};
use log::error;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

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

        if let Some(ref aero) = onchain_cfg.aerodrome {
            let data = Arc::clone(&market_data.aerodrome);
            let shutdown = shutdown.clone();
            let rpc_url = rpc_url.clone();
            let pools = aero.pools.clone();
            handles.push(tokio::spawn(async move {
                if let Err(e) =
                    crate::onchain::aerodrome::listen_aerodrome(data, rpc_url, pools, shutdown)
                        .await
                {
                    error!("Aerodrome listener exited with error {:?}", e);
                }
            }));
        }

        if let Some(ref uni) = onchain_cfg.uniswap {
            let data = Arc::clone(&market_data.uniswap);
            let shutdown = shutdown.clone();
            let rpc_url = rpc_url.clone();
            let pools = uni.pools.clone();
            handles.push(tokio::spawn(async move {
                if let Err(e) =
                    crate::onchain::uniswap::listen_uniswap(data, rpc_url, pools, shutdown).await
                {
                    error!("Uniswap listener exited with error {:?}", e);
                }
            }));
        }
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
    Ok(())
}
