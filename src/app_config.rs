use crate::exchanges::*;
use crate::market_data::AllMarketData;
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
    Ok(())
}
