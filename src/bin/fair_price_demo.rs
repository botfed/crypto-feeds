use anyhow::{Context, Result};
use log::error;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;

use crypto_feeds::app_config::{AppConfig, load_config, load_onchain, load_perp, load_spot};
use crypto_feeds::display::init_display_logger;
use crypto_feeds::fair_price::{
    FairPriceConfig, FairPriceEngine, FairPriceGroupConfig, FairPriceOutputs, GroupMember,
    run_fair_price_task,
};
use crypto_feeds::fp_display::run_display;
use crypto_feeds::market_data::{AllMarketData, Exchange, InstrumentType};
use crypto_feeds::symbol_registry::REGISTRY;

const DEFAULT_CONFIG: &str = "configs/fair_price_demo.yaml";

/// Auto-discover pricing groups from config.yaml.
///
/// Groups symbols by base asset. For each base, collects all (exchange, symbol_id)
/// pairs across spot and perp sections. The underlying is "BASE_USD" style.
fn auto_discover_groups(cfg: &AppConfig) -> Vec<FairPriceGroupConfig> {
    let fp = &cfg.fair_price;
    let model = fp.parse_model();
    let sigma_mode = fp.parse_sigma_mode();
    // (exchange_name, symbol_str, instrument_type, reprice_group)
    type Entry = (String, String, InstrumentType, Option<String>);
    let mut base_map: HashMap<String, Vec<Entry>> = HashMap::new();

    for (exchange, symbols) in &cfg.spot {
        for sym in symbols {
            if let Some(base) = sym.split('_').next() {
                base_map
                    .entry(base.to_string())
                    .or_default()
                    .push((exchange.clone(), sym.clone(), InstrumentType::Spot, None));
            }
        }
    }
    for (exchange, symbols) in &cfg.perp {
        for sym in symbols {
            if let Some(base) = sym.split('_').next() {
                base_map
                    .entry(base.to_string())
                    .or_default()
                    .push((exchange.clone(), sym.clone(), InstrumentType::Perp, None));
            }
        }
    }

    // Onchain pools — USD-quoted directly, others repriced via quote group
    const USD_QUOTES: &[&str] = &["USD", "USDT", "USDC"];
    if let Some(ref onchain) = cfg.onchain {
        for (dex_name, dex_cfg) in [("aerodrome", &onchain.aerodrome), ("uniswap", &onchain.uniswap)] {
            if let Some(pools) = dex_cfg {
                for pool in &pools.validated_pools(dex_name) {
                    let parts: Vec<&str> = pool.symbol.split('_').collect();
                    if parts.len() == 2 {
                        let reprice = if USD_QUOTES.contains(&parts[1]) {
                            None
                        } else {
                            // Quote currency becomes the reprice group name (e.g. "ETH")
                            Some(parts[1].to_string())
                        };
                        base_map
                            .entry(parts[0].to_string())
                            .or_default()
                            .push((dex_name.to_string(), pool.symbol.clone(), InstrumentType::Spot, reprice));
                    }
                }
            }
        }
    }
    let mut groups: Vec<FairPriceGroupConfig> = Vec::new();

    // Sort base assets for stable ordering
    let mut bases: Vec<String> = base_map.keys().cloned().collect();
    bases.sort();

    for base in bases {
        let entries = &base_map[&base];
        let mut members = Vec::new();

        for (exchange_name, symbol_str, itype, reprice) in entries {
            let exchange = match Exchange::from_str(exchange_name) {
                Some(e) => e,
                None => {
                    log::warn!("Unknown exchange '{}', skipping", exchange_name);
                    continue;
                }
            };

            let symbol_id = match REGISTRY.lookup(symbol_str, itype) {
                Some(&id) => id,
                None => {
                    log::warn!(
                        "Symbol '{}' ({}) not in registry, skipping",
                        symbol_str,
                        itype.as_str()
                    );
                    continue;
                }
            };

            members.push(GroupMember {
                exchange,
                symbol_id,
                bias: 0.0,
                noise_var: 4e-8, // 2 bps
                gg_weight: 0.0,
                reprice_group: reprice.clone(),
                invert_reprice: false,
                vol_24h: 0.0,
                vol_adj: 1.0,
            });
        }

        if members.len() >= 2 {
            groups.push(FairPriceGroupConfig {
                name: base.clone(),
                members,
                sigma_mode: sigma_mode,
                model: model,
                bias_ewma_halflife_ms: fp.bias_ewma_halflife_s * 1000.0,
                spread_ewma_halflife_ms: fp.spread_ewma_halflife_s * 1000.0,
                sigma_k_floor: fp.sigma_k_floor,
                h_bias_per_ms: {
                    let bps = fp.bias_process_noise_bps_per_sqrt_s;
                    let log_val = bps * 1e-4;
                    log_val * log_val / 1000.0
                },
                bias_init_p: (fp.bias_init_uncertainty_bps * 1e-4).powi(2),
                liquidity_adjustment: fp.liquidity_adjustment,
                sigma_scale: fp.sigma_scale,
            });
        }
    }

    groups
}

#[tokio::main]
async fn main() -> Result<()> {
    init_display_logger(log::LevelFilter::Info);

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_CONFIG.to_string());

    let cfg: AppConfig = load_config(&config_path).context("loading config")?;

    let market_data = Arc::new(AllMarketData::with_clock_correction(cfg.clock_correction.clone()));
    let shutdown = Arc::new(Notify::new());
    let mut handles = Vec::new();

    // Start feeds
    let _ = load_spot(&mut handles, &cfg, &market_data, &shutdown);
    let _ = load_perp(&mut handles, &cfg, &market_data, &shutdown);
    if let Err(e) = load_onchain(&mut handles, &cfg, &market_data, &shutdown) {
        log::warn!("Onchain feeds not started: {}", e);
    }

    // Auto-discover fair price groups from config
    let groups = auto_discover_groups(&cfg);
    if groups.is_empty() {
        log::error!("No pricing groups discovered (need >= 2 members per base asset)");
        return Ok(());
    }

    // Build vol_provider from fair_price config section
    let group_names: Vec<String> = groups.iter().map(|g| g.name.clone()).collect();
    let vol_provider = cfg.fair_price.to_vol_provider(&group_names);

    let fp_config = FairPriceConfig {
        interval_ms: 100,
        buffer_capacity: 65536,
        groups,
        vol_provider,
    };

    for (gi, g) in fp_config.groups.iter().enumerate() {
        let ann_vol_pct = fp_config.vol_provider.h_per_ms(gi).max(0.0).sqrt()
            * (365.25 * 24.0 * 3600.0 * 1000.0_f64).sqrt() * 100.0;
        log::info!(
            "Group '{}': {} members, ann_vol={:.1}%",
            g.name, g.members.len(), ann_vol_pct,
        );
    }

    let outputs = Arc::new(FairPriceOutputs::new(&fp_config));

    // Start fair price task
    {
        let engine = Arc::new(FairPriceEngine::new(
            Arc::clone(&market_data), Arc::clone(&outputs), fp_config, None,
        ));
        let sd = Arc::clone(&shutdown);
        handles.push(tokio::spawn(run_fair_price_task(engine, sd)));
    }

    // Start display
    {
        let md = Arc::clone(&market_data);
        let out = Arc::clone(&outputs);
        let sd = Arc::clone(&shutdown);
        let model_str = format!("{}_{}", cfg.fair_price.model, cfg.fair_price.sigma_mode);
        let ve = &cfg.fair_price.vol_engine;
        let ve_str = format!("{} hl={}s", ve.engine_type, ve.halflife_s);
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_display(md, out, sd, model_str, ve_str, 100).await {
                error!("display exited with error {:?}", e);
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
