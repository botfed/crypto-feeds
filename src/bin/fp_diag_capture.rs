use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;

use crypto_feeds::app_config::{AppConfig, load_config, load_onchain, load_perp, load_spot};
use crypto_feeds::fair_price::{
    DiagWriter, FairPriceConfig, FairPriceGroupConfig, FairPriceModel, FairPriceOutputs, GroupMember,
    SigmaMode, load_beacon, run_fair_price_task,
};
use crypto_feeds::market_data::{AllMarketData, Exchange, InstrumentType};
use crypto_feeds::symbol_registry::REGISTRY;
use std::path::Path;

const BEACON_PATH: &str = "configs/beacon.yaml";
/// Default h_per_ms from 100% annualized vol
const DEFAULT_H_PER_MS: f64 = 1.0 / (365.25 * 24.0 * 3600.0 * 1000.0);

fn auto_discover_groups(cfg: &AppConfig) -> Vec<FairPriceGroupConfig> {
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

    const USD_QUOTES: &[&str] = &["USD", "USDT", "USDC"];
    if let Some(ref onchain) = cfg.onchain {
        for (dex_name, dex_cfg) in [
            ("aerodrome", &onchain.aerodrome),
            ("uniswap", &onchain.uniswap),
        ] {
            if let Some(pools) = dex_cfg {
                for pool in &pools.pools {
                    let parts: Vec<&str> = pool.symbol.split('_').collect();
                    if parts.len() == 2 {
                        let reprice = if USD_QUOTES.contains(&parts[1]) {
                            None
                        } else {
                            Some(parts[1].to_string())
                        };
                        base_map
                            .entry(parts[0].to_string())
                            .or_default()
                            .push((
                                dex_name.to_string(),
                                pool.symbol.clone(),
                                InstrumentType::Spot,
                                reprice,
                            ));
                    }
                }
            }
        }
    }

    let mut groups: Vec<FairPriceGroupConfig> = Vec::new();
    let mut bases: Vec<String> = base_map.keys().cloned().collect();
    bases.sort();

    for base in bases {
        let entries = &base_map[&base];
        let mut members = Vec::new();

        for (exchange_name, symbol_str, itype, reprice) in entries {
            let exchange = match Exchange::from_str(exchange_name) {
                Some(e) => e,
                None => continue,
            };
            let symbol_id = match REGISTRY.lookup(symbol_str, itype) {
                Some(&id) => id,
                None => continue,
            };
            members.push(GroupMember {
                exchange,
                symbol_id,
                bias: 0.0,
                noise_var: 4e-8,
                gg_weight: 0.0,
                reprice_group: reprice.clone(),
                invert_reprice: reprice.is_some(),
            });
        }

        if members.len() >= 2 {
            groups.push(FairPriceGroupConfig {
                name: base.clone(),
                members,
                h_per_ms: DEFAULT_H_PER_MS,
                sigma_mode: SigmaMode::InstantSpread,
                model: FairPriceModel::Kalman,
                bias_ewma_halflife_ms: 3000.0,
                spread_ewma_halflife_ms: 3000.0,
                sigma_k_floor: 1e-6,
                vol_ewma_halflife_ms: None,
                vol_floor_ann: None,
                vol_init_ann: None,
            });
        }
    }

    groups
}

struct Args {
    output_dir: String,
    warmup_secs: u64,
}

fn parse_args() -> Args {
    let mut output_dir = String::from("data/fp_diag");
    let mut warmup_secs: u64 = 900;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--output-dir" => {
                i += 1;
                if i < args.len() {
                    output_dir = args[i].clone();
                }
            }
            "--warmup-secs" => {
                i += 1;
                if i < args.len() {
                    warmup_secs = args[i].parse().unwrap_or(900);
                }
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                eprintln!(
                    "Usage: fp_diag_capture [--output-dir DIR] [--warmup-secs N]"
                );
                std::process::exit(1);
            }
        }
        i += 1;
    }

    Args {
        output_dir,
        warmup_secs,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = parse_args();
    eprintln!(
        "fp_diag_capture: output_dir={}, warmup_secs={}",
        args.output_dir, args.warmup_secs
    );

    let cfg: AppConfig = load_config("configs/config.yaml").context("loading config.yaml")?;

    let market_data = Arc::new(AllMarketData::new());
    let shutdown = Arc::new(Notify::new());
    let mut handles = Vec::new();

    let _ = load_spot(&mut handles, &cfg, &market_data, &shutdown);
    let _ = load_perp(&mut handles, &cfg, &market_data, &shutdown);
    let _ = load_onchain(&mut handles, &cfg, &market_data, &shutdown);

    let groups = auto_discover_groups(&cfg);
    if groups.is_empty() {
        anyhow::bail!("No pricing groups discovered (need >= 2 members per base asset)");
    }

    // Build config, then load beacon params (h_per_ms, bias, noise_var)
    let beacon = Path::new(BEACON_PATH);
    let mut fp_config = FairPriceConfig {
        interval_ms: 100,
        buffer_capacity: 65536,
        groups,
        vol_ewma_halflife_ms: 0.0,
        vol_floor_ann: 0.50,
        vol_init_ann: 0.0,
    };
    let mut beacon_mtime = std::time::UNIX_EPOCH;
    load_beacon(beacon, &mut beacon_mtime, &mut fp_config);

    for g in &fp_config.groups {
        eprintln!(
            "Group '{}': {} members, h_per_ms={:.2e}",
            g.name, g.members.len(), g.h_per_ms,
        );
    }

    let outputs = Arc::new(FairPriceOutputs::new(&fp_config));

    // Create diagnostic writer
    let diag = DiagWriter::new(&args.output_dir)
        .with_context(|| format!("creating DiagWriter at {}", args.output_dir))?;
    eprintln!("Recording diagnostics to {}/", args.output_dir);

    // Start fair price task with diagnostics
    {
        let tick = Arc::clone(&market_data);
        let out = Arc::clone(&outputs);
        let sd = Arc::clone(&shutdown);
        handles.push(tokio::spawn(
            run_fair_price_task(tick, out, fp_config, sd, Some(diag)),
        ));
    }

    // Progress reporting
    {
        let sd = Arc::clone(&shutdown);
        let out = Arc::clone(&outputs);
        let warmup = args.warmup_secs;
        handles.push(tokio::spawn(async move {
            let start = std::time::Instant::now();
            let shutdown_fut = sd.notified();
            tokio::pin!(shutdown_fut);
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let elapsed = start.elapsed().as_secs();
                        let phase = if elapsed < warmup { "WARMUP" } else { "RECORDING" };
                        let n_groups = out.group_names().len();
                        let total_ticks: u32 = (0..n_groups)
                            .filter_map(|i| out.latest(i).map(|fp| fp.n_ticks_used))
                            .sum();
                        eprintln!(
                            "[{:02}:{:02}:{:02}] {} | groups={} last_ticks={}",
                            elapsed / 3600,
                            (elapsed % 3600) / 60,
                            elapsed % 60,
                            phase,
                            n_groups,
                            total_ticks,
                        );
                    }
                    _ = &mut shutdown_fut => return,
                }
            }
        }));
    }

    signal::ctrl_c().await?;
    eprintln!("\nShutting down, finalizing diagnostics...");
    shutdown.notify_waiters();

    tokio::time::timeout(Duration::from_secs(5), async {
        for h in handles {
            let _ = h.await;
        }
    })
    .await?;

    eprintln!("Done. Run: python scripts/fp_validate.py --data-dir {} --warmup-secs {}", args.output_dir, args.warmup_secs);
    Ok(())
}
