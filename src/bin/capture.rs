use anyhow::{Context, Result};
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;
use tokio::time::{self, MissedTickBehavior};

use crypto_feeds::app_config::{load_config, load_onchain, load_perp, load_spot, AppConfig};
use crypto_feeds::fair_price::{
    DiagWriter, FairPriceConfig, FairPriceEngine, FairPriceGroupConfig, FairPriceOutputs,
    GroupMember, run_fair_price_task,
};
use crypto_feeds::market_data::{AllMarketData, Exchange, InstrumentType, MarketDataCollection};
use crypto_feeds::symbol_registry::{REGISTRY, SymbolId};

/// One entry in our sampling plan: which exchange + symbol to read each tick.
struct SampleTarget {
    exchange_name: &'static str,
    canonical: String,
    collection: Arc<MarketDataCollection>,
    symbol_id: SymbolId,
}

fn build_targets(cfg: &AppConfig, market_data: &AllMarketData) -> Vec<SampleTarget> {
    let mut targets = Vec::new();

    for (exchange, coll) in market_data.iter() {
        let name = exchange.as_str();
        if let Some(syms) = cfg.spot.get(name) {
            for raw in syms {
                if let Some(&id) = REGISTRY.lookup(raw, &InstrumentType::Spot) {
                    let canonical = REGISTRY.get_symbol(id).unwrap_or(raw).to_string();
                    targets.push(SampleTarget {
                        exchange_name: name,
                        canonical,
                        collection: Arc::clone(coll),
                        symbol_id: id,
                    });
                }
            }
        }
        if let Some(syms) = cfg.perp.get(name) {
            for raw in syms {
                if let Some(&id) = REGISTRY.lookup(raw, &InstrumentType::Perp) {
                    let canonical = REGISTRY.get_symbol(id).unwrap_or(raw).to_string();
                    targets.push(SampleTarget {
                        exchange_name: name,
                        canonical,
                        collection: Arc::clone(coll),
                        symbol_id: id,
                    });
                }
            }
        }
    }

    // On-chain pools (aerodrome, uniswap)
    if let Some(ref onchain) = cfg.onchain {
        for (dex_name, dex_cfg, coll) in [
            ("aerodrome", &onchain.aerodrome, &market_data.aerodrome),
            ("uniswap", &onchain.uniswap, &market_data.uniswap),
        ] {
            if let Some(pools) = dex_cfg {
                for pool in &pools.pools {
                    if let Some(&id) = REGISTRY.lookup(&pool.symbol, &InstrumentType::Spot) {
                        let canonical =
                            REGISTRY.get_symbol(id).unwrap_or(&pool.symbol).to_string();
                        targets.push(SampleTarget {
                            exchange_name: dex_name,
                            canonical,
                            collection: Arc::clone(coll),
                            symbol_id: id,
                        });
                    }
                }
            }
        }
    }

    targets
}

fn format_ts(ts: Option<chrono::DateTime<Utc>>) -> String {
    match ts {
        Some(t) => t.timestamp_nanos_opt().map_or(String::new(), |n| n.to_string()),
        None => String::new(),
    }
}

fn auto_discover_groups(cfg: &AppConfig) -> Vec<FairPriceGroupConfig> {
    use std::collections::HashMap;
    let fp = &cfg.fair_price;
    let model = fp.parse_model();
    let sigma_mode = fp.parse_sigma_mode();
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
        for (dex_name, dex_cfg) in [("aerodrome", &onchain.aerodrome), ("uniswap", &onchain.uniswap)] {
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
                            .push((dex_name.to_string(), pool.symbol.clone(), InstrumentType::Spot, reprice));
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
                invert_reprice: false,
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
            });
        }
    }
    groups
}

struct Args {
    config_path: String,
    output_dir: String,
    interval_ms: Option<u64>,
    tick_dump: bool,
    with_fp: bool,
    display: bool,
    warmup_secs: u64,
}

fn parse_args() -> Args {
    let mut args = Args {
        config_path: "configs/capture.yaml".to_string(),
        output_dir: "data/capture".to_string(),
        interval_ms: None,
        tick_dump: false,
        with_fp: false,
        display: false,
        warmup_secs: 5,
    };
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--config" if i + 1 < argv.len() => { args.config_path = argv[i + 1].clone(); i += 2; }
            "--output-dir" if i + 1 < argv.len() => { args.output_dir = argv[i + 1].clone(); i += 2; }
            "--interval-ms" if i + 1 < argv.len() => { args.interval_ms = argv[i + 1].parse().ok(); i += 2; }
            "--tick-dump" => { args.tick_dump = true; i += 1; }
            "--with-fp" => { args.with_fp = true; i += 1; }
            "--display" => { args.display = true; i += 1; }
            "--warmup-secs" if i + 1 < argv.len() => { args.warmup_secs = argv[i + 1].parse().unwrap_or(5); i += 2; }
            "--help" | "-h" => {
                eprintln!("Usage: capture [--interval-ms N] [--tick-dump] [--with-fp] [--display] [--output-dir DIR] [--config PATH] [--warmup-secs N]");
                eprintln!();
                eprintln!("  --interval-ms N   Snap BBOs at N ms interval (e.g. 10)");
                eprintln!("  --tick-dump        Full tick-level filter diagnostics (requires FP engine)");
                eprintln!("  --with-fp          Add fair price columns (starts FP engine)");
                eprintln!("  --display          Live fair price TUI display (starts FP engine)");
                eprintln!("  --output-dir DIR   Output directory (default: data/capture)");
                eprintln!("  --config PATH      Exchange/symbol config (default: configs/capture.yaml)");
                eprintln!("  --warmup-secs N    Wait N seconds before capturing (default: 5)");
                std::process::exit(0);
            }
            _ => { i += 1; }
        }
    }
    if args.interval_ms.is_none() && !args.tick_dump && !args.display {
        eprintln!("Error: at least one of --interval-ms, --tick-dump, or --display is required");
        std::process::exit(1);
    }
    args
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();

    if args.display {
        crypto_feeds::display::init_display_logger(log::LevelFilter::Info);
    } else {
        env_logger::init();
    }
    let needs_fp = args.with_fp || args.tick_dump || args.display;

    let cfg: AppConfig = load_config(&args.config_path)
        .with_context(|| format!("loading {}", args.config_path))?;

    std::fs::create_dir_all(&args.output_dir)?;

    let market_data = Arc::new(AllMarketData::with_clock_correction(cfg.clock_correction.clone()));
    let shutdown = Arc::new(Notify::new());
    let mut handles = Vec::new();

    load_spot(&mut handles, &cfg, &market_data, &shutdown)?;
    load_perp(&mut handles, &cfg, &market_data, &shutdown)?;
    if let Err(e) = load_onchain(&mut handles, &cfg, &market_data, &shutdown) {
        log::warn!("Onchain feeds not started: {}", e);
    }

    let targets = build_targets(&cfg, &market_data);
    eprintln!("Discovered {} sample targets", targets.len());

    // Set up FP engine if needed
    let has_capture = args.interval_ms.is_some();
    let (outputs, fp_engine): (Option<Arc<FairPriceOutputs>>, Option<Arc<FairPriceEngine>>) = if needs_fp {
        let groups = auto_discover_groups(&cfg);
        if groups.is_empty() {
            eprintln!("Warning: no FP groups discovered, --with-fp will have no effect");
            (None, None)
        } else {
            let group_names: Vec<String> = groups.iter().map(|g| g.name.clone()).collect();
            let vol_provider = cfg.fair_price.to_vol_provider(&group_names);
            let drain_interval_ms = cfg.fair_price.drain_interval_ms;
            let fp_config = FairPriceConfig {
                interval_ms: drain_interval_ms.max(1) as u64,
                buffer_capacity: 65536,
                groups,
                vol_provider,
            };

            for g in &fp_config.groups {
                eprintln!("FP group '{}': {} members", g.name, g.members.len());
            }

            let out = Arc::new(FairPriceOutputs::new(&fp_config));

            let diag = if args.tick_dump {
                Some(DiagWriter::new(&args.output_dir)
                    .with_context(|| format!("creating DiagWriter at {}", args.output_dir))?)
            } else {
                None
            };

            let engine = Arc::new(FairPriceEngine::new(
                Arc::clone(&market_data),
                Arc::clone(&out),
                fp_config,
                diag,
            ));

            // Spawn timer task for background drain unless disabled (-1).
            if drain_interval_ms > 0 {
                let sd = Arc::clone(&shutdown);
                handles.push(tokio::spawn(run_fair_price_task(Arc::clone(&engine), sd)));
            }

            // Capture also gets a clone to call update() for freshest FP.
            let engine_for_capture = if has_capture {
                Some(Arc::clone(&engine))
            } else {
                None
            };

            if args.display {
                let md = Arc::clone(&market_data);
                let out_display = Arc::clone(&out);
                let sd = Arc::clone(&shutdown);
                let model_str = format!("{}_{}", cfg.fair_price.model, cfg.fair_price.sigma_mode);
                let ve = &cfg.fair_price.vol_engine;
                let ve_str = format!("{} hl={}s", ve.engine_type, ve.halflife_s);
                handles.push(tokio::spawn(async move {
                    if let Err(e) = crypto_feeds::fp_display::run_display(md, out_display, sd, model_str, ve_str, drain_interval_ms.max(1) as u64).await {
                        log::error!("display error: {:?}", e);
                    }
                }));
            }

            (Some(out), engine_for_capture)
        }
    } else {
        (None, None)
    };

    // Warmup
    if args.warmup_secs > 0 {
        eprintln!("Warming up for {} seconds...", args.warmup_secs);
        tokio::time::sleep(Duration::from_secs(args.warmup_secs)).await;
    }

    // Snapshot capture task
    let snap_handle = if let Some(interval_ms) = args.interval_ms {
        let targets_arc = Arc::new(targets);
        let shutdown_clone = Arc::clone(&shutdown);
        let output_dir = args.output_dir.clone();
        let with_fp = args.with_fp;
        let outputs_clone = outputs.clone();
        let fp_engine = fp_engine;

        Some(tokio::spawn(async move {
            let ts = Utc::now().format("%Y%m%d_%H%M%S");
            let output_path = format!("{}/snapshots_{}.csv.gz", output_dir, ts);
            let file = std::fs::File::create(&output_path).expect("create snapshot file");
            let gz = GzEncoder::new(file, Compression::fast());
            let mut wtr = BufWriter::new(gz);

            // Header
            if with_fp {
                writeln!(wtr, "sample_ts_ns,canonical_symbol,exchange,bid,ask,bid_qty,ask_qty,exchange_ts,exchange_ts_raw,received_ts,fp_group,fp,fp_y,fp_p,fp_vol_ann_pct,fp_bias,fp_sigma_k,fp_snap_ts_ns,fp_mid,fp_tick_exchange_ts,fp_tick_received_ts,fp_half_spread").unwrap();
            } else {
                writeln!(wtr, "sample_ts_ns,canonical_symbol,exchange,bid,ask,bid_qty,ask_qty,exchange_ts,exchange_ts_raw,received_ts").unwrap();
            }

            let mut interval = time::interval(Duration::from_millis(interval_ms));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let shutdown_fut = shutdown_clone.notified();
            tokio::pin!(shutdown_fut);

            let mut row_count: u64 = 0;

            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = &mut shutdown_fut => {
                        eprintln!("Snapshot capture: {} rows written to {}", row_count, output_path);
                        let gz = wtr.into_inner().expect("flush");
                        let _ = gz.finish();
                        return;
                    }
                }

                if let Some(ref engine) = fp_engine {
                    engine.update();
                }

                let sample_ts = Utc::now().timestamp_nanos_opt().unwrap_or(0);

                for t in targets_arc.iter() {
                    let md = match t.collection.latest(&t.symbol_id) {
                        Some(md) => md,
                        None => continue,
                    };

                    let bid = md.bid.map_or(String::new(), |v| v.to_string());
                    let ask = md.ask.map_or(String::new(), |v| v.to_string());
                    let bid_qty = md.bid_qty.map_or(String::new(), |v| v.to_string());
                    let ask_qty = md.ask_qty.map_or(String::new(), |v| v.to_string());
                    let exchange_ts = format_ts(md.exchange_ts);
                    let exchange_ts_raw = format_ts(md.exchange_ts_raw);
                    let received_ts = format_ts(md.received_ts);

                    if with_fp {
                        // Find FP group + per-member params + tick snapshot
                        let empty = String::new;
                        let (group, fp, y, p, vol, bias, sigma_k, fp_snap_ts,
                             fp_mid, fp_tick_exch_ts, fp_tick_recv_ts, fp_hs) =
                        if let Some(ref out) = outputs_clone {
                            let base = t.canonical.split('-').nth(1).unwrap_or("");
                            if let Some(gi) = out.find_group(base) {
                                let fp_cols = if let Some(fp_out) = out.latest(gi) {
                                    (fp_out.fair_price.to_string(),
                                     fp_out.log_fair_price.to_string(),
                                     format!("{:.16e}", (fp_out.uncertainty_bps / 1e4).powi(2)),
                                     format!("{:.4}", fp_out.vol_ann_pct),
                                     fp_out.snap_ts_ns.to_string())
                                } else {
                                    (empty(), empty(), empty(), empty(), empty())
                                };
                                let ex = Exchange::from_str(t.exchange_name);
                                let mi = ex.and_then(|e| out.find_member(gi, &e, t.symbol_id));
                                // Per-member bias and sigma_k
                                let (bias_s, sk_s) = mi
                                    .and_then(|mi| out.get_member_params(gi, mi))
                                    .map(|(b, nv)| (format!("{:.10e}", b), format!("{:.10e}", nv.sqrt())))
                                    .unwrap_or_else(|| (empty(), empty()));
                                // Per-member tick snapshot from engine
                                let tick_snap = mi.and_then(|mi| {
                                    fp_engine.as_ref()?.member_tick_snapshot(gi, mi)
                                });
                                let (fm, ft_e, ft_r, fhs) = match tick_snap {
                                    Some(s) => (
                                        s.raw_mid.to_string(),
                                        s.exchange_ts_ns.to_string(),
                                        s.received_ts_ns.to_string(),
                                        format!("{:.16e}", s.ewma_half_spread),
                                    ),
                                    None => (empty(), empty(), empty(), empty()),
                                };
                                (base.to_string(), fp_cols.0, fp_cols.1, fp_cols.2, fp_cols.3,
                                 bias_s, sk_s, fp_cols.4, fm, ft_e, ft_r, fhs)
                            } else {
                                (empty(), empty(), empty(), empty(), empty(), empty(),
                                 empty(), empty(), empty(), empty(), empty(), empty())
                            }
                        } else {
                            (empty(), empty(), empty(), empty(), empty(), empty(),
                             empty(), empty(), empty(), empty(), empty(), empty())
                        };

                        let _ = writeln!(wtr, "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                            sample_ts, t.canonical, t.exchange_name,
                            bid, ask, bid_qty, ask_qty, exchange_ts, exchange_ts_raw, received_ts,
                            group, fp, y, p, vol, bias, sigma_k, fp_snap_ts,
                            fp_mid, fp_tick_exch_ts, fp_tick_recv_ts, fp_hs);
                    } else {
                        let _ = writeln!(wtr, "{},{},{},{},{},{},{},{},{},{}",
                            sample_ts, t.canonical, t.exchange_name,
                            bid, ask, bid_qty, ask_qty, exchange_ts, exchange_ts_raw, received_ts);
                    }

                    row_count += 1;
                }

                if row_count % 10_000 == 0 {
                    let _ = wtr.flush();
                }
            }
        }))
    } else {
        None
    };

    if args.tick_dump && !args.interval_ms.is_some() {
        eprintln!("Tick dump active (no snapshot interval). Press Ctrl-C to stop.");
    } else if let Some(interval_ms) = args.interval_ms {
        eprintln!(
            "Capturing every {}ms{} -> {}/",
            interval_ms,
            if args.with_fp { " (with FP)" } else { "" },
            args.output_dir
        );
    }

    signal::ctrl_c().await?;
    eprintln!("\nShutting down...");
    shutdown.notify_waiters();

    tokio::time::timeout(Duration::from_secs(5), async {
        if let Some(h) = snap_handle {
            let _ = h.await;
        }
        for h in handles {
            let _ = h.await;
        }
    })
    .await?;

    Ok(())
}
