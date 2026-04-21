use anyhow::Result;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::sync::Arc;
use tokio::sync::Notify;

use crate::app_config::AppConfig;
use crate::display::write_log_section;
use crate::fp_display::{
    CURSOR_HOME, CURSOR_HIDE, CURSOR_SHOW, CLEAR_BELOW, RESET,
    flush_str, prepare_frame, term_size, fmt_price, fmt_qty,
};
use crate::market_data::{AllMarketData, Exchange, InstrumentType, MarketDataCollection};
use crate::symbol_registry::{REGISTRY, SymbolId};

struct FeedTarget {
    exchange_name: &'static str,
    canonical: String,
    collection: Arc<MarketDataCollection>,
    symbol_id: SymbolId,
    /// Base asset for grouping (e.g. "BTC")
    base: String,
}

/// Per-target rolling stats, updated each render cycle.
struct TargetStats {
    prev_write_count: u64,
    prev_ts: std::time::Instant,
    msgs_per_sec: f64,
}

fn build_feed_targets(cfg: &AppConfig, market_data: &AllMarketData) -> Vec<FeedTarget> {
    let mut targets = Vec::new();

    for (exchange, coll) in market_data.iter() {
        let name = exchange.as_str();
        // Leak to get 'static — matches fp_display pattern. Only called once at startup.
        let name_static: &'static str = Box::leak(name.to_string().into_boxed_str());

        for (syms, itype) in [
            (cfg.spot.get(name), InstrumentType::Spot),
            (cfg.perp.get(name), InstrumentType::Perp),
        ] {
            if let Some(syms) = syms {
                for raw in syms {
                    if let Some(&id) = REGISTRY.lookup(raw, &itype) {
                        let canonical = REGISTRY.get_symbol(id).unwrap_or(raw).to_string();
                        let base = raw.split('_').next().unwrap_or(raw).to_string();
                        targets.push(FeedTarget {
                            exchange_name: name_static,
                            canonical,
                            collection: Arc::clone(coll),
                            symbol_id: id,
                            base,
                        });
                    }
                }
            }
        }
    }

    // On-chain pools
    if let Some(ref onchain) = cfg.onchain {
        for (dex_name, dex_cfg, coll) in [
            ("aerodrome", &onchain.aerodrome, &market_data.aerodrome),
            ("uniswap", &onchain.uniswap, &market_data.uniswap),
        ] {
            if let Some(pools) = dex_cfg {
                let name_static: &'static str = Box::leak(dex_name.to_string().into_boxed_str());
                let _exchange = match Exchange::from_str(dex_name) {
                    Some(e) => e,
                    None => continue,
                };
                for pool in &pools.validated_pools(dex_name) {
                    if let Some(&id) = REGISTRY.lookup(&pool.symbol, &InstrumentType::Spot) {
                        let canonical = REGISTRY.get_symbol(id).unwrap_or(&pool.symbol).to_string();
                        let base = pool.symbol.split('_').next().unwrap_or(&pool.symbol).to_string();
                        targets.push(FeedTarget {
                            exchange_name: name_static,
                            canonical,
                            collection: Arc::clone(coll),
                            symbol_id: id,
                            base,
                        });
                    }
                }
            }
        }
    }

    targets
}

pub async fn run_feed_display(
    market_data: Arc<AllMarketData>,
    cfg: Arc<AppConfig>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    flush_str(format!("{}", CURSOR_HIDE)).await?;

    let targets = build_feed_targets(&cfg, &market_data);
    let start = std::time::Instant::now();
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Per-target stats keyed by (exchange_name, symbol_id)
    let mut stats: HashMap<(&'static str, SymbolId), TargetStats> = HashMap::new();

    // Init stats
    let now = std::time::Instant::now();
    for t in &targets {
        let wc = t.collection.write_count(&t.symbol_id);
        stats.insert((t.exchange_name, t.symbol_id), TargetStats {
            prev_write_count: wc,
            prev_ts: now,
            msgs_per_sec: 0.0,
        });
    }

    let result = loop {
        tokio::select! {
            _ = &mut shutdown_fut => break Ok(()),
            _ = interval.tick() => {
                let now_mono = std::time::Instant::now();
                let now_utc = chrono::Utc::now();

                // Update msg/sec stats
                for t in &targets {
                    let wc = t.collection.write_count(&t.symbol_id);
                    let key = (t.exchange_name, t.symbol_id);
                    if let Some(s) = stats.get_mut(&key) {
                        let dt = now_mono.duration_since(s.prev_ts).as_secs_f64();
                        if dt > 0.01 {
                            let delta = wc.saturating_sub(s.prev_write_count) as f64;
                            // EWMA with ~4s halflife at 500ms render interval
                            let alpha = 0.15;
                            let instant_rate = delta / dt;
                            s.msgs_per_sec = s.msgs_per_sec * (1.0 - alpha) + instant_rate * alpha;
                            s.prev_write_count = wc;
                            s.prev_ts = now_mono;
                        }
                    }
                }

                // Build frame
                let elapsed = start.elapsed().as_secs();
                let h = elapsed / 3600;
                let m = (elapsed % 3600) / 60;
                let sec = elapsed % 60;

                let mut buf = String::with_capacity(8192);
                let _ = writeln!(buf,
                    "========== Feed Health Monitor ==========  uptime: {:02}:{:02}:{:02}  targets: {}",
                    h, m, sec, targets.len(),
                );

                // Group targets by base asset
                let mut bases: Vec<String> = Vec::new();
                let mut base_indices: HashMap<String, Vec<usize>> = HashMap::new();
                for (i, t) in targets.iter().enumerate() {
                    base_indices.entry(t.base.clone()).or_insert_with(|| {
                        bases.push(t.base.clone());
                        Vec::new()
                    }).push(i);
                }
                bases.sort();

                for base in &bases {
                    let indices = &base_indices[base];
                    let _ = writeln!(buf);
                    let _ = writeln!(buf, "--- {} ---", base);

                    // Header
                    let _ = writeln!(buf,
                        "  {:<12} {:<18} {:>12} {:>12} {:>10} {:>10} {:>12} {:>8} {:>9} {:>7}",
                        "Exchange", "Symbol", "Bid", "Ask", "BidQty", "AskQty", "Mid", "Sprd", "Lat p50", "msg/s",
                    );
                    let _ = writeln!(buf,
                        "  {:<12} {:<18} {:>12} {:>12} {:>10} {:>10} {:>12} {:>8} {:>9} {:>7}",
                        "", "", "", "", "", "", "", "(bps)", "(ms)", "",
                    );

                    for &idx in indices {
                        let t = &targets[idx];
                        let md = t.collection.latest(&t.symbol_id);
                        let key = (t.exchange_name, t.symbol_id);
                        let mps = stats.get(&key).map(|s| s.msgs_per_sec).unwrap_or(0.0);

                        match md {
                            Some(md) => {
                                let bid = md.bid.unwrap_or(0.0);
                                let ask = md.ask.unwrap_or(0.0);
                                let mid = if bid > 0.0 && ask > 0.0 { (bid + ask) / 2.0 } else { 0.0 };
                                let spread_bps = if mid > 0.0 { (ask - bid) / mid * 1e4 } else { 0.0 };

                                // Latency: received_ts - exchange_ts_raw
                                let lat_str = match (md.received_ts, md.exchange_ts_raw) {
                                    (Some(recv), Some(exch)) => {
                                        let lat_ms = (recv - exch).num_microseconds()
                                            .map(|us| us as f64 / 1000.0)
                                            .unwrap_or(0.0);
                                        if lat_ms.abs() < 100_000.0 {
                                            format!("{:.1}", lat_ms)
                                        } else {
                                            "-".to_string()
                                        }
                                    }
                                    _ => "-".to_string(),
                                };

                                // Age for staleness coloring
                                let age_ms = md.exchange_ts.or(md.received_ts)
                                    .map(|t| (now_utc - t).num_milliseconds().max(0) as f64)
                                    .unwrap_or(f64::MAX);
                                let stale = age_ms > 5000.0;
                                const DIM: &str = "\x1B[2m";

                                let color_on = if stale { DIM } else { "" };
                                let color_off = if stale { RESET } else { "" };

                                let _ = write!(buf, "{}", color_on);
                                let _ = writeln!(buf,
                                    "  {:<12} {:<18} {:>12} {:>12} {:>10} {:>10} {:>12} {:>8} {:>9} {:>7.1}",
                                    t.exchange_name,
                                    t.canonical,
                                    fmt_price(bid),
                                    fmt_price(ask),
                                    fmt_qty(md.bid_qty.unwrap_or(f64::NAN)),
                                    fmt_qty(md.ask_qty.unwrap_or(f64::NAN)),
                                    fmt_price(mid),
                                    format!("{:.2}", spread_bps),
                                    lat_str,
                                    mps,
                                );
                                let _ = write!(buf, "{}", color_off);
                            }
                            None => {
                                const DIM: &str = "\x1B[2m";
                                let _ = writeln!(buf,
                                    "  {DIM}{:<12} {:<18} {:>12} {:>12} {:>10} {:>10} {:>12} {:>8} {:>9} {:>7}{RESET}",
                                    t.exchange_name, t.canonical,
                                    "-", "-", "-", "-", "-", "-", "-", "-",
                                );
                            }
                        }
                    }
                }

                let (_, rows) = term_size();
                let used = buf.lines().count();
                let remaining = rows.saturating_sub(used);
                write_log_section(&mut buf, remaining);
                let frame = prepare_frame(&buf);
                flush_str(format!("{}{}{}", CURSOR_HOME, frame, CLEAR_BELOW)).await?;
            }
        }
    };

    flush_str(format!("{}\n", CURSOR_SHOW)).await?;
    result
}
