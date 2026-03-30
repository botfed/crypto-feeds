use anyhow::{Context, Result};
use log::error;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;

use crypto_feeds::app_config::{load_config, load_onchain, load_perp, load_spot, AppConfig};
use crypto_feeds::bar_manager::{BarManager, BarSymbol};
use crypto_feeds::display::init_display_logger;
use crypto_feeds::historical_bars::{aggregate_bars, load_1m_bars_with_backfill};
use crypto_feeds::market_data::{AllMarketData, Exchange, InstrumentType};
use crypto_feeds::symbol_registry::REGISTRY;
use crypto_feeds::vol_engine::VolEngine;
use crypto_feeds::vol_params;

const CURSOR_HOME: &str = "\x1B[H";
const CLEAR_BELOW: &str = "\x1B[J";
const CURSOR_HIDE: &str = "\x1B[?25l";
const CURSOR_SHOW: &str = "\x1B[?25h";
const ERASE_EOL: &str = "\x1B[K";

const BPS: f64 = 1e4;

fn term_size() -> (usize, usize) {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut ws = MaybeUninit::<libc::winsize>::zeroed();
            if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, ws.as_mut_ptr()) == 0 {
                let ws = ws.assume_init();
                let cols = if ws.ws_col > 0 { ws.ws_col as usize } else { 120 };
                let rows = if ws.ws_row > 0 { ws.ws_row as usize } else { 50 };
                return (cols, rows);
            }
        }
    }
    let cols = std::env::var("COLUMNS").ok().and_then(|v| v.parse().ok()).unwrap_or(120);
    let rows = std::env::var("LINES").ok().and_then(|v| v.parse().ok()).unwrap_or(50);
    (cols, rows)
}

fn prepare_frame(raw: &str) -> String {
    let (width, _) = term_size();
    let mut out = String::with_capacity(raw.len() + 256);
    for line in raw.lines() {
        let visible: String = line.chars().take(width).collect();
        out.push_str(&visible);
        out.push_str(ERASE_EOL);
        out.push('\n');
    }
    out
}

async fn flush_str(s: String) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let mut out = stdout().lock();
        write!(out, "{}", s)?;
        out.flush()?;
        Ok(())
    })
    .await?
}

struct SymbolVenue {
    exchange: Exchange,
    exchange_name: String,
    symbol_id: usize,
    instrument_type: InstrumentType,
}

fn discover_venues_for_base(cfg: &AppConfig, base: &str) -> Vec<SymbolVenue> {
    let mut venues = Vec::new();
    let prefix = format!("{}_", base);

    for (exchange_name, symbols) in &cfg.spot {
        for sym in symbols {
            if sym.starts_with(&prefix) {
                if let Some(exchange) = Exchange::from_str(exchange_name) {
                    if let Some(&id) = REGISTRY.lookup(sym, &InstrumentType::Spot) {
                        venues.push(SymbolVenue {
                            exchange, exchange_name: exchange_name.clone(),
                            symbol_id: id, instrument_type: InstrumentType::Spot,
                        });
                    }
                }
            }
        }
    }

    for (exchange_name, symbols) in &cfg.perp {
        for sym in symbols {
            if sym.starts_with(&prefix) {
                if let Some(exchange) = Exchange::from_str(exchange_name) {
                    if let Some(&id) = REGISTRY.lookup(sym, &InstrumentType::Perp) {
                        venues.push(SymbolVenue {
                            exchange, exchange_name: exchange_name.clone(),
                            symbol_id: id, instrument_type: InstrumentType::Perp,
                        });
                    }
                }
            }
        }
    }

    venues.sort_by(|a, b| {
        let type_ord = |t: &InstrumentType| match t {
            InstrumentType::Perp => 0, InstrumentType::Spot => 1, _ => 2,
        };
        type_ord(&a.instrument_type)
            .cmp(&type_ord(&b.instrument_type))
            .then(a.exchange_name.cmp(&b.exchange_name))
    });
    venues
}

async fn run_display(
    tick_data: Arc<AllMarketData>,
    engine: Arc<VolEngine>,
    sorted_symbols: Arc<Vec<String>>,
    venues_map: Arc<HashMap<String, Vec<SymbolVenue>>>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    flush_str(format!("{}", CURSOR_HIDE)).await?;

    let start = std::time::Instant::now();
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result = loop {
        tokio::select! {
            _ = &mut shutdown_fut => break Ok(()),
            _ = interval.tick() => {
                let md = Arc::clone(&tick_data);
                let eng = Arc::clone(&engine);
                let syms = Arc::clone(&sorted_symbols);
                let vm = Arc::clone(&venues_map);
                let frame = tokio::task::spawn_blocking(move || {
                    render_frame(&md, &eng, &syms, &vm, start.elapsed().as_secs())
                }).await?;
                let output = prepare_frame(&frame);
                flush_str(format!("{}{}{}", CURSOR_HOME, output, CLEAR_BELOW)).await?;
            }
        }
    };

    flush_str(format!("{}\n", CURSOR_SHOW)).await?;
    result
}

fn render_frame(
    tick_data: &AllMarketData,
    engine: &VolEngine,
    symbols: &[String],
    venues_map: &HashMap<String, Vec<SymbolVenue>>,
    elapsed_secs: u64,
) -> String {
    let h = elapsed_secs / 3600;
    let m = (elapsed_secs % 3600) / 60;
    let s = elapsed_secs % 60;
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

    let mut buf = String::with_capacity(4096);
    let _ = writeln!(buf, "══════ Vol Engine ══════  uptime: {}:{:02}:{:02}", h, m, s);
    let _ = writeln!(buf);

    for sym in symbols {
        let vol_result = engine.predict_now(sym, tick_data);
        let venues = match venues_map.get(sym) {
            Some(v) => v,
            None => continue,
        };

        match &vol_result {
            Ok(p) => {
                let fv = |v: f64| -> String {
                    if v.is_nan() { "--".to_string() } else { format!("{:.1}%", v * 100.0) }
                };
                let _ = writeln!(
                    buf, "══ {}  ({} bars, partial: {:.0}s) {}",
                    sym, p.n_completed_bars, p.partial_bar_age_secs, "═".repeat(50),
                );
                let _ = writeln!(
                    buf, "   HAR-QL: {:<8} GARCH: {:<8} EWMA({}m): {:<8} RV({}m): {}",
                    fv(p.har_qlike), fv(p.garch),
                    p.ewma_halflife_min, fv(p.ewma),
                    p.rv_lookback_min, fv(p.realized),
                );
            }
            Err(status) => {
                let _ = writeln!(buf, "══ {}  ({}) {}", sym, status, "═".repeat(50));
            }
        }

        let _ = writeln!(
            buf, "{:<11} {:<5} {:>14} {:>14} {:>14} {:>10} {:>8}",
            "Xchg", "Type", "Mid", "Bid", "Ask", "Sprd(bps)", "Age(ms)"
        );

        for venue in venues {
            let coll = tick_data.get_collection(&venue.exchange);
            let md = coll.latest(&venue.symbol_id);

            match md {
                Some(md) if md.bid.is_some() && md.ask.is_some() => {
                    let bid = md.bid.unwrap();
                    let ask = md.ask.unwrap();
                    let mid = (bid + ask) / 2.0;
                    let spread_bps = if mid > 0.0 { (ask - bid) / mid * BPS } else { 0.0 };
                    let age_ms = md.exchange_ts.or(md.received_ts)
                        .map(|ts| (now_ns - ts.timestamp_nanos_opt().unwrap_or(0)) / 1_000_000)
                        .unwrap_or(-1);

                    let _ = writeln!(
                        buf, "{:<11} {:<5} {:>14.6} {:>14.6} {:>14.6} {:>10.2} {:>8}",
                        venue.exchange_name, venue.instrument_type.as_str(),
                        mid, bid, ask, spread_bps, age_ms
                    );
                }
                _ => {
                    let _ = writeln!(
                        buf, "{:<11} {:<5} {:>14} {:>14} {:>14} {:>10} {:>8}",
                        venue.exchange_name, venue.instrument_type.as_str(),
                        "-", "-", "-", "-", "-"
                    );
                }
            }
        }
        let _ = writeln!(buf);
    }
    buf
}

#[tokio::main]
async fn main() -> Result<()> {
    init_display_logger(log::LevelFilter::Info);

    let cfg: AppConfig = load_config("configs/config.yaml").context("loading config.yaml")?;
    let vol_cfg = cfg.vol_models.as_ref().context("no vol_models section in config.yaml")?;

    // Load vol params
    let params_dir = Path::new(&vol_cfg.params_dir);
    let all_params = vol_params::load_all_vol_params(params_dir)
        .with_context(|| format!("loading vol params from {}", params_dir.display()))?;

    log::info!(
        "loaded vol params for {} symbols: {:?}",
        all_params.len(), all_params.keys().collect::<Vec<_>>()
    );

    let target_min = all_params.values().next().map(|p| p.target_min).unwrap_or(5);
    let max_window = all_params.values()
        .flat_map(|p| [
            p.har_ols.as_ref().map(|h| h.max_window()),
            p.har_qlike.as_ref().map(|h| h.max_window()),
        ])
        .flatten()
        .max()
        .unwrap_or(288) + 16;

    let market_data = Arc::new(AllMarketData::with_clock_correction(cfg.clock_correction.clone()));
    let shutdown = Arc::new(Notify::new());
    let mut handles = Vec::new();

    // Start feeds
    let _ = load_spot(&mut handles, &cfg, &market_data, &shutdown);
    let _ = load_perp(&mut handles, &cfg, &market_data, &shutdown);
    let _ = load_onchain(&mut handles, &cfg, &market_data, &shutdown);

    // Resolve primary venue per symbol → build BarSymbol list
    let mut bar_symbols = Vec::new();
    let mut param_map = HashMap::new();
    for (sym, params) in all_params {
        let perp_sym = format!("{}_USDT", sym);
        let spot_sym = format!("{}_USDT", sym);
        let venue = if let Some(&id) = REGISTRY.lookup(&perp_sym, &InstrumentType::Perp) {
            Some((Exchange::Binance, id))
        } else if let Some(&id) = REGISTRY.lookup(&spot_sym, &InstrumentType::Spot) {
            Some((Exchange::Binance, id))
        } else {
            log::warn!("no Binance venue found for {}, skipping", sym);
            None
        };
        if let Some((exchange, symbol_id)) = venue {
            bar_symbols.push(BarSymbol { name: sym.clone(), exchange, symbol_id });
            param_map.insert(sym, params);
        }
    }

    // Create bar manager
    let bar_mgr = Arc::new(BarManager::new(bar_symbols, target_min, max_window));

    // Warmup: load from disk + backfill from Binance API
    let bar_data_dir = Path::new(&vol_cfg.bar_data_dir);
    for sym in bar_mgr.symbols() {
        match load_1m_bars_with_backfill(bar_data_dir, &sym, vol_cfg.warmup_days).await {
            Ok(bars_1m) => {
                let target_bars = aggregate_bars(&bars_1m, target_min);
                bar_mgr.warmup(&sym, target_bars, &bars_1m);
            }
            Err(e) => log::warn!("warmup failed for {}: {}", sym, e),
        }
    }

    // Spawn bar maintenance
    handles.push(bar_mgr.spawn_maintenance(Arc::clone(&market_data), Arc::clone(&shutdown)));

    // Create vol engine on top of bar manager
    let engine = Arc::new(VolEngine::new(param_map, Arc::clone(&bar_mgr)));
    engine.replay_history();

    // Discover venues per symbol for display
    let mut venues_map = HashMap::new();
    let mut sorted_symbols: Vec<String> = bar_mgr.symbols();
    sorted_symbols.sort();
    for sym in &sorted_symbols {
        venues_map.insert(sym.clone(), discover_venues_for_base(&cfg, sym));
    }

    // Start display
    {
        let md = Arc::clone(&market_data);
        let eng = Arc::clone(&engine);
        let sd = Arc::clone(&shutdown);
        let syms = Arc::new(sorted_symbols);
        let vm = Arc::new(venues_map);
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_display(md, eng, syms, vm, sd).await {
                error!("display exited with error {:?}", e);
            }
        }));
    }

    signal::ctrl_c().await?;
    shutdown.notify_waiters();
    tokio::time::timeout(Duration::from_secs(5), async {
        for h in handles { let _ = h.await; }
    }).await?;
    Ok(())
}
