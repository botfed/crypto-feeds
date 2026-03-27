use anyhow::{Context, Result};
use log::error;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;

use crypto_feeds::app_config::{AppConfig, load_config, load_onchain, load_perp, load_spot};
use crypto_feeds::display::init_display_logger;
use crypto_feeds::fair_price::{
    FairPriceConfig, FairPriceGroupConfig, FairPriceModel, FairPriceOutputs, GroupMember,
    SigmaMode, load_beacon, run_fair_price_task,
};
use crypto_feeds::market_data::{AllMarketData, Exchange, InstrumentType};
use crypto_feeds::symbol_registry::REGISTRY;
use std::path::Path;

const BEACON_PATH: &str = "configs/beacon.yaml";
/// Default h_per_ms from 100% annualized vol
const DEFAULT_H_PER_MS: f64 = 1.0 / (365.25 * 24.0 * 3600.0 * 1000.0);

const ALT_SCREEN_ON: &str = "\x1B[?1049h";
const ALT_SCREEN_OFF: &str = "\x1B[?1049l";
const CURSOR_HOME: &str = "\x1B[H";
const CLEAR_BELOW: &str = "\x1B[J";
const CURSOR_HIDE: &str = "\x1B[?25l";
const CURSOR_SHOW: &str = "\x1B[?25h";
const ERASE_EOL: &str = "\x1B[K";
const GREEN: &str = "\x1B[32m";
const RESET: &str = "\x1B[0m";
const BPS: f64 = 1e4;

fn term_size() -> (usize, usize) {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut ws = MaybeUninit::<libc::winsize>::zeroed();
            if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, ws.as_mut_ptr()) == 0 {
                let ws = ws.assume_init();
                let cols = if ws.ws_col > 0 { ws.ws_col as usize } else { 160 };
                let rows = if ws.ws_row > 0 { ws.ws_row as usize } else { 50 };
                return (cols, rows);
            }
        }
    }
    (160, 50)
}

fn prepare_frame(raw: &str) -> String {
    let (cols, _) = term_size();
    let mut out = String::with_capacity(raw.len() + raw.lines().count() * 6);
    for line in raw.lines() {
        if line.len() > cols {
            out.push_str(&line[..cols]);
        } else {
            out.push_str(line);
        }
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

/// Auto-discover pricing groups from config.yaml.
///
/// Groups symbols by base asset. For each base, collects all (exchange, symbol_id)
/// pairs across spot and perp sections. The underlying is "BASE_USD" style.
fn auto_discover_groups(cfg: &AppConfig) -> Vec<FairPriceGroupConfig> {
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
                for pool in &pools.pools {
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

fn fmt_price(v: f64) -> String {
    format!("{:.6}", v)
}

fn fmt_bps(v: f64) -> String {
    format!("{:.2}", v)
}

fn fmt_qty(v: f64) -> String {
    if v.is_nan() {
        "-".to_string()
    } else {
        format!("{:.4}", v)
    }
}

async fn run_display(
    tick_data: Arc<AllMarketData>,
    outputs: Arc<FairPriceOutputs>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    flush_str(format!("{}{}", ALT_SCREEN_ON, CURSOR_HIDE)).await?;

    let start = std::time::Instant::now();
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result = loop {
        tokio::select! {
            _ = &mut shutdown_fut => {
                break Ok(());
            }
            _ = interval.tick() => {
                let md = Arc::clone(&tick_data);
                let out = Arc::clone(&outputs);
                let frame = tokio::task::spawn_blocking(move || {
                    let elapsed = start.elapsed().as_secs();
                    let h = elapsed / 3600;
                    let m = (elapsed % 3600) / 60;
                    let sec = elapsed % 60;

                    let mut buf = String::with_capacity(8192);
                    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

                    let _ = writeln!(
                        buf,
                        "========== Fair Price Engine (static) ==========  uptime: {:02}:{:02}:{:02}",
                        h, m, sec,
                    );

                    for (group_idx, group_name) in out.group_names().iter().enumerate() {
                        let fp = out.latest(group_idx);

                        let (fair_str, unc_str, vol_ann_str, ticks_str, age_str) = match fp {
                            Some(fp) => {
                                let age_ms = (now_ns - fp.snap_ts_ns) / 1_000_000;
                                let vol_ann = if fp.vol_ann_pct.is_finite() {
                                    format!("{:.1}%", fp.vol_ann_pct)
                                } else {
                                    "-".into()
                                };
                                (
                                    fmt_price(fp.fair_price),
                                    fmt_bps(fp.uncertainty_bps),
                                    vol_ann,
                                    format!("{}", fp.n_ticks_used),
                                    format!("{}ms", age_ms),
                                )
                            }
                            None => (
                                "-".into(), "-".into(), "-".into(), "-".into(), "-".into(),
                            ),
                        };

                        let _ = writeln!(buf);
                        let _ = writeln!(
                            buf,
                            "--- {} --- fair: {}  P_unc: {} bps  vol: {} ann  ticks: {}  age: {}",
                            group_name, fair_str, unc_str, vol_ann_str, ticks_str, age_str
                        );

                        // Header
                        macro_rules! row {
                            ($buf:expr, $($arg:expr),* $(,)?) => {
                                writeln!($buf, "  {:<5} {:<16} {:>13} {:>13} {:>7} {:>7} {:>7} {:>7} {:>13} {:>13} {:>8} {:>8} {:>7} {:>7} {:>7}", $($arg),*)
                            }
                        }
                        let _ = row!(buf,
                            "Xchg", "Symbol", "Mid@Ex", "Fair@Ex",
                            "EdgMid",
                            "HSprd",
                            "EdgBid", "EdgAsk",
                            "BidQty", "AskQty",
                            "m_k", "sigma_k",
                            "P_unc0", "P_unc1", "Age",
                        );
                        let _ = row!(buf,
                            "", "", "", "",
                            "(bps)",
                            "(bps)",
                            "(bps)", "(bps)",
                            "", "",
                            "(bps)", "(bps)",
                            "(bps)", "(bps)", "(ms)",
                        );

                        if let Some(members) = out.group_members(group_idx) {
                            for (mem_idx, member) in members.iter().enumerate() {
                                let ex_name = &member.exchange.as_str()[..member.exchange.as_str().len().min(5)];
                                let sym_full = member.display_name.as_deref()
                                    .unwrap_or_else(|| REGISTRY.get_symbol(member.symbol_id).unwrap_or("?"));
                                let sym_name = &sym_full[..sym_full.len().min(16)];
                                let quote = out.get_fair_quote(
                                    &md,
                                    group_name,
                                    &member.exchange,
                                    member.symbol_id,
                                    0.0,
                                );

                                match quote {
                                    Some(q) => {
                                        let hspread_bps = (q.ask - q.bid) / q.mid_at_exchange * BPS / 2.0;
                                        let mk_bps = q.bias * BPS;
                                        let sk_bps = q.noise_var.sqrt() * BPS;
                                        let h_ms = out.h_per_ms(group_idx);
                                        let age_ms = if q.exchange_ts_ns > 0 {
                                            (now_ns - q.exchange_ts_ns).max(0) as f64 / 1_000_000.0
                                        } else {
                                            0.0
                                        };
                                        let lcu0 = (q.noise_var + h_ms * age_ms).sqrt() * BPS;
                                        let lcu1 = (q.noise_var + h_ms * (age_ms + 100.0)).sqrt() * BPS;

                                        // Highlight if edge > half spread
                                        let edge_exceeds = q.edge_mid_bps.abs() > hspread_bps;
                                        let color_on = if edge_exceeds { GREEN } else { "" };
                                        let color_off = if edge_exceeds { RESET } else { "" };

                                        let age_str = format!("{:.2}", age_ms);
                                        let _ = write!(buf, "{}", color_on);
                                        let _ = row!(buf,
                                            ex_name,
                                            sym_name,
                                            fmt_price(q.mid_at_exchange),
                                            fmt_price(q.fair_at_exchange),
                                            fmt_bps(q.edge_mid_bps),
                                            fmt_bps(hspread_bps),
                                            fmt_bps(q.edge_bid_bps),
                                            fmt_bps(q.edge_ask_bps),
                                            fmt_qty(q.bid_qty),
                                            fmt_qty(q.ask_qty),
                                            fmt_bps(mk_bps),
                                            fmt_bps(sk_bps),
                                            fmt_bps(lcu0),
                                            fmt_bps(lcu1),
                                            age_str,
                                        );
                                        let _ = write!(buf, "{}", color_off);
                                    }
                                    None => {
                                        let params = out.get_member_params(group_idx, mem_idx);
                                        let (mk, sk) = params.unwrap_or((0.0, 0.0));
                                        let mk_bps = mk * BPS;
                                        let sk_bps = sk.sqrt() * BPS;
                                        let _ = row!(buf,
                                            ex_name,
                                            sym_name,
                                            "-", "-",
                                            "-",
                                            "-",
                                            "-", "-",
                                            "-", "-",
                                            fmt_bps(mk_bps),
                                            fmt_bps(sk_bps),
                                            "-", "-", "-",
                                        );
                                    }
                                }
                            }
                        }
                    }

                    crypto_feeds::display::write_log_section(&mut buf, 10);
                    let frame = prepare_frame(&buf);
                    format!("{}{}{}", CURSOR_HOME, frame, CLEAR_BELOW)
                }).await?;
                flush_str(frame).await?;
            }
        }
    };

    flush_str(format!("{}{}", CURSOR_SHOW, ALT_SCREEN_OFF)).await?;
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    init_display_logger(log::LevelFilter::Info);

    let beacon_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| BEACON_PATH.to_string());

    let cfg: AppConfig = load_config("configs/config.yaml").context("loading config.yaml")?;

    let market_data = Arc::new(AllMarketData::new());
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

    // Build config, then load beacon params (h_per_ms, bias, noise_var)
    let beacon = Path::new(&beacon_path);
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
        log::info!(
            "Group '{}': {} members, h_per_ms={:.2e}",
            g.name,
            g.members.len(),
            g.h_per_ms,
        );
    }

    let outputs = Arc::new(FairPriceOutputs::new(&fp_config));

    // Start fair price task
    {
        let tick = Arc::clone(&market_data);
        let out = Arc::clone(&outputs);
        let sd = Arc::clone(&shutdown);
        handles.push(tokio::spawn(run_fair_price_task(tick, out, fp_config, sd, None)));
    }

    // Start display
    {
        let md = Arc::clone(&market_data);
        let out = Arc::clone(&outputs);
        let sd = Arc::clone(&shutdown);
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_display(md, out, sd).await {
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
