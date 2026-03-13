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
    FairPriceConfig, FairPriceGroupConfig, FairPriceOutputs, FairQuote, GarchParams, GroupMember,
    run_fair_price_task,
};
use crypto_feeds::market_data::{AllMarketData, Exchange, InstrumentType};
use crypto_feeds::symbol_registry::REGISTRY;

const CACHE_PATH: &str = "/tmp/fair_price_cache.json";

/// Cache key: "group/exchange/symbol_id"
type ParamCache = HashMap<String, (f64, f64)>;

fn cache_key(group: &str, exchange: &str, symbol_id: usize) -> String {
    format!("{}/{}/{}", group, exchange, symbol_id)
}

fn load_cache() -> ParamCache {
    match std::fs::read_to_string(CACHE_PATH) {
        Ok(data) => serde_json::from_str(&data).unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

fn save_cache(outputs: &FairPriceOutputs) {
    let mut cache = ParamCache::new();
    for (group_idx, group_name) in outputs.group_names().iter().enumerate() {
        if let Some(members) = outputs.group_members(group_idx) {
            for (mem_idx, member) in members.iter().enumerate() {
                if let Some((bias, noise_var)) = outputs.get_member_params(group_idx, mem_idx) {
                    let key = cache_key(group_name, member.exchange.as_str(), member.symbol_id);
                    cache.insert(key, (bias, noise_var));
                }
            }
        }
    }
    if let Ok(json) = serde_json::to_string_pretty(&cache) {
        if let Err(e) = std::fs::write(CACHE_PATH, json) {
            log::warn!("Failed to write param cache: {}", e);
        } else {
            log::info!("Saved param cache to {}", CACHE_PATH);
        }
    }
}

fn apply_cache(groups: &mut [FairPriceGroupConfig], cache: &ParamCache) {
    let mut applied = 0;
    for group in groups.iter_mut() {
        for member in &mut group.members {
            let key = cache_key(&group.name, member.exchange.as_str(), member.symbol_id);
            if let Some(&(bias, noise_var)) = cache.get(&key) {
                member.bias = bias;
                member.noise_var = noise_var;
                applied += 1;
            }
        }
    }
    if applied > 0 {
        log::info!("Loaded {} cached params from {}", applied, CACHE_PATH);
    }
}

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

struct ArbResult {
    dir: &'static str,   // "BUY" or "SELL"
    leg2: String,        // "exchange/SYMBOL"
    profit_bps: f64,
}

/// For row `i`, find best arb against all other members in the group.
/// Prices are adjusted to common basis by removing each member's bias:
///   adjusted = price * exp(-m_k)
/// BUY arb:  buy at adj_ask_i, sell at best adj_bid_j → profit in bps
/// SELL arb: sell at adj_bid_i, buy at best adj_ask_j → profit in bps
fn best_arb(
    i: usize,
    quotes: &[(Option<FairQuote>, &str, String, usize)],
) -> Option<ArbResult> {
    let qi = quotes[i].0.as_ref()?;
    let debias_i = (-qi.bias).exp();
    let adj_bid_i = qi.bid * debias_i;
    let adj_ask_i = qi.ask * debias_i;
    let adj_mid_i = qi.mid_at_exchange * debias_i;
    if adj_mid_i <= 0.0 {
        return None;
    }

    let mut best: Option<ArbResult> = None;

    for (j, (qj_opt, ex_name, sym_name, _)) in quotes.iter().enumerate() {
        if j == i {
            continue;
        }
        let qj = match qj_opt {
            Some(q) => q,
            None => continue,
        };
        let debias_j = (-qj.bias).exp();
        let adj_bid_j = qj.bid * debias_j;
        let adj_ask_j = qj.ask * debias_j;

        // BUY this, SELL other: profit = adj_bid_j - adj_ask_i
        let buy_profit = (adj_bid_j - adj_ask_i) / adj_mid_i * BPS;
        if buy_profit >= 0.0 {
            if best.as_ref().map_or(true, |b| buy_profit > b.profit_bps) {
                best = Some(ArbResult {
                    dir: "BUY",
                    leg2: {
                                    let s = format!("{}/{}", &ex_name[..ex_name.len().min(5)], sym_name);
                                    if s.len() > 20 { s[..20].to_string() } else { s }
                                },
                    profit_bps: buy_profit,
                });
            }
        }

        // SELL this, BUY other: profit = adj_bid_i - adj_ask_j
        let sell_profit = (adj_bid_i - adj_ask_j) / adj_mid_i * BPS;
        if sell_profit >= 0.0 {
            if best.as_ref().map_or(true, |b| sell_profit > b.profit_bps) {
                best = Some(ArbResult {
                    dir: "SELL",
                    leg2: {
                                    let s = format!("{}/{}", &ex_name[..ex_name.len().min(5)], sym_name);
                                    if s.len() > 20 { s[..20].to_string() } else { s }
                                },
                    profit_bps: sell_profit,
                });
            }
        }
    }

    best
}

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
                reprice_group: reprice.clone(),
                invert_reprice: reprice.is_some(),
            });
        }

        if members.len() >= 2 {
            groups.push(FairPriceGroupConfig {
                name: base.clone(),
                members,
                garch: GarchParams {
                    alpha: 0.06,
                    beta: 0.94,
                    initial_var: 1e-6,
                    vol_halflife: 5000,
                },
                process_noise_floor: 1e-8,
                recalib: Some(crypto_feeds::fair_price::RecalibConfig {
                    recalibrate_every: 100,
                    prior_weight: 200.0,
                    ruler_halflife: 3000,
                }),
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
                        "========== Fair Price Engine ==========  uptime: {:02}:{:02}:{:02}  tick: {}ms  last_recalib: {}",
                        h, m, sec, out.interval_ms() as u64,
                        {
                            let best_ns = (0..out.group_names().len())
                                .map(|i| out.last_recalib_ns(i))
                                .max()
                                .unwrap_or(0);
                            if best_ns == 0 {
                                "pending".to_string()
                            } else {
                                let ago_s = (now_ns - best_ns) / 1_000_000_000;
                                format!("{}s ago", ago_s)
                            }
                        }
                    );

                    for (group_idx, group_name) in out.group_names().iter().enumerate() {
                        let fp = out.latest(group_idx);

                        let (fair_str, unc_now_str, unc_100_str, vol_str, ticks_str, age_str) = match fp {
                            Some(fp) => {
                                let age_ms = (now_ns - fp.snap_ts_ns) / 1_000_000;
                                let p = (fp.uncertainty_bps / BPS).powi(2);
                                let h = (fp.vol_bps / BPS).powi(2);
                                let p_100 = (p + h).sqrt() * BPS;
                                (
                                    fmt_price(fp.fair_price),
                                    fmt_bps(fp.uncertainty_bps),
                                    fmt_bps(p_100),
                                    fmt_bps(fp.vol_bps),
                                    format!("{}", fp.n_ticks_used),
                                    format!("{}ms", age_ms),
                                )
                            }
                            None => (
                                "-".into(),
                                "-".into(),
                                "-".into(),
                                "-".into(),
                                "-".into(),
                                "-".into(),
                            ),
                        };

                        let _ = writeln!(buf);
                        let _ = writeln!(
                            buf,
                            "--- {} --- fair: {}  P_unc0: {} bps  P_unc1: {} bps  vol: {} bps/\u{221A}tick  ticks: {}  age: {}",
                            group_name, fair_str, unc_now_str, unc_100_str, vol_str, ticks_str, age_str
                        );

                        // Header
                        macro_rules! row {
                            ($buf:expr, $($arg:expr),* $(,)?) => {
                                writeln!($buf, "  {:<5} {:<16} {:>13} {:>13} {:>7} {:>7} {:>7} {:>7} {:>13} {:>13} {:>8} {:>8} {:>7} {:>7} {:>7} {:>4} {:<20} {:>7}", $($arg),*)
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
                            "Arb", "ArbLeg2", "ArbBps",
                        );
                        let _ = row!(buf,
                            "", "", "", "",
                            "(bps)",
                            "(bps)",
                            "(bps)", "(bps)",
                            "", "",
                            "(bps)", "(bps)",
                            "(bps)", "(bps)", "(ms)",
                            "", "", "(bps)",
                        );

                        if let Some(members) = out.group_members(group_idx) {
                            // 1. Collect all quotes + names + member index
                            let mut row_data: Vec<(Option<FairQuote>, &str, String, usize)> = Vec::new();
                            for (mem_idx, member) in members.iter().enumerate() {
                                let ex_name = member.exchange.as_str();
                                let sym_name = member.display_name.as_deref()
                                    .unwrap_or_else(|| REGISTRY.get_symbol(member.symbol_id).unwrap_or("?"))
                                    .to_string();
                                let quote = out.get_fair_quote(
                                    &md,
                                    group_name,
                                    &member.exchange,
                                    member.symbol_id,
                                    0.0,
                                );
                                row_data.push((quote, ex_name, sym_name, mem_idx));
                            }

                            // Sort by sigma_k ascending (noise_var from quote or params)
                            row_data.sort_by(|a, b| {
                                let nv_a = a.0.as_ref().map(|q| q.noise_var)
                                    .unwrap_or_else(|| out.get_member_params(group_idx, a.3).map(|(_, s)| s).unwrap_or(f64::MAX));
                                let nv_b = b.0.as_ref().map(|q| q.noise_var)
                                    .unwrap_or_else(|| out.get_member_params(group_idx, b.3).map(|(_, s)| s).unwrap_or(f64::MAX));
                                nv_a.partial_cmp(&nv_b).unwrap_or(std::cmp::Ordering::Equal)
                            });

                            // 2. Compute arb for each row
                            let arbs: Vec<Option<ArbResult>> = (0..row_data.len())
                                .map(|i| best_arb(i, &row_data))
                                .collect();

                            // 3. Find best arb row
                            let best_arb_idx = arbs.iter().enumerate()
                                .filter_map(|(i, a)| a.as_ref().map(|a| (i, a.profit_bps)))
                                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                                .map(|(i, _)| i);

                            // 4. Render rows
                            for (row_idx, (quote, ex_name, sym_name, orig_mem_idx)) in row_data.iter().enumerate() {
                                let ex_name = &ex_name[..ex_name.len().min(5)];
                                let sym_name = &sym_name[..sym_name.len().min(16)];
                                let is_best = best_arb_idx == Some(row_idx);
                                let color_on = if is_best { GREEN } else { "" };
                                let color_off = if is_best { RESET } else { "" };

                                let (arb_dir, arb_leg2, arb_bps) = match &arbs[row_idx] {
                                    Some(a) => (a.dir, a.leg2.as_str(), fmt_bps(a.profit_bps)),
                                    None => ("", "", "".to_string()),
                                };

                                match quote {
                                    Some(q) => {
                                        let hspread_bps = (q.ask - q.bid) / q.mid_at_exchange * BPS / 2.0;
                                        let mk_bps = q.bias * BPS;
                                        let sk_bps = q.noise_var.sqrt() * BPS;
                                        // Local exchange uncertainty: σ_k² + h * (age / interval)
                                        // time-dilated from last exchange quote to now and now+100ms
                                        let h = (q.vol_bps / BPS).powi(2);
                                        let interval_ms = out.interval_ms();
                                        let age_ms = if q.exchange_ts_ns > 0 {
                                            (now_ns - q.exchange_ts_ns).max(0) as f64 / 1_000_000.0
                                        } else {
                                            0.0
                                        };
                                        let lcu0 = (q.noise_var + h * (age_ms / interval_ms)).sqrt() * BPS;
                                        let lcu1 = (q.noise_var + h * ((age_ms + 100.0) / interval_ms)).sqrt() * BPS;
                                        let _ = write!(buf, "{}", color_on);
                                        let age_str = format!("{:.0}", age_ms);
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
                                            arb_dir, arb_leg2, arb_bps,
                                        );
                                        let _ = write!(buf, "{}", color_off);
                                    }
                                    None => {
                                        let params = out.get_member_params(group_idx, *orig_mem_idx);
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
                                            "", "", "",
                                        );
                                    }
                                }
                            }
                        }
                    }

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

    let clear_cache = std::env::args().any(|a| a == "--clear-cache");
    if clear_cache {
        let _ = std::fs::remove_file(CACHE_PATH);
        log::info!("Cleared param cache");
    }

    let cfg: AppConfig = load_config("configs/config.yaml").context("loading config.yaml")?;

    let market_data = Arc::new(AllMarketData::new());
    let shutdown = Arc::new(Notify::new());
    let mut handles = Vec::new();

    // Start feeds
    let _ = load_spot(&mut handles, &cfg, &market_data, &shutdown);
    let _ = load_perp(&mut handles, &cfg, &market_data, &shutdown);
    let _ = load_onchain(&mut handles, &cfg, &market_data, &shutdown);

    // Auto-discover fair price groups from config
    let mut groups = auto_discover_groups(&cfg);
    if groups.is_empty() {
        log::error!("No pricing groups discovered (need >= 2 members per base asset)");
        return Ok(());
    }

    // Load cached m_k / sigma_k from previous run
    if !clear_cache {
        let cache = load_cache();
        if !cache.is_empty() {
            apply_cache(&mut groups, &cache);
        }
    }

    for g in &groups {
        log::info!(
            "Group '{}': {} members",
            g.name,
            g.members.len()
        );
    }

    let fp_config = FairPriceConfig {
        interval_ms: 100,
        buffer_capacity: 65536,
        groups,
    };

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
    save_cache(&outputs);
    shutdown.notify_waiters();
    tokio::time::timeout(Duration::from_secs(5), async {
        for h in handles {
            let _ = h.await;
        }
    })
    .await?;
    Ok(())
}
