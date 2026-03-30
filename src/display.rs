use crate::analytics::{Analytics, AnalyticsScratch, DisplayAnalytics};
use crate::market_data::{AllMarketData, Exchange, MarketDataCollection};
use crate::symbol_registry::{MAX_SYMBOLS, SymbolId, REGISTRY};
use anyhow::Result;
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

const CURSOR_HOME: &str = "\x1B[H";
const CLEAR_BELOW: &str = "\x1B[J";
const CURSOR_HIDE: &str = "\x1B[?25l";
const CURSOR_SHOW: &str = "\x1B[?25h";
const ERASE_EOL: &str = "\x1B[K";
const NUM_EXCHANGES: usize = 14;
const MAX_LOG_LINES: usize = 20;

// --------------- in-memory log capture ---------------

static LOG_BUF: Mutex<VecDeque<String>> = Mutex::new(VecDeque::new());

struct DisplayLogger;

impl log::Log for DisplayLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }
    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let ts = Utc::now().format("%H:%M:%S");
        let line = format!("[{} {:>5}] {}", ts, record.level(), record.args());
        if let Ok(mut buf) = LOG_BUF.lock() {
            if buf.len() >= MAX_LOG_LINES {
                buf.pop_front();
            }
            buf.push_back(line);
        }
    }
    fn flush(&self) {}
}

static LOGGER: DisplayLogger = DisplayLogger;

pub fn init_display_logger(level: log::LevelFilter) {
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(level);
}

pub fn write_log_section(buf: &mut String, max_lines: usize) {
    if max_lines == 0 {
        return;
    }
    if let Ok(logs) = LOG_BUF.lock() {
        if !logs.is_empty() {
            let _ = writeln!(buf, "\n--- Log ---");
            // -1 for the header line above
            let avail = max_lines.saturating_sub(2);
            let skip = logs.len().saturating_sub(avail);
            for line in logs.iter().skip(skip) {
                let _ = writeln!(buf, "  {}", line);
            }
        }
    }
}

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
    let cols = std::env::var("COLUMNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(120);
    let rows = std::env::var("LINES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    (cols, rows)
}

/// Truncate each line to terminal width and append erase-to-EOL so that
/// wrapped lines and stale characters from previous frames don't corrupt
/// the display. This is critical for tmux / SSH where terminals are narrow.
fn prepare_frame(raw: &str) -> String {
    let (cols, _) = term_size();
    let mut out = String::with_capacity(raw.len() + raw.lines().count() * 6);
    for line in raw.lines() {
        // Truncate to terminal width (byte-safe: only ASCII content)
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

pub async fn print_bbo_data(market_data: Arc<AllMarketData>, shutdown: Arc<Notify>) -> Result<()> {
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    flush_str(format!("{}", CURSOR_HIDE)).await?;

    let start = std::time::Instant::now();
    let mut state: Option<([Vec<SymbolId>; NUM_EXCHANGES], AnalyticsScratch)> =
        Some((std::array::from_fn(|_| Vec::new()), AnalyticsScratch::new()));
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            _ = &mut shutdown_fut => {
                break Ok(());
            }
            _ = interval.tick() => {
                let (mut s, mut scratch) = state.take().unwrap();
                let md = Arc::clone(&market_data);
                let (frame, s, scratch) = tokio::task::spawn_blocking(move || {
                    let elapsed = start.elapsed().as_secs();
                    let h = elapsed / 3600;
                    let m = (elapsed % 3600) / 60;
                    let sec = elapsed % 60;
                    let mut buf = String::with_capacity(8192);
                    let mut no_cache = HashMap::new();
                    let _ = writeln!(buf, "========== Market Data Snapshot ==========  uptime: {:02}:{:02}:{:02}", h, m, sec);
                    write_header(&mut buf, false);
                    write_market_collection(&mut buf, "Binance ", &md.binance, None, 0, false, &mut s[0], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "Coinbase", &md.coinbase, None, 1, false, &mut s[1], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "Bybit   ", &md.bybit, None, 2, false, &mut s[2], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "Kraken  ", &md.kraken, None, 3, false, &mut s[3], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "MEXC    ", &md.mexc, None, 4, false, &mut s[4], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "Lighter ", &md.lighter, None, 5, false, &mut s[5], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "Extended", &md.extended, None, 6, false, &mut s[6], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "Nado    ", &md.nado, None, 7, false, &mut s[7], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "OKX     ", &md.okx, None, 8, false, &mut s[8], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "KuCoin  ", &md.kucoin, None, 9, false, &mut s[9], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "BingX   ", &md.bingx, None, 10, false, &mut s[10], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "Apex    ", &md.apex, None, 11, false, &mut s[11], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "Aero    ", &md.aerodrome, None, 12, false, &mut s[12], &mut scratch, &mut no_cache);
                    write_market_collection(&mut buf, "Uniswap ", &md.uniswap, None, 13, false, &mut s[13], &mut scratch, &mut no_cache);
                    let (_, rows) = term_size();
                    let used = buf.lines().count();
                    let remaining = rows.saturating_sub(used);
                    write_log_section(&mut buf, remaining);
                    let frame = prepare_frame(&buf);
                    (format!("{}{}{}", CURSOR_HOME, frame, CLEAR_BELOW), s, scratch)
                }).await?;
                state = Some((s, scratch));
                flush_str(frame).await?;
            }
        }
    };

    flush_str(format!("{}\n", CURSOR_SHOW)).await?;
    result
}

pub async fn print_bbo_with_analytics(
    market_data: Arc<AllMarketData>,
    analytics: Arc<Analytics>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    flush_str(format!("{}", CURSOR_HIDE)).await?;

    let start = std::time::Instant::now();
    let mut tick_count: u64 = 0;
    let mut state: Option<(
        [Vec<SymbolId>; NUM_EXCHANGES],
        AnalyticsScratch,
        HashMap<(usize, SymbolId), DisplayAnalytics>,
        String,
    )> = Some((std::array::from_fn(|_| Vec::new()), AnalyticsScratch::new(), HashMap::new(), String::new()));
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            _ = &mut shutdown_fut => {
                break Ok(());
            }
            _ = interval.tick() => {
                let (mut s, mut scratch, mut acache, mut perf_line) = state.take().unwrap();
                let recompute = tick_count % 10 == 0;
                tick_count += 1;
                let md = Arc::clone(&market_data);
                let a = if recompute { Some(Arc::clone(&analytics)) } else { None };
                let (frame, s, scratch, acache, perf_line) = tokio::task::spawn_blocking(move || {
                    let t0 = std::time::Instant::now();
                    let elapsed = start.elapsed().as_secs();
                    let h = elapsed / 3600;
                    let m = (elapsed % 3600) / 60;
                    let sec = elapsed % 60;
                    let mut buf = String::with_capacity(16384);
                    let _ = writeln!(buf, "========== Market Data + Analytics ==========  uptime: {:02}:{:02}:{:02}", h, m, sec);

                    write_header(&mut buf, true);
                    let mut ex_times = [0.0f64; NUM_EXCHANGES];
                    let ex_names = ["bin", "cb", "byb", "krk", "mxc", "ltr", "ext", "ndo", "okx", "kuc", "bgx", "apx", "aero", "uni"];
                    macro_rules! timed_write {
                        ($i:expr, $name:expr, $coll:expr, $ex:expr) => {{
                            let t = std::time::Instant::now();
                            let ap = a.as_deref().map(|a| (a, &$ex));
                            write_market_collection(&mut buf, $name, &$coll, ap, $i, true, &mut s[$i], &mut scratch, &mut acache);
                            ex_times[$i] = t.elapsed().as_secs_f64() * 1000.0;
                        }};
                    }
                    timed_write!(0, "Binance ", md.binance, Exchange::Binance);
                    timed_write!(1, "Coinbase", md.coinbase, Exchange::Coinbase);
                    timed_write!(2, "Bybit   ", md.bybit, Exchange::Bybit);
                    timed_write!(3, "Kraken  ", md.kraken, Exchange::Kraken);
                    timed_write!(4, "MEXC    ", md.mexc, Exchange::Mexc);
                    timed_write!(5, "Lighter ", md.lighter, Exchange::Lighter);
                    timed_write!(6, "Extended", md.extended, Exchange::Extended);
                    timed_write!(7, "Nado    ", md.nado, Exchange::Nado);
                    timed_write!(8, "OKX     ", md.okx, Exchange::Okx);
                    timed_write!(9, "KuCoin  ", md.kucoin, Exchange::Kucoin);
                    timed_write!(10, "BingX   ", md.bingx, Exchange::Bingx);
                    timed_write!(11, "Apex    ", md.apex, Exchange::Apex);
                    timed_write!(12, "Aero    ", md.aerodrome, Exchange::Aerodrome);
                    timed_write!(13, "Uniswap ", md.uniswap, Exchange::Uniswap);
                    let analytics_ms: f64 = ex_times.iter().sum();

                    if a.is_some() {
                        perf_line.clear();
                        let _ = write!(perf_line, "  render: {:.1}ms (analytics: {:.1}ms) [recompute]", t0.elapsed().as_secs_f64() * 1000.0, analytics_ms);
                        for (i, &t) in ex_times.iter().enumerate() {
                            if t > 0.1 {
                                let _ = write!(perf_line, " {}:{:.1}", ex_names[i], t);
                            }
                        }
                    }
                    let _ = writeln!(buf, "{}", perf_line);

                    let (_, rows) = term_size();
                    let used = buf.lines().count();
                    let remaining = rows.saturating_sub(used);
                    write_log_section(&mut buf, remaining);
                    let frame = prepare_frame(&buf);
                    (format!("{}{}{}", CURSOR_HOME, frame, CLEAR_BELOW), s, scratch, acache, perf_line)
                }).await?;
                state = Some((s, scratch, acache, perf_line));
                flush_str(frame).await?;
            }
        }
    };

    flush_str(format!("{}\n", CURSOR_SHOW)).await?;
    result
}

fn fmt_f0(v: Option<f64>) -> String {
    v.map(|x| format!("{:.0}", x)).unwrap_or_else(|| "-".into())
}

fn fmt_f6(v: Option<f64>) -> String {
    v.map(|x| format!("{:.6}", x)).unwrap_or_else(|| "-".into())
}

fn fmt_f1(v: Option<f64>) -> String {
    v.map(|x| format!("{:.1}", x)).unwrap_or_else(|| "-".into())
}

fn fmt_f2(v: Option<f64>) -> String {
    v.map(|x| format!("{:.2}", x)).unwrap_or_else(|| "-".into())
}

fn write_header(buf: &mut String, has_analytics: bool) {
    if has_analytics {
        let _ = writeln!(
            buf,
            "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>5} {:>8} {:>8} {:>8} {:>8} {:>14} {:>8} {:>10} {:>10} {:>16} {:>12} {:>12} {:>10} {:>6} {:>10} {:>10} {:>6} {:>10}",
            "Symbol", "Mid", "Bid", "BidQty", "Ask", "AskQty",
            "Age",
            "ELp50", "ELp9999", "RLp50", "RLp9999",
            "TWAP(10s)", "Sprd", "MdnSprd(1h)", "Vol(60s)", "MaxJmp(1h@100ms)",
            "MdnRng(1s)", "P99Rng(1s)",
            "BidFl/hr", "BidN", "BidMkout", "AskFl/hr", "AskN", "AskMkout",
        );
        let _ = writeln!(
            buf,
            "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>5} {:>8} {:>8} {:>8} {:>8} {:>14} {:>8} {:>10} {:>10} {:>16} {:>12} {:>12} {:>10} {:>6} {:>10} {:>10} {:>6} {:>10}",
            "", "", "", "", "", "",
            "(ms)",
            "(ms,1h)", "(ms,1h)", "(ms,1h)", "(ms,1h)",
            "", "(bps)", "(bps)", "(bps/s)", "(bps)",
            "(bps,1h)", "(bps,1h)",
            "@p99/2", "", "(bps)", "@p99/2", "", "(bps)",
        );
    } else {
        let _ = writeln!(
            buf,
            "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>6} {:>6} {:>5}",
            "Symbol", "Mid", "Bid", "BidQty", "Ask", "AskQty", "ELat", "RLat", "Age",
        );
        let _ = writeln!(
            buf,
            "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>6} {:>6} {:>5}",
            "", "", "", "", "", "", "(ms)", "(ms)", "(ms)",
        );
    }
}

/// Returns true if new symbols were discovered this frame.
pub fn write_market_collection(
    buf: &mut String,
    exchange_name: &str,
    collection: &MarketDataCollection,
    analytics: Option<(&Analytics, &Exchange)>,
    exchange_idx: usize,
    show_analytics: bool,
    seen: &mut Vec<SymbolId>,
    scratch: &mut AnalyticsScratch,
    cache: &mut HashMap<(usize, SymbolId), DisplayAnalytics>,
) -> bool {
    let now = Utc::now();
    let has_analytics = show_analytics;

    let prev_len = seen.len();
    for id in 0..MAX_SYMBOLS {
        if collection.latest(&id).is_some() && !seen.contains(&id) {
            seen.push(id);
        }
    }
    let changed = seen.len() != prev_len;

    if seen.is_empty() {
        return changed;
    }

    let _ = writeln!(buf, "\n--- {} ---", exchange_name);

    // iterate in insertion order — existing rows never shift
    for &id in seen.iter() {
        let sym = match REGISTRY.get_symbol(id) {
            Some(s) => s,
            None => continue,
        };

        let md = collection.latest(&id);
        let mid = md.and_then(|m| m.midquote());

        let (bid, ask, bid_qty, ask_qty) = match md {
            Some(md) => (md.bid, md.ask, md.bid_qty, md.ask_qty),
            None => (None, None, None, None),
        };

        let age = md
            .and_then(|m| m.exchange_ts)
            .map(|t| format!("{}", (now - t).num_milliseconds().max(0)))
            .unwrap_or_else(|| "-".into());

        let sprd_bps = match (bid, ask, mid) {
            (Some(b), Some(a), Some(m)) => Some((a - b) / m * 10_000.0),
            _ => None,
        };

        const ONE_HOUR: usize = 36_000;

        if has_analytics {
            let da = if let Some((a, ex)) = analytics {
                const BUCKET_1S: usize = 10;
                let da = a.compute_display_analytics(ex, id, ONE_HOUR, 100, 600, BUCKET_1S, scratch);
                if let Some(ref d) = da {
                    cache.insert((exchange_idx, id), d.clone());
                }
                da
            } else {
                cache.get(&(exchange_idx, id)).cloned()
            };

            let (twap, mdn_sprd_bps, vol_bps_s, max_jump_bps,
                 el_p50, el_p9999, rl_p50, rl_p9999,
                 mdn_rng_bps, p99_rng_bps,
                 bid_fills_hr, bid_n_fills, bid_mkout,
                 ask_fills_hr, ask_n_fills, ask_mkout,
            ) = match da {
                Some(da) => {
                    let mdn_sprd = da.mdn_spread
                        .and_then(|s| mid.map(|m| s / m * 10_000.0));
                    let vol = da.vol
                        .map(|v| v * 10.0_f64.sqrt() * 10_000.0);
                    let max_jump = da.max_jump
                        .map(|v| v * 10_000.0);

                    let fill_to_fph = |r: &crate::analytics::QuoteFillResult| {
                        let denom = r.elapsed_secs.min(3600.0);
                        if denom > 0.0 { r.n_fills as f64 * 3600.0 / denom } else { 0.0 }
                    };

                    let (bf, bn, bm) = match &da.bid_fill {
                        Some(r) => (Some(fill_to_fph(r)), Some(r.n_fills as f64), Some(r.mean_markout_bps)),
                        None => (None, None, None),
                    };
                    let (af, an, am) = match &da.ask_fill {
                        Some(r) => (Some(fill_to_fph(r)), Some(r.n_fills as f64), Some(r.mean_markout_bps)),
                        None => (None, None, None),
                    };

                    (da.twap, mdn_sprd, vol, max_jump,
                     da.el_p50, da.el_p9999, da.rl_p50, da.rl_p9999,
                     da.mdn_rng, da.p99_rng,
                     bf, bn, bm, af, an, am)
                }
                None => (None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
            };

            let _ = writeln!(
                buf,
                "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>5} {:>8} {:>8} {:>8} {:>8} {:>14} {:>8} {:>10} {:>10} {:>16} {:>12} {:>12} {:>10} {:>6} {:>10} {:>10} {:>6} {:>10}",
                sym,
                fmt_f6(mid),
                fmt_f6(bid),
                fmt_f2(bid_qty),
                fmt_f6(ask),
                fmt_f2(ask_qty),
                age,
                fmt_f1(el_p50),
                fmt_f1(el_p9999),
                fmt_f1(rl_p50),
                fmt_f1(rl_p9999),
                fmt_f6(twap),
                fmt_f2(sprd_bps),
                fmt_f2(mdn_sprd_bps),
                fmt_f2(vol_bps_s),
                fmt_f2(max_jump_bps),
                fmt_f2(mdn_rng_bps),
                fmt_f2(p99_rng_bps),
                fmt_f2(bid_fills_hr),
                fmt_f0(bid_n_fills),
                fmt_f2(bid_mkout),
                fmt_f2(ask_fills_hr),
                fmt_f0(ask_n_fills),
                fmt_f2(ask_mkout),
            );
        } else {
            let (e_lat, r_lat) = match md {
                Some(md) => (
                    match (md.received_ts, md.exchange_ts) {
                        (Some(r), Some(e)) => format!("{}", (r - e).num_milliseconds()),
                        _ => "-".into(),
                    },
                    md.received_ts
                        .map(|t| format!("{}", (now - t).num_milliseconds()))
                        .unwrap_or_else(|| "-".into()),
                ),
                None => ("-".into(), "-".into()),
            };
            let _ = writeln!(
                buf,
                "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>6} {:>6} {:>5}",
                sym,
                fmt_f6(mid),
                fmt_f6(bid),
                fmt_f2(bid_qty),
                fmt_f6(ask),
                fmt_f2(ask_qty),
                e_lat,
                r_lat,
                age,
            );
        }
    }
    changed
}
