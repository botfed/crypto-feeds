use crate::analytics::Analytics;
use crate::market_data::{AllMarketData, Exchange, MarketDataCollection};
use crate::symbol_registry::{MAX_SYMBOLS, SymbolId, REGISTRY};
use anyhow::Result;
use chrono::Utc;
use std::collections::VecDeque;
use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

const ALT_SCREEN_ON: &str = "\x1B[?1049h";
const ALT_SCREEN_OFF: &str = "\x1B[?1049l";
const CURSOR_HOME: &str = "\x1B[H";
const CLEAR_BELOW: &str = "\x1B[J";
const CURSOR_HIDE: &str = "\x1B[?25l";
const CURSOR_SHOW: &str = "\x1B[?25h";
const ERASE_EOL: &str = "\x1B[K";
const NUM_EXCHANGES: usize = 6;
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

fn write_log_section(buf: &mut String) {
    if let Ok(logs) = LOG_BUF.lock() {
        if !logs.is_empty() {
            let _ = writeln!(buf, "\n--- Log ---");
            for line in logs.iter() {
                let _ = writeln!(buf, "  {}", line);
            }
        }
    }
}

fn term_width() -> usize {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut ws = MaybeUninit::<libc::winsize>::zeroed();
            if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, ws.as_mut_ptr()) == 0 {
                let ws = ws.assume_init();
                if ws.ws_col > 0 {
                    return ws.ws_col as usize;
                }
            }
        }
    }
    // Fallback: check COLUMNS env var, or default to 120
    std::env::var("COLUMNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(120)
}

/// Truncate each line to terminal width and append erase-to-EOL so that
/// wrapped lines and stale characters from previous frames don't corrupt
/// the display. This is critical for tmux / SSH where terminals are narrow.
fn prepare_frame(raw: &str) -> String {
    let cols = term_width();
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

fn flush_str(s: &str) -> Result<()> {
    let mut out = stdout().lock();
    write!(out, "{}", s)?;
    out.flush()?;
    Ok(())
}

pub async fn print_bbo_data(market_data: Arc<AllMarketData>, shutdown: Arc<Notify>) -> Result<()> {
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    flush_str(&format!("{}{}", ALT_SCREEN_ON, CURSOR_HIDE))?;

    let start = std::time::Instant::now();
    let mut seen: [Vec<SymbolId>; NUM_EXCHANGES] = std::array::from_fn(|_| Vec::new());
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    let result = loop {
        tokio::select! {
            _ = &mut shutdown_fut => {
                break Ok(());
            }
            _ = interval.tick() => {
                let elapsed = start.elapsed().as_secs();
                let h = elapsed / 3600;
                let m = (elapsed % 3600) / 60;
                let s = elapsed % 60;
                let mut buf = String::with_capacity(8192);
                let _ = writeln!(buf, "========== Market Data Snapshot ==========  uptime: {:02}:{:02}:{:02}", h, m, s);
                write_header(&mut buf, false);
                write_market_collection(&mut buf, "Binance ", &market_data.binance, None, None, &mut seen[0]);
                write_market_collection(&mut buf, "Coinbase", &market_data.coinbase, None, None, &mut seen[1]);
                write_market_collection(&mut buf, "Bybit   ", &market_data.bybit, None, None, &mut seen[2]);
                write_market_collection(&mut buf, "Kraken  ", &market_data.kraken, None, None, &mut seen[3]);
                write_market_collection(&mut buf, "MEXC    ", &market_data.mexc, None, None, &mut seen[4]);
                write_market_collection(&mut buf, "Lighter ", &market_data.lighter, None, None, &mut seen[5]);
                write_log_section(&mut buf);
                let frame = prepare_frame(&buf);
                flush_str(&format!("{}{}{}", CURSOR_HOME, frame, CLEAR_BELOW))?;
            }
        }
    };

    flush_str(&format!("{}{}", CURSOR_SHOW, ALT_SCREEN_OFF))?;
    result
}

pub async fn print_bbo_with_analytics(
    market_data: Arc<AllMarketData>,
    analytics: Arc<Analytics>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    flush_str(&format!("{}{}", ALT_SCREEN_ON, CURSOR_HIDE))?;

    let start = std::time::Instant::now();
    let mut seen: [Vec<SymbolId>; NUM_EXCHANGES] = std::array::from_fn(|_| Vec::new());
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
    let result = loop {
        tokio::select! {
            _ = &mut shutdown_fut => {
                break Ok(());
            }
            _ = interval.tick() => {
                let elapsed = start.elapsed().as_secs();
                let h = elapsed / 3600;
                let m = (elapsed % 3600) / 60;
                let s = elapsed % 60;
                let mut buf = String::with_capacity(16384);
                let _ = writeln!(buf, "========== Market Data + Analytics ==========  uptime: {:02}:{:02}:{:02}", h, m, s);
                write_header(&mut buf, true);
                let a = &analytics;
                write_market_collection(&mut buf, "Binance ", &market_data.binance, Some(a), Some(&Exchange::Binance), &mut seen[0]);
                write_market_collection(&mut buf, "Coinbase", &market_data.coinbase, Some(a), Some(&Exchange::Coinbase), &mut seen[1]);
                write_market_collection(&mut buf, "Bybit   ", &market_data.bybit, Some(a), Some(&Exchange::Bybit), &mut seen[2]);
                write_market_collection(&mut buf, "Kraken  ", &market_data.kraken, Some(a), Some(&Exchange::Kraken), &mut seen[3]);
                write_market_collection(&mut buf, "MEXC    ", &market_data.mexc, Some(a), Some(&Exchange::Mexc), &mut seen[4]);
                write_market_collection(&mut buf, "Lighter ", &market_data.lighter, Some(a), Some(&Exchange::Lighter), &mut seen[5]);
                write_log_section(&mut buf);
                let frame = prepare_frame(&buf);
                flush_str(&format!("{}{}{}", CURSOR_HOME, frame, CLEAR_BELOW))?;
            }
        }
    };

    flush_str(&format!("{}{}", CURSOR_SHOW, ALT_SCREEN_OFF))?;
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
            "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>8} {:>8} {:>8} {:>8} {:>14} {:>8} {:>10} {:>10} {:>16} {:>12} {:>12} {:>10} {:>6} {:>10} {:>10} {:>6} {:>10}",
            "Symbol", "Mid", "Bid", "BidQty", "Ask", "AskQty",
            "ELp50", "ELp9999", "RLp50", "RLp9999",
            "TWAP(10s)", "Sprd", "MdnSprd(1h)", "Vol(60s)", "MaxJmp(1h@100ms)",
            "MdnRng(1s)", "P99Rng(1s)",
            "BidFl/hr", "BidN", "BidMkout", "AskFl/hr", "AskN", "AskMkout",
        );
        let _ = writeln!(
            buf,
            "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>8} {:>8} {:>8} {:>8} {:>14} {:>8} {:>10} {:>10} {:>16} {:>12} {:>12} {:>10} {:>6} {:>10} {:>10} {:>6} {:>10}",
            "", "", "", "", "", "",
            "(ms,1h)", "(ms,1h)", "(ms,1h)", "(ms,1h)",
            "", "(bps)", "(bps)", "(bps/s)", "(bps)",
            "(bps,1h)", "(bps,1h)",
            "@p99/2", "", "(bps)", "@p99/2", "", "(bps)",
        );
    } else {
        let _ = writeln!(
            buf,
            "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>6} {:>6}",
            "Symbol", "Mid", "Bid", "BidQty", "Ask", "AskQty", "ELat", "RLat",
        );
        let _ = writeln!(
            buf,
            "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>6} {:>6}",
            "", "", "", "", "", "", "(ms)", "(ms)",
        );
    }
}

/// Returns true if new symbols were discovered this frame.
pub fn write_market_collection(
    buf: &mut String,
    exchange_name: &str,
    collection: &MarketDataCollection,
    analytics: Option<&Analytics>,
    exchange: Option<&Exchange>,
    seen: &mut Vec<SymbolId>,
) -> bool {
    let now = Utc::now();
    let has_analytics = analytics.is_some();

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

        let sprd_bps = match (bid, ask, mid) {
            (Some(b), Some(a), Some(m)) => Some((a - b) / m * 10_000.0),
            _ => None,
        };

        const ONE_HOUR: usize = 36_000;

        if has_analytics {
            use crate::analytics::SnapshotField;

            let (twap, mdn_sprd_bps, vol_bps_s, max_jump_bps,
                 el_p50, el_p9999, rl_p50, rl_p9999,
                 mdn_rng_bps, p99_rng_bps,
                 bid_fills_hr, bid_n_fills, bid_mkout,
                 ask_fills_hr, ask_n_fills, ask_mkout,
            ) = match (analytics, exchange) {
                (Some(a), Some(ex)) => {
                    use crate::analytics::{QuoteSide, RangeStat};
                    const BUCKET_1S: usize = 10; // 10 snaps @ 100ms = 1s
                    let twap = a.midquote_twap(ex, id, 100);
                    let mdn_sprd = a.snap_median(ex, id, SnapshotField::Spread, ONE_HOUR)
                        .and_then(|s| mid.map(|m| s / m * 10_000.0));
                    let vol = a
                        .realized_vol(ex, id, 600)
                        .map(|v| v * 10.0_f64.sqrt() * 10_000.0);
                    let max_jump = a
                        .max_abs_log_return(ex, id, ONE_HOUR)
                        .map(|v| v * 10_000.0);
                    let el_p50 = a.snap_quantile(ex, id, SnapshotField::ExchangeLatMs, ONE_HOUR, 0.5);
                    let el_p9999 = a.snap_quantile(ex, id, SnapshotField::ExchangeLatMs, ONE_HOUR, 0.9999);
                    let rl_p50 = a.snap_quantile(ex, id, SnapshotField::ReceiveLatMs, ONE_HOUR, 0.5);
                    let rl_p9999 = a.snap_quantile(ex, id, SnapshotField::ReceiveLatMs, ONE_HOUR, 0.9999);
                    let mdn_rng = a.mid_range_bps_stat(ex, id, ONE_HOUR, BUCKET_1S, RangeStat::Median);
                    let p99_rng = a.mid_range_bps_stat(ex, id, ONE_HOUR, BUCKET_1S, RangeStat::Quantile(0.99));

                    // Fill sim at half the p99 range as half-spread
                    let half_spread = p99_rng.map(|r| r / 2.0);
                    let (bf, bn, bm, af, an, am) = match half_spread {
                        Some(hs) if hs > 0.0 => {
                            let bid_sim = a.quote_fill_analysis(ex, id, QuoteSide::Bid, hs, ONE_HOUR);
                            let ask_sim = a.quote_fill_analysis(ex, id, QuoteSide::Ask, hs, ONE_HOUR);
                            // Normalize fills to per-hour rate using actual data duration
                            // Each snapshot ≈ 100ms, so n_total snaps = n_total/10 seconds = n_total/36000 hours
                            let bid_fph = bid_sim.as_ref().map(|r| {
                                if r.n_total > 0 { r.n_fills as f64 * ONE_HOUR as f64 / r.n_total as f64 } else { 0.0 }
                            });
                            let ask_fph = ask_sim.as_ref().map(|r| {
                                if r.n_total > 0 { r.n_fills as f64 * ONE_HOUR as f64 / r.n_total as f64 } else { 0.0 }
                            });
                            (
                                bid_fph,
                                bid_sim.as_ref().map(|r| r.n_fills as f64),
                                bid_sim.as_ref().map(|r| r.mean_markout_bps),
                                ask_fph,
                                ask_sim.as_ref().map(|r| r.n_fills as f64),
                                ask_sim.as_ref().map(|r| r.mean_markout_bps),
                            )
                        }
                        _ => (None, None, None, None, None, None),
                    };

                    (twap, mdn_sprd, vol, max_jump, el_p50, el_p9999, rl_p50, rl_p9999, mdn_rng, p99_rng, bf, bn, bm, af, an, am)
                }
                _ => (None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
            };

            let _ = writeln!(
                buf,
                "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>8} {:>8} {:>8} {:>8} {:>14} {:>8} {:>10} {:>10} {:>16} {:>12} {:>12} {:>10} {:>6} {:>10} {:>10} {:>6} {:>10}",
                sym,
                fmt_f6(mid),
                fmt_f6(bid),
                fmt_f2(bid_qty),
                fmt_f6(ask),
                fmt_f2(ask_qty),
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
                "  {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>6} {:>6}",
                sym,
                fmt_f6(mid),
                fmt_f6(bid),
                fmt_f2(bid_qty),
                fmt_f6(ask),
                fmt_f2(ask_qty),
                e_lat,
                r_lat,
            );
        }
    }
    changed
}
