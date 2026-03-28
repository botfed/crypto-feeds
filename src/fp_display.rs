use anyhow::Result;
use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::sync::Arc;
use tokio::sync::Notify;

use crate::display::write_log_section;
use crate::fair_price::FairPriceOutputs;
use crate::market_data::AllMarketData;
use crate::symbol_registry::REGISTRY;

pub const ALT_SCREEN_ON: &str = "\x1B[?1049h";
pub const ALT_SCREEN_OFF: &str = "\x1B[?1049l";
pub const CURSOR_HOME: &str = "\x1B[H";
pub const CLEAR_BELOW: &str = "\x1B[J";
pub const CURSOR_HIDE: &str = "\x1B[?25l";
pub const CURSOR_SHOW: &str = "\x1B[?25h";
pub const ERASE_EOL: &str = "\x1B[K";
pub const GREEN: &str = "\x1B[32m";
pub const RESET: &str = "\x1B[0m";
pub const BPS: f64 = 1e4;
/// Minimum edge (bps) to highlight a row green. Rough proxy for fees + slippage.
pub const EDGE_HIGHLIGHT_HURDLE_BPS: f64 = 5.0;

pub fn term_size() -> (usize, usize) {
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

pub fn prepare_frame(raw: &str) -> String {
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

pub async fn flush_str(s: String) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let mut out = stdout().lock();
        write!(out, "{}", s)?;
        out.flush()?;
        Ok(())
    })
    .await?
}

pub fn fmt_price(v: f64) -> String {
    format!("{:.6}", v)
}

pub fn fmt_bps(v: f64) -> String {
    format!("{:.2}", v)
}

pub fn fmt_qty(v: f64) -> String {
    if v.is_nan() {
        "-".to_string()
    } else {
        format!("{:.4}", v)
    }
}

pub async fn run_display(
    tick_data: Arc<AllMarketData>,
    outputs: Arc<FairPriceOutputs>,
    shutdown: Arc<Notify>,
    fp_model_str: String,
    vol_engine_str: String,
    snap_interval_ms: u64,
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
                let ms = fp_model_str.clone();
                let vs = vol_engine_str.clone();
                let frame = tokio::task::spawn_blocking(move || {
                    let elapsed = start.elapsed().as_secs();
                    let h = elapsed / 3600;
                    let m = (elapsed % 3600) / 60;
                    let sec = elapsed % 60;

                    let mut buf = String::with_capacity(8192);
                    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

                    let _ = writeln!(
                        buf,
                        "========== Fair Price Engine ({} + {})  snap: {}ms ==========  uptime: {:02}:{:02}:{:02}",
                        ms, vs, snap_interval_ms, h, m, sec,
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
                            "--- {} --- fair: {}  P_unc: {} bps  vol: {} ann  ticks: {}  snap_age: {}",
                            group_name, fair_str, unc_str, vol_ann_str, ticks_str, age_str
                        );

                        // Header
                        macro_rules! row {
                            ($buf:expr, $($arg:expr),* $(,)?) => {
                                writeln!($buf, "  {:<5} {:<16} {:>13} {:>13} {:>7} {:>7} {:>7} {:>13} {:>13} {:>8} {:>8} {:>7} {:>8} {:>7}", $($arg),*)
                            }
                        }
                        let _ = row!(buf,
                            "Xchg", "Symbol", "Mid@Ex", "Fair@Ex",
                            "EdgMid",
                            "HSprd",
                            "TrdEdge",
                            "BidQty", "AskQty",
                            "m_k", "sigma_k",
                            "P_unc", "Age", "ClkAdj",
                        );
                        let _ = row!(buf,
                            "", "", "", "",
                            "(bps)",
                            "(bps)",
                            "(bps)",
                            "", "",
                            "(bps)", "(bps)",
                            "(bps)", "(ms)", "(ms)",
                        );

                        if let Some(members) = out.group_members(group_idx) {
                            // Sort by canonical symbol (PERP before SPOT), then by exchange
                            let mut sorted_indices: Vec<usize> = (0..members.len()).collect();
                            sorted_indices.sort_by(|&a, &b| {
                                let sa = members[a].display_name.as_deref()
                                    .unwrap_or_else(|| REGISTRY.get_symbol(members[a].symbol_id).unwrap_or("?"));
                                let sb = members[b].display_name.as_deref()
                                    .unwrap_or_else(|| REGISTRY.get_symbol(members[b].symbol_id).unwrap_or("?"));
                                sa.cmp(sb).then(members[a].exchange.as_str().cmp(members[b].exchange.as_str()))
                            });
                            for &mem_idx in &sorted_indices {
                                let member = &members[mem_idx];
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
                                        let clk_adj_str = if q.exchange_ts_raw_ns > 0 && q.exchange_ts_ns > 0 {
                                            let adj_ms = (q.exchange_ts_ns - q.exchange_ts_raw_ns) as f64 / 1e6;
                                            format!("{:.1}", adj_ms)
                                        } else { "-".to_string() };
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

                                        let trd_edge = (q.edge_mid_bps.abs() - hspread_bps).max(0.0);
                                        let highlight = trd_edge > EDGE_HIGHLIGHT_HURDLE_BPS;
                                        let color_on = if highlight { GREEN } else { "" };
                                        let color_off = if highlight { RESET } else { "" };

                                        let age_str = format!("{:.2}", age_ms);
                                        let _ = write!(buf, "{}", color_on);
                                        let _ = row!(buf,
                                            ex_name,
                                            sym_name,
                                            fmt_price(q.mid_at_exchange),
                                            fmt_price(q.fair_at_exchange),
                                            fmt_bps(q.edge_mid_bps),
                                            fmt_bps(hspread_bps),
                                            fmt_bps(trd_edge),
                                            fmt_qty(q.bid_qty),
                                            fmt_qty(q.ask_qty),
                                            fmt_bps(mk_bps),
                                            fmt_bps(sk_bps),
                                            fmt_bps(lcu0),
                                            age_str,
                                            clk_adj_str,
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
                                            "-",
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

                    let (_, rows) = term_size();
                    let used = buf.lines().count();
                    let remaining = rows.saturating_sub(used);
                    write_log_section(&mut buf, remaining);
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
