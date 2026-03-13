use crate::market_data::{AllMarketData, Exchange};
use crate::ring_buffer::RingBuffer;
use crate::symbol_registry::SymbolId;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{BufWriter, Write as IoWrite};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::{self, MissedTickBehavior};

const BPS: f64 = 1e4;
const LN2: f64 = std::f64::consts::LN_2;

#[inline]
fn store_f64(atom: &AtomicU64, val: f64) {
    atom.store(val.to_bits(), Ordering::Release);
}

#[inline]
fn load_f64(atom: &AtomicU64) -> f64 {
    f64::from_bits(atom.load(Ordering::Acquire))
}

/// Output of the fair price engine for one pricing group at one snapshot.
#[derive(Debug, Default, Copy, Clone)]
#[repr(C)]
pub struct FairPriceOutput {
    pub fair_price: f64,
    pub log_fair_price: f64,
    pub uncertainty_bps: f64,
    pub vol_bps: f64,
    pub snap_ts_ns: i64,
    pub n_ticks_used: u32,
    _pad: u32,
}

/// Full quoting context for one exchange-symbol pair within a pricing group.
#[derive(Debug, Clone)]
pub struct FairQuote {
    pub fair_price: f64,
    pub fair_at_exchange: f64,
    pub mid_at_exchange: f64,
    pub uncertainty_bps: f64,
    pub uncertainty_at_horizon_bps: f64,
    pub vol_bps: f64,

    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub exchange_ts_ns: i64,

    pub edge_bid_bps: f64,
    pub edge_ask_bps: f64,
    pub edge_mid_bps: f64,

    /// Live recalibrated bias m_k (log-space) for this member.
    pub bias: f64,
    /// Live recalibrated noise variance σ_k² (log-space) for this member.
    pub noise_var: f64,
}

/// A member (exchange + symbol) of a pricing group.
pub struct GroupMember {
    pub exchange: Exchange,
    pub symbol_id: SymbolId,
    pub bias: f64,
    pub noise_var: f64,
    /// If set, reprice this member's ticks using the named group's fair price.
    /// E.g. "ETH" converts AIXBT_ETH → AIXBT_USD via ETH group's fair price.
    pub reprice_group: Option<String>,
    /// If true, the raw price is "quote per base" (e.g. AIXBT per ETH),
    /// so we compute reprice_fp / raw_price instead of raw_price * reprice_fp.
    /// Bid/ask are also swapped since inverting reverses the book.
    pub invert_reprice: bool,
}

/// GARCH(1,1) parameters.
///
/// When α + β < 1: standard GARCH with variance targeting,
///   ω = initial_var · (1 - α - β), long-run variance = initial_var.
/// When α + β = 1: reduces to EWMA (IGARCH), ω = 0, no mean reversion.
pub struct GarchParams {
    pub alpha: f64,
    pub beta: f64,
    /// Starting value for h. Also serves as the variance target when α+β < 1.
    pub initial_var: f64,
    /// EWMA halflife in snapshots for online σ̄² tracking (e.g. 5000 = ~8 min at 100ms).
    /// Controls how fast the vol level adapts to regime shifts.
    /// When α+β < 1, this updates ω = σ̄²·(1-α-β).
    /// When α+β = 1 (EWMA), σ̄² is tracked but ω stays 0.
    pub vol_halflife: u32,
}

/// Online recalibration of ruler params m_k and σ_k².
/// α, β stay fixed from config — they're structural.
pub struct RecalibConfig {
    /// How often to apply blended updates, in snapshots (e.g. 1000 = ~100s).
    pub recalibrate_every: u32,
    /// Prior weight: config is trusted as much as this many observations.
    /// High = slow adaptation, low = fast adaptation.
    pub prior_weight: f64,
    /// EWMA halflife in snapshots for ruler param tracking.
    pub ruler_halflife: u32,
}

/// Configuration for one pricing group.
pub struct FairPriceGroupConfig {
    pub name: String,
    pub members: Vec<GroupMember>,
    pub garch: GarchParams,
    pub process_noise_floor: f64,
    pub recalib: Option<RecalibConfig>,
}

/// Top-level configuration for the fair price task.
pub struct FairPriceConfig {
    pub interval_ms: u64,
    pub buffer_capacity: usize,
    pub groups: Vec<FairPriceGroupConfig>,
}

/// Per-member mutable state tracked between snapshots.
struct MemberState {
    prev_tick_pos: u64,
    // Live params — initialized from config, updated by recalibration
    bias: f64,
    noise_var: f64,
    // EWMA accumulators for recalibration
    ewma_residual: f64,
    ewma_sq_resid: f64,
    total_obs: u64,
}

/// Per-group Kalman filter + GARCH state.
struct GroupState {
    y: f64,
    p: f64,
    h: f64,
    omega: f64,
    y_prev: f64,
    initialized: bool,
    member_states: Vec<MemberState>,
    // EWMA of noise-corrected squared returns → tracks σ̄²
    ewma_r2: f64,
    snap_count: u64,
    snaps_since_recalib: u32,
}

/// A tick observation collected from a raw ring buffer.
struct TickObs {
    member_idx: usize,
    ts_ns: i64,
    log_mid: f64,
}

/// Shared member identity + live params, readable by query callers.
pub struct MemberInfo {
    pub exchange: Exchange,
    pub symbol_id: SymbolId,
    /// Override display name for repriced members (e.g. "SPOT-AIXBT-USD*").
    pub display_name: Option<String>,
    /// Resolved reprice group index, if this member needs cross-currency conversion.
    reprice_group_idx: Option<usize>,
    /// If true, invert the raw price before repricing (divide instead of multiply).
    invert_reprice: bool,
    live_bias: AtomicU64,
    live_noise_var: AtomicU64,
}

/// Collection of fair price output ring buffers, one per group.
pub struct FairPriceOutputs {
    buffers: Vec<Box<RingBuffer<FairPriceOutput>>>,
    group_names: Vec<String>,
    group_members: Vec<Vec<MemberInfo>>,
    interval_ms: f64,
    last_recalib_ns: Vec<AtomicI64>,
}

// SAFETY: AtomicU64 is Sync, everything else is immutable after construction.
unsafe impl Sync for FairPriceOutputs {}

impl std::fmt::Debug for FairPriceOutputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FairPriceOutputs")
            .field("groups", &self.group_names)
            .finish()
    }
}

impl FairPriceOutputs {
    pub fn new(config: &FairPriceConfig) -> Self {
        let buffers = config
            .groups
            .iter()
            .map(|_| Box::new(RingBuffer::with_capacity(config.buffer_capacity)))
            .collect();
        let group_names = config.groups.iter().map(|g| g.name.clone()).collect();
        let group_members = config
            .groups
            .iter()
            .map(|g| {
                g.members
                    .iter()
                    .map(|m| {
                        let reprice_group_idx = m.reprice_group.as_ref().and_then(|name| {
                            config.groups.iter().position(|g2| g2.name == *name)
                        });
                        let display_name = m.reprice_group.as_ref().map(|rg| {
                            let sym = crate::symbol_registry::REGISTRY
                                .get_symbol(m.symbol_id)
                                .unwrap_or("?");
                            let suffix = format!("-{}", rg);
                            if sym.ends_with(&suffix) {
                                format!("{}-USD*", &sym[..sym.len() - suffix.len()])
                            } else {
                                format!("{}*", sym)
                            }
                        });
                        MemberInfo {
                            exchange: m.exchange,
                            symbol_id: m.symbol_id,
                            display_name,
                            reprice_group_idx,
                            invert_reprice: m.invert_reprice,
                            live_bias: AtomicU64::new(m.bias.to_bits()),
                            live_noise_var: AtomicU64::new(m.noise_var.to_bits()),
                        }
                    })
                    .collect()
            })
            .collect();
        let last_recalib_ns = config
            .groups
            .iter()
            .map(|_| AtomicI64::new(0))
            .collect();
        Self {
            buffers,
            group_names,
            group_members,
            interval_ms: config.interval_ms as f64,
            last_recalib_ns,
        }
    }

    fn push(&self, group_idx: usize, output: FairPriceOutput) {
        self.buffers[group_idx].push(output);
    }

    fn set_bias(&self, group_idx: usize, member_idx: usize, bias: f64) {
        store_f64(&self.group_members[group_idx][member_idx].live_bias, bias);
    }

    fn set_noise_var(&self, group_idx: usize, member_idx: usize, noise_var: f64) {
        store_f64(&self.group_members[group_idx][member_idx].live_noise_var, noise_var);
    }

    fn set_last_recalib(&self, group_idx: usize, ts_ns: i64) {
        self.last_recalib_ns[group_idx].store(ts_ns, Ordering::Release);
    }

    pub fn last_recalib_ns(&self, group_idx: usize) -> i64 {
        self.last_recalib_ns[group_idx].load(Ordering::Acquire)
    }

    pub fn latest(&self, group_idx: usize) -> Option<FairPriceOutput> {
        self.buffers.get(group_idx)?.latest()
    }

    pub fn get_buffer(&self, group_idx: usize) -> Option<&RingBuffer<FairPriceOutput>> {
        self.buffers.get(group_idx).map(|b| b.as_ref())
    }

    pub fn find_group(&self, name: &str) -> Option<usize> {
        self.group_names.iter().position(|n| n == name)
    }

    pub fn interval_ms(&self) -> f64 {
        self.interval_ms
    }

    pub fn group_names(&self) -> &[String] {
        &self.group_names
    }

    pub fn group_members(&self, group_idx: usize) -> Option<&[MemberInfo]> {
        self.group_members.get(group_idx).map(|v| v.as_slice())
    }

    /// Get the live (m_k, σ_k²) for a specific member.
    pub fn get_member_params(
        &self,
        group_idx: usize,
        member_idx: usize,
    ) -> Option<(f64, f64)> {
        let members = self.group_members.get(group_idx)?;
        let m = members.get(member_idx)?;
        Some((load_f64(&m.live_bias), load_f64(&m.live_noise_var)))
    }

    /// Find a member within a group by exchange + symbol_id.
    pub fn find_member(
        &self,
        group_idx: usize,
        exchange: &Exchange,
        symbol_id: SymbolId,
    ) -> Option<usize> {
        self.group_members.get(group_idx)?.iter().position(|m| {
            std::mem::discriminant(&m.exchange) == std::mem::discriminant(exchange)
                && m.symbol_id == symbol_id
        })
    }

    /// Get the full quoting context for a specific exchange-symbol within a group.
    pub fn get_fair_quote(
        &self,
        tick_data: &AllMarketData,
        group_name: &str,
        exchange: &Exchange,
        symbol_id: SymbolId,
        horizon_ms: f64,
    ) -> Option<FairQuote> {
        let group_idx = self.find_group(group_name)?;
        let member_idx = self.find_member(group_idx, exchange, symbol_id)?;
        let fp = self.latest(group_idx)?;
        let member = &self.group_members[group_idx][member_idx];
        let bias = load_f64(&member.live_bias);
        let noise_var = load_f64(&member.live_noise_var);

        // Raw market data from this exchange
        let coll = tick_data.get_collection(exchange);
        let md = coll.latest(&symbol_id)?;
        let raw_bid = md.bid?;
        let raw_ask = md.ask?;

        // Apply reprice conversion if member is cross-currency
        let reprice_fp = match member.reprice_group_idx {
            Some(rg_idx) => self.latest(rg_idx).map(|rg_fp| rg_fp.fair_price).unwrap_or(1.0),
            None => 1.0,
        };
        let (bid, ask, bid_qty, ask_qty) = if member.invert_reprice && reprice_fp != 1.0 {
            // Inversion: raw price is "quote per base" (e.g. AIXBT per ETH)
            // Convert: USD_price = reprice_fp / raw_price
            // 1/raw_ask < 1/raw_bid, so bid/ask swap; quantities swap too
            (
                reprice_fp / raw_ask,
                reprice_fp / raw_bid,
                md.ask_qty.unwrap_or(f64::NAN),
                md.bid_qty.unwrap_or(f64::NAN),
            )
        } else {
            (
                raw_bid * reprice_fp,
                raw_ask * reprice_fp,
                md.bid_qty.unwrap_or(f64::NAN),
                md.ask_qty.unwrap_or(f64::NAN),
            )
        };

        let fair_at_exchange = (fp.log_fair_price + bias).exp();
        let mid = (bid + ask) / 2.0;

        // Horizon projection: P_horizon = P + h * (horizon_ms / interval_ms)
        let p = (fp.uncertainty_bps / BPS).powi(2);
        let h = (fp.vol_bps / BPS).powi(2);
        let p_horizon = p + h * (horizon_ms / self.interval_ms);

        let edge_bid = (fair_at_exchange - bid) / fair_at_exchange * BPS;
        let edge_ask = (ask - fair_at_exchange) / fair_at_exchange * BPS;
        let edge_mid = (fair_at_exchange - mid) / fair_at_exchange * BPS;

        Some(FairQuote {
            fair_price: fp.fair_price,
            fair_at_exchange,
            mid_at_exchange: mid,
            uncertainty_bps: fp.uncertainty_bps,
            uncertainty_at_horizon_bps: p_horizon.sqrt() * BPS,
            vol_bps: fp.vol_bps,
            bid,
            ask,
            bid_qty,
            ask_qty,
            exchange_ts_ns: md
                .exchange_ts
                .and_then(|t| t.timestamp_nanos_opt())
                .unwrap_or(0),
            edge_bid_bps: edge_bid,
            edge_ask_bps: edge_ask,
            edge_mid_bps: edge_mid,
            bias,
            noise_var,
        })
    }
}

/// Precision-weighted blend of prior (config) and data (EWMA estimate).
#[inline]
fn blend(prior_val: f64, data_val: f64, prior_weight: f64, data_count: u64, effective_n: f64) -> f64 {
    let w_data = (data_count as f64).min(effective_n);
    let w_total = prior_weight + w_data;
    if w_total <= 0.0 {
        return prior_val;
    }
    (prior_weight * prior_val + w_data * data_val) / w_total
}

/// Diagnostic writer: records tick innovations, group snapshots, and recalib events
/// to gzipped CSV files for offline statistical validation.
pub struct DiagWriter {
    tick_wtr: BufWriter<GzEncoder<std::fs::File>>,
    group_wtr: BufWriter<GzEncoder<std::fs::File>>,
    recalib_wtr: BufWriter<GzEncoder<std::fs::File>>,
    row_count: u64,
}

impl DiagWriter {
    pub fn new(output_dir: &str) -> std::io::Result<Self> {
        std::fs::create_dir_all(output_dir)?;

        let tick_file = std::fs::File::create(format!("{}/tick_innovations.csv.gz", output_dir))?;
        let group_file = std::fs::File::create(format!("{}/group_snaps.csv.gz", output_dir))?;
        let recalib_file =
            std::fs::File::create(format!("{}/recalib_events.csv.gz", output_dir))?;

        let mut tick_wtr = BufWriter::new(GzEncoder::new(tick_file, Compression::fast()));
        let mut group_wtr = BufWriter::new(GzEncoder::new(group_file, Compression::fast()));
        let mut recalib_wtr = BufWriter::new(GzEncoder::new(recalib_file, Compression::fast()));

        writeln!(
            tick_wtr,
            "snap_ts_ns,tick_ts_ns,group_idx,member_idx,log_mid,innovation,s,kalman_gain,standardized_innov,p_pre"
        )?;
        writeln!(
            group_wtr,
            "snap_ts_ns,group_idx,y,y_prev,p,h,omega,ewma_r2,r2_corrected,n_ticks"
        )?;
        writeln!(
            recalib_wtr,
            "snap_ts_ns,group_idx,member_idx,bias_old,bias_new,noise_var_old,noise_var_new,total_obs"
        )?;

        Ok(Self {
            tick_wtr,
            group_wtr,
            recalib_wtr,
            row_count: 0,
        })
    }

    #[inline]
    fn write_tick(
        &mut self,
        snap_ts: i64,
        tick_ts: i64,
        gi: usize,
        mi: usize,
        log_mid: f64,
        innov: f64,
        s: f64,
        k: f64,
        z: f64,
        p_pre: f64,
    ) {
        let _ = writeln!(
            self.tick_wtr,
            "{},{},{},{},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e}",
            snap_ts, tick_ts, gi, mi, log_mid, innov, s, k, z, p_pre
        );
        self.row_count += 1;
    }

    #[inline]
    fn write_group(
        &mut self,
        snap_ts: i64,
        gi: usize,
        y: f64,
        y_prev: f64,
        p: f64,
        h: f64,
        omega: f64,
        ewma_r2: f64,
        r2c: f64,
        n_ticks: usize,
    ) {
        let _ = writeln!(
            self.group_wtr,
            "{},{},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e},{}",
            snap_ts, gi, y, y_prev, p, h, omega, ewma_r2, r2c, n_ticks
        );
    }

    #[inline]
    fn write_recalib(
        &mut self,
        snap_ts: i64,
        gi: usize,
        mi: usize,
        bias_old: f64,
        bias_new: f64,
        nv_old: f64,
        nv_new: f64,
        total_obs: u64,
    ) {
        let _ = writeln!(
            self.recalib_wtr,
            "{},{},{},{:.16e},{:.16e},{:.16e},{:.16e},{}",
            snap_ts, gi, mi, bias_old, bias_new, nv_old, nv_new, total_obs
        );
    }

    fn maybe_flush(&mut self) {
        if self.row_count % 10_000 == 0 {
            let _ = self.tick_wtr.flush();
            let _ = self.group_wtr.flush();
            let _ = self.recalib_wtr.flush();
        }
    }

    pub fn finish(self) {
        let gz = self.tick_wtr.into_inner().expect("flush tick BufWriter");
        let _ = gz.finish();
        let gz = self.group_wtr.into_inner().expect("flush group BufWriter");
        let _ = gz.finish();
        let gz = self
            .recalib_wtr
            .into_inner()
            .expect("flush recalib BufWriter");
        let _ = gz.finish();
        log::info!("DiagWriter finalized ({} tick rows written)", self.row_count);
    }
}

/// Main fair price loop. Reads raw ticks from `AllMarketData` ring buffers,
/// runs a Kalman filter with GARCH(1,1) process noise, optionally recalibrates
/// ruler parameters online, and pushes `FairPriceOutput` into per-group ring buffers.
pub async fn run_fair_price_task(
    tick_data: Arc<AllMarketData>,
    outputs: Arc<FairPriceOutputs>,
    config: FairPriceConfig,
    shutdown: Arc<Notify>,
    mut diag: Option<DiagWriter>,
) {
    let garch_persistence: Vec<f64> = config
        .groups
        .iter()
        .map(|g| (1.0 - g.garch.alpha - g.garch.beta).max(0.0))
        .collect();

    // Pre-resolve reprice_group names → group indices per member
    let reprice_idx: Vec<Vec<Option<usize>>> = config
        .groups
        .iter()
        .map(|g| {
            g.members
                .iter()
                .map(|m| {
                    m.reprice_group.as_ref().and_then(|name| {
                        let idx = config.groups.iter().position(|g2| g2.name == *name);
                        if idx.is_none() {
                            log::warn!("reprice_group '{}' not found, ignoring", name);
                        }
                        idx
                    })
                })
                .collect()
        })
        .collect();

    let mut states: Vec<GroupState> = config
        .groups
        .iter()
        .map(|g| {
            let omega = g.garch.initial_var * (1.0 - g.garch.alpha - g.garch.beta).max(0.0);
            GroupState {
                y: 0.0,
                p: 0.0,
                h: g.garch.initial_var,
                omega,
                y_prev: f64::NAN,
                initialized: false,
                member_states: g
                    .members
                    .iter()
                    .map(|m| MemberState {
                        prev_tick_pos: 0,
                        bias: m.bias,
                        noise_var: m.noise_var,
                        ewma_residual: m.bias,
                        ewma_sq_resid: m.noise_var,
                        total_obs: 0,
                    })
                    .collect(),
                ewma_r2: g.garch.initial_var,
                snap_count: 0,
                snaps_since_recalib: 0,
            }
        })
        .collect();

    let mut interval = time::interval(std::time::Duration::from_millis(config.interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    let mut tick_scratch: Vec<TickObs> = Vec::with_capacity(256);

    // Pre-allocate recalibration scratch buffers
    let max_members = config
        .groups
        .iter()
        .map(|g| g.members.len())
        .max()
        .unwrap_or(0);
    let mut rc_resid_sum = vec![0.0f64; max_members];
    let mut rc_sq_sum = vec![0.0f64; max_members];
    let mut rc_count = vec![0u32; max_members];

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = &mut shutdown_fut => {
                log::info!("Fair price task shutting down");
                if let Some(dw) = diag {
                    dw.finish();
                }
                return;
            }
        }

        let snap_ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        for (group_idx, group_cfg) in config.groups.iter().enumerate() {
            let state = &mut states[group_idx];
            let n_members = group_cfg.members.len();
            tick_scratch.clear();

            // ── 1. Collect new ticks from all members ──────────────────
            for (mem_idx, member) in group_cfg.members.iter().enumerate() {
                // If this member needs repricing, get the reprice group's log fair price
                let reprice_log_fp = match reprice_idx[group_idx][mem_idx] {
                    Some(rg_idx) => match outputs.latest(rg_idx) {
                        Some(fp) if fp.log_fair_price.is_finite() => fp.log_fair_price,
                        _ => continue, // reprice group not ready yet, skip member
                    },
                    None => 0.0,
                };

                let coll = tick_data.get_collection(&member.exchange);
                let tick_buf = match coll.get_buffer(&member.symbol_id) {
                    Some(b) => b,
                    None => continue,
                };

                let cur_pos = tick_buf.write_pos();
                let start_pos = state.member_states[mem_idx].prev_tick_pos;
                let count = cur_pos.saturating_sub(start_pos).min(1000);

                for i in 0..count {
                    if let Some(md) = tick_buf.read_at(start_pos + i) {
                        if let (Some(bid), Some(ask)) = (md.bid, md.ask) {
                            let mid = (bid + ask) / 2.0;
                            if mid > 0.0 {
                                let ts = md
                                    .exchange_ts
                                    .and_then(|t| t.timestamp_nanos_opt())
                                    .or_else(|| {
                                        md.received_ts.and_then(|t| t.timestamp_nanos_opt())
                                    })
                                    .unwrap_or(snap_ts_ns);

                                let log_mid_adj = if member.invert_reprice {
                                    reprice_log_fp - mid.ln()
                                } else {
                                    mid.ln() + reprice_log_fp
                                };
                                tick_scratch.push(TickObs {
                                    member_idx: mem_idx,
                                    ts_ns: ts,
                                    log_mid: log_mid_adj,
                                });
                            }
                        }
                    }
                }
                state.member_states[mem_idx].prev_tick_pos = cur_pos;
            }

            // ── 2. Sort ticks by timestamp ─────────────────────────────
            tick_scratch.sort_unstable_by_key(|t| t.ts_ns);

            let n_ticks = tick_scratch.len();

            // ── 3. Initialize from first observation if needed ─────────
            if !state.initialized {
                if let Some(first) = tick_scratch.first() {
                    let ms = &state.member_states[first.member_idx];
                    state.y = first.log_mid - ms.bias;
                    state.p = ms.noise_var;
                    state.initialized = true;
                } else {
                    continue;
                }
            }

            // ── 4. Kalman filter: predict + update per tick ────────────
            let q_per_tick = if n_ticks > 1 {
                (state.h / n_ticks as f64).max(group_cfg.process_noise_floor)
            } else {
                state.h.max(group_cfg.process_noise_floor)
            };

            let has_recalib = group_cfg.recalib.is_some();

            // Zero recalib accumulators
            if has_recalib {
                for i in 0..n_members {
                    rc_resid_sum[i] = 0.0;
                    rc_sq_sum[i] = 0.0;
                    rc_count[i] = 0;
                }
            }

            if n_ticks == 0 {
                // No observations — pure predict step
                state.p += state.h;
            } else {
                for obs in &tick_scratch {
                    let ms = &state.member_states[obs.member_idx];

                    // Accumulate recalib stats (pre-update y)
                    if has_recalib {
                        let raw_resid = obs.log_mid - state.y;
                        rc_resid_sum[obs.member_idx] += raw_resid;
                        let centered = raw_resid - ms.bias;
                        rc_sq_sum[obs.member_idx] += centered * centered;
                        rc_count[obs.member_idx] += 1;
                    }

                    // Predict
                    state.p += q_per_tick;

                    // Update: observation = log_mid = y + bias + noise
                    let p_pre = state.p;
                    let innovation = obs.log_mid - ms.bias - state.y;
                    let s = state.p + ms.noise_var;
                    let k = state.p / s;

                    if let Some(ref mut dw) = diag {
                        dw.write_tick(
                            snap_ts_ns,
                            obs.ts_ns,
                            group_idx,
                            obs.member_idx,
                            obs.log_mid,
                            innovation,
                            s,
                            k,
                            innovation / s.sqrt(),
                            p_pre,
                        );
                    }

                    state.y += k * innovation;
                    state.p *= 1.0 - k;
                }
            }

            // ── 5. GARCH(1,1) update on snapshot return ────────────────
            let y_prev_snap = state.y_prev;
            let r2_corrected = if !y_prev_snap.is_nan() {
                let r = state.y - y_prev_snap;
                let rc = (r * r - state.p).max(0.0);
                state.h = (state.omega
                    + group_cfg.garch.alpha * rc
                    + group_cfg.garch.beta * state.h)
                    .max(group_cfg.process_noise_floor);
                rc
            } else {
                0.0
            };
            state.y_prev = state.y;

            // ── 6. Online σ̄² tracking (always active) ─────────────────
            {
                let vol_decay = (-LN2 / group_cfg.garch.vol_halflife as f64).exp();
                let vol_alpha = 1.0 - vol_decay;

                if state.snap_count > 0 {
                    state.ewma_r2 = (vol_alpha * r2_corrected + vol_decay * state.ewma_r2)
                        .max(group_cfg.process_noise_floor);
                    // Recompute omega from live σ̄² estimate
                    state.omega = state.ewma_r2 * garch_persistence[group_idx];
                }
                state.snap_count += 1;
            }

            // ── 6b. Diagnostic: group-level snapshot ──────────────────
            if let Some(ref mut dw) = diag {
                dw.write_group(
                    snap_ts_ns,
                    group_idx,
                    state.y,
                    y_prev_snap,
                    state.p,
                    state.h,
                    state.omega,
                    state.ewma_r2,
                    r2_corrected,
                    n_ticks,
                );
            }

            // ── 7. Ruler recalibration: m_k, σ_k² ─────────────────────
            if let Some(rc_cfg) = &group_cfg.recalib {
                let rc_decay = (-LN2 / rc_cfg.ruler_halflife as f64).exp();
                let rc_alpha = 1.0 - rc_decay;

                // Update per-member EWMAs
                for k in 0..n_members {
                    if rc_count[k] > 0 {
                        let n = rc_count[k] as f64;
                        let mean_resid = rc_resid_sum[k] / n;
                        let mean_sq = rc_sq_sum[k] / n;
                        let ms = &mut state.member_states[k];
                        ms.ewma_residual = rc_alpha * mean_resid + rc_decay * ms.ewma_residual;
                        ms.ewma_sq_resid = rc_alpha * mean_sq + rc_decay * ms.ewma_sq_resid;
                        ms.total_obs += rc_count[k] as u64;
                    }
                }

                // Periodic blend of config prior with EWMA estimates
                state.snaps_since_recalib += 1;
                if state.snaps_since_recalib >= rc_cfg.recalibrate_every {
                    let effective_n = rc_cfg.ruler_halflife as f64 / LN2;

                    for k in 0..n_members {
                        let ms = &mut state.member_states[k];
                        let cfg_m = &group_cfg.members[k];

                        let bias_old = ms.bias;
                        let noise_var_old = ms.noise_var;

                        ms.bias = blend(
                            cfg_m.bias,
                            ms.ewma_residual,
                            rc_cfg.prior_weight,
                            ms.total_obs,
                            effective_n,
                        );

                        let sigma_est = (ms.ewma_sq_resid - state.p).max(1e-16);
                        ms.noise_var = blend(
                            cfg_m.noise_var,
                            sigma_est,
                            rc_cfg.prior_weight,
                            ms.total_obs,
                            effective_n,
                        );

                        if let Some(ref mut dw) = diag {
                            dw.write_recalib(
                                snap_ts_ns,
                                group_idx,
                                k,
                                bias_old,
                                ms.bias,
                                noise_var_old,
                                ms.noise_var,
                                ms.total_obs,
                            );
                        }
                    }

                    state.snaps_since_recalib = 0;
                    outputs.set_last_recalib(group_idx, snap_ts_ns);
                }
            }

            // ── 8. Sync live biases + noise vars, push output ─────────
            for (k, ms) in state.member_states.iter().enumerate() {
                outputs.set_bias(group_idx, k, ms.bias);
                outputs.set_noise_var(group_idx, k, ms.noise_var);
            }
            outputs.push(
                group_idx,
                FairPriceOutput {
                    fair_price: state.y.exp(),
                    log_fair_price: state.y,
                    uncertainty_bps: state.p.sqrt() * BPS,
                    vol_bps: state.h.sqrt() * BPS,
                    snap_ts_ns,
                    n_ticks_used: n_ticks as u32,
                    _pad: 0,
                },
            );

            if let Some(ref mut dw) = diag {
                dw.maybe_flush();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::MarketData;
    use chrono::Utc;

    #[test]
    fn test_blend_prior_only() {
        // No data yet → returns prior
        assert_eq!(blend(1.0, 999.0, 100.0, 0, 1000.0), 1.0);
    }

    #[test]
    fn test_blend_equal_weight() {
        // prior_weight=100, data_count=100, effective_n=1000 → 50/50
        let v = blend(1.0, 3.0, 100.0, 100, 1000.0);
        assert!((v - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_blend_data_saturated() {
        // data_count far exceeds effective_n → capped at effective_n
        // prior=100, effective_n=500 → 100/(100+500) prior + 500/(100+500) data
        let v = blend(0.0, 6.0, 100.0, 999999, 500.0);
        let expected = 500.0 / 600.0 * 6.0;
        assert!((v - expected).abs() < 1e-10);
    }

    #[test]
    fn test_blend_zero_weights() {
        assert_eq!(blend(42.0, 0.0, 0.0, 0, 100.0), 42.0);
    }

    #[tokio::test]
    async fn test_fair_price_basic() {
        let tick_data = Arc::new(AllMarketData::new());

        let config = FairPriceConfig {
            interval_ms: 10,
            buffer_capacity: 1024,
            groups: vec![FairPriceGroupConfig {
                name: "TEST".to_string(),
                members: vec![
                    GroupMember {
                        exchange: Exchange::Binance,
                        symbol_id: 0,
                        bias: 0.0,
                        noise_var: 1e-8,
                        reprice_group: None,
                        invert_reprice: false,
                    },
                    GroupMember {
                        exchange: Exchange::Coinbase,
                        symbol_id: 0,
                        bias: 0.0,
                        noise_var: 2e-8,
                        reprice_group: None,
                        invert_reprice: false,
                    },
                ],
                garch: GarchParams {
                    alpha: 0.05,
                    beta: 0.90,
                    initial_var: 4e-8,
                    vol_halflife: 5000,
                },
                process_noise_floor: 1e-14,
                recalib: None,
            }],
        };

        let outputs = Arc::new(FairPriceOutputs::new(&config));
        let shutdown = Arc::new(Notify::new());

        let now = Utc::now();
        tick_data.binance.push(
            &0,
            MarketData {
                bid: Some(100.0),
                ask: Some(100.1),
                bid_qty: Some(1.0),
                ask_qty: Some(1.0),
                exchange_ts: Some(now),
                received_ts: Some(now),
            },
        );
        tick_data.coinbase.push(
            &0,
            MarketData {
                bid: Some(100.05),
                ask: Some(100.15),
                bid_qty: Some(1.0),
                ask_qty: Some(1.0),
                exchange_ts: Some(now),
                received_ts: Some(now),
            },
        );

        let outputs_clone = Arc::clone(&outputs);
        let shutdown_clone = Arc::clone(&shutdown);
        let handle = tokio::spawn(run_fair_price_task(
            Arc::clone(&tick_data),
            outputs_clone,
            config,
            shutdown_clone,
            None,
        ));

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown.notify_waiters();
        handle.await.unwrap();

        let fp = outputs.latest(0);
        assert!(fp.is_some());
        let fp = fp.unwrap();
        assert!(
            (fp.fair_price - 100.075).abs() < 0.5,
            "fair_price={}, expected ~100.075",
            fp.fair_price
        );
        assert!(fp.uncertainty_bps > 0.0);
        assert!(fp.vol_bps > 0.0);
    }

    #[test]
    fn test_outputs_group_lookup() {
        let config = FairPriceConfig {
            interval_ms: 100,
            buffer_capacity: 1024,
            groups: vec![
                FairPriceGroupConfig {
                    name: "BTC".to_string(),
                    members: vec![],
                    garch: GarchParams {
                        alpha: 0.05,
                        beta: 0.90,
                        initial_var: 4e-8,
                    vol_halflife: 5000,
                    },
                    process_noise_floor: 1e-14,
                    recalib: None,
                },
                FairPriceGroupConfig {
                    name: "ETH".to_string(),
                    members: vec![],
                    garch: GarchParams {
                        alpha: 0.05,
                        beta: 0.90,
                        initial_var: 4e-8,
                    vol_halflife: 5000,
                    },
                    process_noise_floor: 1e-14,
                    recalib: None,
                },
            ],
        };

        let outputs = FairPriceOutputs::new(&config);
        assert_eq!(outputs.find_group("BTC"), Some(0));
        assert_eq!(outputs.find_group("ETH"), Some(1));
        assert_eq!(outputs.find_group("SOL"), None);
        assert_eq!(outputs.group_names(), &["BTC", "ETH"]);
    }

    #[test]
    fn test_output_push_and_read() {
        let config = FairPriceConfig {
            interval_ms: 100,
            buffer_capacity: 1024,
            groups: vec![FairPriceGroupConfig {
                name: "TEST".to_string(),
                members: vec![],
                garch: GarchParams {
                    alpha: 0.05,
                    beta: 0.90,
                    initial_var: 4e-8,
                    vol_halflife: 5000,
                },
                process_noise_floor: 1e-14,
                recalib: None,
            }],
        };

        let outputs = FairPriceOutputs::new(&config);
        assert!(outputs.latest(0).is_none());

        outputs.push(
            0,
            FairPriceOutput {
                fair_price: 100.0,
                log_fair_price: 100.0_f64.ln(),
                uncertainty_bps: 1.5,
                vol_bps: 2.0,
                snap_ts_ns: 1000,
                n_ticks_used: 5,
                _pad: 0,
            },
        );

        let fp = outputs.latest(0).unwrap();
        assert!((fp.fair_price - 100.0).abs() < f64::EPSILON);
        assert_eq!(fp.n_ticks_used, 5);

        let buf = outputs.get_buffer(0).unwrap();
        let mut scratch = [FairPriceOutput::default(); 4];
        let count = buf.read_last_n(4, &mut scratch);
        assert_eq!(count, 1);
    }

    #[test]
    fn test_ewma_garch_is_ewma() {
        // α + β = 1 → omega = 0, pure EWMA
        let g = GarchParams {
            alpha: 0.06,
            beta: 0.94,
            initial_var: 1e-6,
            vol_halflife: 5000,
        };
        assert_eq!(g.initial_var * (1.0 - g.alpha - g.beta).max(0.0), 0.0);
    }
}
