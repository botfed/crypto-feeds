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
const MS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0 * 1000.0;

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
    /// Annualized vol as percentage, derived from h_per_ms.
    pub vol_ann_pct: f64,
    pub snap_ts_ns: i64,
    pub n_ticks_used: u32,
    _pad: [u8; 4],
}

/// Full quoting context for one exchange-symbol pair within a pricing group.
#[derive(Debug, Clone)]
pub struct FairQuote {
    pub fair_price: f64,
    pub fair_at_exchange: f64,
    pub mid_at_exchange: f64,
    pub uncertainty_bps: f64,
    pub uncertainty_at_horizon_bps: f64,

    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub exchange_ts_ns: i64,

    pub edge_bid_bps: f64,
    pub edge_ask_bps: f64,
    pub edge_mid_bps: f64,

    /// Bias m_k (log-space) for this member.
    pub bias: f64,
    /// Noise variance σ_k² (log-space) for this member.
    pub noise_var: f64,
}

/// A member (exchange + symbol) of a pricing group.
pub struct GroupMember {
    pub exchange: Exchange,
    pub symbol_id: SymbolId,
    pub bias: f64,
    pub noise_var: f64,
    /// GG weight (alpha_perp). Only used in GonzaloGranger mode.
    pub gg_weight: f64,
    /// If set, reprice this member's ticks using the named group's fair price.
    /// E.g. "ETH" converts AIXBT_ETH → AIXBT_USD via ETH group's fair price.
    pub reprice_group: Option<String>,
    /// If true, the raw price is "quote per base" (e.g. AIXBT per ETH),
    /// so we compute reprice_fp / raw_price instead of raw_price * reprice_fp.
    /// Bid/ask are also swapped since inverting reverses the book.
    pub invert_reprice: bool,
}

/// How sigma_k (observation noise) is determined per tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigmaMode {
    /// sigma_k² = max(log_half_spread², floor²) from each tick's bid/ask.
    InstantSpread,
    /// sigma_k² = max(ewma(log_half_spread)², floor²), smoothed over ticks.
    EwmaSpread,
    /// sigma_k² = member.noise_var from config/beacon, no online estimation.
    Static,
}

/// Which fair price model to use.
#[derive(Debug, Clone, Copy)]
pub enum FairPriceModel {
    /// Tau-based Kalman filter (sequential state estimation).
    Kalman,
    /// Gonzalo-Granger weighted average of exchange log-mids.
    GonzaloGranger {
        /// Max age in ms before an exchange is excluded. 0 = no cutoff.
        max_latency_ms: f64,
        /// Exp decay half-life in ms. 0 = no decay.
        decay_halflife_ms: f64,
    },
}

/// Configuration for one pricing group.
pub struct FairPriceGroupConfig {
    pub name: String,
    pub members: Vec<GroupMember>,
    /// Which fair price model to use. Default: Kalman.
    pub model: FairPriceModel,
    /// How sigma_k is determined. Default: InstantSpread.
    pub sigma_mode: SigmaMode,
    /// EWMA halflife in ms for online m_k bias estimation. 0 = no online bias.
    pub bias_ewma_halflife_ms: f64,
    /// EWMA halflife in ms for spread smoothing (EwmaSpread mode). 0 = same as bias.
    pub spread_ewma_halflife_ms: f64,
    /// Floor for sigma_k in log-price units. Default 1e-6 (≈ 0.01 bps).
    pub sigma_k_floor: f64,
}

/// Top-level configuration for the fair price task.
pub struct FairPriceConfig {
    pub interval_ms: u64,
    pub buffer_capacity: usize,
    pub groups: Vec<FairPriceGroupConfig>,
    pub vol_provider: crate::vol_provider::VolProvider,
}

/// Per-member mutable state tracked between snapshots.
struct MemberState {
    prev_tick_pos: u64,
    /// Running bias estimate (EWMA of residuals).
    bias: f64,
    /// Running noise variance estimate.
    noise_var: f64,
    /// EWMA of log half-spread (for EwmaSpread mode).
    ewma_half_spread: f64,
    /// Most recent log mid from this exchange (for GG mode).
    latest_log_mid: f64,
    /// Timestamp of most recent tick in nanoseconds (for GG staleness).
    latest_ts_ns: i64,
}

/// Per-group Kalman filter state.
struct GroupState {
    y: f64,
    p: f64,
    /// Nanosecond timestamp of the last processed tick (for tau computation).
    prev_ts_ns: i64,
    initialized: bool,
    member_states: Vec<MemberState>,
}

/// A tick observation collected from a raw ring buffer.
struct TickObs {
    member_idx: usize,
    ts_ns: i64,
    log_mid: f64,
    /// log(ask) - log(bid)) / 2, i.e. half-spread in log-price space.
    log_half_spread: f64,
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
    /// h_per_ms per group, live-updated by vol provider.
    h_per_ms: Vec<AtomicU64>,
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
        let group_names: Vec<String> = config.groups.iter().map(|g| g.name.clone()).collect();
        let h_per_ms = group_names
            .iter()
            .enumerate()
            .map(|(i, _)| AtomicU64::new(config.vol_provider.h_per_ms(i).to_bits()))
            .collect();
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
        Self {
            buffers,
            group_names,
            group_members,
            h_per_ms,
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

    pub fn latest(&self, group_idx: usize) -> Option<FairPriceOutput> {
        self.buffers.get(group_idx)?.latest()
    }

    pub fn get_buffer(&self, group_idx: usize) -> Option<&RingBuffer<FairPriceOutput>> {
        self.buffers.get(group_idx).map(|b| b.as_ref())
    }

    pub fn find_group(&self, name: &str) -> Option<usize> {
        self.group_names.iter().position(|n| n == name)
    }

    pub fn h_per_ms(&self, group_idx: usize) -> f64 {
        load_f64(&self.h_per_ms[group_idx])
    }

    fn set_h_per_ms(&self, group_idx: usize, val: f64) {
        store_f64(&self.h_per_ms[group_idx], val);
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

        // Horizon projection: P_horizon = P + h_per_ms * horizon_ms
        let p = (fp.uncertainty_bps / BPS).powi(2);
        let h_ms = load_f64(&self.h_per_ms[group_idx]);
        let p_horizon = p + h_ms * horizon_ms;

        let edge_bid = (fair_at_exchange - bid) / fair_at_exchange * BPS;
        let edge_ask = (ask - fair_at_exchange) / fair_at_exchange * BPS;
        let edge_mid = (fair_at_exchange - mid) / fair_at_exchange * BPS;

        Some(FairQuote {
            fair_price: fp.fair_price,
            fair_at_exchange,
            mid_at_exchange: mid,
            uncertainty_bps: fp.uncertainty_bps,
            uncertainty_at_horizon_bps: p_horizon.sqrt() * BPS,
            bid,
            ask,
            bid_qty,
            ask_qty,
            exchange_ts_ns: md
                .exchange_ts
                .or(md.received_ts)
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

/// Diagnostic writer: records tick innovations and group snapshots
/// to gzipped CSV files for offline validation against Python.
pub struct DiagWriter {
    tick_wtr: BufWriter<GzEncoder<std::fs::File>>,
    group_wtr: BufWriter<GzEncoder<std::fs::File>>,
    row_count: u64,
}

impl DiagWriter {
    pub fn new(output_dir: &str) -> std::io::Result<Self> {
        std::fs::create_dir_all(output_dir)?;

        let tick_file = std::fs::File::create(format!("{}/tick_innovations.csv.gz", output_dir))?;
        let group_file = std::fs::File::create(format!("{}/group_snaps.csv.gz", output_dir))?;

        let mut tick_wtr = BufWriter::new(GzEncoder::new(tick_file, Compression::fast()));
        let mut group_wtr = BufWriter::new(GzEncoder::new(group_file, Compression::fast()));

        writeln!(
            tick_wtr,
            "snap_ts_ns,tick_ts_ns,group_idx,member_idx,log_mid,tau_ms,q,p_pre,innovation,s,kalman_gain,y_post,p_post"
        )?;
        writeln!(
            group_wtr,
            "snap_ts_ns,group_idx,y,p,p_pre_fwd,tau_fwd_ms,n_ticks,last_tick_ts_ns"
        )?;

        Ok(Self {
            tick_wtr,
            group_wtr,
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
        tau_ms: f64,
        q: f64,
        p_pre: f64,
        innov: f64,
        s: f64,
        gain: f64,
        y_post: f64,
        p_post: f64,
    ) {
        let _ = writeln!(
            self.tick_wtr,
            "{},{},{},{},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e},{:.16e}",
            snap_ts, tick_ts, gi, mi, log_mid, tau_ms, q, p_pre, innov, s, gain, y_post, p_post
        );
        self.row_count += 1;
    }

    #[inline]
    fn write_group(
        &mut self,
        snap_ts: i64,
        gi: usize,
        y: f64,
        p: f64,
        p_pre_fwd: f64,
        tau_fwd_ms: f64,
        n_ticks: usize,
        last_tick_ts_ns: i64,
    ) {
        let _ = writeln!(
            self.group_wtr,
            "{},{},{:.16e},{:.16e},{:.16e},{:.16e},{},{}",
            snap_ts, gi, y, p, p_pre_fwd, tau_fwd_ms, n_ticks, last_tick_ts_ns
        );
    }

    fn maybe_flush(&mut self) {
        if self.row_count % 10_000 == 0 {
            let _ = self.tick_wtr.flush();
            let _ = self.group_wtr.flush();
        }
    }

    pub fn finish(self) {
        let gz = self.tick_wtr.into_inner().expect("flush tick BufWriter");
        let _ = gz.finish();
        let gz = self.group_wtr.into_inner().expect("flush group BufWriter");
        let _ = gz.finish();
        log::info!("DiagWriter finalized ({} tick rows written)", self.row_count);
    }
}

/// Main fair price loop. Reads raw ticks from `AllMarketData` ring buffers,
/// runs a static Kalman filter with tau-based process noise, and pushes
/// `FairPriceOutput` into per-group ring buffers.
///
/// All parameters (h_per_ms, bias, noise_var) are fixed from config.
/// Process noise is proportional to elapsed time between ticks.
pub async fn run_fair_price_task(
    tick_data: Arc<AllMarketData>,
    outputs: Arc<FairPriceOutputs>,
    mut config: FairPriceConfig,
    shutdown: Arc<Notify>,
    mut diag: Option<DiagWriter>,
) {
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
            GroupState {
                y: 0.0,
                p: 0.0,
                prev_ts_ns: 0,
                initialized: false,
                member_states: g
                    .members
                    .iter()
                    .map(|m| MemberState {
                        prev_tick_pos: 0,
                        bias: m.bias,
                        noise_var: m.noise_var,
                        ewma_half_spread: m.noise_var.sqrt(),
                        latest_log_mid: f64::NAN,
                        latest_ts_ns: 0,
                    })
                    .collect(),
            }
        })
        .collect();

    let mut interval = time::interval(std::time::Duration::from_millis(config.interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    let mut tick_scratch: Vec<TickObs> = Vec::with_capacity(256);

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
            let h_per_ms = config.vol_provider.h_per_ms(group_idx);
            tick_scratch.clear();

            // ── 1. Collect new ticks from all members ──────────────────
            for (mem_idx, member) in group_cfg.members.iter().enumerate() {
                let reprice_log_fp = match reprice_idx[group_idx][mem_idx] {
                    Some(rg_idx) => match outputs.latest(rg_idx) {
                        Some(fp) if fp.log_fair_price.is_finite() => fp.log_fair_price,
                        _ => continue,
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
                                let log_half_spread = (ask.ln() - bid.ln()) / 2.0;
                                tick_scratch.push(TickObs {
                                    member_idx: mem_idx,
                                    ts_ns: ts,
                                    log_mid: log_mid_adj,
                                    log_half_spread,
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

            let sigma_floor_sq = group_cfg.sigma_k_floor * group_cfg.sigma_k_floor;
            let ln2 = std::f64::consts::LN_2;

            // Helper: compute time-weighted EWMA decay for a given halflife and tau
            #[inline]
            fn ewma_decay(tau_ms: f64, halflife_ms: f64) -> f64 {
                if halflife_ms <= 0.0 { return 1.0; }
                (-tau_ms * std::f64::consts::LN_2 / halflife_ms).exp()
            }

            // Update latest_log_mid / latest_ts_ns and spread EWMA for all ticks
            // (shared between Kalman and GG)
            for obs in &tick_scratch {
                let ms = &mut state.member_states[obs.member_idx];
                ms.latest_log_mid = obs.log_mid;
                ms.latest_ts_ns = obs.ts_ns;

                // Update spread EWMA (time-weighted)
                if group_cfg.spread_ewma_halflife_ms > 0.0 && ms.latest_ts_ns > 0 {
                    let tau = (obs.ts_ns - ms.latest_ts_ns).max(0) as f64 / 1e6;
                    let d = ewma_decay(tau, group_cfg.spread_ewma_halflife_ms);
                    ms.ewma_half_spread = (1.0 - d) * obs.log_half_spread + d * ms.ewma_half_spread;
                } else {
                    ms.ewma_half_spread = obs.log_half_spread;
                }
            }

            // ── 3. Model-specific processing ────────────────────────────
            match group_cfg.model {

            FairPriceModel::Kalman => {
                // Initialize from first observation if needed
                if !state.initialized {
                    if let Some(first) = tick_scratch.first() {
                        let ms = &state.member_states[first.member_idx];
                        state.y = first.log_mid - ms.bias;
                        state.p = ms.noise_var;
                        state.prev_ts_ns = first.ts_ns;
                        state.initialized = true;
                    } else {
                        continue;
                    }
                }

                if n_ticks == 0 {
                    let tau_ms = (snap_ts_ns - state.prev_ts_ns) as f64 / 1e6;
                    state.p += h_per_ms * tau_ms;
                    state.prev_ts_ns = snap_ts_ns;
                } else {
                    for obs in &tick_scratch {
                        let ms = &mut state.member_states[obs.member_idx];

                        let tau_ms = (obs.ts_ns - state.prev_ts_ns) as f64 / 1e6;
                        let q = h_per_ms * tau_ms.max(0.0);
                        state.p += q;

                        // Determine noise_var based on sigma mode
                        let noise_var = match group_cfg.sigma_mode {
                            SigmaMode::InstantSpread => {
                                obs.log_half_spread.powi(2).max(sigma_floor_sq)
                            }
                            SigmaMode::EwmaSpread => {
                                ms.ewma_half_spread.powi(2).max(sigma_floor_sq)
                            }
                            SigmaMode::Static => ms.noise_var,
                        };

                        // Pre-update residual for bias EWMA
                        let residual = obs.log_mid - state.y;

                        let p_pre = state.p;
                        let innovation = obs.log_mid - ms.bias - state.y;
                        let s = state.p + noise_var;
                        let gain = state.p / s;

                        state.y += gain * innovation;
                        state.p *= 1.0 - gain;

                        // Time-weighted bias EWMA
                        if group_cfg.bias_ewma_halflife_ms > 0.0 {
                            let d = ewma_decay(tau_ms.max(0.0), group_cfg.bias_ewma_halflife_ms);
                            ms.bias = (1.0 - d) * residual + d * ms.bias;
                        }

                        ms.noise_var = noise_var;

                        if let Some(ref mut dw) = diag {
                            dw.write_tick(
                                snap_ts_ns, obs.ts_ns, group_idx, obs.member_idx,
                                obs.log_mid, tau_ms, q, p_pre, innovation, s, gain,
                                state.y, state.p,
                            );
                        }

                        state.prev_ts_ns = obs.ts_ns;
                    }

                    // Forward-propagate to snapshot boundary
                    let tau_fwd_ms = (snap_ts_ns - state.prev_ts_ns) as f64 / 1e6;
                    let p_pre_fwd = state.p;
                    if tau_fwd_ms > 0.0 {
                        state.p += h_per_ms * tau_fwd_ms;
                    }

                    if let Some(ref mut dw) = diag {
                        dw.write_group(
                            snap_ts_ns, group_idx, state.y, state.p,
                            p_pre_fwd, tau_fwd_ms, n_ticks, state.prev_ts_ns,
                        );
                    }

                    state.prev_ts_ns = snap_ts_ns;
                }
            }

            FairPriceModel::GonzaloGranger { max_latency_ms, decay_halflife_ms } => {
                // Update bias EWMA for all observed ticks
                if group_cfg.bias_ewma_halflife_ms > 0.0 && state.initialized {
                    for obs in &tick_scratch {
                        let ms = &mut state.member_states[obs.member_idx];
                        let residual = obs.log_mid - state.y;
                        let tau_ms = if ms.latest_ts_ns > 0 {
                            (obs.ts_ns - ms.latest_ts_ns).max(0) as f64 / 1e6
                        } else {
                            0.0
                        };
                        let d = ewma_decay(tau_ms, group_cfg.bias_ewma_halflife_ms);
                        ms.bias = (1.0 - d) * residual + d * ms.bias;
                    }
                }

                // Compute GG weighted average
                let mut w_sum = 0.0f64;
                let mut y = 0.0f64;
                let mut p = 0.0f64;
                let mut freshest_ts_ns: i64 = 0;
                let mut n_active = 0u32;

                for (k, member) in group_cfg.members.iter().enumerate() {
                    let ms = &state.member_states[k];
                    if ms.latest_ts_ns == 0 || ms.latest_log_mid.is_nan() { continue; }

                    let elapsed_ms = (snap_ts_ns - ms.latest_ts_ns) as f64 / 1e6;
                    if max_latency_ms > 0.0 && elapsed_ms > max_latency_ms { continue; }

                    let decay = if decay_halflife_ms > 0.0 {
                        (-elapsed_ms * ln2 / decay_halflife_ms).exp()
                    } else {
                        1.0
                    };

                    let w = member.gg_weight * decay;

                    // sigma_k for uncertainty
                    let noise_var = match group_cfg.sigma_mode {
                        SigmaMode::InstantSpread | SigmaMode::EwmaSpread => {
                            ms.ewma_half_spread.powi(2).max(sigma_floor_sq)
                        }
                        SigmaMode::Static => ms.noise_var,
                    };

                    y += w * (ms.latest_log_mid - ms.bias);
                    p += w * w * noise_var;
                    w_sum += w;
                    n_active += 1;

                    if ms.latest_ts_ns > freshest_ts_ns {
                        freshest_ts_ns = ms.latest_ts_ns;
                    }
                }

                if w_sum.abs() > 1e-30 && n_active > 0 {
                    state.y = y / w_sum;
                    state.p = p / (w_sum * w_sum);

                    // Add process noise for time since freshest tick
                    let staleness_ms = (snap_ts_ns - freshest_ts_ns) as f64 / 1e6;
                    if staleness_ms > 0.0 {
                        state.p += h_per_ms * staleness_ms;
                    }

                    state.prev_ts_ns = snap_ts_ns;
                    state.initialized = true;
                }
            }

            } // match model

            // ── 4. Update vol provider + sync to outputs ────────────────
            if state.initialized {
                config.vol_provider.update(group_idx, state.y.exp(), snap_ts_ns);
            }
            let h_per_ms = config.vol_provider.h_per_ms(group_idx);
            outputs.set_h_per_ms(group_idx, h_per_ms);

            for (k, ms) in state.member_states.iter().enumerate() {
                outputs.set_bias(group_idx, k, ms.bias);
                outputs.set_noise_var(group_idx, k, ms.noise_var);
            }

            let vol_ann_pct = config.vol_provider.ann_vol(group_idx) * 100.0;

            // ── 6. Push output ─────────────────────────────────────────
            outputs.push(
                group_idx,
                FairPriceOutput {
                    fair_price: state.y.exp(),
                    log_fair_price: state.y,
                    uncertainty_bps: state.p.sqrt() * BPS,
                    vol_ann_pct,
                    snap_ts_ns,
                    n_ticks_used: n_ticks as u32,
                    _pad: [0; 4],
                },
            );

            if let Some(ref mut dw) = diag {
                dw.maybe_flush();
            }
        }
    }
}

/// Load beacon parameters from a JSON file if it has changed since last check.
/// Returns true if params were updated.
pub fn load_beacon(
    path: &std::path::Path,
    last_mtime: &mut std::time::SystemTime,
    config: &mut FairPriceConfig,
) -> bool {
    let metadata = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return false,
    };
    let mtime = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
    if mtime <= *last_mtime {
        return false;
    }

    let data = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) => {
            log::warn!("Failed to read beacon file {}: {}", path.display(), e);
            return false;
        }
    };
    // Parse YAML, then convert to serde_json::Value for uniform access
    let yaml_val: serde_yaml::Value = match serde_yaml::from_str(&data) {
        Ok(v) => v,
        Err(e) => {
            log::warn!("Failed to parse beacon file {}: {}", path.display(), e);
            return false;
        }
    };
    let beacon: serde_json::Value = match serde_json::to_value(&yaml_val) {
        Ok(v) => v,
        Err(e) => {
            log::warn!("Failed to convert beacon YAML: {}", e);
            return false;
        }
    };

    let groups = match beacon.get("groups").and_then(|g| g.as_object()) {
        Some(g) => g,
        None => return false,
    };

    let mut applied = 0;
    for (group_idx, group) in config.groups.iter_mut().enumerate() {
        if let Some(params) = groups.get(&group.name).and_then(|p| p.as_object()) {
            // ann_vol → update vol provider
            let has_h = params.get("h_per_ms").and_then(|v| v.as_f64());
            let has_ann = params.get("ann_vol").and_then(|v| v.as_f64());
            match (has_h, has_ann) {
                (_, Some(av)) => {
                    config.vol_provider.set_ann_vol(group_idx, av);
                    if has_h.is_some() {
                        log::warn!(
                            "Group '{}': both h_per_ms and ann_vol specified, using ann_vol",
                            group.name
                        );
                    }
                }
                (Some(h), None) => {
                    config.vol_provider.set_ann_vol(group_idx, (h * MS_PER_YEAR).sqrt());
                }
                _ => {}
            }

            // Model selection
            if let Some(model_str) = params.get("model").and_then(|v| v.as_str()) {
                match model_str {
                    "kalman" => group.model = FairPriceModel::Kalman,
                    "gonzalo_granger" | "gg" => {
                        let max_lat = params.get("max_latency_ms")
                            .and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let decay_hl = params.get("decay_halflife_ms")
                            .and_then(|v| v.as_f64()).unwrap_or(0.0);
                        group.model = FairPriceModel::GonzaloGranger {
                            max_latency_ms: max_lat,
                            decay_halflife_ms: decay_hl,
                        };
                    }
                    _ => log::warn!("Unknown model '{}', keeping current", model_str),
                }
            }

            // EWMA params
            if let Some(v) = params.get("bias_ewma_halflife_ms").and_then(|v| v.as_f64()) {
                group.bias_ewma_halflife_ms = v;
            }
            if let Some(v) = params.get("spread_ewma_halflife_ms").and_then(|v| v.as_f64()) {
                group.spread_ewma_halflife_ms = v;
            }
            if let Some(v) = params.get("sigma_k_floor").and_then(|v| v.as_f64()) {
                group.sigma_k_floor = v;
            }
            if let Some(members) = params.get("members").and_then(|m| m.as_object()) {
                for member in group.members.iter_mut() {
                    let key = format!(
                        "{}/{}",
                        member.exchange.as_str(),
                        crate::symbol_registry::REGISTRY
                            .get_symbol(member.symbol_id)
                            .unwrap_or("?")
                    );
                    if let Some(mp) = members.get(&key).and_then(|v| v.as_object()) {
                        if let Some(v) = mp.get("bias").and_then(|v| v.as_f64()) {
                            member.bias = v;
                        }
                        if let Some(v) = mp.get("noise_var").and_then(|v| v.as_f64()) {
                            member.noise_var = v;
                        }
                        if let Some(v) = mp.get("gg_weight").and_then(|v| v.as_f64()) {
                            member.gg_weight = v;
                        }
                    }
                }
            }
            applied += 1;
        }
    }

    if applied > 0 {
        *last_mtime = mtime;
        log::info!(
            "Loaded beacon params for {} group(s) from {}",
            applied,
            path.display()
        );
    }
    applied > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::MarketData;
    use chrono::Utc;

    #[tokio::test]
    async fn test_fair_price_basic() {
        let tick_data = Arc::new(AllMarketData::new());

        let config = FairPriceConfig {
            interval_ms: 10,
            buffer_capacity: 1024,
            vol_provider: crate::vol_provider::VolProvider::new_static(vec![1.0]),
            groups: vec![FairPriceGroupConfig {
                name: "TEST".to_string(),
                members: vec![
                    GroupMember {
                        exchange: Exchange::Binance,
                        symbol_id: 0,
                        bias: 0.0,
                        noise_var: 1e-8,
                        gg_weight: 0.0,
                        reprice_group: None,
                        invert_reprice: false,
                    },
                    GroupMember {
                        exchange: Exchange::Coinbase,
                        symbol_id: 0,
                        bias: 0.0,
                        noise_var: 2e-8,
                        gg_weight: 0.0,
                        reprice_group: None,
                        invert_reprice: false,
                    },
                ],
                sigma_mode: SigmaMode::Static,
                model: FairPriceModel::Kalman,
                bias_ewma_halflife_ms: 0.0,
                spread_ewma_halflife_ms: 0.0,
                sigma_k_floor: 1e-6,
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
    }

    #[test]
    fn test_outputs_group_lookup() {
        let config = FairPriceConfig {
            interval_ms: 100,
            buffer_capacity: 1024,
            vol_provider: crate::vol_provider::VolProvider::new_static(vec![1e-2, 1e-2]),
            groups: vec![
                FairPriceGroupConfig {
                    name: "BTC".to_string(),
                    members: vec![],
                    sigma_mode: SigmaMode::Static,
                    model: FairPriceModel::Kalman,
                    bias_ewma_halflife_ms: 0.0,
                    spread_ewma_halflife_ms: 0.0,
                    sigma_k_floor: 1e-6,
                },
                FairPriceGroupConfig {
                    name: "ETH".to_string(),
                    members: vec![],
                    sigma_mode: SigmaMode::Static,
                    model: FairPriceModel::Kalman,
                    bias_ewma_halflife_ms: 0.0,
                    spread_ewma_halflife_ms: 0.0,
                    sigma_k_floor: 1e-6,
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
            vol_provider: crate::vol_provider::VolProvider::new_static(vec![1e-2]),
            groups: vec![FairPriceGroupConfig {
                name: "TEST".to_string(),
                members: vec![],
                sigma_mode: SigmaMode::Static,
                model: FairPriceModel::Kalman,
                bias_ewma_halflife_ms: 0.0,
                spread_ewma_halflife_ms: 0.0,
                sigma_k_floor: 1e-6,
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
                vol_ann_pct: 45.0,
                snap_ts_ns: 1000,
                n_ticks_used: 5,
                _pad: [0; 4],
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
}
