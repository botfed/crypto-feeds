use crate::market_data::{AllMarketData, Exchange, MarketData};
use crate::snapshot::{AllSnapshotData, SnapshotData};
use crate::symbol_registry::SymbolId;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum SnapshotField {
    Bid,
    Ask,
    BidQty,
    AskQty,
    Midquote,
    Spread,
    LogReturn,
    MidHigh,
    MidLow,
    BidHigh,
    AskLow,
    ExchangeLatMs,
    ReceiveLatMs,
}

impl SnapshotField {
    pub fn extract(&self, snap: &SnapshotData) -> f64 {
        match self {
            Self::Bid => snap.bid,
            Self::Ask => snap.ask,
            Self::BidQty => snap.bid_qty,
            Self::AskQty => snap.ask_qty,
            Self::Midquote => snap.midquote,
            Self::Spread => snap.spread,
            Self::LogReturn => snap.log_return,
            Self::MidHigh => snap.mid_high,
            Self::MidLow => snap.mid_low,
            Self::BidHigh => snap.bid_high,
            Self::AskLow => snap.ask_low,
            Self::ExchangeLatMs => snap.exchange_lat_ms,
            Self::ReceiveLatMs => snap.receive_lat_ms,
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bid" => Some(Self::Bid),
            "ask" => Some(Self::Ask),
            "bid_qty" | "bidqty" => Some(Self::BidQty),
            "ask_qty" | "askqty" => Some(Self::AskQty),
            "midquote" | "mid" => Some(Self::Midquote),
            "spread" => Some(Self::Spread),
            "log_return" | "logreturn" => Some(Self::LogReturn),
            "mid_high" | "midhigh" | "high" => Some(Self::MidHigh),
            "mid_low" | "midlow" | "low" => Some(Self::MidLow),
            "bid_high" | "bidhigh" => Some(Self::BidHigh),
            "ask_low" | "asklow" => Some(Self::AskLow),
            "exchange_lat_ms" | "elat" => Some(Self::ExchangeLatMs),
            "receive_lat_ms" | "rlat" => Some(Self::ReceiveLatMs),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RangeStat {
    Mean,
    Median,
    Quantile(f64),
}

impl RangeStat {
    pub fn from_str(s: &str, q: Option<f64>) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "mean" => Some(Self::Mean),
            "median" => Some(Self::Median),
            "quantile" | "percentile" => Some(Self::Quantile(q.unwrap_or(0.5))),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QuoteSide {
    Bid,
    Ask,
}

impl QuoteSide {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bid" | "b" => Some(Self::Bid),
            "ask" | "a" => Some(Self::Ask),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuoteFillResult {
    pub n_fills: usize,
    pub n_total: usize,
    pub elapsed_secs: f64,
    pub mean_markout_bps: f64,
    pub stdev_markout_bps: f64,
}

pub struct Analytics {
    tick_data: Arc<AllMarketData>,
    snap_data: Arc<AllSnapshotData>,
}

impl Analytics {
    pub fn new(tick_data: Arc<AllMarketData>, snap_data: Arc<AllSnapshotData>) -> Self {
        Self {
            tick_data,
            snap_data,
        }
    }

    // -- snapshot-based helpers --

    fn read_snap_field(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        field: SnapshotField,
        n: usize,
    ) -> Option<Vec<f64>> {
        let coll = self.snap_data.get_collection(exchange);
        let buf = coll.get_buffer(&symbol_id)?;
        let mut raw = vec![SnapshotData::default(); n];
        let count = buf.read_last_n(n, &mut raw);
        if count == 0 {
            return None;
        }
        let values: Vec<f64> = raw[..count]
            .iter()
            .map(|s| field.extract(s))
            .filter(|v| !v.is_nan())
            .collect();
        if values.is_empty() {
            None
        } else {
            Some(values)
        }
    }

    // -- snapshot-based analytics --

    pub fn snap_mean(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        field: SnapshotField,
        n: usize,
    ) -> Option<f64> {
        let vals = self.read_snap_field(exchange, symbol_id, field, n)?;
        Some(mean(&vals))
    }

    pub fn snap_stdev(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        field: SnapshotField,
        n: usize,
    ) -> Option<f64> {
        let vals = self.read_snap_field(exchange, symbol_id, field, n)?;
        stdev(&vals)
    }

    pub fn snap_median(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        field: SnapshotField,
        n: usize,
    ) -> Option<f64> {
        let vals = self.read_snap_field(exchange, symbol_id, field, n)?;
        Some(median(&vals))
    }

    pub fn snap_quantile(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        field: SnapshotField,
        n: usize,
        q: f64,
    ) -> Option<f64> {
        let vals = self.read_snap_field(exchange, symbol_id, field, n)?;
        Some(quantile(&vals, q))
    }

    // -- convenience methods --

    pub fn midquote_twap(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        self.snap_mean(exchange, symbol_id, SnapshotField::Midquote, n)
    }

    pub fn mean_spread(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        self.snap_mean(exchange, symbol_id, SnapshotField::Spread, n)
    }

    pub fn midquote_stdev(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        self.snap_stdev(exchange, symbol_id, SnapshotField::Midquote, n)
    }

    pub fn realized_vol(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        self.snap_stdev(exchange, symbol_id, SnapshotField::LogReturn, n)
    }

    pub fn max_abs_log_return(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        let vals = self.read_snap_field(exchange, symbol_id, SnapshotField::LogReturn, n)?;
        vals.iter().map(|v| v.abs()).reduce(f64::max)
    }

    pub fn log_returns(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Vec<f64> {
        self.read_snap_field(exchange, symbol_id, SnapshotField::LogReturn, n)
            .unwrap_or_default()
    }

    // -- tick-based analytics --

    pub fn tick_midquote_mean(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        let coll = self.tick_data.get_collection(exchange);
        let buf = coll.get_buffer(&symbol_id)?;
        let mut raw = vec![MarketData::default(); n];
        let count = buf.read_last_n(n, &mut raw);
        if count == 0 {
            return None;
        }
        let mids: Vec<f64> = raw[..count]
            .iter()
            .filter_map(|md| md.midquote())
            .collect();
        if mids.is_empty() {
            None
        } else {
            Some(mean(&mids))
        }
    }

    // -- fill analysis --

    fn read_snaps(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<Vec<SnapshotData>> {
        let coll = self.snap_data.get_collection(exchange);
        let buf = coll.get_buffer(&symbol_id)?;
        let mut raw = vec![SnapshotData::default(); n];
        let count = buf.read_last_n(n, &mut raw);
        if count < 2 {
            return None;
        }
        // read_last_n returns most-recent first; reverse to chronological
        let mut snaps = raw[..count].to_vec();
        snaps.reverse();
        Some(snaps)
    }

    /// Simulate quoting at a fixed spread from mid and compute fill rate + markout.
    ///
    /// For Ask side: quote_price = mid * (1 + spread_bps/10000)
    ///   fill if bid_high >= quote_price
    ///   markout = (quote_price - next_mid) / mid * 10000  (positive = MM profit)
    ///
    /// For Bid side: quote_price = mid * (1 - spread_bps/10000)
    ///   fill if ask_low <= quote_price
    ///   markout = (next_mid - quote_price) / mid * 10000  (positive = MM profit)
    pub fn quote_fill_analysis(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        side: QuoteSide,
        spread_bps: f64,
        lookback_snaps: usize,
    ) -> Option<QuoteFillResult> {
        let snaps = self.read_snaps(exchange, symbol_id, lookback_snaps)?;
        let n = snaps.len();
        if n < 2 {
            return None;
        }

        // Compute actual elapsed time from snapshot timestamps
        let first_ts = snaps.first().unwrap().snap_ts_ns;
        let last_ts = snaps.last().unwrap().snap_ts_ns;
        let elapsed_secs = if first_ts > 0 && last_ts > first_ts {
            (last_ts - first_ts) as f64 / 1_000_000_000.0
        } else {
            // Fallback: estimate from snapshot count at 100ms intervals
            (n - 1) as f64 * 0.1
        };

        let spread_mult = spread_bps / 10_000.0;
        let mut markouts = Vec::new();
        // We can evaluate fills for snaps[0..n-1] (need snaps[t+1] for markout)
        let n_total = n - 1;

        for t in 0..n_total {
            let snap = &snaps[t];
            let mid = snap.midquote;
            if mid <= 0.0 || mid.is_nan() {
                continue;
            }

            let (quote_price, filled) = match side {
                QuoteSide::Ask => {
                    let qp = mid * (1.0 + spread_mult);
                    (qp, snap.bid_high >= qp)
                }
                QuoteSide::Bid => {
                    let qp = mid * (1.0 - spread_mult);
                    (qp, snap.ask_low <= qp)
                }
            };

            if filled {
                let next_mid = snaps[t + 1].midquote;
                if next_mid <= 0.0 || next_mid.is_nan() {
                    continue;
                }
                let markout = match side {
                    QuoteSide::Ask => (quote_price - next_mid) / mid * 10_000.0,
                    QuoteSide::Bid => (next_mid - quote_price) / mid * 10_000.0,
                };
                markouts.push(markout);
            }
        }

        let n_fills = markouts.len();
        if n_fills == 0 {
            return Some(QuoteFillResult {
                n_fills: 0,
                n_total,
                elapsed_secs,
                mean_markout_bps: 0.0,
                stdev_markout_bps: 0.0,
            });
        }

        let mean_mo = mean(&markouts);
        let stdev_mo = stdev(&markouts).unwrap_or(0.0);

        Some(QuoteFillResult {
            n_fills,
            n_total,
            elapsed_secs,
            mean_markout_bps: mean_mo,
            stdev_markout_bps: stdev_mo,
        })
    }

    /// Resample mid-quote range into time buckets, returning range in bps per bucket.
    ///
    /// Groups `window_snaps` snapshots into chunks of `bucket_size`, and for each
    /// bucket computes `(max(mid_high) - min(mid_low)) / avg * 10_000`.
    ///
    /// Example: with 100ms snapshots, `window_snaps=36000, bucket_size=10` gives
    /// the mid-range in bps per 1-second bucket over the past hour.
    pub fn mid_range_bps_buckets(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        window_snaps: usize,
        bucket_size: usize,
    ) -> Option<Vec<f64>> {
        if bucket_size == 0 {
            return None;
        }
        let snaps = self.read_snaps(exchange, symbol_id, window_snaps)?;
        let mut ranges = Vec::with_capacity(snaps.len() / bucket_size + 1);
        for chunk in snaps.chunks(bucket_size) {
            let mut hi = f64::NEG_INFINITY;
            let mut lo = f64::INFINITY;
            for s in chunk {
                if s.mid_high > hi {
                    hi = s.mid_high;
                }
                if s.mid_low < lo {
                    lo = s.mid_low;
                }
            }
            if hi > lo && lo > 0.0 {
                let mid = (hi + lo) / 2.0;
                ranges.push((hi - lo) / mid * 10_000.0);
            }
        }
        if ranges.is_empty() {
            None
        } else {
            Some(ranges)
        }
    }

    /// Compute a summary statistic over resampled mid-range bps buckets.
    ///
    /// Combines `mid_range_bps_buckets` with a reducer (mean / median / quantile).
    /// Use case: "median 1s mid-range over the past hour" as a candidate spread.
    pub fn mid_range_bps_stat(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        window_snaps: usize,
        bucket_size: usize,
        stat: RangeStat,
    ) -> Option<f64> {
        let ranges = self.mid_range_bps_buckets(exchange, symbol_id, window_snaps, bucket_size)?;
        Some(match stat {
            RangeStat::Mean => mean(&ranges),
            RangeStat::Median => median(&ranges),
            RangeStat::Quantile(q) => quantile(&ranges, q),
        })
    }

    pub fn get_latest_snapshot(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
    ) -> Option<SnapshotData> {
        self.snap_data.get_collection(exchange).latest(&symbol_id)
    }

    /// Compute all display analytics for one symbol from a single snapshot read.
    ///
    /// `scratch` is a reusable buffer to avoid per-call allocations. It will be
    /// resized to `lookback` on the first call and reused thereafter.
    pub fn compute_display_analytics(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        lookback: usize,
        twap_n: usize,
        vol_n: usize,
        bucket_size: usize,
        scratch: &mut Vec<SnapshotData>,
    ) -> Option<DisplayAnalytics> {
        // Ensure scratch is large enough, reuse across calls
        if scratch.len() < lookback {
            scratch.resize(lookback, SnapshotData::default());
        }
        let coll = self.snap_data.get_collection(exchange);
        let buf = coll.get_buffer(&symbol_id)?;
        let count = buf.read_last_n(lookback, &mut scratch[..lookback]);
        if count < 2 {
            return None;
        }
        // read_last_n returns most-recent first; reverse to chronological
        let snaps = &mut scratch[..count];
        snaps.reverse();
        let n = snaps.len();

        // Helper: extract a field from a slice, filter NaN
        let extract = |field: SnapshotField, from: &[SnapshotData]| -> Vec<f64> {
            from.iter().map(|s| field.extract(s)).filter(|v| !v.is_nan()).collect()
        };

        // TWAP: mean of last twap_n midquotes
        let twap_start = n.saturating_sub(twap_n);
        let twap_mids: Vec<f64> = snaps[twap_start..]
            .iter()
            .map(|s| s.midquote)
            .filter(|v| !v.is_nan() && *v > 0.0)
            .collect();
        let twap = if twap_mids.is_empty() { None } else { Some(mean(&twap_mids)) };

        // Spread: sort once, read median
        let mut spreads = extract(SnapshotField::Spread, snaps);
        let mdn_spread = if spreads.is_empty() {
            None
        } else {
            spreads.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            Some(quantile_sorted(&spreads, 0.5))
        };

        // Log returns: extract once over full window for max_jump, take tail for vol
        let all_log_rets = extract(SnapshotField::LogReturn, snaps);
        let max_jump = all_log_rets.iter().map(|v| v.abs()).reduce(f64::max);
        let vol_start = all_log_rets.len().saturating_sub(vol_n);
        let vol = stdev(&all_log_rets[vol_start..]);

        // Latency: skip first 5s of observations (warmup), sort once per field
        const WARMUP_NS: i64 = 5_000_000_000;
        let first_ts = snaps[0].snap_ts_ns;
        let lat_start = if first_ts > 0 {
            snaps.iter().position(|s| s.snap_ts_ns >= first_ts + WARMUP_NS).unwrap_or(0)
        } else {
            0
        };
        let lat_snaps = &snaps[lat_start..];

        let mut el_vals = extract(SnapshotField::ExchangeLatMs, lat_snaps);
        let (el_p50, el_p9999) = if el_vals.is_empty() {
            (None, None)
        } else {
            let qs = sort_and_quantiles(&mut el_vals, &[0.5, 0.9999]);
            (Some(qs[0]), Some(qs[1]))
        };

        let mut rl_vals = extract(SnapshotField::ReceiveLatMs, lat_snaps);
        let (rl_p50, rl_p9999) = if rl_vals.is_empty() {
            (None, None)
        } else {
            let qs = sort_and_quantiles(&mut rl_vals, &[0.5, 0.9999]);
            (Some(qs[0]), Some(qs[1]))
        };

        // Mid-range bps buckets: sort once, read median + p99
        let mut ranges = compute_range_buckets(snaps, bucket_size);
        let (mdn_rng, p99_rng) = if ranges.is_empty() {
            (None, None)
        } else {
            let qs = sort_and_quantiles(&mut ranges, &[0.5, 0.99]);
            (Some(qs[0]), Some(qs[1]))
        };

        // Fill analysis
        let half_spread = p99_rng.map(|r| r / 2.0);
        let (bid_fill, ask_fill) = match half_spread {
            Some(hs) if hs > 0.0 => (
                compute_fills(&snaps, QuoteSide::Bid, hs),
                compute_fills(&snaps, QuoteSide::Ask, hs),
            ),
            _ => (None, None),
        };

        Some(DisplayAnalytics {
            twap,
            mdn_spread,
            vol,
            max_jump,
            el_p50,
            el_p9999,
            rl_p50,
            rl_p9999,
            mdn_rng,
            p99_rng,
            bid_fill,
            ask_fill,
        })
    }
}

/// Pre-computed analytics for one exchange-symbol, from a single snapshot read.
#[derive(Clone)]
pub struct DisplayAnalytics {
    pub twap: Option<f64>,
    pub mdn_spread: Option<f64>,
    pub vol: Option<f64>,
    pub max_jump: Option<f64>,
    pub el_p50: Option<f64>,
    pub el_p9999: Option<f64>,
    pub rl_p50: Option<f64>,
    pub rl_p9999: Option<f64>,
    pub mdn_rng: Option<f64>,
    pub p99_rng: Option<f64>,
    pub bid_fill: Option<QuoteFillResult>,
    pub ask_fill: Option<QuoteFillResult>,
}

/// Compute mid-range bps per bucket from a chronological slice of snapshots.
fn compute_range_buckets(snaps: &[SnapshotData], bucket_size: usize) -> Vec<f64> {
    if bucket_size == 0 {
        return Vec::new();
    }
    let mut ranges = Vec::with_capacity(snaps.len() / bucket_size + 1);
    for chunk in snaps.chunks(bucket_size) {
        let mut hi = f64::NEG_INFINITY;
        let mut lo = f64::INFINITY;
        for s in chunk {
            if s.mid_high > hi { hi = s.mid_high; }
            if s.mid_low < lo { lo = s.mid_low; }
        }
        if hi > lo && lo > 0.0 {
            let mid = (hi + lo) / 2.0;
            ranges.push((hi - lo) / mid * 10_000.0);
        }
    }
    ranges
}

/// Compute fill analysis from a chronological slice of snapshots.
fn compute_fills(snaps: &[SnapshotData], side: QuoteSide, spread_bps: f64) -> Option<QuoteFillResult> {
    let n = snaps.len();
    if n < 2 {
        return None;
    }

    let first_ts = snaps.first().unwrap().snap_ts_ns;
    let last_ts = snaps.last().unwrap().snap_ts_ns;
    let elapsed_secs = if first_ts > 0 && last_ts > first_ts {
        (last_ts - first_ts) as f64 / 1_000_000_000.0
    } else {
        (n - 1) as f64 * 0.1
    };

    let spread_mult = spread_bps / 10_000.0;
    let mut markouts = Vec::new();
    let n_total = n - 1;

    for t in 0..n_total {
        let snap = &snaps[t];
        let mid = snap.midquote;
        if mid <= 0.0 || mid.is_nan() {
            continue;
        }

        let (quote_price, filled) = match side {
            QuoteSide::Ask => {
                let qp = mid * (1.0 + spread_mult);
                (qp, snap.bid_high >= qp)
            }
            QuoteSide::Bid => {
                let qp = mid * (1.0 - spread_mult);
                (qp, snap.ask_low <= qp)
            }
        };

        if filled {
            let next_mid = snaps[t + 1].midquote;
            if next_mid <= 0.0 || next_mid.is_nan() {
                continue;
            }
            let markout = match side {
                QuoteSide::Ask => (quote_price - next_mid) / mid * 10_000.0,
                QuoteSide::Bid => (next_mid - quote_price) / mid * 10_000.0,
            };
            markouts.push(markout);
        }
    }

    let n_fills = markouts.len();
    if n_fills == 0 {
        return Some(QuoteFillResult {
            n_fills: 0,
            n_total,
            elapsed_secs,
            mean_markout_bps: 0.0,
            stdev_markout_bps: 0.0,
        });
    }

    let mean_mo = mean(&markouts);
    let stdev_mo = stdev(&markouts).unwrap_or(0.0);

    Some(QuoteFillResult {
        n_fills,
        n_total,
        elapsed_secs,
        mean_markout_bps: mean_mo,
        stdev_markout_bps: stdev_mo,
    })
}

// -- stateless math helpers --

fn mean(vals: &[f64]) -> f64 {
    vals.iter().sum::<f64>() / vals.len() as f64
}

fn stdev(vals: &[f64]) -> Option<f64> {
    if vals.len() < 2 {
        return None;
    }
    let m = mean(vals);
    let variance = vals.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (vals.len() - 1) as f64;
    Some(variance.sqrt())
}

fn median(vals: &[f64]) -> f64 {
    quantile(vals, 0.5)
}

/// Linear-interpolation quantile (same as numpy default).
/// Clones and sorts internally — use `quantile_sorted` when you need multiple
/// quantiles from the same data.
fn quantile(vals: &[f64], q: f64) -> f64 {
    let mut sorted = vals.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    quantile_sorted(&sorted, q)
}

/// Read a quantile from an already-sorted slice.
fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return f64::NAN;
    }
    if n == 1 {
        return sorted[0];
    }
    let pos = q * (n - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = pos - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

/// Sort a vec in-place and return multiple quantiles from it.
fn sort_and_quantiles(vals: &mut Vec<f64>, qs: &[f64]) -> Vec<f64> {
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    qs.iter().map(|&q| quantile_sorted(vals, q)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::AllMarketData;
    use crate::snapshot::{AllSnapshotData, SnapshotData};

    fn make_analytics(snaps: &[(SymbolId, Vec<SnapshotData>)]) -> Analytics {
        let tick_data = Arc::new(AllMarketData::new());
        let snap_data = Arc::new(AllSnapshotData::new(1024));

        for (sym_id, snapshots) in snaps {
            for s in snapshots {
                snap_data.binance.push(sym_id, *s);
            }
        }

        Analytics::new(tick_data, snap_data)
    }

    fn snap_with_mid(mid: f64) -> SnapshotData {
        SnapshotData {
            bid: mid - 0.5,
            ask: mid + 0.5,
            bid_qty: 1.0,
            ask_qty: 1.0,
            midquote: mid,
            spread: 1.0,
            log_return: f64::NAN,
            mid_high: mid + 0.1,
            mid_low: mid - 0.1,
            bid_high: mid - 0.4,
            ask_low: mid + 0.4,
            exchange_lat_ms: f64::NAN,
            receive_lat_ms: f64::NAN,
            exchange_ts_ns: 0,
            received_ts_ns: 0,
            snap_ts_ns: 0,
        }
    }

    #[test]
    fn test_mean() {
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((mean(&vals) - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_stdev() {
        let vals = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let s = stdev(&vals).unwrap();
        // sample stdev (n-1): variance = 32/7 ≈ 4.571, stdev ≈ 2.138
        assert!((s - 2.138).abs() < 0.01);
    }

    #[test]
    fn test_snap_mean_midquote() {
        let snaps: Vec<SnapshotData> = (0..10).map(|i| snap_with_mid(100.0 + i as f64)).collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let twap = analytics
            .midquote_twap(&Exchange::Binance, 0, 10)
            .unwrap();
        assert!((twap - 104.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mean_spread() {
        let snaps: Vec<SnapshotData> = (0..5).map(|_| snap_with_mid(100.0)).collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let spread = analytics
            .mean_spread(&Exchange::Binance, 0, 5)
            .unwrap();
        assert!((spread - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_realized_vol() {
        let snaps: Vec<SnapshotData> = vec![
            SnapshotData {
                log_return: 0.01,
                midquote: 100.0,
                ..Default::default()
            },
            SnapshotData {
                log_return: -0.01,
                midquote: 99.0,
                ..Default::default()
            },
            SnapshotData {
                log_return: 0.02,
                midquote: 101.0,
                ..Default::default()
            },
            SnapshotData {
                log_return: -0.02,
                midquote: 99.0,
                ..Default::default()
            },
        ];
        let analytics = make_analytics(&[(0, snaps)]);
        let vol = analytics.realized_vol(&Exchange::Binance, 0, 10);
        assert!(vol.is_some());
        assert!(vol.unwrap() > 0.0);
    }

    #[test]
    fn test_log_returns() {
        let snaps: Vec<SnapshotData> = vec![
            SnapshotData {
                log_return: 0.01,
                ..Default::default()
            },
            SnapshotData {
                log_return: f64::NAN,
                ..Default::default()
            },
            SnapshotData {
                log_return: -0.01,
                ..Default::default()
            },
        ];
        let analytics = make_analytics(&[(0, snaps)]);
        let returns = analytics.log_returns(&Exchange::Binance, 0, 10);
        // NAN should be filtered out
        assert_eq!(returns.len(), 2);
    }

    #[test]
    fn test_quantile_p99() {
        let vals: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let q = quantile(&vals, 0.9999);
        // 0.9999 * 999 = 998.9001 → interpolate between 998 and 999
        assert!((q - 998.9001).abs() < 0.01);
    }

    #[test]
    fn test_quantile_extremes() {
        let vals = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        assert!((quantile(&vals, 0.0) - 10.0).abs() < f64::EPSILON);
        assert!((quantile(&vals, 1.0) - 50.0).abs() < f64::EPSILON);
        assert!((quantile(&vals, 0.5) - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_median_uses_quantile() {
        // Verify median still works after refactor
        let vals = vec![1.0, 3.0, 2.0];
        assert!((median(&vals) - 2.0).abs() < f64::EPSILON);
        let vals = vec![1.0, 2.0, 3.0, 4.0];
        assert!((median(&vals) - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_quote_fill_ask_side() {
        // 10 snapshots at mid=100, bid_high=100.03 (3 bps above mid)
        // Quote ask at 2 bps above mid → quote = 100.02 → bid_high 100.03 >= 100.02 → filled
        let snaps: Vec<SnapshotData> = (0..10)
            .map(|_| SnapshotData {
                bid: 99.5,
                ask: 100.5,
                midquote: 100.0,
                spread: 1.0,
                bid_high: 100.03, // 3 bps above mid
                ask_low: 99.97,
                mid_high: 100.05,
                mid_low: 99.95,
                log_return: 0.0,
                ..Default::default()
            })
            .collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let result = analytics
            .quote_fill_analysis(&Exchange::Binance, 0, QuoteSide::Ask, 2.0, 10)
            .unwrap();
        // All 9 evaluable snapshots should be filled (bid_high=100.03 >= 100.02)
        assert_eq!(result.n_fills, 9);
        assert_eq!(result.n_total, 9);
        // markout = (100.02 - 100.0) / 100.0 * 10000 = 2.0 bps
        assert!((result.mean_markout_bps - 2.0).abs() < 0.01);
        // all markouts identical → stdev = 0
        assert!(result.stdev_markout_bps < 0.01);
    }

    #[test]
    fn test_quote_fill_bid_side() {
        // ask_low = 99.97 (3 bps below mid)
        // Quote bid at 2 bps below mid → quote = 99.98 → ask_low 99.97 <= 99.98 → filled
        let snaps: Vec<SnapshotData> = (0..10)
            .map(|_| SnapshotData {
                bid: 99.5,
                ask: 100.5,
                midquote: 100.0,
                spread: 1.0,
                bid_high: 100.03,
                ask_low: 99.97,
                mid_high: 100.05,
                mid_low: 99.95,
                log_return: 0.0,
                ..Default::default()
            })
            .collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let result = analytics
            .quote_fill_analysis(&Exchange::Binance, 0, QuoteSide::Bid, 2.0, 10)
            .unwrap();
        assert_eq!(result.n_fills, 9);
        assert_eq!(result.n_total, 9);
        // markout = (100.0 - 99.98) / 100.0 * 10000 = 2.0 bps
        assert!((result.mean_markout_bps - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_quote_fill_no_fills() {
        // bid_high = 100.01 (1 bp above mid)
        // Quote ask at 5 bps → quote = 100.05 → bid_high 100.01 < 100.05 → no fill
        let snaps: Vec<SnapshotData> = (0..10)
            .map(|_| SnapshotData {
                bid: 99.5,
                ask: 100.5,
                midquote: 100.0,
                spread: 1.0,
                bid_high: 100.01,
                ask_low: 99.99,
                mid_high: 100.02,
                mid_low: 99.98,
                log_return: 0.0,
                ..Default::default()
            })
            .collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let result = analytics
            .quote_fill_analysis(&Exchange::Binance, 0, QuoteSide::Ask, 5.0, 10)
            .unwrap();
        assert_eq!(result.n_fills, 0);
        assert_eq!(result.n_total, 9);
    }

    #[test]
    fn test_quote_fill_with_varying_mids() {
        // Mid moves: 100, 100.01, 100.02, 100.01, 100.0
        // Ask quote at 2 bps: 100.02, 100.0302, 100.0400, 100.0302, 100.02
        // bid_high always 100.03 → fills where 100.03 >= quote_price
        // snap0: qp=100.02, fill yes, markout=(100.02-100.01)/100*10000=1.0 bps
        // snap1: qp=100.0302, fill no (100.03 < 100.0302)
        // snap2: qp=100.0400, fill no
        // snap3: qp=100.0302, fill no
        let snaps = vec![
            SnapshotData { midquote: 100.0,  bid_high: 100.03, ask_low: 99.97, ..Default::default() },
            SnapshotData { midquote: 100.01, bid_high: 100.03, ask_low: 99.97, ..Default::default() },
            SnapshotData { midquote: 100.02, bid_high: 100.03, ask_low: 99.97, ..Default::default() },
            SnapshotData { midquote: 100.01, bid_high: 100.03, ask_low: 99.97, ..Default::default() },
            SnapshotData { midquote: 100.0,  bid_high: 100.03, ask_low: 99.97, ..Default::default() },
        ];
        let analytics = make_analytics(&[(0, snaps)]);

        let result = analytics
            .quote_fill_analysis(&Exchange::Binance, 0, QuoteSide::Ask, 2.0, 10)
            .unwrap();
        // Only snap0 fills (qp=100.02 <= 100.03)
        assert_eq!(result.n_fills, 1);
        assert_eq!(result.n_total, 4);
        // markout = (100.02 - 100.01) / 100.0 * 10000 = 1.0 bps
        assert!((result.mean_markout_bps - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_mid_range_bps_buckets() {
        // 20 snapshots, bucket_size=5 → 4 buckets
        // Each snap has mid_high = mid + 0.1, mid_low = mid - 0.1
        // So each bucket: hi = max(mid_high), lo = min(mid_low)
        // All mids = 100 → hi=100.1, lo=99.9 → range = 0.2, avg = 100.0
        // range_bps = 0.2 / 100.0 * 10000 = 20.0
        let snaps: Vec<SnapshotData> = (0..20).map(|_| snap_with_mid(100.0)).collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let buckets = analytics
            .mid_range_bps_buckets(&Exchange::Binance, 0, 20, 5)
            .unwrap();
        assert_eq!(buckets.len(), 4);
        for &b in &buckets {
            assert!((b - 20.0).abs() < 0.01);
        }
    }

    #[test]
    fn test_mid_range_bps_buckets_varying() {
        // 6 snapshots, bucket_size=3 → 2 buckets
        // Bucket 0: mids 100, 101, 102 → mid_highs 100.1, 101.1, 102.1 → hi=102.1
        //                                 mid_lows  99.9, 100.9, 101.9 → lo=99.9
        //           range = 2.2, avg = 101.0, bps = 2.2/101.0 * 10000 ≈ 217.82
        // Bucket 1: mids 103, 104, 105 → hi=105.1, lo=102.9
        //           range = 2.2, avg = 104.0, bps = 2.2/104.0 * 10000 ≈ 211.54
        let snaps: Vec<SnapshotData> = (0..6)
            .map(|i| snap_with_mid(100.0 + i as f64))
            .collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let buckets = analytics
            .mid_range_bps_buckets(&Exchange::Binance, 0, 6, 3)
            .unwrap();
        assert_eq!(buckets.len(), 2);
        assert!((buckets[0] - 217.82).abs() < 0.1);
        assert!((buckets[1] - 211.54).abs() < 0.1);
    }

    #[test]
    fn test_mid_range_bps_stat_mean() {
        let snaps: Vec<SnapshotData> = (0..20).map(|_| snap_with_mid(100.0)).collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let result = analytics
            .mid_range_bps_stat(&Exchange::Binance, 0, 20, 5, RangeStat::Mean)
            .unwrap();
        assert!((result - 20.0).abs() < 0.01);
    }

    #[test]
    fn test_mid_range_bps_stat_median() {
        let snaps: Vec<SnapshotData> = (0..20).map(|_| snap_with_mid(100.0)).collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let result = analytics
            .mid_range_bps_stat(&Exchange::Binance, 0, 20, 5, RangeStat::Median)
            .unwrap();
        assert!((result - 20.0).abs() < 0.01);
    }

    #[test]
    fn test_mid_range_bps_stat_quantile() {
        // Create varying buckets to make quantile meaningful
        let mut snaps = Vec::new();
        // 10 buckets of 2 snaps each, with increasing volatility
        for i in 0..10 {
            let base = 100.0;
            let offset = i as f64 * 0.1; // increasing range per bucket
            snaps.push(SnapshotData {
                mid_high: base + offset,
                mid_low: base - offset,
                midquote: base,
                ..Default::default()
            });
            snaps.push(SnapshotData {
                mid_high: base + offset,
                mid_low: base - offset,
                midquote: base,
                ..Default::default()
            });
        }
        let analytics = make_analytics(&[(0, snaps)]);

        // p90 should be near the high end of the range distribution
        let p90 = analytics
            .mid_range_bps_stat(&Exchange::Binance, 0, 20, 2, RangeStat::Quantile(0.9))
            .unwrap();
        let p10 = analytics
            .mid_range_bps_stat(&Exchange::Binance, 0, 20, 2, RangeStat::Quantile(0.1))
            .unwrap();
        assert!(p90 > p10);
    }

    #[test]
    fn test_mid_range_bps_empty() {
        let analytics = make_analytics(&[]);
        assert!(analytics
            .mid_range_bps_buckets(&Exchange::Binance, 0, 100, 10)
            .is_none());
        assert!(analytics
            .mid_range_bps_stat(&Exchange::Binance, 0, 100, 10, RangeStat::Median)
            .is_none());
    }

    #[test]
    fn test_empty_returns_none() {
        let analytics = make_analytics(&[]);
        assert!(analytics.midquote_twap(&Exchange::Binance, 0, 10).is_none());
        assert!(analytics.realized_vol(&Exchange::Binance, 0, 10).is_none());
    }

    #[test]
    fn test_tick_midquote_mean() {
        use crate::market_data::MarketData;

        let tick_data = Arc::new(AllMarketData::new());
        let snap_data = Arc::new(AllSnapshotData::new(1024));

        let sym: SymbolId = 0;
        for i in 0..5 {
            tick_data.binance.push(
                &sym,
                MarketData {
                    bid: Some(100.0 + i as f64),
                    ask: Some(102.0 + i as f64),
                    bid_qty: Some(1.0),
                    ask_qty: Some(1.0),
                    exchange_ts: None,
                    received_ts: None,
                },
            );
        }

        let analytics = Analytics::new(tick_data, snap_data);
        let mean = analytics.tick_midquote_mean(&Exchange::Binance, 0, 5).unwrap();
        // mids: 101, 102, 103, 104, 105 → but read_last_n returns most recent first
        // mean of [103, 102, 101, 104, 105] = 103.0 regardless of order
        assert!((mean - 103.0).abs() < f64::EPSILON);
    }
}
