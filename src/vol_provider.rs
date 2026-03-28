use crate::bar_manager::BarBuilder;
use crate::vol_engine::raw_gk;

const MS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0 * 1000.0;
const LN2: f64 = std::f64::consts::LN_2;
const MINUTES_PER_YEAR: f64 = 525_960.0;

/// Vol provider abstraction — the fair price engine queries this for h_per_ms.
/// Each variant is a self-contained vol engine.
pub enum VolProvider {
    /// Static ann_vol per group. No adaptation.
    Static {
        ann_vols: Vec<f64>,
    },
    /// EWMA of squared log-returns on settled fair price.
    /// Degenerates to static when halflife_ms = 0.
    Ewma {
        states: Vec<EwmaVolState>,
    },
    /// GK variance on fair-price OHLC bars, EWMA smoothed in log-space.
    GkEwma {
        states: Vec<GkEwmaVolState>,
    },
}

pub struct EwmaVolState {
    h_per_ms: f64,
    halflife_ms: f64,
    h_floor: f64,
    /// Previous snapshot's log(fair_price). NAN until first valid update.
    log_fp_prev: f64,
    snap_ts_prev_ns: i64,
}

pub struct GkEwmaVolState {
    h_per_ms: f64,
    halflife_ms: f64,
    h_floor: f64,
    bar_builder: BarBuilder,
    bars_processed: usize,
    target_min: usize,
    ewma_log_gk: f64,
    ewma_log_gk_sq: f64,
}

impl VolProvider {
    /// Build a static provider from per-group annualized vols.
    pub fn new_static(ann_vols: Vec<f64>) -> Self {
        VolProvider::Static { ann_vols }
    }

    /// Build an EWMA provider from per-group (ann_vol, halflife_ms, floor_ann).
    pub fn new_ewma(groups: &[(f64, f64, f64)]) -> Self {
        let states = groups
            .iter()
            .map(|&(ann_vol, halflife_ms, floor_ann)| EwmaVolState {
                h_per_ms: ann_vol * ann_vol / MS_PER_YEAR,
                halflife_ms,
                h_floor: floor_ann * floor_ann / MS_PER_YEAR,
                log_fp_prev: f64::NAN,
                snap_ts_prev_ns: 0,
            })
            .collect();
        VolProvider::Ewma { states }
    }

    /// Build a GK+EWMA provider from per-group (ann_vol, halflife_ms, floor_ann) and bar length.
    pub fn new_gk_ewma(groups: &[(f64, f64, f64)], target_min: usize) -> Self {
        let bars_per_year = MINUTES_PER_YEAR / target_min as f64;
        let states = groups
            .iter()
            .map(|&(ann_vol, halflife_ms, floor_ann)| {
                let var_per_bar = ann_vol * ann_vol / bars_per_year;
                let log_gk_seed = if var_per_bar > 0.0 { var_per_bar.ln() } else { f64::NAN };
                GkEwmaVolState {
                    h_per_ms: ann_vol * ann_vol / MS_PER_YEAR,
                    halflife_ms,
                    h_floor: floor_ann * floor_ann / MS_PER_YEAR,
                    bar_builder: BarBuilder::new(target_min, 64),
                    bars_processed: 0,
                    target_min,
                    ewma_log_gk: log_gk_seed,
                    ewma_log_gk_sq: if log_gk_seed.is_finite() { log_gk_seed * log_gk_seed } else { f64::NAN },
                    }
            })
            .collect();
        VolProvider::GkEwma { states }
    }

    /// Current h_per_ms (per-millisecond transition variance) for a group.
    pub fn h_per_ms(&self, group_idx: usize) -> f64 {
        match self {
            VolProvider::Static { ann_vols } => {
                let av = ann_vols[group_idx];
                av * av / MS_PER_YEAR
            }
            VolProvider::Ewma { states } => states[group_idx].h_per_ms,
            VolProvider::GkEwma { states } => states[group_idx].h_per_ms,
        }
    }

    /// Current annualized vol (fraction, not percent) for a group.
    pub fn ann_vol(&self, group_idx: usize) -> f64 {
        match self {
            VolProvider::Static { ann_vols } => ann_vols[group_idx],
            VolProvider::Ewma { states } => {
                (states[group_idx].h_per_ms * MS_PER_YEAR).sqrt()
            }
            VolProvider::GkEwma { states } => {
                (states[group_idx].h_per_ms * MS_PER_YEAR).sqrt()
            }
        }
    }

    /// Feed the estimator with the settled fair price after each snapshot.
    /// Skips if fair_price <= 0 (not yet initialized).
    pub fn update(&mut self, group_idx: usize, fair_price: f64, snap_ts_ns: i64) {
        if fair_price <= 0.0 {
            return;
        }
        match self {
            VolProvider::Static { .. } => {}
            VolProvider::Ewma { states } => {
                let s = &mut states[group_idx];
                let log_fp = fair_price.ln();

                if s.halflife_ms > 0.0 && s.log_fp_prev.is_finite() && s.snap_ts_prev_ns > 0 {
                    let tau_ms = (snap_ts_ns - s.snap_ts_prev_ns) as f64 / 1e6;
                    if tau_ms > 0.0 {
                        let r = log_fp - s.log_fp_prev;
                        let r2 = r * r;
                        let d = (-tau_ms * LN2 / s.halflife_ms).exp();
                        s.h_per_ms = d * s.h_per_ms + (1.0 - d) * (r2 / tau_ms);
                        s.h_per_ms = s.h_per_ms.max(s.h_floor);
                    }
                }
                s.log_fp_prev = log_fp;
                s.snap_ts_prev_ns = snap_ts_ns;
            }
            VolProvider::GkEwma { states } => {
                let s = &mut states[group_idx];
                let ts_ms = snap_ts_ns / 1_000_000;
                s.bar_builder.feed(fair_price, ts_ms);

                // Process any newly completed bars
                let total = s.bar_builder.total_produced();
                if total > s.bars_processed {
                    let completed = s.bar_builder.completed();
                    let n = completed.len();
                    let new_count = (total - s.bars_processed).min(n);
                    let bar_ms = s.target_min as f64 * 60_000.0;
                    for i in (n - new_count)..n {
                        let bar = &completed[i];
                        let gk = raw_gk(bar);
                        if gk > 0.0 {
                            let log_gk = gk.ln();
                            let d = (-bar_ms * LN2 / s.halflife_ms).exp();
                            s.ewma_log_gk = d * s.ewma_log_gk + (1.0 - d) * log_gk;
                            s.ewma_log_gk_sq = d * s.ewma_log_gk_sq + (1.0 - d) * log_gk * log_gk;
                            // Bias-corrected: E[GK] = exp(μ + σ²/2)
                            let var_log = (s.ewma_log_gk_sq - s.ewma_log_gk * s.ewma_log_gk).max(0.0);
                            s.h_per_ms = (s.ewma_log_gk + var_log / 2.0).exp() / bar_ms;
                            s.h_per_ms = s.h_per_ms.max(s.h_floor);
                        }
                    }
                    s.bars_processed = total;
                }

                // Virtual step: what would h_per_ms be if the current partial bar closed now?
                if let Some(head) = s.bar_builder.virtual_head(fair_price) {
                    let gk = raw_gk(&head);
                    if gk > 0.0 {
                        let bar_ms = s.target_min as f64 * 60_000.0;
                        let log_gk = gk.ln();
                        let d = (-bar_ms * LN2 / s.halflife_ms).exp();
                        let virt_ewma = d * s.ewma_log_gk + (1.0 - d) * log_gk;
                        let virt_sq = d * s.ewma_log_gk_sq + (1.0 - d) * log_gk * log_gk;
                        let var_log = (virt_sq - virt_ewma * virt_ewma).max(0.0);
                        s.h_per_ms = (virt_ewma + var_log / 2.0).exp() / bar_ms;
                        s.h_per_ms = s.h_per_ms.max(s.h_floor);
                    }
                }
            }
        }
    }

    /// Update the seed ann_vol for a group (e.g. from beacon reload).
    pub fn set_ann_vol(&mut self, group_idx: usize, ann_vol: f64) {
        match self {
            VolProvider::Static { ann_vols } => {
                ann_vols[group_idx] = ann_vol;
            }
            VolProvider::Ewma { states } => {
                states[group_idx].h_per_ms = ann_vol * ann_vol / MS_PER_YEAR;
            }
            VolProvider::GkEwma { states } => {
                let s = &mut states[group_idx];
                s.h_per_ms = ann_vol * ann_vol / MS_PER_YEAR;
                let bars_per_year = MINUTES_PER_YEAR / s.target_min as f64;
                let var_per_bar = ann_vol * ann_vol / bars_per_year;
                if var_per_bar > 0.0 {
                    let log_gk = var_per_bar.ln();
                    s.ewma_log_gk = log_gk;
                    s.ewma_log_gk_sq = log_gk * log_gk;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_provider() {
        let vp = VolProvider::new_static(vec![1.0, 2.0]);
        let h0 = vp.h_per_ms(0);
        let h1 = vp.h_per_ms(1);
        assert!((h0 - 1.0 / MS_PER_YEAR).abs() < 1e-20);
        assert!((h1 - 4.0 / MS_PER_YEAR).abs() < 1e-20);
        assert!((vp.ann_vol(0) - 1.0).abs() < 1e-10);
        assert!((vp.ann_vol(1) - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_ewma_static_degenerate() {
        // halflife=0 → no adaptation, h_per_ms stays at seed
        let mut vp = VolProvider::new_ewma(&[(1.0, 0.0, 0.5)]);
        let h_before = vp.h_per_ms(0);
        vp.update(0, 100.0, 1_000_000_000);
        vp.update(0, 101.0, 1_100_000_000);
        assert!((vp.h_per_ms(0) - h_before).abs() < 1e-30);
    }

    #[test]
    fn test_ewma_skips_zero_price() {
        let mut vp = VolProvider::new_ewma(&[(1.0, 60_000.0, 0.5)]);
        let h_before = vp.h_per_ms(0);
        vp.update(0, 0.0, 1_000_000_000);
        vp.update(0, 0.0, 1_100_000_000);
        assert_eq!(vp.h_per_ms(0), h_before);
    }

    #[test]
    fn test_ewma_adapts() {
        let mut vp = VolProvider::new_ewma(&[(1.0, 60_000.0, 0.5)]);
        let h_init = vp.h_per_ms(0);

        // Two snapshots with a large price move
        let t0 = 1_000_000_000_i64;
        let t1 = t0 + 100_000_000; // +100ms
        vp.update(0, 100.0, t0);
        vp.update(0, 101.0, t1); // 1% move in 100ms

        let h_after = vp.h_per_ms(0);
        assert!(h_after > h_init, "h_per_ms should increase: {} vs {}", h_after, h_init);
    }

    #[test]
    fn test_ewma_floor() {
        let mut vp = VolProvider::new_ewma(&[(1.0, 60_000.0, 0.8)]);
        let h_floor = 0.8 * 0.8 / MS_PER_YEAR;

        // Simulate snapshots with zero return → h_per_ms decays toward floor
        let mut t = 1_000_000_000_i64;
        vp.update(0, 100.0, t);
        for _ in 0..100 {
            t += 1_000_000_000; // 1s
            vp.update(0, 100.0, t); // no move
        }

        assert!(
            vp.h_per_ms(0) >= h_floor * 0.999,
            "h_per_ms should be floored: {} vs {}",
            vp.h_per_ms(0),
            h_floor,
        );
    }

    #[test]
    fn test_gk_ewma_survives_ring_wrap() {
        // Regression: after max_bars target bars, the EWMA must keep updating.
        // target_min=1, max_bars=4 (small to hit wrap quickly).
        let max_bars = 4;
        let target_min = 1;
        let halflife_ms = 120_000.0; // 2 min
        let mut vp = VolProvider::new_gk_ewma(&[(0.5, halflife_ms, 0.1)], target_min);

        // Override bar_builder to use small max_bars
        if let VolProvider::GkEwma { states } = &mut vp {
            states[0].bar_builder = crate::bar_manager::BarBuilder::new(target_min, max_bars);
        }

        let mut ts_ns: i64 = 0;
        let one_min_ns: i64 = 60_000_000_000;

        // Generate 10 bars (well past max_bars=4) with oscillating prices
        // to ensure non-zero GK on every bar.
        for bar_i in 0..10u64 {
            // Feed ticks within the minute: open, high, low, close
            let base = 100.0 + (bar_i as f64) * 0.1;
            let prices = [base, base + 0.5, base - 0.3, base + 0.2];
            for &p in prices.iter() {
                ts_ns += one_min_ns / 4;
                vp.update(0, p, ts_ns);
            }
        }

        let h_after_10 = vp.h_per_ms(0);

        // Now feed 5 more bars with much larger moves → vol should increase
        for bar_i in 10..15u64 {
            let base = 100.0 + (bar_i as f64) * 0.1;
            let prices = [base, base + 3.0, base - 2.0, base + 1.0];
            for &p in prices.iter() {
                ts_ns += one_min_ns / 4;
                vp.update(0, p, ts_ns);
            }
        }

        let h_after_15 = vp.h_per_ms(0);
        assert!(
            h_after_15 > h_after_10 * 1.5,
            "EWMA must keep adapting past ring wrap: h@10={:.2e}, h@15={:.2e}",
            h_after_10,
            h_after_15,
        );
    }
}
