use crate::bar_manager::{BarBuilder, BarManager};
use crate::historical_bars::Bar;
use crate::vol_engine::{deseasoned_gk, raw_gk};
use crate::vol_params::{HarParams, SeasonalFactors};
use chrono::{Datelike, Timelike, Utc};
use std::collections::VecDeque;
use std::sync::Arc;

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
    /// HAR (Heterogeneous Autoregressive) model — stateless regression over bar history.
    /// Lock-free: reads completed bars from BarManager (lock-free ring buffers).
    Har {
        states: Vec<HarVolState>,
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
    /// ln(h_floor * bar_ms) — floor for ewma_log_gk to prevent state from sinking
    /// into an unrecoverable hole during quiet periods.
    log_gk_floor: f64,
    bar_builder: BarBuilder,
    bars_processed: usize,
    target_min: usize,
    ewma_log_gk: f64,
    ewma_log_gk_sq: f64,
    /// Rolling buffer of (price, ts_ms) for virtual head — covers target_min minutes.
    snap_buf: VecDeque<(f64, i64)>,
}

pub struct HarVolState {
    har_params: HarParams,
    seasonality: SeasonalFactors,
    target_min: usize,
    symbol: String,
    bars: Arc<BarManager>,
    cached_ann_vol: f64,
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
    pub fn new_gk_ewma(groups: &[(f64, f64, f64)], target_min: usize, max_bars: usize) -> Self {
        let bars_per_year = MINUTES_PER_YEAR / target_min as f64;
        let states = groups
            .iter()
            .map(|&(ann_vol, halflife_ms, floor_ann)| {
                let var_per_bar = ann_vol * ann_vol / bars_per_year;
                let log_gk_seed = if var_per_bar > 0.0 { var_per_bar.ln() } else { f64::NAN };
                let h_floor = floor_ann * floor_ann / MS_PER_YEAR;
                let bar_ms = target_min as f64 * 60_000.0;
                let log_gk_floor = (h_floor * bar_ms).ln();
                GkEwmaVolState {
                    h_per_ms: ann_vol * ann_vol / MS_PER_YEAR,
                    halflife_ms,
                    h_floor,
                    log_gk_floor,
                    bar_builder: BarBuilder::new(target_min, max_bars),
                    bars_processed: 0,
                    target_min,
                    ewma_log_gk: log_gk_seed,
                    ewma_log_gk_sq: if log_gk_seed.is_finite() { log_gk_seed * log_gk_seed } else { f64::NAN },
                    snap_buf: VecDeque::new(),
                    }
            })
            .collect();
        VolProvider::GkEwma { states }
    }

    /// Build a HAR provider. One state per (symbol, params) pair.
    pub fn new_har(
        groups: Vec<(String, HarParams, SeasonalFactors, usize)>, // (symbol, params, seasonality, target_min)
        bars: Arc<BarManager>,
        seed_ann_vol: f64,
    ) -> Self {
        let states = groups
            .into_iter()
            .map(|(symbol, har_params, seasonality, target_min)| HarVolState {
                har_params,
                seasonality,
                target_min,
                symbol,
                bars: Arc::clone(&bars),
                cached_ann_vol: seed_ann_vol,
            })
            .collect();
        VolProvider::Har { states }
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
            VolProvider::Har { states } => {
                let av = states[group_idx].cached_ann_vol;
                av * av / MS_PER_YEAR
            }
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
            VolProvider::Har { states } => states[group_idx].cached_ann_vol,
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
                            // Floor the EWMA state to prevent sinking into an unrecoverable hole
                            if s.ewma_log_gk < s.log_gk_floor {
                                s.ewma_log_gk = s.log_gk_floor;
                                s.ewma_log_gk_sq = s.log_gk_floor * s.log_gk_floor;
                            }
                            // Bias-corrected: E[GK] = exp(μ + σ²/2)
                            let var_log = (s.ewma_log_gk_sq - s.ewma_log_gk * s.ewma_log_gk).max(0.0);
                            s.h_per_ms = (s.ewma_log_gk + var_log / 2.0).exp() / bar_ms;
                            s.h_per_ms = s.h_per_ms.max(s.h_floor);
                        }
                    }
                    s.bars_processed = total;
                }

                // Virtual step: rolling snap buffer covers target_min minutes.
                let window_ms = s.target_min as i64 * 60_000;
                s.snap_buf.push_back((fair_price, ts_ms));
                while let Some(&(_, t)) = s.snap_buf.front() {
                    if ts_ms - t > window_ms { s.snap_buf.pop_front(); } else { break; }
                }
                if s.snap_buf.len() >= 2 {
                    let (open, _) = s.snap_buf[0];
                    let mut high = open;
                    let mut low = open;
                    for &(p, _) in &s.snap_buf {
                        if p > high { high = p; }
                        if p < low { low = p; }
                    }
                    let close = fair_price;
                    let vbar = Bar { open_time_ms: 0, open, high, low, close, volume: 0.0 };
                    let gk = raw_gk(&vbar);
                    if gk > 0.0 {
                        let bar_ms = window_ms as f64;
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
            VolProvider::Har { states } => {
                let s = &mut states[group_idx];
                let Some(view) = s.bars.get_bars(&s.symbol) else { return };
                let n = view.n_completed();
                let max_w = s.har_params.max_window();
                if n + 1 < max_w { return; }

                let Some(virtual_head) = view.virtual_head(fair_price) else { return };
                let completed = view.completed();

                // HAR prediction in log-deseasoned space
                let mut log_rvs = Vec::with_capacity(s.har_params.windows.len());
                for &w in &s.har_params.windows {
                    let mut rv_sum = deseasoned_gk(&virtual_head, &s.seasonality);
                    let take = (w - 1).min(n);
                    for i in 0..take {
                        rv_sum += deseasoned_gk(&completed[n - 1 - i], &s.seasonality);
                    }
                    if rv_sum <= 0.0 { return; }
                    log_rvs.push(rv_sum.ln());
                }

                let log_pred = s.har_params.predict(&log_rvs);

                // Annualize
                let now = Utc::now();
                let seasonal_now = s.seasonality.factor(
                    now.hour() as usize,
                    now.weekday().num_days_from_monday() as usize,
                );
                let bars_per_year = MINUTES_PER_YEAR / s.target_min as f64;
                let var_per_bar = log_pred.exp() * seasonal_now;
                let ann_vol = (var_per_bar * bars_per_year).sqrt();
                if ann_vol.is_finite() && ann_vol > 0.0 {
                    s.cached_ann_vol = ann_vol;
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
            VolProvider::Har { states } => {
                states[group_idx].cached_ann_vol = ann_vol;
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
    fn test_har_matches_vol_engine() {
        use crate::bar_manager::{BarManager, BarSymbol};
        use crate::historical_bars::Bar;
        use crate::market_data::{AllMarketData, Exchange};
        use crate::vol_engine::VolEngine;
        use crate::vol_params::{HarParams, SeasonalFactors, VolParams};

        let target_min = 5;
        let max_bars = 300;
        let symbol_id: usize = 0;

        // Create test HAR params (simple: 2 windows)
        let har = HarParams {
            intercept: -2.0,
            betas: vec![0.4, 0.6],
            windows: vec![1, 6],
        };
        let seasonality = SeasonalFactors {
            f_h: [1.0; 24],
            f_d: [1.0; 7],
        };

        // Create BarManager
        let bar_mgr = Arc::new(BarManager::new(
            vec![BarSymbol {
                name: "HARTEST".to_string(),
                exchange: Exchange::Binance,
                symbol_id,
            }],
            target_min,
            max_bars,
        ));

        // Generate synthetic bars (300 bars of 5-min OHLC)
        let base_ts = 1_700_000_000_000i64; // some epoch ms
        let bar_ms = target_min as i64 * 60_000;
        let mut bars = Vec::new();
        for i in 0..max_bars {
            let t = base_ts + i as i64 * bar_ms;
            let base_price = 100.0 + (i as f64 * 0.01).sin() * 2.0;
            bars.push(Bar {
                open_time_ms: t,
                open: base_price,
                high: base_price + 0.5,
                low: base_price - 0.3,
                close: base_price + 0.1,
                volume: 1000.0,
            });
        }

        // Warmup bar manager
        bar_mgr.warmup("HARTEST", bars.clone(), &[]);

        // Set up AllMarketData with a mid price for the virtual head
        let market_data = AllMarketData::new();
        let latest_mid = 100.5;
        market_data.binance.push(
            &symbol_id,
            crate::market_data::MarketData {
                bid: Some(100.0),
                ask: Some(101.0),
                bid_qty: Some(1.0),
                ask_qty: Some(1.0),
                exchange_ts_raw: None,
                exchange_ts: None,
                received_ts: None,
            },
        );

        // --- VolEngine path ---
        let vol_params = VolParams {
            symbol: "HARTEST".to_string(),
            target_min,
            seasonality: seasonality.clone(),
            har_ols: None,
            har_qlike: Some(har.clone()),
            garch: None,
            ewma: None,
        };
        let mut param_map = std::collections::HashMap::new();
        param_map.insert("HARTEST".to_string(), vol_params);
        let vol_engine = VolEngine::new(param_map, Arc::clone(&bar_mgr));
        vol_engine.replay_history();
        let preds = vol_engine.predict_now("HARTEST", &market_data).unwrap();
        let ve_vol = preds.har_qlike;

        // --- VolProvider::Har path ---
        let mut vp = VolProvider::new_har(
            vec![("HARTEST".to_string(), har, seasonality, target_min)],
            Arc::clone(&bar_mgr),
            1.0, // seed (will be overwritten by update)
        );
        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        vp.update(0, latest_mid, ts_ns);
        let vp_vol = vp.ann_vol(0);

        // Compare — should be identical (same math, same data)
        let diff_pct = ((ve_vol - vp_vol) / ve_vol).abs() * 100.0;
        assert!(
            diff_pct < 0.01,
            "VolEngine ({:.6}) vs VolProvider ({:.6}) differ by {:.4}%",
            ve_vol, vp_vol, diff_pct,
        );
    }

    #[test]
    fn test_gk_ewma_survives_ring_wrap() {
        // Regression: after max_bars target bars, the EWMA must keep updating.
        // target_min=1, max_bars=4 (small to hit wrap quickly).
        let max_bars = 4;
        let target_min = 1;
        let halflife_ms = 120_000.0; // 2 min
        let mut vp = VolProvider::new_gk_ewma(&[(0.5, halflife_ms, 0.1)], target_min, max_bars);

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
