use crate::bar_manager::BarManager;
use crate::historical_bars::Bar;
use crate::market_data::AllMarketData;
use crate::vol_params::{SeasonalFactors, VolParams};
use chrono::{Datelike, Timelike, Utc};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

const LN2: f64 = std::f64::consts::LN_2;
const MINUTES_PER_YEAR: f64 = 525_960.0;
/// EWMA half-life for live vol tracking, in minutes.
const EWMA_HALFLIFE_MIN: usize = 60;
/// Realized vol lookback in minutes.
const RV_LOOKBACK_MIN: usize = 60;

// ── Public types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct VolPredictions {
    pub har_ols: f64,
    pub har_qlike: f64,
    pub garch: f64,
    pub ewma: f64,
    pub realized: f64,
    pub n_completed_bars: usize,
    pub partial_bar_age_secs: f64,
    pub ewma_halflife_min: usize,
    pub rv_lookback_min: usize,
}

#[derive(Debug, Clone)]
pub enum VolStatus {
    UnknownSymbol,
    NoTicks { exchange: String },
    InsufficientBars { have: usize, need: usize },
    DegenerateData,
}

impl std::fmt::Display for VolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolStatus::UnknownSymbol => write!(f, "symbol not configured"),
            VolStatus::NoTicks { exchange } => write!(f, "no ticks from {} (feed down?)", exchange),
            VolStatus::InsufficientBars { have, need } => {
                write!(f, "need {} bars, have {} (loading history?)", need, have)
            }
            VolStatus::DegenerateData => write!(f, "degenerate data (zero variance)"),
        }
    }
}

// ── Internal state ────────────────────────────────────────────────────

struct SymbolVolState {
    params: VolParams,
    garch_h: f64,
    ewma_log_gk: f64,
    /// Number of completed target_min bars already processed for GARCH/EWMA.
    bars_processed: usize,
}

struct VolEngineInner {
    symbols: HashMap<String, SymbolVolState>,
}

// ── Public engine handle ──────────────────────────────────────────────

pub struct VolEngine {
    inner: Arc<RwLock<VolEngineInner>>,
    bars: Arc<BarManager>,
}

impl VolEngine {
    pub fn new(
        all_params: HashMap<String, VolParams>,
        bars: Arc<BarManager>,
    ) -> Self {
        let mut symbols = HashMap::new();
        for (sym, params) in all_params {
            let garch_h = params.garch.as_ref().map(|g| g.last_h).unwrap_or(0.0);
            let ewma_log_gk = params.ewma.as_ref().map(|e| e.last_value).unwrap_or(0.0);
            symbols.insert(
                sym,
                SymbolVolState { params, garch_h, ewma_log_gk, bars_processed: 0 },
            );
        }

        Self {
            inner: Arc::new(RwLock::new(VolEngineInner { symbols })),
            bars,
        }
    }

    /// Replay GARCH/EWMA through all historical bars loaded in the bar manager.
    /// Call after bar_manager.warmup().
    pub fn replay_history(&self) {
        let mut inner = self.inner.write().unwrap();
        for (sym, state) in inner.symbols.iter_mut() {
            // Reset to calibration starting point
            if let Some(ref gp) = state.params.garch {
                state.garch_h = gp.last_h;
            }
            if let Some(ref ep) = state.params.ewma {
                state.ewma_log_gk = ep.last_value;
            }

            if let Some(view) = self.bars.get_bars(sym) {
                let completed = view.completed();
                for bar in completed.iter() {
                    update_garch_ewma_bar(state, bar);
                }
                state.bars_processed = completed.len();
                log::info!(
                    "vol replay {}: {} bars, garch_h={:.2}, ewma={:.4}",
                    sym, state.bars_processed, state.garch_h, state.ewma_log_gk,
                );
            }
        }
    }

    pub fn symbols(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner.symbols.keys().cloned().collect()
    }

    pub fn predict_now(
        &self,
        symbol: &str,
        tick_data: &AllMarketData,
    ) -> Result<VolPredictions, VolStatus> {
        let view = self.bars.get_bars(symbol).ok_or(VolStatus::UnknownSymbol)?;

        if !view.has_ticks() {
            return Err(VolStatus::NoTicks {
                exchange: view.exchange().as_str().to_string(),
            });
        }

        let n_completed = view.n_completed();
        let target_min = view.target_min();

        // Lazy GARCH/EWMA catch-up first (needs write lock, released before prediction)
        {
            let mut inner = self.inner.write().map_err(|_| VolStatus::UnknownSymbol)?;
            if let Some(state) = inner.symbols.get_mut(symbol) {
                if n_completed > state.bars_processed {
                    let completed = view.completed();
                    for i in state.bars_processed..n_completed {
                        update_garch_ewma_bar(state, &completed[i]);
                    }
                    state.bars_processed = n_completed;
                }
            }
        }

        // Now read lock for prediction
        let inner = self.inner.read().map_err(|_| VolStatus::UnknownSymbol)?;
        let state = inner.symbols.get(symbol).ok_or(VolStatus::UnknownSymbol)?;
        let params = &state.params;

        let max_window = [
            params.har_ols.as_ref().map(|h| h.max_window()),
            params.har_qlike.as_ref().map(|h| h.max_window()),
        ]
        .iter()
        .filter_map(|v| *v)
        .max()
        .unwrap_or(288);

        if n_completed + 1 < max_window {
            return Err(VolStatus::InsufficientBars {
                have: n_completed,
                need: max_window,
            });
        }

        // Build virtual head bar
        let latest_mid = {
            let coll = tick_data.get_collection(&view.exchange());
            coll.get_midquote(&view.symbol_id()).unwrap_or(0.0)
        };
        let virtual_head = match view.virtual_head(latest_mid) {
            Some(bar) => bar,
            None => return Err(VolStatus::DegenerateData),
        };

        let now = Utc::now();
        let hour = now.hour() as usize;
        let weekday = now.weekday().num_days_from_monday() as usize;
        let seasonal_now = params.seasonality.factor(hour, weekday);
        let bars_per_year = MINUTES_PER_YEAR / target_min as f64;

        let annualize = |log_gk_deseas: f64| -> f64 {
            let var_per_bar = log_gk_deseas.exp() * seasonal_now;
            (var_per_bar * bars_per_year).sqrt()
        };

        let completed = view.completed();

        // HAR (both variants)
        let har_ols_vol = predict_har(
            params.har_ols.as_ref(), &virtual_head, completed, &params.seasonality,
        ).map(|lg| annualize(lg)).unwrap_or(f64::NAN);

        let har_qlike_vol = predict_har(
            params.har_qlike.as_ref(), &virtual_head, completed, &params.seasonality,
        ).map(|lg| annualize(lg)).unwrap_or(f64::NAN);

        // GARCH — run one virtual step using the virtual head bar
        let garch_vol = if let Some(ref gp) = params.garch {
            let r_deseas = deseasoned_log_return(&virtual_head, &params.seasonality);
            let r_scaled = r_deseas * gp.scale_factor;
            let h_virtual = gp.omega + gp.alpha * r_scaled * r_scaled + gp.beta * state.garch_h;
            let h_unscaled = h_virtual / (gp.scale_factor * gp.scale_factor);
            if h_unscaled > 0.0 {
                annualize(gp.cal_a + gp.cal_b * h_unscaled.ln())
            } else { f64::NAN }
        } else { f64::NAN };

        // EWMA — run one virtual step using the virtual head bar
        let ewma_vol = {
            let gk_d = deseasoned_gk(&virtual_head, &params.seasonality);
            if gk_d > 0.0 && state.ewma_log_gk.is_finite() {
                let hl_bars = EWMA_HALFLIFE_MIN as f64 / target_min as f64;
                let decay = 1.0 - (-LN2 / hl_bars).exp();
                let ewma_virtual = decay * gk_d.ln() + (1.0 - decay) * state.ewma_log_gk;
                annualize(ewma_virtual)
            } else { f64::NAN }
        };

        // Realized vol
        let rv_bars = (RV_LOOKBACK_MIN / target_min).max(1);
        let rv_window = rv_bars.min(n_completed + 1);
        let mut rv_sum = raw_gk(&virtual_head);
        let take = (rv_window - 1).min(n_completed);
        for i in 0..take {
            rv_sum += raw_gk(&completed[n_completed - 1 - i]);
        }
        let realized_vol = if rv_sum > 0.0 {
            ((rv_sum / rv_window as f64) * bars_per_year).sqrt()
        } else { f64::NAN };

        let partial_age = view.partial_age_secs();

        Ok(VolPredictions {
            har_ols: har_ols_vol,
            har_qlike: har_qlike_vol,
            garch: garch_vol,
            ewma: ewma_vol,
            realized: realized_vol,
            n_completed_bars: n_completed,
            partial_bar_age_secs: partial_age,
            ewma_halflife_min: EWMA_HALFLIFE_MIN,
            rv_lookback_min: RV_LOOKBACK_MIN,
        })
    }
}

// ── GARCH/EWMA update ────────────────────────────────────────────────

fn update_garch_ewma_bar(state: &mut SymbolVolState, bar: &Bar) {
    let seasonality = &state.params.seasonality;

    if let Some(ref gp) = state.params.garch {
        let r_deseas = deseasoned_log_return(bar, seasonality);
        let r_scaled = r_deseas * gp.scale_factor;
        state.garch_h = gp.omega + gp.alpha * r_scaled * r_scaled + gp.beta * state.garch_h;
    }

    // EWMA with live half-life (not the calibration span which is too slow)
    {
        let gk_d = deseasoned_gk(bar, seasonality);
        if gk_d > 0.0 {
            let hl_bars = EWMA_HALFLIFE_MIN as f64 / state.params.target_min as f64;
            let decay = 1.0 - (-LN2 / hl_bars).exp();
            state.ewma_log_gk = decay * gk_d.ln() + (1.0 - decay) * state.ewma_log_gk;
        }
    }
}

// ── HAR prediction helper ─────────────────────────────────────────────

fn predict_har(
    har: Option<&crate::vol_params::HarParams>,
    virtual_head: &Bar,
    completed: &VecDeque<Bar>,
    seasonality: &SeasonalFactors,
) -> Option<f64> {
    let har = har?;
    let n = completed.len();

    let mut log_rvs = Vec::with_capacity(har.windows.len());
    for &w in &har.windows {
        let mut rv_sum = deseasoned_gk(virtual_head, seasonality);
        let take = (w - 1).min(n);
        for i in 0..take {
            rv_sum += deseasoned_gk(&completed[n - 1 - i], seasonality);
        }
        if rv_sum <= 0.0 {
            return None;
        }
        log_rvs.push(rv_sum.ln());
    }

    Some(har.predict(&log_rvs))
}

// ── Garman-Klass helpers ──────────────────────────────────────────────

#[inline]
pub fn raw_gk(bar: &Bar) -> f64 {
    if bar.high <= 0.0 || bar.low <= 0.0 || bar.open <= 0.0 || bar.close <= 0.0 {
        return 0.0;
    }
    let hl = (bar.high / bar.low).ln();
    let co = (bar.close / bar.open).ln();
    (0.5 * hl * hl - (2.0 * LN2 - 1.0) * co * co).max(0.0)
}

#[inline]
pub fn deseasoned_gk(bar: &Bar, seasonality: &SeasonalFactors) -> f64 {
    let gk = raw_gk(bar);
    if gk == 0.0 {
        return 0.0;
    }
    let dt = chrono::DateTime::from_timestamp_millis(bar.open_time_ms);
    match dt {
        Some(dt) => {
            let factor = seasonality.factor(
                dt.hour() as usize,
                dt.weekday().num_days_from_monday() as usize,
            );
            if factor > 0.0 { gk / factor } else { gk }
        }
        None => gk,
    }
}

#[inline]
pub fn deseasoned_log_return(bar: &Bar, seasonality: &SeasonalFactors) -> f64 {
    if bar.open <= 0.0 || bar.close <= 0.0 {
        return 0.0;
    }
    let r = (bar.close / bar.open).ln();
    let dt = chrono::DateTime::from_timestamp_millis(bar.open_time_ms);
    match dt {
        Some(dt) => {
            let factor = seasonality.factor(
                dt.hour() as usize,
                dt.weekday().num_days_from_monday() as usize,
            );
            if factor > 0.0 { r / factor.sqrt() } else { r }
        }
        None => r,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vol_params::{CalibratedGarchParams, HarParams};

    fn make_bar(open_time_ms: i64, o: f64, h: f64, l: f64, c: f64) -> Bar {
        Bar { open_time_ms, open: o, high: h, low: l, close: c, volume: 1.0 }
    }

    fn flat_seasonality() -> SeasonalFactors {
        SeasonalFactors { f_h: [1.0; 24], f_d: [1.0; 7] }
    }

    #[test]
    fn test_garman_klass_flat_bar() {
        assert_eq!(deseasoned_gk(&make_bar(0, 100.0, 100.0, 100.0, 100.0), &flat_seasonality()), 0.0);
    }

    #[test]
    fn test_garman_klass_known() {
        let bar = make_bar(0, 100.0, 101.0, 99.0, 100.5);
        let gk = deseasoned_gk(&bar, &flat_seasonality());
        let hl = (101.0_f64 / 99.0).ln();
        let co = (100.5_f64 / 100.0).ln();
        let expected = (0.5 * hl * hl - (2.0 * LN2 - 1.0) * co * co).max(0.0);
        assert!((gk - expected).abs() < 1e-12);
    }

    #[test]
    fn test_garman_klass_deseason() {
        let bar = make_bar(0, 100.0, 101.0, 99.0, 100.5);
        let gk_base = deseasoned_gk(&bar, &flat_seasonality());
        let mut s = flat_seasonality();
        s.f_h[0] = 2.0;
        s.f_d[3] = 1.5;
        assert!((deseasoned_gk(&bar, &s) - gk_base / 3.0).abs() < 1e-12);
    }

    #[test]
    fn test_garch_update() {
        let gp = CalibratedGarchParams {
            omega: 2.0, alpha: 0.1, beta: 0.9,
            scale_factor: 10000.0, last_h: 50.0,
            cal_a: 0.0, cal_b: 1.0,
        };
        let bar = make_bar(0, 100.0, 101.0, 99.0, 100.5);
        let r_deseas = deseasoned_log_return(&bar, &flat_seasonality());
        let r_scaled = r_deseas * gp.scale_factor;
        let h_new = gp.omega + gp.alpha * r_scaled * r_scaled + gp.beta * 50.0;
        assert!(h_new > 0.0);
        assert!(h_new != 50.0);
    }

    #[test]
    fn test_har_predict_integration() {
        let har = HarParams {
            intercept: -10.0,
            betas: vec![0.2, 0.3, 0.3, 0.2],
            windows: vec![1, 6, 24, 288],
        };
        let log_rvs = [-5.0, -4.0, -3.5, -3.0];
        let expected = -10.0 + 0.2 * (-5.0) + 0.3 * (-4.0) + 0.3 * (-3.5) + 0.2 * (-3.0);
        assert!((har.predict(&log_rvs) - expected).abs() < 1e-12);
    }
}
