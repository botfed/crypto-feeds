use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Raw YAML structure matching botfed-params/vol/{symbol}.yaml
#[derive(Debug, Deserialize)]
struct RawVolFile {
    symbol: String,
    target_min: usize,
    #[serde(default)]
    lookback_weeks: Option<u32>,
    seasonality: RawSeasonality,
    models: RawModels,
}

#[derive(Debug, Deserialize)]
struct RawSeasonality {
    f_h: HashMap<usize, f64>,
    f_d: HashMap<usize, f64>,
}

#[derive(Debug, Deserialize)]
struct RawModels {
    #[serde(default)]
    har_ols: Option<RawHarModel>,
    #[serde(default)]
    har_qlike: Option<RawHarModel>,
    #[serde(default)]
    garch: Option<RawGarchModel>,
    #[serde(default)]
    ewma: Option<RawEwmaModel>,
}

#[derive(Debug, Deserialize)]
struct RawHarModel {
    #[allow(dead_code)]
    loss: String,
    feature_names: Vec<String>,
    params: Vec<f64>,
}

#[derive(Debug, Deserialize)]
struct RawGarchModel {
    pub omega: f64,
    pub alpha: f64,
    pub beta: f64,
    pub scale_factor: f64,
    pub last_h: f64,
    pub cal_a: f64,
    pub cal_b: f64,
}

#[derive(Debug, Deserialize)]
struct RawEwmaModel {
    pub span: usize,
    pub last_value: f64,
}

// ── Public types ──────────────────────────────────────────────────────

/// HAR model: intercept + betas dot log_rv features.
/// `windows[i]` is the number of bars for feature i (e.g. 1, 6, 24, 288).
/// `predict(log_rvs)` = intercept + sum(betas[i] * log_rvs[i]).
#[derive(Debug, Clone)]
pub struct HarParams {
    pub intercept: f64,
    pub betas: Vec<f64>,
    pub windows: Vec<usize>,
}

impl HarParams {
    /// HAR prediction in log-deseasoned space.
    #[inline]
    pub fn predict(&self, log_rvs: &[f64]) -> f64 {
        debug_assert_eq!(log_rvs.len(), self.betas.len());
        let mut v = self.intercept;
        for (b, x) in self.betas.iter().zip(log_rvs) {
            v += b * x;
        }
        v
    }

    /// Maximum lookback in bars needed to compute all features.
    pub fn max_window(&self) -> usize {
        self.windows.iter().copied().max().unwrap_or(0)
    }
}

/// Calibrated GARCH parameters (offline fit, not the online GARCH in fair_price).
#[derive(Debug, Clone)]
pub struct CalibratedGarchParams {
    pub omega: f64,
    pub alpha: f64,
    pub beta: f64,
    pub scale_factor: f64,
    pub last_h: f64,
    pub cal_a: f64,
    pub cal_b: f64,
}

/// EWMA baseline model.
#[derive(Debug, Clone)]
pub struct EwmaParams {
    pub span: usize,
    pub last_value: f64,
}

/// Hourly (24) and day-of-week (7) seasonal multipliers.
/// Variance at hour h, day d is scaled by `f_h[h] * f_d[d]`.
#[derive(Debug, Clone)]
pub struct SeasonalFactors {
    pub f_h: [f64; 24],
    pub f_d: [f64; 7],
}

impl SeasonalFactors {
    /// Seasonal multiplier for a given UTC hour and weekday (Mon=0).
    #[inline]
    pub fn factor(&self, hour: usize, weekday: usize) -> f64 {
        self.f_h[hour % 24] * self.f_d[weekday % 7]
    }
}

/// All calibrated parameters for one symbol.
#[derive(Debug, Clone)]
pub struct VolParams {
    pub symbol: String,
    pub target_min: usize,
    pub seasonality: SeasonalFactors,
    pub har_ols: Option<HarParams>,
    pub har_qlike: Option<HarParams>,
    pub garch: Option<CalibratedGarchParams>,
    pub ewma: Option<EwmaParams>,
}

/// Which HAR variant to use for predictions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarVariant {
    HarOls,
    HarQlike,
}

impl VolParams {
    /// Get the HAR params for the requested variant, if present.
    pub fn har(&self, variant: HarVariant) -> Option<&HarParams> {
        match variant {
            HarVariant::HarOls => self.har_ols.as_ref(),
            HarVariant::HarQlike => self.har_qlike.as_ref(),
        }
    }
}

// ── Parsing ───────────────────────────────────────────────────────────

/// Parse HAR window sizes from feature names like "log_rv_6" → 6.
fn parse_har_windows(feature_names: &[String]) -> Result<Vec<usize>> {
    feature_names
        .iter()
        .map(|name| {
            let suffix = name
                .strip_prefix("log_rv_")
                .with_context(|| format!("unexpected HAR feature name: {name}"))?;
            suffix
                .parse::<usize>()
                .with_context(|| format!("cannot parse window from feature: {name}"))
        })
        .collect()
}

fn convert_har(raw: &RawHarModel) -> Result<HarParams> {
    let windows = parse_har_windows(&raw.feature_names)?;
    anyhow::ensure!(
        raw.params.len() == windows.len() + 1,
        "HAR params length ({}) != features ({}) + 1 (intercept)",
        raw.params.len(),
        windows.len()
    );
    Ok(HarParams {
        intercept: raw.params[0],
        betas: raw.params[1..].to_vec(),
        windows,
    })
}

fn convert_seasonality(raw: &RawSeasonality) -> Result<SeasonalFactors> {
    let mut f_h = [1.0f64; 24];
    for (&k, &v) in &raw.f_h {
        anyhow::ensure!(k < 24, "f_h key {k} out of range 0..23");
        f_h[k] = v;
    }
    let mut f_d = [1.0f64; 7];
    for (&k, &v) in &raw.f_d {
        anyhow::ensure!(k < 7, "f_d key {k} out of range 0..6");
        f_d[k] = v;
    }
    Ok(SeasonalFactors { f_h, f_d })
}

/// Load a single symbol's vol params from a YAML file.
pub fn load_vol_params(path: &Path) -> Result<VolParams> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let raw: RawVolFile = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    let seasonality = convert_seasonality(&raw.seasonality)?;

    let har_ols = raw.models.har_ols.as_ref().map(convert_har).transpose()?;
    let har_qlike = raw
        .models
        .har_qlike
        .as_ref()
        .map(convert_har)
        .transpose()?;
    let garch = raw.models.garch.map(|g| CalibratedGarchParams {
        omega: g.omega,
        alpha: g.alpha,
        beta: g.beta,
        scale_factor: g.scale_factor,
        last_h: g.last_h,
        cal_a: g.cal_a,
        cal_b: g.cal_b,
    });
    let ewma = raw.models.ewma.map(|e| EwmaParams {
        span: e.span,
        last_value: e.last_value,
    });

    Ok(VolParams {
        symbol: raw.symbol,
        target_min: raw.target_min,
        seasonality,
        har_ols,
        har_qlike,
        garch,
        ewma,
    })
}

/// Load vol params for all YAML files in a directory.
/// Returns a map keyed by uppercase symbol name (e.g. "BTC", "ETH").
pub fn load_all_vol_params(dir: &Path) -> Result<HashMap<String, VolParams>> {
    let mut map = HashMap::new();
    let entries = fs::read_dir(dir)
        .with_context(|| format!("cannot read vol params dir: {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "yaml" || e == "yml") {
            let params = load_vol_params(&path)?;
            let key = params.symbol.to_uppercase();
            map.insert(key, params);
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn btc_yaml_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("botfed-params")
            .join("vol")
            .join("btc.yaml")
    }

    #[test]
    fn test_load_btc_params() {
        let path = btc_yaml_path();
        if !path.exists() {
            eprintln!("skipping test — {} not found", path.display());
            return;
        }
        let params = load_vol_params(&path).unwrap();
        assert_eq!(params.symbol, "BTC");
        assert_eq!(params.target_min, 5);

        let har = params.har_ols.as_ref().unwrap();
        assert_eq!(har.windows, vec![1, 6, 24, 288]);
        assert_eq!(har.betas.len(), 4);
        assert!((har.intercept - (-2.173194)).abs() < 1e-4);

        // Seasonality
        assert!(params.seasonality.f_h[0] > 1.0); // hour 0 > 1
        assert!(params.seasonality.f_d[5] < 1.0); // Saturday < 1
    }

    #[test]
    fn test_har_predict() {
        let har = HarParams {
            intercept: -2.0,
            betas: vec![0.2, 0.4, 0.3, 0.1],
            windows: vec![1, 6, 24, 288],
        };
        let log_rvs = [1.0, 2.0, 3.0, 4.0];
        let expected = -2.0 + 0.2 * 1.0 + 0.4 * 2.0 + 0.3 * 3.0 + 0.1 * 4.0;
        assert!((har.predict(&log_rvs) - expected).abs() < 1e-12);
    }

    #[test]
    fn test_parse_har_windows() {
        let names = vec![
            "log_rv_1".to_string(),
            "log_rv_6".to_string(),
            "log_rv_24".to_string(),
            "log_rv_288".to_string(),
        ];
        let windows = parse_har_windows(&names).unwrap();
        assert_eq!(windows, vec![1, 6, 24, 288]);
    }

    #[test]
    fn test_seasonal_factor() {
        let s = SeasonalFactors {
            f_h: {
                let mut a = [1.0; 24];
                a[14] = 2.0;
                a
            },
            f_d: {
                let mut a = [1.0; 7];
                a[4] = 1.5;
                a
            },
        };
        assert!((s.factor(14, 4) - 3.0).abs() < 1e-12);
        assert!((s.factor(0, 0) - 1.0).abs() < 1e-12);
    }
}
