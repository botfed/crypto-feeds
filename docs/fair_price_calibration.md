# Fair Price Engine Calibration

Offline calibration of structural GARCH parameters and per-exchange noise priors
from real market data. Runs from a single `lead_lag_capture` session -- one process,
one set of exchange connections.

## Quick Start

```bash
# 1. Build
cargo build --release

# 2. Capture raw cross-exchange mids (one terminal, 4+ hours, Ctrl-C to stop)
cargo run --release --bin lead_lag_capture -- \
  configs/config.yaml data/fp_calib/lead_lag.csv.gz

# 3. Calibrate
python scripts/fp_calibrate.py --data-dir data/fp_calib --warmup-secs 300

# 4. Start the engine (loads calibrated params automatically)
cargo run --release --bin fair_price_demo

# 5. Validate (optional): capture diagnostics and run tests
cargo run --release --bin fp_diag_capture -- \
  --warmup-secs 300 --output-dir data/fp_validate
# (run 2+ hours, Ctrl-C)
python scripts/fp_validate.py --data-dir data/fp_validate --warmup-secs 300
```

## What Gets Calibrated

| Parameter | Default | Estimated from | Frequency |
|-----------|---------|----------------|-----------|
| `sigma_k^2` (noise) | 4e-8 | Cross-exchange mid disagreement | 100ms |
| `m_k` (bias) | 0.0 | Mean cross-exchange deviation | 100ms |
| `alpha` (GARCH shock) | 0.06 | Noise-corrected QMLE on median returns | 100ms |
| `beta` (GARCH memory) | 0.94 | Same QMLE | 100ms |
| `initial_var` | 1e-6 | Signal variance of median returns | Full sample |
| `process_noise_floor` | 1e-8 | `initial_var * 0.01` | Derived |
| `vol_halflife` | 5000 | ACF of squared returns | Full sample |

**Not calibrated** (meta-parameters, keep defaults): `prior_weight`, `ruler_halflife`, `recalibrate_every`.

## Data Requirements

| Parameter | Minimum | Recommended |
|-----------|---------|-------------|
| Per-exchange rulers | 30 min | 1 hour |
| GARCH alpha, beta | 2 hours | 4 hours |
| initial_var | 1 hour | 4 hours |
| vol_halflife | 4 hours | 8+ hours |

Target: 4 hours of active trading (e.g. 14:00-18:00 UTC).

## How It Works

Everything is estimated from the cross-sectional structure of raw exchange mids
in `lead_lag.csv.gz`. No Kalman filter or GARCH engine is needed for calibration,
avoiding circularity (estimating filter params from filter output).

### Fair price estimate

At each 100ms sample, the cross-sectional median of `log((bid+ask)/2)` across
exchanges gives a robust fair price estimate, independent of any model parameters.

### Rulers (m_k, sigma_k^2)

1. `y_hat = median(log_mid_k)` across exchanges
2. `residual_k = log_mid_k - y_hat`
3. `m_k = mean(residual_k)` -- systematic offset
4. `sigma_k^2 = var(residual_k) - spread_k^2/12` -- spread-corrected noise variance

The `spread^2/12` correction removes bid-ask bounce (uniform distribution of true
price within spread).

### GARCH (alpha, beta)

Noise-corrected quasi-MLE on returns of the cross-sectional median:

1. Returns: `r_t = y_hat_t - y_hat_{t-1}`, demeaned
2. Noise correction via lag-1 autocovariance:
   - The median has estimator noise: `median_t = true_price_t + noise_t`
   - Returns: `r_t = signal_t + (noise_t - noise_{t-1})`
   - `cov(r_t, r_{t+1}) = -var(noise)` for iid noise
   - `R_noise = -2 * cov(r_t, r_{t+1})`
3. Signal variance: `var_signal = var(r) - R_noise`
4. GARCH QMLE with variance targeting:
   - `omega = var_signal * (1 - alpha - beta)`
   - Grid search over (alpha, beta) pairs, refine with Nelder-Mead
   - Noise-corrected shock: `r2_est = max(r^2 - R_noise, 0)` (same as engine's `r2_corrected`)

### Derived Parameters

- `initial_var = var_signal`
- `process_noise_floor = initial_var * 0.01`
- `vol_halflife`: lag where ACF of noise-corrected squared returns drops to 0.5
  (default 5000 if insufficient data)

## Output Files

### `configs/fp_calibrated.json` (structural params)

```json
{
  "calibrated_at": "2026-03-13T18:00:00Z",
  "capture_hours": 4.2,
  "groups": {
    "BTC": {
      "alpha": 0.048,
      "beta": 0.91,
      "initial_var": 8.3e-07,
      "process_noise_floor": 8.3e-09,
      "vol_halflife": 5000
    }
  }
}
```

Loaded by `load_calibration()` on startup, before `apply_cache()`.

### `/tmp/fair_price_cache.json` (per-exchange rulers, optional)

If the engine was run previously and a cache exists, the calibration script
updates it with cross-sectional estimates. Otherwise, rulers converge via
online recalibration within ~5 minutes of engine startup.

## Verification Checklist

After calibrating, run the engine with new params and validate:

1. Innovation whiteness tests should pass more cleanly
2. P_unc coverage at 1-sigma should be closer to 68%
3. GARCH Mincer-Zarnowitz slope should be closer to 1.0
4. Cross-sectional sigma_k^2 should agree with innovation-based sigma_k^2 within 2x

## Iterating

If validation shows issues:

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| z variance > 1 | noise_var too low | Re-run calibration with more data |
| z variance < 1 | noise_var too high | Check spread correction, reduce priors |
| Positive lag-1 autocorr | alpha too low | Increase alpha or initial_var |
| Negative lag-1 autocorr | alpha too high | Decrease alpha |
| P_unc undercoverage | GARCH floor too low | Increase process_noise_floor |
| GARCH slope << 1 | alpha+beta too high | Check data quality |

## Script Reference

```
python scripts/fp_calibrate.py \
  --data-dir data/fp_calib \
  --warmup-secs 300 \
  --config configs/config.yaml \
  [--no-plots] \
  [--no-cache]
```

Options:
- `--data-dir`: Directory containing `lead_lag.csv.gz`
- `--warmup-secs`: Warmup period to strip (default 300s)
- `--config`: Path to config.yaml for group discovery
- `--no-plots`: Skip diagnostic plot generation
- `--no-cache`: Don't update `/tmp/fair_price_cache.json`
