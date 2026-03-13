# Fair Price Engine — Statistical Validation

Verify that the Kalman filter + GARCH(1,1) + per-exchange ruler calibration
produces correct uncertainty estimates, unbiased parameters, and predictive edges.

## Prerequisites

- Rust binary: `fp_diag_capture` (built from `src/bin/fp_diag_capture.rs`)
- Python 3.10+ with: `pandas`, `numpy`, `scipy`, `matplotlib`

```bash
pip install pandas numpy scipy matplotlib
```

## Step 1: Build the capture binary

```bash
cargo build --release --bin fp_diag_capture
```

## Step 2: Capture live diagnostics

```bash
# Fresh start (no cached params, 15-min warmup for recalib convergence):
./target/release/fp_diag_capture --output-dir data/fp_diag --clear-cache --warmup-secs 900

# With cached params from a previous run (shorter warmup):
./target/release/fp_diag_capture --output-dir data/fp_diag --warmup-secs 300
```

Let it run for **4+ hours** for full validation (10 min minimum for a smoke test).
Press Ctrl-C to stop — diagnostics are finalized and params are cached on shutdown.

**CLI flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir DIR` | `data/fp_diag` | Where to write csv.gz files |
| `--clear-cache` | off | Delete `/tmp/fair_price_cache.json` and start from priors |
| `--warmup-secs N` | 900 | Seconds of warmup stripped by the analysis script |

**Output files:**

| File | Content | ~Size (4h) |
|------|---------|------------|
| `tick_innovations.csv.gz` | One row per Kalman update (standardized innovations, gains) | ~50-80 MB |
| `group_snaps.csv.gz` | One row per group per 100ms snap (GARCH state, volatility) | ~5-10 MB |
| `recalib_events.csv.gz` | One row per member per recalib (bias/noise before & after) | ~1 MB |

## Step 3: Run the validation

```bash
python scripts/fp_validate.py --data-dir data/fp_diag --warmup-secs 900
```

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir DIR` | `data/fp_diag` | Directory containing csv.gz files |
| `--warmup-secs N` | 900 | Must match the capture's warmup |
| `--group N` | all | Only analyze group index N |
| `--no-plots` | off | Skip plot generation |

**Output:** pass/fail table per group, diagnostic plots in `data/fp_diag/plots/`.

## The 8 Tests

| # | Test | Key Statistic | Pass Criteria |
|---|------|--------------|---------------|
| 1a | Innovation mean = 0 | z-test on standardized innovations | \|t\| < 2.58 |
| 1b | Innovation variance = 1 | Sample variance of z_t | s^2 in [0.9, 1.1] |
| 1c | No autocorrelation | Ljung-Box Q(20) | Q < 37.6 |
| 1d | Per-exchange variance = 1 | Variance stratified by member | Each in [0.9, 1.1] |
| 2 | P_unc coverage | Forward return / predicted sigma | 1-sigma ~ 68%, 2-sigma ~ 95% |
| 3 | GARCH vol calibration | Mincer-Zarnowitz regression | slope in [0.7, 1.3] |
| 4 | Bias accuracy | \|mean(innovation)\| per member | < 0.5 bps |
| 5 | Noise calibration | var(innovation) / E[s] per member | ratio in [0.8, 1.2] |
| 6 | Edge predictiveness | Regress future delta_mid on edge | beta > 0, hit rate > 50% |
| 7 | Normality | Kurtosis of innovations | < 8 (informational) |
| 8 | Regime tracking | GARCH h vs realized vol MAE | < 0.5 over 10-min windows |

## Interpreting Failures

| Failure | Meaning | Fix |
|---------|---------|-----|
| z mean != 0 | Bias (m_k) is wrong | Adjust `prior_weight`, `ruler_halflife` |
| z var > 1 | Uncertainty underestimated | Increase `noise_var` prior |
| z var < 1 | Uncertainty overestimated | Decrease `noise_var` prior |
| Positive lag-1 autocorrelation | Filter too slow | Increase `alpha` or `initial_var` |
| Negative lag-1 autocorrelation | Filter overshooting | Decrease `alpha` |
| P_unc undercoverage | GARCH vol too small | Check `process_noise_floor`, `alpha + beta` |
| Edge beta < 0 | Fair price worse than mid | Fundamental model problem |
| Kurtosis > 10 | Outlier ticks not handled | Add tick rejection |

After adjusting parameters, re-run from Step 2 and iterate until all tests pass.
