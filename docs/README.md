# Fair Price Engine — Documentation

Offline tools for calibrating, validating, and iterating on the fair price engine's
Kalman filter + GARCH(1,1) model.

## Workflow

```
 capture (4h)          calibrate            validate (2h)
┌────────────────┐   ┌──────────────┐     ┌──────────────┐
│lead_lag_capture │──>│fp_calibrate.py│────>│fp_validate.py│
└────────────────┘   └──────────────┘     └──────────────┘
                           │                     │
                           v                     v
                   fp_calibrated.json       pass/fail table
                                            diagnostic plots
```

1. **Capture** raw cross-exchange mids with `lead_lag_capture`. One process,
   one set of exchange connections. Run for 4+ hours.

2. **Calibrate** structural parameters offline with `fp_calibrate.py`. Computes
   the fair price as the cross-sectional median, estimates GARCH alpha/beta from
   median returns with autocovariance noise correction, and derives per-exchange
   noise and bias. Writes `configs/fp_calibrated.json`.

3. **Validate** by starting the engine with calibrated params, capturing 2+ hours
   of diagnostics with `fp_diag_capture`, and running `fp_validate.py` to check
   8 statistical tests.

4. **Iterate** if validation tests fail — the calibration doc has a symptom/fix
   lookup table.

## Documents

| Document | Purpose |
|----------|---------|
| [fair_price_calibration.md](fair_price_calibration.md) | How to run the calibration pipeline: capture procedure, estimation methods, output format, verification checklist |
| [fair_price_validation.md](fair_price_validation.md) | How to run statistical validation: the 8 tests, interpreting failures, parameter tuning |

## Scripts

| Script | Input | Output |
|--------|-------|--------|
| `scripts/fp_calibrate.py` | `lead_lag.csv.gz` | `configs/fp_calibrated.json` |
| `scripts/fp_validate.py` | `group_snaps.csv.gz`, `tick_innovations.csv.gz`, `recalib_events.csv.gz` | Pass/fail table, diagnostic plots |

## Quick Start

```bash
# Build
cargo build --release

# Capture (one terminal, 4+ hours, Ctrl-C to stop)
cargo run --release --bin lead_lag_capture -- \
  configs/config.yaml data/fp_calib/lead_lag.csv.gz

# Calibrate
python scripts/fp_calibrate.py --data-dir data/fp_calib --warmup-secs 300

# Start the engine (loads calibrated params automatically)
cargo run --release --bin fair_price_demo

# Validate (optional, capture diagnostics with calibrated params)
cargo run --release --bin fp_diag_capture -- \
  --warmup-secs 300 --output-dir data/fp_validate
# (run 2+ hours, Ctrl-C)
python scripts/fp_validate.py --data-dir data/fp_validate --warmup-secs 300
```

## Python Dependencies

```bash
pip install pandas numpy scipy matplotlib pyyaml
```
