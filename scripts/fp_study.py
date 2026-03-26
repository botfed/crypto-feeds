#!/usr/bin/env python3
"""
Deep fair price calibration study.

Evaluates the Kalman fair price under different vol models for ETH + AIXBT.
Uses lead_lag_capture data, replays the Kalman filter offline, and scores
each vol model by the quality of the resulting fair price.

Usage:
  # 1. Capture data (4-8 hours, Ctrl-C to stop)
  cargo run --release --bin lead_lag_capture -- \
    configs/lead_lag_study.yaml data/fp_study/lead_lag.csv.gz

  # 2. Calibrate initial params
  python scripts/fp_calibrate.py --data-dir data/fp_study \
    --config configs/lead_lag_study.yaml --warmup-secs 300

  # 3. Run this study
  python scripts/fp_study.py --data-dir data/fp_study \
    --config configs/lead_lag_study.yaml
"""

import argparse
import math
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml
from scipy import stats as sp_stats

BPS = 1e4
LN2 = math.log(2)

# ── Data loading ────────────────────────────────────────────────────────


def load_lead_lag(data_dir: Path, warmup_secs: int = 300) -> pd.DataFrame:
    ll_path = data_dir / "lead_lag.csv.gz"
    if not ll_path.exists():
        print(f"ERROR: {ll_path} not found")
        sys.exit(1)

    ll = pd.read_csv(ll_path)
    t0 = ll["sample_ts"].min()
    warmup_ns = warmup_secs * 1_000_000_000
    ll = ll[ll["sample_ts"] >= t0 + warmup_ns].copy()
    ll = ll.dropna(subset=["bid", "ask"])
    ll = ll[(ll["bid"] > 0) & (ll["ask"] > 0)]
    ll["log_mid"] = np.log((ll["bid"] + ll["ask"]) / 2)
    ll["mid"] = (ll["bid"] + ll["ask"]) / 2
    ll["rel_spread"] = (ll["ask"] - ll["bid"]) / ll["mid"]

    def extract_base(sym: str) -> str:
        return sym.split("-")[1] if "-" in sym else sym.split("_")[0]

    ll["base"] = ll["canonical_symbol"].map(extract_base)

    duration_s = (ll["sample_ts"].max() - ll["sample_ts"].min()) / 1e9
    print(f"Loaded {len(ll)} rows, {duration_s / 3600:.1f} hours")
    return ll


def load_vol_params(params_dir: Path, symbol: str) -> dict | None:
    path = params_dir / f"{symbol.lower()}.yaml"
    if not path.exists():
        return None
    with open(path) as f:
        return yaml.safe_load(f)


# ── Fair price estimators ───────────────────────────────────────────────


def compute_median_fp(pivot: pd.DataFrame) -> pd.Series:
    """Cross-sectional median of exchange log-mids."""
    return pivot.median(axis=1)


def resample_to_100ms(ts: np.ndarray, vals: np.ndarray, native_dt_ns: float):
    """Resample to ~100ms by subsampling."""
    ENGINE_NS = 100_000_000
    ratio = max(1, round(ENGINE_NS / native_dt_ns))
    if ratio > 1:
        return ts[::ratio], vals[::ratio], ratio
    return ts, vals, 1


# ── Observation extraction with latency ──────────────────────────────────


def build_observations(
    ll_base: pd.DataFrame,
    tick_interval_ns: int = 100_000_000,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list[str], int]:
    """Build observation arrays from lead_lag data with latency-based age.

    Every exchange quote at every sample tick is an observation.
    Age = (sample_ts - exchange_ts) in ticks, used to inflate σ_k².

    Returns: (obs_tick, obs_exch, obs_val, obs_age_ticks, exchanges, T)
    """
    # Assign tick indices from sample_ts
    sample_ts = ll_base["sample_ts"].values
    ts_unique = np.unique(sample_ts)
    ts_unique.sort()
    T = len(ts_unique)
    ts_to_tick = {ts: i for i, ts in enumerate(ts_unique)}

    exchanges = sorted(ll_base["exchange"].unique())
    ex_to_idx = {ex: i for i, ex in enumerate(exchanges)}

    obs_tick_list = []
    obs_exch_list = []
    obs_val_list = []
    obs_age_list = []

    for _, row in ll_base.iterrows():
        bid, ask = row["bid"], row["ask"]
        if not (np.isfinite(bid) and np.isfinite(ask) and bid > 0 and ask > 0):
            continue
        log_mid = math.log((bid + ask) / 2.0)
        ts = row["sample_ts"]
        ex_ts = row["exchange_ts"]
        tick = ts_to_tick.get(ts)
        if tick is None:
            continue

        age_ns = ts - ex_ts if np.isfinite(ex_ts) and ex_ts > 0 else 0
        age_ticks = max(age_ns / tick_interval_ns, 0.0)

        obs_tick_list.append(tick)
        obs_exch_list.append(ex_to_idx[row["exchange"]])
        obs_val_list.append(log_mid)
        obs_age_list.append(age_ticks)

    obs_tick = np.array(obs_tick_list, dtype=np.int64)
    obs_exch = np.array(obs_exch_list, dtype=np.int32)
    obs_val = np.array(obs_val_list, dtype=np.float64)
    obs_age = np.array(obs_age_list, dtype=np.float64)

    # Sort by tick
    order = np.argsort(obs_tick, kind="stable")
    obs_tick = obs_tick[order]
    obs_exch = obs_exch[order]
    obs_val = obs_val[order]
    obs_age = obs_age[order]

    K = len(exchanges)
    N_obs = len(obs_tick)
    median_age = float(np.median(obs_age))
    print(f"    {N_obs} observations from {K} exchanges over {T} ticks "
          f"({N_obs / T:.1f} obs/tick, median age={median_age:.1f} ticks)")

    return obs_tick, obs_exch, obs_val, obs_age, exchanges, T


# ── Kalman smoother (forward-backward) ──────────────────────────────────


def tick_smoother(
    T: int,
    obs_tick: np.ndarray,
    obs_exch: np.ndarray,
    obs_val: np.ndarray,
    obs_age: np.ndarray,
    m: np.ndarray,
    sigma_k: np.ndarray,
    mu: float,
    h_tick: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Tick-level Kalman forward-backward with latency-adjusted noise.

    Each observation's effective noise is σ_k² + h * age_ticks,
    where age_ticks = (sample_ts - exchange_ts) / tick_interval.
    Stale quotes automatically get lower Kalman gain.

    Returns Y_fwd, P_fwd, Y_smooth, P_smooth (all length T).
    """
    N_obs = len(obs_tick)
    Y_fwd = np.empty(T)
    P_fwd = np.empty(T)

    # Init from first observation
    if N_obs > 0:
        k0 = obs_exch[0]
        Y_pred = obs_val[0] - m[k0]
        P_pred = sigma_k[k0] ** 2
    else:
        Y_pred = 0.0
        P_pred = 1.0

    obs_ptr = 0

    # Forward pass
    for t in range(T):
        y = Y_pred
        p = P_pred
        h_t = h_tick[min(t, len(h_tick) - 1)]

        while obs_ptr < N_obs and obs_tick[obs_ptr] == t:
            k = obs_exch[obs_ptr]
            w = obs_val[obs_ptr]
            age = obs_age[obs_ptr]
            # Effective noise: base σ_k² + diffusion since exchange_ts
            R = sigma_k[k] ** 2 + h_t * age
            innov = (w - m[k]) - y
            S = p + R
            if S > 0:
                gain = p / S
                y = y + gain * innov
                p = (1.0 - gain) * p
            obs_ptr += 1

        Y_fwd[t] = y
        P_fwd[t] = p

        if t < T - 1:
            Y_pred = y + mu
            P_pred = p + h_t

    # Backward pass (RTS smoother)
    Y_smooth = np.empty(T)
    P_smooth = np.empty(T)
    Y_smooth[T - 1] = Y_fwd[T - 1]
    P_smooth[T - 1] = P_fwd[T - 1]

    for t in range(T - 2, -1, -1):
        P_pred_t = P_fwd[t] + h_tick[min(t, len(h_tick) - 1)]
        if P_pred_t > 0:
            J = P_fwd[t] / P_pred_t
        else:
            J = 0.0
        Y_smooth[t] = Y_fwd[t] + J * (Y_smooth[t + 1] - Y_fwd[t] - mu)
        P_smooth[t] = P_fwd[t] + J * J * (P_smooth[t + 1] - P_pred_t)

    return Y_fwd, P_fwd, Y_smooth, P_smooth


# ── EM fitting of ruler params (smoother-based) ─────────────────────────


def fit_rulers_em(
    ll_base: pd.DataFrame,
    h_series: np.ndarray,
    n_iter: int = 30,
    tick_interval_ns: int = 100_000_000,
) -> tuple[dict[str, tuple[float, float]], np.ndarray, np.ndarray, list[str], int]:
    """Fit (m_k, σ_k) via EM with RTS smoother and latency-adjusted noise.

    Returns:
        rulers: {exchange_name: (m_k, sigma_k_sq)}
        Y_smooth: smoothed state (log price), length T
        P_smooth: smoothed uncertainty, length T
        exchanges: list of exchange names
        T: number of ticks
    """
    obs_tick, obs_exch, obs_val, obs_age, exchanges, T = build_observations(
        ll_base, tick_interval_ns
    )
    K = len(exchanges)

    # Initialize
    m = np.zeros(K)
    sigma_k = np.ones(K) * 0.005
    for k in range(K):
        mask = obs_exch == k
        if mask.sum() > 0:
            m[k] = float(np.median(obs_val[mask]))
    m -= m.mean()
    mu = 0.0

    # h_tick array
    if len(h_series) < T - 1:
        h_tick = np.full(T - 1, h_series[-1] if len(h_series) > 0 else 1e-10)
        h_tick[:len(h_series)] = h_series
    else:
        h_tick = h_series[:T - 1]

    Y_s = None
    P_s = None

    for it in range(n_iter):
        Y_f, P_f, Y_s, P_s = tick_smoother(
            T, obs_tick, obs_exch, obs_val, obs_age, m, sigma_k, mu, h_tick
        )

        # Update drift
        dY = Y_s[1:] - Y_s[:-1]
        mu = float(dY.mean())

        # Update ruler params
        for k in range(K):
            mask = obs_exch == k
            if mask.sum() == 0:
                continue
            ticks_k = obs_tick[mask]
            vals_k = obs_val[mask]
            ages_k = obs_age[mask]
            resid = vals_k - Y_s[ticks_k]
            m[k] = float(resid.mean())
            P_at_obs = P_s[ticks_k]
            # E[resid²] = σ_k² + h * age_k + P_smooth
            # So σ_k² = E[(resid - m_k)²] + P_smooth - h * age_k
            h_at_obs = np.array([h_tick[min(t, len(h_tick) - 1)] for t in ticks_k])
            raw_var = float(np.mean((resid - m[k]) ** 2 + P_at_obs))
            age_correction = float(np.mean(h_at_obs * ages_k))
            sigma_k[k] = math.sqrt(max(raw_var - age_correction, 1e-16))

        m -= m.mean()

    rulers = {}
    for k, ex in enumerate(exchanges):
        rulers[ex] = (float(m[k]), float(sigma_k[k] ** 2))

    return rulers, Y_s, P_s, exchanges, T


# ── Kalman filter replay (forward only, for test-set evaluation) ────────


def replay_kalman(
    ll_base: pd.DataFrame,
    rulers: dict[str, tuple[float, float]],
    h_series: np.ndarray,
    tick_interval_ns: int = 100_000_000,
) -> tuple[np.ndarray, np.ndarray, int]:
    """Replay forward Kalman filter with fitted rulers on new data.

    Uses latency-adjusted noise from exchange_ts.
    Returns (Y_fwd, P_fwd, T).
    """
    obs_tick, obs_exch, obs_val, obs_age, exchanges, T = build_observations(
        ll_base, tick_interval_ns
    )
    K = len(exchanges)

    m = np.zeros(K)
    sigma_k = np.ones(K) * 0.005
    for k, ex in enumerate(exchanges):
        if ex in rulers:
            m_k, sigma_k_sq = rulers[ex]
            m[k] = m_k
            sigma_k[k] = math.sqrt(sigma_k_sq)

    if len(h_series) < T - 1:
        h_tick = np.full(T - 1, h_series[-1] if len(h_series) > 0 else 1e-10)
        h_tick[:len(h_series)] = h_series
    else:
        h_tick = h_series[:T - 1]

    Y_f, P_f, _, _ = tick_smoother(
        T, obs_tick, obs_exch, obs_val, obs_age, m, sigma_k, 0.0, h_tick
    )
    return Y_f, P_f, T


# ── Vol model candidates ────────────────────────────────────────────────


def vol_constant(r: np.ndarray, R_noise: float, **_) -> np.ndarray:
    """Constant vol: h = var_signal for all t."""
    var_signal = max(np.var(r) - R_noise, 1e-20)
    return np.full(len(r), var_signal)


def vol_inline_garch(
    r: np.ndarray, R_noise: float, alpha: float, beta: float, **_
) -> np.ndarray:
    """GARCH(1,1) on 100ms returns with noise correction."""
    var_signal = max(np.var(r) - R_noise, 1e-20)
    omega = var_signal * max(1 - alpha - beta, 0.001)
    h = var_signal
    out = np.empty(len(r))
    for i in range(len(r)):
        out[i] = h
        rc = max(r[i] ** 2 - R_noise, 0.0)
        h = omega + alpha * rc + beta * h
        h = max(h, 1e-20)
    return out


def vol_ewma(r: np.ndarray, R_noise: float, halflife_ticks: int = 6000, **_) -> np.ndarray:
    """EWMA of noise-corrected squared returns."""
    decay = math.exp(-LN2 / halflife_ticks)
    var_signal = max(np.var(r) - R_noise, 1e-20)
    ewma = var_signal
    out = np.empty(len(r))
    for i in range(len(r)):
        out[i] = ewma
        rc = max(r[i] ** 2 - R_noise, 0.0)
        ewma = (1 - decay) * rc + decay * ewma
        ewma = max(ewma, 1e-20)
    return out


def vol_har_qlike(
    r_100ms: np.ndarray,
    ts_100ms: np.ndarray,
    vol_params: dict,
    **_,
) -> np.ndarray:
    """HAR-QLIKE vol from pre-calibrated params, scaled to per-100ms variance.

    Builds 5-min bars from 100ms returns, runs HAR, scales back down.
    """
    target_min = vol_params["target_min"]
    bar_interval_ns = target_min * 60 * 1_000_000_000
    snaps_per_bar = target_min * 60 * 10  # at 100ms

    har = vol_params["models"]["har_qlike"]
    params = har["params"]  # [intercept, beta_1, beta_6, beta_24, beta_288]
    feature_names = har["feature_names"]
    windows = [int(fn.split("_")[-1]) for fn in feature_names]

    f_h = vol_params["seasonality"]["f_h"]
    f_d = vol_params["seasonality"]["f_d"]

    # Build 5-min bars from 100ms data
    # Group by bar boundary
    bar_starts = (ts_100ms // bar_interval_ns) * bar_interval_ns
    unique_bars = np.unique(bar_starts)

    # For each bar, compute GK from the 100ms returns within it
    # Use open/close of the bar's cumulative return to approximate OHLC
    cum_log = np.cumsum(r_100ms)
    cum_log = np.insert(cum_log, 0, 0.0)  # prepend 0

    bar_gk = []
    bar_ts = []
    for bs in unique_bars:
        mask = bar_starts == bs
        idx = np.where(mask)[0]
        if len(idx) < snaps_per_bar * 0.5:
            continue
        # Cumulative log-price within bar (relative to bar start)
        bar_cum = cum_log[idx[0]:idx[-1] + 2] - cum_log[idx[0]]
        o = bar_cum[0]
        c = bar_cum[-1]
        h = bar_cum.max()
        l = bar_cum.min()
        hl = h - l
        co = c - o
        gk = max(0.5 * hl ** 2 - (2 * LN2 - 1) * co ** 2, 0.0)

        # De-seasonalize
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(bs / 1e9, tz=timezone.utc)
        hour = dt.hour
        dow = dt.weekday()  # Monday=0
        sf = f_h.get(hour, 1.0) * f_d.get(dow, 1.0)
        gk_deseas = gk / sf if sf > 0 else gk

        bar_gk.append(gk_deseas)
        bar_ts.append(bs)

    bar_gk = np.array(bar_gk)
    bar_ts_arr = np.array(bar_ts)
    n_bars = len(bar_gk)

    if n_bars < max(windows) + 1:
        # Not enough bars for HAR — return constant
        var_signal = max(np.var(r_100ms) * 0.5, 1e-20)
        return np.full(len(r_100ms), var_signal)

    # Compute HAR predictions per bar
    har_log_gk = np.full(n_bars, np.nan)
    for t in range(max(windows), n_bars):
        log_rvs = []
        for w in windows:
            rv = bar_gk[t - w + 1 : t + 1].sum()
            if rv <= 0:
                break
            log_rvs.append(math.log(rv))
        if len(log_rvs) == len(windows):
            pred = params[0] + sum(p * x for p, x in zip(params[1:], log_rvs))
            har_log_gk[t] = pred

    # Fill NaN at start with first valid
    first_valid = np.where(np.isfinite(har_log_gk))[0]
    if len(first_valid) == 0:
        var_signal = max(np.var(r_100ms) * 0.5, 1e-20)
        return np.full(len(r_100ms), var_signal)
    har_log_gk[:first_valid[0]] = har_log_gk[first_valid[0]]
    # Forward-fill remaining NaN
    for i in range(1, n_bars):
        if not np.isfinite(har_log_gk[i]):
            har_log_gk[i] = har_log_gk[i - 1]

    # Convert to per-100ms variance:
    # HAR predicts log(GK_deseasoned) per target_min bar
    # var_per_bar = exp(pred) * seasonal_now
    # var_per_100ms = var_per_bar / snaps_per_bar
    # For now, use average seasonal (1.0) since we're computing relative scores
    h_per_bar = np.exp(har_log_gk)
    h_per_100ms = h_per_bar / snaps_per_bar

    # Expand to 100ms timescale: each bar's h covers its time range
    h_out = np.full(len(r_100ms), h_per_100ms[-1])
    for i, bs in enumerate(bar_ts_arr):
        mask = bar_starts == bs
        h_out[mask] = h_per_100ms[i]

    return h_out


# ── Scoring ─────────────────────────────────────────────────────────────


def ljung_box(x: np.ndarray, lags: int = 20) -> tuple[float, float]:
    """Returns (Q statistic, p-value)."""
    n = len(x)
    x = x - x.mean()
    var = np.sum(x ** 2) / n
    if var < 1e-30 or n < lags + 10:
        return 0.0, 1.0
    q = 0.0
    for k in range(1, lags + 1):
        rho = np.sum(x[k:] * x[:-k]) / (n * var)
        q += rho ** 2 / (n - k)
    q *= n * (n + 2)
    p = 1 - sp_stats.chi2.cdf(q, df=lags)
    return q, p


def variance_ratio(r: np.ndarray, horizons: list[int]) -> dict[int, float]:
    """Variance ratio test: VR(q) = var(r_q) / (q * var(r_1))."""
    var1 = np.var(r)
    if var1 < 1e-30:
        return {h: np.nan for h in horizons}
    out = {}
    for q in horizons:
        r_q = np.array([r[i : i + q].sum() for i in range(0, len(r) - q, q)])
        if len(r_q) < 10:
            out[q] = np.nan
        else:
            out[q] = np.var(r_q) / (q * var1)
    return out


def residual_r2(
    fp_returns: np.ndarray,
    residuals: dict[str, np.ndarray],
) -> dict[str, float]:
    """Regress fp_return[t+1] on residual_k[t] for each exchange."""
    r2s = {}
    y = fp_returns[1:]
    for ex, res in residuals.items():
        x = res[:-1]
        valid = np.isfinite(x) & np.isfinite(y)
        if valid.sum() < 100:
            r2s[ex] = np.nan
            continue
        x_v, y_v = x[valid], y[valid]
        x_v = x_v - x_v.mean()
        y_v = y_v - y_v.mean()
        ss_tot = np.sum(y_v ** 2)
        if ss_tot < 1e-30:
            r2s[ex] = 0.0
            continue
        beta = np.sum(x_v * y_v) / np.sum(x_v ** 2) if np.sum(x_v ** 2) > 0 else 0.0
        ss_res = np.sum((y_v - beta * x_v) ** 2)
        r2s[ex] = 1 - ss_res / ss_tot
    return r2s


def score_fair_price(
    y_fp: np.ndarray,
    pivot: pd.DataFrame,
    rulers: dict[str, tuple[float, float]],
    label: str,
) -> dict:
    """Score a fair price series on all quality metrics."""
    # Align lengths (y_fp from smoother/filter may differ from pivot rows)
    n = min(len(y_fp), len(pivot))
    y_fp = y_fp[:n]
    pivot_vals = {ex: pivot[ex].values[:n] for ex in pivot.columns}

    valid = np.isfinite(y_fp)
    y_clean = y_fp[valid]
    r_fp = np.diff(y_clean)

    if len(r_fp) < 1000:
        print(f"  [{label}] Too few valid FP returns ({len(r_fp)}), skipping")
        return {}

    # Ljung-Box
    lb_q, lb_p = ljung_box(r_fp, lags=20)

    # Variance ratios
    vr = variance_ratio(r_fp, [1, 5, 10, 50, 100])

    # Residual R²
    exchanges = [c for c in pivot.columns if c in rulers]
    residuals = {}
    for ex in exchanges:
        m_k = rulers[ex][0]
        res = pivot_vals[ex][valid] - m_k - y_fp[valid]
        residuals[ex] = res

    r_fp_for_r2 = np.diff(y_fp[valid])
    res_shifted = {ex: res[:-1] for ex, res in residuals.items()}
    r2s = residual_r2(r_fp_for_r2, res_shifted)

    acf1 = np.corrcoef(r_fp[:-1], r_fp[1:])[0, 1] if len(r_fp) > 2 else 0.0

    result = {
        "label": label,
        "n_returns": len(r_fp),
        "ljung_box_Q": lb_q,
        "ljung_box_p": lb_p,
        "acf1": acf1,
        "variance_ratios": vr,
        "residual_r2": r2s,
        "mean_r2": np.nanmean(list(r2s.values())) if r2s else np.nan,
        "max_r2": np.nanmax(list(r2s.values())) if r2s else np.nan,
    }

    print(f"\n  [{label}] n={len(r_fp)}, ACF(1)={acf1:.6f}, "
          f"LB(20) Q={lb_q:.1f} p={lb_p:.4f}")
    print(f"    VR: {', '.join(f'{h}:{v:.3f}' for h, v in vr.items())}")
    print(f"    R²: {', '.join(f'{ex}:{v:.6f}' for ex, v in r2s.items())}")
    print(f"    mean R²={result['mean_r2']:.6f}, max R²={result['max_r2']:.6f}")

    return result


# ── Plotting ────────────────────────────────────────────────────────────


def plot_acf_comparison(results: list[dict], output_dir: Path, base: str):
    """Plot ACF of FP returns for each vol model."""
    fig, axes = plt.subplots(1, len(results), figsize=(5 * len(results), 4), squeeze=False)
    for i, res in enumerate(results):
        if "r_fp" not in res:
            continue
        r = res["r_fp"]
        n = len(r)
        max_lag = min(50, n - 1)
        acf = np.array([np.corrcoef(r[:-k], r[k:])[0, 1] if k > 0 else 1.0
                        for k in range(max_lag + 1)])
        axes[0, i].bar(range(max_lag + 1), acf, width=0.8, alpha=0.7)
        axes[0, i].axhline(1.96 / np.sqrt(n), color="r", ls="--", alpha=0.5)
        axes[0, i].axhline(-1.96 / np.sqrt(n), color="r", ls="--", alpha=0.5)
        axes[0, i].set_title(res["label"])
        axes[0, i].set_xlabel("Lag")
        axes[0, i].set_ylabel("ACF")
    fig.suptitle(f"{base}: FP Return ACF by Vol Model")
    fig.tight_layout()
    fig.savefig(output_dir / f"{base}_acf_comparison.png", dpi=100)
    plt.close(fig)


def plot_variance_signature(r_100ms: np.ndarray, output_dir: Path, base: str):
    """Variance signature: realized var at different aggregation levels."""
    horizons = [1, 5, 10, 50, 100, 500, 1000, 3000]
    var1 = np.var(r_100ms)
    vrs = []
    for h in horizons:
        r_agg = np.array([r_100ms[i:i + h].sum() for i in range(0, len(r_100ms) - h, h)])
        if len(r_agg) > 10:
            vrs.append(np.var(r_agg) / (h * var1))
        else:
            vrs.append(np.nan)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(horizons, vrs, "o-", markersize=6)
    ax.axhline(1.0, color="r", ls="--", alpha=0.5, label="Pure diffusion")
    ax.set_xscale("log")
    ax.set_xlabel("Aggregation horizon (ticks @ 100ms)")
    ax.set_ylabel("Variance ratio")
    ax.set_title(f"{base}: Variance Signature Plot")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / f"{base}_variance_signature.png", dpi=100)
    plt.close(fig)


def plot_r2_comparison(results: list[dict], output_dir: Path, base: str):
    """Bar chart comparing mean R² across vol models."""
    labels = [r["label"] for r in results if "mean_r2" in r]
    mean_r2 = [r["mean_r2"] for r in results if "mean_r2" in r]
    max_r2 = [r["max_r2"] for r in results if "max_r2" in r]

    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(labels))
    ax.bar(x - 0.15, mean_r2, 0.3, label="mean R²", alpha=0.8)
    ax.bar(x + 0.15, max_r2, 0.3, label="max R²", alpha=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel("R² (lower = better)")
    ax.set_title(f"{base}: Exchange Residual → FP Return Predictability")
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / f"{base}_r2_comparison.png", dpi=100)
    plt.close(fig)


# ── Main ────────────────────────────────────────────────────────────────


def study_group(
    ll_base: pd.DataFrame,
    base: str,
    vol_params: dict | None,
    garch_params: dict | None,
    output_dir: Path,
):
    """Run the full study for one base asset."""
    print(f"\n{'=' * 60}")
    print(f"  {base}")
    print(f"{'=' * 60}")

    # Split raw data into train/test halves by time
    ts_all = ll_base["sample_ts"].values
    ts_mid = np.median(ts_all)
    ll_train = ll_base[ll_base["sample_ts"] < ts_mid].copy()
    ll_test = ll_base[ll_base["sample_ts"] >= ts_mid].copy()
    print(f"  Train: {len(ll_train)} rows, Test: {len(ll_test)} rows")

    if len(ll_train) < 5000 or len(ll_test) < 5000:
        print("  Too few rows, skipping")
        return

    # Pivot for median FP and scoring (still needed for R² computation)
    pivot_train = ll_train.pivot_table(
        index="sample_ts", columns="exchange", values="log_mid", aggfunc="mean"
    ).sort_index()
    pivot_test = ll_test.pivot_table(
        index="sample_ts", columns="exchange", values="log_mid", aggfunc="mean"
    ).sort_index()

    # Median FP for baseline scoring and vol model inputs
    median_fp_train = compute_median_fp(pivot_train)
    median_fp_test = compute_median_fp(pivot_test)

    ts_train = pivot_train.index.values
    ts_test = pivot_test.index.values
    native_dt = float(np.median(np.diff(ts_train)))
    ts_train_100, mv_train_100, ratio = resample_to_100ms(
        ts_train, median_fp_train.values, native_dt
    )
    ts_test_100, mv_test_100, ratio_t = resample_to_100ms(
        ts_test, median_fp_test.values, native_dt
    )
    print(f"  Ticks: train={len(ts_train_100)}, test={len(ts_test_100)} @ ~100ms")

    # Median returns for noise correction
    r_train = np.diff(mv_train_100)
    r_train = r_train[np.isfinite(r_train)]
    r_test = np.diff(mv_test_100)
    r_test = r_test[np.isfinite(r_test)]

    lag1_cov = float(np.mean(r_train[:-1] * r_train[1:])) - float(np.mean(r_train)) ** 2
    R_noise = max(-2.0 * lag1_cov, 0.0)
    var_signal = max(np.var(r_train) - R_noise, 1e-20)
    print(f"  R_noise={R_noise:.2e}, var_signal={var_signal:.2e}")

    plot_variance_signature(r_test, output_dir, base)

    # ── Vol model candidates (from median returns) ──
    garch_alpha = garch_params["alpha"] if garch_params else 0.06
    garch_beta = garch_params["beta"] if garch_params else 0.94

    train_candidates = [
        ("Constant", vol_constant(r_train, R_noise)),
        ("GARCH", vol_inline_garch(r_train, R_noise, garch_alpha, garch_beta)),
        ("EWMA(60s)", vol_ewma(r_train, R_noise, halflife_ticks=600)),
        ("EWMA(10min)", vol_ewma(r_train, R_noise, halflife_ticks=6000)),
    ]
    test_candidates = [
        ("Constant", vol_constant(r_test, R_noise)),
        ("GARCH", vol_inline_garch(r_test, R_noise, garch_alpha, garch_beta)),
        ("EWMA(60s)", vol_ewma(r_test, R_noise, halflife_ticks=600)),
        ("EWMA(10min)", vol_ewma(r_test, R_noise, halflife_ticks=6000)),
    ]

    if vol_params and "har_qlike" in vol_params.get("models", {}):
        h_har_train = vol_har_qlike(r_train, ts_train_100[1:], vol_params)
        h_har_test = vol_har_qlike(r_test, ts_test_100[1:], vol_params)
        train_candidates.append(("HAR-QL", h_har_train))
        test_candidates.append(("HAR-QL", h_har_test))

    # ── Median baseline ──
    median_rulers = {}
    for ex in pivot_test.columns:
        col = pivot_test[ex].dropna()
        med = median_fp_test[col.index]
        res = (col - med).dropna()
        if len(res) > 50:
            median_rulers[ex] = (float(res.mean()), max(float(res.var()), 1e-14))
    median_result = score_fair_price(
        mv_test_100, pivot_test, median_rulers, "Median (baseline)"
    )

    median_rulers_train = {}
    for ex in pivot_train.columns:
        col = pivot_train[ex].dropna()
        med = median_fp_train[col.index]
        res = (col - med).dropna()
        if len(res) > 50:
            median_rulers_train[ex] = (float(res.mean()), max(float(res.var()), 1e-14))
    median_is = score_fair_price(
        mv_train_100, pivot_train, median_rulers_train, "Median (IS)"
    )

    # ── For each vol model: EM-fit rulers on train, evaluate on both ──
    all_results = [median_result]
    all_results_is = [median_is]
    for (name, h_train), (_, h_test) in zip(train_candidates, test_candidates):
        print(f"\n  -- Vol model: {name} --")

        # EM fit on train (smoother-based, latency-adjusted)
        print(f"    Fitting rulers via EM (smoother) on train set...")
        rulers, Y_smooth_train, P_smooth_train, em_exchanges, T_train = fit_rulers_em(
            ll_train, h_train, n_iter=250
        )
        for ex, (m, s) in sorted(rulers.items()):
            print(f"      {ex}: m_k={m * BPS:.3f} bps, sigma_k={np.sqrt(s) * BPS:.3f} bps")

        # In-sample: score smoother on train
        result_is = score_fair_price(Y_smooth_train, pivot_train, rulers, f"Kalman+{name} (IS)")
        all_results_is.append(result_is)

        # Out-of-sample: forward filter on test with fitted rulers
        y_fp, p_fp, T_test = replay_kalman(ll_test, rulers, h_test)
        result = score_fair_price(y_fp, pivot_test, rulers, f"Kalman+{name}")
        if result:
            result["r_fp"] = np.diff(y_fp[np.isfinite(y_fp)])
            result["rulers"] = rulers
        all_results.append(result)

    # ── Plots ──
    valid_results = [r for r in all_results if r]
    plot_r2_comparison(valid_results, output_dir, base)
    plot_acf_comparison(
        [r for r in valid_results if "r_fp" in r], output_dir, base
    )

    # ── Summary table (in-sample + out-of-sample side by side) ──
    print(f"\n  {'=' * 100}")
    print(f"  SUMMARY: {base}")
    print(f"  {'=' * 100}")
    print(f"  {'Model':<25} {'ACF(IS)':>9} {'R²(IS)':>9} {'ACF(OOS)':>9} {'R²(OOS)':>9} {'LB p(OOS)':>10} {'maxR²(OOS)':>11}")
    print(f"  {'-' * 95}")

    for r_is, r_oos in zip(all_results_is, all_results):
        label = r_oos.get("label", "?") if r_oos else r_is.get("label", "?")
        label = label.replace(" (IS)", "")
        acf_is = r_is.get("acf1", np.nan) if r_is else np.nan
        r2_is = r_is.get("mean_r2", np.nan) if r_is else np.nan
        acf_oos = r_oos.get("acf1", np.nan) if r_oos else np.nan
        r2_oos = r_oos.get("mean_r2", np.nan) if r_oos else np.nan
        lb_oos = r_oos.get("ljung_box_p", np.nan) if r_oos else np.nan
        maxr2_oos = r_oos.get("max_r2", np.nan) if r_oos else np.nan
        print(f"  {label:<25} "
              f"{acf_is:>9.4f} {r2_is:>9.6f} "
              f"{acf_oos:>9.4f} {r2_oos:>9.6f} "
              f"{lb_oos:>10.4f} {maxr2_oos:>11.6f}")


def main():
    parser = argparse.ArgumentParser(description="Deep fair price calibration study")
    parser.add_argument("--data-dir", default="data/fp_study")
    parser.add_argument("--config", default="configs/lead_lag_study.yaml")
    parser.add_argument("--warmup-secs", type=int, default=300)
    parser.add_argument("--vol-params-dir", default="../botfed-params/vol",
                        help="Directory with pre-calibrated HAR params")
    parser.add_argument("--base", default=None,
                        help="Run only this base asset (e.g. ETH)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    vol_params_dir = Path(args.vol_params_dir)

    # Load calibrated GARCH params if available
    import json
    calib_path = Path("configs/fp_calibrated.json")
    calib = {}
    if calib_path.exists():
        with open(calib_path) as f:
            calib = json.load(f).get("groups", {})

    ll = load_lead_lag(data_dir, args.warmup_secs)

    bases = sorted(ll["base"].unique())
    if args.base:
        bases = [b for b in bases if b.upper() == args.base.upper()]
    print(f"\nBases: {bases}")

    for base in bases:
        ll_base = ll[ll["base"] == base]
        if len(ll_base) < 5000:
            print(f"\nSkipping {base}: only {len(ll_base)} rows")
            continue

        vp = load_vol_params(vol_params_dir, base)
        gp = calib.get(base)
        study_group(ll_base, base, vp, gp, data_dir)

    print(f"\nPlots saved to {data_dir}/")


if __name__ == "__main__":
    main()
