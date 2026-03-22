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


# ── Kalman filter replay ───────────────────────────────────────────────


def replay_kalman(
    pivot: pd.DataFrame,
    rulers: dict[str, tuple[float, float]],
    h_series: np.ndarray,
    engine_interval_ns: int = 100_000_000,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Replay the Kalman filter on pivot data with a given h(t) series.

    Args:
        pivot: DataFrame with columns=exchanges, rows=timestamps, values=log_mid
        rulers: {exchange: (m_k, sigma_k_sq)}
        h_series: process noise variance per snapshot (len = n_snapshots)

    Returns:
        (y_series, p_series, innovations_by_exchange)
        y_series: Kalman fair price (log space)
        p_series: Kalman uncertainty
    """
    exchanges = [c for c in pivot.columns if c in rulers]
    if not exchanges:
        return np.array([]), np.array([]), {}

    n = len(pivot)
    y = np.full(n, np.nan)
    p = np.full(n, np.nan)
    innovations = {ex: np.full(n, np.nan) for ex in exchanges}

    # Initialize from first valid observation
    initialized = False
    y_cur = 0.0
    p_cur = 0.0

    for t in range(n):
        h_t = h_series[min(t, len(h_series) - 1)]

        if not initialized:
            # Find first valid observation
            for ex in exchanges:
                val = pivot.iloc[t][ex]
                if np.isfinite(val):
                    m_k, sigma_k_sq = rulers[ex]
                    y_cur = val - m_k
                    p_cur = sigma_k_sq
                    initialized = True
                    break
            if not initialized:
                continue

        # Predict
        p_cur += h_t

        # Update from each exchange
        for ex in exchanges:
            val = pivot.iloc[t][ex]
            if not np.isfinite(val):
                continue
            m_k, sigma_k_sq = rulers[ex]
            z = val - m_k  # bias-corrected observation
            innovation = z - y_cur
            s = p_cur + sigma_k_sq
            if s > 0:
                k = p_cur / s  # Kalman gain
                y_cur += k * innovation
                p_cur *= (1.0 - k)
                innovations[ex][t] = innovation / math.sqrt(s)

        y[t] = y_cur
        p[t] = p_cur

    return y, p, innovations


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
        res = pivot[ex].values[valid] - m_k - y_fp[valid]
        residuals[ex] = res

    r_fp_for_r2 = np.diff(y_fp[valid])
    res_shifted = {ex: res[:-1] for ex, res in residuals.items()}
    r2s = residual_r2(r_fp_for_r2, res_shifted)

    # Innovation whiteness (overall)
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

    # Pivot: rows=sample_ts, cols=exchange, values=log_mid
    pivot = ll_base.pivot_table(
        index="sample_ts", columns="exchange", values="log_mid", aggfunc="mean"
    )
    valid = pivot.notna().sum(axis=1) >= 2
    pivot = pivot[valid].sort_index()
    print(f"  {len(pivot)} valid samples, {pivot.shape[1]} exchanges")

    if len(pivot) < 5000:
        print("  Too few samples, skipping")
        return

    # Split: first half for calibration, second half for evaluation
    mid = len(pivot) // 2
    pivot_train = pivot.iloc[:mid]
    pivot_test = pivot.iloc[mid:]
    print(f"  Train: {len(pivot_train)}, Test: {len(pivot_test)}")

    # ── Calibrate rulers from train set (median-based) ──
    median_fp_train = compute_median_fp(pivot_train)
    rulers = {}
    for ex in pivot_train.columns:
        res = (pivot_train[ex] - median_fp_train).dropna()
        if len(res) < 200:
            continue
        m_k = float(res.mean())
        # Spread correction
        ex_mask = ll_base["exchange"] == ex
        median_spread = float(ll_base.loc[ex_mask, "rel_spread"].median())
        sigma_k_sq = max(float(res.var()) - median_spread ** 2 / 12, 1e-12)
        rulers[ex] = (m_k, sigma_k_sq)

    print(f"  Rulers: {len(rulers)} exchanges")
    for ex, (m, s) in rulers.items():
        print(f"    {ex}: m_k={m * BPS:.3f} bps, sigma_k={np.sqrt(s) * BPS:.3f} bps")

    # ── Resample test set to 100ms ──
    ts = pivot_test.index.values
    native_dt = float(np.median(np.diff(ts)))
    median_fp_test = compute_median_fp(pivot_test)

    ts_100, mv_100, ratio = resample_to_100ms(ts, median_fp_test.values, native_dt)
    if ratio > 1:
        pivot_test = pivot_test.iloc[::ratio]
    print(f"  Resampled: {len(ts_100)} snapshots at ~100ms (ratio={ratio})")

    # Compute median returns for noise correction
    r_100 = np.diff(mv_100)
    r_100 = r_100[np.isfinite(r_100)]

    lag1_cov = float(np.mean(r_100[:-1] * r_100[1:])) - float(np.mean(r_100)) ** 2
    R_noise = max(-2.0 * lag1_cov, 0.0)
    var_signal = max(np.var(r_100) - R_noise, 1e-20)
    print(f"  R_noise={R_noise:.2e}, var_signal={var_signal:.2e}")

    # ── Variance signature ──
    plot_variance_signature(r_100, output_dir, base)

    # ── Build vol model candidates ──
    garch_alpha = garch_params["alpha"] if garch_params else 0.06
    garch_beta = garch_params["beta"] if garch_params else 0.94

    candidates = [
        ("Constant", vol_constant(r_100, R_noise)),
        ("GARCH", vol_inline_garch(r_100, R_noise, garch_alpha, garch_beta)),
        ("EWMA(60s)", vol_ewma(r_100, R_noise, halflife_ticks=600)),
        ("EWMA(10min)", vol_ewma(r_100, R_noise, halflife_ticks=6000)),
    ]

    if vol_params and "har_qlike" in vol_params.get("models", {}):
        h_har = vol_har_qlike(r_100, ts_100[1:], vol_params)
        candidates.append(("HAR-QL", h_har))

    # Also score the median FP (no Kalman, no vol model)
    median_result = score_fair_price(mv_100, pivot_test, rulers, "Median (baseline)")

    # ── Evaluate each vol model ──
    all_results = [median_result]
    for name, h_series in candidates:
        print(f"\n  -- Vol model: {name} --")
        y_fp, p_fp, innov = replay_kalman(pivot_test, rulers, h_series)
        result = score_fair_price(y_fp, pivot_test, rulers, f"Kalman+{name}")
        if result:
            result["r_fp"] = np.diff(y_fp[np.isfinite(y_fp)])
        all_results.append(result)

    # ── Plots ──
    valid_results = [r for r in all_results if r]
    plot_r2_comparison(valid_results, output_dir, base)
    plot_acf_comparison(
        [r for r in valid_results if "r_fp" in r], output_dir, base
    )

    # ── Summary table ──
    print(f"\n  {'=' * 70}")
    print(f"  SUMMARY: {base}")
    print(f"  {'=' * 70}")
    print(f"  {'Model':<25} {'ACF(1)':>10} {'LB p':>8} {'mean R²':>10} {'max R²':>10}")
    print(f"  {'-' * 65}")
    for r in valid_results:
        print(f"  {r.get('label', '?'):<25} "
              f"{r.get('acf1', np.nan):>10.6f} "
              f"{r.get('ljung_box_p', np.nan):>8.4f} "
              f"{r.get('mean_r2', np.nan):>10.6f} "
              f"{r.get('max_r2', np.nan):>10.6f}")


def main():
    parser = argparse.ArgumentParser(description="Deep fair price calibration study")
    parser.add_argument("--data-dir", default="data/fp_study")
    parser.add_argument("--config", default="configs/lead_lag_study.yaml")
    parser.add_argument("--warmup-secs", type=int, default=300)
    parser.add_argument("--vol-params-dir", default="../botfed-params/vol",
                        help="Directory with pre-calibrated HAR params")
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
    print(f"\nBases found: {bases}")

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
