#!/usr/bin/env python3
"""
Statistical validation of the fair price engine.

Reads diagnostic csv.gz files produced by fp_diag_capture and runs 8 tests:
  1. Standardized innovation whiteness (mean, variance, autocorrelation, per-exchange)
  2. P_unc coverage at multiple horizons
  3. GARCH vol calibration (Mincer-Zarnowitz)
  4. Bias estimation accuracy
  5. Per-exchange noise calibration
  6. Edge predictiveness
  7. Innovation normality (Jarque-Bera)
  8. Regime tracking (GARCH adaptation speed)

Usage:
  python scripts/fp_validate.py --data-dir data/fp_diag --warmup-secs 900
"""

import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats


# ── Helpers ──────────────────────────────────────────────────────────────────

BPS = 1e4


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def strip_warmup(df: pd.DataFrame, warmup_ns: int) -> pd.DataFrame:
    t0 = df["snap_ts_ns"].min()
    return df[df["snap_ts_ns"] >= t0 + warmup_ns].copy()


def ljung_box(x: np.ndarray, lags: int = 20) -> float:
    """Ljung-Box Q statistic."""
    n = len(x)
    if n < lags + 1:
        return 0.0
    x = x - x.mean()
    var = np.sum(x**2) / n
    if var < 1e-30:
        return 0.0
    q = 0.0
    for k in range(1, lags + 1):
        rho = np.sum(x[k:] * x[:-k]) / (n * var)
        q += rho**2 / (n - k)
    return n * (n + 2) * q


# ── Test functions ───────────────────────────────────────────────────────────


def test1_innovation_whiteness(
    ticks: pd.DataFrame,
) -> list[dict]:
    """Test 1: Standardized innovation whiteness."""
    results = []
    z = ticks["standardized_innov"].values
    z = z[np.isfinite(z)]
    n = len(z)
    if n < 100:
        results.append(
            dict(test="1a_z_mean", value=np.nan, threshold="n<100", passed=False)
        )
        return results

    # 1a: mean = 0
    z_mean = np.mean(z)
    z_se = np.std(z, ddof=1) / np.sqrt(n)
    t_stat = z_mean / z_se if z_se > 0 else 0.0
    results.append(
        dict(
            test="1a_z_mean",
            value=f"{z_mean:.6f} (t={t_stat:.2f})",
            threshold="|t| < 2.58",
            passed=abs(t_stat) < 2.58,
        )
    )

    # 1b: variance = 1
    z_var = np.var(z, ddof=1)
    results.append(
        dict(
            test="1b_z_var",
            value=f"{z_var:.4f}",
            threshold="[0.9, 1.1]",
            passed=0.9 <= z_var <= 1.1,
        )
    )

    # 1c: no autocorrelation (Ljung-Box)
    q = ljung_box(z, lags=20)
    results.append(
        dict(
            test="1c_ljung_box_Q20",
            value=f"{q:.2f}",
            threshold="< 37.6",
            passed=q < 37.6,
        )
    )

    # 1d: per-exchange variance
    for mi, grp in ticks.groupby("member_idx"):
        zi = grp["standardized_innov"].values
        zi = zi[np.isfinite(zi)]
        if len(zi) < 50:
            continue
        vi = np.var(zi, ddof=1)
        results.append(
            dict(
                test=f"1d_z_var_member{mi}",
                value=f"{vi:.4f}",
                threshold="[0.9, 1.1]",
                passed=0.9 <= vi <= 1.1,
            )
        )

    return results


def test2_punc_coverage(group: pd.DataFrame) -> list[dict]:
    """Test 2: P_unc coverage at multiple horizons."""
    results = []
    y = group["y"].values
    p = group["p"].values
    h = group["h"].values
    n = len(y)

    horizons = [1, 5, 10, 50, 100]
    for k in horizons:
        if n <= k + 1:
            continue
        dy = y[k:] - y[:-k]
        p_fwd = p[:-k]
        h_fwd = h[:-k]
        sigma = np.sqrt(p_fwd + k * h_fwd)

        # Avoid division by zero
        valid = sigma > 0
        dy = dy[valid]
        sigma = sigma[valid]
        if len(dy) < 100:
            continue

        z_fwd = dy / sigma
        cov_1s = np.mean(np.abs(z_fwd) < 1.0)
        cov_2s = np.mean(np.abs(z_fwd) < 2.0)

        results.append(
            dict(
                test=f"2_coverage_1s_k{k}",
                value=f"{cov_1s:.3f}",
                threshold="[0.58, 0.78]",
                passed=0.58 <= cov_1s <= 0.78,
            )
        )
        results.append(
            dict(
                test=f"2_coverage_2s_k{k}",
                value=f"{cov_2s:.3f}",
                threshold="[0.90, 1.00]",
                passed=0.90 <= cov_2s <= 1.00,
            )
        )

    return results


def test3_garch_vol(group: pd.DataFrame) -> list[dict]:
    """Test 3: GARCH vol calibration (Mincer-Zarnowitz regression)."""
    results = []
    h = group["h"].values
    r2c = group["r2_corrected"].values

    valid = np.isfinite(h) & np.isfinite(r2c) & (h > 0)
    h = h[valid]
    r2c = r2c[valid]
    if len(h) < 100:
        results.append(
            dict(test="3_garch_MZ", value="n<100", threshold="-", passed=False)
        )
        return results

    # Mincer-Zarnowitz: r2_corrected = a + b*h + eps
    slope, intercept, r_value, p_value, se = stats.linregress(h, r2c)
    results.append(
        dict(
            test="3a_MZ_intercept",
            value=f"{intercept:.2e}",
            threshold="~0",
            passed=True,  # informational
        )
    )
    results.append(
        dict(
            test="3b_MZ_slope",
            value=f"{slope:.4f}",
            threshold="[0.7, 1.3]",
            passed=0.7 <= slope <= 1.3,
        )
    )

    # Conditional unbiasedness by h decile
    try:
        deciles = pd.qcut(h, 10, duplicates="drop")
        df_tmp = pd.DataFrame({"h": h, "r2c": r2c, "dec": deciles})
        ratios = df_tmp.groupby("dec", observed=True).apply(
            lambda g: g["r2c"].mean() / g["h"].mean() if g["h"].mean() > 0 else np.nan
        )
        ratios = ratios.dropna()
        if len(ratios) > 0:
            min_r, max_r = ratios.min(), ratios.max()
            results.append(
                dict(
                    test="3c_conditional_ratio_range",
                    value=f"[{min_r:.2f}, {max_r:.2f}]",
                    threshold="decile ratios within [0.5, 2.0]",
                    passed=min_r >= 0.5 and max_r <= 2.0,
                )
            )
    except Exception:
        pass

    return results


def test4_bias(ticks: pd.DataFrame, group: pd.DataFrame) -> list[dict]:
    """Test 4: Bias estimation accuracy.

    For each member, compare mean(log_mid - y) to the member's bias (m_k).
    We use the last recalib-period's bias. Since ticks record log_mid and group
    records y per snap, we join on snap_ts_ns.
    """
    results = []

    # Build a snap_ts -> y lookup
    snap_y = group.set_index("snap_ts_ns")["y"]

    for mi, grp in ticks.groupby("member_idx"):
        # innovation = log_mid - bias - y  =>  log_mid - y = innovation + bias
        # We have innovation directly; empirical bias = mean(log_mid - y) = mean(innovation) + bias
        # But simpler: mean(log_mid - y) across ticks for this member
        merged = grp.merge(
            snap_y.rename("y_snap"),
            left_on="snap_ts_ns",
            right_index=True,
            how="inner",
        )
        if len(merged) < 100:
            continue

        empirical_bias = (merged["log_mid"] - merged["y_snap"]).mean()
        # The filter's bias for this member = mean(innovation) + m_k
        # innovation = log_mid - m_k - y => log_mid - y = innovation + m_k
        # So empirical_bias ≈ m_k if filter is correct
        # The mean innovation should be ~0 if filter is correct (test 1a)
        # Error = |empirical_bias - m_k| but we don't have m_k in ticks.
        # Use: empirical_bias = mean(log_mid - y) and the innovation mean:
        mean_innov = merged["innovation"].mean()
        # m_k ≈ empirical_bias - mean_innov (residual)
        # Error is |mean_innov| in bps
        error_bps = abs(mean_innov) * BPS
        results.append(
            dict(
                test=f"4_bias_error_member{mi}",
                value=f"{error_bps:.3f} bps",
                threshold="< 0.5 bps",
                passed=error_bps < 0.5,
            )
        )

    return results


def test5_noise_calibration(ticks: pd.DataFrame, group: pd.DataFrame) -> list[dict]:
    """Test 5: Per-exchange noise calibration.

    var(log_mid - m_k - y) should equal sigma_k^2 + P.
    We use innovation = log_mid - m_k - y, so var(innovation) ≈ s = P + sigma_k^2.
    """
    results = []

    snap_p = group.set_index("snap_ts_ns")["p"]

    for mi, grp in ticks.groupby("member_idx"):
        merged = grp.merge(
            snap_p.rename("p_snap"),
            left_on="snap_ts_ns",
            right_index=True,
            how="inner",
        )
        if len(merged) < 200:
            continue

        empirical_var = merged["innovation"].var()
        # Expected: mean(s) for this member (s = P + sigma_k^2 at each tick)
        expected_var = merged["s"].mean()

        if expected_var > 0:
            ratio = empirical_var / expected_var
            results.append(
                dict(
                    test=f"5_noise_ratio_member{mi}",
                    value=f"{ratio:.4f}",
                    threshold="[0.8, 1.2]",
                    passed=0.8 <= ratio <= 1.2,
                )
            )

    return results


def test6_edge_predictiveness(
    ticks: pd.DataFrame, group: pd.DataFrame
) -> list[dict]:
    """Test 6: Edge predictiveness.

    For each member, compute edge = (fair_at_exchange - mid) / fair_at_exchange * BPS
    where fair_at_exchange = exp(y + m_k), mid = exp(log_mid).
    Then regress future delta_mid on edge.
    """
    results = []

    snap_y = group.set_index("snap_ts_ns")["y"]

    for mi, grp in ticks.groupby("member_idx"):
        # Deduplicate to one observation per snap per member (take last)
        grp = grp.sort_values("tick_ts_ns").drop_duplicates(
            subset="snap_ts_ns", keep="last"
        )
        merged = grp.merge(
            snap_y.rename("y_snap"),
            left_on="snap_ts_ns",
            right_index=True,
            how="inner",
        )
        if len(merged) < 200:
            continue

        merged = merged.sort_values("snap_ts_ns").reset_index(drop=True)

        # edge_mid_bps = (exp(y + m_k) - exp(log_mid)) / exp(y + m_k) * BPS
        # Since innovation = log_mid - m_k - y, we have:
        # edge_mid_bps ≈ -innovation * BPS  (first-order Taylor)
        edge = -merged["innovation"].values * BPS

        # Future return of mid: delta_log_mid at horizon k=10 snaps
        log_mid = merged["log_mid"].values
        k = 10
        if len(log_mid) <= k:
            continue
        delta_mid = (log_mid[k:] - log_mid[:-k]) * BPS
        edge_trim = edge[:-k]

        valid = np.isfinite(delta_mid) & np.isfinite(edge_trim)
        delta_mid = delta_mid[valid]
        edge_trim = edge_trim[valid]
        if len(delta_mid) < 100:
            continue

        slope, intercept, r_value, p_value, se = stats.linregress(
            edge_trim, delta_mid
        )
        hit_rate = np.mean(np.sign(edge_trim) == np.sign(delta_mid))

        results.append(
            dict(
                test=f"6a_edge_beta_member{mi}",
                value=f"{slope:.4f} (p={p_value:.2e})",
                threshold="beta > 0",
                passed=slope > 0,
            )
        )
        results.append(
            dict(
                test=f"6b_edge_hitrate_member{mi}",
                value=f"{hit_rate:.3f}",
                threshold="> 0.50",
                passed=hit_rate > 0.50,
            )
        )

    return results


def test7_normality(ticks: pd.DataFrame) -> list[dict]:
    """Test 7: Innovation normality (Jarque-Bera + kurtosis)."""
    results = []
    z = ticks["standardized_innov"].values
    z = z[np.isfinite(z)]
    if len(z) < 100:
        return results

    kurt = float(stats.kurtosis(z, fisher=False))  # excess=False → raw kurtosis
    jb_stat, jb_p = stats.jarque_bera(z)

    results.append(
        dict(
            test="7a_kurtosis",
            value=f"{kurt:.2f}",
            threshold="< 8",
            passed=kurt < 8,
        )
    )
    results.append(
        dict(
            test="7b_jarque_bera",
            value=f"JB={jb_stat:.1f} (p={jb_p:.2e})",
            threshold="informational",
            passed=True,  # informational, crypto is expected to be fat-tailed
        )
    )

    return results


def test8_regime_tracking(group: pd.DataFrame) -> list[dict]:
    """Test 8: Regime tracking — GARCH adaptation speed."""
    results = []
    h = group["h"].values
    ewma_r2 = group["ewma_r2"].values
    r2c = group["r2_corrected"].values

    if len(h) < 6000:  # need at least 10 min of data
        results.append(
            dict(
                test="8_regime_tracking",
                value="insufficient data",
                threshold="-",
                passed=False,
            )
        )
        return results

    # Rolling 10-min window (6000 snaps at 100ms)
    window = 6000
    n = len(h)
    tracking_errors = []

    for start in range(0, n - window, window):
        end = start + window
        block_h = h[start:end]
        block_r2c = r2c[start:end]
        # Mean absolute error of h vs rolling mean of r2_corrected
        realized_vol = np.mean(block_r2c)
        mean_h = np.mean(block_h)
        if realized_vol > 0:
            tracking_errors.append(abs(mean_h - realized_vol) / realized_vol)

    if tracking_errors:
        mae = np.mean(tracking_errors)
        results.append(
            dict(
                test="8_regime_tracking_MAE",
                value=f"{mae:.4f}",
                threshold="< 0.5",
                passed=mae < 0.5,
            )
        )

    return results


# ── Plotting ─────────────────────────────────────────────────────────────────


def make_plots(
    ticks: pd.DataFrame,
    group: pd.DataFrame,
    recalib: pd.DataFrame,
    output_dir: Path,
    gi: int,
):
    """Generate diagnostic plots for one group."""
    plot_dir = output_dir / "plots"
    plot_dir.mkdir(exist_ok=True)

    z = ticks["standardized_innov"].values
    z = z[np.isfinite(z)]

    # 1. Innovation histogram vs N(0,1)
    if len(z) > 100:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(z, bins=200, density=True, alpha=0.7, label="empirical")
        x = np.linspace(-5, 5, 200)
        ax.plot(x, stats.norm.pdf(x), "r-", lw=2, label="N(0,1)")
        ax.set_xlim(-5, 5)
        ax.set_title(f"Group {gi}: Standardized Innovations")
        ax.legend()
        fig.savefig(plot_dir / f"g{gi}_innovation_hist.png", dpi=100)
        plt.close(fig)

    # 2. Innovation autocorrelation
    if len(z) > 100:
        max_lag = min(50, len(z) - 1)
        acf = np.correlate(z - z.mean(), z - z.mean(), mode="full")
        acf = acf[len(acf) // 2 :]
        acf = acf / acf[0]
        lags = np.arange(1, max_lag + 1)

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(lags, acf[1 : max_lag + 1], width=0.8, alpha=0.7)
        ci = 1.96 / np.sqrt(len(z))
        ax.axhline(ci, color="r", ls="--", alpha=0.5)
        ax.axhline(-ci, color="r", ls="--", alpha=0.5)
        ax.axhline(0, color="k", lw=0.5)
        ax.set_xlabel("Lag")
        ax.set_ylabel("ACF")
        ax.set_title(f"Group {gi}: Innovation Autocorrelation")
        fig.savefig(plot_dir / f"g{gi}_innovation_acf.png", dpi=100)
        plt.close(fig)

    # 3. GARCH h vs ewma_r2 vs r2_corrected time series
    if len(group) > 100:
        t = np.arange(len(group)) * 0.1  # seconds
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(t, np.sqrt(group["h"].values) * BPS, label="sqrt(h) bps", alpha=0.8)
        ax.plot(
            t,
            np.sqrt(group["ewma_r2"].values) * BPS,
            label="sqrt(ewma_r2) bps",
            alpha=0.8,
        )
        # Smooth r2_corrected for visibility
        r2c = group["r2_corrected"].values
        window = min(100, len(r2c))
        if window > 1:
            r2c_smooth = np.convolve(r2c, np.ones(window) / window, mode="valid")
            t_smooth = t[window - 1 :]
            ax.plot(
                t_smooth,
                np.sqrt(np.maximum(r2c_smooth, 0)) * BPS,
                label=f"sqrt(r2c) {window}-MA bps",
                alpha=0.6,
            )
        ax.set_xlabel("Time (s)")
        ax.set_ylabel("Vol (bps/tick)")
        ax.set_title(f"Group {gi}: Volatility Tracking")
        ax.legend()
        fig.savefig(plot_dir / f"g{gi}_vol_tracking.png", dpi=100)
        plt.close(fig)

    # 4. P_unc coverage by horizon
    y = group["y"].values
    p = group["p"].values
    h = group["h"].values
    horizons = [1, 5, 10, 50, 100]
    cov_1s = []
    cov_2s = []
    valid_h = []
    for k in horizons:
        if len(y) <= k + 1:
            continue
        dy = y[k:] - y[:-k]
        sigma = np.sqrt(p[:-k] + k * h[:-k])
        mask = sigma > 0
        if mask.sum() < 100:
            continue
        z_fwd = dy[mask] / sigma[mask]
        cov_1s.append(np.mean(np.abs(z_fwd) < 1.0))
        cov_2s.append(np.mean(np.abs(z_fwd) < 2.0))
        valid_h.append(k)

    if valid_h:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.plot(valid_h, cov_1s, "o-", label="1-sigma coverage")
        ax.axhline(0.6827, color="b", ls="--", alpha=0.5, label="68.27% target")
        ax.plot(valid_h, cov_2s, "s-", label="2-sigma coverage")
        ax.axhline(0.9545, color="r", ls="--", alpha=0.5, label="95.45% target")
        ax.set_xlabel("Horizon (ticks)")
        ax.set_ylabel("Coverage")
        ax.set_title(f"Group {gi}: P_unc Coverage by Horizon")
        ax.legend()
        fig.savefig(plot_dir / f"g{gi}_coverage.png", dpi=100)
        plt.close(fig)

    # 5. Recalib convergence
    if len(recalib) > 0:
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        for mi, grp in recalib.groupby("member_idx"):
            t = (grp["snap_ts_ns"].values - grp["snap_ts_ns"].values[0]) / 1e9
            axes[0].plot(t, grp["bias_new"].values * BPS, label=f"m{mi}")
            axes[1].plot(
                t, np.sqrt(grp["noise_var_new"].values) * BPS, label=f"m{mi}"
            )
        axes[0].set_xlabel("Time (s)")
        axes[0].set_ylabel("Bias (bps)")
        axes[0].set_title(f"Group {gi}: Bias Convergence")
        axes[0].legend(fontsize=7)
        axes[1].set_xlabel("Time (s)")
        axes[1].set_ylabel("sigma_k (bps)")
        axes[1].set_title(f"Group {gi}: Noise Std Convergence")
        axes[1].legend(fontsize=7)
        fig.tight_layout()
        fig.savefig(plot_dir / f"g{gi}_recalib.png", dpi=100)
        plt.close(fig)


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Fair price engine validation")
    parser.add_argument(
        "--data-dir", default="data/fp_diag", help="Directory with csv.gz files"
    )
    parser.add_argument(
        "--warmup-secs", type=int, default=900, help="Warmup period to strip (seconds)"
    )
    parser.add_argument(
        "--no-plots", action="store_true", help="Skip plot generation"
    )
    parser.add_argument(
        "--group", type=int, default=None, help="Only analyze this group index"
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    warmup_ns = args.warmup_secs * 1_000_000_000

    # Load data
    print(f"Loading data from {data_dir}/")
    ticks = load_csv(data_dir / "tick_innovations.csv.gz")
    group = load_csv(data_dir / "group_snaps.csv.gz")
    recalib = load_csv(data_dir / "recalib_events.csv.gz")

    print(
        f"  Raw: {len(ticks)} tick rows, {len(group)} group rows, {len(recalib)} recalib rows"
    )

    # Strip warmup
    ticks = strip_warmup(ticks, warmup_ns)
    group = strip_warmup(group, warmup_ns)
    recalib = strip_warmup(recalib, warmup_ns)

    print(
        f"  After warmup strip ({args.warmup_secs}s): {len(ticks)} tick rows, {len(group)} group rows"
    )

    if len(ticks) == 0 or len(group) == 0:
        print("ERROR: No data after warmup stripping. Capture longer or reduce --warmup-secs.")
        sys.exit(1)

    # Run per group
    group_indices = [args.group] if args.group is not None else sorted(group["group_idx"].unique())

    all_results = []

    for gi in group_indices:
        print(f"\n{'='*60}")
        print(f"  Group {gi}")
        print(f"{'='*60}")

        g_ticks = ticks[ticks["group_idx"] == gi].copy()
        g_group = group[group["group_idx"] == gi].copy()
        g_recalib = recalib[recalib["group_idx"] == gi].copy() if len(recalib) > 0 else pd.DataFrame()

        n_ticks = len(g_ticks)
        n_snaps = len(g_group)
        duration_s = 0
        if n_snaps > 0:
            duration_s = (g_group["snap_ts_ns"].max() - g_group["snap_ts_ns"].min()) / 1e9

        print(f"  {n_ticks} ticks, {n_snaps} snaps, {duration_s:.0f}s duration")

        if n_ticks < 100 or n_snaps < 100:
            print("  SKIP: insufficient data")
            continue

        results = []
        results.extend(test1_innovation_whiteness(g_ticks))
        results.extend(test2_punc_coverage(g_group))
        results.extend(test3_garch_vol(g_group))
        results.extend(test4_bias(g_ticks, g_group))
        results.extend(test5_noise_calibration(g_ticks, g_group))
        results.extend(test6_edge_predictiveness(g_ticks, g_group))
        results.extend(test7_normality(g_ticks))
        results.extend(test8_regime_tracking(g_group))

        # Print results table
        print(f"\n  {'Test':<35} {'Value':<30} {'Threshold':<25} {'Result':<6}")
        print(f"  {'-'*35} {'-'*30} {'-'*25} {'-'*6}")
        for r in results:
            status = "PASS" if r["passed"] else "FAIL"
            marker = " " if r["passed"] else "*"
            print(
                f" {marker}{r['test']:<35} {r['value']:<30} {r['threshold']:<25} {status}"
            )

        n_pass = sum(1 for r in results if r["passed"])
        n_total = len(results)
        n_fail = n_total - n_pass
        print(f"\n  Summary: {n_pass}/{n_total} passed, {n_fail} failed")

        for r in results:
            r["group_idx"] = gi
        all_results.extend(results)

        if not args.no_plots:
            make_plots(g_ticks, g_group, g_recalib, data_dir, gi)
            print(f"  Plots saved to {data_dir}/plots/")

    # Overall summary
    if all_results:
        print(f"\n{'='*60}")
        print("  OVERALL SUMMARY")
        print(f"{'='*60}")
        n_pass = sum(1 for r in all_results if r["passed"])
        n_total = len(all_results)
        n_fail = n_total - n_pass
        print(f"  {n_pass}/{n_total} tests passed across {len(group_indices)} group(s)")
        if n_fail > 0:
            print(f"\n  Failed tests:")
            for r in all_results:
                if not r["passed"]:
                    print(f"    group {r['group_idx']}: {r['test']} = {r['value']}")

            print("\n  Failure -> Fix lookup:")
            print("    z mean != 0        -> Adjust prior_weight, ruler_halflife")
            print("    z var > 1          -> Increase noise_var prior")
            print("    z var < 1          -> Decrease noise_var prior")
            print("    lag-1 autocorr > 0 -> Increase alpha or initial_var")
            print("    lag-1 autocorr < 0 -> Decrease alpha")
            print("    P_unc undercoverage -> Check GARCH floor, alpha+beta")
            print("    Edge beta < 0      -> Fundamental model problem")
            print("    Kurtosis > 10      -> Add tick rejection")


if __name__ == "__main__":
    main()
